#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Procedural macros for the `dingding` bot framework.
//!
//! This crate is intentionally small and is normally consumed through
//! `dingding`'s optional `macros` feature, which re-exports [`handler`].

use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use quote::{format_ident, quote};
use syn::{Expr, ExprLit, ItemFn, Lit, Meta, Token, parse_macro_input, punctuated::Punctuated};

/// Declares a DingTalk bot handler and generates a `<function>_route` helper.
///
/// Example:
///
/// ```ignore
/// #[dingding::handler(scope = Scope::Group, msg = Msg::Text, commands = ["/ping", "ping"])]
/// async fn ping(ctx: dingding::bot::GroupContext) -> dingding::Result<()> {
///     ctx.reply_text("pong").await
/// }
/// ```
#[proc_macro_attribute]
pub fn handler(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr with Punctuated::<Meta, Token![,]>::parse_terminated);
    let input = parse_macro_input!(item as ItemFn);

    expand_handler(args, input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn expand_handler(
    args: Punctuated<Meta, Token![,]>,
    input: ItemFn,
) -> syn::Result<proc_macro2::TokenStream> {
    if input.sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            input.sig.fn_token,
            "dingding handlers must be async functions",
        ));
    }

    let mut scope = "any";
    let mut msg = "any";
    let mut command = None::<CommandConfig>;
    let mut filter_markers = Vec::<Expr>::new();

    for arg in args {
        let Meta::NameValue(name_value) = arg else {
            return Err(syn::Error::new_spanned(arg, "expected `key = value`"));
        };

        let key = name_value.path;
        let value = name_value.value;
        if key.is_ident("scope") {
            scope = scope_filter(&value)?;
            if should_mark_path_as_used(&value) {
                filter_markers.push(value);
            }
        } else if key.is_ident("msg") || key.is_ident("message") {
            msg = message_filter(&value)?;
            if should_mark_path_as_used(&value) {
                filter_markers.push(value);
            }
        } else if key.is_ident("command") {
            set_command_config(
                &mut command,
                CommandConfig::single(command_expr(&value)?),
                &value,
            )?;
        } else if key.is_ident("commands") {
            set_command_config(
                &mut command,
                CommandConfig::many(commands_expr(&value)?),
                &value,
            )?;
        } else {
            return Err(syn::Error::new_spanned(
                key,
                "unknown dingding handler option",
            ));
        }
    }

    let crate_path = dingding_crate_path();
    let scope_tokens = scope_tokens(&crate_path, scope)?;
    let ctx_tokens = context_tokens(scope)?;
    let message_tokens = message_tokens(&crate_path, msg)?;
    let route_ident = format_ident!("{}_route", input.sig.ident);
    let fn_ident = &input.sig.ident;
    let vis = &input.vis;
    let arg_count = input.sig.inputs.len();
    let call = match arg_count {
        0 => quote! { #fn_ident().await },
        1 => quote! { #fn_ident(ctx).await },
        2 => quote! { #fn_ident(ctx, event).await },
        _ => {
            return Err(syn::Error::new_spanned(
                &input.sig.inputs,
                "dingding handlers may accept at most `(ctx, event)`",
            ));
        }
    };
    let command_tokens = command
        .as_ref()
        .map(CommandConfig::tokens)
        .unwrap_or_else(|| quote! {});
    let filter_marker_tokens = if filter_markers.is_empty() {
        quote! {}
    } else {
        quote! {
            const _: () = {
                #(let _ = #filter_markers;)*
            };
        }
    };

    Ok(quote! {
        #input
        #filter_marker_tokens

        #vis fn #route_ident() -> #crate_path::bot::Route {
            #crate_path::bot::Route::new(#scope_tokens)
                .message_type(#message_tokens)
                #command_tokens
                .handle(|ctx, event| async move {
                    let ctx = #ctx_tokens;
                    #call
                })
        }
    })
}

fn value_name(expr: &Expr) -> syn::Result<String> {
    match expr {
        Expr::Lit(ExprLit {
            lit: Lit::Str(value),
            ..
        }) => Ok(value.value()),
        Expr::Path(path) if path.qself.is_none() => path
            .path
            .segments
            .last()
            .map(|segment| segment.ident.to_string())
            .ok_or_else(|| syn::Error::new_spanned(expr, "expected non-empty path")),
        _ => Err(syn::Error::new_spanned(
            expr,
            "expected string literal, bare identifier, or enum variant path",
        )),
    }
}

fn should_mark_path_as_used(expr: &Expr) -> bool {
    let Expr::Path(path) = expr else {
        return false;
    };
    if path.qself.is_some() {
        return false;
    }

    let Some(last) = path.path.segments.last() else {
        return false;
    };

    path.path.segments.len() > 1
        || last
            .ident
            .to_string()
            .chars()
            .next()
            .is_some_and(char::is_uppercase)
}

fn command_expr(expr: &Expr) -> syn::Result<Expr> {
    match expr {
        Expr::Lit(ExprLit {
            lit: Lit::Str(_), ..
        })
        | Expr::Path(_) => Ok(expr.clone()),
        _ => Err(syn::Error::new_spanned(
            expr,
            "command must be a string literal or path to a string constant",
        )),
    }
}

fn commands_expr(expr: &Expr) -> syn::Result<Expr> {
    match expr {
        Expr::Array(array) => {
            for element in &array.elems {
                command_expr(element)?;
            }
            Ok(expr.clone())
        }
        Expr::Path(_) => Ok(expr.clone()),
        _ => Err(syn::Error::new_spanned(
            expr,
            "commands must be an array of string literals/string constants or a path to an iterable of strings",
        )),
    }
}

enum CommandConfig {
    Single(Expr),
    Many(Expr),
}

impl CommandConfig {
    fn single(expr: Expr) -> Self {
        Self::Single(expr)
    }

    fn many(expr: Expr) -> Self {
        Self::Many(expr)
    }

    fn tokens(&self) -> proc_macro2::TokenStream {
        match self {
            Self::Single(command) => quote! { .command(#command) },
            Self::Many(commands) => quote! { .commands(#commands) },
        }
    }
}

fn set_command_config(
    slot: &mut Option<CommandConfig>,
    value: CommandConfig,
    span: &Expr,
) -> syn::Result<()> {
    if slot.is_some() {
        return Err(syn::Error::new_spanned(
            span,
            "use either `command` or `commands`, not both",
        ));
    }

    *slot = Some(value);
    Ok(())
}

fn scope_filter(expr: &Expr) -> syn::Result<&'static str> {
    match value_name(expr)?.as_str() {
        "any" | "Any" => Ok("any"),
        "group" | "Group" => Ok("group"),
        "private" | "Private" | "single" | "Single" | "oto" | "Oto" => Ok("private"),
        _ => Err(syn::Error::new_spanned(
            expr,
            "scope must be one of: Scope::Any, Scope::Group, Scope::Private",
        )),
    }
}

fn message_filter(expr: &Expr) -> syn::Result<&'static str> {
    match value_name(expr)?.as_str() {
        "any" | "Any" => Ok("any"),
        "text" | "Text" => Ok("text"),
        "markdown" | "Markdown" => Ok("markdown"),
        "audio" | "Audio" => Ok("audio"),
        "picture" | "Picture" => Ok("picture"),
        "video" | "Video" => Ok("video"),
        "file" | "File" => Ok("file"),
        "richText" | "rich_text" | "RichText" => Ok("richText"),
        _ => Err(syn::Error::new_spanned(
            expr,
            "msg must be one of: Msg::Any, Msg::Text, Msg::Markdown, Msg::Audio, Msg::Picture, Msg::Video, Msg::File, Msg::RichText",
        )),
    }
}

fn dingding_crate_path() -> proc_macro2::TokenStream {
    match crate_name("dingding") {
        Ok(FoundCrate::Itself) => quote!(::dingding),
        Ok(FoundCrate::Name(name)) => {
            let ident = syn::Ident::new(&name, proc_macro2::Span::call_site());
            quote!(::#ident)
        }
        Err(_error) => quote!(::dingding),
    }
}

fn scope_tokens(
    crate_path: &proc_macro2::TokenStream,
    scope: &str,
) -> syn::Result<proc_macro2::TokenStream> {
    match scope {
        "any" => Ok(quote! { #crate_path::bot::ConversationScope::Any }),
        "group" => Ok(quote! { #crate_path::bot::ConversationScope::Group }),
        "private" | "single" | "oto" => Ok(quote! { #crate_path::bot::ConversationScope::Private }),
        _ => Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "scope must be one of: any, group, private",
        )),
    }
}

fn context_tokens(scope: &str) -> syn::Result<proc_macro2::TokenStream> {
    match scope {
        "any" => Ok(quote! { ctx }),
        "group" => Ok(quote! { ctx.into_group()? }),
        "private" | "single" | "oto" => Ok(quote! { ctx.into_private()? }),
        _ => Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "scope must be one of: any, group, private",
        )),
    }
}

fn message_tokens(
    crate_path: &proc_macro2::TokenStream,
    msg: &str,
) -> syn::Result<proc_macro2::TokenStream> {
    match msg {
        "any" => Ok(quote! { #crate_path::bot::MessageType::Any }),
        "text" => Ok(quote! { #crate_path::bot::MessageType::Text }),
        "markdown" => Ok(quote! { #crate_path::bot::MessageType::Markdown }),
        "audio" => Ok(quote! { #crate_path::bot::MessageType::Audio }),
        "picture" => Ok(quote! { #crate_path::bot::MessageType::Picture }),
        "video" => Ok(quote! { #crate_path::bot::MessageType::Video }),
        "file" => Ok(quote! { #crate_path::bot::MessageType::File }),
        "richText" | "rich_text" => Ok(quote! { #crate_path::bot::MessageType::RichText }),
        _ => Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "msg must be one of: any, text, markdown, audio, picture, video, file, richText",
        )),
    }
}
