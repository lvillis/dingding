#![cfg(feature = "bot")]
#![allow(clippy::expect_used)]

use std::{error::Error as _, io};

use dingding::{
    DingTalk, Error, ErrorKind, HandlerResult,
    bot::{Bot, BotEvent, ConversationScope, HandleOutcome, MessageType, Route},
    handler_future,
};

#[tokio::test]
async fn handler_future_infers_mixed_errors_and_typed_contexts() -> HandlerResult {
    let bot = Bot::new(DingTalk::new()?)
        .state(42_u32)
        .on_group_text_command("/shortcut", |ctx| {
            handler_future(async move {
                assert_eq!(*ctx.state_required::<u32>()?, 42);
                let _ = String::from_utf8(b"hello".to_vec())?;
                Ok(())
            })
        })
        .route(
            Route::new(ConversationScope::Any)
                .command("/group")
                .handle_group(|ctx| {
                    handler_future(async move {
                        assert!(ctx.is_group());
                        assert_eq!(*ctx.state_required::<u32>()?, 42);
                        Ok(())
                    })
                }),
        )
        .route(
            Route::new(ConversationScope::Any)
                .command("/private")
                .handle_private(|ctx| {
                    handler_future(async move {
                        assert!(ctx.is_private());
                        let _ = serde_json::from_str::<serde_json::Value>("{}")?;
                        Ok(())
                    })
                }),
        )
        .fallback(|_| handler_future(async { Ok(()) }));
    for (scope, text) in [
        (ConversationScope::Group, "/shortcut"),
        (ConversationScope::Group, "/group"),
        (ConversationScope::Private, "/private"),
    ] {
        assert_eq!(
            bot.handle_event(BotEvent::text(scope, text)).await?,
            HandleOutcome::Matched
        );
    }
    assert_eq!(
        bot.handle_event(BotEvent::text(ConversationScope::Group, "other"))
            .await?,
        HandleOutcome::Fallback
    );
    Ok(())
}

#[tokio::test]
async fn handler_future_preserves_sdk_and_application_error_sources() -> HandlerResult {
    let bot = Bot::new(DingTalk::new()?)
        .on_group_text_command("/sdk", |ctx| {
            handler_future(async move {
                ctx.state_required::<u32>()?;
                Ok(())
            })
        })
        .on_group_text_command("/app", |_| {
            handler_future(async move {
                Err(io::Error::other("access_token=application-secret"))?;
                Ok(())
            })
        });
    let sdk = bot
        .handle_event(BotEvent::text(ConversationScope::Group, "/sdk"))
        .await
        .expect_err("missing state");
    assert_eq!(sdk.kind(), ErrorKind::InvalidConfig);
    let app = bot
        .handle_event(BotEvent::text(ConversationScope::Group, "/app"))
        .await
        .expect_err("application error");
    assert_eq!(app.kind(), ErrorKind::Handler);
    assert!(
        app.source()
            .and_then(|error| error.downcast_ref::<io::Error>())
            .is_some()
    );
    assert!(!app.to_string().contains("application-secret"));
    Ok(())
}

#[tokio::test]
async fn scoped_shortcuts_accept_named_typed_handlers() -> HandlerResult {
    use dingding::bot::{GroupContext, PrivateContext};
    async fn group(ctx: GroupContext) -> HandlerResult {
        assert!(ctx.is_group());
        assert_eq!(*ctx.state_required::<u32>()?, 42);
        Ok(())
    }
    async fn private(ctx: PrivateContext) -> HandlerResult {
        assert!(ctx.is_private());
        assert_eq!(*ctx.state_required::<u32>()?, 42);
        Ok(())
    }
    let client = DingTalk::new()?;
    let bot = || Bot::new(client.clone()).state(42_u32);
    let cases = [
        (
            bot().on_group_text_command("/run", group),
            ConversationScope::Group,
        ),
        (
            bot().on_group_text_commands(["/alias", "/run"], group),
            ConversationScope::Group,
        ),
        (
            bot().on_group_message(MessageType::Text, group),
            ConversationScope::Group,
        ),
        (
            bot().on_private_text_command("/run", private),
            ConversationScope::Private,
        ),
        (
            bot().on_private_text_commands(["/alias", "/run"], private),
            ConversationScope::Private,
        ),
        (
            bot().on_private_message(MessageType::Text, private),
            ConversationScope::Private,
        ),
    ];
    for (bot, scope) in cases {
        let other = if scope == ConversationScope::Group {
            ConversationScope::Private
        } else {
            ConversationScope::Group
        };
        assert_eq!(
            bot.handle_event(BotEvent::text(scope, "/run")).await?,
            HandleOutcome::Matched
        );
        assert_eq!(
            bot.handle_event(BotEvent::text(other, "/run")).await?,
            HandleOutcome::Ignored
        );
    }
    #[cfg(feature = "stream")]
    dingding::stream::StreamBot::builder()
        .client_id_and_secret("key", "secret")
        .on_group_text_command("/run", group)
        .on_group_text_commands(["/alias"], group)
        .on_group_message(MessageType::Text, group)
        .on_private_text_command("/run", private)
        .on_private_text_commands(["/alias"], private)
        .on_private_message(MessageType::Text, private)
        .build()?;
    Ok(())
}

#[tokio::test]
async fn unit_handlers_work_with_typed_contexts_and_fallbacks() -> HandlerResult {
    let bot = Bot::new(DingTalk::new()?)
        .route(
            Route::new(ConversationScope::Any)
                .command("/group")
                .handle_group(|ctx| async move {
                    assert!(ctx.is_group());
                }),
        )
        .route(
            Route::new(ConversationScope::Any)
                .command("/private")
                .handle_private(|ctx| async move {
                    assert!(ctx.is_private());
                }),
        )
        .on_text_command(ConversationScope::Any, "/any", |_| async {})
        .fallback(|_| async {});
    for (scope, text, expected) in [
        (ConversationScope::Group, "/group", HandleOutcome::Matched),
        (
            ConversationScope::Private,
            "/private",
            HandleOutcome::Matched,
        ),
        (ConversationScope::Private, "/any", HandleOutcome::Matched),
        (
            ConversationScope::Private,
            "/group",
            HandleOutcome::Fallback,
        ),
    ] {
        assert_eq!(
            bot.handle_event(BotEvent::text(scope, text)).await?,
            expected
        );
    }
    Ok(())
}

#[tokio::test]
async fn routes_accept_concrete_application_errors() -> HandlerResult {
    let bot = Bot::new(DingTalk::new()?).route(
        Route::new(ConversationScope::Any)
            .message_type(MessageType::Text)
            .handle(|_ctx| async { Err::<(), _>(io::Error::other("business operation failed")) }),
    );
    let error = bot
        .handle_event(BotEvent::text(ConversationScope::Private, "hello"))
        .await
        .expect_err("application error");
    assert_eq!(error.kind(), ErrorKind::Handler);
    assert!(
        error
            .source()
            .and_then(|source| source.downcast_ref::<io::Error>())
            .is_some()
    );
    Ok(())
}

#[tokio::test]
async fn typed_routes_accept_mixed_sdk_and_application_errors() -> HandlerResult {
    async fn group(ctx: dingding::bot::GroupContext) -> HandlerResult {
        assert!(ctx.is_group());
        let _ = serde_json::from_str::<serde_json::Value>("{}")?;
        let _ = String::from_utf8(b"message".to_vec())?;
        Ok(())
    }
    async fn private(ctx: dingding::bot::PrivateContext) -> io::Result<()> {
        assert!(ctx.is_private());
        Ok(())
    }
    let bot = Bot::new(DingTalk::new()?)
        .route(Route::new(ConversationScope::Any).handle_group(group))
        .route(Route::new(ConversationScope::Any).handle_private(private));
    for scope in [ConversationScope::Group, ConversationScope::Private] {
        assert_eq!(
            bot.handle_event(BotEvent::text(scope, "hello")).await?,
            HandleOutcome::Matched
        );
    }
    Ok(())
}

#[tokio::test]
async fn routes_preserve_sdk_error_categories() -> HandlerResult {
    let bot = Bot::new(DingTalk::new()?).route(
        Route::new(ConversationScope::Any)
            .handle(|_ctx| async { Err::<(), _>(Error::MissingCredentials) }),
    );
    let error = bot
        .handle_event(BotEvent::text(ConversationScope::Private, "hello"))
        .await
        .expect_err("SDK error");
    assert_eq!(error.kind(), ErrorKind::MissingCredentials);
    Ok(())
}

#[tokio::test]
async fn all_shortcuts_dispatch_application_errors() -> HandlerResult {
    async fn fail<S>(_: dingding::bot::BotContext<S>) -> io::Result<()> {
        Err(io::Error::other("shortcut failed"))
    }
    let client = DingTalk::new()?;
    let bot = || Bot::new(client.clone());
    let cases = [
        (
            bot().on_text_command(ConversationScope::Any, "/run", fail),
            ConversationScope::Private,
        ),
        (
            bot().on_text_commands(ConversationScope::Any, ["/other", "/run"], fail),
            ConversationScope::Group,
        ),
        (
            bot().on_message(ConversationScope::Any, MessageType::Text, fail),
            ConversationScope::Private,
        ),
        (
            bot().on_group_text_command("/run", fail),
            ConversationScope::Group,
        ),
        (
            bot().on_group_text_commands(["/other", "/run"], fail),
            ConversationScope::Group,
        ),
        (
            bot().on_private_text_command("/run", fail),
            ConversationScope::Private,
        ),
        (
            bot().on_private_text_commands(["/other", "/run"], fail),
            ConversationScope::Private,
        ),
        (
            bot().on_group_message(MessageType::Text, fail),
            ConversationScope::Group,
        ),
        (
            bot().on_private_message(MessageType::Text, fail),
            ConversationScope::Private,
        ),
        (bot().fallback(fail), ConversationScope::Private),
        (
            bot().on_unmatched_text(ConversationScope::Group, fail),
            ConversationScope::Group,
        ),
    ];
    for (bot, scope) in cases {
        let error = bot
            .handle_event(BotEvent::text(scope, "/run value"))
            .await
            .expect_err("application error");
        assert_eq!(error.kind(), ErrorKind::Handler);
        assert!(
            error
                .source()
                .and_then(|source| source.downcast_ref::<io::Error>())
                .is_some()
        );
    }
    Ok(())
}

#[tokio::test]
async fn shortcuts_keep_scope_validation_priority_and_sdk_errors() -> HandlerResult {
    let client = DingTalk::new()?;
    let bot = Bot::new(client.clone())
        .on_group_text_commands(["/run", "/r"], |ctx| async move {
            assert_eq!(ctx.args(), Some("value"));
            Err::<(), _>(Error::MissingCredentials)
        })
        .fallback(|_| async { Ok::<_, io::Error>(()) });
    assert_eq!(
        bot.handle_event(BotEvent::text(ConversationScope::Group, "/r value"))
            .await
            .expect_err("SDK error")
            .kind(),
        ErrorKind::MissingCredentials
    );
    assert_eq!(
        bot.handle_event(BotEvent::text(ConversationScope::Private, "/r value"))
            .await?,
        HandleOutcome::Fallback
    );
    let invalid = Bot::new(client)
        .on_private_text_command("bad command", |_| async { Ok::<_, io::Error>(()) });
    assert!(invalid.validate().is_err());
    Ok(())
}

#[cfg(feature = "stream")]
#[test]
fn stream_bot_exposes_all_matching_shortcuts() -> HandlerResult {
    use dingding::stream::StreamBot;
    async fn handler<S>(_: dingding::bot::BotContext<S>) -> HandlerResult {
        Ok(())
    }
    StreamBot::builder()
        .client_id_and_secret("key", "secret")
        .on_text_command(ConversationScope::Any, "/one", handler)
        .on_text_commands(ConversationScope::Any, ["/two"], handler)
        .on_group_text_command("/group", handler)
        .on_group_text_commands(["/g"], handler)
        .on_private_text_command("/private", handler)
        .on_private_text_commands(["/p"], handler)
        .on_group_message(MessageType::Text, handler)
        .on_private_message(MessageType::Text, handler)
        .on_message(ConversationScope::Any, MessageType::Text, handler)
        .on_unmatched_text(ConversationScope::Any, handler)
        .fallback(handler)
        .build()?;
    assert!(
        StreamBot::builder()
            .client_id_and_secret("key", "secret")
            .on_text_command(ConversationScope::Any, "bad command", handler)
            .build()
            .is_err()
    );
    Ok(())
}

#[test]
fn io_errors_retain_sources_and_redact_display() {
    let error = Error::from(io::Error::other("access_token=secret-value"));
    assert_eq!(error.kind(), ErrorKind::Io);
    assert_eq!(error.kind().as_str(), "io");
    assert!(!error.to_string().contains("secret-value"));
    assert!(!format!("{error:?}").contains("secret-value"));
    assert!(
        error
            .source()
            .and_then(|source| source.downcast_ref::<io::Error>())
            .is_some()
    );
}
