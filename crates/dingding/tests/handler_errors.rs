#![cfg(feature = "bot")]
#![allow(clippy::expect_used)]

use std::{error::Error as _, io};

use dingding::{
    DingTalk, Error, ErrorKind, HandlerResult,
    bot::{Bot, BotEvent, ConversationScope, HandleOutcome, MessageType, Route},
};

#[tokio::test]
async fn unit_handlers_work_with_typed_contexts_and_fallbacks() -> HandlerResult {
    let bot = Bot::new(DingTalk::new()?)
        .route(
            Route::new(ConversationScope::Any)
                .command("/group")
                .handle_group(|ctx, _| async move {
                    assert!(ctx.is_group());
                }),
        )
        .route(
            Route::new(ConversationScope::Any)
                .command("/private")
                .handle_private(|ctx, _| async move {
                    assert!(ctx.is_private());
                }),
        )
        .on_text_command(ConversationScope::Any, "/any", |_, _| async {})
        .fallback(|_, _| async {});
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
            .handle(|_ctx, _event| async {
                Err::<(), _>(io::Error::other("business operation failed"))
            }),
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
    async fn group(ctx: dingding::bot::GroupContext, _event: BotEvent) -> HandlerResult {
        assert!(ctx.is_group());
        let _ = serde_json::from_str::<serde_json::Value>("{}")?;
        let _ = String::from_utf8(b"message".to_vec())?;
        Ok(())
    }
    async fn private(ctx: dingding::bot::PrivateContext, _event: BotEvent) -> io::Result<()> {
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
            .handle(|_ctx, _event| async { Err::<(), _>(Error::MissingCredentials) }),
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
    async fn fail(_: dingding::bot::BotContext, _: BotEvent) -> io::Result<()> {
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
        .on_group_text_commands(["/run", "/r"], |ctx, _| async move {
            assert_eq!(ctx.args(), Some("value"));
            Err::<(), _>(Error::MissingCredentials)
        })
        .fallback(|_, _| async { Ok::<_, io::Error>(()) });
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
        .on_private_text_command("bad command", |_, _| async { Ok::<_, io::Error>(()) });
    assert!(invalid.validate().is_err());
    Ok(())
}

#[cfg(feature = "stream")]
#[test]
fn stream_bot_exposes_all_matching_shortcuts() -> HandlerResult {
    use dingding::stream::StreamBot;
    async fn handler(_: dingding::bot::BotContext, _: BotEvent) -> HandlerResult {
        Ok(())
    }
    StreamBot::builder()
        .client_id_and_secret("key", "secret")
        .on_text_command(ConversationScope::Any, "/one", handler)
        .on_text_commands(ConversationScope::Any, ["/two"], handler)
        .on_message(ConversationScope::Any, MessageType::Text, handler)
        .on_group_text_command("/group", handler)
        .on_group_text_commands(["/g"], handler)
        .on_private_text_command("/private", handler)
        .on_private_text_commands(["/p"], handler)
        .on_group_message(MessageType::Text, handler)
        .on_private_message(MessageType::Text, handler)
        .fallback(handler)
        .on_unmatched_text(ConversationScope::Any, handler)
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
