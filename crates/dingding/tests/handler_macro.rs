#![allow(clippy::expect_used)]

use std::sync::atomic::{AtomicUsize, Ordering};

use dingding::{
    DingTalk, Result,
    bot::{
        AnyContext, Bot, BotEvent, ConversationScope, GroupContext, HandleOutcome, Msg,
        PrivateContext, Scope,
    },
};

const PING: &str = "/ping";
const PING_ALIASES: [&str; 2] = ["/ping", "ping"];

static PATH_HIT_COUNT: AtomicUsize = AtomicUsize::new(0);
static IDENT_HIT_COUNT: AtomicUsize = AtomicUsize::new(0);
static STRING_HIT_COUNT: AtomicUsize = AtomicUsize::new(0);
static PASCAL_IDENT_HIT_COUNT: AtomicUsize = AtomicUsize::new(0);
static ALIASES_HIT_COUNT: AtomicUsize = AtomicUsize::new(0);
static ALIASES_PATH_HIT_COUNT: AtomicUsize = AtomicUsize::new(0);
static ZERO_ARG_HIT_COUNT: AtomicUsize = AtomicUsize::new(0);
static MESSAGE_ALIAS_HIT_COUNT: AtomicUsize = AtomicUsize::new(0);

#[dingding::handler(command = "/unit-zero")]
async fn unit_zero() {}

#[dingding::handler(scope = Scope::Group, command = "/unit-group")]
async fn unit_group(ctx: GroupContext) {
    assert!(ctx.is_group());
}

#[dingding::handler(scope = Scope::Private, command = "/unit-private")]
async fn unit_private(ctx: PrivateContext) {
    assert!(ctx.is_private());
    assert_eq!(ctx.event().conversation_scope, Scope::Private);
}

#[tokio::test]
async fn handler_macro_accepts_unit_for_all_supported_signatures() -> Result<()> {
    let bot = Bot::new(DingTalk::new()?)
        .route(unit_zero_route())
        .route(unit_group_route())
        .route(unit_private_route());
    for (scope, command) in [
        (Scope::Private, "/unit-zero"),
        (Scope::Group, "/unit-group"),
        (Scope::Private, "/unit-private"),
    ] {
        assert_eq!(
            bot.handle_event(BotEvent::text(scope, command)).await?,
            HandleOutcome::Matched
        );
    }
    assert_eq!(
        bot.handle_event(BotEvent::text(Scope::Private, "/unit-group"))
            .await?,
        HandleOutcome::Ignored
    );
    Ok(())
}

#[dingding::handler(scope = Scope::Any, msg = Msg::Text, command = "/io")]
async fn io_handler(_ctx: AnyContext) -> std::io::Result<()> {
    Err(std::io::Error::other("application IO failure"))
}

#[dingding::handler(scope = Scope::Group, msg = Msg::Text, command = "/mixed")]
async fn mixed_handler(ctx: GroupContext) -> dingding::HandlerResult {
    assert!(ctx.is_group());
    let _ = String::from_utf8(b"text".to_vec())?;
    Err(dingding::Error::MissingCredentials.into())
}

#[dingding::handler(scope = Scope::Private, command = "/zero")]
async fn io_zero_arg() -> std::io::Result<()> {
    Ok(())
}

#[tokio::test]
async fn handler_macro_accepts_application_errors_and_preserves_sdk_errors()
-> dingding::HandlerResult {
    use std::error::Error as _;
    let bot = Bot::new(DingTalk::new()?)
        .route(io_handler_route())
        .route(mixed_handler_route())
        .route(io_zero_arg_route());
    let error = bot
        .handle_event(BotEvent::text(Scope::Private, "/io"))
        .await
        .expect_err("IO failure");
    assert_eq!(error.kind(), dingding::ErrorKind::Handler);
    assert!(
        error
            .source()
            .and_then(|source| source.downcast_ref::<std::io::Error>())
            .is_some()
    );
    let error = bot
        .handle_event(BotEvent::text(Scope::Group, "/mixed"))
        .await
        .expect_err("SDK failure");
    assert_eq!(error.kind(), dingding::ErrorKind::MissingCredentials);
    assert_eq!(
        bot.handle_event(BotEvent::text(Scope::Private, "/zero"))
            .await?,
        HandleOutcome::Matched
    );
    Ok(())
}

#[dingding::handler(scope = Scope::Any, msg = Msg::Text, command = PING)]
async fn ping_path(_ctx: AnyContext) -> Result<()> {
    PATH_HIT_COUNT.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

#[dingding::handler(scope = group, msg = text, command = "/ping")]
async fn ping_ident(_ctx: GroupContext) -> Result<()> {
    IDENT_HIT_COUNT.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

#[dingding::handler(scope = "private", msg = "text", command = "/ping")]
async fn ping_string(_ctx: PrivateContext) -> Result<()> {
    STRING_HIT_COUNT.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

#[dingding::handler(scope = Group, msg = Text, command = "/ping")]
async fn ping_pascal_ident(_ctx: GroupContext) -> Result<()> {
    PASCAL_IDENT_HIT_COUNT.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

#[dingding::handler(scope = Scope::Any, msg = Msg::Text, commands = ["/ping", "ping"])]
async fn ping_aliases(ctx: AnyContext) -> Result<()> {
    assert_eq!(ctx.command(), Some("ping"));
    assert_eq!(ctx.args(), Some("ops"));
    ALIASES_HIT_COUNT.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

#[dingding::handler(scope = Scope::Any, msg = Msg::Text, commands = PING_ALIASES)]
async fn ping_aliases_path(ctx: AnyContext) -> Result<()> {
    assert_eq!(ctx.command(), Some("/ping"));
    assert_eq!(ctx.args(), Some("api"));
    ALIASES_PATH_HIT_COUNT.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

#[dingding::handler(scope = any, msg = text, command = "/ping")]
async fn ping_zero_arg() -> Result<()> {
    ZERO_ARG_HIT_COUNT.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

#[dingding::handler(scope = any, msg = image)]
async fn image_alias(_ctx: AnyContext) -> Result<()> {
    MESSAGE_ALIAS_HIT_COUNT.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

#[dingding::handler(scope = any, msg = voice)]
async fn voice_alias(_ctx: AnyContext) -> Result<()> {
    MESSAGE_ALIAS_HIT_COUNT.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

#[tokio::test]
async fn handler_macro_accepts_enum_path_filters() {
    PATH_HIT_COUNT.store(0, Ordering::SeqCst);

    let client = DingTalk::builder().build().expect("client");
    let bot = Bot::new(client).route(ping_path_route());
    let outcome = bot
        .handle_event(BotEvent::text(ConversationScope::Group, "/ping"))
        .await
        .expect("handler should run");

    assert_eq!(outcome, HandleOutcome::Matched);
    assert_eq!(PATH_HIT_COUNT.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn handler_macro_accepts_bare_ident_filters() {
    IDENT_HIT_COUNT.store(0, Ordering::SeqCst);

    let client = DingTalk::builder().build().expect("client");
    let bot = Bot::new(client).route(ping_ident_route());
    let outcome = bot
        .handle_event(BotEvent::text(ConversationScope::Group, "/ping"))
        .await
        .expect("handler should run");

    assert_eq!(outcome, HandleOutcome::Matched);
    assert_eq!(IDENT_HIT_COUNT.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn handler_macro_still_accepts_string_filters() {
    STRING_HIT_COUNT.store(0, Ordering::SeqCst);

    let client = DingTalk::builder().build().expect("client");
    let bot = Bot::new(client).route(ping_string_route());
    let outcome = bot
        .handle_event(BotEvent::text(ConversationScope::Private, "/ping"))
        .await
        .expect("handler should run");

    assert_eq!(outcome, HandleOutcome::Matched);
    assert_eq!(STRING_HIT_COUNT.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn handler_macro_accepts_pascal_case_bare_ident_filters() {
    PASCAL_IDENT_HIT_COUNT.store(0, Ordering::SeqCst);

    let client = DingTalk::builder().build().expect("client");
    let bot = Bot::new(client).route(ping_pascal_ident_route());
    let outcome = bot
        .handle_event(BotEvent::text(ConversationScope::Group, "/ping"))
        .await
        .expect("handler should run");

    assert_eq!(outcome, HandleOutcome::Matched);
    assert_eq!(PASCAL_IDENT_HIT_COUNT.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn handler_macro_accepts_command_aliases() {
    ALIASES_HIT_COUNT.store(0, Ordering::SeqCst);

    let client = DingTalk::builder().build().expect("client");
    let bot = Bot::new(client).route(ping_aliases_route());
    let outcome = bot
        .handle_event(BotEvent::text(ConversationScope::Group, "ping ops"))
        .await
        .expect("handler should run");

    assert_eq!(outcome, HandleOutcome::Matched);
    assert_eq!(ALIASES_HIT_COUNT.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn handler_macro_accepts_command_alias_path() {
    ALIASES_PATH_HIT_COUNT.store(0, Ordering::SeqCst);

    let client = DingTalk::builder().build().expect("client");
    let bot = Bot::new(client).route(ping_aliases_path_route());
    let outcome = bot
        .handle_event(BotEvent::text(ConversationScope::Group, "/ping api"))
        .await
        .expect("handler should run");

    assert_eq!(outcome, HandleOutcome::Matched);
    assert_eq!(ALIASES_PATH_HIT_COUNT.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn handler_macro_accepts_zero_arg_handlers() {
    ZERO_ARG_HIT_COUNT.store(0, Ordering::SeqCst);

    let client = DingTalk::builder().build().expect("client");
    let bot = Bot::new(client).route(ping_zero_arg_route());
    let outcome = bot
        .handle_event(BotEvent::text(ConversationScope::Private, "/ping"))
        .await
        .expect("handler should run");

    assert_eq!(outcome, HandleOutcome::Matched);
    assert_eq!(ZERO_ARG_HIT_COUNT.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn handler_macro_accepts_dingtalk_message_aliases() {
    MESSAGE_ALIAS_HIT_COUNT.store(0, Ordering::SeqCst);

    let client = DingTalk::builder().build().expect("client");
    let bot = Bot::new(client)
        .route(image_alias_route())
        .route(voice_alias_route());
    let image = bot
        .handle_event(BotEvent::from_value(serde_json::json!({
            "conversationType": "2",
            "msgtype": "image",
            "content": { "downloadCode": "image-code" }
        })))
        .await
        .expect("image alias handler should run");
    let voice = bot
        .handle_event(BotEvent::from_value(serde_json::json!({
            "conversationType": "2",
            "msgtype": "voice",
            "content": { "downloadCode": "voice-code" }
        })))
        .await
        .expect("voice alias handler should run");

    assert_eq!(image, HandleOutcome::Matched);
    assert_eq!(voice, HandleOutcome::Matched);
    assert_eq!(MESSAGE_ALIAS_HIT_COUNT.load(Ordering::SeqCst), 2);
}
