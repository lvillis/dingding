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
static ALIASES_HIT_COUNT: AtomicUsize = AtomicUsize::new(0);
static ALIASES_PATH_HIT_COUNT: AtomicUsize = AtomicUsize::new(0);

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
