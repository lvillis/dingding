#![cfg(feature = "bot")]
#![allow(clippy::expect_used, clippy::panic)]

use dingding::{
    DingTalk, Error,
    bot::{
        Bot, BotEvent, ConversationScope, Route,
        dedup::{Deduplication, DeduplicationFuture, EventDeduplicator},
    },
};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

fn event(id: &str) -> BotEvent {
    BotEvent::from_value(
        json!({"msgId":id,"msgtype":"text","conversationType":"2","conversationId":"cid","text":{"content":"command"}}),
    )
}

#[tokio::test]
async fn bot_callbacks_replay_success_and_retry_failure() {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&calls);
    let bot = Bot::new(DingTalk::new().expect("client")).route(
        Route::new(ConversationScope::Any).handle(move |_, _| {
            let first = count.fetch_add(1, Ordering::SeqCst) == 0;
            async move {
                if first {
                    Err(Error::InvalidConfig("injected".into()))
                } else {
                    Ok(())
                }
            }
        }),
    );
    assert!(bot.handle_event(event("one")).await.is_err());
    assert!(
        bot.handle_event(event("one"))
            .await
            .expect("retry")
            .is_matched()
    );
    assert!(
        bot.clone()
            .handle_event(event("one"))
            .await
            .expect("duplicate")
            .is_matched()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    bot.handle_event(event("two")).await.expect("new event");
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn cancelled_bot_callback_releases_its_claim() {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&calls);
    let bot = Bot::new(DingTalk::new().expect("client")).route(
        Route::new(ConversationScope::Any).handle(move |_, _| {
            let first = count.fetch_add(1, Ordering::SeqCst) == 0;
            async move {
                if first {
                    std::future::pending::<()>().await;
                }
            }
        }),
    );
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(20),
            bot.handle_event(event("one"))
        )
        .await
        .is_err()
    );
    bot.handle_event(event("one"))
        .await
        .expect("retry after cancellation");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

struct Unavailable;
impl EventDeduplicator for Unavailable {
    fn claim<'a>(&'a self, _: &'a str) -> DeduplicationFuture<'a, Deduplication> {
        Box::pin(async { Err(Error::InvalidConfig("store unavailable".into())) })
    }
}

#[tokio::test]
async fn custom_store_errors_do_not_execute_unreserved_events() {
    async fn unexpected(_: dingding::bot::BotContext, _: BotEvent) {
        panic!("must not execute");
    }
    let bot = Bot::new(DingTalk::new().expect("client"))
        .deduplicator(Arc::new(Unavailable))
        .route(Route::new(ConversationScope::Any).handle(unexpected));
    assert!(
        bot.handle_event(event("one"))
            .await
            .expect_err("backend error")
            .to_string()
            .contains("store unavailable")
    );
}
