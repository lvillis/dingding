#![cfg(feature = "bot")]
#![allow(clippy::expect_used)]

use std::sync::{Arc, Mutex};
use std::{
    error::Error as _,
    sync::atomic::{AtomicUsize, Ordering},
};

use dingding::{ErrorKind, prelude::*};

#[test]
fn overlapping_command_aliases_fail_validation() -> Result<()> {
    for (first, second) in [
        (Scope::Any, Scope::Any),
        (Scope::Any, Scope::Group),
        (Scope::Group, Scope::Any),
        (Scope::Group, Scope::Group),
        (Scope::Private, Scope::Private),
    ] {
        let routes = [
            Route::new(first)
                .commands(["/ping", "ping"])
                .handle(|_| async {}),
            Route::new(second).command("ping").handle(|_| async {}),
        ];
        let bot = Bot::new(DingTalk::new()?)
            .route(routes[0].clone())
            .route(routes[1].clone());
        let error = bot.validate().expect_err("command overlap");
        assert_eq!(error.kind(), ErrorKind::InvalidConfig);
        let message = error.to_string();
        assert!(message.contains("routes[1]"));
        assert!(message.contains("routes[0]"));
        assert!(message.contains("`ping`"));

        #[cfg(feature = "stream")]
        {
            let error = StreamBot::builder()
                .route(routes[0].clone())
                .route(routes[1].clone())
                .build()
                .err()
                .expect("validate before credentials or network access");
            assert_eq!(error.to_string(), message);
            let client = DingTalk::builder()
                .app_key_and_secret("key", "secret")
                .build()?;
            let error = StreamClient::builder(client)?
                .bot(bot)
                .build()
                .err()
                .expect("invalid router");
            assert_eq!(error.to_string(), message);
        }
    }
    Ok(())
}

#[tokio::test]
async fn a_conflict_prevents_any_handler_from_running() -> Result<()> {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = Arc::clone(&calls);
    let bot = Bot::new(DingTalk::new()?)
        .on_text_command(Scope::Any, "/ping", move |_| {
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async {}
        })
        .on_group_text_command("/ping", |_| async {});
    assert_eq!(
        bot.handle_event(BotEvent::text(Scope::Group, "/ping"))
            .await
            .expect_err("invalid router")
            .kind(),
        ErrorKind::InvalidConfig
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn commands_in_disjoint_scopes_and_prefixes_remain_distinct() -> Result<()> {
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    async fn group(ctx: GroupContext<Arc<Mutex<Vec<String>>>>) -> Result<()> {
        assert_eq!(ctx.event().conversation_scope, Scope::Group);
        ctx.state().lock().expect("state").push(format!(
            "group:{}:{}",
            ctx.command().expect("command"),
            ctx.args_or_empty()
        ));
        Ok(())
    }
    async fn private(ctx: PrivateContext<Arc<Mutex<Vec<String>>>>) -> Result<()> {
        ctx.state()
            .lock()
            .expect("state")
            .push(format!("private:{}", ctx.args_or_empty()));
        Ok(())
    }
    let bot = Bot::with_state(DingTalk::new()?, Arc::clone(&seen))
        .on_group_text_commands(["/ping", "ping"], group)
        .on_private_text_command("/ping", private)
        .on_group_text_command("/pinged", group);
    bot.validate()?;
    for (scope, command) in [
        (Scope::Group, "@bot ping value"),
        (Scope::Private, "/ping private"),
        (Scope::Group, "/pinged"),
    ] {
        assert_eq!(
            bot.handle_event(BotEvent::text(scope, command)).await?,
            HandleOutcome::Matched
        );
    }
    assert_eq!(
        *seen.lock().expect("state"),
        ["group:ping:value", "private:private", "group:/pinged:"]
    );
    Ok(())
}

#[tokio::test]
async fn scoped_fallbacks_accumulate_and_normal_routes_win() -> Result<()> {
    let seen = Arc::new(Mutex::new(Vec::<&'static str>::new()));
    let record = |name| {
        move |ctx: Context<Arc<Mutex<Vec<&'static str>>>>| async move {
            ctx.state().lock().expect("state").push(name);
            Ok::<_, Error>(())
        }
    };
    let bot = Bot::with_state(DingTalk::new()?, Arc::clone(&seen))
        .on_unmatched_text(Scope::Group, record("group"))
        .on_unmatched_text(Scope::Private, record("private"))
        .fallback(record("other"))
        .on_text_command(Scope::Any, "/ping", record("command"));
    bot.validate()?;
    for (scope, text, outcome) in [
        (Scope::Group, "/ping", HandleOutcome::Matched),
        (Scope::Group, "hello", HandleOutcome::Fallback),
        (Scope::Private, "hello", HandleOutcome::Fallback),
        (
            Scope::Unknown("future".into()),
            "hello",
            HandleOutcome::Fallback,
        ),
    ] {
        assert_eq!(
            bot.handle_event(BotEvent::text(scope, text)).await?,
            outcome
        );
    }
    assert_eq!(
        *seen.lock().expect("state"),
        ["command", "group", "private", "other"]
    );
    Ok(())
}

#[test]
fn shadowed_routes_and_fallbacks_fail_instead_of_silently_disappearing() -> Result<()> {
    let client = DingTalk::new()?;
    let normal = Bot::new(client.clone())
        .on_message(Scope::Any, Msg::Text, |_| async {})
        .on_group_text_command("/ping", |_| async {});
    assert!(
        normal
            .validate()
            .expect_err("shadowed command")
            .to_string()
            .contains("unreachable")
    );
    let fallback = Bot::new(client.clone())
        .fallback(|_| async {})
        .on_unmatched_text(Scope::Group, |_| async {});
    assert!(
        fallback
            .validate()
            .expect_err("shadowed fallback")
            .to_string()
            .contains("fallbacks[1]")
    );
    let repeated = Bot::new(client.clone())
        .on_unmatched_text(Scope::Group, |_| async {})
        .on_unmatched_text(Scope::Group, |_| async {});
    assert!(repeated.validate().is_err());
    Bot::new(client)
        .on_group_text_command("/ping", |_| async {})
        .on_message(Scope::Any, Msg::Text, |_| async {})
        .on_unmatched_text(Scope::Group, |_| async {})
        .on_unmatched_text(Scope::Private, |_| async {})
        .fallback(|_| async {})
        .validate()?;
    #[cfg(feature = "stream")]
    assert!(
        StreamBot::builder()
            .client_id_and_secret("key", "secret")
            .fallback(|_| async {})
            .on_unmatched_text(Scope::Group, |_| async {})
            .build()
            .is_err()
    );
    Ok(())
}

#[cfg(feature = "macros")]
#[test]
fn macro_routes_use_the_same_conflict_validation() -> Result<()> {
    #[dingding::handler(commands = ["/ping", "ping"])]
    async fn ping(ctx: Context) -> Result<()> {
        ctx.reply_text("pong").await
    }
    let bot = Bot::new(DingTalk::new()?)
        .route(ping_route())
        .on_group_text_command("ping", |_| async {});
    assert!(bot.validate().is_err());
    Ok(())
}

#[derive(Default)]
struct AppState {
    calls: AtomicUsize,
    recoveries: AtomicUsize,
}

#[tokio::test]
async fn error_hook_keeps_context_and_state_and_deduplicates_recovered_outcomes() -> Result<()> {
    let state = Arc::new(AppState::default());
    let bot = Bot::with_state(DingTalk::new()?, Arc::clone(&state))
        .on_group_text_command("/run", |ctx| async move {
            ctx.state().calls.fetch_add(1, Ordering::SeqCst);
            Err::<(), _>(std::io::Error::other("external service unavailable"))
        })
        .on_error(|ctx, error| async move {
            assert_eq!(error.kind(), ErrorKind::Handler);
            assert!(
                error
                    .source()
                    .and_then(|e| e.downcast_ref::<std::io::Error>())
                    .is_some()
            );
            assert_eq!(ctx.command(), Some("/run"));
            assert_eq!(ctx.args(), Some("job"));
            assert_eq!(ctx.message_id(), Some("recoverable"));
            assert_eq!(ctx.scope(), &Scope::Group);
            tokio::task::yield_now().await;
            ctx.state().recoveries.fetch_add(1, Ordering::SeqCst);
        });
    let event = BotEvent::from_value(serde_json::json!({
        "conversationType": "2", "conversationId": "group", "msgId": "recoverable",
        "msgtype": "text", "text": {"content": "/run job"}
    }));
    for _ in 0..2 {
        assert_eq!(
            bot.handle_event(event.clone()).await?,
            HandleOutcome::Matched
        );
    }
    assert_eq!(state.calls.load(Ordering::SeqCst), 1);
    assert_eq!(state.recoveries.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn error_hook_failure_is_not_recursive_or_cached_as_success() -> Result<()> {
    for propagate_original in [false, true] {
        let state = Arc::new(AppState::default());
        let bot = Bot::with_state(DingTalk::new()?, Arc::clone(&state))
            .on_group_text_command("/run", |ctx| async move {
                ctx.state().calls.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(dingding::Error::InvalidConfig("original failure".into()))
            })
            .on_error(move |ctx, error| async move {
                ctx.state().recoveries.fetch_add(1, Ordering::SeqCst);
                if propagate_original {
                    Err(error)
                } else {
                    ctx.reply_text("Please try again later.").await
                }
            });
        let event = BotEvent::from_value(serde_json::json!({
            "conversationType": "2", "msgId": "failed", "msgtype": "text",
            "text": {"content": "/run"}
        }));
        for _ in 0..2 {
            let error = bot
                .handle_event(event.clone())
                .await
                .expect_err("failed recovery");
            assert_eq!(error.kind(), ErrorKind::InvalidConfig);
            assert!(error.to_string().contains(if propagate_original {
                "original failure"
            } else {
                "sessionWebhook"
            }));
        }
        assert_eq!(state.calls.load(Ordering::SeqCst), 2);
        assert_eq!(state.recoveries.load(Ordering::SeqCst), 2);
    }
    Ok(())
}

#[tokio::test]
async fn error_hook_handles_fallbacks_but_not_successful_or_ignored_events() -> Result<()> {
    let state = Arc::new(AppState::default());
    let bot = Bot::with_state(DingTalk::new()?, Arc::clone(&state))
        .on_group_text_command("/ok", |_| async {})
        .on_unmatched_text(Scope::Group, |_| async {
            Err::<(), _>(std::io::Error::other("fallback"))
        })
        .on_error(|ctx, _| async move {
            assert_eq!(ctx.command(), None);
            ctx.state().recoveries.fetch_add(1, Ordering::SeqCst);
        });
    assert_eq!(
        bot.handle_event(BotEvent::text(Scope::Group, "hello"))
            .await?,
        HandleOutcome::Fallback
    );
    assert_eq!(
        bot.handle_event(BotEvent::text(Scope::Group, "/ok"))
            .await?,
        HandleOutcome::Matched
    );
    assert_eq!(
        bot.handle_event(BotEvent::text(Scope::Private, "hello"))
            .await?,
        HandleOutcome::Ignored
    );
    assert_eq!(state.recoveries.load(Ordering::SeqCst), 1);
    Ok(())
}

#[cfg(feature = "macros")]
#[tokio::test]
async fn macro_routes_accept_stateful_context_aliases_without_clone_bounds() -> Result<()> {
    type Send = ();
    type Sync = ();
    let _: (Send, Sync) = ((), ());
    type AppContext = GroupContext<AppState>;
    #[dingding::handler(scope = Scope::Group, command = "/count")]
    async fn count(ctx: AppContext) {
        let clone = ctx.clone();
        assert!(Arc::ptr_eq(&ctx.shared_state(), &clone.shared_state()));
        ctx.state().calls.fetch_add(1, Ordering::SeqCst);
    }
    #[dingding::handler(command = "/noop")]
    async fn noop() {}
    let route = count_route();
    let bot = Bot::with_state(DingTalk::new()?, AppState::default())
        .route(route.clone())
        .route(noop_route());
    bot.clone()
        .handle_event(BotEvent::text(Scope::Group, "/count"))
        .await?;
    bot.handle_event(BotEvent::text(Scope::Private, "/noop"))
        .await?;
    Ok(())
}
