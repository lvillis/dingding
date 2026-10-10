#![cfg(feature = "bot")]
#![allow(clippy::expect_used)]

use std::sync::{Arc, Mutex};

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
    async fn group(ctx: GroupContext) -> Result<()> {
        assert_eq!(ctx.event().conversation_scope, Scope::Group);
        ctx.state_required::<Arc<Mutex<Vec<String>>>>()?
            .lock()
            .expect("state")
            .push(format!(
                "group:{}:{}",
                ctx.command().expect("command"),
                ctx.args_or_empty()
            ));
        Ok(())
    }
    async fn private(ctx: PrivateContext) -> Result<()> {
        ctx.state_required::<Arc<Mutex<Vec<String>>>>()?
            .lock()
            .expect("state")
            .push(format!("private:{}", ctx.args_or_empty()));
        Ok(())
    }
    let bot = Bot::new(DingTalk::new()?)
        .state(Arc::clone(&seen))
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
        move |ctx: Context| async move {
            ctx.state_required::<Arc<Mutex<Vec<&'static str>>>>()?
                .lock()
                .expect("state")
                .push(name);
            Ok::<_, Error>(())
        }
    };
    let bot = Bot::new(DingTalk::new()?)
        .state(Arc::clone(&seen))
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
