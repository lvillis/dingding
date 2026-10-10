use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;
use crate::{ErrorKind, HandlerResult, handler_future};

fn client() -> DingTalk {
    DingTalk::builder()
        .app_key_and_secret("original-id", "original-secret")
        .build()
        .expect("client")
}

#[tokio::test]
async fn high_level_builder_preserves_all_fallbacks_and_their_state() {
    let bot = StreamBot::from_client(client())
        .state(AtomicUsize::new(0))
        .on_unmatched_text(ConversationScope::Group, |ctx| async move {
            ctx.state_required::<AtomicUsize>()
                .expect("shared state")
                .fetch_add(1, Ordering::SeqCst);
        })
        .on_unmatched_text(ConversationScope::Private, |ctx| async move {
            ctx.state_required::<AtomicUsize>()
                .expect("shared state")
                .fetch_add(10, Ordering::SeqCst);
        })
        .fallback(|ctx| async move {
            ctx.state_required::<AtomicUsize>()
                .expect("shared state")
                .fetch_add(100, Ordering::SeqCst);
        })
        .build()
        .expect("fallback-only bot");
    for scope in [
        ConversationScope::Group,
        ConversationScope::Private,
        ConversationScope::Unknown("future".into()),
    ] {
        let router = bot.client.bot.as_ref().expect("router");
        assert_eq!(
            router
                .handle_event(BotEvent::text(scope, "hello"))
                .await
                .expect("fallback"),
            HandleOutcome::Fallback
        );
    }
    assert_eq!(
        bot.client
            .handler_context()
            .state_required::<AtomicUsize>()
            .expect("state")
            .load(Ordering::SeqCst),
        111
    );
}

fn card_frame() -> String {
    serde_json::json!({
        "type": "CALLBACK",
        "headers": {"topic": CARD_CALLBACK_TOPIC, "messageId": "card-id"},
        "data": {"cardBizId": "card-1"},
    })
    .to_string()
}

fn assert_context(ctx: &StreamContext) {
    assert_eq!(
        ctx.client()
            .openapi()
            .credentials()
            .map(AppCredentials::app_key),
        Some("stream-id")
    );
    assert!(ctx.state::<String>().is_none());
    ctx.state_required::<AtomicUsize>()
        .expect("state")
        .fetch_add(1, Ordering::SeqCst);
}

async fn frame_handler(ctx: StreamContext, _: StreamFrame) {
    assert_context(&ctx);
}

async fn card_handler(ctx: StreamContext, _: CardCallbackEvent) -> Result<CardCallbackResponse> {
    assert_context(&ctx);
    CardCallbackResponse::new().card_data([("status", "done")])
}

#[tokio::test]
async fn high_level_context_shares_state_with_routes_and_works_without_routes() {
    let original = client();
    for with_routes in [false, true] {
        let mut builder = StreamBot::from_client(original.clone())
            .on_frame(frame_handler)
            .on_card_callback(card_handler)
            .state(AtomicUsize::new(0))
            .client_id_and_secret("stream-id", "stream-secret");
        if with_routes {
            builder = builder.on_group_text_command("/count", |ctx| {
                handler_future(async move {
                    ctx.state_required::<AtomicUsize>()?
                        .fetch_add(1, Ordering::SeqCst);
                    assert_eq!(
                        ctx.client()
                            .openapi()
                            .credentials()
                            .map(AppCredentials::app_key),
                        Some("stream-id")
                    );
                    Ok(())
                })
            });
        }
        let stream = builder.build().expect("bot").client;
        if let Some(bot) = &stream.bot {
            bot.handle_event(BotEvent::text(ConversationScope::Group, "/count"))
                .await
                .expect("route");
        }
        let card = stream.handle_text_frame(&card_frame()).await.expect("card");
        assert!(card.error.is_none());
        let clone = stream.clone();
        let frame = clone
            .handle_text_frame(
                r#"{"type":"EVENT","headers":{"topic":"custom","messageId":"frame-id"},"data":{}}"#,
            )
            .await
            .expect("frame");
        assert!(frame.error.is_none());
        let context = stream.handler_context();
        assert_eq!(
            context
                .state_required::<AtomicUsize>()
                .expect("state")
                .load(Ordering::SeqCst),
            if with_routes { 3 } else { 2 }
        );
    }
    assert_eq!(
        original
            .openapi()
            .credentials()
            .map(AppCredentials::app_key),
        Some("original-id")
    );
}

#[tokio::test]
async fn low_level_context_inherits_router_state_or_uses_explicit_shared_state() {
    for order in 0..3 {
        let original = client();
        let bot = Bot::new(original.clone())
            .state(AtomicUsize::new(10))
            .on_group_text_command("/count", |ctx| {
                handler_future(async move {
                    ctx.state_required::<AtomicUsize>()?
                        .fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            });
        let original_bot = bot.clone();
        let builder = StreamClient::builder(original)
            .expect("builder")
            .on_frame(frame_handler)
            .on_card_callback(card_handler)
            .client_id_and_secret("stream-id", "stream-secret");
        let stream = match order {
            0 => builder.bot(bot),
            1 => builder.state(AtomicUsize::new(20)).bot(bot),
            _ => builder.bot(bot).state(AtomicUsize::new(20)),
        }
        .build()
        .expect("stream");
        stream
            .bot
            .as_ref()
            .expect("router")
            .handle_event(BotEvent::text(ConversationScope::Group, "/count"))
            .await
            .expect("route");
        assert!(
            stream
                .handle_text_frame(&card_frame())
                .await
                .expect("card")
                .error
                .is_none()
        );
        let context = stream.handler_context();
        assert_eq!(
            context
                .state_required::<AtomicUsize>()
                .expect("state")
                .load(Ordering::SeqCst),
            if order == 0 { 12 } else { 22 }
        );
        let old = original_bot.shared_state().expect("original state");
        assert_eq!(
            old.downcast_ref::<AtomicUsize>()
                .expect("counter")
                .load(Ordering::SeqCst),
            if order == 0 { 12 } else { 10 }
        );
    }
}

#[tokio::test]
async fn low_level_card_only_context_and_missing_state_are_supported() {
    let stream = StreamClient::builder(client())
        .expect("builder")
        .state(AtomicUsize::new(0))
        .client_id_and_secret("stream-id", "stream-secret")
        .on_card_callback(card_handler)
        .build()
        .expect("stream");
    assert!(stream.bot.is_none());
    assert!(
        stream
            .handle_text_frame(&card_frame())
            .await
            .expect("card")
            .error
            .is_none()
    );
    let stream = StreamClient::builder(client())
        .expect("builder")
        .on_card_callback(|ctx, _| {
            handler_future(async move {
                ctx.state_required::<AtomicUsize>()?;
                Ok(())
            })
        })
        .build()
        .expect("stream");
    assert!(stream.handler_context().state::<AtomicUsize>().is_none());
    let handled = stream
        .handle_text_frame(&card_frame())
        .await
        .expect("handled");
    assert_eq!(
        handled.error.expect("missing state").error.kind(),
        ErrorKind::InvalidConfig
    );
    assert_eq!(serde_json::to_value(handled.ack).expect("ack")["code"], 500);
}

#[tokio::test]
async fn typed_card_responses_work_directly_or_inside_results_on_both_builders() {
    fn response() -> CardCallbackResponse {
        CardCallbackResponse::new()
            .card_data([("status", "done")])
            .expect("response")
    }
    let low = || StreamClient::builder(client()).expect("builder");
    let high = || StreamBot::from_client(client());
    let streams = [
        low()
            .on_card_callback(|_, _| async { response() })
            .build()
            .expect("stream"),
        high()
            .on_card_callback(|_, _| async { response() })
            .build()
            .expect("bot")
            .client,
        low()
            .on_card_callback(|_, _| async {
                CardCallbackResponse::new().card_data([("status", "done")])
            })
            .build()
            .expect("stream"),
        high()
            .on_card_callback(|_, _| async {
                CardCallbackResponse::new().card_data([("status", "done")])
            })
            .build()
            .expect("bot")
            .client,
        low()
            .on_card_callback(|_, _| {
                handler_future(async {
                    let status = String::from_utf8(b"done".to_vec())?;
                    Ok(CardCallbackResponse::new().card_data([("status", status)])?)
                })
            })
            .build()
            .expect("stream"),
        high()
            .on_card_callback(|_, _| handler_future(async { Ok(response()) }))
            .build()
            .expect("bot")
            .client,
    ];
    for stream in streams {
        let result = stream.card_callback_handler.as_ref().expect("handler")(
            stream.handler_context(),
            CardCallbackEvent::from_value(serde_json::json!({})),
        )
        .await
        .expect("response");
        assert_eq!(
            result.as_value()["cardData"]["cardParamMap"]["status"],
            "done"
        );
    }
}

#[tokio::test]
async fn invalid_direct_card_responses_and_application_errors_produce_failure_acks() {
    async fn fail(_: StreamContext, _: CardCallbackEvent) -> HandlerResult<CardCallbackResponse> {
        Err(std::io::Error::other("application failure"))?;
        Ok(CardCallbackResponse::new())
    }
    let low = || StreamClient::builder(client()).expect("builder");
    let high = || StreamBot::from_client(client());
    for (stream, kind) in [
        (
            low()
                .on_card_callback(|_, _| async { CardCallbackResponse::new() })
                .build()
                .expect("stream"),
            ErrorKind::InvalidInput,
        ),
        (
            high()
                .on_card_callback(|_, _| async { CardCallbackResponse::new() })
                .build()
                .expect("bot")
                .client,
            ErrorKind::InvalidInput,
        ),
        (
            low().on_card_callback(fail).build().expect("stream"),
            ErrorKind::Handler,
        ),
        (
            high().on_card_callback(fail).build().expect("bot").client,
            ErrorKind::Handler,
        ),
    ] {
        let handled = stream
            .handle_text_frame(&card_frame())
            .await
            .expect("handled");
        assert_eq!(handled.error.expect("callback error").error.kind(), kind);
        assert_eq!(serde_json::to_value(handled.ack).expect("ack")["code"], 500);
    }
}
