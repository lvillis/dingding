//! Handles enterprise interactive card callbacks through DingTalk Stream.
//!
//! Required environment:
//! - `DINGTALK_CLIENT_ID`
//! - `DINGTALK_CLIENT_SECRET`
//!
//! Send a card with `interactive_card.rs`, then click a card action while this
//! example is running. The card callback subscription is registered automatically.

use dingding::prelude::*;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Default, serde::Deserialize)]
struct CardForm {
    #[serde(default)]
    env: Option<String>,
    #[serde(default)]
    reason: Option<String>,
}

#[handler(scope = Scope::Any, msg = Msg::Text, command = "/ping")]
async fn ping(ctx: Context) -> Result<()> {
    ctx.reply_text("pong; card callback listener is running")
        .await
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    info!("stream card callback listener is running");

    StreamBot::from_env()?
        .route(ping_route())
        .on_card_callback(handle_card_callback)
        .on_event(log_stream_event)
        .run_until(shutdown_signal())
        .await
}

async fn handle_card_callback(event: CardCallbackEvent) -> Result<StreamFrameResponse> {
    let payload = event.payload();
    let action = payload.action().unwrap_or("unknown");
    let card_biz_id = payload.card_biz_id().unwrap_or("unknown");
    let user_id = payload.operator().user_id().unwrap_or("unknown");
    let form = payload
        .action_value()
        .deserialize::<CardForm>()?
        .unwrap_or_default();
    let env = form.env.as_deref().unwrap_or("default");

    info!(
        action,
        card_biz_id,
        user_id,
        env,
        reason = ?form.reason,
        "card callback received"
    );

    let response = match action {
        "approve" | "confirm" => CardCallbackResponse::new()
            .card_data([
                ("status", format!("approved by {user_id}")),
                ("lastAction", action.to_string()),
                ("env", env.to_string()),
            ])?
            .user_private_data([("notice", format!("you approved {card_biz_id}"))])?,
        "reject" | "cancel" => {
            let status = form
                .reason
                .as_deref()
                .map(|reason| format!("rejected by {user_id}: {reason}"))
                .unwrap_or_else(|| format!("rejected by {user_id}"));

            CardCallbackResponse::new()
                .card_data([
                    ("status", status),
                    ("lastAction", action.to_string()),
                    ("env", env.to_string()),
                ])?
                .user_private_data([("notice", format!("you rejected {card_biz_id}"))])?
        }
        _ => CardCallbackResponse::new().user_private_data([(
            "notice",
            format!("received action `{action}` for {card_biz_id}"),
        )])?,
    };

    response.into_stream_response()
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
}

fn log_stream_event(event: StreamRunEvent) {
    match event {
        StreamRunEvent::ConnectionOpening { attempt } => {
            info!(attempt, "stream connection opening");
        }
        StreamRunEvent::ConnectionOpened { attempt } => {
            info!(attempt, "stream connection opened");
        }
        StreamRunEvent::ConnectionClosed { attempt, exit } => {
            info!(attempt, %exit, "stream connection closed");
        }
        StreamRunEvent::ConnectionError {
            attempt,
            retrying,
            error,
        } => {
            warn!(attempt, retrying, %error, "stream connection error");
        }
        StreamRunEvent::ReconnectScheduled {
            next_attempt,
            delay,
        } => {
            info!(next_attempt, ?delay, "stream reconnect scheduled");
        }
        StreamRunEvent::FrameError { message_id, error } => {
            warn!(?message_id, %error, "stream frame error");
        }
        StreamRunEvent::BotEventHandled {
            message_id,
            outcome,
            conversation_scope,
            message_type,
        } => {
            info!(
                %message_id,
                %outcome,
                scope = %conversation_scope,
                msg = %message_type,
                "bot event handled"
            );
        }
        StreamRunEvent::CardCallbackHandled {
            message_id,
            card_biz_id,
            action,
        } => {
            info!(%message_id, ?card_biz_id, ?action, "card callback handled");
        }
        StreamRunEvent::Shutdown => {
            info!("stream shutdown requested");
        }
        other => {
            info!(?other, "stream event");
        }
    }
}

async fn shutdown_signal() {
    match tokio::signal::ctrl_c().await {
        Ok(()) => info!("ctrl-c received, shutting down"),
        Err(error) => error!(%error, "failed to listen for ctrl-c"),
    }
}
