use dingding::prelude::*;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[handler(scope = Scope::Any, msg = Msg::Text, commands = ["/ping", "ping"])]
async fn ping(ctx: Context) -> Result<()> {
    let sender = ctx.sender_name().unwrap_or("there");
    let reply = match ctx.scope() {
        Scope::Group => format!("pong from group, {sender}"),
        Scope::Private => "pong from private chat".to_string(),
        Scope::Any | Scope::Unknown(_) => "pong".to_string(),
    };
    ctx.reply_text(reply).await
}

#[handler(scope = Scope::Any, msg = Msg::Text)]
async fn unknown(ctx: Context) -> Result<()> {
    if let Some(text) = ctx.text()
        && text.trim_start().starts_with('/')
    {
        ctx.reply_markdown("Unknown command", "Unknown command. Try `/ping`.")
            .await?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    info!("stream bot is running. Send `/ping` to the bot; in a group, @ the bot first.");

    StreamBot::from_env()?
        .route(ping_route())
        .fallback_route(unknown_route())
        .on_event(log_stream_event)
        .run_until(shutdown_signal())
        .await
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
        StreamRunEvent::ConnectionOpening { attempt, .. } => {
            info!(attempt, "stream connection opening");
        }
        StreamRunEvent::ConnectionOpened { attempt, .. } => {
            info!(attempt, "stream connection opened");
        }
        StreamRunEvent::ConnectionClosed { attempt, exit, .. } => {
            info!(attempt, %exit, "stream connection closed");
        }
        StreamRunEvent::ConnectionError {
            attempt,
            retrying,
            error,
            ..
        } => {
            warn!(attempt, retrying, %error, "stream connection error");
        }
        StreamRunEvent::ReconnectScheduled {
            next_attempt,
            delay,
            ..
        } => {
            info!(next_attempt, ?delay, "stream reconnect scheduled");
        }
        StreamRunEvent::FrameError {
            message_id, error, ..
        } => {
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
