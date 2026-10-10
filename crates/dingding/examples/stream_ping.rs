use dingding::prelude::*;
use tracing_subscriber::EnvFilter;

#[handler(commands = ["/ping", "ping"])]
async fn ping(ctx: Context) -> Result<()> {
    ctx.reply_text("pong").await
}

#[handler(command = "/echo")]
async fn echo(ctx: Context) -> Result<()> {
    let text = ctx.args_or_empty();
    ctx.reply_text(if text.is_empty() {
        "Usage: /echo <text>"
    } else {
        text
    })
    .await
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("dingding=info")),
        )
        .init();

    StreamBot::from_env()?
        .route(ping_route())
        .route(echo_route())
        .on_unmatched_text(Scope::Any, |ctx| async move {
            ctx.reply_text("Try /ping or /echo <text>.").await
        })
        .run_until(async {
            if let Err(error) = tokio::signal::ctrl_c().await {
                tracing::error!(%error, "failed to listen for Ctrl-C");
            }
        })
        .await
}
