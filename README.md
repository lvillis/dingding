# dingding

Rust SDK and bot framework for DingTalk. HTTP is powered by `reqx`.

## Usage

```toml
dingding = "0.1"
```

```toml
dingding = { version = "0.1", default-features = false, features = [
  "async-tls-rustls-ring", "webhook", "openapi", "bot", "stream", "macros",
] }
```

## Stream Bot

```rust
use dingding::prelude::*;

#[handler(scope = Scope::Any, msg = Msg::Text, command = "/ping")]
async fn ping(ctx: Context) -> Result<()> {
    ctx.reply_text("pong").await
}

#[tokio::main]
async fn main() -> Result<()> {
    StreamBot::from_env()?.route(ping_route()).run().await
}
```

## Webhook Robot

```rust
use dingding::{DingTalk, Result};

#[tokio::main]
async fn main() -> Result<()> {
    DingTalk::new()?
        .webhook("access-token")?
        .signing_secret("SEC...")?
        .send_markdown("deploy", "**done**")
        .await?;
    Ok(())
}
```

## Capabilities

- Custom webhook robot messages: text, markdown, link, action card, feed card.
- Enterprise robot messages: text, markdown, link, image, action card, audio, file, video, custom templates.
- Media upload/download.
- Interactive cards and Stream callbacks.
- Bot routing with optional macros.
- Group/private message send-status and read-status queries, including group pagination.
- Group/private message recall with per-message success and failure results.
- Bounded Stream processing, handler timeouts, event deduplication, and graceful shutdown.

## Runtime Behavior

Stream handlers run in arrival order by default. `StreamProcessingPolicy` enables bounded
concurrency and buffering while connection reads, heartbeat handling, and ACK writes continue.
`run_until` stops accepting work and waits for accepted handlers and ACKs up to the configured
shutdown deadline. A deadline failure is returned as an error.

OpenAPI calls refresh explicitly rejected access tokens and replay the request at most once.
Permission errors and ambiguous delivery failures do not trigger token recovery.

Successful event results are deduplicated in memory and replayed on redelivery. Applications
with multiple replicas can provide an asynchronous `EventDeduplicator` backend. Business
operations still need durable idempotency across crashes and cache expiry.

See [runtime and integration guidance](docs/runtime.md) for limits, failure behavior, message
lifecycle examples, and validation coverage.

## Examples

```text
DINGTALK_CLIENT_ID=... DINGTALK_CLIENT_SECRET=... cargo run -p dingding --example stream_ping
DINGTALK_WEBHOOK_ACCESS_TOKEN=... cargo run -p dingding --example webhook_robot
cargo run -p dingding --example robot_message
cargo run -p dingding --example robot_media
cargo run -p dingding --example interactive_card
cargo run -p dingding --example stream_card_callback
```

## CI

```text
just ci
```
