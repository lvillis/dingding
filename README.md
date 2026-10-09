# dingding

Rust SDK and bot framework for DingTalk. HTTP is powered by `reqx`.

## Usage

```toml
[dependencies]
dingding = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "signal"] }
```

The default features include webhook messages, OpenAPI, bot routing, Stream, and handler macros.
For a custom feature set, keep the Tokio dependency above and replace the `dingding` entry:

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
    StreamBot::from_env()?
        .route(ping_route())
        .run_until(async { let _ = tokio::signal::ctrl_c().await; })
        .await
}
```

Set `DINGTALK_CLIENT_ID` and `DINGTALK_CLIENT_SECRET` before starting the bot. The matching
`DINGTALK_APP_KEY` / `DINGTALK_APP_SECRET` pair is also supported.

Handlers that combine SDK calls with file, database, or other application operations can
return `HandlerResult` and use `?`. Concrete results such as `std::io::Result<()>` are also
accepted by all asynchronous handler-registration methods and `#[handler]`. Handlers that cannot fail return
`()`, without a result wrapper. `on_frame` and `on_card_callback` also accept
`StreamFrameResponse`, directly or inside a result, through the same method.

The former `*_fallible` and `*_with_response` callback methods have been removed. Use their
ordinary names. Replace untyped `async { Ok(()) }` with `async {}` for success-only callbacks,
or specify a result error type when the closure uses `?`.

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
- In-memory media helpers, streaming file uploads, and size-limited downloads to async writers.
- Interactive cards and Stream callbacks.
- Bot routing with optional macros.
- Group/private message send-status and read-status queries, including lazy group pagination.
- Explicit proactive replies and serializable destinations for background jobs.
- Group/private message recall with per-message success and failure results.
- Bounded Stream processing, handler timeouts, event deduplication, and graceful shutdown.

## Runtime Behavior

Stream handlers run in arrival order by default. `StreamProcessingPolicy` enables bounded
concurrency and buffering while connection reads, heartbeat handling, and ACK writes continue.
`run_until` stops accepting work and waits for accepted handlers and ACKs up to the configured
shutdown deadline. A deadline failure is returned as an error.

OpenAPI calls refresh explicitly rejected access tokens and replay the request at most once.
Permission errors and ambiguous delivery failures do not trigger token recovery.

API errors retain the HTTP status, DingTalk business code, request id, and `Retry-After` hint
when supplied. `is_retryable()` identifies potentially transient failures; it does not mean
replaying a message send is safe after an ambiguous timeout.

Stream builder credentials apply to both the connection and handler OpenAPI calls, including
when a custom SDK client is supplied. Other client clones retain their credentials and share
the existing transport and credential-keyed token caches.

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
