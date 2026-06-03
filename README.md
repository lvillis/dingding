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
        .await
}
```

## Capabilities

- Custom webhook robot messages: text, markdown, link, action card, feed card.
- Enterprise robot messages: text, markdown, link, image, action card, audio, file, video, custom templates.
- Media upload/download.
- Interactive cards and Stream callbacks.
- Bot routing with optional macros.

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
