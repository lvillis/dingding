# Runtime and Integration

## Dependencies and Features

The library uses async HTTP. Applications using `#[tokio::main]` need their own Tokio
dependency; `dingding` does not expose transitive crates to application code.

```toml
[dependencies]
dingding = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "signal"] }
```

Default features enable webhook messages, OpenAPI, bot routing, Stream, macros, and the
Rustls ring backend. A webhook-only application can use:

```toml
dingding = { version = "0.1", default-features = false, features = [
  "async-tls-rustls-ring", "webhook",
] }
```

Enable exactly one async TLS backend. Changing to `async-tls-native` or
`async-tls-rustls-aws-lc-rs` requires disabling default features and selecting the desired
capabilities. The TLS features are mutually exclusive, so `--all-features` is not supported.
The minimum Rust version is declared in the package's `rust-version` field.

## Credentials and Client Reuse

`AppCredentials::from_env()` and `StreamBot::from_env()` read the complete
`DINGTALK_CLIENT_ID` / `DINGTALK_CLIENT_SECRET` pair, falling back to the complete
`DINGTALK_APP_KEY` / `DINGTALK_APP_SECRET` pair. An incomplete primary pair is an error;
values from the two pairs are never combined.

Credentials explicitly set on a Stream builder take precedence over its client's defaults.
They apply to the Stream connection, its internal SDK client, and `ctx.client().openapi()`.
This also applies to a separately supplied `Bot` router. Each client retains its own base
URLs, timeouts, and proxy configuration; changing Stream credentials does not rebuild the
HTTP transport or change other SDK client clones.

```rust
use std::time::Duration;
use dingding::prelude::*;

#[handler(command = "/ping")]
async fn ping(ctx: Context) -> Result<()> {
    ctx.reply_text("pong").await
}

#[tokio::main]
async fn main() -> Result<()> {
    let client = DingTalk::builder()
        .connect_timeout(Duration::from_secs(5))
        .build()?;
    StreamBot::from_client(client)
        .credentials(AppCredentials::from_env()?)
        .route(ping_route())
        .run_until(async { let _ = tokio::signal::ctrl_c().await; })
        .await
}
```

For OpenAPI calls outside a Stream handler, configure the original client with
`app_credentials_from_env()` or obtain a credential-specific clone with
`client.clone().with_app_credentials(credentials)?`. Token caches and refresh locks are
shared and keyed by the full credential pair.

## Application Errors

SDK methods return `dingding::Result<T>`. A handler that also performs application work can
return `HandlerResult<T>`, which boxes `Send + Sync` errors and supports `?` across SDK and
application operations.

```rust
use dingding::prelude::*;

#[handler(command = "/message")]
async fn message(ctx: Context) -> HandlerResult {
    let text = String::from_utf8(vec![104, 101, 108, 108, 111])?;
    ctx.reply_text(text).await?;
    Ok(())
}
```

The macro and every asynchronous handler-registration method accept the same return types. Use `()` when no
operation can fail, an SDK `Result<()>`, `HandlerResult`, or a concrete application result
such as `std::io::Result<()>`. This applies to `Route::handle`, `handle_group`, `handle_private`,
all shortcuts and fallback handlers on `Bot` and `StreamBotBuilder`.

Both Stream builders expose just `on_frame` and `on_card_callback` for asynchronous
callbacks. Return `()` for an empty ACK, or `StreamFrameResponse` for a payload. Either can
be wrapped in a result. Successful values are normalized by `IntoHandlerResult`; errors
produce failure ACKs and never masquerade as a successful empty response.

```rust
use dingding::prelude::*;

pub fn configure(client: DingTalk) -> StreamBotBuilder {
    StreamBot::from_client(client)
        .on_group_text_command("/ping", |ctx, _| async move {
            ctx.reply_text("pong").await
        })
        .fallback(|_, _| async {})
        .on_card_callback(|_| async { StreamFrameResponse::empty() })
}
```

This is a breaking API change: remove `_fallible` and `_with_response` from registration
method names; no compatibility aliases remain for those methods. A closure returning only
`Ok(())` no longer has a fixed SDK error type to infer. Prefer `async {}` for a handler that
cannot fail, use a named function with an explicit result type, or write
`Ok::<_, dingding::Error>(())` / `Ok::<_, dingding::BoxError>(())` when using `?` inside a
closure. An error-only closure can use `Err::<(), _>(error)`.

Existing SDK errors retain their category and metadata. Other application errors become
`ErrorKind::Handler`; `std::error::Error::source()` retains the original error for downcasting.
`Error::handler(error)` provides the same conversion for explicit adapters. Display and
Debug output redact recognized secret fields. The original source is unredacted, so logging
an entire source chain needs application-specific care.

Handler futures and errors must support `Send`, and captured state must also support `Sync`.
Use async file/database operations in live handlers. Synchronous IO blocks connection work
on the same executor thread; move blocking work to `tokio::task::spawn_blocking` when needed.

## Errors and Retries

Use `kind()`, `errcode()`, `api_code()`, `status()`, and `request_id()` for structured
diagnostics. `errcode()` is the DingTalk business code; for responses without a business
code it retains the SDK's numeric fallback. `status()` separately reports the actual HTTP
status, including HTTP 200 responses carrying a business error.

`retry_after()` retains a valid `Retry-After` delay. Delta seconds and HTTP dates are
supported; expired dates yield a zero delay and malformed values are ignored. For API errors,
the delay is evaluated when the response is parsed. Response header request ids take
precedence over body request ids. Error body snippets are redacted and bounded and can be
disabled with `DingTalkBuilder::error_body_snippet`.

`is_retryable()` means the failure may be transient. It does not establish whether replay is
safe: a timeout can happen after DingTalk accepted a send operation. Transport retries of
non-idempotent requests are disabled by default. Enabling
`retry_non_idempotent_requests(true)` requires application-level duplicate handling.
`RetryPolicy::disabled()` disables transport retries when the application owns retry policy.

OpenAPI automatically invalidates an explicitly rejected access token, refreshes it, and
replays the same request at most once. Permission failures and ambiguous delivery failures
do not trigger token recovery.

When matching `Error::Api`, use `..` to ignore metadata fields you do not need. The variant
now includes `status` and `retry_after`; direct construction or exhaustive field destructuring
must account for these fields.

## Stream Processing and Shutdown

The defaults are one concurrent handler, 64 waiting business frames, a 30-second handler
timeout, a 5-second write timeout, and a 30-second shutdown deadline. Use
`StreamProcessingPolicy` to change these bounds. Multiple concurrent handlers can complete
out of arrival order.

Heartbeat handling and ACK writes continue while async business handlers run. A full queue,
handler error, panic, or timeout produces a failure ACK rather than successful completion.
The handler timeout also covers deduplication storage operations.

`run_until` stops accepting new business work when the shutdown future resolves. Accepted
handlers and ACKs drain within the configured deadline; remaining work is cancelled and a
deadline failure is returned as an error. `run()` continues until a terminal error or external
cancellation. Reconnection is enabled by default with backoff from one to thirty seconds.
`ReconnectPolicy::no_retry()` returns connection errors to the application.

Successful results are retained in memory by default for five minutes, with a capacity of
10,000 entries. Completed duplicates replay the saved result; in-flight duplicates do not
receive a successful completion. Failed or cancelled work releases its reservation. Active
reservations are not evicted to make room; a full cache fails closed.

Multiple replicas can share an asynchronous `EventDeduplicator` implementation. Give each
application its own storage namespace, use renewable leases and stale-owner fencing, and
make completion atomic with saved response storage. Business side effects still require
durable idempotency across process crashes and retention expiry.

## Replies and Message Lifecycle

`ctx.reply_text`, `reply_markdown`, and `reply_message` use the event's session webhook.
They reject a known expired webhook or a missing webhook before sending. Expiration can be
checked with `is_session_webhook_expired()` and `session_webhook_expires_at()`; absent expiry
metadata does not guarantee that the URL remains valid.

For delayed work, capture `ctx.robot_reply_target()?` (also available on `BotEvent`) and
persist the resulting `RobotReplyTarget` with the job. It supports Serde serialization and
contains only the group conversation id or private staff id, not the session URL or client
credentials. Treat persisted recipient ids as sensitive and store the application's identity
alongside the job so the worker chooses the correct `RobotApi`.

```rust
use dingding::prelude::*;

pub async fn complete_job(robot: &RobotApi, target: &RobotReplyTarget) -> Result<()> {
    let response = target.send_text(robot, "Job completed").await?;
    let _query_key = response.process_query_key();
    Ok(())
}
```

For an immediate proactive response, use `ctx.reply_via_robot(&robot, message).await`.
These APIs work independently of session webhook expiry and require proactive-message
permissions. They never guess a missing private staff id from `senderId`, and never switch
channels after an ambiguous failure. Existing `reply_*` methods remain webhook-only.
For work longer than the handler deadline, persist the job and complete it in a worker.

Sending returns a `process_query_key` that can be stored with the business operation. Use
`query_group_message_pages` for lazy pagination:

```rust
use dingding::prelude::*;

pub async fn inspect_readers(robot: &RobotApi, conversation_id: &str, key: &str) -> Result<()> {
    let mut pages = robot.query_group_message_pages(
        conversation_id, GroupMessageQuery::new(key)?.max_results(50)?,
    )?;
    while let Some(page) = pages.next_page().await? {
        // Process this page before requesting another; no pages are accumulated.
        let _readers = (page.read_user_ids, page.read_users);
    }
    Ok(())
}
```

The paginator preserves page size, accepts an initial cursor, and issues no requests after
completion. A request failure or cancelled future leaves the same page available for an
explicit retry. Repeated cursors terminate with an error; cursor history uses memory
proportional to page count. `query_group_message` remains available for manual pagination.
Private send/read status is available through `query_private_message`. Batch recall can
contain both successes and failures; inspect both collections even when HTTP succeeds.

## Large Media

`MediaUpload` and `download_message_file` retain their in-memory behavior for small files.
Use `MediaFileUpload` and `upload_media_file` to stream a local file without loading it into
memory. Optional `max_bytes` rejects oversized files before sending. Filename, MIME type,
file access, and size are validated locally. A rejected token triggers one fresh attempt
from a reopened file; keep the file unchanged until the call returns. Streaming HTTP bodies
are not replayed after ambiguous transport failures.

Use `download_message_file_to(code, &mut writer, max_bytes)` with a Tokio file or another
`AsyncWrite + Unpin + Send` destination. The positive limit applies even when the server
omits Content-Length. The result contains MIME type, byte count, and the temporary URL.
Local I/O errors preserve their source under `ErrorKind::Io`; transport-layer I/O failures
can still be `ErrorKind::Transport`.

Downloads request identity encoding and reject unexpected compressed responses. HTTP
errors and complete DingTalk error envelopes within the first 64 KiB are rejected before
writing. Larger successful responses are streamed without full JSON inspection. The client
total timeout covers the transfer and destination writes; the initial prefix write also
uses the configured request timeout, or 30 seconds when unspecified.

The SDK flushes but does not close the writer. Failure or cancellation may leave partial
output. Write to a unique temporary file, close it, and rename only after success; clean up
partial output on failure. Destination paths and overwrite policy remain application-owned.

## Validation

`just ci` checks formatting, strict Clippy, reduced feature combinations, documentation,
doctests, and the workspace test suite. Local HTTP and WebSocket mock tests cover token
recovery, error metadata, routing, bounded processing, ACKs, deduplication, and shutdown.
These tests exercise SDK behavior without live credentials; platform permissions and actual
delivery must be checked in the target DingTalk application.
