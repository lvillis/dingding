#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(rustdoc::broken_intra_doc_links)]
#![cfg_attr(test, allow(clippy::expect_used, clippy::panic))]

//! DingTalk SDK and bot framework.
//!
//! Start with [`DingTalk`] for outbound API calls, or `stream::StreamBot` for an
//! application that receives messages and card callbacks. Reuse clients: clones
//! share connection pools, credential-keyed token caches, and refresh locks.
//!
//! # Dependencies and features
//!
//! Applications need their own Tokio dependency to use its runtime and signals:
//!
//! ```toml
//! [dependencies]
//! dingding = "0.1"
//! tokio = { version = "1", features = ["macros", "rt-multi-thread", "signal"] }
//! tracing-subscriber = "0.3"
//! ```
//!
//! Default features enable all capabilities and the Rustls ring TLS backend.
//!
//! | Feature | Capability | Also enables |
//! | --- | --- | --- |
//! | `webhook` | Custom robot and session webhook messages | |
//! | `openapi` | Enterprise robot messages, media, cards, message lifecycle | |
//! | `bot` | Callback parsing, routing, typed contexts, verification | `webhook` |
//! | `stream` | Stream connection, callbacks, processing and shutdown | `bot`, `openapi` |
//! | `macros` | Optional `#[handler]` route generation | `bot` |
//!
//! For webhook-only applications:
//!
//! ```toml
//! dingding = { version = "0.1", default-features = false, features = [
//!     "async-tls-rustls-ring", "webhook",
//! ] }
//! ```
//!
//! Select exactly one TLS feature: `async-tls-rustls-ring`,
//! `async-tls-rustls-aws-lc-rs`, or `async-tls-native`. Disable default features
//! when changing the backend. `--all-features` is not supported because these
//! backends are mutually exclusive. Enable at least one capability from the table.
//! The minimum Rust version is declared in the package's `rust-version` field.
//!
//! # Receive and reply over Stream
//!
//! Set `DINGTALK_CLIENT_ID` and `DINGTALK_CLIENT_SECRET`, or the complete fallback
//! pair `DINGTALK_APP_KEY` and `DINGTALK_APP_SECRET`. Incomplete primary credentials
//! are rejected, not combined with fallback values. See [`auth::AppCredentials::from_env`].
//!
//! ```no_run
//! # #[cfg(feature = "stream")]
//! use dingding::prelude::*;
//!
//! # #[cfg(feature = "stream")]
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     tracing_subscriber::fmt::init();
//!     StreamBot::from_env()?
//!         .on_text_command(Scope::Any, "/ping", |ctx| async move {
//!             ctx.reply_text("pong").await
//!         })
//!         .run_until(async { let _ = tokio::signal::ctrl_c().await; })
//!         .await
//! }
//! # #[cfg(not(feature = "stream"))]
//! # fn main() {}
//! ```
//!
//! Scope-specific shortcuts receive `GroupContext` or `PrivateContext`. Use
//! `on_text_command(Scope::Any, ...)` for a handler shared across conversation types.
//! All bot handlers take only the context; use `ctx.event()` for the full event.
//! Stream emits `tracing` diagnostics without requiring an `on_event` callback.
//! The application owns subscriber setup; the library never installs one globally.
//! See the `stream` module for callback state, concurrency, deduplication, and shutdown.
//!
//! # Send a webhook message
//!
//! ```no_run
//! # #[cfg(feature = "webhook")]
//! # async fn example() -> dingding::Result<()> {
//! use dingding::DingTalk;
//!
//! DingTalk::new()?
//!     .webhook("access-token")?
//!     .signing_secret("SEC...")?
//!     .send_markdown("deploy", "**done**")
//!     .await?;
//! # Ok(()) }
//! ```
//!
//! # Errors and retries
//!
//! SDK methods return [`Result`]. Handlers also accept `()`, [`HandlerResult`], or
//! concrete application results such as `std::io::Result<()>`. Use
//! [`handler_future`] inside inline closures that combine `?` with `Ok(...)` to
//! infer the error type. It does not allocate or spawn. An error-only closure may
//! additionally need a success annotation, for example `Err::<(), _>(error)`.
//!
//! SDK errors retain their kind and API metadata. Application errors become
//! [`ErrorKind::Handler`] and retain their original [`std::error::Error::source`].
//! Display and Debug output redact recognized secret fields, but original error
//! sources are unredacted; logging complete source chains requires care.
//!
//! Use [`Error::kind`], [`Error::errcode`], [`Error::api_code`], [`Error::status`],
//! [`Error::request_id`], and [`Error::retry_after`] for structured diagnostics.
//! [`Error::is_retryable`] identifies potentially transient failures, not whether
//! replay is safe. An ambiguous timeout can occur after a send has succeeded.
//! Non-idempotent transport retries are disabled by default. Opting in through
//! [`DingTalkBuilder::retry_non_idempotent_requests`] requires duplicate handling;
//! [`RetryPolicy::disabled`] disables transport retries altogether.
//!
//! OpenAPI refreshes an explicitly rejected access token and replays at most once.
//! Permission errors and ambiguous delivery failures do not trigger token recovery.
//! A transport profile provides defaults; explicit timeout and retry settings win
//! regardless of setter order. See [`DingTalkBuilder::profile`] and
//! [`DingTalkBuilder::total_timeout`] for deadline scope and disabling a deadline.
//!
//! # Choosing an API
//!
//! - `bot`: routes and typed contexts; session replies versus proactive replies.
//! - `stream`: high-level runner and low-level connection; state and processing policy.
//! - `openapi`: robot messages, persistent reply targets, pagination and batch recall;
//!   bounded-memory uploads and downloads.
//! - [`auth`]: credential loading and reusable token caches.
//! - [`prelude`]: common application imports; [`types`] groups types by capability.
//!
//! Futures must support `Send`, and shared state must also support `Sync`. Keep
//! handler I/O asynchronous or move blocking work to `tokio::task::spawn_blocking`.
//! Platform permissions and actual delivery must be verified in the target application.

extern crate self as dingding;

#[cfg(not(any(
    feature = "async-tls-rustls-ring",
    feature = "async-tls-rustls-aws-lc-rs",
    feature = "async-tls-native"
)))]
compile_error!(
    "Enable exactly one async TLS feature: \
     `async-tls-rustls-ring`, `async-tls-rustls-aws-lc-rs`, or `async-tls-native`."
);

#[cfg(all(
    feature = "async-tls-rustls-ring",
    feature = "async-tls-rustls-aws-lc-rs"
))]
compile_error!("`async-tls-rustls-ring` and `async-tls-rustls-aws-lc-rs` are mutually exclusive.");

#[cfg(all(feature = "async-tls-rustls-ring", feature = "async-tls-native"))]
compile_error!("`async-tls-rustls-ring` and `async-tls-native` are mutually exclusive.");

#[cfg(all(feature = "async-tls-rustls-aws-lc-rs", feature = "async-tls-native"))]
compile_error!("`async-tls-rustls-aws-lc-rs` and `async-tls-native` are mutually exclusive.");

#[cfg(not(any(feature = "webhook", feature = "openapi", feature = "bot")))]
compile_error!("Enable at least one capability feature: `webhook`, `openapi`, or `bot`.");

mod client;
mod error;
mod handler_result;
#[cfg(feature = "webhook")]
mod signature;
mod transport;
mod util;

/// Authentication credentials and token cache helpers.
pub mod auth;

#[cfg(feature = "bot")]
pub mod bot;
#[cfg(feature = "openapi")]
pub mod openapi;
#[cfg(feature = "stream")]
pub mod stream;
#[cfg(feature = "webhook")]
/// Custom robot and session webhook message sender.
pub mod webhook;

/// Common imports for building DingTalk robot applications.
pub mod prelude;
/// Public type re-exports grouped by capability.
pub mod types;

pub use client::{DingTalk, DingTalkBuilder};
pub use error::{BoxError, Error, ErrorKind, HandlerResult, Result};
pub use handler_result::{IntoHandlerResult, handler_future};
pub use reqx::advanced::ClientProfile;
pub use reqx::prelude::RetryPolicy;
pub use transport::BodySnippetConfig;

#[cfg(feature = "macros")]
pub use dingding_macros::handler;
