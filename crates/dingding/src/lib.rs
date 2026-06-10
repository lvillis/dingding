#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(rustdoc::broken_intra_doc_links)]
#![cfg_attr(test, allow(clippy::expect_used, clippy::panic))]

//! DingTalk SDK and bot framework.
//!
//! The crate keeps the public surface small:
//! - [`DingTalk`] is the SDK entry point.
//! - `webhook` sends custom robot and session webhook messages when the `webhook` feature is enabled.
//! - `bot` routes incoming robot events when the `bot` feature is enabled.
//! - `stream` runs enterprise application robots over DingTalk Stream when the `stream` feature is enabled.

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
#[cfg(feature = "webhook")]
mod signature;
mod transport;
mod util;

/// Authentication credentials and token cache helpers.
pub mod auth;

#[cfg(feature = "bot")]
/// Bot callback routing, typed message contexts, and webhook callback verification.
pub mod bot;
#[cfg(feature = "openapi")]
/// Typed OpenAPI helpers for enterprise application robots and interactive cards.
pub mod openapi;
#[cfg(feature = "stream")]
/// DingTalk Stream client, robot runner, subscriptions, and Stream callback frames.
pub mod stream;
#[cfg(feature = "webhook")]
/// Custom robot and session webhook message sender.
pub mod webhook;

/// Common imports for building DingTalk robot applications.
pub mod prelude;
/// Public type re-exports grouped by capability.
pub mod types;

pub use client::{DingTalk, DingTalkBuilder};
pub use error::{Error, ErrorKind, Result};
pub use reqx::advanced::ClientProfile;
pub use reqx::prelude::RetryPolicy;
pub use transport::BodySnippetConfig;

#[cfg(feature = "macros")]
pub use dingding_macros::handler;

/// Backward-friendly alias for the main SDK client.
pub type Client = DingTalk;
