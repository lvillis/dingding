use std::{fmt, time::SystemTime, time::SystemTimeError};

use thiserror::Error as ThisError;

/// SDK result type.
pub type Result<T> = std::result::Result<T, Error>;

/// Stable high-level error category.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// DingTalk returned a business error.
    Api,
    /// HTTP transport or request construction failed.
    Transport,
    /// Stream connection or protocol failed.
    Stream,
    /// JSON serialization or deserialization failed.
    Serialization,
    /// Signature generation failed.
    Signature,
    /// System timestamp generation failed.
    Timestamp,
    /// Client configuration is invalid.
    InvalidConfig,
    /// User input is invalid.
    InvalidInput,
    /// An operation requires app credentials that were not configured.
    MissingCredentials,
    /// A bot route was invoked with an incompatible conversation scope.
    BotScope,
}

impl ErrorKind {
    /// Returns a stable lowercase label for logs and metrics.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Api => "api",
            Self::Transport => "transport",
            Self::Stream => "stream",
            Self::Serialization => "serialization",
            Self::Signature => "signature",
            Self::Timestamp => "timestamp",
            Self::InvalidConfig => "invalid_config",
            Self::InvalidInput => "invalid_input",
            Self::MissingCredentials => "missing_credentials",
            Self::BotScope => "bot_scope",
        }
    }
}

/// Unified SDK error.
#[derive(Debug, ThisError)]
#[non_exhaustive]
pub enum Error {
    /// DingTalk API business error, usually represented by `errcode != 0`.
    #[error("DingTalk API error (code={code}): {message}")]
    Api {
        /// DingTalk error code.
        code: i64,
        /// DingTalk error message.
        message: String,
        /// Optional request id returned by DingTalk.
        request_id: Option<String>,
        /// Optional redacted body snippet.
        error_body_snippet: Option<String>,
    },

    /// HTTP transport or request construction failure.
    #[error("HTTP transport error: {0}")]
    Transport(#[from] reqx::Error),

    /// Stream connection or protocol failure.
    #[error("stream error: {0}")]
    Stream(String),

    /// JSON serialization or deserialization failure.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// System time failure while generating timestamps.
    #[error("timestamp generation failed: {0}")]
    Timestamp(#[from] SystemTimeError),

    /// HMAC signature generation failure.
    #[error("signature generation failed")]
    Signature,

    /// Incoming callback signature verification failure.
    #[error("signature verification failed: {0}")]
    InvalidSignature(String),

    /// Invalid SDK configuration.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// Invalid user input.
    #[error("invalid input `{field}`: {message}")]
    InvalidInput {
        /// Field or parameter name.
        field: &'static str,
        /// Human-readable reason.
        message: String,
    },

    /// App credentials are required for the requested OpenAPI operation.
    #[error("app credentials are required for this operation")]
    MissingCredentials,

    /// A typed bot context was requested for a different conversation scope.
    #[error("bot context scope mismatch: expected {expected}, got {actual}")]
    BotScope {
        /// Expected scope.
        expected: &'static str,
        /// Actual scope.
        actual: String,
    },
}

impl Error {
    /// Returns a stable high-level error category.
    #[must_use]
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::Api { .. } => ErrorKind::Api,
            Self::Transport(_) => ErrorKind::Transport,
            Self::Stream(_) => ErrorKind::Stream,
            Self::Serialization(_) => ErrorKind::Serialization,
            Self::Signature | Self::InvalidSignature(_) => ErrorKind::Signature,
            Self::Timestamp(_) => ErrorKind::Timestamp,
            Self::InvalidConfig(_) => ErrorKind::InvalidConfig,
            Self::InvalidInput { .. } => ErrorKind::InvalidInput,
            Self::MissingCredentials => ErrorKind::MissingCredentials,
            Self::BotScope { .. } => ErrorKind::BotScope,
        }
    }

    /// Returns DingTalk request id when present.
    #[must_use]
    pub fn request_id(&self) -> Option<&str> {
        match self {
            Self::Api { request_id, .. } => request_id.as_deref(),
            Self::Transport(error) => error.request_id(),
            _ => None,
        }
    }

    /// Returns redacted response body snippet when retained.
    #[must_use]
    pub fn error_body_snippet(&self) -> Option<&str> {
        match self {
            Self::Api {
                error_body_snippet, ..
            } => error_body_snippet.as_deref(),
            _ => None,
        }
    }

    /// Returns HTTP status code when available.
    #[must_use]
    pub fn status(&self) -> Option<u16> {
        match self {
            Self::Transport(error) => error.status_code(),
            _ => None,
        }
    }

    /// Returns whether the error is likely retryable.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Transport(error) => match error.code() {
                reqx::ErrorCode::Timeout
                | reqx::ErrorCode::DeadlineExceeded
                | reqx::ErrorCode::Transport
                | reqx::ErrorCode::RetryBudgetExhausted
                | reqx::ErrorCode::CircuitOpen => true,
                reqx::ErrorCode::HttpStatus => {
                    matches!(error.status_code(), Some(429 | 500..=599))
                }
                _ => false,
            },
            Self::Api { code, .. } => matches!(*code, 130101 | 130102),
            _ => false,
        }
    }

    /// Returns retry-after hint when the transport layer parsed one.
    #[must_use]
    pub fn retry_after(&self) -> Option<std::time::Duration> {
        match self {
            Self::Transport(error) => error.retry_after(SystemTime::now()),
            _ => None,
        }
    }

    pub(crate) fn invalid_input(field: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidInput {
            field,
            message: message.into(),
        }
    }

    #[cfg(feature = "bot")]
    pub(crate) fn invalid_signature(message: impl Into<String>) -> Self {
        Self::InvalidSignature(message.into())
    }

    pub(crate) fn api(
        code: i64,
        message: impl Into<String>,
        request_id: Option<String>,
        error_body_snippet: Option<String>,
    ) -> Self {
        Self::Api {
            code,
            message: message.into(),
            request_id,
            error_body_snippet,
        }
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_kind_exposes_stable_labels() {
        assert_eq!(ErrorKind::Api.as_str(), "api");
        assert_eq!(ErrorKind::Transport.to_string(), "transport");
        assert_eq!(ErrorKind::InvalidConfig.as_str(), "invalid_config");
        assert_eq!(
            ErrorKind::MissingCredentials.to_string(),
            "missing_credentials"
        );
    }
}
