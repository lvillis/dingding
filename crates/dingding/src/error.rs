use std::{
    error::Error as StdError,
    fmt,
    time::{Duration, SystemTime, SystemTimeError},
};

use thiserror::Error as ThisError;

use crate::util::redact::redact_text;

/// SDK result type.
pub type Result<T> = std::result::Result<T, Error>;

/// Application error accepted by bot and Stream handlers.
pub type BoxError = Box<dyn StdError + Send + Sync + 'static>;

/// Handler result that supports `?` with SDK and application errors.
pub type HandlerResult<T = ()> = std::result::Result<T, BoxError>;

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
    /// An application handler failed.
    Handler,
    /// A local file or asynchronous I/O operation failed.
    Io,
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
            Self::Handler => "handler",
            Self::Io => "io",
        }
    }
}

/// Unified SDK error.
#[derive(ThisError)]
#[non_exhaustive]
pub enum Error {
    /// DingTalk API business error, usually represented by `errcode != 0`
    /// or a non-success modern OpenAPI `code`.
    #[error("DingTalk API error (errcode={code}{api_code_suffix}): {message}", api_code_suffix = api_code_suffix(api_code.as_deref()))]
    Api {
        /// Legacy DingTalk numeric `errcode`, HTTP status, or `-1` when DingTalk only returned a
        /// modern string `code`.
        code: i64,
        /// Modern DingTalk OpenAPI string `code` when supplied.
        api_code: Option<String>,
        /// DingTalk error message.
        message: String,
        /// Optional request id returned by DingTalk.
        request_id: Option<String>,
        /// Optional redacted body snippet.
        error_body_snippet: Option<String>,
        /// HTTP response status, independent of the DingTalk business error code.
        status: Option<u16>,
        /// Delay requested by the server, parsed when the response was received.
        retry_after: Option<Box<Duration>>,
    },

    /// HTTP transport or request construction failure.
    #[error("HTTP transport error: {message}")]
    Transport {
        /// Original transport error.
        #[source]
        source: Box<reqx::Error>,
        /// Redacted transport error message.
        message: String,
    },

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

    /// Application handler failure with the original error preserved.
    #[error("handler error: {message}")]
    Handler {
        /// Original application error.
        #[source]
        source: BoxError,
        /// Redacted application error message.
        message: String,
    },

    /// File or asynchronous I/O failure, with the original source preserved.
    #[error("I/O error: {message}")]
    Io {
        /// Original I/O failure.
        #[source]
        source: std::io::Error,
        /// Redacted error message.
        message: String,
    },
}

impl Error {
    /// Returns a stable high-level error category.
    #[must_use]
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::Api { .. } => ErrorKind::Api,
            Self::Transport { .. } => ErrorKind::Transport,
            Self::Stream(_) => ErrorKind::Stream,
            Self::Serialization(_) => ErrorKind::Serialization,
            Self::Signature | Self::InvalidSignature(_) => ErrorKind::Signature,
            Self::Timestamp(_) => ErrorKind::Timestamp,
            Self::InvalidConfig(_) => ErrorKind::InvalidConfig,
            Self::InvalidInput { .. } => ErrorKind::InvalidInput,
            Self::MissingCredentials => ErrorKind::MissingCredentials,
            Self::BotScope { .. } => ErrorKind::BotScope,
            Self::Handler { .. } => ErrorKind::Handler,
            Self::Io { .. } => ErrorKind::Io,
        }
    }

    /// Returns DingTalk request id when present.
    #[must_use]
    pub fn request_id(&self) -> Option<&str> {
        match self {
            Self::Api { request_id, .. } => request_id.as_deref(),
            Self::Transport { source, .. } => source.request_id(),
            _ => None,
        }
    }

    /// Returns the legacy DingTalk numeric `errcode` when this is an API error.
    ///
    /// Modern OpenAPI responses can return only a string `code`; in that case this returns the
    /// SDK's synthetic numeric code, usually `-1`. Use [`Self::api_code`] for the structured
    /// modern code.
    #[must_use]
    pub fn errcode(&self) -> Option<i64> {
        match self {
            Self::Api { code, .. } => Some(*code),
            _ => None,
        }
    }

    /// Returns the modern DingTalk OpenAPI string `code` when present.
    #[must_use]
    pub fn api_code(&self) -> Option<&str> {
        match self {
            Self::Api { api_code, .. } => api_code.as_deref(),
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
            Self::Api { status, .. } => *status,
            Self::Transport { source, .. } => source.status_code(),
            _ => None,
        }
    }

    /// Returns whether the failure may be transient.
    ///
    /// This does not imply that replaying a message send or another non-idempotent operation
    /// is safe. A timeout can occur after the server accepted the request.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Transport { source, .. } => match source.code() {
                reqx::ErrorCode::Timeout
                | reqx::ErrorCode::DeadlineExceeded
                | reqx::ErrorCode::Transport
                | reqx::ErrorCode::RetryBudgetExhausted
                | reqx::ErrorCode::CircuitOpen => true,
                reqx::ErrorCode::HttpStatus => {
                    matches!(source.status_code(), Some(429 | 500..=599))
                }
                _ => false,
            },
            Self::Api {
                code,
                api_code,
                status,
                ..
            } => {
                matches!(status, Some(429 | 500..=599))
                    || matches!(*code, 429 | 500..=599 | 130101 | 130102)
                    || api_code
                        .as_deref()
                        .is_some_and(is_retryable_dingtalk_api_code)
            }
            _ => false,
        }
    }

    /// Returns the server's retry-after hint when one was parsed.
    #[must_use]
    pub fn retry_after(&self) -> Option<std::time::Duration> {
        match self {
            Self::Api { retry_after, .. } => retry_after.as_deref().copied(),
            Self::Transport { source, .. } => source.retry_after(SystemTime::now()),
            _ => None,
        }
    }

    /// Converts an application error into an SDK handler error.
    ///
    /// Existing SDK errors retain their category and metadata. Other errors retain their
    /// original source for downcasting while their displayed message is redacted.
    pub fn handler(source: impl Into<BoxError>) -> Self {
        match source.into().downcast::<Self>() {
            Ok(error) => *error,
            Err(source) => {
                let message = redact_text(&source.to_string());
                Self::Handler { source, message }
            }
        }
    }

    pub(crate) fn with_response_metadata(
        mut self,
        response_status: u16,
        response_request_id: Option<String>,
        response_retry_after: Option<Duration>,
    ) -> Self {
        if let Self::Api {
            status,
            request_id,
            retry_after,
            ..
        } = &mut self
        {
            *status = Some(response_status);
            if response_request_id.is_some() {
                *request_id = response_request_id;
            }
            *retry_after = response_retry_after.map(Box::new);
        }
        self
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

    #[cfg(feature = "stream")]
    pub(crate) fn stream(message: impl Into<String>) -> Self {
        Self::Stream(redact_text(&message.into()))
    }

    pub(crate) fn api_with_code(
        code: i64,
        api_code: Option<String>,
        message: impl Into<String>,
        request_id: Option<String>,
        error_body_snippet: Option<String>,
    ) -> Self {
        Self::Api {
            code,
            api_code,
            message: message.into(),
            request_id,
            error_body_snippet,
            status: None,
            retry_after: None,
        }
    }
}

impl From<reqx::Error> for Error {
    fn from(source: reqx::Error) -> Self {
        let message = normalize_transport_error_message(&source.to_string());
        Self::Transport {
            source: Box::new(source),
            message,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(source: std::io::Error) -> Self {
        let message = redact_text(&source.to_string());
        Self::Io { source, message }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Api {
                code,
                api_code,
                message,
                request_id,
                error_body_snippet,
                status,
                retry_after,
            } => f
                .debug_struct("Api")
                .field("code", code)
                .field("api_code", api_code)
                .field("message", message)
                .field("request_id", request_id)
                .field("error_body_snippet", error_body_snippet)
                .field("status", status)
                .field("retry_after", retry_after)
                .finish(),
            Self::Transport { message, .. } => f
                .debug_struct("Transport")
                .field("message", message)
                .finish(),
            Self::Stream(message) => f.debug_tuple("Stream").field(message).finish(),
            Self::Serialization(source) => f.debug_tuple("Serialization").field(source).finish(),
            Self::Timestamp(source) => f.debug_tuple("Timestamp").field(source).finish(),
            Self::Signature => f.write_str("Signature"),
            Self::InvalidSignature(message) => {
                f.debug_tuple("InvalidSignature").field(message).finish()
            }
            Self::InvalidConfig(message) => f.debug_tuple("InvalidConfig").field(message).finish(),
            Self::InvalidInput { field, message } => f
                .debug_struct("InvalidInput")
                .field("field", field)
                .field("message", message)
                .finish(),
            Self::MissingCredentials => f.write_str("MissingCredentials"),
            Self::BotScope { expected, actual } => f
                .debug_struct("BotScope")
                .field("expected", expected)
                .field("actual", actual)
                .finish(),
            Self::Handler { message, .. } => {
                f.debug_struct("Handler").field("message", message).finish()
            }
            Self::Io { message, .. } => f.debug_struct("Io").field("message", message).finish(),
        }
    }
}

fn normalize_transport_error_message(message: &str) -> String {
    let message = message.trim();
    if message.is_empty() {
        "unknown transport error".to_string()
    } else {
        redact_text(message)
    }
}

fn api_code_suffix(api_code: Option<&str>) -> String {
    api_code
        .map(|api_code| format!(", api_code={api_code}"))
        .unwrap_or_default()
}

fn is_retryable_dingtalk_api_code(value: &str) -> bool {
    let value = value.trim();
    if value
        .parse::<i64>()
        .is_ok_and(|code| matches!(code, 429 | 500..=599 | 130101 | 130102))
    {
        return true;
    }

    let value = value.to_ascii_lowercase();
    value.contains("throttl")
        || value.contains("too_many")
        || value.contains("toomany")
        || value.contains("too many")
        || value.contains("rate_limit")
        || value.contains("ratelimit")
        || value.contains("timeout")
        || value.contains("temporar")
        || value.contains("internal")
        || value.contains("unavailable")
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
    fn sdk_error_stays_compact_for_downstream_results() {
        assert!(std::mem::size_of::<Error>() <= 128);
    }

    #[test]
    fn handler_errors_preserve_sources_and_redact_display() {
        let error = Error::handler(std::io::Error::other(
            "request failed: https://example.test/?access_token=business-secret",
        ));
        assert_eq!(error.kind(), ErrorKind::Handler);
        let source = error.source().expect("application source");
        assert!(source.downcast_ref::<std::io::Error>().is_some());
        assert!(source.to_string().contains("business-secret"));
        for message in [error.to_string(), format!("{error:?}")] {
            assert!(!message.contains("business-secret"));
            assert!(message.contains("<redacted>"));
        }
    }

    #[test]
    fn handler_conversion_preserves_sdk_error_metadata() {
        let original =
            Error::api_with_code(130101, None, "rate limited", Some("body-id".into()), None)
                .with_response_metadata(
                    429,
                    Some("header-id".into()),
                    Some(Duration::from_secs(7)),
                );
        let boxed: BoxError = Box::new(original);
        let error = Error::handler(boxed);
        assert_eq!(error.kind(), ErrorKind::Api);
        assert_eq!(error.errcode(), Some(130101));
        assert_eq!(error.status(), Some(429));
        assert_eq!(error.request_id(), Some("header-id"));
        assert_eq!(error.retry_after(), Some(Duration::from_secs(7)));
        assert!(error.is_retryable());
    }

    #[test]
    fn retryability_uses_http_status_separately_from_business_code() {
        let error = Error::api_with_code(99999, None, "busy", None, None)
            .with_response_metadata(503, None, None);
        assert_eq!(error.errcode(), Some(99999));
        assert!(error.is_retryable());
    }

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

    #[test]
    fn api_http_status_codes_can_be_retryable() {
        assert!(Error::api_with_code(429, None, "too many requests", None, None).is_retryable());
        assert!(Error::api_with_code(503, None, "unavailable", None, None).is_retryable());
        assert!(!Error::api_with_code(400, None, "bad request", None, None).is_retryable());
    }

    #[test]
    fn api_error_exposes_modern_code() {
        let error = Error::api_with_code(
            -1,
            Some("InvalidParameter".to_string()),
            "bad request",
            Some("request-1".to_string()),
            None,
        );

        assert_eq!(error.errcode(), Some(-1));
        assert_eq!(error.api_code(), Some("InvalidParameter"));
        assert_eq!(error.request_id(), Some("request-1"));
        assert!(error.to_string().contains("api_code=InvalidParameter"));
        assert!(!error.is_retryable());
    }

    #[test]
    fn modern_retryable_api_codes_are_retryable() {
        assert!(
            Error::api_with_code(
                -1,
                Some("TooManyRequests".to_string()),
                "limited",
                None,
                None,
            )
            .is_retryable()
        );
        assert!(
            Error::api_with_code(-1, Some("503".to_string()), "unavailable", None, None,)
                .is_retryable()
        );
    }

    #[test]
    fn transport_error_messages_are_redacted_before_display() {
        let message = normalize_transport_error_message(
            "request failed: https://oapi.dingtalk.com/robot/send?access_token=secret-token&sign=callback-sign",
        );

        assert!(message.contains("access_token=<redacted>"));
        assert!(message.contains("sign=<redacted>"));
        assert!(!message.contains("secret-token"));
        assert!(!message.contains("callback-sign"));
    }

    #[cfg(feature = "stream")]
    #[test]
    fn stream_error_messages_are_redacted_before_display() {
        let error = Error::stream(
            "websocket connect failed: wss://example.test/connect?ticket=stream-ticket",
        );
        let message = error.to_string();
        let debug = format!("{error:?}");

        assert!(message.contains("ticket=<redacted>"));
        assert!(debug.contains("ticket=<redacted>"));
        assert!(!message.contains("stream-ticket"));
        assert!(!debug.contains("stream-ticket"));
    }
}
