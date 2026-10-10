use std::{fmt, time::Duration};

use crate::{Error, ErrorKind, util::redact::redact_text};

/// Owned, redacted error metadata for Stream observers.
///
/// Captures metadata available on the SDK error without retaining response bodies
/// or original error sources, which can contain credentials. Retryability describes
/// a potentially transient failure, not permission to replay business operations
/// or the reconnect policy's decision. Retry-after is captured when the event is made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamError {
    kind: ErrorKind,
    message: String,
    status: Option<u16>,
    errcode: Option<i64>,
    api_code: Option<String>,
    request_id: Option<String>,
    retry_after: Option<Duration>,
    retryable: bool,
}

impl From<&Error> for StreamError {
    fn from(error: &Error) -> Self {
        Self {
            kind: error.kind(),
            message: redact_text(&error.to_string()),
            status: error.status(),
            errcode: error.errcode(),
            api_code: error.api_code().map(redact_text),
            request_id: error.request_id().map(redact_text),
            retry_after: error.retry_after(),
            retryable: error.is_retryable(),
        }
    }
}

impl StreamError {
    /// Returns the SDK error category.
    #[must_use]
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Returns redacted human-readable error text.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the HTTP status, independent of the business error code.
    #[must_use]
    pub fn status(&self) -> Option<u16> {
        self.status
    }

    /// Returns the legacy or synthetic numeric DingTalk code. See [`Error::errcode`].
    #[must_use]
    pub fn errcode(&self) -> Option<i64> {
        self.errcode
    }

    /// Returns the modern DingTalk API code, when available.
    #[must_use]
    pub fn api_code(&self) -> Option<&str> {
        self.api_code.as_deref()
    }

    /// Returns the redacted request id, when available.
    #[must_use]
    pub fn request_id(&self) -> Option<&str> {
        self.request_id.as_deref()
    }

    /// Returns the server's retry delay captured at event creation.
    #[must_use]
    pub fn retry_after(&self) -> Option<Duration> {
        self.retry_after
    }

    /// Returns whether the SDK considers the failure potentially transient.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        self.retryable
    }
}

impl fmt::Display for StreamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summary_redacts_all_text_fields_and_excludes_sources_and_bodies() {
        let error = Error::Api {
            code: 1,
            api_code: Some("access_token=api-secret".into()),
            message: "app_secret=message-secret".into(),
            request_id: Some("ticket=request-secret".into()),
            error_body_snippet: Some("private-body".into()),
            status: Some(403),
            retry_after: None,
        };
        let summary = StreamError::from(&error);
        assert_eq!(summary.kind(), ErrorKind::Api);
        assert!(!summary.is_retryable());
        let output = format!("{summary:?} {summary}");
        for secret in [
            "api-secret",
            "message-secret",
            "request-secret",
            "private-body",
        ] {
            assert!(!output.contains(secret));
        }
        assert!(summary.api_code().expect("code").contains("<redacted>"));
        assert!(
            summary
                .request_id()
                .expect("request id")
                .contains("<redacted>")
        );
        let error = Error::handler(std::io::Error::other("access_token=source-secret"));
        let summary = StreamError::from(&error);
        assert_eq!(summary.kind(), ErrorKind::Handler);
        assert!(!format!("{summary:?}").contains("source-secret"));
    }

    #[test]
    fn websocket_handshake_metadata_survives_conversion() {
        use tokio_tungstenite::tungstenite::{Error as WsError, http::Response};
        let response = Response::builder()
            .status(429)
            .header("x-request-id", "request-1")
            .header("retry-after", "7")
            .body(Some(b"private-response".to_vec()))
            .expect("response");
        let error = Error::websocket_connect(WsError::Http(Box::new(response)));
        let summary = StreamError::from(&error);
        assert_eq!(summary.kind(), ErrorKind::Stream);
        assert_eq!(summary.status(), Some(429));
        assert_eq!(summary.request_id(), Some("request-1"));
        assert_eq!(summary.retry_after(), Some(Duration::from_secs(7)));
        assert!(summary.is_retryable());
        assert!(!format!("{summary:?} {error:?}").contains("private-response"));
    }
}
