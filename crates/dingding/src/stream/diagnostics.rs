use super::StreamRunEvent;
use crate::util::redact::redact_text;

impl StreamRunEvent {
    pub(super) fn trace(&self) {
        match self {
            Self::ConnectionOpening { attempt } => {
                tracing::debug!(target: "dingding::stream", attempt, "opening connection");
            }
            Self::ConnectionOpened { attempt } => {
                tracing::info!(target: "dingding::stream", attempt, "connection opened");
            }
            Self::ConnectionClosed { attempt, exit } => {
                tracing::info!(target: "dingding::stream", attempt, %exit, "connection closed");
            }
            Self::ConnectionError {
                attempt,
                retrying,
                error,
            } => {
                tracing::warn!(target: "dingding::stream", attempt, retrying,
                    error = %error, kind = %error.kind(), status = ?error.status(),
                    api_code = ?error.api_code(), request_id = ?error.request_id(),
                    retry_after = ?error.retry_after(), "connection failed");
            }
            Self::ReconnectScheduled {
                next_attempt,
                consecutive_failures,
                delay,
            } => {
                tracing::debug!(target: "dingding::stream", next_attempt, consecutive_failures, ?delay, "reconnect scheduled");
            }
            Self::FrameError { message_id, error } => {
                tracing::warn!(target: "dingding::stream",
                    message_id = ?message_id.as_deref().map(redact_text),
                    error = %error, kind = %error.kind(), status = ?error.status(),
                    api_code = ?error.api_code(), request_id = ?error.request_id(),
                    retry_after = ?error.retry_after(), "handler or frame processing failed");
            }
            Self::FrameCancelled { message_id, reason } => {
                tracing::warn!(target: "dingding::stream",
                    message_id = %redact_text(message_id), %reason, "frame processing cancelled");
            }
            Self::BotEventHandled {
                message_id,
                outcome,
                ..
            } => {
                tracing::debug!(target: "dingding::stream",
                    message_id = %redact_text(message_id), %outcome, "bot event handled");
            }
            Self::CardCallbackHandled { message_id, .. } => {
                tracing::debug!(target: "dingding::stream",
                    message_id = %redact_text(message_id), "card callback handled");
            }
            Self::Shutdown => {
                tracing::info!(target: "dingding::stream", "shutdown requested");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::Write,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use super::*;
    use crate::{
        DingTalk,
        stream::{StreamClient, StreamExit},
    };

    #[derive(Clone)]
    struct Output(Arc<Mutex<Vec<u8>>>);

    impl Write for Output {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("logs").extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn diagnostics_are_redacted_and_independent_of_event_observers() {
        for with_observer in [false, true] {
            let logs = Arc::new(Mutex::new(Vec::new()));
            let output = Output(Arc::clone(&logs));
            let subscriber = tracing_subscriber::fmt()
                .with_ansi(false)
                .without_time()
                .with_max_level(tracing::Level::DEBUG)
                .with_writer(move || output.clone())
                .finish();
            let _guard = tracing::subscriber::set_default(subscriber);
            let calls = Arc::new(AtomicUsize::new(0));
            let counter = Arc::clone(&calls);
            let sdk = DingTalk::builder()
                .app_key_and_secret("id", "secret")
                .build()
                .expect("SDK");
            let mut builder = StreamClient::builder(sdk)
                .expect("builder")
                .on_frame(|_, _| async {});
            if with_observer {
                builder = builder.on_event(move |_| {
                    counter.fetch_add(1, Ordering::SeqCst);
                });
            }
            let client = builder.build().expect("stream");
            let events = [
                StreamRunEvent::ConnectionOpening { attempt: 1 },
                StreamRunEvent::ConnectionOpened { attempt: 1 },
                StreamRunEvent::ConnectionClosed {
                    attempt: 1,
                    exit: StreamExit::Closed,
                },
                StreamRunEvent::ConnectionError {
                    attempt: 1,
                    retrying: true,
                    error: (&crate::Error::stream("access_token=connection-secret")).into(),
                },
                StreamRunEvent::ReconnectScheduled {
                    next_attempt: 2,
                    consecutive_failures: 1,
                    delay: std::time::Duration::from_secs(1),
                },
                StreamRunEvent::FrameError {
                    message_id: Some("frame-id".into()),
                    error: (&crate::Error::stream("app_secret=handler-secret")).into(),
                },
                StreamRunEvent::CardCallbackHandled {
                    message_id: "card-id".into(),
                    card_biz_id: Some("private-business-id".into()),
                    action: Some("private-form-value".into()),
                },
                StreamRunEvent::Shutdown,
            ];
            let count = events.len();
            for event in events {
                client.emit_event(event);
            }
            assert_eq!(
                calls.load(Ordering::SeqCst),
                if with_observer { count } else { 0 }
            );
            let output = String::from_utf8(logs.lock().expect("logs").clone()).expect("UTF-8");
            for expected in [
                "dingding::stream",
                "WARN",
                "INFO",
                "DEBUG",
                "connection failed",
                "handler or frame processing failed",
                "frame-id",
                "retrying=true",
                "<redacted>",
            ] {
                assert!(output.contains(expected), "missing {expected}: {output}");
            }
            for secret in [
                "connection-secret",
                "handler-secret",
                "private-business-id",
                "private-form-value",
            ] {
                assert!(!output.contains(secret));
            }
        }
    }
}
