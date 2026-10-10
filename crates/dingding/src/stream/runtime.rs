use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    future::Future,
    panic::AssertUnwindSafe,
    pin::Pin,
    time::Duration,
};

use futures_util::{FutureExt, Sink, SinkExt, StreamExt, stream::FuturesUnordered};
use serde_json::Value;
use tokio::{sync::mpsc, time::Instant};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};

use super::{
    StreamAck, StreamAckData, StreamClient, StreamExit, StreamFrame, StreamHandleResult,
    StreamRunEvent, validate_stream_duration,
};
use crate::{Error, Result, bot::dedup::Deduplication};

/// Bounds for Stream business handlers, buffering, network writes, and shutdown.
///
/// ```no_run
/// use std::time::Duration;
/// use dingding::prelude::*;
/// # async fn run() -> Result<()> {
/// StreamBot::from_env()?
///     .processing_policy(StreamProcessingPolicy {
///         max_concurrent_handlers: 4,
///         queue_capacity: 32,
///         handler_timeout: Duration::from_secs(10),
///         shutdown_timeout: Duration::from_secs(20),
///         ..StreamProcessingPolicy::default()
///     })
///     .on_frame(|_ctx, _frame| async {})
///     .run_until(async { let _ = tokio::signal::ctrl_c().await; })
///     .await
/// # }
/// ```
#[derive(Debug, Clone, Copy)]
pub struct StreamProcessingPolicy {
    /// Concurrent handlers. The default of one preserves arrival order.
    pub max_concurrent_handlers: usize,
    /// Waiting business frames. Zero rejects work whenever all handlers are occupied.
    pub queue_capacity: usize,
    /// Maximum time for one handler, including deduplication storage operations.
    pub handler_timeout: Duration,
    /// Maximum time for a WebSocket write or close.
    pub write_timeout: Duration,
    /// Maximum total time to finish accepted work, send ACKs, and close after shutdown.
    pub shutdown_timeout: Duration,
    /// Maximum total time to finish accepted work after disconnect or transport loss.
    /// ACKs are attempted only while the socket is usable. Defaults to 30 seconds.
    pub disconnect_timeout: Duration,
}

impl Default for StreamProcessingPolicy {
    fn default() -> Self {
        Self {
            max_concurrent_handlers: 1,
            queue_capacity: 64,
            handler_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(5),
            shutdown_timeout: Duration::from_secs(30),
            disconnect_timeout: Duration::from_secs(30),
        }
    }
}

impl StreamProcessingPolicy {
    /// Validates limits before opening a connection.
    pub fn validate(&self) -> Result<()> {
        if self.max_concurrent_handlers == 0
            || self
                .max_concurrent_handlers
                .checked_add(self.queue_capacity)
                .and_then(|n| n.checked_add(8))
                .is_none_or(|n| n > tokio::sync::Semaphore::MAX_PERMITS)
        {
            return Err(Error::InvalidConfig(
                "invalid Stream processing capacity".into(),
            ));
        }
        validate_stream_duration("handler_timeout", self.handler_timeout)?;
        validate_stream_duration("write_timeout", self.write_timeout)?;
        validate_stream_duration("shutdown_timeout", self.shutdown_timeout)?;
        validate_stream_duration("disconnect_timeout", self.disconnect_timeout)
    }
}

/// Why accepted Stream frame processing was cancelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StreamCancellationReason {
    /// The per-frame processing deadline expired, including deduplication operations.
    HandlerTimeout,
    /// The connection's bounded drain deadline expired after disconnect or transport loss.
    DisconnectTimeout,
    /// The caller's graceful shutdown deadline expired.
    ShutdownTimeout,
    /// The running future was dropped without completing its drain.
    RunnerDropped,
}

impl StreamCancellationReason {
    /// Returns a stable lowercase label for logs and metrics.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::HandlerTimeout => "handler_timeout",
            Self::DisconnectTimeout => "disconnect_timeout",
            Self::ShutdownTimeout => "shutdown_timeout",
            Self::RunnerDropped => "runner_dropped",
        }
    }
}

impl fmt::Display for StreamCancellationReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// Kept outside the job futures so cancellation also reports work not yet polled.
struct PendingFrames<'a> {
    client: &'a StreamClient,
    ids: BTreeMap<u64, String>,
    reason: StreamCancellationReason,
}

impl Drop for PendingFrames<'_> {
    fn drop(&mut self) {
        for message_id in self.ids.values() {
            self.client.emit_event(StreamRunEvent::FrameCancelled {
                message_id: message_id.clone(),
                reason: self.reason,
            });
        }
    }
}

struct SocketState {
    sender: Option<mpsc::Sender<Message>>,
    connected: bool,
    shutdown: bool,
    exit: Result<StreamExit>,
    drain: Option<(Instant, StreamCancellationReason)>,
    policy: StreamProcessingPolicy,
}

impl SocketState {
    fn drain(&mut self, timeout: Duration, reason: StreamCancellationReason) {
        let deadline = Instant::now() + timeout;
        if self.drain.is_none_or(|(current, _)| deadline < current) {
            self.drain = Some((deadline, reason));
        }
    }

    fn disconnect(&mut self, error: Option<Error>) {
        self.connected = false;
        self.sender = None;
        if let Some(error) = error
            && self.exit.is_ok()
        {
            self.exit = Err(error);
        }
        self.drain(
            self.policy.disconnect_timeout,
            StreamCancellationReason::DisconnectTimeout,
        );
    }

    fn send(&mut self, message: Message) {
        if let Some(sender) = &self.sender
            && let Err(error) = enqueue_message(sender, message)
        {
            self.disconnect(Some(error));
        }
    }

    fn ack(&mut self, ack: &StreamAck) {
        match serde_json::to_string(ack) {
            Ok(text) => self.send(Message::Text(text.into())),
            Err(error) => self.disconnect(Some(error.into())),
        }
    }
}

impl StreamClient {
    pub(super) async fn run_socket<S, F>(
        &self,
        socket: WebSocketStream<S>,
        mut shutdown: Pin<&mut F>,
        connected_for: &mut Duration,
    ) -> Result<Option<StreamExit>>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
        F: Future<Output = ()> + ?Sized,
    {
        let connected_at = Instant::now();
        let mut lifetime_recorded = false;
        let (sink, mut source) = socket.split();
        let (sender, receiver) = mpsc::channel(
            self.processing.queue_capacity + self.processing.max_concurrent_handlers + 8,
        );
        let mut pending = PendingFrames {
            client: self,
            ids: BTreeMap::new(),
            reason: StreamCancellationReason::RunnerDropped,
        };
        let mut jobs = FuturesUnordered::new();
        let mut queue = VecDeque::new();
        let mut next_id = 0_u64;
        let mut state = SocketState {
            sender: Some(sender),
            connected: true,
            shutdown: false,
            exit: Ok(StreamExit::Closed),
            drain: None,
            policy: self.processing,
        };
        let writer = write_socket(sink, receiver, self.processing.write_timeout);
        tokio::pin!(writer);
        let mut writer_done = false;
        loop {
            // A buffered input burst must also give the sibling ACK writer time to run.
            tokio::task::yield_now().await;
            while jobs.len() < self.processing.max_concurrent_handlers {
                let Some((id, frame)) = queue.pop_front() else {
                    break;
                };
                jobs.push(async move { (id, self.process_business_frame(frame).await) });
            }
            if state.drain.is_some() && jobs.is_empty() && queue.is_empty() {
                state.sender = None;
                if writer_done {
                    return state
                        .exit
                        .map(|exit| if state.shutdown { None } else { Some(exit) });
                }
            }
            let deadline = state
                .drain
                .map(|(deadline, _)| deadline)
                .unwrap_or_else(Instant::now);
            tokio::select! {
                // Stop accepting business work before handling another frame on shutdown.
                biased;
                () = shutdown.as_mut(), if !state.shutdown => {
                    state.shutdown = true;
                    state.drain(self.processing.shutdown_timeout, StreamCancellationReason::ShutdownTimeout);
                }
                () = tokio::time::sleep_until(deadline), if state.drain.is_some() => {
                    let reason = state.drain.map(|(_, reason)| reason).unwrap_or(StreamCancellationReason::RunnerDropped);
                    pending.reason = reason;
                    let message = if reason == StreamCancellationReason::ShutdownTimeout {
                        "shutdown drain timed out; unfinished handlers were cancelled"
                    } else {
                        "disconnect drain timed out; unfinished handlers were cancelled"
                    };
                    return Err(Error::stream(message));
                }
                result = &mut writer, if !writer_done => {
                    writer_done = true;
                    if let Err(error) = result
                        && state.connected
                    {
                        state.disconnect(Some(error));
                    }
                }
                Some((id, handled)) = jobs.next(), if !jobs.is_empty() => {
                    pending.ids.remove(&id);
                    self.emit_frame_error(&handled);
                    state.ack(&handled.ack);
                }
                message = source.next(), if state.connected && state.sender.is_some() => {
                    let text = match message {
                        Some(Ok(Message::Text(text))) => Some(Ok(text.to_string())),
                        Some(Ok(Message::Binary(bytes))) => Some(String::from_utf8(bytes.to_vec())
                            .map_err(|error| Error::stream(format!("invalid utf-8 frame: {error}")))),
                        Some(Ok(Message::Ping(bytes))) => {
                            state.send(Message::Pong(bytes));
                            None
                        }
                        Some(Ok(Message::Close(_))) | None => {
                            state.disconnect(None);
                            None
                        }
                        Some(Err(error)) => {
                            state.disconnect(Some(Error::stream(format!("websocket receive failed: {error}"))));
                            None
                        }
                        Some(Ok(Message::Pong(_) | Message::Frame(_))) => None,
                    };
                    match text {
                        Some(Err(error)) => state.disconnect(Some(error)),
                        Some(Ok(text)) => match StreamFrame::from_text(&text) {
                            Err(error) => {
                                let id = StreamFrame::message_id_from_text(&text).unwrap_or_default();
                                let handled = frame_failure(id, error);
                                self.emit_frame_error(&handled);
                                state.ack(&handled.ack);
                            }
                            Ok(frame) if frame.frame_type().is_system() => match self.handle_system_frame(frame) {
                                Ok(handled) => {
                                    state.ack(&handled.ack);
                                    if handled.exit_after_ack {
                                        if state.exit.is_ok() {
                                            state.exit = Ok(StreamExit::Disconnect);
                                        }
                                        state.drain(self.processing.disconnect_timeout, StreamCancellationReason::DisconnectTimeout);
                                    }
                                }
                                Err(error) => state.disconnect(Some(error)),
                            },
                            Ok(frame) => {
                                if state.drain.is_some() || (jobs.len() >= self.processing.max_concurrent_handlers
                                    && queue.len() >= self.processing.queue_capacity)
                                {
                                    let message = if state.drain.is_some() { "stream is draining" } else { "business frame queue is full" };
                                    let handled = frame_failure(frame.message_id().to_owned(), Error::stream(message));
                                    self.emit_frame_error(&handled);
                                    state.ack(&handled.ack);
                                } else {
                                    let id = next_id;
                                    next_id = next_id.wrapping_add(1);
                                    pending.ids.insert(id, frame.message_id().to_owned());
                                    queue.push_back((id, frame));
                                }
                            }
                        },
                        None => {}
                    }
                }
            }
            if state.drain.is_some() && !lifetime_recorded {
                *connected_for = connected_at.elapsed();
                lifetime_recorded = true;
            }
        }
    }

    async fn process_business_frame(&self, frame: StreamFrame) -> StreamHandleResult {
        let message_id = frame.message_id().to_owned();
        let processing = AssertUnwindSafe(self.process_deduplicated_frame(frame)).catch_unwind();
        match tokio::time::timeout(self.processing.handler_timeout, processing).await {
            Ok(Ok(Ok(handled))) => handled,
            Ok(Ok(Err(error))) => frame_failure(message_id, error),
            Ok(Err(_)) => frame_failure(message_id, Error::stream("business handler panicked")),
            Err(_) => {
                self.emit_event(StreamRunEvent::FrameCancelled {
                    message_id: message_id.clone(),
                    reason: StreamCancellationReason::HandlerTimeout,
                });
                frame_failure(message_id, Error::stream("business handler timed out"))
            }
        }
    }

    async fn process_deduplicated_frame(&self, frame: StreamFrame) -> Result<StreamHandleResult> {
        let key = serde_json::to_string(&(
            "stream",
            self.credentials.app_key(),
            frame.frame_type().as_str(),
            frame.topic(),
            frame.message_id(),
        ))?;
        match self.deduplicator.claim(&key).await? {
            Deduplication::Acquired(lease) => {
                let handled = self.handle_parsed_frame(frame).await?;
                if handled.error.is_none() && handled.ack.code == 200 {
                    lease
                        .complete(Value::String(handled.ack.data.clone()))
                        .await?;
                }
                Ok(handled)
            }
            Deduplication::Completed(value) => {
                let data = value
                    .as_str()
                    .ok_or_else(|| Error::stream("invalid cached Stream response"))?;
                let mut ack = StreamAck::ok(
                    frame.message_id().to_owned(),
                    StreamAckData::Response(Value::Null),
                );
                ack.data = data.to_owned();
                Ok(StreamHandleResult::ack(ack))
            }
            Deduplication::InFlight => Err(Error::stream("event is already being processed")),
        }
    }
}

fn frame_failure(message_id: String, error: Error) -> StreamHandleResult {
    StreamHandleResult::frame_error(
        Some(message_id.clone()),
        StreamAck::internal_error(message_id),
        error,
    )
}

fn enqueue_message(sender: &mpsc::Sender<Message>, message: Message) -> Result<()> {
    sender
        .try_send(message)
        .map_err(|_| Error::stream("websocket write queue is unavailable or full"))
}

async fn write_socket<S>(
    mut sink: S,
    mut receiver: mpsc::Receiver<Message>,
    timeout: Duration,
) -> Result<()>
where
    S: Sink<Message> + Unpin,
    S::Error: fmt::Display,
{
    while let Some(message) = receiver.recv().await {
        tokio::time::timeout(timeout, sink.send(message))
            .await
            .map_err(|_| Error::stream("websocket write timed out"))?
            .map_err(|error| Error::stream(format!("websocket send failed: {error}")))?;
    }
    tokio::time::timeout(timeout, sink.close())
        .await
        .map_err(|_| Error::stream("websocket close timed out"))?
        .map_err(|error| Error::stream(format!("websocket close failed: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FailingWriter {
        inner: tokio::io::DuplexStream,
        fail: std::sync::Arc<std::sync::atomic::AtomicBool>,
        failed: std::sync::Arc<tokio::sync::Notify>,
    }

    impl tokio::io::AsyncRead for FailingWriter {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buffer: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_read(cx, buffer)
        }
    }

    impl tokio::io::AsyncWrite for FailingWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            bytes: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                self.failed.notify_one();
                return std::task::Poll::Ready(Err(std::io::Error::other(
                    "injected write failure",
                )));
            }
            Pin::new(&mut self.inner).poll_write(cx, bytes)
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_flush(cx)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    #[tokio::test]
    async fn ack_writer_failure_drains_instead_of_dropping_handlers() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        use tokio_tungstenite::tungstenite::protocol::Role;

        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let permits = Arc::clone(&gate);
        let completed = Arc::new(AtomicBool::new(false));
        let done = Arc::clone(&completed);
        let (started, mut starts) = mpsc::unbounded_channel();
        let (events, mut received) = mpsc::unbounded_channel();
        let client = StreamClient::builder(
            crate::DingTalk::builder()
                .app_key_and_secret("id", "secret")
                .build()
                .expect("client"),
        )
        .expect("builder")
        .on_event(move |event| {
            let _ = events.send(event);
        })
        .on_frame(move |_, _| {
            let permits = Arc::clone(&permits);
            let done = Arc::clone(&done);
            let started = started.clone();
            async move {
                started.send(()).expect("started");
                permits.acquire().await.expect("gate").forget();
                done.store(true, Ordering::SeqCst);
            }
        })
        .build()
        .expect("stream");
        let (local, remote) = tokio::io::duplex(4096);
        let fail = Arc::new(AtomicBool::new(false));
        let failed = Arc::new(tokio::sync::Notify::new());
        let socket = WebSocketStream::from_raw_socket(
            FailingWriter {
                inner: local,
                fail: Arc::clone(&fail),
                failed: Arc::clone(&failed),
            },
            Role::Client,
            None,
        )
        .await;
        let mut server = WebSocketStream::from_raw_socket(remote, Role::Server, None).await;
        let task = tokio::spawn(async move {
            let shutdown = std::future::pending();
            tokio::pin!(shutdown);
            let mut lifetime = Duration::ZERO;
            client
                .run_socket(socket, shutdown.as_mut(), &mut lifetime)
                .await
        });
        server
            .send(Message::Text(
                serde_json::json!({
                    "type":"EVENT", "headers":{"topic":"test","messageId":"work"}, "data":"{}"
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("work");
        tokio::time::timeout(Duration::from_secs(1), starts.recv())
            .await
            .expect("deadline")
            .expect("started");
        fail.store(true, Ordering::SeqCst);
        server
            .send(Message::Text(
                serde_json::json!({
                    "type":"SYSTEM", "headers":{"topic":"ping","messageId":"ping"}, "data":"{}"
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("ping");
        tokio::time::timeout(Duration::from_secs(1), failed.notified())
            .await
            .expect("write failed");
        assert!(!task.is_finished());
        gate.add_permits(1);
        let error = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("deadline")
            .expect("task")
            .expect_err("write failure");
        assert!(error.to_string().contains("websocket send failed"));
        assert!(completed.load(Ordering::SeqCst));
        while let Ok(event) = received.try_recv() {
            assert!(!matches!(event, StreamRunEvent::FrameCancelled { .. }));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn overlapping_drains_keep_the_earliest_deadline_and_reason() {
        let (sender, _receiver) = mpsc::channel(1);
        let mut state = SocketState {
            sender: Some(sender),
            connected: true,
            shutdown: false,
            exit: Ok(StreamExit::Closed),
            drain: None,
            policy: StreamProcessingPolicy::default(),
        };
        state.drain(
            Duration::from_secs(10),
            StreamCancellationReason::DisconnectTimeout,
        );
        let first = state.drain;
        tokio::time::advance(Duration::from_secs(3)).await;
        state.drain(
            Duration::from_secs(10),
            StreamCancellationReason::DisconnectTimeout,
        );
        state.drain(
            Duration::from_secs(30),
            StreamCancellationReason::ShutdownTimeout,
        );
        assert_eq!(state.drain, first);
        state.drain(
            Duration::from_secs(1),
            StreamCancellationReason::ShutdownTimeout,
        );
        assert_eq!(
            state.drain,
            Some((
                Instant::now() + Duration::from_secs(1),
                StreamCancellationReason::ShutdownTimeout
            ))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn connection_lifetime_excludes_disconnect_drain_time() {
        use std::sync::Arc;
        use tokio_tungstenite::tungstenite::protocol::Role;

        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let permits = Arc::clone(&gate);
        let (started, mut starts) = mpsc::unbounded_channel();
        let client = StreamClient::builder(
            crate::DingTalk::builder()
                .app_key_and_secret("id", "secret")
                .build()
                .expect("client"),
        )
        .expect("builder")
        .on_frame(move |_, _| {
            let permits = Arc::clone(&permits);
            let started = started.clone();
            async move {
                started.send(()).expect("started");
                permits.acquire().await.expect("gate").forget();
            }
        })
        .build()
        .expect("stream");
        let (local, remote) = tokio::io::duplex(4096);
        let socket = WebSocketStream::from_raw_socket(local, Role::Client, None).await;
        let mut server = WebSocketStream::from_raw_socket(remote, Role::Server, None).await;
        let task = tokio::spawn(async move {
            let shutdown = std::future::pending();
            tokio::pin!(shutdown);
            let mut lifetime = Duration::ZERO;
            let result = client
                .run_socket(socket, shutdown.as_mut(), &mut lifetime)
                .await;
            (result, lifetime)
        });
        server
            .send(Message::Text(
                serde_json::json!({
                    "type":"EVENT", "headers":{"topic":"test","messageId":"work"}, "data":"{}"
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("work");
        starts.recv().await.expect("handler started");
        tokio::time::advance(Duration::from_secs(2)).await;
        server.send(Message::Text(serde_json::json!({
            "type":"SYSTEM", "headers":{"topic":"disconnect","messageId":"disconnect"}, "data":"{}"
        }).to_string().into())).await.expect("disconnect");
        server.next().await.expect("disconnect ACK").expect("ACK");
        tokio::time::advance(Duration::from_secs(10)).await;
        assert!(!task.is_finished());
        gate.add_permits(1);
        let (result, lifetime) = task.await.expect("task");
        assert_eq!(result.expect("drain"), Some(StreamExit::Disconnect));
        assert_eq!(lifetime, Duration::from_secs(2));
    }

    #[tokio::test]
    async fn stalled_ack_writes_have_a_deadline() {
        let sink = Box::pin(futures_util::sink::unfold((), |(), _: Message| {
            std::future::pending::<std::result::Result<(), std::io::Error>>()
        }));
        let (sender, receiver) = mpsc::channel(1);
        enqueue_message(&sender, Message::Text("ack".into())).expect("enqueue");
        let error = tokio::time::timeout(
            Duration::from_secs(1),
            write_socket(sink, receiver, Duration::from_millis(10)),
        )
        .await
        .expect("writer deadline")
        .expect_err("stalled write");
        assert!(error.to_string().contains("write timed out"));
    }

    #[test]
    fn invalid_processing_limits_are_rejected_before_connecting() {
        for policy in [
            StreamProcessingPolicy {
                max_concurrent_handlers: 0,
                ..StreamProcessingPolicy::default()
            },
            StreamProcessingPolicy {
                queue_capacity: usize::MAX,
                ..StreamProcessingPolicy::default()
            },
            StreamProcessingPolicy {
                handler_timeout: Duration::ZERO,
                ..StreamProcessingPolicy::default()
            },
            StreamProcessingPolicy {
                write_timeout: Duration::MAX,
                ..StreamProcessingPolicy::default()
            },
            StreamProcessingPolicy {
                shutdown_timeout: Duration::ZERO,
                ..StreamProcessingPolicy::default()
            },
            StreamProcessingPolicy {
                disconnect_timeout: Duration::ZERO,
                ..StreamProcessingPolicy::default()
            },
            StreamProcessingPolicy {
                disconnect_timeout: Duration::MAX,
                ..StreamProcessingPolicy::default()
            },
        ] {
            assert!(policy.validate().is_err());
        }
        assert!(
            StreamProcessingPolicy {
                queue_capacity: 0,
                ..StreamProcessingPolicy::default()
            }
            .validate()
            .is_ok()
        );
    }
}
