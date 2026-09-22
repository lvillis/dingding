use std::{
    collections::VecDeque, fmt, future::Future, panic::AssertUnwindSafe, pin::Pin, time::Duration,
};

use futures_util::{
    FutureExt, Sink, SinkExt, Stream, StreamExt, future::try_join, stream::FuturesUnordered,
};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{self, Message},
};

use super::{
    StreamAck, StreamAckData, StreamClient, StreamExit, StreamFrame, StreamHandleResult,
    validate_stream_duration,
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
///     .on_frame(|_frame| async { Ok(()) })
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
}

impl Default for StreamProcessingPolicy {
    fn default() -> Self {
        Self {
            max_concurrent_handlers: 1,
            queue_capacity: 64,
            handler_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(5),
            shutdown_timeout: Duration::from_secs(30),
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
        validate_stream_duration("shutdown_timeout", self.shutdown_timeout)
    }
}

impl StreamClient {
    pub(super) async fn run_socket<S, F>(
        &self,
        socket: WebSocketStream<S>,
        shutdown: Pin<&mut F>,
    ) -> Result<Option<StreamExit>>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
        F: Future<Output = ()> + ?Sized,
    {
        let (sink, source) = socket.split();
        let (sender, receiver) = mpsc::channel(
            self.processing.queue_capacity + self.processing.max_concurrent_handlers + 8,
        );
        let (stop_sender, stop_receiver) = oneshot::channel();
        let session = try_join(
            self.dispatch_socket(source, sender, stop_receiver),
            write_socket(sink, receiver, self.processing.write_timeout),
        );
        tokio::pin!(session);
        tokio::select! {
            biased;
            () = shutdown => {
                let _ = stop_sender.send(());
                tokio::time::timeout(self.processing.shutdown_timeout, &mut session)
                    .await
                    .map_err(|_| Error::stream("shutdown drain timed out; unfinished handlers were cancelled"))??;
                Ok(None)
            }
            result = &mut session => result.map(|(exit, ())| Some(exit)),
        }
    }

    async fn dispatch_socket<S>(
        &self,
        mut source: S,
        sender: mpsc::Sender<Message>,
        mut stop: oneshot::Receiver<()>,
    ) -> Result<StreamExit>
    where
        S: Stream<Item = std::result::Result<Message, tungstenite::Error>> + Unpin,
    {
        let mut jobs = FuturesUnordered::new();
        let mut queue = VecDeque::new();
        let mut draining = false;
        loop {
            // A buffered input burst must also give the sibling ACK writer time to run.
            tokio::task::yield_now().await;
            while jobs.len() < self.processing.max_concurrent_handlers {
                let Some(frame) = queue.pop_front() else {
                    break;
                };
                jobs.push(self.process_business_frame(frame));
            }
            if draining && jobs.is_empty() && queue.is_empty() {
                return Ok(StreamExit::Closed);
            }
            tokio::select! {
                // Stop accepting business work before handling another frame on shutdown.
                biased;
                _ = &mut stop, if !draining => { draining = true; }
                Some(handled) = jobs.next(), if !jobs.is_empty() => {
                    self.emit_frame_error(&handled);
                    enqueue_ack(&sender, &handled.ack)?;
                }
                message = source.next() => {
                    let Some(message) = message else {
                        return if draining {
                            Err(Error::stream("websocket closed before shutdown drain completed"))
                        } else {
                            Ok(StreamExit::Closed)
                        };
                    };
                    let message = message.map_err(|error| Error::stream(format!("websocket receive failed: {error}")))?;
                    let text = match message {
                        Message::Text(text) => text.to_string(),
                        Message::Binary(bytes) => String::from_utf8(bytes.to_vec()).map_err(|error| Error::stream(format!("invalid utf-8 frame: {error}")))?,
                        Message::Ping(bytes) => {
                            enqueue_message(&sender, Message::Pong(bytes))?;
                            continue;
                        }
                        Message::Close(_) => return if draining {
                            Err(Error::stream("websocket closed before shutdown drain completed"))
                        } else {
                            Ok(StreamExit::Closed)
                        },
                        Message::Pong(_) | Message::Frame(_) => continue,
                    };
                    let frame = match StreamFrame::from_text(&text) {
                        Ok(frame) => frame,
                        Err(error) => {
                            let id = StreamFrame::message_id_from_text(&text).unwrap_or_default();
                            let handled = frame_failure(id, error);
                            self.emit_frame_error(&handled);
                            enqueue_ack(&sender, &handled.ack)?;
                            continue;
                        }
                    };
                    if frame.frame_type().is_system() {
                        let handled = self.handle_system_frame(frame)?;
                        enqueue_ack(&sender, &handled.ack)?;
                        if handled.exit_after_ack && !draining {
                            return Ok(StreamExit::Disconnect);
                        }
                    } else if draining {
                        let handled = frame_failure(frame.message_id().to_owned(), Error::stream("stream is shutting down"));
                        self.emit_frame_error(&handled);
                        enqueue_ack(&sender, &handled.ack)?;
                    } else if jobs.len() < self.processing.max_concurrent_handlers {
                        jobs.push(self.process_business_frame(frame));
                    } else if queue.len() < self.processing.queue_capacity {
                        queue.push_back(frame);
                    } else {
                        let handled = frame_failure(frame.message_id().to_owned(), Error::stream("business frame queue is full"));
                        self.emit_frame_error(&handled);
                        enqueue_ack(&sender, &handled.ack)?;
                    }
                }
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
            Err(_) => frame_failure(message_id, Error::stream("business handler timed out")),
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

fn enqueue_ack(sender: &mpsc::Sender<Message>, ack: &StreamAck) -> Result<()> {
    enqueue_message(sender, Message::Text(serde_json::to_string(ack)?.into()))
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
