#![cfg(feature = "stream")]
#![allow(clippy::expect_used, clippy::panic)]

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use dingding::{
    DingTalk, Error,
    stream::{
        CARD_CALLBACK_TOPIC, ReconnectPolicy, StreamCancellationReason, StreamClient,
        StreamFrameResponse, StreamProcessingPolicy, StreamRunEvent,
    },
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Semaphore, mpsc, oneshot},
    task::JoinHandle,
};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};

type Socket = WebSocketStream<TcpStream>;

#[derive(Clone, Default)]
struct LogOutput(Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for LogOutput {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("logs").extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn text_handler_failure_is_logged_without_an_event_observer() {
    let (client, listener, http) = gateway(1).await;
    let output = LogOutput::default();
    let writer = output.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_max_level(tracing::Level::INFO)
        .with_writer(move || writer.clone())
        .finish();
    // This integration-test process owns logging, just like an application.
    tracing::subscriber::set_global_default(subscriber).expect("application subscriber");
    let bot = dingding::stream::StreamBot::from_client(client)
        .on_group_text_command("/fail", |_| async {
            Err::<(), _>(std::io::Error::other("access_token=handler-secret"))
        })
        .build()
        .expect("bot");
    let (stop, shutdown) = oneshot::channel();
    let task = tokio::spawn(async move {
        bot.run_until(async {
            let _ = shutdown.await;
        })
        .await
    });
    let mut socket = accept(&listener).await;
    let body = json!({"type":"CALLBACK", "headers":{
        "topic":dingding::stream::BOT_MESSAGE_TOPIC, "messageId":"text-failure"
    }, "data":{
        "conversationType":"2", "msgtype":"text", "text":{"content":"/fail private-text-content"}
    }});
    send(&mut socket, Message::Text(body.to_string().into())).await;
    assert_eq!(ack(&mut socket).await["code"], 500);
    assert!(
        !task.is_finished(),
        "handler failure must not stop the runner"
    );
    stop.send(()).expect("shutdown");
    bounded(task)
        .await
        .expect("task")
        .expect("graceful shutdown");
    bounded(http).await.expect("gateway task");
    let logs = String::from_utf8(output.0.lock().expect("logs").clone()).expect("UTF-8");
    for expected in [
        "WARN",
        "dingding::stream",
        "text-failure",
        "handler or frame processing failed",
        "connection opened",
        "shutdown requested",
    ] {
        assert!(logs.contains(expected), "missing {expected}: {logs}");
    }
    for secret in [
        "handler-secret",
        "private-text-content",
        "client-secret",
        "test-ticket",
    ] {
        assert!(!logs.contains(secret));
    }
}

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("test deadline")
}

async fn gateway(connections: usize) -> (DingTalk, TcpListener, JoinHandle<()>) {
    let websocket = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("websocket listener");
    let endpoint = format!("ws://{}", websocket.local_addr().expect("address"));
    let http = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("http listener");
    let client = DingTalk::builder()
        .openapi_base_url(format!("http://{}", http.local_addr().expect("address")))
        .app_key_and_secret("client-id", "client-secret")
        .system_proxy(false)
        .build()
        .expect("SDK client");
    let task = tokio::spawn(async move {
        for _ in 0..connections {
            let (mut socket, _) = bounded(http.accept()).await.expect("gateway accept");
            let mut request = Vec::new();
            loop {
                let mut buffer = [0; 4096];
                let n = bounded(socket.read(&mut buffer))
                    .await
                    .expect("gateway request");
                assert!(n > 0);
                request.extend_from_slice(&buffer[..n]);
                if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&request[..end]);
                    assert!(headers.starts_with("POST /v1.0/gateway/connections/open "));
                    let length = headers
                        .lines()
                        .filter_map(|line| line.split_once(':'))
                        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                        .map(|(_, value)| value.trim().parse::<usize>().expect("length"))
                        .unwrap_or(0);
                    if request.len() >= end + 4 + length {
                        break;
                    }
                }
            }
            let body = json!({"endpoint":endpoint,"ticket":"test-ticket"}).to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            bounded(socket.write_all(response.as_bytes()))
                .await
                .expect("gateway response");
        }
    });
    (client, websocket, task)
}

async fn accept(listener: &TcpListener) -> Socket {
    let (socket, _) = bounded(listener.accept()).await.expect("accept websocket");
    bounded(accept_async(socket))
        .await
        .expect("websocket handshake")
}

fn frame(kind: &str, topic: &str, id: &str) -> Message {
    Message::Text(json!({"specVersion":"1.0","type":kind,"headers":{"topic":topic,"messageId":id,"contentType":"application/json"},"data":"{}"}).to_string().into())
}

async fn send(socket: &mut Socket, message: Message) {
    bounded(socket.send(message)).await.expect("send frame");
}

async fn ack(socket: &mut Socket) -> Value {
    loop {
        let message = bounded(socket.next())
            .await
            .expect("ACK frame")
            .expect("read ACK");
        if let Message::Text(text) = message {
            return serde_json::from_str(&text).expect("ACK JSON");
        }
    }
}

fn launch(stream: StreamClient) -> (oneshot::Sender<()>, JoinHandle<dingding::Result<()>>) {
    let (stop, _observed, task) = launch_with_observed_shutdown(stream);
    (stop, task)
}

fn launch_with_observed_shutdown(
    stream: StreamClient,
) -> (
    oneshot::Sender<()>,
    oneshot::Receiver<()>,
    JoinHandle<dingding::Result<()>>,
) {
    let (stop, shutdown) = oneshot::channel();
    let (observed, observation) = oneshot::channel();
    let task = tokio::spawn(async move {
        stream
            .run_until(async {
                let _ = shutdown.await;
                let _ = observed.send(());
            })
            .await
    });
    (stop, observation, task)
}

#[tokio::test]
async fn shutdown_preserves_heartbeats_but_rejects_new_business_frames() {
    let (client, listener, http) = gateway(1).await;
    let permits = Arc::new(Semaphore::new(0));
    let gate = Arc::clone(&permits);
    let (started, mut starts) = mpsc::unbounded_channel();
    let stream = StreamClient::builder(client)
        .expect("builder")
        .on_frame(move |_ctx, frame| {
            let gate = Arc::clone(&gate);
            let started = started.clone();
            async move {
                started
                    .send(frame.message_id().to_owned())
                    .expect("started");
                gate.acquire().await.expect("gate").forget();
            }
        })
        .build()
        .expect("stream");
    let (stop, observed, task) = launch_with_observed_shutdown(stream);
    let mut socket = accept(&listener).await;
    send(&mut socket, frame("EVENT", "topic", "accepted")).await;
    assert_eq!(bounded(starts.recv()).await.as_deref(), Some("accepted"));
    stop.send(()).expect("shutdown");
    bounded(observed).await.expect("shutdown observed");

    send(&mut socket, frame("SYSTEM", "ping", "draining-ping")).await;
    let heartbeat = ack(&mut socket).await;
    assert_eq!(heartbeat["headers"]["messageId"], "draining-ping");
    assert_eq!(heartbeat["code"], 200);
    send(&mut socket, Message::Ping(b"draining".to_vec().into())).await;
    assert!(
        matches!(bounded(socket.next()).await, Some(Ok(Message::Pong(bytes))) if bytes.as_ref() == b"draining")
    );
    send(&mut socket, frame("EVENT", "topic", "rejected")).await;
    let rejected = ack(&mut socket).await;
    assert_eq!(rejected["headers"]["messageId"], "rejected");
    assert_eq!(rejected["code"], 500);
    assert!(starts.try_recv().is_err());

    send(
        &mut socket,
        frame("SYSTEM", "disconnect", "draining-disconnect"),
    )
    .await;
    let disconnect = ack(&mut socket).await;
    assert_eq!(disconnect["headers"]["messageId"], "draining-disconnect");
    assert!(!task.is_finished());

    permits.add_permits(1);
    let accepted = ack(&mut socket).await;
    assert_eq!(accepted["headers"]["messageId"], "accepted");
    assert_eq!(accepted["code"], 200);
    bounded(task)
        .await
        .expect("task")
        .expect("drained shutdown");
    bounded(http).await.expect("gateway task");
}

#[tokio::test]
async fn slow_handlers_preserve_heartbeats_order_and_graceful_shutdown() {
    let (client, listener, http) = gateway(1).await;
    let permits = Arc::new(Semaphore::new(0));
    let gate = Arc::clone(&permits);
    let (started, mut starts) = mpsc::unbounded_channel();
    let stream = StreamClient::builder(client)
        .expect("builder")
        .on_frame(move |_ctx, frame| {
            let gate = Arc::clone(&gate);
            let started = started.clone();
            async move {
                started
                    .send(frame.message_id().to_owned())
                    .expect("started");
                gate.acquire().await.expect("gate").forget();
            }
        })
        .build()
        .expect("stream");
    let (stop, task) = launch(stream);
    let mut socket = accept(&listener).await;
    send(&mut socket, frame("EVENT", "topic", "one")).await;
    assert_eq!(bounded(starts.recv()).await.as_deref(), Some("one"));
    send(&mut socket, frame("EVENT", "topic", "two")).await;
    send(&mut socket, frame("SYSTEM", "ping", "heartbeat")).await;
    assert_eq!(ack(&mut socket).await["headers"]["messageId"], "heartbeat");
    send(&mut socket, Message::Ping(b"alive".to_vec().into())).await;
    assert!(matches!(
        bounded(socket.next()).await,
        Some(Ok(Message::Pong(_)))
    ));
    assert!(starts.try_recv().is_err());
    stop.send(()).expect("shutdown");
    permits.add_permits(2);
    assert_eq!(ack(&mut socket).await["headers"]["messageId"], "one");
    assert_eq!(ack(&mut socket).await["headers"]["messageId"], "two");
    assert_eq!(bounded(starts.recv()).await.as_deref(), Some("two"));
    bounded(task)
        .await
        .expect("task")
        .expect("drained shutdown");
    bounded(http).await.expect("gateway task");
}

#[tokio::test]
async fn concurrency_and_queue_are_bounded_without_blocking_control_frames() {
    let (client, listener, http) = gateway(1).await;
    let permits = Arc::new(Semaphore::new(0));
    let gate = Arc::clone(&permits);
    let (started, mut starts) = mpsc::unbounded_channel();
    let stream = StreamClient::builder(client)
        .expect("builder")
        .processing_policy(StreamProcessingPolicy {
            max_concurrent_handlers: 2,
            queue_capacity: 1,
            ..StreamProcessingPolicy::default()
        })
        .on_frame(move |_ctx, event| {
            let gate = Arc::clone(&gate);
            let started = started.clone();
            async move {
                started
                    .send(event.message_id().to_owned())
                    .expect("started");
                gate.acquire().await.expect("gate").forget();
            }
        })
        .build()
        .expect("stream");
    let (stop, task) = launch(stream);
    let mut socket = accept(&listener).await;
    for id in ["one", "two"] {
        send(&mut socket, frame("EVENT", "topic", id)).await;
    }
    for _ in 0..2 {
        bounded(starts.recv()).await.expect("handler started");
    }
    for id in ["queued", "overflow"] {
        send(&mut socket, frame("EVENT", "topic", id)).await;
    }
    let rejected = ack(&mut socket).await;
    assert_eq!(rejected["headers"]["messageId"], "overflow");
    assert_eq!(rejected["code"], 500);
    send(&mut socket, frame("SYSTEM", "ping", "ping")).await;
    assert_eq!(ack(&mut socket).await["code"], 200);
    assert!(starts.try_recv().is_err());
    stop.send(()).expect("shutdown");
    permits.add_permits(3);
    let mut ids = Vec::new();
    for _ in 0..3 {
        let response = ack(&mut socket).await;
        assert_eq!(response["code"], 200);
        ids.push(
            response["headers"]["messageId"]
                .as_str()
                .expect("id")
                .to_owned(),
        );
    }
    ids.sort();
    assert_eq!(ids, ["one", "queued", "two"]);
    bounded(task).await.expect("task").expect("shutdown");
    bounded(http).await.expect("gateway task");
}

#[tokio::test]
async fn timeout_failure_and_panic_allow_retry_and_success_is_deduplicated() {
    for failure in ["timeout", "error", "panic"] {
        let (events, mut received) = mpsc::unbounded_channel();
        let (client, listener, http) = gateway(1).await;
        let calls = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&calls);
        let stream = StreamClient::builder(client)
            .expect("builder")
            .on_event(move |event| {
                let _ = events.send(event);
            })
            .processing_policy(StreamProcessingPolicy {
                handler_timeout: Duration::from_millis(100),
                ..StreamProcessingPolicy::default()
            })
            .on_frame(move |_ctx, _| {
                let first = count.fetch_add(1, Ordering::SeqCst) == 0;
                async move {
                    if first {
                        match failure {
                            "timeout" => std::future::pending::<()>().await,
                            "error" => return Err(Error::InvalidConfig("injected".into())),
                            _ => panic!("injected handler panic"),
                        }
                    }
                    Ok::<_, dingding::Error>(())
                }
            })
            .build()
            .expect("stream");
        let (stop, task) = launch(stream);
        let mut socket = accept(&listener).await;
        for code in [500, 200, 200] {
            send(&mut socket, frame("EVENT", "topic", "same-id")).await;
            assert_eq!(ack(&mut socket).await["code"], code);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        stop.send(()).expect("shutdown");
        bounded(task).await.expect("task").expect("shutdown");
        bounded(http).await.expect("gateway task");
        let mut cancellations = Vec::new();
        while let Ok(event) = received.try_recv() {
            if let StreamRunEvent::FrameCancelled { message_id, reason } = event {
                cancellations.push((message_id, reason));
            }
        }
        let expected = if failure == "timeout" {
            vec![(
                "same-id".to_owned(),
                StreamCancellationReason::HandlerTimeout,
            )]
        } else {
            Vec::new()
        };
        assert_eq!(cancellations, expected);
    }
}

#[tokio::test]
async fn server_disconnect_drains_accepted_work_and_preserves_results_for_redelivery() {
    for termination in ["disconnect", "close", "transport"] {
        let (client, listener, http) = gateway(2).await;
        let gate = Arc::new(Semaphore::new(0));
        let permits = Arc::clone(&gate);
        let calls = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&calls);
        let (started, mut starts) = mpsc::unbounded_channel();
        let (events, mut received) = mpsc::unbounded_channel();
        let stream = StreamClient::builder(client)
            .expect("builder")
            .on_event(move |event| {
                let _ = events.send(event);
            })
            .on_frame(move |_, frame| {
                let permits = Arc::clone(&permits);
                let started = started.clone();
                let count = Arc::clone(&count);
                async move {
                    started
                        .send(frame.message_id().to_owned())
                        .expect("started");
                    permits.acquire().await.expect("gate").forget();
                    count.fetch_add(1, Ordering::SeqCst);
                    StreamFrameResponse::json(json!({"saved":frame.message_id()}))
                }
            })
            .build()
            .expect("stream");
        let first = stream.clone();
        let task = tokio::spawn(async move { first.run_once().await });
        let mut socket = accept(&listener).await;
        send(&mut socket, frame("EVENT", "topic", "running")).await;
        bounded(starts.recv()).await.expect("started");
        send(&mut socket, frame("EVENT", "topic", "queued")).await;
        send(&mut socket, frame("SYSTEM", "ping", "accepted-barrier")).await;
        assert_eq!(
            ack(&mut socket).await["headers"]["messageId"],
            "accepted-barrier"
        );
        match termination {
            "disconnect" => {
                send(&mut socket, frame("SYSTEM", "disconnect", "disconnect")).await;
                assert_eq!(ack(&mut socket).await["headers"]["messageId"], "disconnect");
                send(&mut socket, frame("EVENT", "topic", "rejected")).await;
                assert_eq!(ack(&mut socket).await["code"], 500);
            }
            "close" => send(&mut socket, Message::Close(None)).await,
            _ => {
                socket
                    .get_mut()
                    .shutdown()
                    .await
                    .expect("transport shutdown");
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            !task.is_finished(),
            "{termination} must allow local completion"
        );
        gate.add_permits(2);
        let result = bounded(task).await.expect("task");
        if termination != "transport" {
            result.expect("graceful disconnect");
        } else {
            result.expect_err("transport failure");
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        while let Ok(event) = received.try_recv() {
            assert!(!matches!(event, StreamRunEvent::FrameCancelled { .. }));
        }
        let (stop, task) = launch(stream);
        let mut socket = accept(&listener).await;
        for id in ["running", "queued"] {
            send(&mut socket, frame("EVENT", "topic", id)).await;
            let response = ack(&mut socket).await;
            assert_eq!(response["code"], 200);
            let data: Value =
                serde_json::from_str(response["data"].as_str().expect("response")).expect("data");
            assert_eq!(data["response"]["saved"], id);
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "completed work must not run twice"
        );
        stop.send(()).expect("shutdown");
        bounded(task).await.expect("task").expect("shutdown");
        bounded(http).await.expect("gateway");
    }
}

#[tokio::test]
async fn disconnect_deadline_reports_running_and_queued_cancellations_and_releases_claims() {
    for termination in ["disconnect", "close", "transport"] {
        let (client, listener, http) = gateway(2).await;
        let (events, mut received) = mpsc::unbounded_channel();
        let calls = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&calls);
        let (started, mut starts) = mpsc::unbounded_channel();
        let stream = StreamClient::builder(client)
            .expect("builder")
            .processing_policy(StreamProcessingPolicy {
                disconnect_timeout: Duration::from_millis(60),
                ..StreamProcessingPolicy::default()
            })
            .on_event(move |event| {
                let _ = events.send(event);
            })
            .on_frame(move |_, _| {
                let first = count.fetch_add(1, Ordering::SeqCst) == 0;
                let started = started.clone();
                async move {
                    started.send(()).expect("started");
                    if first {
                        std::future::pending::<()>().await;
                    }
                }
            })
            .build()
            .expect("stream");
        let first = stream.clone();
        let task = tokio::spawn(async move { first.run_once().await });
        let mut socket = accept(&listener).await;
        send(&mut socket, frame("EVENT", "topic", "running")).await;
        bounded(starts.recv()).await.expect("started");
        send(&mut socket, frame("EVENT", "topic", "queued")).await;
        send(&mut socket, frame("SYSTEM", "ping", "barrier")).await;
        assert_eq!(ack(&mut socket).await["code"], 200);
        match termination {
            "disconnect" => {
                send(&mut socket, frame("SYSTEM", "disconnect", "disconnect")).await;
                assert_eq!(ack(&mut socket).await["code"], 200);
            }
            "close" => send(&mut socket, Message::Close(None)).await,
            _ => {
                socket
                    .get_mut()
                    .shutdown()
                    .await
                    .expect("transport shutdown");
            }
        }
        let error = bounded(task).await.expect("task").expect_err("deadline");
        assert!(error.to_string().contains("disconnect drain timed out"));
        let mut cancellations = Vec::new();
        while let Ok(event) = received.try_recv() {
            if let StreamRunEvent::FrameCancelled { message_id, reason } = event {
                assert_eq!(reason, StreamCancellationReason::DisconnectTimeout);
                cancellations.push(message_id);
            }
        }
        cancellations.sort();
        assert_eq!(cancellations, ["queued", "running"]);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "queued handler must not start before cancellation"
        );
        let (stop, task) = launch(stream);
        let mut socket = accept(&listener).await;
        send(&mut socket, frame("EVENT", "topic", "running")).await;
        assert_eq!(ack(&mut socket).await["code"], 200);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        stop.send(()).expect("shutdown");
        bounded(task).await.expect("task").expect("shutdown");
        bounded(http).await.expect("gateway");
    }
}

#[tokio::test]
async fn dropping_runner_reports_cancellation_for_running_and_queued_frames() {
    let (client, listener, http) = gateway(1).await;
    let (events, mut received) = mpsc::unbounded_channel();
    let stream = StreamClient::builder(client)
        .expect("builder")
        .on_event(move |event| {
            let _ = events.send(event);
        })
        .on_frame(|_, _| std::future::pending::<()>())
        .build()
        .expect("stream");
    let task = tokio::spawn(async move { stream.run_once().await });
    let mut socket = accept(&listener).await;
    for id in ["running", "queued"] {
        send(&mut socket, frame("EVENT", "topic", id)).await;
    }
    send(&mut socket, frame("SYSTEM", "ping", "barrier")).await;
    assert_eq!(ack(&mut socket).await["code"], 200);
    task.abort();
    assert!(bounded(task).await.expect_err("aborted").is_cancelled());
    let mut ids = Vec::new();
    while let Ok(event) = received.try_recv() {
        if let StreamRunEvent::FrameCancelled { message_id, reason } = event {
            assert_eq!(reason, StreamCancellationReason::RunnerDropped);
            ids.push(message_id);
        }
    }
    ids.sort();
    assert_eq!(ids, ["queued", "running"]);
    bounded(http).await.expect("gateway");
}

#[tokio::test]
async fn reconnect_events_separate_attempts_from_failures_and_reset_after_stability() {
    let (client, listener, http) = gateway(5).await;
    let (events, mut received) = mpsc::unbounded_channel();
    let stream = StreamClient::builder(client)
        .expect("builder")
        .reconnect_policy(
            ReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(8))
                .reset_after(Duration::from_millis(500)),
        )
        .on_event(move |event| {
            let _ = events.send(event);
        })
        .on_frame(|_, _| async {})
        .build()
        .expect("stream");
    let (stop, task) = launch(stream);
    for (index, failures) in [1, 2, 0, 1].into_iter().enumerate() {
        let mut socket = accept(&listener).await;
        if index == 2 {
            tokio::time::sleep(Duration::from_millis(600)).await;
        }
        send(&mut socket, Message::Close(None)).await;
        loop {
            if let StreamRunEvent::ReconnectScheduled {
                next_attempt,
                consecutive_failures,
                delay,
            } = bounded(received.recv()).await.expect("event")
            {
                assert_eq!(next_attempt, index as u32 + 2);
                assert_eq!(consecutive_failures, failures);
                assert_eq!(
                    delay,
                    Duration::from_millis(if failures == 2 { 2 } else { 1 })
                );
                break;
            }
        }
    }
    let _socket = accept(&listener).await;
    stop.send(()).expect("shutdown");
    bounded(task).await.expect("task").expect("shutdown");
    bounded(http).await.expect("gateway");
}

#[tokio::test]
async fn frame_error_events_preserve_redacted_structured_metadata() {
    let (client, listener, http) = gateway(1).await;
    let (events, mut received) = mpsc::unbounded_channel();
    let stream = StreamClient::builder(client)
        .expect("builder")
        .on_event(move |event| {
            let _ = events.send(event);
        })
        .on_frame(|_, _| async {
            Err::<(), _>(Error::Api {
                code: 130101,
                api_code: Some("TooManyRequests".into()),
                message: "access_token=application-secret".into(),
                request_id: Some("request-1".into()),
                error_body_snippet: Some("private-business-payload".into()),
                status: Some(429),
                retry_after: Some(Box::new(Duration::from_secs(7))),
            })
        })
        .build()
        .expect("stream");
    let (stop, task) = launch(stream);
    let mut socket = accept(&listener).await;
    send(&mut socket, frame("EVENT", "topic", "failure")).await;
    assert_eq!(ack(&mut socket).await["code"], 500);
    loop {
        if let StreamRunEvent::FrameError { message_id, error } =
            bounded(received.recv()).await.expect("event")
        {
            assert_eq!(message_id.as_deref(), Some("failure"));
            assert_eq!(error.kind(), dingding::ErrorKind::Api);
            assert_eq!(error.status(), Some(429));
            assert_eq!(error.errcode(), Some(130101));
            assert_eq!(error.api_code(), Some("TooManyRequests"));
            assert_eq!(error.request_id(), Some("request-1"));
            assert_eq!(error.retry_after(), Some(Duration::from_secs(7)));
            assert!(error.is_retryable());
            for text in [error.to_string(), format!("{error:?}")] {
                assert!(!text.contains("application-secret"));
                assert!(!text.contains("private-business-payload"));
            }
            break;
        }
    }
    stop.send(()).expect("shutdown");
    bounded(task).await.expect("task").expect("shutdown");
    bounded(http).await.expect("gateway");
}

#[tokio::test]
async fn concurrent_duplicate_is_not_executed_or_acknowledged_as_completed() {
    let (client, listener, http) = gateway(1).await;
    let permits = Arc::new(Semaphore::new(0));
    let gate = Arc::clone(&permits);
    let (started, mut starts) = mpsc::unbounded_channel();
    let stream = StreamClient::builder(client)
        .expect("builder")
        .processing_policy(StreamProcessingPolicy {
            max_concurrent_handlers: 2,
            ..StreamProcessingPolicy::default()
        })
        .on_frame(move |_ctx, _| {
            let gate = Arc::clone(&gate);
            let started = started.clone();
            async move {
                started.send(()).expect("started");
                gate.acquire().await.expect("gate").forget();
            }
        })
        .build()
        .expect("stream");
    let (stop, task) = launch(stream);
    let mut socket = accept(&listener).await;
    send(&mut socket, frame("EVENT", "topic", "same")).await;
    bounded(starts.recv()).await.expect("started");
    send(&mut socket, frame("EVENT", "topic", "same")).await;
    assert_eq!(ack(&mut socket).await["code"], 500);
    assert!(starts.try_recv().is_err());
    permits.add_permits(1);
    assert_eq!(ack(&mut socket).await["code"], 200);
    stop.send(()).expect("shutdown");
    bounded(task).await.expect("task").expect("shutdown");
    bounded(http).await.expect("gateway task");
}

#[tokio::test]
async fn card_ack_is_replayed_after_duplicate_delivery_and_reconnection() {
    let (client, listener, http) = gateway(2).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&calls);
    let stream = StreamClient::builder(client)
        .expect("builder")
        .reconnect_policy(ReconnectPolicy::new(
            Duration::from_millis(10),
            Duration::from_millis(20),
        ))
        .on_card_callback(move |_ctx, _| {
            count.fetch_add(1, Ordering::SeqCst);
            async {
                StreamFrameResponse::json(json!({"cardData":{"cardParamMap":{"title":"updated"}}}))
            }
        })
        .build()
        .expect("stream");
    let (stop, task) = launch(stream);
    let mut socket = accept(&listener).await;
    send(
        &mut socket,
        frame("CALLBACK", CARD_CALLBACK_TOPIC, "card-id"),
    )
    .await;
    let original = ack(&mut socket).await;
    assert_eq!(original["code"], 200);
    send(
        &mut socket,
        frame("CALLBACK", CARD_CALLBACK_TOPIC, "card-id"),
    )
    .await;
    assert_eq!(ack(&mut socket).await, original);
    send(&mut socket, frame("SYSTEM", "disconnect", "disconnect-id")).await;
    assert_eq!(
        ack(&mut socket).await["headers"]["messageId"],
        "disconnect-id"
    );
    let mut socket = accept(&listener).await;
    send(
        &mut socket,
        frame("CALLBACK", CARD_CALLBACK_TOPIC, "card-id"),
    )
    .await;
    assert_eq!(ack(&mut socket).await, original);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    stop.send(()).expect("shutdown");
    bounded(task).await.expect("task").expect("shutdown");
    bounded(http).await.expect("gateway task");
}

struct Cancelled(Arc<AtomicUsize>);
impl Drop for Cancelled {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn connection_loss_during_shutdown_reports_unfinished_work() {
    let (client, listener, http) = gateway(1).await;
    let cancelled = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&cancelled);
    let (started, mut starts) = mpsc::unbounded_channel();
    let stream = StreamClient::builder(client)
        .expect("builder")
        .processing_policy(StreamProcessingPolicy {
            disconnect_timeout: Duration::from_millis(100),
            ..StreamProcessingPolicy::default()
        })
        .on_frame(move |_ctx, _| {
            let guard = Cancelled(Arc::clone(&count));
            let started = started.clone();
            async move {
                let _guard = guard;
                started.send(()).expect("started");
                std::future::pending::<()>().await;
            }
        })
        .build()
        .expect("stream");
    let (stop, observed, task) = launch_with_observed_shutdown(stream);
    let mut socket = accept(&listener).await;
    send(&mut socket, frame("EVENT", "topic", "unfinished")).await;
    bounded(starts.recv()).await.expect("handler started");
    stop.send(()).expect("shutdown");
    bounded(observed).await.expect("shutdown observed");
    send(&mut socket, Message::Close(None)).await;
    let error = bounded(task)
        .await
        .expect("task")
        .expect_err("unfinished drain");
    assert!(error.to_string().contains("disconnect drain timed out"));
    assert_eq!(cancelled.load(Ordering::SeqCst), 1);
    bounded(http).await.expect("gateway task");
}

#[tokio::test]
async fn shutdown_deadline_cancels_unfinished_work_and_reports_failure() {
    let (client, listener, http) = gateway(1).await;
    let (events, mut received) = mpsc::unbounded_channel();
    let cancelled = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&cancelled);
    let (started, mut starts) = mpsc::unbounded_channel();
    let stream = StreamClient::builder(client)
        .expect("builder")
        .on_event(move |event| {
            let _ = events.send(event);
        })
        .processing_policy(StreamProcessingPolicy {
            shutdown_timeout: Duration::from_millis(100),
            ..StreamProcessingPolicy::default()
        })
        .on_frame(move |_ctx, _| {
            let guard = Cancelled(Arc::clone(&count));
            let started = started.clone();
            async move {
                let _guard = guard;
                started.send(()).expect("started");
                std::future::pending::<()>().await;
            }
        })
        .build()
        .expect("stream");
    let (stop, task) = launch(stream);
    let mut socket = accept(&listener).await;
    send(&mut socket, frame("EVENT", "topic", "unfinished")).await;
    bounded(starts.recv()).await.expect("handler started");
    stop.send(()).expect("shutdown");
    let error = bounded(task)
        .await
        .expect("task")
        .expect_err("drain timed out");
    assert!(error.to_string().contains("shutdown drain timed out"));
    assert_eq!(cancelled.load(Ordering::SeqCst), 1);
    let mut cancellations = Vec::new();
    while let Ok(event) = received.try_recv() {
        if let StreamRunEvent::FrameCancelled { message_id, reason } = event {
            cancellations.push((message_id, reason));
        }
    }
    assert_eq!(
        cancellations,
        [(
            "unfinished".into(),
            StreamCancellationReason::ShutdownTimeout
        )]
    );
    bounded(http).await.expect("gateway task");
}

#[tokio::test]
async fn buffered_control_bursts_do_not_starve_the_ack_writer() {
    let (client, listener, http) = gateway(1).await;
    let stream = StreamClient::builder(client)
        .expect("builder")
        .processing_policy(StreamProcessingPolicy {
            queue_capacity: 0,
            ..StreamProcessingPolicy::default()
        })
        .reconnect_policy(ReconnectPolicy::no_retry())
        .on_frame(|_ctx, _| async {})
        .build()
        .expect("stream");
    let (stop, task) = launch(stream);
    let mut socket = accept(&listener).await;
    for index in 0..256 {
        bounded(socket.feed(frame("SYSTEM", "ping", &index.to_string())))
            .await
            .expect("feed buffered frame");
    }
    bounded(socket.flush()).await.expect("flush burst");
    for index in 0..256 {
        let response = ack(&mut socket).await;
        assert_eq!(response["code"], 200);
        assert_eq!(response["headers"]["messageId"], index.to_string());
    }
    stop.send(()).expect("shutdown");
    bounded(task).await.expect("task").expect("shutdown");
    bounded(http).await.expect("gateway task");
}

#[tokio::test]
async fn already_signalled_shutdown_does_not_open_a_connection() {
    let stream = StreamClient::builder(
        DingTalk::builder()
            .app_key_and_secret("id", "secret")
            .openapi_base_url("http://127.0.0.1:1")
            .build()
            .expect("client"),
    )
    .expect("builder")
    .on_frame(|_ctx, _| async {})
    .build()
    .expect("stream");
    bounded(stream.run_until(async {}))
        .await
        .expect("shutdown before connect");
}
