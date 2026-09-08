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
        CARD_CALLBACK_TOPIC, ReconnectPolicy, StreamClient, StreamFrameResponse,
        StreamProcessingPolicy,
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
    let (stop, shutdown) = oneshot::channel();
    let task = tokio::spawn(async move {
        stream
            .run_until(async {
                let _ = shutdown.await;
            })
            .await
    });
    (stop, task)
}

#[tokio::test]
async fn slow_handlers_preserve_heartbeats_order_and_graceful_shutdown() {
    let (client, listener, http) = gateway(1).await;
    let permits = Arc::new(Semaphore::new(0));
    let gate = Arc::clone(&permits);
    let (started, mut starts) = mpsc::unbounded_channel();
    let stream = StreamClient::builder(client)
        .expect("builder")
        .on_frame(move |frame| {
            let gate = Arc::clone(&gate);
            let started = started.clone();
            async move {
                started
                    .send(frame.message_id().to_owned())
                    .expect("started");
                gate.acquire().await.expect("gate").forget();
                Ok(())
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
        .on_frame(move |event| {
            let gate = Arc::clone(&gate);
            let started = started.clone();
            async move {
                started
                    .send(event.message_id().to_owned())
                    .expect("started");
                gate.acquire().await.expect("gate").forget();
                Ok(())
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
        let (client, listener, http) = gateway(1).await;
        let calls = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&calls);
        let stream = StreamClient::builder(client)
            .expect("builder")
            .processing_policy(StreamProcessingPolicy {
                handler_timeout: Duration::from_millis(100),
                ..StreamProcessingPolicy::default()
            })
            .on_frame(move |_| {
                let first = count.fetch_add(1, Ordering::SeqCst) == 0;
                async move {
                    if first {
                        match failure {
                            "timeout" => std::future::pending::<()>().await,
                            "error" => return Err(Error::InvalidConfig("injected".into())),
                            _ => panic!("injected handler panic"),
                        }
                    }
                    Ok(())
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
    }
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
        .on_frame(move |_| {
            let gate = Arc::clone(&gate);
            let started = started.clone();
            async move {
                started.send(()).expect("started");
                gate.acquire().await.expect("gate").forget();
                Ok(())
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
        .on_card_callback_with_response(move |_| {
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
async fn shutdown_deadline_cancels_unfinished_work_and_reports_failure() {
    let (client, listener, http) = gateway(1).await;
    let cancelled = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&cancelled);
    let (started, mut starts) = mpsc::unbounded_channel();
    let stream = StreamClient::builder(client)
        .expect("builder")
        .processing_policy(StreamProcessingPolicy {
            shutdown_timeout: Duration::from_millis(100),
            ..StreamProcessingPolicy::default()
        })
        .on_frame(move |_| {
            let guard = Cancelled(Arc::clone(&count));
            let started = started.clone();
            async move {
                let _guard = guard;
                started.send(()).expect("started");
                std::future::pending::<()>().await;
                Ok(())
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
        .on_frame(|_| async { Ok(()) })
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
    .on_frame(|_| async { Ok(()) })
    .build()
    .expect("stream");
    bounded(stream.run_until(async {}))
        .await
        .expect("shutdown before connect");
}
