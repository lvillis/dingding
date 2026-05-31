#![cfg(all(feature = "webhook", feature = "openapi"))]

use std::{
    error::Error,
    io::{self, Read, Write},
    net::TcpListener,
    sync::mpsc,
    thread,
    time::Duration,
};

use dingding::{
    DingTalk, ErrorKind,
    openapi::{MediaType, MediaUpload},
    webhook::At,
};
use serde_json::{Value, json};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn webhook_robot_posts_expected_query_and_json_body() -> TestResult<()> {
    let server = MockServer::spawn([MockResponse::json(
        r#"{"errcode":0,"errmsg":"ok","requestId":"req-webhook"}"#,
    )])?;
    let client = DingTalk::builder()
        .webhook_base_url(server.base_url())
        .system_proxy(false)
        .build()?;

    let response = client
        .webhook("webhook-token")
        .send_text_with_at("hello", At::new().user_id("user-1").mobile("13800000000"))
        .await?;

    assert_eq!(response.code(), 0);
    assert_eq!(response.request_id(), Some("req-webhook"));

    let request = server.next_request()?;
    assert_eq!(request.method, "POST");
    assert_eq!(request.path(), "/robot/send");
    assert_eq!(
        request.query_value("access_token").as_deref(),
        Some("webhook-token")
    );
    assert_header_contains(&request, "content-type", "application/json");

    let body = request.json_body()?;
    assert_eq!(body["msgtype"], "text");
    assert_eq!(body["text"]["content"], "hello");
    assert_eq!(body["at"]["atUserIds"], json!(["user-1"]));
    assert_eq!(body["at"]["atMobiles"], json!(["13800000000"]));
    assert_eq!(body["at"]["isAtAll"], false);

    server.finish()
}

#[tokio::test]
async fn signed_webhook_robot_encodes_signature_once() -> TestResult<()> {
    let server = MockServer::spawn([MockResponse::json(r#"{"errcode":0,"errmsg":"ok"}"#)])?;
    let client = DingTalk::builder()
        .webhook_base_url(server.base_url())
        .system_proxy(false)
        .build()?;

    client
        .webhook("webhook-token")
        .signing_secret("custom-secret")
        .send_text("hello")
        .await?;

    let request = server.next_request()?;
    let timestamp = request
        .query_value("timestamp")
        .ok_or("timestamp query should be present")?;
    let sign = request
        .query_value("sign")
        .ok_or("sign query should be present")?;

    assert!(!sign.contains('%'), "sign query was double-encoded: {sign}");
    assert!(!request.target.contains("%25"), "raw query contains %25");
    assert_eq!(sign, webhook_signature(&timestamp, "custom-secret")?);

    server.finish()
}

#[tokio::test]
async fn openapi_group_message_fetches_token_and_posts_with_access_token_header() -> TestResult<()>
{
    let server = MockServer::spawn([
        MockResponse::json(
            r#"{"errcode":0,"errmsg":"ok","access_token":"token-123","expires_in":7200}"#,
        ),
        MockResponse::json(r#"{"errcode":0,"errmsg":"ok","processQueryKey":"pq-123"}"#),
    ])?;
    let client = dingtalk_for_mock(&server)?;

    let response = client
        .openapi()
        .robot("robot-code")
        .send_group_text("open-cid", "hello from openapi")
        .await?;

    assert_eq!(
        response,
        r#"{"errcode":0,"errmsg":"ok","processQueryKey":"pq-123"}"#
    );

    let token_request = server.next_request()?;
    assert_eq!(token_request.method, "GET");
    assert_eq!(token_request.path(), "/gettoken");
    assert_eq!(
        token_request.query_value("appkey").as_deref(),
        Some("client-id")
    );
    assert_eq!(
        token_request.query_value("appsecret").as_deref(),
        Some("client-secret")
    );

    let send_request = server.next_request()?;
    assert_eq!(send_request.method, "POST");
    assert_eq!(send_request.path(), "/v1.0/robot/groupMessages/send");
    assert_eq!(
        send_request.header("x-acs-dingtalk-access-token"),
        Some("token-123")
    );

    let body = send_request.json_body()?;
    assert_eq!(body["robotCode"], "robot-code");
    assert_eq!(body["openConversationId"], "open-cid");
    assert_eq!(body["msgKey"], "sampleText");

    let msg_param = body["msgParam"]
        .as_str()
        .ok_or("msgParam should be a JSON object string")?;
    let msg_param: Value = serde_json::from_str(msg_param)?;
    assert_eq!(msg_param, json!({ "content": "hello from openapi" }));

    server.finish()
}

#[tokio::test]
async fn openapi_upload_media_posts_legacy_multipart_body() -> TestResult<()> {
    let server = MockServer::spawn([
        MockResponse::json(
            r#"{"errcode":0,"errmsg":"ok","access_token":"token-123","expires_in":7200}"#,
        ),
        MockResponse::json(
            r#"{"errcode":0,"errmsg":"ok","media_id":"media-123","type":"image","created_at":1700000000000}"#,
        ),
    ])?;
    let client = dingtalk_for_mock(&server)?;

    let uploaded = client
        .openapi()
        .upload_media(MediaUpload::image("demo.png", b"image-bytes").content_type("image/png"))
        .await?;

    assert_eq!(uploaded.media_id(), "media-123");
    assert_eq!(uploaded.media_type(), &MediaType::Image);
    assert_eq!(uploaded.created_at_millis(), Some(1_700_000_000_000));

    let token_request = server.next_request()?;
    assert_eq!(token_request.path(), "/gettoken");

    let upload_request = server.next_request()?;
    assert_eq!(upload_request.method, "POST");
    assert_eq!(upload_request.path(), "/media/upload");
    assert_eq!(
        upload_request.query_value("access_token").as_deref(),
        Some("token-123")
    );
    assert_eq!(upload_request.query_value("type").as_deref(), Some("image"));
    assert_header_contains(&upload_request, "content-type", "multipart/form-data");

    let body = upload_request.body_text_lossy();
    assert!(body.contains("name=\"type\""));
    assert!(body.contains("\r\n\r\nimage\r\n"));
    assert!(body.contains("name=\"media\"; filename=\"demo.png\""));
    assert!(body.contains("Content-Type: image/png"));
    assert!(body.contains("image-bytes"));

    server.finish()
}

#[tokio::test]
async fn openapi_business_error_preserves_request_id_and_body_snippet() -> TestResult<()> {
    let server = MockServer::spawn([
        MockResponse::json(
            r#"{"errcode":0,"errmsg":"ok","access_token":"token-123","expires_in":7200}"#,
        ),
        MockResponse::json(
            r#"{"errcode":40001,"errmsg":"invalid robot code","requestId":"req-openapi"}"#,
        ),
    ])?;
    let client = dingtalk_for_mock(&server)?;

    let error = client
        .openapi()
        .robot("robot-code")
        .send_group_text("open-cid", "hello")
        .await
        .err()
        .ok_or("request should fail")?;

    assert_eq!(error.kind(), ErrorKind::Api);
    assert_eq!(error.request_id(), Some("req-openapi"));
    assert!(
        error
            .error_body_snippet()
            .is_some_and(|body| body.contains("invalid robot code"))
    );

    let token_request = server.next_request()?;
    assert_eq!(token_request.path(), "/gettoken");

    let send_request = server.next_request()?;
    assert_eq!(send_request.path(), "/v1.0/robot/groupMessages/send");

    server.finish()
}

fn dingtalk_for_mock(server: &MockServer) -> dingding::Result<DingTalk> {
    DingTalk::builder()
        .webhook_base_url(server.base_url())
        .openapi_base_url(server.base_url())
        .app_key_and_secret("client-id", "client-secret")
        .system_proxy(false)
        .build()
}

fn assert_header_contains(request: &RecordedRequest, name: &str, expected: &str) {
    assert!(
        request
            .header(name)
            .is_some_and(|value| value.contains(expected)),
        "expected header `{name}` to contain `{expected}`, got {:?}",
        request.header(name)
    );
}

fn webhook_signature(timestamp: &str, secret: &str) -> TestResult<String> {
    use base64::{Engine, engine::general_purpose::STANDARD};
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;

    let string_to_sign = format!("{timestamp}\n{secret}");
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())?;
    mac.update(string_to_sign.as_bytes());
    Ok(STANDARD.encode(mac.finalize().into_bytes()))
}

struct MockServer {
    base_url: String,
    requests: mpsc::Receiver<RecordedRequest>,
    handle: Option<thread::JoinHandle<io::Result<()>>>,
}

impl MockServer {
    fn spawn<I>(responses: I) -> io::Result<Self>
    where
        I: IntoIterator<Item = MockResponse>,
    {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let base_url = format!("http://{}", listener.local_addr()?);
        let responses = responses.into_iter().collect::<Vec<_>>();
        let (sender, requests) = mpsc::channel();
        let handle = thread::spawn(move || serve(listener, sender, responses));

        Ok(Self {
            base_url,
            requests,
            handle: Some(handle),
        })
    }

    fn base_url(&self) -> &str {
        &self.base_url
    }

    fn next_request(&self) -> TestResult<RecordedRequest> {
        Ok(self.requests.recv_timeout(Duration::from_secs(5))?)
    }

    fn finish(mut self) -> TestResult<()> {
        let handle = self.handle.take().ok_or("mock server already finished")?;
        match handle.join() {
            Ok(result) => {
                result?;
                Ok(())
            }
            Err(_panic) => Err("mock server thread panicked".into()),
        }
    }
}

struct MockResponse {
    status: u16,
    reason: &'static str,
    content_type: &'static str,
    body: Vec<u8>,
}

impl MockResponse {
    fn json(body: &str) -> Self {
        Self {
            status: 200,
            reason: "OK",
            content_type: "application/json",
            body: body.as_bytes().to_vec(),
        }
    }
}

#[derive(Debug)]
struct RecordedRequest {
    method: String,
    target: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl RecordedRequest {
    fn path(&self) -> &str {
        self.target
            .split_once('?')
            .map(|(path, _query)| path)
            .unwrap_or(&self.target)
    }

    fn query_value(&self, name: &str) -> Option<String> {
        let (_path, query) = self.target.split_once('?')?;
        url::form_urlencoded::parse(query.as_bytes())
            .find(|(key, _value)| key == name)
            .map(|(_key, value)| value.into_owned())
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _value)| key.eq_ignore_ascii_case(name))
            .map(|(_key, value)| value.as_str())
    }

    fn json_body(&self) -> TestResult<Value> {
        Ok(serde_json::from_slice(&self.body)?)
    }

    fn body_text_lossy(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

fn serve(
    listener: TcpListener,
    sender: mpsc::Sender<RecordedRequest>,
    responses: Vec<MockResponse>,
) -> io::Result<()> {
    for response in responses {
        let (mut stream, _addr) = listener.accept()?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        let request = read_request(&mut stream)?;
        sender
            .send(request)
            .map_err(|_error| io::Error::new(io::ErrorKind::BrokenPipe, "receiver dropped"))?;
        write_response(&mut stream, response)?;
    }

    Ok(())
}

fn read_request(stream: &mut impl Read) -> io::Result<RecordedRequest> {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 1024];
    let header_end = loop {
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed before headers",
            ));
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(index) = find_subslice(&buffer, b"\r\n\r\n") {
            break index;
        }
    };

    let header_bytes = &buffer[..header_end];
    let header_text = std::str::from_utf8(header_bytes)
        .map_err(|source| io::Error::new(io::ErrorKind::InvalidData, source))?;
    let mut lines = header_text.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing request line"))?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing request method"))?
        .to_string();
    let target = request_parts
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing request target"))?
        .to_string();

    let headers = lines
        .filter_map(|line| {
            line.split_once(':')
                .map(|(name, value)| (name.trim().to_string(), value.trim().to_string()))
        })
        .collect::<Vec<_>>();
    let content_length = headers
        .iter()
        .find(|(name, _value)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_name, value)| value.parse::<usize>().ok())
        .unwrap_or(0);
    let body_start = header_end + b"\r\n\r\n".len();
    let mut body = buffer
        .get(body_start..)
        .map(ToOwned::to_owned)
        .unwrap_or_default();

    while body.len() < content_length {
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed before full body",
            ));
        }
        body.extend_from_slice(&chunk[..read]);
    }
    body.truncate(content_length);

    Ok(RecordedRequest {
        method,
        target,
        headers,
        body,
    })
}

fn write_response(stream: &mut impl Write, response: MockResponse) -> io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 {} {}\r\ncontent-type: {}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        response.status,
        response.reason,
        response.content_type,
        response.body.len()
    )?;
    stream.write_all(&response.body)?;
    stream.flush()
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}
