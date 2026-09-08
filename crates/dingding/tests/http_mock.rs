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
        .webhook("webhook-token")?
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
        .webhook("webhook-token")?
        .signing_secret("custom-secret")?
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
async fn webhook_robot_rejects_response_without_errcode() -> TestResult<()> {
    let server = MockServer::spawn([MockResponse::json("{}")])?;
    let client = DingTalk::builder()
        .webhook_base_url(server.base_url())
        .system_proxy(false)
        .build()?;

    let error = client
        .webhook("webhook-token")?
        .send_text("hello")
        .await
        .err()
        .ok_or("missing errcode should fail")?;

    assert_eq!(error.kind(), ErrorKind::Api);
    assert!(
        error
            .error_body_snippet()
            .is_some_and(|snippet| snippet == "{}")
    );

    let request = server.next_request()?;
    assert_eq!(request.path(), "/robot/send");

    server.finish()
}

#[tokio::test]
async fn webhook_robot_rejects_invalid_json_response_with_body_snippet() -> TestResult<()> {
    let server = MockServer::spawn([MockResponse::json("not-json")])?;
    let client = DingTalk::builder()
        .webhook_base_url(server.base_url())
        .system_proxy(false)
        .build()?;

    let error = client
        .webhook("webhook-token")?
        .send_text("hello")
        .await
        .err()
        .ok_or("invalid JSON response should fail")?;

    assert_eq!(error.kind(), ErrorKind::Api);
    assert!(error.to_string().contains("invalid DingTalk JSON response"));
    assert!(
        error
            .error_body_snippet()
            .is_some_and(|snippet| snippet.contains("not-json"))
    );

    let request = server.next_request()?;
    assert_eq!(request.path(), "/robot/send");

    server.finish()
}

#[tokio::test]
async fn webhook_robot_rejects_modern_code_error_even_with_zero_errcode() -> TestResult<()> {
    let server = MockServer::spawn([MockResponse::json(
        r#"{"errcode":0,"code":"InvalidParameter","message":"bad webhook","requestId":"req-webhook"}"#,
    )])?;
    let client = DingTalk::builder()
        .webhook_base_url(server.base_url())
        .system_proxy(false)
        .build()?;

    let error = client
        .webhook("webhook-token")?
        .send_text("hello")
        .await
        .err()
        .ok_or("modern code error should fail")?;

    assert_eq!(error.kind(), ErrorKind::Api);
    assert_eq!(error.errcode(), Some(-1));
    assert_eq!(error.api_code(), Some("InvalidParameter"));
    assert_eq!(error.request_id(), Some("req-webhook"));
    assert!(error.to_string().contains("bad webhook"));

    let request = server.next_request()?;
    assert_eq!(request.path(), "/robot/send");

    server.finish()
}

#[tokio::test]
async fn webhook_robot_rejects_success_false_response() -> TestResult<()> {
    let server = MockServer::spawn([MockResponse::json(
        r#"{"errcode":0,"success":false,"errmsg":"denied","requestId":"req-webhook"}"#,
    )])?;
    let client = DingTalk::builder()
        .webhook_base_url(server.base_url())
        .system_proxy(false)
        .build()?;

    let error = client
        .webhook("webhook-token")?
        .send_text("hello")
        .await
        .err()
        .ok_or("success=false response should fail")?;

    assert_eq!(error.kind(), ErrorKind::Api);
    assert_eq!(error.errcode(), Some(-1));
    assert_eq!(error.request_id(), Some("req-webhook"));
    assert!(error.to_string().contains("denied"));

    let request = server.next_request()?;
    assert_eq!(request.path(), "/robot/send");

    server.finish()
}

#[tokio::test]
async fn openapi_group_message_fetches_token_and_posts_with_access_token_header() -> TestResult<()>
{
    let server = MockServer::spawn([
        MockResponse::json(r#"{"errcode":"0","access_token":" token-123 ","expires_in":"7200"}"#),
        MockResponse::json(r#"{"errcode":"0","errmsg":"ok","processQueryKey":"pq-123"}"#),
    ])?;
    let client = dingtalk_for_mock(&server)?;

    let response = client
        .openapi()
        .robot("robot-code")?
        .send_group_text("open-cid", "hello from openapi")
        .await?;

    assert_eq!(response.process_query_key(), "pq-123");
    assert_eq!(response.raw()["processQueryKey"], "pq-123");

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
async fn openapi_robot_message_rejects_missing_process_query_key() -> TestResult<()> {
    let server = MockServer::spawn([
        MockResponse::json(
            r#"{"errcode":0,"errmsg":"ok","access_token":"token-123","expires_in":7200}"#,
        ),
        MockResponse::json(r#"{"errcode":0,"requestId":12345,"result":{}}"#),
    ])?;
    let client = dingtalk_for_mock(&server)?;

    let error = client
        .openapi()
        .robot("robot-code")?
        .send_group_text("open-cid", "hello")
        .await
        .err()
        .ok_or("missing processQueryKey should fail")?;

    assert_eq!(error.kind(), ErrorKind::Api);
    assert_eq!(error.request_id(), Some("12345"));
    assert!(
        error
            .error_body_snippet()
            .is_some_and(|snippet| snippet.contains("requestId"))
    );

    let token_request = server.next_request()?;
    assert_eq!(token_request.path(), "/gettoken");

    let send_request = server.next_request()?;
    assert_eq!(send_request.path(), "/v1.0/robot/groupMessages/send");

    server.finish()
}

#[tokio::test]
async fn openapi_raw_send_rejects_success_false_response() -> TestResult<()> {
    let server = MockServer::spawn([
        MockResponse::json(
            r#"{"errcode":0,"errmsg":"ok","access_token":"token-123","expires_in":7200}"#,
        ),
        MockResponse::json(
            r#"{"success":false,"errorMessage":"denied","requestId":"req-success-false","result":{"processQueryKey":"pq-123"}}"#,
        ),
    ])?;
    let client = dingtalk_for_mock(&server)?;

    let error = client
        .openapi()
        .robot("robot-code")?
        .send_group_text("open-cid", "hello")
        .await
        .err()
        .ok_or("success=false standard response should fail")?;

    assert_eq!(error.kind(), ErrorKind::Api);
    assert_eq!(error.errcode(), Some(-1));
    assert_eq!(error.request_id(), Some("req-success-false"));
    assert!(error.to_string().contains("denied"));

    let token_request = server.next_request()?;
    assert_eq!(token_request.path(), "/gettoken");

    let send_request = server.next_request()?;
    assert_eq!(send_request.path(), "/v1.0/robot/groupMessages/send");

    server.finish()
}

#[tokio::test]
async fn openapi_rejects_blank_access_token() -> TestResult<()> {
    let server = MockServer::spawn([MockResponse::json(
        r#"{"errcode":0,"access_token":"  ","expires_in":7200,"requestId":"req-token"}"#,
    )])?;
    let client = dingtalk_for_mock(&server)?;

    let error = client
        .openapi()
        .access_token()
        .await
        .err()
        .ok_or("blank access_token should fail")?;

    assert_eq!(error.kind(), ErrorKind::Api);
    assert_eq!(error.request_id(), Some("req-token"));
    assert!(
        error
            .error_body_snippet()
            .is_some_and(|snippet| snippet.contains("access_token"))
    );

    let token_request = server.next_request()?;
    assert_eq!(token_request.path(), "/gettoken");

    server.finish()
}

#[tokio::test]
async fn openapi_access_token_rejects_success_false_response() -> TestResult<()> {
    let server = MockServer::spawn([MockResponse::json(
        r#"{"errcode":0,"success":false,"errorMessage":"denied","access_token":"token-123","expires_in":7200,"requestId":"req-token-false"}"#,
    )])?;
    let client = dingtalk_for_mock(&server)?;

    let error = client
        .openapi()
        .access_token()
        .await
        .err()
        .ok_or("success=false token response should fail")?;

    assert_eq!(error.kind(), ErrorKind::Api);
    assert_eq!(error.errcode(), Some(-1));
    assert_eq!(error.request_id(), Some("req-token-false"));
    assert!(error.to_string().contains("denied"));

    let token_request = server.next_request()?;
    assert_eq!(token_request.path(), "/gettoken");

    server.finish()
}

#[tokio::test]
async fn openapi_result_allows_success_without_errmsg() -> TestResult<()> {
    let server = MockServer::spawn([
        MockResponse::json(
            r#"{"errcode":0,"errmsg":"ok","access_token":"token-123","expires_in":7200}"#,
        ),
        MockResponse::json(r#"{"errcode":0,"result":{"accepted":true}}"#),
    ])?;
    let client = dingtalk_for_mock(&server)?;

    let result = client
        .openapi()
        .post_json_result::<Value, _>(&["v1.0", "custom", "endpoint"], &json!({ "ping": true }))
        .await?;

    assert_eq!(result, json!({ "accepted": true }));

    let token_request = server.next_request()?;
    assert_eq!(token_request.path(), "/gettoken");

    let api_request = server.next_request()?;
    assert_eq!(api_request.method, "POST");
    assert_eq!(api_request.path(), "/v1.0/custom/endpoint");

    server.finish()
}

#[tokio::test]
async fn openapi_result_allows_result_without_errcode() -> TestResult<()> {
    let server = MockServer::spawn([
        MockResponse::json(
            r#"{"errcode":0,"errmsg":"ok","access_token":"token-123","expires_in":7200}"#,
        ),
        MockResponse::json(r#"{"requestId":"req-result","result":{"accepted":true}}"#),
    ])?;
    let client = dingtalk_for_mock(&server)?;

    let result = client
        .openapi()
        .post_json_result::<Value, _>(&["v1.0", "custom", "endpoint"], &json!({ "ping": true }))
        .await?;

    assert_eq!(result, json!({ "accepted": true }));

    let token_request = server.next_request()?;
    assert_eq!(token_request.path(), "/gettoken");

    let api_request = server.next_request()?;
    assert_eq!(api_request.method, "POST");
    assert_eq!(api_request.path(), "/v1.0/custom/endpoint");

    server.finish()
}

#[tokio::test]
async fn openapi_result_rejects_success_false_response() -> TestResult<()> {
    let server = MockServer::spawn([
        MockResponse::json(
            r#"{"errcode":0,"errmsg":"ok","access_token":"token-123","expires_in":7200}"#,
        ),
        MockResponse::json(
            r#"{"success":false,"message":"denied","requestId":"req-result-false","result":{"accepted":true}}"#,
        ),
    ])?;
    let client = dingtalk_for_mock(&server)?;

    let error = client
        .openapi()
        .post_json_result::<Value, _>(&["v1.0", "custom", "endpoint"], &json!({ "ping": true }))
        .await
        .err()
        .ok_or("success=false result response should fail")?;

    assert_eq!(error.kind(), ErrorKind::Api);
    assert_eq!(error.errcode(), Some(-1));
    assert_eq!(error.request_id(), Some("req-result-false"));
    assert!(error.to_string().contains("denied"));

    let token_request = server.next_request()?;
    assert_eq!(token_request.path(), "/gettoken");

    let api_request = server.next_request()?;
    assert_eq!(api_request.method, "POST");
    assert_eq!(api_request.path(), "/v1.0/custom/endpoint");

    server.finish()
}

#[tokio::test]
async fn openapi_missing_result_preserves_request_id() -> TestResult<()> {
    let server = MockServer::spawn([
        MockResponse::json(
            r#"{"errcode":0,"errmsg":"ok","access_token":"token-123","expires_in":7200}"#,
        ),
        MockResponse::json(r#"{"errcode":0,"requestId":"req-missing-result"}"#),
    ])?;
    let client = dingtalk_for_mock(&server)?;

    let error = client
        .openapi()
        .post_json_result::<Value, _>(&["v1.0", "custom", "endpoint"], &json!({ "ping": true }))
        .await
        .err()
        .ok_or("missing result should fail")?;

    assert_eq!(error.kind(), ErrorKind::Api);
    assert_eq!(error.request_id(), Some("req-missing-result"));

    let token_request = server.next_request()?;
    assert_eq!(token_request.path(), "/gettoken");

    let api_request = server.next_request()?;
    assert_eq!(api_request.method, "POST");
    assert_eq!(api_request.path(), "/v1.0/custom/endpoint");

    server.finish()
}

#[tokio::test]
async fn openapi_raw_send_rejects_empty_standard_response() -> TestResult<()> {
    let server = MockServer::spawn([
        MockResponse::json(
            r#"{"errcode":0,"errmsg":"ok","access_token":"token-123","expires_in":7200}"#,
        ),
        MockResponse::json(r#"{"requestId":"req-empty-response"}"#),
    ])?;
    let client = dingtalk_for_mock(&server)?;

    let error = client
        .openapi()
        .robot("robot-code")?
        .send_group_text("open-cid", "hello")
        .await
        .err()
        .ok_or("empty standard response should fail")?;

    assert_eq!(error.kind(), ErrorKind::Api);
    assert_eq!(error.request_id(), Some("req-empty-response"));
    assert!(
        error
            .error_body_snippet()
            .is_some_and(|snippet| snippet.contains("requestId"))
    );

    let token_request = server.next_request()?;
    assert_eq!(token_request.path(), "/gettoken");

    let send_request = server.next_request()?;
    assert_eq!(send_request.path(), "/v1.0/robot/groupMessages/send");

    server.finish()
}

#[tokio::test]
async fn openapi_raw_send_rejects_non_json_standard_response() -> TestResult<()> {
    let server = MockServer::spawn([
        MockResponse::json(
            r#"{"errcode":0,"errmsg":"ok","access_token":"token-123","expires_in":7200}"#,
        ),
        MockResponse::json("temporary upstream failure"),
    ])?;
    let client = dingtalk_for_mock(&server)?;

    let error = client
        .openapi()
        .robot("robot-code")?
        .send_group_text("open-cid", "hello")
        .await
        .err()
        .ok_or("non-json standard response should fail")?;

    assert_eq!(error.kind(), ErrorKind::Api);
    assert!(error.to_string().contains("invalid DingTalk JSON response"));
    assert!(
        error
            .error_body_snippet()
            .is_some_and(|snippet| snippet.contains("temporary upstream failure"))
    );

    let token_request = server.next_request()?;
    assert_eq!(token_request.path(), "/gettoken");

    let send_request = server.next_request()?;
    assert_eq!(send_request.path(), "/v1.0/robot/groupMessages/send");

    server.finish()
}

#[tokio::test]
async fn openapi_raw_send_preserves_modern_code_error_response() -> TestResult<()> {
    let server = MockServer::spawn([
        MockResponse::json(
            r#"{"errcode":0,"errmsg":"ok","access_token":"token-123","expires_in":7200}"#,
        ),
        MockResponse::json(
            r#"{"code":"InvalidParameter","message":"bad robotCode","requestid":"req-modern"}"#,
        ),
    ])?;
    let client = dingtalk_for_mock(&server)?;

    let error = client
        .openapi()
        .robot("robot-code")?
        .send_group_text("open-cid", "hello")
        .await
        .err()
        .ok_or("modern code error should fail")?;

    assert_eq!(error.kind(), ErrorKind::Api);
    assert_eq!(error.errcode(), Some(-1));
    assert_eq!(error.api_code(), Some("InvalidParameter"));
    assert_eq!(error.request_id(), Some("req-modern"));
    assert!(error.to_string().contains("InvalidParameter"));
    assert!(error.to_string().contains("bad robotCode"));

    let token_request = server.next_request()?;
    assert_eq!(token_request.path(), "/gettoken");

    let send_request = server.next_request()?;
    assert_eq!(send_request.path(), "/v1.0/robot/groupMessages/send");

    server.finish()
}

#[tokio::test]
async fn openapi_upload_media_posts_legacy_multipart_body() -> TestResult<()> {
    let server = MockServer::spawn([
        MockResponse::json(
            r#"{"errcode":0,"errmsg":"ok","access_token":"token-123","expires_in":7200}"#,
        ),
        MockResponse::json(
            r#"{"errcode":"0","errmsg":"ok","media_id":" media-123 ","type":" image ","created_at":"1700000000000"}"#,
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
async fn openapi_concurrent_requests_share_token_refresh() -> TestResult<()> {
    let server = MockServer::spawn([
        MockResponse::json(
            r#"{"errcode":0,"errmsg":"ok","access_token":"token-123","expires_in":7200}"#,
        ),
        MockResponse::json(r#"{"errcode":0,"errmsg":"ok","processQueryKey":"pq-1"}"#),
        MockResponse::json(r#"{"errcode":0,"errmsg":"ok","processQueryKey":"pq-2"}"#),
    ])?;
    let client = dingtalk_for_mock(&server)?;
    let robot = client.openapi().robot("robot-code")?;

    let (first, second) = tokio::join!(
        robot.send_group_text("open-cid-1", "first"),
        robot.send_group_text("open-cid-2", "second")
    );
    let mut keys = [
        first?.process_query_key().to_string(),
        second?.process_query_key().to_string(),
    ];
    keys.sort();

    assert_eq!(keys, ["pq-1".to_string(), "pq-2".to_string()]);

    let token_request = server.next_request()?;
    assert_eq!(token_request.path(), "/gettoken");

    let first_send = server.next_request()?;
    let second_send = server.next_request()?;
    assert_eq!(first_send.path(), "/v1.0/robot/groupMessages/send");
    assert_eq!(second_send.path(), "/v1.0/robot/groupMessages/send");
    assert_eq!(
        first_send.header("x-acs-dingtalk-access-token"),
        Some("token-123")
    );
    assert_eq!(
        second_send.header("x-acs-dingtalk-access-token"),
        Some("token-123")
    );

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
        .robot("robot-code")?
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

#[tokio::test]
async fn openapi_upload_media_rejects_success_false_response() -> TestResult<()> {
    let server = MockServer::spawn([
        MockResponse::json(
            r#"{"errcode":0,"errmsg":"ok","access_token":"token-123","expires_in":7200}"#,
        ),
        MockResponse::json(
            r#"{"success":false,"error_message":"denied","requestId":"req-media-false","media_id":"media-123","type":"image"}"#,
        ),
    ])?;
    let client = dingtalk_for_mock(&server)?;

    let error = client
        .openapi()
        .upload_media(MediaUpload::image("demo.png", b"image-bytes"))
        .await
        .err()
        .ok_or("success=false media upload should fail")?;

    assert_eq!(error.kind(), ErrorKind::Api);
    assert_eq!(error.errcode(), Some(-1));
    assert_eq!(error.request_id(), Some("req-media-false"));
    assert!(error.to_string().contains("denied"));

    let token_request = server.next_request()?;
    assert_eq!(token_request.path(), "/gettoken");

    let upload_request = server.next_request()?;
    assert_eq!(upload_request.path(), "/media/upload");

    server.finish()
}

#[tokio::test]
async fn openapi_download_message_file_fetches_temporary_url_bytes() -> TestResult<()> {
    let download_server = MockServer::spawn([MockResponse::bytes(
        "application/octet-stream",
        b"downloaded-file-bytes",
    )])?;
    let download_url = format!(
        "{}/files/report.bin?ticket=temporary",
        download_server.base_url()
    );
    let download_response = json!({
        "errcode": 0,
        "result": {
            "downloadUrl": download_url,
        },
        "requestId": "req-download-url",
    })
    .to_string();
    let api_server = MockServer::spawn([
        MockResponse::json(
            r#"{"errcode":0,"errmsg":"ok","access_token":"token-123","expires_in":7200}"#,
        ),
        MockResponse::json(&download_response),
    ])?;
    let client = dingtalk_for_mock(&api_server)?;

    let downloaded = client
        .openapi()
        .robot("robot-code")?
        .download_message_file("download-code")
        .await?;

    assert_eq!(downloaded.download_url(), download_url);
    assert_eq!(downloaded.content_type(), Some("application/octet-stream"));
    assert_eq!(downloaded.bytes(), b"downloaded-file-bytes");

    let token_request = api_server.next_request()?;
    assert_eq!(token_request.path(), "/gettoken");

    let download_url_request = api_server.next_request()?;
    assert_eq!(download_url_request.method, "POST");
    assert_eq!(
        download_url_request.path(),
        "/v1.0/robot/messageFiles/download"
    );
    let body = download_url_request.json_body()?;
    assert_eq!(body["robotCode"], "robot-code");
    assert_eq!(body["downloadCode"], "download-code");

    let file_request = download_server.next_request()?;
    assert_eq!(file_request.method, "GET");
    assert_eq!(file_request.path(), "/files/report.bin");
    assert_eq!(
        file_request.query_value("ticket").as_deref(),
        Some("temporary")
    );

    api_server.finish()?;
    download_server.finish()
}

#[tokio::test]
async fn openapi_download_message_file_rejects_json_error_body() -> TestResult<()> {
    let download_server = MockServer::spawn([MockResponse::json(
        r#"{"errcode":40001,"errmsg":"download denied","requestId":"req-file"}"#,
    )])?;
    let download_url = format!(
        "{}/files/report.bin?ticket=temporary",
        download_server.base_url()
    );
    let download_response = json!({
        "errcode": 0,
        "result": {
            "downloadUrl": download_url,
        },
        "requestId": "req-download-url",
    })
    .to_string();
    let api_server = MockServer::spawn([
        MockResponse::json(
            r#"{"errcode":0,"errmsg":"ok","access_token":"token-123","expires_in":7200}"#,
        ),
        MockResponse::json(&download_response),
    ])?;
    let client = dingtalk_for_mock(&api_server)?;

    let error = client
        .openapi()
        .robot("robot-code")?
        .download_message_file("download-code")
        .await
        .err()
        .ok_or("json file error should fail")?;

    assert_eq!(error.kind(), ErrorKind::Api);
    assert_eq!(error.errcode(), Some(40001));
    assert_eq!(error.request_id(), Some("req-file"));
    assert!(error.to_string().contains("download denied"));

    let token_request = api_server.next_request()?;
    assert_eq!(token_request.path(), "/gettoken");
    let download_url_request = api_server.next_request()?;
    assert_eq!(
        download_url_request.path(),
        "/v1.0/robot/messageFiles/download"
    );
    let file_request = download_server.next_request()?;
    assert_eq!(file_request.path(), "/files/report.bin");

    api_server.finish()?;
    download_server.finish()
}

#[tokio::test]
async fn openapi_http_error_uses_body_request_id_when_header_is_missing() -> TestResult<()> {
    let server = MockServer::spawn([
        MockResponse::json(
            r#"{"errcode":0,"errmsg":"ok","access_token":"token-123","expires_in":7200}"#,
        ),
        MockResponse::json_status(
            429,
            "Too Many Requests",
            r#"{"errcode":429,"errmsg":"too many requests","requestId":"req-body"}"#,
        ),
    ])?;
    let client = dingtalk_for_mock(&server)?;

    let error = client
        .openapi()
        .robot("robot-code")?
        .send_group_text("open-cid", "hello")
        .await
        .err()
        .ok_or("request should fail")?;

    assert_eq!(error.kind(), ErrorKind::Api);
    assert_eq!(error.request_id(), Some("req-body"));
    assert!(
        error
            .error_body_snippet()
            .is_some_and(|snippet| snippet.contains("too many requests"))
    );

    let token_request = server.next_request()?;
    assert_eq!(token_request.path(), "/gettoken");

    let send_request = server.next_request()?;
    assert_eq!(send_request.path(), "/v1.0/robot/groupMessages/send");

    server.finish()
}

#[tokio::test]
async fn rejected_tokens_are_refreshed_once_before_replaying_the_same_request() -> TestResult<()> {
    for rejection in [
        MockResponse::json(r#"{"errcode":40014,"errmsg":"invalid access token"}"#),
        MockResponse::json_status(
            401,
            "Unauthorized",
            r#"{"errcode":40014,"errmsg":"invalid access token"}"#,
        ),
        MockResponse::json_status(
            401,
            "Unauthorized",
            r#"{"code":"InvalidAuthentication","message":"invalid access token"}"#,
        ),
    ] {
        let server = MockServer::spawn([
            MockResponse::json(r#"{"errcode":0,"access_token":"old-token","expires_in":7200}"#),
            rejection,
            MockResponse::json(r#"{"errcode":0,"access_token":"new-token","expires_in":7200}"#),
            MockResponse::json(r#"{"processQueryKey":"pq"}"#),
        ])?;
        let robot = dingtalk_for_mock(&server)?.openapi().robot("robot-code")?;
        assert_eq!(
            robot
                .send_group_text("cid", "hello")
                .await?
                .process_query_key(),
            "pq"
        );
        assert_eq!(server.next_request()?.path(), "/gettoken");
        let original = server.next_request()?;
        assert_eq!(server.next_request()?.path(), "/gettoken");
        let retry = server.next_request()?;
        assert_eq!(
            original.header("x-acs-dingtalk-access-token"),
            Some("old-token")
        );
        assert_eq!(
            retry.header("x-acs-dingtalk-access-token"),
            Some("new-token")
        );
        assert_eq!(original.json_body()?, retry.json_body()?);
        server.finish()?;
    }
    Ok(())
}

#[tokio::test]
async fn repeated_token_rejection_stops_and_invalidates_the_rejected_refresh() -> TestResult<()> {
    let server = MockServer::spawn([
        MockResponse::json(r#"{"errcode":0,"access_token":"old-token","expires_in":7200}"#),
        MockResponse::json(r#"{"errcode":42001,"errmsg":"expired"}"#),
        MockResponse::json(r#"{"errcode":0,"access_token":"new-token","expires_in":7200}"#),
        MockResponse::json(
            r#"{"errcode":40014,"errmsg":"still invalid","requestId":"last-error"}"#,
        ),
        MockResponse::json(r#"{"errcode":0,"access_token":"fresh-token","expires_in":7200}"#),
    ])?;
    let api = dingtalk_for_mock(&server)?.openapi();
    let error = api
        .post_json_result::<Value, _>(&["test"], &json!({}))
        .await
        .err()
        .ok_or("retry must stop")?;
    assert_eq!(error.errcode(), Some(40014));
    assert_eq!(error.request_id(), Some("last-error"));
    assert_eq!(api.access_token().await?, "fresh-token");
    for path in ["/gettoken", "/test", "/gettoken", "/test", "/gettoken"] {
        assert_eq!(server.next_request()?.path(), path);
    }
    server.finish()
}

#[tokio::test]
async fn permission_errors_do_not_refresh_or_resend() -> TestResult<()> {
    let server = MockServer::spawn([
        MockResponse::json(r#"{"errcode":0,"access_token":"token","expires_in":7200}"#),
        MockResponse::json_status(
            403,
            "Forbidden",
            r#"{"code":"Forbidden.AccessDenied","message":"permission denied"}"#,
        ),
    ])?;
    let api = dingtalk_for_mock(&server)?.openapi();
    assert_eq!(
        api.robot("robot-code")?
            .send_group_text("cid", "hello")
            .await
            .err()
            .ok_or("permission denied")?
            .api_code(),
        Some("Forbidden.AccessDenied")
    );
    assert_eq!(api.access_token().await?, "token");
    assert_eq!(server.next_request()?.path(), "/gettoken");
    assert_eq!(
        server.next_request()?.path(),
        "/v1.0/robot/groupMessages/send"
    );
    server.finish()
}

#[tokio::test]
async fn media_upload_and_card_update_recover_rejected_tokens() -> TestResult<()> {
    for upload in [true, false] {
        let success = if upload {
            r#"{"errcode":0,"media_id":"media-1","type":"file"}"#
        } else {
            r#"{"processQueryKey":"pq"}"#
        };
        let server = MockServer::spawn([
            MockResponse::json(r#"{"errcode":0,"access_token":"old","expires_in":7200}"#),
            MockResponse::json(r#"{"errcode":40014,"errmsg":"invalid token"}"#),
            MockResponse::json(r#"{"errcode":0,"access_token":"new","expires_in":7200}"#),
            MockResponse::json(success),
        ])?;
        let api = dingtalk_for_mock(&server)?.openapi();
        if upload {
            assert_eq!(
                api.upload_media(MediaUpload::file("test.txt", b"test".to_vec()))
                    .await?
                    .media_id(),
                "media-1"
            );
        } else {
            let update = dingding::openapi::InteractiveCardUpdate::card_data(
                "card",
                json!({"title":"updated"}),
            )?;
            api.robot("robot-code")?
                .update_interactive_card(update)
                .await?;
        }
        server.next_request()?;
        let original = server.next_request()?;
        server.next_request()?;
        let retry = server.next_request()?;
        if upload {
            assert_eq!(original.query_value("access_token").as_deref(), Some("old"));
            assert_eq!(retry.query_value("access_token").as_deref(), Some("new"));
            assert!(retry.body_text_lossy().contains("test"));
        } else {
            assert_eq!(retry.method, "PUT");
            assert_eq!(original.json_body()?, retry.json_body()?);
            assert_eq!(retry.header("x-acs-dingtalk-access-token"), Some("new"));
        }
        server.finish()?;
    }
    Ok(())
}

#[tokio::test]
async fn message_lifecycle_uses_documented_endpoints_and_preserves_partial_results()
-> TestResult<()> {
    use dingding::openapi::GroupMessageQuery;
    let server = MockServer::spawn([
        MockResponse::json(r#"{"errcode":0,"access_token":"token","expires_in":7200}"#),
        MockResponse::json(
            r#"{"sendStatus":"SUCCESS","hasMore":true,"nextToken":"next+/=","readUserIds":["u1"],"readUsers":[{"userId":"u1","unionId":"union1"}]}"#,
        ),
        MockResponse::json(r#"{"sendStatus":"SUCCESS","hasMore":false,"readUserIds":["u2"]}"#),
        MockResponse::json(
            r#"{"sendStatus":"SUCESS","messageReadInfoList":[{"userId":"u1","name":"reader","readStatus":"READ","readTimestamp":123}]}"#,
        ),
        MockResponse::json(r#"{"successResult":["pq1"],"failedResult":{"pq2":"SYSTEM_ERROR"}}"#),
        MockResponse::json(r#"{"successResult":["pq3"],"failedResult":{}}"#),
    ])?;
    let robot = dingtalk_for_mock(&server)?.openapi().robot("robot-code")?;
    let first = robot
        .query_group_message("cid", GroupMessageQuery::new("pq1")?.max_results(50)?)
        .await?;
    assert!(first.has_more);
    assert_eq!(first.read_users[0].user_id.as_deref(), Some("u1"));
    let second = robot
        .query_group_message(
            "cid",
            GroupMessageQuery::new("pq1")?.next_token(first.next_token.ok_or("missing cursor")?)?,
        )
        .await?;
    assert!(!second.has_more);
    let private = robot.query_private_message("pq+/=").await?;
    assert_eq!(private.message_read_info_list[0].read_timestamp, Some(123));
    let recall = robot
        .recall_group_messages("cid", ["pq1", "pq2", "pq1"])
        .await?;
    assert!(!recall.is_success());
    assert_eq!(recall.failed_result["pq2"], "SYSTEM_ERROR");
    assert!(robot.recall_private_messages(["pq3"]).await?.is_success());
    server.next_request()?;
    let first = server.next_request()?;
    assert_eq!(first.path(), "/v1.0/robot/groupMessages/query");
    assert_eq!(
        first.json_body()?,
        json!({"robotCode":"robot-code","openConversationId":"cid","processQueryKey":"pq1","maxResults":50})
    );
    assert_eq!(server.next_request()?.json_body()?["nextToken"], "next+/=");
    let private = server.next_request()?;
    assert_eq!(private.method, "GET");
    assert_eq!(private.path(), "/v1.0/robot/oToMessages/readStatus");
    assert_eq!(
        private.query_value("processQueryKey").as_deref(),
        Some("pq+/=")
    );
    assert_eq!(private.header("x-acs-dingtalk-access-token"), Some("token"));
    let group_recall = server.next_request()?;
    assert_eq!(group_recall.path(), "/v1.0/robot/groupMessages/recall");
    assert_eq!(
        group_recall.json_body()?["processQueryKeys"],
        json!(["pq1", "pq2"])
    );
    let private_recall = server.next_request()?;
    assert_eq!(private_recall.path(), "/v1.0/robot/otoMessages/batchRecall");
    assert!(
        private_recall
            .json_body()?
            .get("openConversationId")
            .is_none()
    );
    server.finish()
}

#[tokio::test]
async fn private_status_get_refreshes_invalid_tokens_and_rejects_empty_payloads() -> TestResult<()>
{
    let server = MockServer::spawn([
        MockResponse::json(r#"{"errcode":0,"access_token":"old","expires_in":7200}"#),
        MockResponse::json_status(
            401,
            "Unauthorized",
            r#"{"code":"InvalidAuthentication","message":"invalid"}"#,
        ),
        MockResponse::json(r#"{"errcode":0,"access_token":"new","expires_in":7200}"#),
        MockResponse::json(r#"{"sendStatus":"SUCCESS","messageReadInfoList":[]}"#),
        MockResponse::json(r#"{"requestId":"malformed"}"#),
    ])?;
    let robot = dingtalk_for_mock(&server)?.openapi().robot("robot")?;
    assert_eq!(
        robot.query_private_message("pq").await?.send_status,
        "SUCCESS"
    );
    assert_eq!(
        robot
            .query_private_message("pq")
            .await
            .err()
            .ok_or("missing status")?
            .request_id(),
        Some("malformed")
    );
    for _ in 0..5 {
        server.next_request()?;
    }
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

    fn json_status(status: u16, reason: &'static str, body: &str) -> Self {
        Self {
            status,
            reason,
            content_type: "application/json",
            body: body.as_bytes().to_vec(),
        }
    }

    fn bytes(content_type: &'static str, body: &[u8]) -> Self {
        Self {
            status: 200,
            reason: "OK",
            content_type,
            body: body.to_vec(),
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
