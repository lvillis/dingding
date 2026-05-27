use std::{sync::Arc, time::Duration};

use reqx::{
    advanced::{ClientProfile, PermissiveRetryEligibility},
    prelude::{Client as HttpClient, RetryPolicy},
};
use serde::de::DeserializeOwned;
use url::Url;

use crate::{
    Error, Result,
    util::redact::{redact_text, truncate_snippet},
};

pub(crate) const DEFAULT_WEBHOOK_BASE_URL: &str = "https://oapi.dingtalk.com";
#[cfg(feature = "openapi")]
pub(crate) const DEFAULT_OPENAPI_BASE_URL: &str = "https://api.dingtalk.com";

/// Controls whether response snippets are retained on DingTalk API errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BodySnippetConfig {
    /// Enables body snippet capture.
    pub enabled: bool,
    /// Maximum retained bytes.
    pub max_bytes: usize,
}

impl Default for BodySnippetConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_bytes: 4096,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TransportConfig {
    pub(crate) client_name: String,
    pub(crate) profile: ClientProfile,
    pub(crate) request_timeout: Option<Duration>,
    pub(crate) total_timeout: Option<Duration>,
    pub(crate) connect_timeout: Duration,
    pub(crate) system_proxy: bool,
    pub(crate) retry_policy: Option<RetryPolicy>,
    pub(crate) retry_non_idempotent_requests: bool,
    pub(crate) default_headers: Vec<(String, String)>,
    pub(crate) error_body_snippet: BodySnippetConfig,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            client_name: concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"))
                .to_string(),
            profile: ClientProfile::StandardSdk,
            request_timeout: None,
            total_timeout: None,
            connect_timeout: Duration::from_secs(5),
            system_proxy: true,
            retry_policy: None,
            retry_non_idempotent_requests: false,
            default_headers: Vec::new(),
            error_body_snippet: BodySnippetConfig::default(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct Transport {
    webhook_http: HttpClient,
    #[cfg(feature = "openapi")]
    openapi_http: HttpClient,
    error_body_snippet: BodySnippetConfig,
}

impl Transport {
    pub(crate) fn new(
        webhook_base_url: &Url,
        openapi_base_url: Option<&Url>,
        config: &TransportConfig,
    ) -> Result<Self> {
        #[cfg(not(feature = "openapi"))]
        let _ = openapi_base_url;

        Ok(Self {
            webhook_http: build_http_client(webhook_base_url, config)?,
            #[cfg(feature = "openapi")]
            openapi_http: build_http_client(
                openapi_base_url.ok_or_else(|| {
                    Error::InvalidConfig("openapi base URL is required".to_string())
                })?,
                config,
            )?,
            error_body_snippet: config.error_body_snippet,
        })
    }

    #[cfg(feature = "webhook")]
    pub(crate) async fn post_webhook_json<T>(&self, url: &Url, body: &T) -> Result<reqx::Response>
    where
        T: serde::Serialize + ?Sized,
    {
        Ok(self
            .webhook_http
            .post(url.as_str())
            .json(body)?
            .send_response()
            .await?)
    }

    #[cfg(feature = "openapi")]
    pub(crate) async fn get_webhook(&self, url: &Url) -> Result<reqx::Response> {
        Ok(self.webhook_http.get(url.as_str()).send_response().await?)
    }

    #[cfg(feature = "openapi")]
    pub(crate) async fn get_url(&self, url: &Url) -> Result<reqx::Response> {
        Ok(self.webhook_http.get(url.as_str()).send_response().await?)
    }

    #[cfg(feature = "openapi")]
    pub(crate) async fn post_webhook_body(
        &self,
        url: &Url,
        content_type: &str,
        body: Vec<u8>,
    ) -> Result<reqx::Response> {
        Ok(self
            .webhook_http
            .post(url.as_str())
            .try_header("content-type", content_type)?
            .body(body)
            .send_response()
            .await?)
    }

    #[cfg(feature = "openapi")]
    pub(crate) async fn post_openapi_json<T>(
        &self,
        url: &Url,
        access_token: Option<&str>,
        body: &T,
    ) -> Result<reqx::Response>
    where
        T: serde::Serialize + ?Sized,
    {
        let mut request = self.openapi_http.post(url.as_str());
        if let Some(access_token) = access_token {
            request = request.try_header("x-acs-dingtalk-access-token", access_token)?;
        }

        Ok(request.json(body)?.send_response().await?)
    }

    #[cfg(feature = "openapi")]
    pub(crate) async fn put_openapi_json<T>(
        &self,
        url: &Url,
        access_token: Option<&str>,
        body: &T,
    ) -> Result<reqx::Response>
    where
        T: serde::Serialize + ?Sized,
    {
        let mut request = self.openapi_http.put(url.as_str());
        if let Some(access_token) = access_token {
            request = request.try_header("x-acs-dingtalk-access-token", access_token)?;
        }

        Ok(request.json(body)?.send_response().await?)
    }

    pub(crate) fn error_body_snippet(&self) -> BodySnippetConfig {
        self.error_body_snippet
    }
}

fn build_http_client(base_url: &Url, config: &TransportConfig) -> Result<HttpClient> {
    let mut builder = HttpClient::builder(base_url.as_str())
        .profile(config.profile)
        .client_name(config.client_name.clone())
        .connect_timeout(config.connect_timeout);

    if let Some(request_timeout) = config.request_timeout {
        builder = builder.request_timeout(request_timeout);
    }

    if let Some(total_timeout) = config.total_timeout {
        builder = builder.total_timeout(total_timeout);
    }

    if !config.system_proxy {
        builder = builder.no_proxy(["*"]);
    }

    if let Some(retry_policy) = &config.retry_policy {
        builder = builder.retry_policy(retry_policy.clone());
    }

    if config.retry_non_idempotent_requests {
        builder = builder.retry_eligibility(Arc::new(PermissiveRetryEligibility));
    }

    for (name, value) in &config.default_headers {
        builder = builder.try_default_header(name, value)?;
    }

    Ok(builder.build()?)
}

#[derive(Debug, serde::Deserialize)]
pub(crate) struct StandardApiResponse {
    pub(crate) errcode: Option<i64>,
    #[serde(alias = "message")]
    pub(crate) errmsg: Option<String>,
    #[serde(default, alias = "requestId", alias = "RequestId")]
    pub(crate) request_id: Option<String>,
}

pub(crate) fn decode_json_response<T>(
    response: reqx::Response,
    error_body_snippet: BodySnippetConfig,
) -> Result<(T, String)>
where
    T: DeserializeOwned,
{
    let body = successful_body(response, error_body_snippet)?;
    let value = serde_json::from_str(&body)?;
    Ok((value, body))
}

#[cfg(feature = "webhook")]
pub(crate) fn parse_standard_response(
    response: reqx::Response,
    error_body_snippet: BodySnippetConfig,
) -> Result<StandardApiResponse> {
    let (value, body) = decode_json_response::<StandardApiResponse>(response, error_body_snippet)?;
    if let Some(code) = value.errcode
        && code != 0
    {
        return Err(api_error_from_body(
            code,
            value
                .errmsg
                .clone()
                .unwrap_or_else(|| "unknown dingtalk api error".to_string()),
            value.request_id.clone(),
            &body,
            error_body_snippet,
        ));
    }
    Ok(value)
}

#[cfg(feature = "openapi")]
pub(crate) fn parse_standard_text_response(
    response: reqx::Response,
    error_body_snippet: BodySnippetConfig,
) -> Result<String> {
    let body = successful_body(response, error_body_snippet)?;
    if let Ok(value) = serde_json::from_str::<StandardApiResponse>(&body)
        && let Some(code) = value.errcode
        && code != 0
    {
        return Err(api_error_from_body(
            code,
            value
                .errmsg
                .unwrap_or_else(|| "unknown dingtalk api error".to_string()),
            value.request_id,
            &body,
            error_body_snippet,
        ));
    }

    Ok(body)
}

#[cfg(feature = "openapi")]
pub(crate) fn parse_dingtalk_result<T>(
    response: reqx::Response,
    error_body_snippet: BodySnippetConfig,
) -> Result<T>
where
    T: DeserializeOwned,
{
    let (value, body) = decode_json_response::<DingTalkResult<T>>(response, error_body_snippet)?;
    if value.errcode != 0 {
        return Err(api_error_from_body(
            value.errcode,
            value.errmsg,
            value.request_id,
            &body,
            error_body_snippet,
        ));
    }

    value.result.ok_or_else(|| {
        api_error_from_body(
            -1,
            "missing result field in DingTalk response",
            None,
            &body,
            error_body_snippet,
        )
    })
}

#[cfg(feature = "openapi")]
pub(crate) fn parse_binary_response(
    response: reqx::Response,
    error_body_snippet: BodySnippetConfig,
) -> Result<Vec<u8>> {
    let status = response.status().as_u16();
    let request_id = response_request_id(&response);

    if !(200..=299).contains(&status) {
        let body = response.text_lossy();
        let message = serde_json::from_str::<StandardApiResponse>(&body)
            .ok()
            .and_then(|parsed| parsed.errmsg);
        return Err(Error::api(
            status.into(),
            message.unwrap_or_else(|| format!("HTTP {status}")),
            request_id,
            body_snippet_for_error(&body, error_body_snippet),
        ));
    }

    Ok(response.body().to_vec())
}

#[cfg(feature = "openapi")]
#[derive(Debug, serde::Deserialize)]
struct DingTalkResult<T> {
    errcode: i64,
    #[serde(alias = "message")]
    errmsg: String,
    result: Option<T>,
    #[serde(default, alias = "requestId", alias = "RequestId")]
    request_id: Option<String>,
}

fn successful_body(
    response: reqx::Response,
    error_body_snippet: BodySnippetConfig,
) -> Result<String> {
    let status = response.status().as_u16();
    let request_id = response_request_id(&response);
    let body = response.text_lossy();

    if !(200..=299).contains(&status) {
        let message = serde_json::from_str::<StandardApiResponse>(&body)
            .ok()
            .and_then(|parsed| parsed.errmsg);
        return Err(Error::api(
            status.into(),
            message.unwrap_or_else(|| format!("HTTP {status}")),
            request_id,
            body_snippet_for_error(&body, error_body_snippet),
        ));
    }

    Ok(body)
}

pub(crate) fn api_error_from_body(
    code: i64,
    message: impl Into<String>,
    request_id: Option<String>,
    body: &str,
    config: BodySnippetConfig,
) -> Error {
    Error::api(
        code,
        message,
        request_id,
        body_snippet_for_error(body, config),
    )
}

fn response_request_id(response: &reqx::Response) -> Option<String> {
    response
        .headers()
        .get("x-request-id")
        .or_else(|| response.headers().get("x-acs-request-id"))
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

fn body_snippet_for_error(body: &str, config: BodySnippetConfig) -> Option<String> {
    if !config.enabled {
        return None;
    }

    let snippet = truncate_snippet(body, config.max_bytes);
    Some(redact_text(&snippet))
}
