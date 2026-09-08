use std::{fmt, sync::Arc, time::Duration};

use reqx::{
    advanced::{ClientProfile, PermissiveRetryEligibility},
    prelude::{Client as HttpClient, RetryPolicy},
};
use serde::{
    Deserialize,
    de::{DeserializeOwned, Error as DeError},
};
use serde_json::Value;
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

#[derive(Clone)]
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

impl fmt::Debug for TransportConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let default_headers = self
            .default_headers
            .iter()
            .map(|(name, _value)| (name.as_str(), "<redacted>"))
            .collect::<Vec<_>>();

        f.debug_struct("TransportConfig")
            .field("client_name", &self.client_name)
            .field("profile", &self.profile)
            .field("request_timeout", &self.request_timeout)
            .field("total_timeout", &self.total_timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("system_proxy", &self.system_proxy)
            .field("retry_policy", &self.retry_policy)
            .field(
                "retry_non_idempotent_requests",
                &self.retry_non_idempotent_requests,
            )
            .field("default_headers", &default_headers)
            .field("error_body_snippet", &self.error_body_snippet)
            .finish()
    }
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

impl TransportConfig {
    fn validate(&self) -> Result<()> {
        validate_client_name(&self.client_name)?;
        validate_duration("connect_timeout", self.connect_timeout)?;
        if let Some(request_timeout) = self.request_timeout {
            validate_duration("request_timeout", request_timeout)?;
        }
        if let Some(total_timeout) = self.total_timeout {
            validate_duration("total_timeout", total_timeout)?;
        }
        for (index, (name, value)) in self.default_headers.iter().enumerate() {
            validate_default_header(index, name, value)?;
        }
        Ok(())
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

        config.validate()?;

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
    pub(crate) async fn get_openapi(
        &self,
        url: &Url,
        access_token: &str,
    ) -> Result<reqx::Response> {
        Ok(self
            .openapi_http
            .get(url.as_str())
            .try_header("x-acs-dingtalk-access-token", access_token)?
            .send_response()
            .await?)
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

fn validate_client_name(value: &str) -> Result<()> {
    if value.chars().any(char::is_control) {
        return Err(Error::InvalidConfig(
            "client_name must not contain control characters".to_string(),
        ));
    }
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(Error::InvalidConfig(
            "client_name must not be empty".to_string(),
        ));
    }
    if trimmed != value {
        return Err(Error::InvalidConfig(
            "client_name must not contain leading or trailing whitespace".to_string(),
        ));
    }
    Ok(())
}

fn validate_duration(field: &'static str, value: Duration) -> Result<()> {
    if value.is_zero() {
        return Err(Error::InvalidConfig(format!(
            "{field} must be greater than zero"
        )));
    }
    Ok(())
}

fn validate_default_header(index: usize, name: &str, value: &str) -> Result<()> {
    validate_header_name(index, name)?;
    validate_header_value(index, value)?;
    Ok(())
}

fn validate_header_name(index: usize, value: &str) -> Result<()> {
    if value.is_empty() {
        return Err(Error::InvalidConfig(format!(
            "default_headers[{index}].name must not be empty"
        )));
    }
    if !value.bytes().all(is_header_name_byte) {
        return Err(Error::InvalidConfig(format!(
            "default_headers[{index}].name must be a valid HTTP header name"
        )));
    }
    Ok(())
}

fn is_header_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

fn validate_header_value(index: usize, value: &str) -> Result<()> {
    if value
        .bytes()
        .any(|byte| byte.is_ascii_control() && byte != b'\t')
    {
        return Err(Error::InvalidConfig(format!(
            "default_headers[{index}].value must not contain control characters"
        )));
    }
    Ok(())
}

#[derive(Debug, serde::Deserialize)]
pub(crate) struct StandardApiResponse {
    #[serde(default, deserialize_with = "deserialize_optional_i64")]
    pub(crate) errcode: Option<i64>,
    #[serde(
        rename = "code",
        default,
        alias = "Code",
        deserialize_with = "deserialize_optional_string"
    )]
    pub(crate) api_code: Option<String>,
    #[serde(
        default,
        alias = "message",
        alias = "errorMessage",
        alias = "ErrorMessage",
        alias = "error_message",
        deserialize_with = "deserialize_optional_string"
    )]
    pub(crate) errmsg: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_bool")]
    pub(crate) success: Option<bool>,
    #[serde(
        default,
        alias = "requestId",
        alias = "RequestId",
        alias = "requestid",
        deserialize_with = "deserialize_optional_string"
    )]
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
    let value = serde_json::from_str(&body).map_err(|source| {
        api_error_from_body(
            -1,
            format!("invalid DingTalk JSON response: {source}"),
            None,
            &body,
            error_body_snippet,
        )
    })?;
    Ok((value, body))
}

#[cfg(feature = "webhook")]
pub(crate) fn parse_standard_response(
    response: reqx::Response,
    error_body_snippet: BodySnippetConfig,
) -> Result<StandardApiResponse> {
    let (value, body) = decode_json_response::<StandardApiResponse>(response, error_body_snippet)?;
    if let Some(error) = response_envelope_error(
        value.errcode,
        value.api_code.as_deref(),
        value.errmsg.as_deref(),
        value.success,
        value.request_id.as_deref(),
        &body,
        error_body_snippet,
    ) {
        return Err(error);
    }
    match value.errcode {
        Some(0) => Ok(value),
        Some(_) => unreachable!("non-zero errcode should be handled before this match"),
        None => Err(api_error_from_body(
            -1,
            response_error_message(
                value.errmsg.clone(),
                "missing errcode field in DingTalk response",
            ),
            value.request_id.clone(),
            &body,
            error_body_snippet,
        )),
    }
}

#[cfg(feature = "openapi")]
pub(crate) fn parse_standard_text_response(
    response: reqx::Response,
    error_body_snippet: BodySnippetConfig,
) -> Result<String> {
    let body = successful_body(response, error_body_snippet)?;
    let raw = serde_json::from_str::<Value>(&body).map_err(|source| {
        api_error_from_body(
            -1,
            format!("invalid DingTalk JSON response: {source}"),
            None,
            &body,
            error_body_snippet,
        )
    })?;
    let Value::Object(object) = &raw else {
        return Err(api_error_from_body(
            -1,
            "DingTalk response must be a JSON object",
            None,
            &body,
            error_body_snippet,
        ));
    };
    let has_success_payload = standard_text_object_has_success_payload(object);

    let value = serde_json::from_value::<StandardApiResponse>(raw).map_err(|source| {
        api_error_from_body(
            -1,
            format!("invalid DingTalk JSON response: {source}"),
            None,
            &body,
            error_body_snippet,
        )
    })?;
    if let Some(error) = response_envelope_error(
        value.errcode,
        value.api_code.as_deref(),
        value.errmsg.as_deref(),
        value.success,
        value.request_id.as_deref(),
        &body,
        error_body_snippet,
    ) {
        return Err(error);
    }
    if value.errcode.is_none() && !has_success_payload {
        return Err(api_error_from_body(
            -1,
            response_error_message(value.errmsg, "missing errcode field in DingTalk response"),
            value.request_id,
            &body,
            error_body_snippet,
        ));
    }

    Ok(body)
}

#[cfg(feature = "openapi")]
fn standard_text_object_has_success_payload(value: &serde_json::Map<String, Value>) -> bool {
    value.contains_key("result")
        || value.contains_key("processQueryKey")
        || value.contains_key("process_query_key")
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
    if let Some(error) = response_envelope_error(
        value.errcode,
        value.api_code.as_deref(),
        value.errmsg.as_deref(),
        value.success,
        value.request_id.as_deref(),
        &body,
        error_body_snippet,
    ) {
        return Err(error);
    }

    value.result.ok_or_else(|| {
        api_error_from_body(
            -1,
            "missing result field in DingTalk response",
            value.request_id,
            &body,
            error_body_snippet,
        )
    })
}

#[cfg(feature = "openapi")]
pub(crate) fn parse_openapi_response<T: DeserializeOwned>(
    response: reqx::Response,
    config: BodySnippetConfig,
) -> Result<T> {
    let (envelope, body) = decode_json_response::<StandardApiResponse>(response, config)?;
    if let Some(error) = response_envelope_error(
        envelope.errcode,
        envelope.api_code.as_deref(),
        envelope.errmsg.as_deref(),
        envelope.success,
        envelope.request_id.as_deref(),
        &body,
        config,
    ) {
        return Err(error);
    }
    serde_json::from_str(&body).map_err(|error| {
        api_error_from_body(
            -1,
            format!("invalid DingTalk response payload: {error}"),
            envelope.request_id,
            &body,
            config,
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
        let parsed = serde_json::from_str::<StandardApiResponse>(&body).ok();
        let message = parsed
            .as_ref()
            .map(|parsed| response_error_message(parsed.errmsg.clone(), &format!("HTTP {status}")));
        let request_id =
            request_id.or_else(|| parsed.as_ref().and_then(|parsed| parsed.request_id.clone()));
        return Err(api_error_from_body_with_code(
            status.into(),
            parsed.as_ref().and_then(|parsed| parsed.api_code.clone()),
            message.unwrap_or_else(|| format!("HTTP {status}")),
            request_id,
            &body,
            error_body_snippet,
        ));
    }

    let content_type_is_json = response_content_type_is_json(&response);
    let body = response.body().to_vec();
    if let Some(error) = binary_success_body_error(&body, content_type_is_json, error_body_snippet)
    {
        return Err(error);
    }

    Ok(body)
}

pub(crate) fn response_envelope_error(
    errcode: Option<i64>,
    api_code: Option<&str>,
    errmsg: Option<&str>,
    success: Option<bool>,
    request_id: Option<&str>,
    body: &str,
    error_body_snippet: BodySnippetConfig,
) -> Option<Error> {
    let (code, fallback) = if success == Some(false) {
        (
            errcode.filter(|code| *code != 0).unwrap_or(-1),
            "DingTalk response success=false",
        )
    } else if let Some(code) = errcode.filter(|code| *code != 0) {
        (code, "unknown dingtalk api error")
    } else if api_code.is_some_and(|api_code| !is_success_api_code(api_code)) {
        (-1, "unknown dingtalk api error")
    } else {
        return None;
    };

    Some(api_error_from_body_with_code(
        code,
        api_code.map(ToOwned::to_owned),
        response_error_message(errmsg.map(ToOwned::to_owned), fallback),
        request_id.map(ToOwned::to_owned),
        body,
        error_body_snippet,
    ))
}

#[cfg(feature = "openapi")]
fn response_content_type_is_json(response: &reqx::Response) -> bool {
    response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.to_ascii_lowercase().contains("json"))
}

#[cfg(feature = "openapi")]
fn binary_success_body_error(
    body: &[u8],
    content_type_is_json: bool,
    error_body_snippet: BodySnippetConfig,
) -> Option<Error> {
    let text = std::str::from_utf8(body).ok()?;
    if !content_type_is_json && !text.trim_start().starts_with('{') {
        return None;
    }
    let parsed = serde_json::from_str::<StandardApiResponse>(text).ok()?;
    response_envelope_error(
        parsed.errcode,
        parsed.api_code.as_deref(),
        parsed.errmsg.as_deref(),
        parsed.success,
        parsed.request_id.as_deref(),
        text,
        error_body_snippet,
    )
}

#[cfg(feature = "openapi")]
#[derive(serde::Deserialize)]
struct DingTalkResult<T> {
    #[serde(default, deserialize_with = "deserialize_optional_i64")]
    errcode: Option<i64>,
    #[serde(
        rename = "code",
        default,
        alias = "Code",
        deserialize_with = "deserialize_optional_string"
    )]
    api_code: Option<String>,
    #[serde(
        default,
        alias = "message",
        alias = "errorMessage",
        alias = "ErrorMessage",
        alias = "error_message",
        deserialize_with = "deserialize_optional_string"
    )]
    errmsg: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_bool")]
    success: Option<bool>,
    result: Option<T>,
    #[serde(
        default,
        alias = "requestId",
        alias = "RequestId",
        alias = "requestid",
        deserialize_with = "deserialize_optional_string"
    )]
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
        let parsed = serde_json::from_str::<StandardApiResponse>(&body).ok();
        let message = parsed
            .as_ref()
            .map(|parsed| response_error_message(parsed.errmsg.clone(), &format!("HTTP {status}")));
        let request_id =
            request_id.or_else(|| parsed.as_ref().and_then(|parsed| parsed.request_id.clone()));
        return Err(api_error_from_body_with_code(
            parsed
                .as_ref()
                .and_then(|parsed| parsed.errcode)
                .filter(|code| *code != 0)
                .unwrap_or(status.into()),
            parsed.as_ref().and_then(|parsed| parsed.api_code.clone()),
            message.unwrap_or_else(|| format!("HTTP {status}")),
            request_id,
            &body,
            error_body_snippet,
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
    api_error_from_body_with_code(code, None, message, request_id, body, config)
}

pub(crate) fn api_error_from_body_with_code(
    code: i64,
    api_code: Option<String>,
    message: impl Into<String>,
    request_id: Option<String>,
    body: &str,
    config: BodySnippetConfig,
) -> Error {
    Error::api_with_code(
        code,
        normalize_optional_response_string(api_code),
        normalize_error_message(message),
        normalize_optional_response_string(request_id),
        body_snippet_for_error(body, config),
    )
}

fn normalize_error_message(message: impl Into<String>) -> String {
    let message = message.into();
    let message = message.trim();
    if message.is_empty() {
        "unknown dingtalk api error".to_string()
    } else {
        redact_text(message)
    }
}

pub(crate) fn response_error_message(message: Option<String>, fallback: &str) -> String {
    normalize_error_message(message.unwrap_or_else(|| fallback.to_string()))
}

pub(crate) fn is_success_api_code(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "0" | "ok" | "success"
    )
}

fn response_request_id(response: &reqx::Response) -> Option<String> {
    response
        .headers()
        .get("x-request-id")
        .or_else(|| response.headers().get("x-acs-request-id"))
        .and_then(|value| value.to_str().ok())
        .and_then(normalize_response_string)
}

fn normalize_optional_response_string(value: Option<String>) -> Option<String> {
    value.and_then(|value| normalize_response_string(&value))
}

fn normalize_response_string(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

pub(crate) fn deserialize_optional_i64<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<i64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;
    let Some(value) = value else {
        return Ok(None);
    };

    match value {
        Value::Null => Ok(None),
        Value::Number(value) => value
            .as_i64()
            .ok_or_else(|| DeError::custom("expected signed integer"))
            .map(Some),
        Value::String(value) => {
            let value = value.trim();
            if value.is_empty() {
                Ok(None)
            } else {
                value
                    .parse::<i64>()
                    .map(Some)
                    .map_err(|source| DeError::custom(format!("expected signed integer: {source}")))
            }
        }
        _ => Err(DeError::custom("expected signed integer or string integer")),
    }
}

pub(crate) fn deserialize_optional_string<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;
    let Some(value) = value else {
        return Ok(None);
    };

    let value = match value {
        Value::Null => return Ok(None),
        Value::String(value) => value,
        Value::Number(value) => value.to_string(),
        _ => return Err(DeError::custom("expected string or number")),
    };

    Ok(normalize_response_string(&value))
}

pub(crate) fn deserialize_optional_bool<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<bool>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;
    let Some(value) = value else {
        return Ok(None);
    };

    match value {
        Value::Null => Ok(None),
        Value::Bool(value) => Ok(Some(value)),
        Value::Number(value) => match value.as_i64() {
            Some(0) => Ok(Some(false)),
            Some(1) => Ok(Some(true)),
            Some(_) | None => Err(DeError::custom("expected boolean or 0/1")),
        },
        Value::String(value) => {
            let value = value.trim();
            if value.is_empty() {
                return Ok(None);
            }
            match value.to_ascii_lowercase().as_str() {
                "false" | "0" => Ok(Some(false)),
                "true" | "1" => Ok(Some(true)),
                _ => Err(DeError::custom("expected boolean string")),
            }
        }
        _ => Err(DeError::custom("expected boolean, 0/1, or string boolean")),
    }
}

#[cfg(feature = "stream")]
pub(crate) fn deserialize_string<'de, D>(deserializer: D) -> std::result::Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    let value = match value {
        Value::String(value) => value,
        Value::Number(value) => value.to_string(),
        _ => return Err(DeError::custom("expected string or number")),
    };

    Ok(value.trim().to_string())
}

fn body_snippet_for_error(body: &str, config: BodySnippetConfig) -> Option<String> {
    if !config.enabled || config.max_bytes == 0 {
        return None;
    }

    let snippet = truncate_snippet(body, config.max_bytes);
    Some(redact_text(&snippet))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_config_rejects_invalid_default_headers() {
        let mut invalid_name = TransportConfig::default();
        invalid_name
            .default_headers
            .push(("bad header".to_string(), "value".to_string()));
        let mut invalid_value = TransportConfig::default();
        invalid_value
            .default_headers
            .push(("x-test".to_string(), "value\r\nx-injected: 1".to_string()));

        assert_eq!(
            invalid_name
                .validate()
                .expect_err("header name with space should fail")
                .kind(),
            crate::ErrorKind::InvalidConfig
        );
        assert_eq!(
            invalid_value
                .validate()
                .expect_err("header value with CRLF should fail")
                .kind(),
            crate::ErrorKind::InvalidConfig
        );
    }

    #[test]
    fn transport_config_debug_redacts_default_header_values() {
        let mut config = TransportConfig::default();
        config.default_headers.push((
            "authorization".to_string(),
            "Bearer header-secret".to_string(),
        ));

        let debug = format!("{config:?}");

        assert!(debug.contains("authorization"));
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("header-secret"));
    }

    #[test]
    fn api_error_from_body_normalizes_message_and_request_id() {
        let error = api_error_from_body(
            40001,
            "  ",
            Some(" request-1 ".to_string()),
            r#"{"errcode":40001,"errmsg":"  ","access_token":"secret-token"}"#,
            BodySnippetConfig::default(),
        );

        assert_eq!(error.kind(), crate::ErrorKind::Api);
        assert_eq!(error.request_id(), Some("request-1"));
        assert!(error.to_string().contains("unknown dingtalk api error"));
        assert!(error.error_body_snippet().is_some_and(
            |snippet| snippet.contains("<redacted>") && !snippet.contains("secret-token")
        ));
    }

    #[test]
    fn api_error_from_body_preserves_modern_api_code() {
        let error = api_error_from_body_with_code(
            -1,
            Some(" InvalidParameter ".to_string()),
            " bad request ",
            Some(" request-1 ".to_string()),
            r#"{"code":"InvalidParameter","message":"bad request"}"#,
            BodySnippetConfig::default(),
        );

        assert_eq!(error.errcode(), Some(-1));
        assert_eq!(error.api_code(), Some("InvalidParameter"));
        assert_eq!(error.request_id(), Some("request-1"));
        assert!(error.to_string().contains("api_code=InvalidParameter"));
        assert!(error.to_string().contains("bad request"));
    }

    #[test]
    fn api_error_from_body_redacts_sensitive_error_message_values() {
        let error = api_error_from_body(
            40001,
            "invalid access_token=secret-token, signature=callback-sign",
            None,
            r#"{"errcode":40001,"errmsg":"invalid"}"#,
            BodySnippetConfig::default(),
        );

        let message = error.to_string();
        assert!(message.contains("access_token=<redacted>"));
        assert!(message.contains("signature=<redacted>"));
        assert!(!message.contains("secret-token"));
        assert!(!message.contains("callback-sign"));
    }

    #[test]
    fn standard_api_response_accepts_string_error_codes() {
        let parsed = serde_json::from_str::<StandardApiResponse>(
            r#"{"errcode":"40001","errmsg":"invalid","requestId":" request-1 "}"#,
        )
        .expect("response");

        assert_eq!(parsed.errcode, Some(40001));
        assert_eq!(parsed.request_id.as_deref(), Some("request-1"));
    }

    #[test]
    fn standard_api_response_accepts_numeric_request_id() {
        let parsed =
            serde_json::from_str::<StandardApiResponse>(r#"{"errcode":0,"requestId":12345}"#)
                .expect("response");

        assert_eq!(parsed.request_id.as_deref(), Some("12345"));
    }

    #[test]
    fn standard_api_response_accepts_modern_code_and_lowercase_request_id() {
        let parsed = serde_json::from_str::<StandardApiResponse>(
            r#"{"code":"InvalidParameter","message":"bad request","requestid":" request-1 "}"#,
        )
        .expect("response");

        assert_eq!(parsed.api_code.as_deref(), Some("InvalidParameter"));
        assert_eq!(parsed.errmsg.as_deref(), Some("bad request"));
        assert_eq!(parsed.request_id.as_deref(), Some("request-1"));
    }

    #[test]
    fn standard_api_response_accepts_modern_error_message_aliases() {
        let camel = serde_json::from_str::<StandardApiResponse>(
            r#"{"code":"InvalidParameter","errorMessage":"bad request"}"#,
        )
        .expect("response");
        let snake = serde_json::from_str::<StandardApiResponse>(
            r#"{"code":"InvalidParameter","error_message":"bad request"}"#,
        )
        .expect("response");

        assert_eq!(camel.errmsg.as_deref(), Some("bad request"));
        assert_eq!(snake.errmsg.as_deref(), Some("bad request"));
    }

    #[test]
    fn standard_api_response_normalizes_numeric_message() {
        let parsed = serde_json::from_str::<StandardApiResponse>(
            r#"{"errcode":"40001","errmsg":12345,"requestId":"request-1"}"#,
        )
        .expect("response");

        assert_eq!(parsed.errmsg.as_deref(), Some("12345"));
    }

    #[test]
    fn standard_api_response_accepts_boolean_success_values() {
        let parsed =
            serde_json::from_str::<StandardApiResponse>(r#"{"errcode":0,"success":"false"}"#)
                .expect("response");
        let parsed_bool =
            serde_json::from_str::<StandardApiResponse>(r#"{"errcode":0,"success":true}"#)
                .expect("response");

        assert_eq!(parsed.success, Some(false));
        assert_eq!(parsed_bool.success, Some(true));
    }

    #[cfg(feature = "openapi")]
    #[test]
    fn binary_success_body_detects_json_error_envelope() {
        let body = br#"{"errcode":40001,"errmsg":"invalid token","requestId":"request-1"}"#;
        let error = binary_success_body_error(body, true, BodySnippetConfig::default())
            .expect("json error envelope should fail");

        assert_eq!(error.kind(), crate::ErrorKind::Api);
        assert_eq!(error.errcode(), Some(40001));
        assert_eq!(error.request_id(), Some("request-1"));
        assert!(error.to_string().contains("invalid token"));
    }

    #[cfg(feature = "openapi")]
    #[test]
    fn binary_success_body_ignores_non_json_binary_payload() {
        assert!(
            binary_success_body_error(
                b"downloaded-file-bytes",
                false,
                BodySnippetConfig::default()
            )
            .is_none()
        );
    }

    #[test]
    fn zero_length_body_snippet_disables_capture() {
        let error = api_error_from_body(
            40001,
            "invalid",
            None,
            r#"{"errcode":40001,"errmsg":"invalid"}"#,
            BodySnippetConfig {
                enabled: true,
                max_bytes: 0,
            },
        );

        assert_eq!(error.error_body_snippet(), None);
    }
}
