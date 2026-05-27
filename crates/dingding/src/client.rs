use std::{sync::Arc, time::Duration};

use reqx::{advanced::ClientProfile, prelude::RetryPolicy};
use url::Url;

#[cfg(feature = "openapi")]
use crate::auth::{AppCredentials, MemoryTokenCache};
#[cfg(feature = "openapi")]
use crate::transport::DEFAULT_OPENAPI_BASE_URL;
use crate::{
    Result,
    transport::{DEFAULT_WEBHOOK_BASE_URL, Transport, TransportConfig},
    util::url::{endpoint_url, normalize_base_url},
};

/// Main SDK entry point.
#[derive(Clone)]
pub struct DingTalk {
    inner: Arc<Inner>,
}

struct Inner {
    webhook_base_url: Url,
    #[cfg(feature = "openapi")]
    openapi_base_url: Url,
    #[cfg(feature = "openapi")]
    app_credentials: Option<AppCredentials>,
    #[cfg(feature = "openapi")]
    token_cache: MemoryTokenCache,
    transport: Transport,
}

impl DingTalk {
    /// Creates a new builder.
    #[must_use]
    pub fn builder() -> DingTalkBuilder {
        DingTalkBuilder::new()
    }

    /// Builds a client with default configuration.
    pub fn new() -> Result<Self> {
        Self::builder().build()
    }

    /// Creates a custom robot webhook sender.
    #[cfg(feature = "webhook")]
    #[must_use]
    pub fn webhook(&self, access_token: impl Into<String>) -> crate::webhook::Webhook {
        crate::webhook::Webhook::robot(self.clone(), access_token)
    }

    /// Creates a sender from a full session webhook URL.
    #[cfg(feature = "webhook")]
    #[must_use]
    pub fn session_webhook(&self, url: impl Into<String>) -> crate::webhook::Webhook {
        crate::webhook::Webhook::session(self.clone(), url)
    }

    /// Creates an OpenAPI service using credentials configured on the client.
    #[cfg(feature = "openapi")]
    #[must_use]
    pub fn openapi(&self) -> crate::openapi::OpenApi {
        crate::openapi::OpenApi::new(self.clone(), self.inner.app_credentials.clone())
    }

    /// Creates an OpenAPI service with explicit app credentials.
    #[cfg(feature = "openapi")]
    #[must_use]
    pub fn openapi_with_credentials(&self, credentials: AppCredentials) -> crate::openapi::OpenApi {
        crate::openapi::OpenApi::new(self.clone(), Some(credentials))
    }

    pub(crate) fn webhook_endpoint(&self, segments: &[&str]) -> Result<Url> {
        endpoint_url(&self.inner.webhook_base_url, segments)
    }

    #[cfg(feature = "openapi")]
    pub(crate) fn openapi_endpoint(&self, segments: &[&str]) -> Result<Url> {
        endpoint_url(&self.inner.openapi_base_url, segments)
    }

    pub(crate) fn transport(&self) -> &Transport {
        &self.inner.transport
    }

    #[cfg(feature = "stream")]
    pub(crate) fn app_credentials(&self) -> Option<AppCredentials> {
        self.inner.app_credentials.clone()
    }

    #[cfg(feature = "openapi")]
    pub(crate) fn cached_access_token(&self, credentials: &AppCredentials) -> Option<String> {
        self.inner.token_cache.get(credentials)
    }

    #[cfg(feature = "openapi")]
    pub(crate) fn store_access_token(
        &self,
        credentials: AppCredentials,
        token: String,
        expires_in_seconds: Option<i64>,
    ) {
        self.inner
            .token_cache
            .store(credentials, token, expires_in_seconds);
    }
}

/// Builder for [`DingTalk`].
#[derive(Clone)]
pub struct DingTalkBuilder {
    webhook_base_url: String,
    #[cfg(feature = "openapi")]
    openapi_base_url: String,
    #[cfg(feature = "openapi")]
    app_credentials: Option<AppCredentials>,
    #[cfg(feature = "openapi")]
    token_refresh_margin: Duration,
    transport: TransportConfig,
}

impl Default for DingTalkBuilder {
    fn default() -> Self {
        Self {
            webhook_base_url: DEFAULT_WEBHOOK_BASE_URL.to_string(),
            #[cfg(feature = "openapi")]
            openapi_base_url: DEFAULT_OPENAPI_BASE_URL.to_string(),
            #[cfg(feature = "openapi")]
            app_credentials: None,
            #[cfg(feature = "openapi")]
            token_refresh_margin: Duration::from_secs(120),
            transport: TransportConfig::default(),
        }
    }
}

impl DingTalkBuilder {
    /// Creates a builder with defaults.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Configures app credentials used by OpenAPI calls.
    #[cfg(feature = "openapi")]
    #[must_use]
    pub fn app_credentials(mut self, credentials: AppCredentials) -> Self {
        self.app_credentials = Some(credentials);
        self
    }

    /// Configures app credentials used by OpenAPI calls.
    #[cfg(feature = "openapi")]
    #[must_use]
    pub fn app_key_and_secret(
        mut self,
        app_key: impl Into<String>,
        app_secret: impl Into<String>,
    ) -> Self {
        self.app_credentials = Some(AppCredentials::new(app_key, app_secret));
        self
    }

    /// Configures app credentials from environment variables.
    ///
    /// Reads `DINGTALK_CLIENT_ID` / `DINGTALK_CLIENT_SECRET`, falling back to
    /// `DINGTALK_APP_KEY` / `DINGTALK_APP_SECRET`.
    #[cfg(feature = "openapi")]
    pub fn app_credentials_from_env(self) -> Result<Self> {
        Ok(self.app_credentials(AppCredentials::from_env()?))
    }

    /// Overrides the legacy `oapi.dingtalk.com` base URL.
    #[must_use]
    pub fn webhook_base_url(mut self, value: impl Into<String>) -> Self {
        self.webhook_base_url = value.into();
        self
    }

    /// Overrides the modern OpenAPI base URL.
    #[cfg(feature = "openapi")]
    #[must_use]
    pub fn openapi_base_url(mut self, value: impl Into<String>) -> Self {
        self.openapi_base_url = value.into();
        self
    }

    /// Sets how early cached OpenAPI access tokens should be refreshed.
    #[cfg(feature = "openapi")]
    #[must_use]
    pub fn access_token_refresh_margin(mut self, value: Duration) -> Self {
        self.token_refresh_margin = value;
        self
    }

    /// Applies a reqx transport profile.
    #[must_use]
    pub fn profile(mut self, value: ClientProfile) -> Self {
        self.transport.profile = value;
        self.transport.request_timeout = None;
        self.transport.total_timeout = None;
        self.transport.retry_policy = None;
        self
    }

    /// Sets client name used by the underlying HTTP client.
    #[must_use]
    pub fn client_name(mut self, value: impl Into<String>) -> Self {
        self.transport.client_name = value.into();
        self
    }

    /// Sets per-request timeout.
    #[must_use]
    pub fn request_timeout(mut self, value: Duration) -> Self {
        self.transport.request_timeout = Some(value.max(Duration::from_millis(1)));
        self
    }

    /// Sets total request deadline.
    #[must_use]
    pub fn total_timeout(mut self, value: Duration) -> Self {
        self.transport.total_timeout = Some(value.max(Duration::from_millis(1)));
        self
    }

    /// Sets TCP connect timeout.
    #[must_use]
    pub fn connect_timeout(mut self, value: Duration) -> Self {
        self.transport.connect_timeout = value.max(Duration::from_millis(1));
        self
    }

    /// Sets whether system proxy configuration is used by the underlying HTTP client.
    #[must_use]
    pub fn system_proxy(mut self, enabled: bool) -> Self {
        self.transport.system_proxy = enabled;
        self
    }

    /// Overrides reqx retry policy.
    #[must_use]
    pub fn retry_policy(mut self, value: RetryPolicy) -> Self {
        self.transport.retry_policy = Some(value);
        self
    }

    /// Enables retry for non-idempotent requests.
    #[must_use]
    pub fn retry_non_idempotent_requests(mut self, enabled: bool) -> Self {
        self.transport.retry_non_idempotent_requests = enabled;
        self
    }

    /// Adds a default header to all SDK requests.
    #[must_use]
    pub fn default_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.transport
            .default_headers
            .push((name.into(), value.into()));
        self
    }

    /// Configures response body snippets retained on API errors.
    #[must_use]
    pub fn error_body_snippet(mut self, value: crate::BodySnippetConfig) -> Self {
        self.transport.error_body_snippet = value;
        self
    }

    /// Builds a [`DingTalk`] client.
    pub fn build(self) -> Result<DingTalk> {
        let webhook_base_url = normalize_base_url(self.webhook_base_url)?;
        #[cfg(feature = "openapi")]
        let openapi_base_url = normalize_base_url(self.openapi_base_url)?;
        #[cfg(feature = "openapi")]
        if let Some(credentials) = &self.app_credentials {
            credentials.validate()?;
        }
        #[cfg(feature = "openapi")]
        let transport =
            Transport::new(&webhook_base_url, Some(&openapi_base_url), &self.transport)?;
        #[cfg(not(feature = "openapi"))]
        let transport = Transport::new(&webhook_base_url, None, &self.transport)?;

        Ok(DingTalk {
            inner: Arc::new(Inner {
                webhook_base_url,
                #[cfg(feature = "openapi")]
                openapi_base_url,
                #[cfg(feature = "openapi")]
                app_credentials: self.app_credentials,
                #[cfg(feature = "openapi")]
                token_cache: MemoryTokenCache::new().with_refresh_margin(self.token_refresh_margin),
                transport,
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "openapi")]
    #[test]
    fn openapi_with_credentials_overrides_client_credentials() {
        let client = DingTalk::builder()
            .app_key_and_secret("client-key", "client-secret")
            .build()
            .expect("client");
        let openapi =
            client.openapi_with_credentials(AppCredentials::new("override-key", "override-secret"));

        assert_eq!(
            openapi
                .credentials()
                .map(|credentials| credentials.app_key()),
            Some("override-key")
        );
    }
}
