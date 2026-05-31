use std::{env, fmt};

#[cfg(feature = "openapi")]
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

/// DingTalk application credentials.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct AppCredentials {
    app_key: String,
    app_secret: String,
}

impl AppCredentials {
    /// Creates credentials from app key and app secret.
    #[must_use]
    pub fn new(app_key: impl Into<String>, app_secret: impl Into<String>) -> Self {
        let app_key = app_key.into();
        let app_secret = app_secret.into();
        Self {
            app_key: app_key.trim().to_string(),
            app_secret: app_secret.trim().to_string(),
        }
    }

    /// Returns the app key.
    #[must_use]
    pub fn app_key(&self) -> &str {
        &self.app_key
    }

    /// Returns the app secret.
    #[must_use]
    pub fn app_secret(&self) -> &str {
        &self.app_secret
    }

    /// Creates credentials from environment variables.
    ///
    /// Reads `DINGTALK_CLIENT_ID` / `DINGTALK_CLIENT_SECRET`, falling back to
    /// `DINGTALK_APP_KEY` / `DINGTALK_APP_SECRET`.
    pub fn from_env() -> crate::Result<Self> {
        Self::from_env_vars(
            "DINGTALK_CLIENT_ID",
            "DINGTALK_CLIENT_SECRET",
            "DINGTALK_APP_KEY",
            "DINGTALK_APP_SECRET",
        )
    }

    /// Creates credentials from explicit primary and fallback environment variable names.
    pub fn from_env_vars(
        app_key_var: &'static str,
        app_secret_var: &'static str,
        fallback_app_key_var: &'static str,
        fallback_app_secret_var: &'static str,
    ) -> crate::Result<Self> {
        Ok(Self::new(
            env_value(app_key_var, fallback_app_key_var)?,
            env_value(app_secret_var, fallback_app_secret_var)?,
        ))
    }

    /// Validates that both credential fields are present.
    pub fn validate(&self) -> crate::Result<()> {
        if self.app_key.trim().is_empty() {
            return Err(crate::Error::invalid_input(
                "app_key",
                "value must not be empty",
            ));
        }

        if self.app_secret.trim().is_empty() {
            return Err(crate::Error::invalid_input(
                "app_secret",
                "value must not be empty",
            ));
        }

        Ok(())
    }
}

fn env_value(primary: &'static str, fallback: &'static str) -> crate::Result<String> {
    env::var(primary)
        .or_else(|_| env::var(fallback))
        .map_err(|_error| {
            crate::Error::InvalidConfig(format!("set {primary} or {fallback} environment variable"))
        })
}

impl fmt::Debug for AppCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppCredentials")
            .field("app_key", &self.app_key)
            .field("app_secret", &"<redacted>")
            .finish()
    }
}

/// In-memory access token cache.
#[cfg(feature = "openapi")]
#[derive(Debug, Clone)]
pub struct MemoryTokenCache {
    inner: Arc<RwLock<HashMap<AppCredentials, CachedToken>>>,
    refresh_margin: Duration,
}

#[cfg(feature = "openapi")]
impl Default for MemoryTokenCache {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            refresh_margin: Duration::from_secs(120),
        }
    }
}

#[cfg(feature = "openapi")]
impl MemoryTokenCache {
    /// Creates a cache with default refresh margin.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets how early a token should be considered stale.
    #[must_use]
    pub fn with_refresh_margin(mut self, refresh_margin: Duration) -> Self {
        self.refresh_margin = refresh_margin;
        self
    }

    pub(crate) fn get(&self, credentials: &AppCredentials) -> Option<String> {
        let now = Instant::now();
        let guard = self.inner.read().ok()?;
        let cached = guard.get(credentials)?;
        let refresh_at = now.checked_add(self.refresh_margin)?;
        if refresh_at < cached.expires_at {
            Some(cached.token.clone())
        } else {
            None
        }
    }

    pub(crate) fn store(
        &self,
        credentials: AppCredentials,
        token: String,
        expires_in_seconds: Option<i64>,
    ) {
        let ttl = normalize_token_ttl(expires_in_seconds);
        let expires_at = Instant::now().checked_add(ttl).unwrap_or_else(Instant::now);
        if let Ok(mut guard) = self.inner.write() {
            guard.insert(credentials, CachedToken { token, expires_at });
        }
    }
}

#[cfg(feature = "openapi")]
#[derive(Debug, Clone)]
struct CachedToken {
    token: String,
    expires_at: Instant,
}

#[cfg(feature = "openapi")]
fn normalize_token_ttl(expires_in_seconds: Option<i64>) -> Duration {
    match expires_in_seconds {
        Some(value) if value > 0 => Duration::from_secs(value as u64),
        Some(_) => Duration::ZERO,
        None => Duration::from_secs(7200),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "openapi")]
    #[test]
    fn token_cache_respects_refresh_margin() {
        let credentials = AppCredentials::new("app-key", "app-secret");
        let cache = MemoryTokenCache::new().with_refresh_margin(Duration::from_secs(60));

        cache.store(credentials.clone(), "token".to_string(), Some(30));

        assert_eq!(cache.get(&credentials), None);
    }

    #[cfg(feature = "openapi")]
    #[test]
    fn token_cache_returns_fresh_token() {
        let credentials = AppCredentials::new("app-key", "app-secret");
        let cache = MemoryTokenCache::new().with_refresh_margin(Duration::from_secs(1));

        cache.store(credentials.clone(), "token".to_string(), Some(30));

        assert_eq!(cache.get(&credentials).as_deref(), Some("token"));
    }

    #[cfg(feature = "openapi")]
    #[test]
    fn token_cache_does_not_extend_expired_tokens() {
        let credentials = AppCredentials::new("app-key", "app-secret");
        let cache = MemoryTokenCache::new().with_refresh_margin(Duration::ZERO);

        cache.store(credentials.clone(), "token".to_string(), Some(0));

        assert_eq!(cache.get(&credentials), None);
    }

    #[test]
    fn credentials_validate_rejects_empty_values() {
        let error = AppCredentials::new(" ", "secret")
            .validate()
            .expect_err("empty app key should fail");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn credentials_new_trims_values() {
        let credentials = AppCredentials::new(" app-key ", " app-secret ");

        assert_eq!(credentials.app_key(), "app-key");
        assert_eq!(credentials.app_secret(), "app-secret");
        credentials.validate().expect("credentials should be valid");
    }

    #[test]
    fn credentials_from_missing_env_reports_names() {
        let error = AppCredentials::from_env_vars(
            "DINGDING_TEST_MISSING_CLIENT_ID",
            "DINGDING_TEST_MISSING_CLIENT_SECRET",
            "DINGDING_TEST_MISSING_APP_KEY",
            "DINGDING_TEST_MISSING_APP_SECRET",
        )
        .expect_err("missing env vars should fail");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidConfig);
        assert!(
            error
                .to_string()
                .contains("DINGDING_TEST_MISSING_CLIENT_ID")
        );
    }
}
