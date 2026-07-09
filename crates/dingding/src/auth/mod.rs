use std::{env, fmt};

#[cfg(feature = "openapi")]
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
#[cfg(feature = "openapi")]
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

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
        Self {
            app_key: app_key.into(),
            app_secret: app_secret.into(),
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
        let primary = EnvCredentialNames {
            app_key: app_key_var,
            app_secret: app_secret_var,
        };
        let fallback = EnvCredentialNames {
            app_key: fallback_app_key_var,
            app_secret: fallback_app_secret_var,
        };
        let credentials = select_env_credentials(
            primary,
            fallback,
            read_env_credentials(primary)?,
            read_env_credentials(fallback)?,
        );
        let credentials = credentials?;
        credentials.validate()?;
        Ok(credentials)
    }

    /// Validates that both credential fields are present.
    pub fn validate(&self) -> crate::Result<()> {
        validate_credential_token(&self.app_key, "app_key")?;
        validate_credential_token(&self.app_secret, "app_secret")?;
        Ok(())
    }
}

fn validate_credential_token(value: &str, field: &'static str) -> crate::Result<()> {
    if value.chars().any(char::is_control) {
        return Err(crate::Error::invalid_input(
            field,
            "value must not contain control characters",
        ));
    }
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(crate::Error::invalid_input(
            field,
            "value must not be empty",
        ));
    }
    if trimmed != value {
        return Err(crate::Error::invalid_input(
            field,
            "value must not contain leading or trailing whitespace",
        ));
    }
    if value.chars().any(char::is_whitespace) {
        return Err(crate::Error::invalid_input(
            field,
            "value must not contain whitespace",
        ));
    }
    Ok(())
}

fn env_value(name: &'static str) -> crate::Result<Option<String>> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_value)) => Err(crate::Error::InvalidConfig(format!(
            "{name} environment variable must be valid unicode"
        ))),
    }
}

#[derive(Debug, Clone, Copy)]
struct EnvCredentialNames {
    app_key: &'static str,
    app_secret: &'static str,
}

struct EnvCredentialValues {
    app_key: Option<String>,
    app_secret: Option<String>,
}

fn read_env_credentials(names: EnvCredentialNames) -> crate::Result<EnvCredentialValues> {
    Ok(EnvCredentialValues {
        app_key: env_value(names.app_key)?,
        app_secret: env_value(names.app_secret)?,
    })
}

fn select_env_credentials(
    primary_names: EnvCredentialNames,
    fallback_names: EnvCredentialNames,
    primary_values: EnvCredentialValues,
    fallback_values: EnvCredentialValues,
) -> crate::Result<AppCredentials> {
    if primary_values.app_key.is_some() || primary_values.app_secret.is_some() {
        return complete_env_credentials(primary_names, primary_values);
    }
    if fallback_values.app_key.is_some() || fallback_values.app_secret.is_some() {
        return complete_env_credentials(fallback_names, fallback_values);
    }

    Err(crate::Error::InvalidConfig(format!(
        "set {} and {}, or {} and {} environment variables",
        primary_names.app_key,
        primary_names.app_secret,
        fallback_names.app_key,
        fallback_names.app_secret,
    )))
}

fn complete_env_credentials(
    names: EnvCredentialNames,
    values: EnvCredentialValues,
) -> crate::Result<AppCredentials> {
    match (values.app_key, values.app_secret) {
        (Some(app_key), Some(app_secret)) => Ok(AppCredentials::new(app_key, app_secret)),
        (Some(_app_key), None) => Err(crate::Error::InvalidConfig(format!(
            "set {} environment variable to pair with {}",
            names.app_secret, names.app_key
        ))),
        (None, Some(_app_secret)) => Err(crate::Error::InvalidConfig(format!(
            "set {} environment variable to pair with {}",
            names.app_key, names.app_secret
        ))),
        (None, None) => Err(crate::Error::InvalidConfig(format!(
            "set {} and {} environment variables",
            names.app_key, names.app_secret
        ))),
    }
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
#[derive(Clone)]
pub struct MemoryTokenCache {
    inner: Arc<RwLock<HashMap<AppCredentials, CachedToken>>>,
    refresh_margin: Duration,
}

#[cfg(feature = "openapi")]
impl fmt::Debug for MemoryTokenCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let entry_count = self.inner.read().map(|guard| guard.len()).ok();
        f.debug_struct("MemoryTokenCache")
            .field("entry_count", &entry_count)
            .field("refresh_margin", &self.refresh_margin)
            .finish()
    }
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
        let remaining = cached.expires_at.checked_duration_since(now)?;
        if remaining > self.refresh_margin {
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
#[derive(Clone, Default)]
pub(crate) struct TokenRefreshLocks {
    inner: Arc<AsyncMutex<HashMap<AppCredentials, Arc<AsyncMutex<()>>>>>,
}

#[cfg(feature = "openapi")]
impl TokenRefreshLocks {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) async fn lock(&self, credentials: &AppCredentials) -> TokenRefreshGuard {
        let lock = {
            let mut guard = self.inner.lock().await;
            guard
                .entry(credentials.clone())
                .or_insert_with(|| Arc::new(AsyncMutex::new(())))
                .clone()
        };

        TokenRefreshGuard {
            _guard: lock.lock_owned().await,
        }
    }
}

#[cfg(feature = "openapi")]
pub(crate) struct TokenRefreshGuard {
    _guard: OwnedMutexGuard<()>,
}

#[cfg(feature = "openapi")]
#[derive(Clone)]
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

    #[cfg(feature = "openapi")]
    #[test]
    fn token_cache_debug_does_not_dump_credentials_or_tokens() {
        let credentials = AppCredentials::new("app-key", "app-secret");
        let cache = MemoryTokenCache::new();

        cache.store(credentials, "access-token".to_string(), Some(7200));

        let debug = format!("{cache:?}");
        assert!(debug.contains("entry_count"));
        assert!(!debug.contains("app-secret"));
        assert!(!debug.contains("access-token"));
    }

    #[cfg(feature = "openapi")]
    #[tokio::test]
    async fn token_refresh_locks_serialize_same_credentials() {
        let credentials = AppCredentials::new("app-key", "app-secret");
        let locks = TokenRefreshLocks::new();
        let first = locks.lock(&credentials).await;

        assert!(
            tokio::time::timeout(Duration::from_millis(10), locks.lock(&credentials))
                .await
                .is_err()
        );

        drop(first);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), locks.lock(&credentials))
                .await
                .is_ok()
        );
    }

    #[test]
    fn credentials_validate_rejects_empty_values() {
        let error = AppCredentials::new(" ", "secret")
            .validate()
            .expect_err("empty app key should fail");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn credentials_new_preserves_values_for_validation() {
        let credentials = AppCredentials::new(" app-key ", " app-secret ");

        assert_eq!(credentials.app_key(), " app-key ");
        assert_eq!(credentials.app_secret(), " app-secret ");
        assert_eq!(
            credentials
                .validate()
                .expect_err("credentials should not be rewritten")
                .kind(),
            crate::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn credentials_validate_rejects_internal_whitespace_and_control_chars() {
        let whitespace = AppCredentials::new("app key", "secret")
            .validate()
            .expect_err("internal whitespace should fail");
        let control = AppCredentials::new("app-key", "secret\n")
            .validate()
            .expect_err("control characters should fail");

        assert_eq!(whitespace.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(control.kind(), crate::ErrorKind::InvalidInput);
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

    #[test]
    fn credentials_from_env_values_selects_whole_pairs() {
        let primary_names = EnvCredentialNames {
            app_key: "CLIENT_ID",
            app_secret: "CLIENT_SECRET",
        };
        let fallback_names = EnvCredentialNames {
            app_key: "APP_KEY",
            app_secret: "APP_SECRET",
        };
        let primary = select_env_credentials(
            primary_names,
            fallback_names,
            EnvCredentialValues {
                app_key: Some("client-id".to_string()),
                app_secret: Some("client-secret".to_string()),
            },
            EnvCredentialValues {
                app_key: Some("app-key".to_string()),
                app_secret: Some("app-secret".to_string()),
            },
        )
        .expect("primary pair should win");
        let fallback = select_env_credentials(
            primary_names,
            fallback_names,
            EnvCredentialValues {
                app_key: None,
                app_secret: None,
            },
            EnvCredentialValues {
                app_key: Some("app-key".to_string()),
                app_secret: Some("app-secret".to_string()),
            },
        )
        .expect("fallback pair should be used");
        let partial = select_env_credentials(
            primary_names,
            fallback_names,
            EnvCredentialValues {
                app_key: Some("client-id".to_string()),
                app_secret: None,
            },
            EnvCredentialValues {
                app_key: Some("app-key".to_string()),
                app_secret: Some("app-secret".to_string()),
            },
        )
        .expect_err("primary credentials must not be mixed with fallback credentials");

        assert_eq!(primary.app_key(), "client-id");
        assert_eq!(primary.app_secret(), "client-secret");
        assert_eq!(fallback.app_key(), "app-key");
        assert_eq!(fallback.app_secret(), "app-secret");
        assert_eq!(partial.kind(), crate::ErrorKind::InvalidConfig);
        assert!(partial.to_string().contains("CLIENT_SECRET"));
    }
}
