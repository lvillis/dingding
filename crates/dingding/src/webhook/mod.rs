mod message;

use std::fmt;

pub use message::{
    ActionCardButton, At, ButtonOrientation, FeedCardLink, WebhookMessage, WebhookResponse,
};

use url::Url;

use crate::{DingTalk, Error, Result, signature, transport::parse_standard_response};

/// Sender for custom robot webhooks and session webhooks.
#[derive(Clone)]
pub struct Webhook {
    client: DingTalk,
    target: WebhookTarget,
}

impl fmt::Debug for Webhook {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.target {
            WebhookTarget::Robot { secret, .. } => f
                .debug_struct("Webhook")
                .field("target", &"robot")
                .field("has_signing_secret", &secret.is_some())
                .finish_non_exhaustive(),
            WebhookTarget::Session { .. } => f
                .debug_struct("Webhook")
                .field("target", &"session")
                .finish_non_exhaustive(),
        }
    }
}

#[derive(Clone)]
enum WebhookTarget {
    Robot {
        access_token: String,
        secret: Option<String>,
    },
    Session {
        url: String,
    },
}

impl Webhook {
    pub(crate) fn robot(client: DingTalk, access_token: impl Into<String>) -> Result<Self> {
        let access_token = access_token.into();
        validate_webhook_token(&access_token, "access_token")?;
        Ok(Self {
            client,
            target: WebhookTarget::Robot {
                access_token,
                secret: None,
            },
        })
    }

    pub(crate) fn session(client: DingTalk, url: impl Into<String>) -> Result<Self> {
        let url = url.into();
        parse_session_webhook_url(&url)?;
        Ok(Self {
            client,
            target: WebhookTarget::Session { url },
        })
    }

    /// Adds a custom robot secret for DingTalk HMAC signing.
    pub fn signing_secret(mut self, secret: impl Into<String>) -> Result<Self> {
        match &mut self.target {
            WebhookTarget::Robot { secret: slot, .. } => {
                let secret = secret.into();
                validate_webhook_token(&secret, "secret")?;
                *slot = Some(secret);
                Ok(self)
            }
            WebhookTarget::Session { .. } => Err(Error::InvalidConfig(
                "signing secret is only supported for custom robot webhooks".to_string(),
            )),
        }
    }

    /// Sends a typed webhook message.
    pub async fn send_message(&self, message: WebhookMessage) -> Result<WebhookResponse> {
        message.validate()?;
        let url = self.target_url()?;
        let response = self
            .client
            .transport()
            .post_webhook_json(&url, &message)
            .await?;
        let parsed =
            parse_standard_response(response, self.client.transport().error_body_snippet())?;
        let errcode = parsed.errcode.ok_or_else(|| {
            Error::api_with_code(
                -1,
                None,
                "missing errcode field in DingTalk response",
                parsed.request_id.clone(),
                None,
            )
        })?;
        Ok(WebhookResponse {
            errcode,
            errmsg: parsed.errmsg.unwrap_or_else(|| "ok".to_string()),
            request_id: parsed.request_id,
        })
    }

    /// Sends a text message.
    pub async fn send_text(&self, content: impl Into<String>) -> Result<WebhookResponse> {
        self.send_message(WebhookMessage::text(content)).await
    }

    /// Sends a text message with `@` metadata.
    pub async fn send_text_with_at(
        &self,
        content: impl Into<String>,
        at: At,
    ) -> Result<WebhookResponse> {
        self.send_message(WebhookMessage::text(content).at(at)?)
            .await
    }

    /// Sends a markdown message.
    pub async fn send_markdown(
        &self,
        title: impl Into<String>,
        text: impl Into<String>,
    ) -> Result<WebhookResponse> {
        self.send_message(WebhookMessage::markdown(title, text))
            .await
    }

    /// Sends a markdown message with `@` metadata.
    pub async fn send_markdown_with_at(
        &self,
        title: impl Into<String>,
        text: impl Into<String>,
        at: At,
    ) -> Result<WebhookResponse> {
        self.send_message(WebhookMessage::markdown(title, text).at(at)?)
            .await
    }

    /// Sends a link message.
    pub async fn send_link(
        &self,
        title: impl Into<String>,
        text: impl Into<String>,
        message_url: impl Into<String>,
    ) -> Result<WebhookResponse> {
        self.send_message(WebhookMessage::link(title, text, message_url))
            .await
    }

    /// Sends a link message with an image URL.
    pub async fn send_link_with_image_url(
        &self,
        title: impl Into<String>,
        text: impl Into<String>,
        message_url: impl Into<String>,
        image_url: impl Into<String>,
    ) -> Result<WebhookResponse> {
        self.send_message(WebhookMessage::link(title, text, message_url).image_url(image_url)?)
            .await
    }

    /// Sends a single-button action card.
    pub async fn send_action_card(
        &self,
        title: impl Into<String>,
        text: impl Into<String>,
        button_title: impl Into<String>,
        button_url: impl Into<String>,
    ) -> Result<WebhookResponse> {
        self.send_message(WebhookMessage::action_card(
            title,
            text,
            button_title,
            button_url,
        ))
        .await
    }

    /// Sends a multi-button action card.
    pub async fn send_action_card_buttons(
        &self,
        title: impl Into<String>,
        text: impl Into<String>,
        buttons: Vec<ActionCardButton>,
    ) -> Result<WebhookResponse> {
        self.send_message(WebhookMessage::action_card_buttons(title, text, buttons))
            .await
    }

    /// Sends a multi-button action card with button orientation.
    pub async fn send_action_card_buttons_with_orientation(
        &self,
        title: impl Into<String>,
        text: impl Into<String>,
        buttons: Vec<ActionCardButton>,
        orientation: ButtonOrientation,
    ) -> Result<WebhookResponse> {
        self.send_message(
            WebhookMessage::action_card_buttons(title, text, buttons)
                .button_orientation(orientation)?,
        )
        .await
    }

    /// Sends a feed card.
    pub async fn send_feed_card(&self, links: Vec<FeedCardLink>) -> Result<WebhookResponse> {
        self.send_message(WebhookMessage::feed_card(links)).await
    }

    fn target_url(&self) -> Result<Url> {
        match &self.target {
            WebhookTarget::Robot {
                access_token,
                secret,
            } => {
                validate_webhook_token(access_token, "access_token")?;
                let mut url = self.client.webhook_endpoint(&["robot", "send"])?;
                {
                    let mut query = url.query_pairs_mut();
                    query.append_pair("access_token", access_token);
                    if let Some(secret) = secret {
                        validate_webhook_token(secret, "secret")?;
                        let timestamp = signature::current_timestamp_millis()?;
                        let sign = signature::create_signature(&timestamp, secret)?;
                        query.append_pair("timestamp", &timestamp);
                        query.append_pair("sign", &sign);
                    }
                }
                Ok(url)
            }
            WebhookTarget::Session { url } => parse_session_webhook_url(url),
        }
    }
}

fn parse_session_webhook_url(value: &str) -> Result<Url> {
    validate_no_surrounding_whitespace(value, "webhook_url")?;
    let parsed = Url::parse(value)
        .map_err(|source| Error::invalid_input("webhook_url", format!("invalid URL: {source}")))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(Error::invalid_input(
            "webhook_url",
            "URL scheme must be http or https",
        ));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(Error::invalid_input(
            "webhook_url",
            "URL must not contain username or password",
        ));
    }
    if parsed.fragment().is_some() {
        return Err(Error::invalid_input(
            "webhook_url",
            "URL must not contain a fragment",
        ));
    }
    Ok(parsed)
}

fn validate_webhook_token(value: &str, field: &'static str) -> Result<()> {
    validate_no_surrounding_whitespace(value, field)?;
    if value.chars().any(char::is_whitespace) {
        return Err(Error::invalid_input(
            field,
            "value must not contain whitespace",
        ));
    }
    Ok(())
}

fn validate_no_surrounding_whitespace(value: &str, field: &'static str) -> Result<()> {
    if value.chars().any(char::is_control) {
        return Err(Error::invalid_input(
            field,
            "value must not contain control characters",
        ));
    }
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(Error::invalid_input(field, "value must not be empty"));
    }
    if trimmed != value {
        return Err(Error::invalid_input(
            field,
            "value must not contain leading or trailing whitespace",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorKind;

    #[test]
    fn rejects_empty_robot_access_token() {
        let client = DingTalk::new().expect("client");
        let error = client.webhook("  ").expect_err("empty token should fail");

        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn rejects_untrimmed_robot_access_token_and_secret() {
        let client = DingTalk::new().expect("client");
        let token_error = client
            .webhook(" token ")
            .expect_err("access token should not be rewritten");
        let secret_error = client
            .webhook("token")
            .expect("webhook")
            .signing_secret(" secret ")
            .expect_err("secret should not be rewritten");

        assert_eq!(token_error.kind(), ErrorKind::InvalidInput);
        assert_eq!(secret_error.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn rejects_non_http_session_webhook() {
        let client = DingTalk::new().expect("client");
        let error = client
            .session_webhook("file:///tmp/webhook")
            .expect_err("non-http URL should fail");

        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn rejects_session_webhook_userinfo() {
        let client = DingTalk::new().expect("client");
        let error = client
            .session_webhook("https://user:pass@example.com/session-webhook")
            .expect_err("session webhook URL should not contain credentials");

        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn rejects_session_webhook_fragment() {
        let client = DingTalk::new().expect("client");
        let error = client
            .session_webhook("https://example.com/session-webhook#token")
            .expect_err("session webhook URL should not contain a fragment");

        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn rejects_untrimmed_session_webhook_url() {
        let client = DingTalk::new().expect("client");
        let error = client
            .session_webhook(" https://example.com/session-webhook?token=abc ")
            .expect_err("session webhook URL should not be rewritten");

        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn rejects_signing_secret_for_session_webhook() {
        let client = DingTalk::new().expect("client");
        let error = client
            .session_webhook("https://example.com/session-webhook?token=abc")
            .expect("session webhook")
            .signing_secret("secret")
            .expect_err("session webhooks are already signed by DingTalk");

        assert_eq!(error.kind(), ErrorKind::InvalidConfig);
    }
}
