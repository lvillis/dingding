mod message;

pub use message::{
    ActionCardButton, At, ButtonOrientation, FeedCardLink, WebhookMessage, WebhookResponse,
};

use url::Url;

use crate::{
    DingTalk, Error, Result, signature, transport::parse_standard_response, util::non_empty_trimmed,
};

/// Sender for custom robot webhooks and session webhooks.
#[derive(Clone)]
pub struct Webhook {
    client: DingTalk,
    target: WebhookTarget,
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
    pub(crate) fn robot(client: DingTalk, access_token: impl Into<String>) -> Self {
        Self {
            client,
            target: WebhookTarget::Robot {
                access_token: access_token.into(),
                secret: None,
            },
        }
    }

    pub(crate) fn session(client: DingTalk, url: impl Into<String>) -> Self {
        Self {
            client,
            target: WebhookTarget::Session { url: url.into() },
        }
    }

    /// Adds a custom robot secret for DingTalk HMAC signing.
    #[must_use]
    pub fn signing_secret(mut self, secret: impl Into<String>) -> Self {
        if let WebhookTarget::Robot { secret: slot, .. } = &mut self.target {
            *slot = Some(secret.into());
        }
        self
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
        self.send_message(WebhookMessage::text(content).at(at))
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
        self.send_message(WebhookMessage::markdown(title, text).at(at))
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
        self.send_message(WebhookMessage::link(title, text, message_url).image_url(image_url))
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
                .button_orientation(orientation),
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
                let access_token = non_empty_trimmed(access_token, "access_token")?;
                let mut url = self.client.webhook_endpoint(&["robot", "send"])?;
                {
                    let mut query = url.query_pairs_mut();
                    query.append_pair("access_token", &access_token);
                    if let Some(secret) = secret {
                        let secret = non_empty_trimmed(secret, "secret")?;
                        let timestamp = signature::current_timestamp_millis()?;
                        let sign = signature::create_signature(&timestamp, &secret)?;
                        query.append_pair("timestamp", &timestamp);
                        query.append_pair("sign", &sign);
                    }
                }
                Ok(url)
            }
            WebhookTarget::Session { url } => {
                let url = non_empty_trimmed(url, "webhook_url")?;
                let parsed = Url::parse(&url).map_err(|source| {
                    Error::invalid_input("webhook_url", format!("invalid URL: {source}"))
                })?;
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorKind;

    #[test]
    fn rejects_empty_robot_access_token() {
        let client = DingTalk::new().expect("client");
        let error = client
            .webhook("  ")
            .target_url()
            .expect_err("empty token should fail");

        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn rejects_non_http_session_webhook() {
        let client = DingTalk::new().expect("client");
        let error = client
            .session_webhook("file:///tmp/webhook")
            .target_url()
            .expect_err("non-http URL should fail");

        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn rejects_session_webhook_userinfo() {
        let client = DingTalk::new().expect("client");
        let error = client
            .session_webhook("https://user:pass@example.com/session-webhook")
            .target_url()
            .expect_err("session webhook URL should not contain credentials");

        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn rejects_session_webhook_fragment() {
        let client = DingTalk::new().expect("client");
        let error = client
            .session_webhook("https://example.com/session-webhook#token")
            .target_url()
            .expect_err("session webhook URL should not contain a fragment");

        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn trims_session_webhook_url() {
        let client = DingTalk::new().expect("client");
        let url = client
            .session_webhook(" https://example.com/session-webhook?token=abc ")
            .target_url()
            .expect("url");

        assert_eq!(
            url.as_str(),
            "https://example.com/session-webhook?token=abc"
        );
    }
}
