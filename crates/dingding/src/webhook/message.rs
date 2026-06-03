use std::{fmt, str::FromStr};

use serde::Serialize;
use url::Url;

use crate::{Error, Result};

/// DingTalk webhook message.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "msgtype")]
pub enum WebhookMessage {
    /// Plain text message.
    #[serde(rename = "text")]
    Text {
        /// Message body.
        text: TextContent,
        /// Optional mention metadata.
        #[serde(skip_serializing_if = "Option::is_none")]
        at: Option<At>,
    },
    /// Markdown message.
    #[serde(rename = "markdown")]
    Markdown {
        /// Message body.
        markdown: MarkdownContent,
        /// Optional mention metadata.
        #[serde(skip_serializing_if = "Option::is_none")]
        at: Option<At>,
    },
    /// Link message.
    #[serde(rename = "link")]
    Link {
        /// Message body.
        link: LinkContent,
    },
    /// Action card message.
    #[serde(rename = "actionCard")]
    ActionCard {
        /// Message body.
        #[serde(rename = "actionCard")]
        action_card: ActionCardContent,
    },
    /// Feed card message.
    #[serde(rename = "feedCard")]
    FeedCard {
        /// Message body.
        #[serde(rename = "feedCard")]
        feed_card: FeedCardContent,
    },
}

impl WebhookMessage {
    /// Creates a text message.
    #[must_use]
    pub fn text(content: impl Into<String>) -> Self {
        Self::Text {
            text: TextContent {
                content: content.into(),
            },
            at: None,
        }
    }

    /// Creates a markdown message.
    #[must_use]
    pub fn markdown(title: impl Into<String>, text: impl Into<String>) -> Self {
        Self::Markdown {
            markdown: MarkdownContent {
                title: title.into(),
                text: text.into(),
            },
            at: None,
        }
    }

    /// Creates a link message.
    #[must_use]
    pub fn link(
        title: impl Into<String>,
        text: impl Into<String>,
        message_url: impl Into<String>,
    ) -> Self {
        Self::Link {
            link: LinkContent {
                title: title.into(),
                text: text.into(),
                message_url: message_url.into(),
                pic_url: None,
            },
        }
    }

    /// Creates a single-button action card message.
    #[must_use]
    pub fn action_card(
        title: impl Into<String>,
        text: impl Into<String>,
        button_title: impl Into<String>,
        button_url: impl Into<String>,
    ) -> Self {
        Self::ActionCard {
            action_card: ActionCardContent {
                title: title.into(),
                text: text.into(),
                btn_orientation: None,
                single_title: Some(button_title.into()),
                single_url: Some(button_url.into()),
                btns: None,
            },
        }
    }

    /// Creates a multi-button action card message.
    #[must_use]
    pub fn action_card_buttons(
        title: impl Into<String>,
        text: impl Into<String>,
        buttons: Vec<ActionCardButton>,
    ) -> Self {
        Self::ActionCard {
            action_card: ActionCardContent {
                title: title.into(),
                text: text.into(),
                btn_orientation: None,
                single_title: None,
                single_url: None,
                btns: Some(buttons),
            },
        }
    }

    /// Creates a feed card message.
    #[must_use]
    pub fn feed_card(links: Vec<FeedCardLink>) -> Self {
        Self::FeedCard {
            feed_card: FeedCardContent { links },
        }
    }

    /// Adds mention metadata to text or markdown messages.
    pub fn at(mut self, at: At) -> Result<Self> {
        if at.is_empty() {
            return Ok(self);
        }
        at.validate()?;
        match &mut self {
            Self::Text { at: slot, .. } | Self::Markdown { at: slot, .. } => {
                *slot = Some(at);
                Ok(self)
            }
            Self::Link { .. } | Self::ActionCard { .. } | Self::FeedCard { .. } => Err(
                Error::invalid_input("at", "mentions are only supported for text and markdown"),
            ),
        }
    }

    /// Adds an image URL to a link message.
    pub fn image_url(mut self, image_url: impl Into<String>) -> Result<Self> {
        let image_url = image_url.into();
        validate_http_url(&image_url, "link.pic_url")?;
        match &mut self {
            Self::Link { link } => {
                link.pic_url = Some(image_url);
                Ok(self)
            }
            Self::Text { .. }
            | Self::Markdown { .. }
            | Self::ActionCard { .. }
            | Self::FeedCard { .. } => Err(Error::invalid_input(
                "image_url",
                "image URLs are only supported for link messages",
            )),
        }
    }

    /// Sets action-card button orientation.
    pub fn button_orientation(mut self, orientation: ButtonOrientation) -> Result<Self> {
        match &mut self {
            Self::ActionCard { action_card } => {
                action_card.btn_orientation = Some(orientation.as_dingtalk_value().to_string());
                Ok(self)
            }
            Self::Text { .. }
            | Self::Markdown { .. }
            | Self::Link { .. }
            | Self::FeedCard { .. } => Err(Error::invalid_input(
                "button_orientation",
                "button orientation is only supported for action card messages",
            )),
        }
    }

    /// Validates this message before sending.
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Text { text, at } => {
                validate_non_empty(&text.content, "text.content")?;
                if let Some(at) = at {
                    at.validate()?;
                }
                Ok(())
            }
            Self::Markdown { markdown, at } => {
                validate_non_empty(&markdown.title, "markdown.title")?;
                validate_non_empty(&markdown.text, "markdown.text")?;
                if let Some(at) = at {
                    at.validate()?;
                }
                Ok(())
            }
            Self::Link { link } => {
                validate_non_empty(&link.title, "link.title")?;
                validate_non_empty(&link.text, "link.text")?;
                validate_http_url(&link.message_url, "link.message_url")?;
                if let Some(pic_url) = &link.pic_url {
                    validate_http_url(pic_url, "link.pic_url")?;
                }
                Ok(())
            }
            Self::ActionCard { action_card } => action_card.validate(),
            Self::FeedCard { feed_card } => {
                if feed_card.links.is_empty() {
                    return Err(Error::invalid_input(
                        "feed_card.links",
                        "at least one link is required",
                    ));
                }

                for link in &feed_card.links {
                    link.validate()?;
                }

                Ok(())
            }
        }
    }
}

fn validate_non_empty(value: &str, field: &'static str) -> Result<()> {
    if value.trim().is_empty() {
        Err(Error::invalid_input(field, "value must not be empty"))
    } else {
        Ok(())
    }
}

fn validate_http_url(value: &str, field: &'static str) -> Result<()> {
    let trimmed = value.trim();
    validate_non_empty(trimmed, field)?;
    if trimmed != value {
        return Err(Error::invalid_input(
            field,
            "value must not contain leading or trailing whitespace",
        ));
    }

    let parsed = Url::parse(value)
        .map_err(|source| Error::invalid_input(field, format!("invalid URL: {source}")))?;
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(Error::invalid_input(
            field,
            "URL must not contain username or password",
        ));
    }
    if matches!(parsed.scheme(), "http" | "https") {
        Ok(())
    } else {
        Err(Error::invalid_input(
            field,
            "URL scheme must be http or https",
        ))
    }
}

/// Action-card button orientation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ButtonOrientation {
    /// Buttons are arranged vertically.
    Vertical,
    /// Buttons are arranged horizontally.
    Horizontal,
}

impl ButtonOrientation {
    /// Parses a DingTalk wire value or a human-readable label.
    ///
    /// Accepts `0` / `vertical` and `1` / `horizontal`.
    #[must_use]
    pub fn from_dingtalk_value(value: impl AsRef<str>) -> Option<Self> {
        match value.as_ref().trim().to_ascii_lowercase().as_str() {
            "0" | "vertical" => Some(Self::Vertical),
            "1" | "horizontal" => Some(Self::Horizontal),
            _ => None,
        }
    }

    /// Returns the DingTalk wire value for this orientation.
    #[must_use]
    pub fn as_dingtalk_value(self) -> &'static str {
        match self {
            Self::Vertical => "0",
            Self::Horizontal => "1",
        }
    }

    /// Returns a stable lowercase label for logs and configuration.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Vertical => "vertical",
            Self::Horizontal => "horizontal",
        }
    }

    /// Returns whether buttons are arranged vertically.
    #[must_use]
    pub fn is_vertical(self) -> bool {
        matches!(self, Self::Vertical)
    }

    /// Returns whether buttons are arranged horizontally.
    #[must_use]
    pub fn is_horizontal(self) -> bool {
        matches!(self, Self::Horizontal)
    }
}

impl FromStr for ButtonOrientation {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        Self::from_dingtalk_value(value).ok_or_else(|| {
            Error::invalid_input(
                "button_orientation",
                "value must be vertical, horizontal, 0, or 1",
            )
        })
    }
}

impl fmt::Display for ButtonOrientation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Text body.
#[derive(Debug, Clone, Serialize)]
pub struct TextContent {
    /// Text content.
    pub content: String,
}

/// Markdown body.
#[derive(Debug, Clone, Serialize)]
pub struct MarkdownContent {
    /// Title shown in notification surfaces.
    pub title: String,
    /// Markdown text.
    pub text: String,
}

/// Link body.
#[derive(Debug, Clone, Serialize)]
pub struct LinkContent {
    /// Link title.
    pub title: String,
    /// Link text.
    pub text: String,
    /// Link target.
    #[serde(rename = "messageUrl")]
    pub message_url: String,
    /// Optional image URL.
    #[serde(rename = "picUrl", skip_serializing_if = "Option::is_none")]
    pub pic_url: Option<String>,
}

/// Action card body.
#[derive(Debug, Clone, Serialize)]
pub struct ActionCardContent {
    /// Card title.
    pub title: String,
    /// Markdown text.
    pub text: String,
    /// Button orientation.
    #[serde(rename = "btnOrientation", skip_serializing_if = "Option::is_none")]
    pub btn_orientation: Option<String>,
    /// Single button title.
    #[serde(rename = "singleTitle", skip_serializing_if = "Option::is_none")]
    pub single_title: Option<String>,
    /// Single button URL.
    #[serde(rename = "singleURL", skip_serializing_if = "Option::is_none")]
    pub single_url: Option<String>,
    /// Multiple buttons.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub btns: Option<Vec<ActionCardButton>>,
}

impl ActionCardContent {
    fn validate(&self) -> Result<()> {
        validate_non_empty(&self.title, "action_card.title")?;
        validate_non_empty(&self.text, "action_card.text")?;
        if let Some(orientation) = &self.btn_orientation
            && !matches!(orientation.as_str(), "0" | "1")
        {
            return Err(Error::invalid_input(
                "action_card.btn_orientation",
                "value must be 0 or 1",
            ));
        }

        match (&self.single_title, &self.single_url, &self.btns) {
            (Some(title), Some(url), None) => {
                validate_non_empty(title, "action_card.single_title")?;
                validate_http_url(url, "action_card.single_url")
            }
            (None, None, Some(buttons)) if !buttons.is_empty() => {
                for button in buttons {
                    button.validate()?;
                }
                Ok(())
            }
            _ => Err(Error::invalid_input(
                "action_card",
                "provide either single button fields or at least one multi-button item",
            )),
        }
    }
}

/// Feed card body.
#[derive(Debug, Clone, Serialize)]
pub struct FeedCardContent {
    /// Feed links.
    pub links: Vec<FeedCardLink>,
}

/// Action card button.
#[derive(Debug, Clone, Serialize)]
pub struct ActionCardButton {
    /// Button title.
    pub title: String,
    /// Button URL.
    #[serde(rename = "actionURL")]
    pub action_url: String,
}

impl ActionCardButton {
    /// Creates an action-card button.
    #[must_use]
    pub fn new(title: impl Into<String>, action_url: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            action_url: action_url.into(),
        }
    }

    fn validate(&self) -> Result<()> {
        validate_non_empty(&self.title, "action_card.button.title")?;
        validate_http_url(&self.action_url, "action_card.button.action_url")
    }
}

/// Feed card link item.
#[derive(Debug, Clone, Serialize)]
pub struct FeedCardLink {
    /// Link title.
    pub title: String,
    /// Link target.
    #[serde(rename = "messageURL")]
    pub message_url: String,
    /// Image URL.
    #[serde(rename = "picURL")]
    pub pic_url: String,
}

impl FeedCardLink {
    /// Creates a feed-card link.
    #[must_use]
    pub fn new(
        title: impl Into<String>,
        message_url: impl Into<String>,
        pic_url: impl Into<String>,
    ) -> Self {
        Self {
            title: title.into(),
            message_url: message_url.into(),
            pic_url: pic_url.into(),
        }
    }

    fn validate(&self) -> Result<()> {
        validate_non_empty(&self.title, "feed_card.link.title")?;
        validate_http_url(&self.message_url, "feed_card.link.message_url")?;
        validate_http_url(&self.pic_url, "feed_card.link.pic_url")
    }
}

/// Mention metadata.
#[derive(Debug, Clone, Default, Serialize)]
pub struct At {
    /// Mentioned mobile numbers.
    #[serde(rename = "atMobiles", skip_serializing_if = "Vec::is_empty")]
    pub mobiles: Vec<String>,
    /// Mentioned DingTalk user ids.
    #[serde(rename = "atUserIds", skip_serializing_if = "Vec::is_empty")]
    pub user_ids: Vec<String>,
    /// Mentions everyone.
    #[serde(rename = "isAtAll")]
    pub is_at_all: bool,
}

impl At {
    /// Creates empty mention metadata.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates mention metadata from users mentioned in an incoming bot message.
    ///
    /// DingTalk webhook replies use staff user ids in `atUserIds`; incoming
    /// `dingtalk_id` values are intentionally ignored because they are not valid
    /// webhook mention ids.
    #[cfg(feature = "bot")]
    #[must_use]
    pub fn from_mentioned_users<'a, I>(users: I) -> Self
    where
        I: IntoIterator<Item = &'a crate::bot::AtUser>,
    {
        Self::new().mentioned_users(users)
    }

    /// Adds a mobile number.
    #[must_use]
    pub fn mobile(mut self, value: impl Into<String>) -> Self {
        push_unique(&mut self.mobiles, value.into());
        self
    }

    /// Adds multiple mobile numbers.
    #[must_use]
    pub fn mobiles<I, S>(mut self, values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        for value in values {
            push_unique(&mut self.mobiles, value.into());
        }
        self
    }

    /// Adds a DingTalk user id.
    #[must_use]
    pub fn user_id(mut self, value: impl Into<String>) -> Self {
        push_unique(&mut self.user_ids, value.into());
        self
    }

    /// Adds multiple DingTalk user ids.
    #[must_use]
    pub fn user_ids<I, S>(mut self, values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        for value in values {
            push_unique(&mut self.user_ids, value.into());
        }
        self
    }

    /// Adds a user mentioned in an incoming bot message.
    ///
    /// Only `staff_id` is used because DingTalk webhook `atUserIds` expects
    /// staff user ids.
    #[cfg(feature = "bot")]
    #[must_use]
    pub fn mentioned_user(mut self, user: &crate::bot::AtUser) -> Self {
        if let Some(staff_id) = user.staff_id.as_deref()
            && validate_mention_value(staff_id, "at.user_ids").is_ok()
        {
            push_unique(&mut self.user_ids, staff_id);
        }
        self
    }

    /// Adds users mentioned in an incoming bot message.
    #[cfg(feature = "bot")]
    #[must_use]
    pub fn mentioned_users<'a, I>(mut self, users: I) -> Self
    where
        I: IntoIterator<Item = &'a crate::bot::AtUser>,
    {
        for user in users {
            self = self.mentioned_user(user);
        }
        self
    }

    /// Mentions everyone in the conversation.
    #[must_use]
    pub fn all_users(mut self) -> Self {
        self.is_at_all = true;
        self
    }

    /// Returns whether no users or everyone are mentioned.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.mobiles.is_empty() && self.user_ids.is_empty() && !self.is_at_all
    }

    /// Validates mention metadata before sending.
    pub fn validate(&self) -> Result<()> {
        for mobile in &self.mobiles {
            validate_mention_value(mobile, "at.mobiles")?;
        }
        for user_id in &self.user_ids {
            validate_mention_value(user_id, "at.user_ids")?;
        }
        Ok(())
    }
}

fn validate_mention_value(value: &str, field: &'static str) -> Result<()> {
    validate_non_empty(value, field)?;
    if value.trim() != value {
        return Err(Error::invalid_input(
            field,
            "value must not contain leading or trailing whitespace",
        ));
    }
    if value.chars().any(char::is_whitespace) {
        return Err(Error::invalid_input(
            field,
            "value must not contain whitespace",
        ));
    }
    if value.chars().any(char::is_control) {
        return Err(Error::invalid_input(
            field,
            "value must not contain control characters",
        ));
    }
    Ok(())
}

fn push_unique(values: &mut Vec<String>, value: impl Into<String>) {
    let value = value.into();
    if !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
}

/// Standard webhook response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookResponse {
    /// DingTalk error code.
    pub errcode: i64,
    /// DingTalk message.
    pub errmsg: String,
    /// Optional request id.
    pub request_id: Option<String>,
}

impl WebhookResponse {
    /// Returns whether DingTalk accepted the webhook message.
    #[must_use]
    pub fn is_success(&self) -> bool {
        self.errcode == 0
    }

    /// Returns whether DingTalk reported a non-zero business code.
    #[must_use]
    pub fn is_error(&self) -> bool {
        !self.is_success()
    }

    /// Returns the DingTalk business code.
    #[must_use]
    pub fn code(&self) -> i64 {
        self.errcode
    }

    /// Returns the DingTalk response message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.errmsg
    }

    /// Returns DingTalk request id when present.
    #[must_use]
    pub fn request_id(&self) -> Option<&str> {
        self.request_id.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn serializes_markdown_with_mentions() {
        let message = WebhookMessage::markdown("title", "**body**")
            .at(At::new()
                .mobile("13800000000")
                .mobiles(["13900000000"])
                .user_ids(["user-1"]))
            .expect("mentions");
        let value = serde_json::to_value(message).expect("serialize");

        assert_eq!(
            value,
            json!({
                "msgtype": "markdown",
                "markdown": {
                    "title": "title",
                    "text": "**body**"
                },
                "at": {
                    "atMobiles": ["13800000000", "13900000000"],
                    "atUserIds": ["user-1"],
                    "isAtAll": false
                }
            })
        );
    }

    #[test]
    fn serializes_action_card_orientation() {
        let message = WebhookMessage::action_card("title", "body", "open", "https://example.com")
            .button_orientation(ButtonOrientation::Horizontal)
            .expect("orientation");
        let value = serde_json::to_value(message).expect("serialize");

        assert_eq!(
            ButtonOrientation::from_dingtalk_value(" horizontal "),
            Some(ButtonOrientation::Horizontal)
        );
        assert_eq!(
            "0".parse::<ButtonOrientation>().expect("orientation"),
            ButtonOrientation::Vertical
        );
        assert_eq!(ButtonOrientation::Horizontal.as_dingtalk_value(), "1");
        assert_eq!(ButtonOrientation::Horizontal.as_str(), "horizontal");
        assert_eq!(ButtonOrientation::Horizontal.to_string(), "horizontal");
        assert!(ButtonOrientation::Vertical.is_vertical());
        assert!(ButtonOrientation::Horizontal.is_horizontal());
        assert_eq!(
            value,
            json!({
                "msgtype": "actionCard",
                "actionCard": {
                    "title": "title",
                    "text": "body",
                    "btnOrientation": "1",
                    "singleTitle": "open",
                    "singleURL": "https://example.com"
                }
            })
        );
    }

    #[test]
    fn webhook_message_builders_reject_untrimmed_urls() {
        let link_image_error = WebhookMessage::link("title", "body", "https://example.com/page")
            .image_url(" https://example.com/pic.png ")
            .expect_err("link image URL whitespace should fail");
        let link = WebhookMessage::link("title", "body", " https://example.com/page ");
        let action_card =
            WebhookMessage::action_card("title", "body", "open", " https://example.com/action ");
        let feed_card = WebhookMessage::feed_card(vec![FeedCardLink::new(
            "title",
            " https://example.com/feed ",
            " https://example.com/feed.png ",
        )]);
        let button = ActionCardButton::new("open", " https://example.com/button ");

        assert_eq!(link_image_error.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(
            link.validate()
                .expect_err("link URL whitespace should fail")
                .kind(),
            crate::ErrorKind::InvalidInput
        );
        assert_eq!(
            action_card
                .validate()
                .expect_err("action URL whitespace should fail")
                .kind(),
            crate::ErrorKind::InvalidInput
        );
        assert_eq!(
            feed_card
                .validate()
                .expect_err("feed URL whitespace should fail")
                .kind(),
            crate::ErrorKind::InvalidInput
        );
        assert_eq!(
            button
                .validate()
                .expect_err("button URL whitespace should fail")
                .kind(),
            crate::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn rejects_variant_specific_setters_on_unsupported_message_types() {
        let image_error = WebhookMessage::text("hello")
            .image_url("https://example.com/pic.png")
            .expect_err("text messages do not support link images");
        let orientation_error = WebhookMessage::text("hello")
            .button_orientation(ButtonOrientation::Horizontal)
            .expect_err("text messages do not support action-card orientation");

        assert_eq!(image_error.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(orientation_error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn rejects_manually_constructed_untrimmed_urls() {
        let message = WebhookMessage::Link {
            link: LinkContent {
                title: "title".to_string(),
                text: "body".to_string(),
                message_url: " https://example.com/path ".to_string(),
                pic_url: Some(" https://example.com/pic.png ".to_string()),
            },
        };

        let error = message
            .validate()
            .expect_err("serialized URL would contain whitespace");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn webhook_response_exposes_helpers() {
        let ok = WebhookResponse {
            errcode: 0,
            errmsg: "ok".to_string(),
            request_id: Some("request-1".to_string()),
        };
        let error = WebhookResponse {
            errcode: 310000,
            errmsg: "invalid token".to_string(),
            request_id: None,
        };

        assert!(ok.is_success());
        assert!(!ok.is_error());
        assert_eq!(ok.code(), 0);
        assert_eq!(ok.message(), "ok");
        assert_eq!(ok.request_id(), Some("request-1"));
        assert!(!error.is_success());
        assert!(error.is_error());
        assert_eq!(error.code(), 310000);
        assert_eq!(error.message(), "invalid token");
        assert_eq!(error.request_id(), None);
    }

    #[test]
    fn mention_builders_deduplicate_without_rewriting_values() {
        let at = At::new()
            .mobile("13800000000")
            .mobiles(["13900000000", "13900000000"])
            .user_id("user-1")
            .user_ids(["user-1", "user-2"]);

        assert_eq!(at.mobiles, ["13800000000", "13900000000"]);
        assert_eq!(at.user_ids, ["user-1", "user-2"]);

        let invalid = At::new().mobile(" 13800000000 ");
        let empty = At::new().user_ids([""]);
        assert_eq!(
            invalid
                .validate()
                .expect_err("mention values should not be rewritten")
                .kind(),
            crate::ErrorKind::InvalidInput
        );
        assert_eq!(
            empty
                .validate()
                .expect_err("empty mention values should not be dropped")
                .kind(),
            crate::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn rejects_empty_text_message() {
        let error = WebhookMessage::text("  ")
            .validate()
            .expect_err("empty text should fail");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn rejects_empty_mention_values() {
        let mobile_error = WebhookMessage::text("hello")
            .at(At {
                mobiles: vec![" ".to_string()],
                user_ids: Vec::new(),
                is_at_all: false,
            })
            .expect_err("empty mobile should fail");
        let user_id_error = WebhookMessage::markdown("title", "body")
            .at(At {
                mobiles: Vec::new(),
                user_ids: vec!["".to_string()],
                is_at_all: false,
            })
            .expect_err("empty user id should fail");

        assert_eq!(mobile_error.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(user_id_error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn rejects_manually_constructed_whitespace_mention_values() {
        let untrimmed_user_id = WebhookMessage::text("hello")
            .at(At {
                mobiles: Vec::new(),
                user_ids: vec![" user-1 ".to_string()],
                is_at_all: false,
            })
            .expect_err("manual user id should not keep whitespace");
        let internal_space_mobile = WebhookMessage::text("hello")
            .at(At {
                mobiles: vec!["138 0000 0000".to_string()],
                user_ids: Vec::new(),
                is_at_all: false,
            })
            .expect_err("mobile should not contain whitespace");

        assert_eq!(untrimmed_user_id.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(internal_space_mobile.kind(), crate::ErrorKind::InvalidInput);
    }

    #[cfg(feature = "bot")]
    #[test]
    fn creates_mentions_from_incoming_at_users() {
        let users = [
            crate::bot::AtUser {
                dingtalk_id: Some("$:LWCP_v1:$open-id".to_string()),
                staff_id: Some("staff-1".to_string()),
            },
            crate::bot::AtUser {
                dingtalk_id: None,
                staff_id: Some("staff-1".to_string()),
            },
            crate::bot::AtUser {
                dingtalk_id: Some("$:LWCP_v1:$only-open-id".to_string()),
                staff_id: None,
            },
            crate::bot::AtUser {
                dingtalk_id: None,
                staff_id: Some(" ".to_string()),
            },
        ];

        let at = At::new().user_id("existing").mentioned_users(&users);
        let from_users = At::from_mentioned_users(&users);

        assert_eq!(at.user_ids, ["existing", "staff-1"]);
        assert_eq!(from_users.user_ids, ["staff-1"]);
        assert!(from_users.mobiles.is_empty());
        assert!(!from_users.is_at_all);
    }

    #[test]
    fn empty_mentions_are_omitted() {
        let message = WebhookMessage::text("hello")
            .at(At::new())
            .expect("empty mentions");
        let value = serde_json::to_value(message).expect("serialize");

        assert!(value.get("at").is_none());
    }

    #[test]
    fn rejects_mentions_on_unsupported_message_types() {
        let error = WebhookMessage::link("title", "body", "https://example.com")
            .at(At::new().user_id("user-1"))
            .expect_err("link messages do not support webhook mentions");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn rejects_invalid_action_card_orientation() {
        let mut message =
            WebhookMessage::action_card("title", "body", "open", "https://example.com");
        let WebhookMessage::ActionCard { action_card } = &mut message else {
            panic!("expected action card");
        };
        action_card.btn_orientation = Some("sideways".to_string());

        let error = message
            .validate()
            .expect_err("invalid orientation should fail");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn rejects_feed_card_without_links() {
        let error = WebhookMessage::feed_card(Vec::new())
            .validate()
            .expect_err("empty feed card should fail");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn rejects_non_http_link_url() {
        let error = WebhookMessage::link("title", "text", "ftp://example.com")
            .validate()
            .expect_err("non-http link should fail");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn rejects_link_url_userinfo() {
        let error = WebhookMessage::link("title", "text", "https://user:pass@example.com")
            .validate()
            .expect_err("link URL should not contain credentials");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    }
}
