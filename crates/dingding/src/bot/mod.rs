//! Route incoming callbacks and reply using typed conversation contexts.
//!
//! [`Bot`] accepts routes directly or via shortcuts. Group-only shortcuts pass a
//! [`GroupContext`]; private-only shortcuts pass a [`PrivateContext`]. Methods
//! accepting an explicit [`ConversationScope`] use the general [`BotContext`].
//! All accept `()`, SDK results, and application results. [`crate::handler_future`]
//! resolves the error type of inline async blocks using both `?` and `Ok(...)`.
//!
//! ```
//! use dingding::{DingTalk, HandlerResult, bot::*};
//!
//! async fn group(ctx: GroupContext) -> HandlerResult {
//!     let message = String::from_utf8(b"pong".to_vec())?;
//!     ctx.reply_text(message).await?;
//!     Ok(())
//! }
//!
//! # fn example() -> dingding::Result<()> {
//! let bot = Bot::new(DingTalk::new()?).on_group_text_command("/ping", group);
//! bot.validate()?;
//! # Ok(()) }
//! ```
//!
//! The optional `#[handler]` macro produces routes with the same return-type
//! support. It is not required for routing. Store application data with
//! [`Bot::state`] and access it through [`BotContext::state_required`]. The type
//! requested must match the stored type, including any `Arc` wrapper.
//!
//! # Routing rules
//!
//! Normal routes are checked first, in registration order. If none matches,
//! fallback routes are checked in their own registration order. Fallbacks accumulate:
//! group and private fallbacks can coexist. Register a catch-all last.
//!
//! Validation rejects duplicate command aliases across overlapping scopes (`Any`
//! overlaps both `Group` and `Private`), and routes fully hidden by an earlier
//! catch-all. The same command in disjoint group/private scopes is allowed.
//! [`Bot::validate`] and Stream builder validation apply these rules before dispatch.
//!
//! Handlers take a single context; access the event through [`BotContext::event`].
//! The `#[handler]` macro accepts zero arguments or one context.
//! Frame/card callbacks use a separate
//! `(StreamContext, event)` signature.
//!
//! # Replies and background jobs
//!
//! [`BotContext::reply_text`], [`BotContext::reply_markdown`], and
//! [`BotContext::reply_message`] use only the event's session webhook. A missing or
//! known-expired webhook is rejected before sending. Use
//! [`BotContext::is_session_webhook_expired`] and
//! [`BotContext::session_webhook_expires_at`] to inspect expiry. Missing expiry
//! metadata does not guarantee that the URL is still valid.
//!
//! With `openapi`, `ctx.reply_via_robot(&robot, message)` explicitly chooses proactive
//! messaging. For delayed work, persist `ctx.robot_reply_target()?` (also available
//! on [`BotEvent`]) with the job and the application's identity. The target contains
//! only a group conversation id or private staff id, not credentials or session URLs.
//! Protect recipient ids as sensitive data. A worker supplies the matching `RobotApi`.
//! These APIs require proactive-message permissions, never guess a missing staff id
//! from `senderId`, and never switch channels after an ambiguous send failure.

use std::{
    any::{Any, type_name},
    convert::Infallible,
    fmt,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::{Engine, engine::general_purpose::STANDARD};
use hmac::{Hmac, KeyInit, Mac};
use serde_json::Value;
use sha2::Sha256;

use crate::{DingTalk, Error, IntoHandlerResult, Result, util::redact::redact_text};

/// Event deduplication backends and reservation contracts.
pub mod dedup;
mod registration;
#[cfg(feature = "openapi")]
mod reply;

use dedup::{Deduplication, EventDeduplicator, MemoryEventDeduplicator};

type BoxFuture = Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;
type BoxedHandler = Arc<dyn Fn(BotContext) -> BoxFuture + Send + Sync + 'static>;
pub(crate) type BotState = Arc<dyn Any + Send + Sync + 'static>;

/// Conversation scope used by bot routing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConversationScope {
    /// Match any conversation.
    Any,
    /// Match group conversations only.
    Group,
    /// Match private conversations only.
    Private,
    /// Unknown conversation type supplied by DingTalk.
    Unknown(String),
}

impl ConversationScope {
    /// Parses a DingTalk conversation type value.
    ///
    /// Accepts DingTalk numeric values (`"1"` private, `"2"` group) and common textual aliases.
    #[must_use]
    pub fn from_dingtalk_value(value: impl AsRef<str>) -> Self {
        parse_conversation_scope(value.as_ref())
    }

    /// Returns whether `self` accepts `actual`.
    #[must_use]
    pub fn accepts(&self, actual: &Self) -> bool {
        matches!(self, Self::Any) || self == actual
    }

    /// Returns a stable textual representation.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Any => "any",
            Self::Group => "group",
            Self::Private => "private",
            Self::Unknown(value) => value.as_str(),
        }
    }

    /// Returns whether this is the group conversation scope.
    #[must_use]
    pub fn is_group(&self) -> bool {
        matches!(self, Self::Group)
    }

    /// Returns whether this is the private conversation scope.
    #[must_use]
    pub fn is_private(&self) -> bool {
        matches!(self, Self::Private)
    }

    /// Returns whether this is an unknown DingTalk conversation scope.
    #[must_use]
    pub fn is_unknown(&self) -> bool {
        matches!(self, Self::Unknown(_))
    }

    #[must_use]
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Self::Any => "any",
            Self::Group => "group",
            Self::Private => "private",
            Self::Unknown(_) => "unknown",
        }
    }

    #[must_use]
    fn actual_name(&self) -> String {
        match self {
            Self::Unknown(value) => format!("unknown({value})"),
            other => other.name().to_string(),
        }
    }
}

impl std::str::FromStr for ConversationScope {
    type Err = Infallible;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        Ok(Self::from_dingtalk_value(value))
    }
}

impl From<&str> for ConversationScope {
    fn from(value: &str) -> Self {
        Self::from_dingtalk_value(value)
    }
}

impl From<String> for ConversationScope {
    fn from(value: String) -> Self {
        Self::from_dingtalk_value(value)
    }
}

impl fmt::Display for ConversationScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Incoming bot message type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageType {
    /// Any message type.
    Any,
    /// Text message.
    Text,
    /// Markdown message.
    Markdown,
    /// Audio message.
    Audio,
    /// Picture message.
    Picture,
    /// Video message.
    Video,
    /// File message.
    File,
    /// Rich text message.
    RichText,
    /// Unknown or not-yet-modeled message type.
    Unknown(String),
}

impl MessageType {
    /// Parses a DingTalk message type value.
    #[must_use]
    pub fn from_dingtalk_value(value: impl AsRef<str>) -> Self {
        Self::from_raw(Some(value.as_ref()))
    }

    fn from_raw(value: Option<&str>) -> Self {
        let value = value.unwrap_or_default().trim();
        if value.is_empty() {
            return Self::Unknown("missing".to_string());
        }

        match normalized_filter_value(value).as_str() {
            "any" => Self::Any,
            "text" => Self::Text,
            "markdown" => Self::Markdown,
            "audio" | "voice" => Self::Audio,
            "picture" | "image" => Self::Picture,
            "video" => Self::Video,
            "file" => Self::File,
            "richtext" => Self::RichText,
            _ => Self::Unknown(value.to_string()),
        }
    }

    /// Returns whether `self` accepts `actual`.
    #[must_use]
    pub fn accepts(&self, actual: &Self) -> bool {
        matches!(self, Self::Any) || self == actual
    }

    /// Returns the DingTalk message type name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Any => "any",
            Self::Text => "text",
            Self::Markdown => "markdown",
            Self::Audio => "audio",
            Self::Picture => "picture",
            Self::Video => "video",
            Self::File => "file",
            Self::RichText => "richText",
            Self::Unknown(value) => value.as_str(),
        }
    }

    /// Returns whether this is a text message.
    #[must_use]
    pub fn is_text(&self) -> bool {
        matches!(self, Self::Text)
    }

    /// Returns whether this is an unknown DingTalk message type.
    #[must_use]
    pub fn is_unknown(&self) -> bool {
        matches!(self, Self::Unknown(_))
    }
}

impl std::str::FromStr for MessageType {
    type Err = Infallible;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        Ok(Self::from_dingtalk_value(value))
    }
}

impl From<&str> for MessageType {
    fn from(value: &str) -> Self {
        Self::from_dingtalk_value(value)
    }
}

impl From<String> for MessageType {
    fn from(value: String) -> Self {
        Self::from_dingtalk_value(value)
    }
}

impl fmt::Display for MessageType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Short alias for [`ConversationScope`], intended for handler macros and IDE completion.
pub use ConversationScope as Scope;

/// Short alias for [`MessageType`], intended for handler macros and IDE completion.
pub use MessageType as Msg;

/// Incoming text message.
#[derive(Clone, PartialEq, Eq)]
pub struct TextMessage {
    /// Message content.
    pub content: String,
}

impl fmt::Debug for TextMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TextMessage")
            .field("content", &redact_text(&self.content))
            .finish()
    }
}

fn redacted_optional_value(value: &Option<String>) -> Option<&'static str> {
    value.as_ref().map(|_value| "<redacted>")
}

fn redacted_optional_text(value: &Option<String>) -> Option<String> {
    value.as_deref().map(redact_text)
}

fn redacted_json_value(value: &Value) -> String {
    redact_text(&value.to_string())
}

/// Normalized incoming message payload.
#[derive(Clone, PartialEq)]
pub enum IncomingMessage {
    /// Text message.
    Text(TextMessage),
    /// Markdown message payload.
    Markdown(Value),
    /// Audio message.
    Audio(AudioMessage),
    /// Picture message.
    Picture(PictureMessage),
    /// Video message.
    Video(VideoMessage),
    /// File message.
    File(FileMessage),
    /// Rich text message.
    RichText(RichTextMessage),
    /// Unknown or future message type.
    Unknown {
        /// Raw DingTalk message type.
        message_type: String,
        /// Raw message content when supplied.
        content: Option<Value>,
    },
}

impl fmt::Debug for IncomingMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Text(message) => f.debug_tuple("Text").field(message).finish(),
            Self::Markdown(content) => f
                .debug_tuple("Markdown")
                .field(&redacted_json_value(content))
                .finish(),
            Self::Audio(message) => f.debug_tuple("Audio").field(message).finish(),
            Self::Picture(message) => f.debug_tuple("Picture").field(message).finish(),
            Self::Video(message) => f.debug_tuple("Video").field(message).finish(),
            Self::File(message) => f.debug_tuple("File").field(message).finish(),
            Self::RichText(message) => f.debug_tuple("RichText").field(message).finish(),
            Self::Unknown {
                message_type,
                content,
            } => {
                let content = content.as_ref().map(redacted_json_value);
                f.debug_struct("Unknown")
                    .field("message_type", message_type)
                    .field("content", &content)
                    .finish()
            }
        }
    }
}

impl IncomingMessage {
    fn from_parts(
        message_type: &MessageType,
        text: Option<TextMessage>,
        content: Option<Value>,
    ) -> Self {
        match message_type {
            MessageType::Text => text.map(Self::Text).unwrap_or_else(|| Self::Unknown {
                message_type: "text".to_string(),
                content,
            }),
            MessageType::Markdown => Self::Markdown(content.unwrap_or(Value::Null)),
            MessageType::Audio => Self::Audio(AudioMessage::from_content(content.as_ref())),
            MessageType::Picture => Self::Picture(PictureMessage::from_content(content.as_ref())),
            MessageType::Video => Self::Video(VideoMessage::from_content(content.as_ref())),
            MessageType::File => Self::File(FileMessage::from_content(content.as_ref())),
            MessageType::RichText => {
                Self::RichText(RichTextMessage::from_content(content.as_ref()))
            }
            MessageType::Any => Self::Unknown {
                message_type: String::new(),
                content,
            },
            MessageType::Unknown(value) => Self::Unknown {
                message_type: value.clone(),
                content,
            },
        }
    }

    /// Returns text content when this is a text message.
    #[must_use]
    pub fn text(&self) -> Option<&str> {
        self.text_message().map(|message| message.content.as_str())
    }

    /// Returns the text message payload when this is a text message.
    #[must_use]
    pub fn text_message(&self) -> Option<&TextMessage> {
        match self {
            Self::Text(message) => Some(message),
            _ => None,
        }
    }

    /// Returns the markdown payload when this is a markdown message.
    #[must_use]
    pub fn markdown(&self) -> Option<&Value> {
        match self {
            Self::Markdown(content) => Some(content),
            _ => None,
        }
    }

    /// Returns the audio payload when this is an audio message.
    #[must_use]
    pub fn audio(&self) -> Option<&AudioMessage> {
        match self {
            Self::Audio(message) => Some(message),
            _ => None,
        }
    }

    /// Returns the picture payload when this is a picture message.
    #[must_use]
    pub fn picture(&self) -> Option<&PictureMessage> {
        match self {
            Self::Picture(message) => Some(message),
            _ => None,
        }
    }

    /// Returns the video payload when this is a video message.
    #[must_use]
    pub fn video(&self) -> Option<&VideoMessage> {
        match self {
            Self::Video(message) => Some(message),
            _ => None,
        }
    }

    /// Returns the file payload when this is a file message.
    #[must_use]
    pub fn file(&self) -> Option<&FileMessage> {
        match self {
            Self::File(message) => Some(message),
            _ => None,
        }
    }

    /// Returns the rich-text payload when this is a rich-text message.
    #[must_use]
    pub fn rich_text(&self) -> Option<&RichTextMessage> {
        match self {
            Self::RichText(message) => Some(message),
            _ => None,
        }
    }

    /// Returns the raw type and content when this is an unknown message type.
    #[must_use]
    pub fn unknown(&self) -> Option<(&str, Option<&Value>)> {
        match self {
            Self::Unknown {
                message_type,
                content,
            } => Some((message_type.as_str(), content.as_ref())),
            _ => None,
        }
    }
}

/// Incoming audio message.
#[derive(Clone, PartialEq, Eq)]
pub struct AudioMessage {
    /// Download code for fetching the audio file.
    pub download_code: Option<String>,
    /// Speech recognition result when supplied.
    pub recognition: Option<String>,
    /// Audio duration in milliseconds when supplied.
    pub duration_millis: Option<u64>,
}

impl fmt::Debug for AudioMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AudioMessage")
            .field(
                "download_code",
                &redacted_optional_value(&self.download_code),
            )
            .field("recognition", &redacted_optional_text(&self.recognition))
            .field("duration_millis", &self.duration_millis)
            .finish()
    }
}

impl AudioMessage {
    fn from_content(content: Option<&Value>) -> Self {
        Self {
            download_code: content_string(content, "downloadCode"),
            recognition: content_string(content, "recognition"),
            duration_millis: content_u64(content, "duration"),
        }
    }
}

/// Incoming picture message.
#[derive(Clone, PartialEq, Eq)]
pub struct PictureMessage {
    /// Download code for fetching the picture.
    pub download_code: Option<String>,
    /// Picture-specific download code when supplied.
    pub picture_download_code: Option<String>,
}

impl fmt::Debug for PictureMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PictureMessage")
            .field(
                "download_code",
                &redacted_optional_value(&self.download_code),
            )
            .field(
                "picture_download_code",
                &redacted_optional_value(&self.picture_download_code),
            )
            .finish()
    }
}

impl PictureMessage {
    fn from_content(content: Option<&Value>) -> Self {
        Self {
            download_code: content_string(content, "downloadCode"),
            picture_download_code: content_string(content, "pictureDownloadCode"),
        }
    }
}

/// Incoming video message.
#[derive(Clone, PartialEq, Eq)]
pub struct VideoMessage {
    /// Download code for fetching the video.
    pub download_code: Option<String>,
    /// Video type such as `mp4`.
    pub video_type: Option<String>,
    /// Video duration in milliseconds when supplied.
    pub duration_millis: Option<u64>,
}

impl fmt::Debug for VideoMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VideoMessage")
            .field(
                "download_code",
                &redacted_optional_value(&self.download_code),
            )
            .field("video_type", &self.video_type)
            .field("duration_millis", &self.duration_millis)
            .finish()
    }
}

impl VideoMessage {
    fn from_content(content: Option<&Value>) -> Self {
        Self {
            download_code: content_string(content, "downloadCode"),
            video_type: content_string(content, "videoType"),
            duration_millis: content_u64(content, "duration"),
        }
    }
}

/// Incoming file message.
#[derive(Clone, PartialEq, Eq)]
pub struct FileMessage {
    /// Download code for fetching the file.
    pub download_code: Option<String>,
    /// File name when supplied.
    pub file_name: Option<String>,
    /// DingTalk file id when supplied.
    pub file_id: Option<String>,
    /// DingTalk space id when supplied.
    pub space_id: Option<String>,
}

impl fmt::Debug for FileMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileMessage")
            .field(
                "download_code",
                &redacted_optional_value(&self.download_code),
            )
            .field("file_name", &redacted_optional_text(&self.file_name))
            .field("file_id", &redacted_optional_value(&self.file_id))
            .field("space_id", &redacted_optional_value(&self.space_id))
            .finish()
    }
}

impl FileMessage {
    fn from_content(content: Option<&Value>) -> Self {
        Self {
            download_code: content_string(content, "downloadCode"),
            file_name: content_string(content, "fileName"),
            file_id: content_string(content, "fileId"),
            space_id: content_string(content, "spaceId"),
        }
    }
}

/// Incoming rich-text message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RichTextMessage {
    /// Rich-text parts in original order.
    pub items: Vec<RichTextItem>,
}

impl RichTextMessage {
    fn from_content(content: Option<&Value>) -> Self {
        let items = content
            .and_then(|value| value_by_names(value, &["richText"]))
            .and_then(Value::as_array)
            .map(|values| values.iter().filter_map(RichTextItem::from_value).collect())
            .unwrap_or_default();

        Self { items }
    }
}

/// Rich-text item.
#[derive(Clone, PartialEq, Eq)]
pub enum RichTextItem {
    /// Text part.
    Text {
        /// Text content.
        text: String,
    },
    /// Picture part.
    Picture {
        /// Download code for fetching the picture.
        download_code: Option<String>,
        /// Picture-specific download code when supplied.
        picture_download_code: Option<String>,
    },
    /// Unknown rich-text part.
    Unknown {
        /// Item type when supplied.
        item_type: Option<String>,
    },
}

impl fmt::Debug for RichTextItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Text { text } => f
                .debug_struct("Text")
                .field("text", &redact_text(text))
                .finish(),
            Self::Picture {
                download_code,
                picture_download_code,
            } => f
                .debug_struct("Picture")
                .field("download_code", &redacted_optional_value(download_code))
                .field(
                    "picture_download_code",
                    &redacted_optional_value(picture_download_code),
                )
                .finish(),
            Self::Unknown { item_type } => f
                .debug_struct("Unknown")
                .field("item_type", item_type)
                .finish(),
        }
    }
}

impl RichTextItem {
    fn from_value(value: &Value) -> Option<Self> {
        if let Some(text) = value_by_names(value, &["text"]).and_then(Value::as_str) {
            return Some(Self::Text {
                text: text.to_string(),
            });
        }

        let item_type = value_by_names(value, &["type"]).and_then(value_string);
        match item_type.as_deref() {
            Some(kind) if kind.eq_ignore_ascii_case("picture") => Some(Self::Picture {
                download_code: value_by_names(value, &["downloadCode"]).and_then(value_string),
                picture_download_code: value_by_names(value, &["pictureDownloadCode"])
                    .and_then(value_string),
            }),
            Some(kind) => Some(Self::Unknown {
                item_type: Some(kind.to_string()),
            }),
            None => None,
        }
    }
}

fn content_string(content: Option<&Value>, key: &str) -> Option<String> {
    content_value(content, key).and_then(value_string)
}

fn content_u64(content: Option<&Value>, key: &str) -> Option<u64> {
    content_value(content, key).and_then(value_u64)
}

fn content_value<'a>(content: Option<&'a Value>, key: &str) -> Option<&'a Value> {
    content.and_then(|value| value_by_names(value, &[key]))
}

fn value_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|raw| raw.trim().parse().ok()))
}

fn value_bool(value: &Value) -> Option<bool> {
    if let Some(value) = value.as_bool() {
        return Some(value);
    }

    if let Some(value) = value.as_u64() {
        return match value {
            0 => Some(false),
            1 => Some(true),
            _ => None,
        };
    }

    match value.as_str()?.trim().to_ascii_lowercase().as_str() {
        "false" | "0" => Some(false),
        "true" | "1" => Some(true),
        _ => None,
    }
}

fn value_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => non_empty_trimmed_string(value),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn non_empty_trimmed_string(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn raw_string(raw: &Value, keys: &[&str]) -> Option<String> {
    value_by_names(raw, keys).and_then(value_string)
}

fn raw_value_bool_any(raw: &Value, keys: &[&str]) -> Option<bool> {
    value_by_names(raw, keys).and_then(value_bool)
}

fn raw_value_u64_any(raw: &Value, keys: &[&str]) -> Option<u64> {
    value_by_names(raw, keys).and_then(value_u64)
}

fn raw_array<'a>(raw: &'a Value, keys: &[&str]) -> Option<&'a Vec<Value>> {
    value_by_names(raw, keys).and_then(Value::as_array)
}

fn value_by_names<'a>(raw: &'a Value, names: &[&str]) -> Option<&'a Value> {
    names.iter().find_map(|name| {
        raw.get(name).or_else(|| {
            raw.as_object()?
                .iter()
                .find(|(key, _value)| field_name_matches(key, name))
                .map(|(_key, value)| value)
        })
    })
}

fn field_name_matches(left: &str, right: &str) -> bool {
    normalized_filter_value(left) == normalized_filter_value(right)
}

fn normalized_filter_value(value: &str) -> String {
    value
        .chars()
        .filter(|ch| !matches!(ch, '-' | '_'))
        .flat_map(char::to_lowercase)
        .collect()
}

fn normalized_content_from_raw(raw: &Value) -> Option<Value> {
    value_by_names(raw, &["content"])
        .cloned()
        .map(normalize_content_value)
}

fn normalize_content_value(value: Value) -> Value {
    let Value::String(raw) = value else {
        return value;
    };

    let trimmed = raw.trim();
    if !(trimmed.starts_with('{') || trimmed.starts_with('[')) {
        return Value::String(raw);
    }

    serde_json::from_str(trimmed).unwrap_or(Value::String(raw))
}

fn conversation_scope_from_raw(raw: &Value) -> ConversationScope {
    let Some(value) = value_by_names(raw, &["conversationType", "conversation_type"]) else {
        return ConversationScope::Unknown("missing".to_string());
    };

    if let Some(value) = value.as_str() {
        return parse_conversation_scope(value);
    }

    value
        .as_u64()
        .map(|value| parse_conversation_scope(&value.to_string()))
        .unwrap_or_else(|| ConversationScope::Unknown(value.to_string()))
}

fn text_content_from_raw(raw: &Value, message_type: &MessageType) -> Option<String> {
    let from_text = value_by_names(raw, &["text"]).and_then(text_content_from_value);
    if from_text.is_some() {
        return from_text;
    }

    matches!(message_type, MessageType::Text)
        .then(|| value_by_names(raw, &["content"]).and_then(text_content_from_value))
        .flatten()
}

fn text_content_from_value(value: &Value) -> Option<String> {
    if let Some(content) = value.as_str() {
        return string_or_stringified_text_content(content);
    }

    value_by_names(value, &["content"])
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn string_or_stringified_text_content(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.starts_with('{')
        && let Ok(value) = serde_json::from_str::<Value>(trimmed)
        && let Some(content) = value_by_names(&value, &["content"]).and_then(Value::as_str)
    {
        return Some(content.to_string());
    }

    Some(value.to_string())
}

/// Normalized incoming robot event.
#[derive(Clone)]
pub struct BotEvent {
    /// DingTalk message id when supplied.
    pub message_id: Option<String>,
    /// Conversation scope.
    pub conversation_scope: ConversationScope,
    /// Message type.
    pub message_type: MessageType,
    /// Text content when available.
    pub text: Option<TextMessage>,
    /// Raw message body for non-text messages or future message types.
    pub content: Option<Value>,
    /// Typed normalized incoming message payload.
    pub message: IncomingMessage,
    /// Session webhook URL supplied by DingTalk app robot events.
    pub session_webhook: Option<String>,
    /// Open conversation id when supplied.
    pub open_conversation_id: Option<String>,
    /// Sender user id when supplied.
    pub sender_id: Option<String>,
    /// Sender staff id when supplied.
    pub sender_staff_id: Option<String>,
    /// Sender display name when supplied.
    pub sender_nick: Option<String>,
    /// Group title when supplied.
    pub conversation_title: Option<String>,
    /// Whether the sender is a group admin when supplied.
    pub is_admin: Option<bool>,
    /// Whether the bot is mentioned when supplied.
    pub is_in_at_list: Option<bool>,
    /// Session webhook expiration timestamp in milliseconds when supplied.
    pub session_webhook_expires_at_millis: Option<u64>,
    /// Users mentioned in the incoming message.
    pub at_users: Vec<AtUser>,
    /// Original JSON payload.
    pub raw: Value,
}

impl fmt::Debug for BotEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let session_webhook = self.session_webhook.as_deref().map(redact_text);
        let open_conversation_id = redacted_optional_value(&self.open_conversation_id);
        let sender_id = redacted_optional_value(&self.sender_id);
        let sender_staff_id = redacted_optional_value(&self.sender_staff_id);
        let sender_nick = self.sender_nick.as_deref().map(redact_text);
        let conversation_title = self.conversation_title.as_deref().map(redact_text);
        let content = self.content.as_ref().map(redacted_json_value);
        let raw = redacted_json_value(&self.raw);

        f.debug_struct("BotEvent")
            .field("message_id", &self.message_id)
            .field("conversation_scope", &self.conversation_scope)
            .field("message_type", &self.message_type)
            .field("text", &self.text)
            .field("content", &content)
            .field("message", &self.message)
            .field("session_webhook", &session_webhook)
            .field("open_conversation_id", &open_conversation_id)
            .field("sender_id", &sender_id)
            .field("sender_staff_id", &sender_staff_id)
            .field("sender_nick", &sender_nick)
            .field("conversation_title", &conversation_title)
            .field("is_admin", &self.is_admin)
            .field("is_in_at_list", &self.is_in_at_list)
            .field(
                "session_webhook_expires_at_millis",
                &self.session_webhook_expires_at_millis,
            )
            .field("at_users", &self.at_users)
            .field("raw", &raw)
            .finish()
    }
}

impl BotEvent {
    /// Creates an event from a DingTalk callback JSON payload.
    #[must_use]
    pub fn from_value(raw: Value) -> Self {
        let conversation_scope = conversation_scope_from_raw(&raw);
        let message_type =
            value_by_names(&raw, &["msgtype", "msgType", "msg_type"]).and_then(value_string);
        let message_type = MessageType::from_raw(message_type.as_deref());
        let text =
            text_content_from_raw(&raw, &message_type).map(|content| TextMessage { content });
        let content = normalized_content_from_raw(&raw);
        let message = IncomingMessage::from_parts(&message_type, text.clone(), content.clone());
        let session_webhook = raw_string(&raw, &["sessionWebhook", "session_webhook"]);
        let open_conversation_id = raw_string(
            &raw,
            &[
                "conversationId",
                "openConversationId",
                "open_conversation_id",
            ],
        );
        let sender_id = raw_string(&raw, &["senderId", "sender_id"]);
        let at_users = raw_array(&raw, &["atUsers", "at_users"])
            .map(|values| {
                values
                    .iter()
                    .filter_map(AtUser::from_value)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        Self {
            message_id: raw_string(&raw, &["msgId", "messageId", "msg_id", "message_id"]),
            conversation_scope,
            message_type,
            text,
            content,
            message,
            session_webhook,
            open_conversation_id,
            sender_id,
            sender_staff_id: raw_string(&raw, &["senderStaffId", "sender_staff_id"]),
            sender_nick: raw_string(&raw, &["senderNick", "sender_nick"]),
            conversation_title: raw_string(&raw, &["conversationTitle", "conversation_title"]),
            is_admin: raw_value_bool_any(&raw, &["isAdmin", "is_admin"]),
            is_in_at_list: raw_value_bool_any(&raw, &["isInAtList", "is_in_at_list"]),
            session_webhook_expires_at_millis: raw_value_u64_any(
                &raw,
                &[
                    "sessionWebhookExpiredTime",
                    "session_webhook_expires_at_millis",
                ],
            ),
            at_users,
            raw,
        }
    }

    /// Creates a text event for tests and local routing.
    #[must_use]
    pub fn text(scope: ConversationScope, content: impl Into<String>) -> Self {
        let text = TextMessage {
            content: content.into(),
        };
        Self {
            message_id: None,
            conversation_scope: scope,
            message_type: MessageType::Text,
            text: Some(text.clone()),
            content: None,
            message: IncomingMessage::Text(text),
            session_webhook: None,
            open_conversation_id: None,
            sender_id: None,
            sender_staff_id: None,
            sender_nick: None,
            conversation_title: None,
            is_admin: None,
            is_in_at_list: None,
            session_webhook_expires_at_millis: None,
            at_users: Vec::new(),
            raw: Value::Null,
        }
    }
}

/// User mentioned in an incoming bot message.
#[derive(Clone, PartialEq, Eq)]
pub struct AtUser {
    /// DingTalk id when supplied.
    pub dingtalk_id: Option<String>,
    /// Staff user id when supplied.
    pub staff_id: Option<String>,
}

impl fmt::Debug for AtUser {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AtUser")
            .field("has_dingtalk_id", &self.dingtalk_id.is_some())
            .field("has_staff_id", &self.staff_id.is_some())
            .finish()
    }
}

impl AtUser {
    fn from_value(value: &Value) -> Option<Self> {
        let dingtalk_id = raw_string(value, &["dingtalkId", "dingtalk_id"]);
        let staff_id = raw_string(value, &["staffId", "staff_id", "userId", "user_id"]);

        (dingtalk_id.is_some() || staff_id.is_some()).then_some(Self {
            dingtalk_id,
            staff_id,
        })
    }
}

/// Verifies DingTalk robot callback signatures.
#[derive(Clone)]
pub struct CallbackVerifier {
    app_secret: String,
    max_clock_skew: Duration,
}

impl fmt::Debug for CallbackVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CallbackVerifier")
            .field("app_secret", &"<redacted>")
            .field("max_clock_skew", &self.max_clock_skew)
            .finish()
    }
}

impl CallbackVerifier {
    /// Creates a verifier from the robot app secret.
    pub fn new(app_secret: impl Into<String>) -> Result<Self> {
        let app_secret = app_secret.into();
        validate_callback_secret(&app_secret)?;

        Ok(Self {
            app_secret,
            max_clock_skew: Duration::from_secs(300),
        })
    }

    /// Sets the maximum accepted clock skew.
    #[must_use]
    pub fn max_clock_skew(mut self, value: Duration) -> Self {
        self.max_clock_skew = value;
        self
    }

    /// Verifies callback `timestamp` and `sign` header values.
    pub fn verify(&self, timestamp_millis: &str, sign: &str) -> Result<()> {
        self.verify_at(timestamp_millis, sign, SystemTime::now())
    }

    /// Verifies optional callback header values.
    pub fn verify_optional(
        &self,
        timestamp_millis: Option<&str>,
        sign: Option<&str>,
    ) -> Result<()> {
        let timestamp_millis =
            timestamp_millis.ok_or_else(|| Error::invalid_signature("missing timestamp header"))?;
        let sign = sign.ok_or_else(|| Error::invalid_signature("missing sign header"))?;
        self.verify(timestamp_millis, sign)
    }

    fn verify_at(&self, timestamp_millis: &str, sign: &str, now: SystemTime) -> Result<()> {
        let timestamp_millis = timestamp_millis.trim();
        let sign = sign.trim();
        let timestamp = parse_timestamp_millis(timestamp_millis)?;
        let now = now
            .duration_since(UNIX_EPOCH)
            .map_err(|source| Error::invalid_signature(source.to_string()))?
            .as_millis();
        let delta = timestamp.abs_diff(now);
        if delta > self.max_clock_skew.as_millis() {
            return Err(Error::invalid_signature(
                "timestamp is outside accepted clock skew",
            ));
        }

        let sign = urlencoding::decode(sign).map_err(|source| {
            Error::invalid_signature(format!("invalid sign encoding: {source}"))
        })?;
        let actual = STANDARD
            .decode(sign.as_bytes())
            .map_err(|source| Error::invalid_signature(format!("invalid sign base64: {source}")))?;

        verify_callback_signature(timestamp_millis, &self.app_secret, &actual)
    }
}

fn validate_callback_secret(value: &str) -> Result<()> {
    if value.chars().any(char::is_control) {
        return Err(Error::invalid_input(
            "app_secret",
            "value must not contain control characters",
        ));
    }
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(Error::invalid_input(
            "app_secret",
            "value must not be empty",
        ));
    }
    if trimmed != value {
        return Err(Error::invalid_input(
            "app_secret",
            "value must not contain leading or trailing whitespace",
        ));
    }
    if value.chars().any(char::is_whitespace) {
        return Err(Error::invalid_input(
            "app_secret",
            "value must not contain whitespace",
        ));
    }
    Ok(())
}

/// DingTalk callback header values.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct CallbackHeaders {
    /// DingTalk `timestamp` header.
    pub timestamp: Option<String>,
    /// DingTalk `sign` header.
    pub sign: Option<String>,
}

impl fmt::Debug for CallbackHeaders {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CallbackHeaders")
            .field("timestamp", &self.timestamp)
            .field("sign", &redacted_optional_value(&self.sign))
            .finish()
    }
}

impl CallbackHeaders {
    /// Creates callback headers from optional values.
    #[must_use]
    pub fn new(timestamp: Option<impl Into<String>>, sign: Option<impl Into<String>>) -> Self {
        Self {
            timestamp: timestamp.map(Into::into),
            sign: sign.map(Into::into),
        }
    }

    fn timestamp(&self) -> Option<&str> {
        self.timestamp.as_deref()
    }

    fn sign(&self) -> Option<&str> {
        self.sign.as_deref()
    }
}

/// DingTalk callback request parts independent of any HTTP framework.
#[derive(Clone, PartialEq, Eq)]
pub struct CallbackRequest<B> {
    /// Callback headers.
    pub headers: CallbackHeaders,
    /// Raw request body.
    pub body: B,
}

impl<B> fmt::Debug for CallbackRequest<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CallbackRequest")
            .field("headers", &self.headers)
            .field("body", &"<redacted>")
            .finish()
    }
}

impl<B> CallbackRequest<B> {
    /// Creates callback request parts.
    #[must_use]
    pub fn new(headers: CallbackHeaders, body: B) -> Self {
        Self { headers, body }
    }
}

fn parse_timestamp_millis(value: &str) -> Result<u128> {
    value
        .trim()
        .parse::<u128>()
        .map_err(|source| Error::invalid_signature(format!("invalid timestamp: {source}")))
}

fn verify_callback_signature(
    timestamp_millis: &str,
    app_secret: &str,
    actual: &[u8],
) -> Result<()> {
    let string_to_sign = format!("{timestamp_millis}\n{app_secret}");
    let mut mac =
        Hmac::<Sha256>::new_from_slice(app_secret.as_bytes()).map_err(|_error| Error::Signature)?;
    mac.update(string_to_sign.as_bytes());
    mac.verify_slice(actual)
        .map_err(|_error| Error::invalid_signature("signature mismatch"))
}

/// HTTP response body helpers for DingTalk robot callbacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BotAck {
    /// Callback was accepted.
    Ok,
}

impl BotAck {
    /// Returns the response body expected by DingTalk callback mode.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "OK",
        }
    }

    /// Returns whether this ACK accepts the callback.
    #[must_use]
    pub fn is_ok(self) -> bool {
        matches!(self, Self::Ok)
    }
}

impl fmt::Display for BotAck {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

fn parse_conversation_scope(value: &str) -> ConversationScope {
    let value = value.trim();
    if value.is_empty() {
        return ConversationScope::Unknown("missing".to_string());
    }

    match normalized_filter_value(value).as_str() {
        "any" => ConversationScope::Any,
        "1" | "private" | "single" | "oto" => ConversationScope::Private,
        "2" | "group" => ConversationScope::Group,
        _ => ConversationScope::Unknown(value.to_string()),
    }
}

fn command_args<'a>(text: &'a str, command: &str) -> Option<&'a str> {
    let mut text = text.trim_start();
    loop {
        if let Some(args) = command_args_at(text, command) {
            return Some(args);
        }

        text = strip_leading_mention(text)?.trim_start();
    }
}

fn command_args_at<'a>(text: &'a str, command: &str) -> Option<&'a str> {
    let rest = text.strip_prefix(command)?;
    if rest.is_empty() || rest.starts_with(char::is_whitespace) {
        Some(rest.trim_start())
    } else {
        None
    }
}

fn strip_leading_mention(text: &str) -> Option<&str> {
    let without_at = text.trim_start().strip_prefix('@')?;
    for (index, ch) in without_at.char_indices() {
        if !ch.is_whitespace() {
            continue;
        }
        if index == 0 {
            return None;
        }
        return Some(&without_at[index..]);
    }
    None
}

/// Marker for any conversation context.
#[derive(Debug, Clone, Copy)]
pub struct AnyScope;

/// Marker for group-only context.
#[derive(Debug, Clone, Copy)]
pub struct GroupScope;

/// Marker for private-only context.
#[derive(Debug, Clone, Copy)]
pub struct PrivateScope;

/// Bot handler context.
#[derive(Clone)]
pub struct BotContext<S = AnyScope> {
    client: DingTalk,
    event: BotEvent,
    command: Option<Arc<str>>,
    state: Option<BotState>,
    _scope: PhantomData<S>,
}

/// Context accepted by routes for any conversation.
pub type AnyContext = BotContext<AnyScope>;

/// Short alias for [`AnyContext`], intended for general-purpose bot handlers.
pub type Context = AnyContext;

/// Context accepted by group-only routes.
pub type GroupContext = BotContext<GroupScope>;

/// Context accepted by private-only routes.
pub type PrivateContext = BotContext<PrivateScope>;

impl<S> BotContext<S> {
    fn new(
        client: DingTalk,
        event: BotEvent,
        command: Option<Arc<str>>,
        state: Option<BotState>,
    ) -> Self {
        Self {
            client,
            event,
            command,
            state,
            _scope: PhantomData,
        }
    }

    /// Returns the underlying SDK client.
    #[must_use]
    pub fn client(&self) -> &DingTalk {
        &self.client
    }

    /// Returns the normalized event.
    #[must_use]
    pub fn event(&self) -> &BotEvent {
        &self.event
    }

    /// Returns text content when the event is text.
    #[must_use]
    pub fn text(&self) -> Option<&str> {
        self.event.message.text()
    }

    /// Returns the typed incoming message payload.
    #[must_use]
    pub fn message(&self) -> &IncomingMessage {
        &self.event.message
    }

    /// Returns the text message payload when this is a text message.
    #[must_use]
    pub fn text_message(&self) -> Option<&TextMessage> {
        self.event.message.text_message()
    }

    /// Returns the markdown payload when this is a markdown message.
    #[must_use]
    pub fn markdown_message(&self) -> Option<&Value> {
        self.event.message.markdown()
    }

    /// Returns the audio payload when this is an audio message.
    #[must_use]
    pub fn audio_message(&self) -> Option<&AudioMessage> {
        self.event.message.audio()
    }

    /// Returns the picture payload when this is a picture message.
    #[must_use]
    pub fn picture_message(&self) -> Option<&PictureMessage> {
        self.event.message.picture()
    }

    /// Returns the video payload when this is a video message.
    #[must_use]
    pub fn video_message(&self) -> Option<&VideoMessage> {
        self.event.message.video()
    }

    /// Returns the file payload when this is a file message.
    #[must_use]
    pub fn file_message(&self) -> Option<&FileMessage> {
        self.event.message.file()
    }

    /// Returns the rich-text payload when this is a rich-text message.
    #[must_use]
    pub fn rich_text_message(&self) -> Option<&RichTextMessage> {
        self.event.message.rich_text()
    }

    /// Returns normalized message content when DingTalk supplied a `content` field.
    #[must_use]
    pub fn content(&self) -> Option<&Value> {
        self.event.content.as_ref()
    }

    /// Returns shared application state when it was configured with this type.
    #[must_use]
    pub fn state<T>(&self) -> Option<&T>
    where
        T: Send + Sync + 'static,
    {
        self.state.as_deref()?.downcast_ref::<T>()
    }

    /// Returns shared application state or an invalid-configuration error.
    pub fn state_required<T>(&self) -> Result<&T>
    where
        T: Send + Sync + 'static,
    {
        self.state::<T>().ok_or_else(|| {
            Error::InvalidConfig(format!(
                "bot state `{}` is not configured",
                type_name::<T>()
            ))
        })
    }

    /// Returns the matched command prefix when this route was command-filtered.
    #[must_use]
    pub fn command(&self) -> Option<&str> {
        self.command.as_deref()
    }

    /// Returns text after the matched command prefix, with leading whitespace removed.
    #[must_use]
    pub fn args(&self) -> Option<&str> {
        let command = self.command.as_deref()?;
        command_args(self.text()?, command)
    }

    /// Returns command arguments, or an empty string when this is not a command route.
    #[must_use]
    pub fn args_or_empty(&self) -> &str {
        self.args().unwrap_or_default()
    }

    /// Returns whether command arguments are present after trimming whitespace.
    #[must_use]
    pub fn has_args(&self) -> bool {
        self.args().is_some_and(|args| !args.is_empty())
    }

    /// Returns the `index`-th whitespace-separated command argument.
    #[must_use]
    pub fn arg(&self, index: usize) -> Option<&str> {
        self.args_iter().nth(index)
    }

    /// Iterates whitespace-separated command arguments without allocating.
    #[must_use]
    pub fn args_iter(&self) -> std::str::SplitWhitespace<'_> {
        self.args_or_empty().split_whitespace()
    }

    /// Returns whitespace-separated command arguments.
    #[must_use]
    pub fn args_vec(&self) -> Vec<&str> {
        self.args_iter().collect()
    }

    /// Returns the conversation scope.
    #[must_use]
    pub fn scope(&self) -> &ConversationScope {
        &self.event.conversation_scope
    }

    /// Returns the incoming message type.
    #[must_use]
    pub fn message_type(&self) -> &MessageType {
        &self.event.message_type
    }

    /// Returns whether this message came from a group conversation.
    #[must_use]
    pub fn is_group(&self) -> bool {
        self.event.conversation_scope == ConversationScope::Group
    }

    /// Returns whether this message came from a private conversation.
    #[must_use]
    pub fn is_private(&self) -> bool {
        self.event.conversation_scope == ConversationScope::Private
    }

    /// Returns DingTalk message id when supplied.
    #[must_use]
    pub fn message_id(&self) -> Option<&str> {
        self.event.message_id.as_deref()
    }

    /// Returns open conversation id when supplied.
    #[must_use]
    pub fn conversation_id(&self) -> Option<&str> {
        self.event.open_conversation_id.as_deref()
    }

    /// Returns group title when supplied.
    #[must_use]
    pub fn conversation_title(&self) -> Option<&str> {
        self.event.conversation_title.as_deref()
    }

    /// Returns sender display name when supplied.
    #[must_use]
    pub fn sender_name(&self) -> Option<&str> {
        self.event.sender_nick.as_deref()
    }

    /// Returns sender id when supplied.
    #[must_use]
    pub fn sender_id(&self) -> Option<&str> {
        self.event.sender_id.as_deref()
    }

    /// Returns sender staff id when supplied.
    #[must_use]
    pub fn sender_staff_id(&self) -> Option<&str> {
        self.event.sender_staff_id.as_deref()
    }

    /// Returns whether the sender is a group admin when supplied.
    #[must_use]
    pub fn is_admin(&self) -> Option<bool> {
        self.event.is_admin
    }

    /// Returns whether the bot was mentioned when DingTalk supplied the flag.
    #[must_use]
    pub fn is_at_bot(&self) -> Option<bool> {
        self.event.is_in_at_list
    }

    /// Returns users mentioned in the incoming message.
    #[must_use]
    pub fn at_users(&self) -> &[AtUser] {
        &self.event.at_users
    }

    /// Builds webhook mention metadata for users mentioned in the incoming message.
    #[cfg(feature = "webhook")]
    #[must_use]
    pub fn mentioned_users_at(&self) -> crate::webhook::At {
        crate::webhook::At::from_mentioned_users(self.at_users())
    }

    /// Builds webhook mention metadata for the sender, when DingTalk supplied a staff id.
    #[cfg(feature = "webhook")]
    #[must_use]
    pub fn sender_mention(&self) -> crate::webhook::At {
        let Some(staff_id) = self.sender_staff_id() else {
            return crate::webhook::At::new();
        };

        let at = crate::webhook::At::new().user_id(staff_id);
        if at.validate().is_ok() {
            at
        } else {
            crate::webhook::At::new()
        }
    }

    /// Returns the session webhook URL supplied by DingTalk, when present.
    #[must_use]
    pub fn session_webhook(&self) -> Option<&str> {
        self.event.session_webhook.as_deref()
    }

    /// Returns the session webhook expiration timestamp in milliseconds, when supplied.
    #[must_use]
    pub fn session_webhook_expires_at_millis(&self) -> Option<u64> {
        self.event.session_webhook_expires_at_millis
    }

    /// Returns the session webhook expiration time, when supplied.
    #[must_use]
    pub fn session_webhook_expires_at(&self) -> Option<SystemTime> {
        self.event
            .session_webhook_expires_at_millis
            .and_then(|millis| UNIX_EPOCH.checked_add(Duration::from_millis(millis)))
    }

    /// Returns whether DingTalk says the session webhook is already expired.
    ///
    /// Missing expiration metadata is treated as not expired.
    #[must_use]
    pub fn is_session_webhook_expired(&self) -> bool {
        let Some(expires_at_millis) = self.event.session_webhook_expires_at_millis else {
            return false;
        };

        UNIX_EPOCH
            .checked_add(Duration::from_millis(expires_at_millis))
            .is_none_or(|expires_at| expires_at <= SystemTime::now())
    }

    /// Returns the original incoming JSON payload.
    #[must_use]
    pub fn raw(&self) -> &Value {
        &self.event.raw
    }

    /// Replies through the session webhook supplied by DingTalk.
    #[cfg(feature = "webhook")]
    pub async fn reply_text(&self, content: impl Into<String>) -> Result<()> {
        self.reply_text_response(content).await?;
        Ok(())
    }

    /// Replies through the session webhook and returns DingTalk response metadata.
    #[cfg(feature = "webhook")]
    pub async fn reply_text_response(
        &self,
        content: impl Into<String>,
    ) -> Result<crate::webhook::WebhookResponse> {
        self.reply_message_response(crate::webhook::WebhookMessage::text(content))
            .await
    }

    /// Replies with a text message and mention metadata.
    #[cfg(feature = "webhook")]
    pub async fn reply_text_with_at(
        &self,
        content: impl Into<String>,
        at: crate::webhook::At,
    ) -> Result<()> {
        self.reply_text_with_at_response(content, at).await?;
        Ok(())
    }

    /// Replies with a text message and mention metadata, returning DingTalk response metadata.
    #[cfg(feature = "webhook")]
    pub async fn reply_text_with_at_response(
        &self,
        content: impl Into<String>,
        at: crate::webhook::At,
    ) -> Result<crate::webhook::WebhookResponse> {
        self.reply_message_response(crate::webhook::WebhookMessage::text(content).at(at)?)
            .await
    }

    /// Replies with a markdown message through the session webhook supplied by DingTalk.
    #[cfg(feature = "webhook")]
    pub async fn reply_markdown(
        &self,
        title: impl Into<String>,
        text: impl Into<String>,
    ) -> Result<()> {
        self.reply_markdown_response(title, text).await?;
        Ok(())
    }

    /// Replies with a markdown message and returns DingTalk response metadata.
    #[cfg(feature = "webhook")]
    pub async fn reply_markdown_response(
        &self,
        title: impl Into<String>,
        text: impl Into<String>,
    ) -> Result<crate::webhook::WebhookResponse> {
        self.reply_message_response(crate::webhook::WebhookMessage::markdown(title, text))
            .await
    }

    /// Replies with a markdown message and mention metadata.
    #[cfg(feature = "webhook")]
    pub async fn reply_markdown_with_at(
        &self,
        title: impl Into<String>,
        text: impl Into<String>,
        at: crate::webhook::At,
    ) -> Result<()> {
        self.reply_markdown_with_at_response(title, text, at)
            .await?;
        Ok(())
    }

    /// Replies with a markdown message and mention metadata, returning DingTalk response metadata.
    #[cfg(feature = "webhook")]
    pub async fn reply_markdown_with_at_response(
        &self,
        title: impl Into<String>,
        text: impl Into<String>,
        at: crate::webhook::At,
    ) -> Result<crate::webhook::WebhookResponse> {
        self.reply_message_response(crate::webhook::WebhookMessage::markdown(title, text).at(at)?)
            .await
    }

    /// Replies with a typed webhook message through the session webhook supplied by DingTalk.
    #[cfg(feature = "webhook")]
    pub async fn reply_message(&self, message: crate::webhook::WebhookMessage) -> Result<()> {
        self.reply_message_response(message).await?;
        Ok(())
    }

    /// Replies with a typed webhook message and returns DingTalk response metadata.
    #[cfg(feature = "webhook")]
    pub async fn reply_message_response(
        &self,
        message: crate::webhook::WebhookMessage,
    ) -> Result<crate::webhook::WebhookResponse> {
        let webhook = self.session_webhook_available()?;
        self.client
            .session_webhook(webhook)?
            .send_message(message)
            .await
    }

    #[cfg(feature = "webhook")]
    fn session_webhook_available(&self) -> Result<&str> {
        let webhook = self
            .event
            .session_webhook
            .as_deref()
            .ok_or_else(|| Error::InvalidConfig("event has no sessionWebhook".to_string()))?;

        if self.is_session_webhook_expired() {
            return Err(Error::InvalidConfig(
                "event sessionWebhook has expired".to_string(),
            ));
        }

        Ok(webhook)
    }
}

impl BotContext<AnyScope> {
    /// Converts this context into a group-only context.
    pub fn into_group(self) -> Result<GroupContext> {
        self.into_scope(ConversationScope::Group)
    }

    /// Converts this context into a private-only context.
    pub fn into_private(self) -> Result<PrivateContext> {
        self.into_scope(ConversationScope::Private)
    }

    fn into_scope<T>(self, expected: ConversationScope) -> Result<BotContext<T>> {
        if expected.accepts(&self.event.conversation_scope) {
            Ok(BotContext {
                client: self.client,
                event: self.event,
                command: self.command,
                state: self.state,
                _scope: PhantomData,
            })
        } else {
            Err(Error::BotScope {
                expected: expected.name(),
                actual: self.event.conversation_scope.actual_name(),
            })
        }
    }
}

/// Bot route.
#[derive(Clone)]
pub struct Route {
    scope: ConversationScope,
    message_type: MessageType,
    commands: Option<Vec<Arc<str>>>,
    handler: Option<BoxedHandler>,
}

impl Route {
    /// Creates a route for a conversation scope.
    #[must_use]
    pub fn new(scope: ConversationScope) -> Self {
        Self {
            scope,
            message_type: MessageType::Any,
            commands: None,
            handler: None,
        }
    }

    /// Restricts the route to a message type.
    #[must_use]
    pub fn message_type(mut self, message_type: MessageType) -> Self {
        self.message_type = message_type;
        self
    }

    /// Restricts text messages to a command prefix.
    #[must_use]
    pub fn command(mut self, command: impl Into<String>) -> Self {
        self.commands = Some(collect_commands([command]));
        self
    }

    /// Restricts text messages to one of several command prefixes.
    #[must_use]
    pub fn commands<I, S>(mut self, commands: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.commands = Some(collect_commands(commands));
        self
    }

    /// Attaches a context-only handler returning `()` or an SDK/application result.
    ///
    /// The complete event is available through [`BotContext::event`].
    /// Use [`crate::HandlerResult`] for named functions combining SDK and application errors,
    /// or [`crate::handler_future`] inside a closure to infer its result's error type.
    ///
    /// ```
    /// # use dingding::{Result, bot::{Route, ConversationScope}};
    /// # fn main() -> Result<()> {
    /// let route = Route::new(ConversationScope::Any).handle(|_| async {});
    /// route.validate()?;
    /// # Ok(()) }
    /// ```
    #[must_use]
    pub fn handle<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(BotContext) -> Fut + Send + Sync + 'static,
        Fut: Future + Send + 'static,
        Fut::Output: IntoHandlerResult,
    {
        self.handler = Some(Arc::new(move |ctx| {
            let future = handler(ctx);
            Box::pin(async move { future.await.into_handler_result() })
        }));
        self
    }

    /// Attaches a group-only handler and restricts the route to group conversations.
    #[must_use]
    pub fn handle_group<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(GroupContext) -> Fut + Send + Sync + 'static,
        Fut: Future + Send + 'static,
        Fut::Output: IntoHandlerResult,
    {
        self.scope = ConversationScope::Group;
        let handler = Arc::new(handler);
        self.handler = Some(Arc::new(move |ctx| {
            let handler = Arc::clone(&handler);
            Box::pin(async move {
                let ctx = ctx.into_group()?;
                handler(ctx).await.into_handler_result()
            })
        }));
        self
    }

    /// Attaches a private-chat-only handler and restricts the route to private conversations.
    #[must_use]
    pub fn handle_private<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(PrivateContext) -> Fut + Send + Sync + 'static,
        Fut: Future + Send + 'static,
        Fut::Output: IntoHandlerResult,
    {
        self.scope = ConversationScope::Private;
        let handler = Arc::new(handler);
        self.handler = Some(Arc::new(move |ctx| {
            let handler = Arc::clone(&handler);
            Box::pin(async move {
                let ctx = ctx.into_private()?;
                handler(ctx).await.into_handler_result()
            })
        }));
        self
    }

    /// Validates this route configuration.
    pub fn validate(&self) -> Result<()> {
        if self.handler.is_none() {
            return Err(Error::InvalidConfig(
                "bot route handler is required".to_string(),
            ));
        }
        if let Some(commands) = &self.commands {
            if !matches!(self.message_type, MessageType::Any | MessageType::Text) {
                return Err(Error::InvalidConfig(
                    "text commands require MessageType::Text or MessageType::Any".to_string(),
                ));
            }
            validate_commands(commands)?;
        }
        Ok(())
    }

    fn matches(&self, event: &BotEvent) -> bool {
        self.scope.accepts(&event.conversation_scope)
            && self.message_type.accepts(&event.message_type)
            && self.command_matches(event)
    }

    pub(crate) fn validate_after(&self, previous: &[Self], kind: &str) -> Result<()> {
        self.validate()?;
        for (index, route) in previous.iter().enumerate() {
            let scopes_overlap =
                self.scope.accepts(&route.scope) || route.scope.accepts(&self.scope);
            if scopes_overlap
                && let (Some(commands), Some(existing)) = (&self.commands, &route.commands)
                && let Some(command) = commands.iter().find(|command| existing.contains(command))
            {
                return Err(Error::InvalidConfig(format!(
                    "{kind}[{}] command `{command}` overlaps {kind}[{index}] in conversation scope; use one handler or disjoint group/private scopes",
                    previous.len(),
                )));
            }
            let message_type = if self.commands.is_some() {
                &MessageType::Text
            } else {
                &self.message_type
            };
            if route.commands.is_none()
                && route.scope.accepts(&self.scope)
                && route.message_type.accepts(message_type)
            {
                return Err(Error::InvalidConfig(format!(
                    "{kind}[{}] is unreachable because {kind}[{index}] matches all its messages; register specific handlers first and catch-all handlers last",
                    previous.len(),
                )));
            }
        }
        Ok(())
    }

    fn command_matches(&self, event: &BotEvent) -> bool {
        match &self.commands {
            Some(_commands) => self.matched_command(event).is_some(),
            None => true,
        }
    }

    fn matched_command(&self, event: &BotEvent) -> Option<Arc<str>> {
        let text = event.text.as_ref()?;
        self.commands
            .as_ref()?
            .iter()
            .find(|command| command_args(&text.content, command).is_some())
            .cloned()
    }
}

fn collect_commands<I, S>(commands: I) -> Vec<Arc<str>>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    commands
        .into_iter()
        .map(|command| Arc::<str>::from(command.into()))
        .collect()
}

fn validate_commands(commands: &[Arc<str>]) -> Result<()> {
    if commands.is_empty() {
        return Err(Error::invalid_input(
            "commands",
            "at least one text command is required",
        ));
    }

    for (index, command) in commands.iter().enumerate() {
        validate_command(command)?;
        if commands[..index]
            .iter()
            .any(|existing| existing.as_ref() == command.as_ref())
        {
            return Err(Error::invalid_input(
                "commands",
                "text command aliases must be unique",
            ));
        }
    }
    Ok(())
}

fn validate_command(command: &str) -> Result<()> {
    if command.chars().any(char::is_control) {
        return Err(Error::invalid_input(
            "command",
            "text command must not contain control characters",
        ));
    }
    if command.trim().is_empty() {
        return Err(Error::invalid_input(
            "command",
            "text command must not be empty",
        ));
    }
    if command.trim() != command {
        return Err(Error::invalid_input(
            "command",
            "text command must not contain leading or trailing whitespace",
        ));
    }
    if command.chars().any(char::is_whitespace) {
        return Err(Error::invalid_input(
            "command",
            "text command must not contain whitespace",
        ));
    }
    Ok(())
}

fn dispatch_route(
    client: &DingTalk,
    state: Option<&BotState>,
    route: &Route,
    event: BotEvent,
) -> Option<BoxFuture> {
    let handler = route.handler.as_ref()?;
    let command = route.matched_command(&event);
    let ctx = BotContext::new(client.clone(), event, command, state.cloned());
    Some(handler(ctx))
}

#[derive(Clone)]
enum BotValidationError {
    InvalidConfig(String),
    InvalidInput {
        field: &'static str,
        message: String,
    },
}

impl BotValidationError {
    fn from_error(error: Error) -> Self {
        match error {
            Error::InvalidConfig(message) => Self::InvalidConfig(message),
            Error::InvalidInput { field, message } => Self::InvalidInput { field, message },
            error => Self::InvalidConfig(error.to_string()),
        }
    }

    fn to_error(&self) -> Error {
        match self {
            Self::InvalidConfig(message) => Error::InvalidConfig(message.clone()),
            Self::InvalidInput { field, message } => Error::invalid_input(field, message.clone()),
        }
    }
}

/// Bot router and dispatcher.
#[derive(Clone)]
pub struct Bot {
    client: DingTalk,
    routes: Vec<Route>,
    fallbacks: Vec<Route>,
    state: Option<BotState>,
    validation_error: Option<BotValidationError>,
    deduplicator: Arc<dyn EventDeduplicator>,
}

impl Bot {
    /// Creates a bot router.
    #[must_use]
    pub fn new(client: DingTalk) -> Self {
        Self {
            client,
            routes: Vec::new(),
            fallbacks: Vec::new(),
            state: None,
            validation_error: None,
            deduplicator: Arc::new(MemoryEventDeduplicator::default()),
        }
    }

    /// Uses a shared event deduplication backend for callbacks with a message id.
    #[must_use]
    pub fn deduplicator(mut self, store: Arc<dyn EventDeduplicator>) -> Self {
        self.deduplicator = store;
        self
    }

    /// Configures shared application state available from [`BotContext::state`].
    #[must_use]
    pub fn state<T>(mut self, state: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        self.state = Some(Arc::new(state));
        self
    }

    #[cfg(feature = "stream")]
    pub(crate) fn state_arc(mut self, state: BotState) -> Self {
        self.state = Some(state);
        self
    }

    #[cfg(feature = "stream")]
    pub(crate) fn shared_state(&self) -> Option<BotState> {
        self.state.clone()
    }

    #[cfg(feature = "stream")]
    pub(crate) fn with_app_credentials(
        mut self,
        credentials: crate::auth::AppCredentials,
    ) -> Result<Self> {
        self.client = self.client.with_app_credentials(credentials)?;
        Ok(self)
    }

    #[cfg(feature = "stream")]
    pub(crate) fn has_routes(&self) -> bool {
        !self.routes.is_empty() || !self.fallbacks.is_empty()
    }

    /// Appends a route. The first matching normal route handles each event.
    ///
    /// Duplicate commands in overlapping scopes and routes fully shadowed by an
    /// earlier catch-all are rejected by [`Self::validate`].
    #[must_use]
    pub fn route(mut self, route: Route) -> Self {
        self.record_validation(route.validate_after(&self.routes, "routes"));
        self.routes.push(route);
        self
    }

    /// Appends a fallback route, used only when no normal route matches.
    ///
    /// Fallbacks accumulate and are matched in registration order. Register
    /// scope/message-specific fallbacks before a catch-all. Unreachable fallbacks
    /// are rejected by [`Self::validate`] instead of silently replacing a handler.
    #[must_use]
    pub fn fallback_route(mut self, route: Route) -> Self {
        self.record_validation(route.validate_after(&self.fallbacks, "fallbacks"));
        self.fallbacks.push(route);
        self
    }

    /// Dispatches an event to the first matching route.
    pub async fn handle_event(&self, event: BotEvent) -> Result<HandleOutcome> {
        self.validate()?;

        let Some(message_id) = event.message_id.as_deref().filter(|id| !id.is_empty()) else {
            return self.dispatch_event(event).await;
        };
        let key = serde_json::to_string(&(
            "bot",
            &event.raw.get("robotCode"),
            &event.open_conversation_id,
            &event.sender_id,
            message_id,
        ))?;
        match self.deduplicator.claim(&key).await? {
            Deduplication::Acquired(lease) => {
                let outcome = self.dispatch_event(event).await?;
                lease
                    .complete(Value::String(outcome.as_str().to_owned()))
                    .await?;
                Ok(outcome)
            }
            Deduplication::Completed(value) => match value.as_str() {
                Some("matched") => Ok(HandleOutcome::Matched),
                Some("fallback") => Ok(HandleOutcome::Fallback),
                Some("ignored") => Ok(HandleOutcome::Ignored),
                _ => Err(Error::InvalidConfig("invalid cached bot outcome".into())),
            },
            Deduplication::InFlight => Err(Error::invalid_input(
                "message_id",
                "event is already being processed",
            )),
        }
    }

    async fn dispatch_event(&self, event: BotEvent) -> Result<HandleOutcome> {
        for route in &self.routes {
            if !route.matches(&event) {
                continue;
            }

            if let Some(handler) =
                dispatch_route(&self.client, self.state.as_ref(), route, event.clone())
            {
                handler.await?;
                return Ok(HandleOutcome::Matched);
            }
        }

        for route in &self.fallbacks {
            if route.matches(&event)
                && let Some(handler) =
                    dispatch_route(&self.client, self.state.as_ref(), route, event.clone())
            {
                handler.await?;
                return Ok(HandleOutcome::Fallback);
            }
        }

        Ok(HandleOutcome::Ignored)
    }

    /// Validates all configured routes.
    pub fn validate(&self) -> Result<()> {
        if let Some(error) = &self.validation_error {
            return Err(error.to_error());
        }
        Ok(())
    }

    fn record_validation(&mut self, result: Result<()>) {
        if self.validation_error.is_none()
            && let Err(error) = result
        {
            self.validation_error = Some(BotValidationError::from_error(error));
        }
    }

    /// Parses and dispatches an event from JSON.
    pub async fn handle_json(&self, value: Value) -> Result<HandleOutcome> {
        self.handle_event(BotEvent::from_value(value)).await
    }

    /// Verifies, parses, and dispatches a DingTalk callback JSON value.
    pub async fn handle_callback_json(
        &self,
        verifier: &CallbackVerifier,
        timestamp_millis: &str,
        sign: &str,
        value: Value,
    ) -> Result<BotAck> {
        verifier.verify(timestamp_millis, sign)?;
        self.handle_json(value).await?;
        Ok(BotAck::Ok)
    }

    /// Verifies, parses, and dispatches a DingTalk callback body.
    pub async fn handle_callback_bytes(
        &self,
        verifier: &CallbackVerifier,
        timestamp_millis: &str,
        sign: &str,
        body: impl AsRef<[u8]>,
    ) -> Result<BotAck> {
        verifier.verify(timestamp_millis, sign)?;
        let value = serde_json::from_slice(body.as_ref())?;
        self.handle_json(value).await?;
        Ok(BotAck::Ok)
    }

    /// Verifies optional header values, then parses and dispatches a callback body.
    pub async fn handle_callback_parts(
        &self,
        verifier: &CallbackVerifier,
        timestamp_millis: Option<&str>,
        sign: Option<&str>,
        body: impl AsRef<[u8]>,
    ) -> Result<BotAck> {
        verifier.verify_optional(timestamp_millis, sign)?;
        let value = serde_json::from_slice(body.as_ref())?;
        self.handle_json(value).await?;
        Ok(BotAck::Ok)
    }

    /// Verifies, parses, and dispatches framework-independent callback request parts.
    pub async fn handle_callback_request<B>(
        &self,
        verifier: &CallbackVerifier,
        request: CallbackRequest<B>,
    ) -> Result<BotAck>
    where
        B: AsRef<[u8]>,
    {
        self.handle_callback_parts(
            verifier,
            request.headers.timestamp(),
            request.headers.sign(),
            request.body,
        )
        .await
    }
}

/// Result of dispatching an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandleOutcome {
    /// A route matched and its handler completed.
    Matched,
    /// No normal route matched, and the fallback handler completed.
    Fallback,
    /// No route matched.
    Ignored,
}

impl HandleOutcome {
    /// Returns a stable lowercase label for logs and metrics.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Matched => "matched",
            Self::Fallback => "fallback",
            Self::Ignored => "ignored",
        }
    }

    /// Returns whether a normal route matched.
    #[must_use]
    pub fn is_matched(self) -> bool {
        matches!(self, Self::Matched)
    }

    /// Returns whether the fallback route handled the event.
    #[must_use]
    pub fn is_fallback(self) -> bool {
        matches!(self, Self::Fallback)
    }

    /// Returns whether no route handled the event.
    #[must_use]
    pub fn is_ignored(self) -> bool {
        matches!(self, Self::Ignored)
    }
}

impl fmt::Display for HandleOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    };

    use super::*;

    #[test]
    fn conversation_scope_exposes_parse_and_display_helpers() {
        let any = ConversationScope::from_dingtalk_value("Any");
        let group = ConversationScope::from_dingtalk_value("2");
        let private: ConversationScope = "single".parse().expect("scope parse");
        let missing = ConversationScope::from_dingtalk_value(" ");
        let unknown = ConversationScope::from("future");

        assert_eq!(any, ConversationScope::Any);
        assert_eq!(group, ConversationScope::Group);
        assert_eq!(private, ConversationScope::Private);
        assert_eq!(missing, ConversationScope::Unknown("missing".to_string()));
        assert_eq!(group.as_str(), "group");
        assert_eq!(private.to_string(), "private");
        assert!(group.is_group());
        assert!(private.is_private());
        assert!(unknown.is_unknown());
        assert_eq!(unknown.as_str(), "future");
        assert!(ConversationScope::Any.accepts(&ConversationScope::Group));
    }

    #[test]
    fn message_type_exposes_parse_and_display_helpers() {
        let any = MessageType::from_dingtalk_value("any");
        let text = MessageType::from_dingtalk_value(" TEXT ");
        let rich_text: MessageType = "rich-text".parse().expect("message type parse");
        let image = MessageType::from_dingtalk_value("image");
        let voice = MessageType::from_dingtalk_value("voice");
        let unknown = MessageType::from("sticker");
        let missing = MessageType::from_dingtalk_value(" ");

        assert_eq!(any, MessageType::Any);
        assert_eq!(text, MessageType::Text);
        assert_eq!(rich_text, MessageType::RichText);
        assert_eq!(image, MessageType::Picture);
        assert_eq!(voice, MessageType::Audio);
        assert_eq!(missing, MessageType::Unknown("missing".to_string()));
        assert_eq!(text.as_str(), "text");
        assert_eq!(rich_text.to_string(), "richText");
        assert!(text.is_text());
        assert!(unknown.is_unknown());
        assert_eq!(unknown.as_str(), "sticker");
        assert!(MessageType::Any.accepts(&MessageType::Picture));
        assert!(!MessageType::Text.accepts(&MessageType::Picture));
    }

    #[test]
    fn handle_outcome_exposes_stable_labels() {
        assert_eq!(HandleOutcome::Matched.as_str(), "matched");
        assert_eq!(HandleOutcome::Fallback.to_string(), "fallback");
        assert_eq!(HandleOutcome::Ignored.as_str(), "ignored");
        assert!(HandleOutcome::Matched.is_matched());
        assert!(HandleOutcome::Fallback.is_fallback());
        assert!(HandleOutcome::Ignored.is_ignored());
        assert!(!HandleOutcome::Ignored.is_matched());
    }

    #[test]
    fn bot_ack_exposes_stable_label() {
        assert_eq!(BotAck::Ok.as_str(), "OK");
        assert_eq!(BotAck::Ok.to_string(), "OK");
        assert!(BotAck::Ok.is_ok());
    }

    #[test]
    fn callback_verifier_debug_redacts_app_secret() {
        let verifier = CallbackVerifier::new("callback-secret").expect("verifier");
        let debug = format!("{verifier:?}");

        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("callback-secret"));
    }

    #[test]
    fn bot_event_debug_redacts_temporary_credentials() {
        let event = BotEvent::from_value(serde_json::json!({
            "msgId": "message-secret",
            "conversationType": "2",
            "msgtype": "file",
            "openConversationId": "conversation-secret",
            "senderId": "sender-secret",
            "senderStaffId": "staff-secret",
            "senderNick": "Alice token=nick-secret",
            "conversationTitle": "Ops token=title-secret",
            "sessionWebhook": "https://oapi.dingtalk.com/robot/sendBySession?token=session-token",
            "atUsers": [
                {
                    "dingtalkId": "dingtalk-secret",
                    "staffId": "at-staff-secret"
                }
            ],
            "content": {
                "downloadCode": "download-code",
                "fileId": "file-secret"
            }
        }));
        let debug = format!("{event:?}");

        assert!(debug.contains("sessionWebhook"));
        assert!(debug.contains("<redacted>"));
        assert!(debug.contains("has_dingtalk_id"));
        assert!(debug.contains("has_staff_id"));
        assert!(debug.contains("message-secret"));
        assert!(!debug.contains("session-token"));
        assert!(!debug.contains("download-code"));
        assert!(!debug.contains("conversation-secret"));
        assert!(!debug.contains("sender-secret"));
        assert!(!debug.contains("staff-secret"));
        assert!(!debug.contains("nick-secret"));
        assert!(!debug.contains("title-secret"));
        assert!(!debug.contains("dingtalk-secret"));
        assert!(!debug.contains("at-staff-secret"));
        assert!(!debug.contains("file-secret"));
    }

    #[test]
    fn callback_request_debug_redacts_sign_and_body() {
        let request = CallbackRequest::new(
            CallbackHeaders::new(Some("1700000000000"), Some("callback-sign")),
            r#"{"sessionWebhook":"https://example.test/hook?token=body-token"}"#,
        );
        let debug = format!("{request:?}");

        assert!(debug.contains("1700000000000"));
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("callback-sign"));
        assert!(!debug.contains("body-token"));
        assert!(!debug.contains("sessionWebhook"));
    }

    #[tokio::test]
    async fn routes_group_text_command() {
        let client = DingTalk::builder().build().expect("client");
        let hit = Arc::new(AtomicBool::new(false));
        let seen = Arc::clone(&hit);
        let bot =
            Bot::new(client).on_text_command(ConversationScope::Group, "/ping", move |_ctx| {
                let seen = Arc::clone(&seen);
                async move {
                    seen.store(true, Ordering::SeqCst);
                }
            });

        let outcome = bot
            .handle_event(BotEvent::text(ConversationScope::Group, "/ping"))
            .await
            .expect("handled");

        assert_eq!(outcome, HandleOutcome::Matched);
        assert!(hit.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn routes_non_text_messages_with_builder_helper() {
        let client = DingTalk::builder().build().expect("client");
        let hit = Arc::new(AtomicBool::new(false));
        let seen = Arc::clone(&hit);
        let bot =
            Bot::new(client).on_message(ConversationScope::Any, MessageType::Picture, move |ctx| {
                let seen = Arc::clone(&seen);
                async move {
                    assert_eq!(ctx.message_type(), &MessageType::Picture);
                    seen.store(true, Ordering::SeqCst);
                }
            });
        let event = BotEvent::from_value(serde_json::json!({
            "conversationType": "2",
            "msgtype": "picture",
            "content": {
                "downloadCode": "download-code"
            }
        }));

        let outcome = bot.handle_event(event).await.expect("handled");

        assert_eq!(outcome, HandleOutcome::Matched);
        assert!(hit.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn text_scope_shortcuts_route_expected_conversations() {
        let client = DingTalk::builder().build().expect("client");
        let group_hit = Arc::new(AtomicBool::new(false));
        let private_hit = Arc::new(AtomicBool::new(false));
        let seen_group = Arc::clone(&group_hit);
        let seen_private = Arc::clone(&private_hit);
        let bot = Bot::new(client)
            .on_group_text_command("/group", move |ctx| {
                let seen_group = Arc::clone(&seen_group);
                async move {
                    assert!(ctx.is_group());
                    seen_group.store(true, Ordering::SeqCst);
                }
            })
            .on_private_text_command("/private", move |ctx| {
                let seen_private = Arc::clone(&seen_private);
                async move {
                    assert!(ctx.is_private());
                    seen_private.store(true, Ordering::SeqCst);
                }
            });

        let group_outcome = bot
            .handle_event(BotEvent::text(ConversationScope::Group, "/group"))
            .await
            .expect("handled");
        let private_outcome = bot
            .handle_event(BotEvent::text(ConversationScope::Private, "/private"))
            .await
            .expect("handled");

        assert_eq!(group_outcome, HandleOutcome::Matched);
        assert_eq!(private_outcome, HandleOutcome::Matched);
        assert!(group_hit.load(Ordering::SeqCst));
        assert!(private_hit.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn route_attaches_typed_scope_handlers() {
        let client = DingTalk::builder().build().expect("client");
        let group_hit = Arc::new(AtomicBool::new(false));
        let private_hit = Arc::new(AtomicBool::new(false));
        let seen_group = Arc::clone(&group_hit);
        let seen_private = Arc::clone(&private_hit);
        let bot = Bot::new(client)
            .route(
                Route::new(ConversationScope::Any)
                    .message_type(MessageType::Text)
                    .command("/typed-group")
                    .handle_group(move |ctx: GroupContext| {
                        let seen_group = Arc::clone(&seen_group);
                        async move {
                            assert!(ctx.is_group());
                            seen_group.store(true, Ordering::SeqCst);
                        }
                    }),
            )
            .route(
                Route::new(ConversationScope::Any)
                    .message_type(MessageType::Text)
                    .command("/typed-private")
                    .handle_private(move |ctx: PrivateContext| {
                        let seen_private = Arc::clone(&seen_private);
                        async move {
                            assert!(ctx.is_private());
                            seen_private.store(true, Ordering::SeqCst);
                        }
                    }),
            );

        let ignored = bot
            .handle_event(BotEvent::text(ConversationScope::Private, "/typed-group"))
            .await
            .expect("handled");
        let group_matched = bot
            .handle_event(BotEvent::text(ConversationScope::Group, "/typed-group"))
            .await
            .expect("handled");
        let private_matched = bot
            .handle_event(BotEvent::text(ConversationScope::Private, "/typed-private"))
            .await
            .expect("handled");

        assert_eq!(ignored, HandleOutcome::Ignored);
        assert_eq!(group_matched, HandleOutcome::Matched);
        assert_eq!(private_matched, HandleOutcome::Matched);
        assert!(group_hit.load(Ordering::SeqCst));
        assert!(private_hit.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn text_command_exposes_args_and_sender_helpers() {
        let client = DingTalk::builder().build().expect("client");
        let seen_args = Arc::new(Mutex::new(None::<String>));
        let seen_sender = Arc::new(Mutex::new(None::<String>));
        let args_slot = Arc::clone(&seen_args);
        let sender_slot = Arc::clone(&seen_sender);
        let bot = Bot::new(client).on_text_command(ConversationScope::Group, "/echo", move |ctx| {
            let args_slot = Arc::clone(&args_slot);
            let sender_slot = Arc::clone(&sender_slot);
            async move {
                *args_slot.lock().expect("args lock") = ctx.args().map(ToOwned::to_owned);
                *sender_slot.lock().expect("sender lock") =
                    ctx.sender_name().map(ToOwned::to_owned);
            }
        });
        let mut event = BotEvent::text(ConversationScope::Group, "  /echo hello world");
        event.sender_nick = Some("Alice".to_string());

        let outcome = bot.handle_event(event).await.expect("handled");

        assert_eq!(outcome, HandleOutcome::Matched);
        assert_eq!(
            seen_args.lock().expect("args lock").as_deref(),
            Some("hello world")
        );
        assert_eq!(
            seen_sender.lock().expect("sender lock").as_deref(),
            Some("Alice")
        );
    }

    #[tokio::test]
    async fn text_command_exposes_split_args() {
        let client = DingTalk::builder().build().expect("client");
        let seen = Arc::new(Mutex::new(Vec::<String>::new()));
        let seen_args = Arc::clone(&seen);
        let bot =
            Bot::new(client).on_text_command(ConversationScope::Group, "/deploy", move |ctx| {
                let seen_args = Arc::clone(&seen_args);
                async move {
                    assert!(ctx.has_args());
                    assert_eq!(ctx.arg(0), Some("api"));
                    assert_eq!(ctx.arg(1), Some("prod"));
                    assert_eq!(ctx.args_or_empty(), "api   prod");
                    assert_eq!(ctx.args_iter().count(), 2);
                    *seen_args.lock().expect("args lock") =
                        ctx.args_vec().into_iter().map(ToOwned::to_owned).collect();
                }
            });

        let outcome = bot
            .handle_event(BotEvent::text(
                ConversationScope::Group,
                "/deploy   api   prod",
            ))
            .await
            .expect("handled");

        assert_eq!(outcome, HandleOutcome::Matched);
        assert_eq!(
            seen.lock().expect("args lock").as_slice(),
            ["api".to_string(), "prod".to_string()]
        );
    }

    #[tokio::test]
    async fn text_command_matches_after_leading_mentions() {
        let client = DingTalk::builder().build().expect("client");
        let seen_args = Arc::new(Mutex::new(None::<String>));
        let args_slot = Arc::clone(&seen_args);
        let bot = Bot::new(client).on_text_command(ConversationScope::Group, "/ping", move |ctx| {
            let args_slot = Arc::clone(&args_slot);
            async move {
                *args_slot.lock().expect("args lock") = ctx.args().map(ToOwned::to_owned);
            }
        });

        let outcome = bot
            .handle_event(BotEvent::text(
                ConversationScope::Group,
                "@Alice @机器人 /ping hello",
            ))
            .await
            .expect("handled");

        assert_eq!(outcome, HandleOutcome::Matched);
        assert_eq!(
            seen_args.lock().expect("args lock").as_deref(),
            Some("hello")
        );
    }

    #[tokio::test]
    async fn text_command_aliases_expose_actual_matched_command() {
        let client = DingTalk::builder().build().expect("client");
        let seen = Arc::new(Mutex::new(Vec::<(String, String)>::new()));
        let seen_aliases = Arc::clone(&seen);
        let bot = Bot::new(client).on_group_text_commands(["/ping", "ping"], move |ctx| {
            let seen_aliases = Arc::clone(&seen_aliases);
            async move {
                seen_aliases.lock().expect("alias lock").push((
                    ctx.command().unwrap_or_default().to_string(),
                    ctx.args_or_empty().to_string(),
                ));
            }
        });

        let slash_outcome = bot
            .handle_event(BotEvent::text(ConversationScope::Group, "/ping api"))
            .await
            .expect("handled");
        let bare_outcome = bot
            .handle_event(BotEvent::text(ConversationScope::Group, "@机器人 ping ops"))
            .await
            .expect("handled");

        assert_eq!(slash_outcome, HandleOutcome::Matched);
        assert_eq!(bare_outcome, HandleOutcome::Matched);
        assert_eq!(
            seen.lock().expect("alias lock").as_slice(),
            [
                ("/ping".to_string(), "api".to_string()),
                ("ping".to_string(), "ops".to_string())
            ]
        );
    }

    #[tokio::test]
    async fn empty_text_commands_are_invalid() {
        let client = DingTalk::builder().build().expect("client");
        let hit = Arc::new(AtomicBool::new(false));
        let seen = Arc::clone(&hit);
        let bot =
            Bot::new(client).on_text_commands(ConversationScope::Group, [" ", ""], move |_ctx| {
                let seen = Arc::clone(&seen);
                async move {
                    seen.store(true, Ordering::SeqCst);
                }
            });

        let error = bot
            .handle_event(BotEvent::text(ConversationScope::Group, "/anything"))
            .await
            .expect_err("empty command route should fail");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
        assert!(!hit.load(Ordering::SeqCst));
    }

    #[test]
    fn command_routes_reject_ambiguous_configuration() {
        let untrimmed = Route::new(ConversationScope::Any)
            .message_type(MessageType::Text)
            .command(" /ping ")
            .handle(|_ctx| async {})
            .validate()
            .expect_err("commands should not be rewritten");
        let duplicate = Route::new(ConversationScope::Any)
            .message_type(MessageType::Text)
            .commands(["/ping", "/ping"])
            .handle(|_ctx| async {})
            .validate()
            .expect_err("duplicate aliases should fail");
        let non_text = Route::new(ConversationScope::Any)
            .message_type(MessageType::Picture)
            .command("/ping")
            .handle(|_ctx| async {})
            .validate()
            .expect_err("command routes should not be attached to non-text messages");

        assert_eq!(untrimmed.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(duplicate.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(non_text.kind(), crate::ErrorKind::InvalidConfig);
    }

    #[tokio::test]
    async fn route_without_handler_is_invalid() {
        let client = DingTalk::builder().build().expect("client");
        let bot = Bot::new(client).route(Route::new(ConversationScope::Any));

        let error = bot
            .handle_event(BotEvent::text(ConversationScope::Group, "/anything"))
            .await
            .expect_err("route without handler should fail");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidConfig);
    }

    #[tokio::test]
    async fn text_command_requires_boundary() {
        let client = DingTalk::builder().build().expect("client");
        let hit = Arc::new(AtomicBool::new(false));
        let seen = Arc::clone(&hit);
        let bot =
            Bot::new(client).on_text_command(ConversationScope::Group, "/ping", move |_ctx| {
                let seen = Arc::clone(&seen);
                async move {
                    seen.store(true, Ordering::SeqCst);
                }
            });

        let outcome = bot
            .handle_event(BotEvent::text(ConversationScope::Group, "/pinged"))
            .await
            .expect("handled");

        assert_eq!(outcome, HandleOutcome::Ignored);
        assert!(!hit.load(Ordering::SeqCst));

        let outcome = bot
            .handle_event(BotEvent::text(ConversationScope::Group, "@机器人 /pinged"))
            .await
            .expect("handled");

        assert_eq!(outcome, HandleOutcome::Ignored);
        assert!(!hit.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn fallback_handles_unmatched_text() {
        let client = DingTalk::builder().build().expect("client");
        let hit = Arc::new(AtomicBool::new(false));
        let seen = Arc::clone(&hit);
        let bot = Bot::new(client)
            .on_text_command(ConversationScope::Group, "/ping", |_ctx| async {})
            .on_unmatched_text(ConversationScope::Any, move |_ctx| {
                let seen = Arc::clone(&seen);
                async move {
                    seen.store(true, Ordering::SeqCst);
                }
            });

        let outcome = bot
            .handle_event(BotEvent::text(ConversationScope::Private, "/missing"))
            .await
            .expect("handled");

        assert_eq!(outcome, HandleOutcome::Fallback);
        assert!(hit.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn appending_fallback_does_not_hide_an_invalid_registration() {
        let client = DingTalk::builder().build().expect("client");
        let bot = Bot::new(client)
            .fallback_route(Route::new(ConversationScope::Any))
            .fallback(|_| async {});

        assert!(bot.validate().is_err());
        let error = bot
            .handle_event(BotEvent::text(ConversationScope::Private, "hello"))
            .await
            .expect_err("invalid fallback prevents dispatch");
        assert_eq!(error.kind(), crate::ErrorKind::InvalidConfig);
    }

    #[test]
    fn fallback_registration_preserves_current_route_errors() {
        let client = DingTalk::builder().build().expect("client");
        let bot = Bot::new(client.clone())
            .fallback(|_| async {})
            .fallback_route(Route::new(ConversationScope::Any));
        assert!(bot.validate().is_err());

        let bot = Bot::new(client)
            .fallback_route(Route::new(ConversationScope::Any))
            .route(Route::new(ConversationScope::Group))
            .fallback(|_| async {});
        assert!(bot.validate().is_err());
    }

    #[derive(Debug)]
    struct AppState {
        name: &'static str,
    }

    #[tokio::test]
    async fn context_exposes_shared_state() {
        let client = DingTalk::builder().build().expect("client");
        let seen = Arc::new(Mutex::new(None::<String>));
        let seen_state = Arc::clone(&seen);
        let bot = Bot::new(client)
            .state(AppState { name: "ops" })
            .on_text_command(ConversationScope::Group, "/state", move |ctx| {
                let seen_state = Arc::clone(&seen_state);
                async move {
                    *seen_state.lock().expect("state lock") =
                        Some(ctx.state_required::<AppState>()?.name.to_string());
                    Ok::<_, dingding::Error>(())
                }
            });

        let outcome = bot
            .handle_event(BotEvent::text(ConversationScope::Group, "/state"))
            .await
            .expect("handled");

        assert_eq!(outcome, HandleOutcome::Matched);
        assert_eq!(seen.lock().expect("state lock").as_deref(), Some("ops"));
    }

    #[tokio::test]
    async fn context_exposes_callback_metadata() {
        let client = DingTalk::builder().build().expect("client");
        let seen = Arc::new(Mutex::new(None::<(usize, Option<String>, Option<u64>)>));
        let seen_metadata = Arc::clone(&seen);
        let bot = Bot::new(client).on_text_command(ConversationScope::Group, "/meta", move |ctx| {
            let seen_metadata = Arc::clone(&seen_metadata);
            async move {
                *seen_metadata.lock().expect("metadata lock") = Some((
                    ctx.at_users().len(),
                    ctx.session_webhook().map(ToOwned::to_owned),
                    ctx.session_webhook_expires_at_millis(),
                ));
                assert_eq!(
                    ctx.raw().get("messageId").and_then(Value::as_str),
                    Some("m1")
                );
            }
        });
        let event = BotEvent::from_value(serde_json::json!({
            "conversationType": 2,
            "msgType": "text",
            "messageId": "m1",
            "content": "/meta",
            "session_webhook": "https://oapi.dingtalk.com/robot/sendBySession?token=redacted",
            "session_webhook_expires_at_millis": "1700000000000",
            "at_users": [
                {
                    "userId": "staff-1"
                }
            ]
        }));

        let outcome = bot.handle_event(event).await.expect("handled");

        assert_eq!(outcome, HandleOutcome::Matched);
        assert_eq!(
            seen.lock().expect("metadata lock").as_ref(),
            Some(&(
                1,
                Some("https://oapi.dingtalk.com/robot/sendBySession?token=redacted".to_string()),
                Some(1700000000000)
            ))
        );
    }

    #[tokio::test]
    async fn context_builds_webhook_mentions() {
        let client = DingTalk::builder().build().expect("client");
        let seen = Arc::new(Mutex::new(None::<(Vec<String>, Vec<String>)>));
        let seen_mentions = Arc::clone(&seen);
        let bot =
            Bot::new(client).on_text_command(ConversationScope::Group, "/mention", move |ctx| {
                let seen_mentions = Arc::clone(&seen_mentions);
                async move {
                    *seen_mentions.lock().expect("mention lock") = Some((
                        ctx.sender_mention().user_ids,
                        ctx.mentioned_users_at().user_ids,
                    ));
                }
            });
        let event = BotEvent::from_value(serde_json::json!({
            "conversationType": 2,
            "msgType": "text",
            "content": "/mention",
            "senderStaffId": " sender-1 ",
            "atUsers": [
                {
                    "staffId": " staff-1 "
                },
                {
                    "staffId": "staff-1"
                },
                {
                    "dingtalkId": "$:LWCP_v1:$open-id-only"
                }
            ]
        }));

        let outcome = bot.handle_event(event).await.expect("handled");

        assert_eq!(outcome, HandleOutcome::Matched);
        assert_eq!(
            seen.lock().expect("mention lock").as_ref(),
            Some(&(vec!["sender-1".to_string()], vec!["staff-1".to_string()]))
        );
    }

    #[test]
    fn sender_mention_skips_invalid_staff_id() {
        let client = DingTalk::builder().build().expect("client");
        let event = BotEvent::from_value(serde_json::json!({
            "senderStaffId": "sender 1"
        }));
        let ctx: BotContext = BotContext::new(client, event, None, None);

        assert!(ctx.sender_mention().is_empty());
    }

    #[tokio::test]
    async fn context_exposes_typed_message_shortcuts() {
        let client = DingTalk::builder().build().expect("client");
        let seen = Arc::new(Mutex::new(None::<String>));
        let seen_picture = Arc::clone(&seen);
        let bot = Bot::new(client).on_message(
            ConversationScope::Private,
            MessageType::Picture,
            move |ctx| {
                let seen_picture = Arc::clone(&seen_picture);
                async move {
                    assert!(ctx.text_message().is_none());
                    assert!(ctx.file_message().is_none());
                    *seen_picture.lock().expect("picture lock") = ctx
                        .picture_message()
                        .and_then(|message| message.download_code.clone());
                }
            },
        );
        let event = BotEvent::from_value(serde_json::json!({
            "conversationType": "1",
            "msgtype": "picture",
            "content": {
                "downloadCode": "download-code"
            }
        }));

        let outcome = bot.handle_event(event).await.expect("handled");

        assert_eq!(outcome, HandleOutcome::Matched);
        assert_eq!(
            seen.lock().expect("picture lock").as_deref(),
            Some("download-code")
        );
    }

    #[tokio::test]
    async fn context_exposes_session_webhook_expiration_helpers() {
        let client = DingTalk::builder().build().expect("client");
        let seen = Arc::new(Mutex::new(None::<(Option<SystemTime>, bool)>));
        let seen_expiration = Arc::clone(&seen);
        let bot =
            Bot::new(client).on_text_command(ConversationScope::Group, "/expiry", move |ctx| {
                let seen_expiration = Arc::clone(&seen_expiration);
                async move {
                    *seen_expiration.lock().expect("expiration lock") = Some((
                        ctx.session_webhook_expires_at(),
                        ctx.is_session_webhook_expired(),
                    ));
                }
            });
        let event = BotEvent::from_value(serde_json::json!({
            "conversationType": 2,
            "msgType": "text",
            "content": "/expiry",
            "sessionWebhookExpiredTime": 1_u64
        }));

        let outcome = bot.handle_event(event).await.expect("handled");

        assert_eq!(outcome, HandleOutcome::Matched);
        assert_eq!(
            seen.lock().expect("expiration lock").as_ref(),
            Some(&(Some(UNIX_EPOCH + Duration::from_millis(1)), true))
        );
    }

    #[tokio::test]
    async fn context_reports_missing_required_state() {
        let client = DingTalk::builder().build().expect("client");
        let bot = Bot::new(client).on_text_command(
            ConversationScope::Group,
            "/state",
            |ctx| async move {
                let _state = ctx.state_required::<AppState>()?;
                Ok::<_, dingding::Error>(())
            },
        );

        let error = bot
            .handle_event(BotEvent::text(ConversationScope::Group, "/state"))
            .await
            .expect_err("missing state should fail");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidConfig);
    }

    #[tokio::test]
    async fn reply_helpers_reject_expired_session_webhook() {
        let client = DingTalk::builder().build().expect("client");
        let bot = Bot::new(client).on_text_command(
            ConversationScope::Group,
            "/reply",
            |ctx| async move { ctx.reply_text("too late").await },
        );
        let event = BotEvent::from_value(serde_json::json!({
            "conversationType": 2,
            "msgType": "text",
            "content": "/reply",
            "sessionWebhook": "https://oapi.dingtalk.com/robot/sendBySession?token=redacted",
            "sessionWebhookExpiredTime": 1_u64
        }));

        let error = bot
            .handle_event(event)
            .await
            .expect_err("expired session webhook should fail before sending");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidConfig);
        assert!(error.to_string().contains("sessionWebhook has expired"));
    }

    #[tokio::test]
    async fn reply_response_helpers_require_session_webhook() {
        let client = DingTalk::builder().build().expect("client");
        let bot = Bot::new(client).on_text_command(
            ConversationScope::Group,
            "/reply",
            |ctx| async move {
                ctx.reply_text_response("hello").await?;
                Ok::<_, dingding::Error>(())
            },
        );

        let error = bot
            .handle_event(BotEvent::text(ConversationScope::Group, "/reply"))
            .await
            .expect_err("missing session webhook should fail");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidConfig);
    }

    #[tokio::test]
    async fn reply_at_helpers_require_session_webhook() {
        let client = DingTalk::builder().build().expect("client");
        let bot = Bot::new(client).on_text_command(
            ConversationScope::Group,
            "/reply",
            |ctx| async move {
                ctx.reply_text_with_at("hello", crate::webhook::At::new().all_users())
                    .await
            },
        );

        let error = bot
            .handle_event(BotEvent::text(ConversationScope::Group, "/reply"))
            .await
            .expect_err("missing session webhook should fail");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidConfig);
    }

    #[test]
    fn parses_dingtalk_callback_event() {
        let event = BotEvent::from_value(serde_json::json!({
            "conversationId": "cid-example",
            "conversationType": "2",
            "conversationTitle": "ops",
            "msgId": "msg-1",
            "msgtype": "text",
            "senderId": "sender-open-id",
            "senderStaffId": "staff-1",
            "senderNick": "Alice",
            "isAdmin": true,
            "isInAtList": true,
            "sessionWebhook": "https://oapi.dingtalk.com/robot/sendBySession?token=redacted",
            "sessionWebhookExpiredTime": 1700000000000_u64,
            "text": {
                "content": "/ping"
            },
            "atUsers": [
                {
                    "dingtalkId": "$:LWCP_v1:$example",
                    "staffId": "staff-2"
                }
            ]
        }));

        assert_eq!(event.conversation_scope, ConversationScope::Group);
        assert_eq!(event.message_type, MessageType::Text);
        assert_eq!(
            event.text.as_ref().map(|text| text.content.as_str()),
            Some("/ping")
        );
        assert_eq!(event.open_conversation_id.as_deref(), Some("cid-example"));
        assert_eq!(event.sender_id.as_deref(), Some("sender-open-id"));
        assert_eq!(event.sender_staff_id.as_deref(), Some("staff-1"));
        assert_eq!(event.sender_nick.as_deref(), Some("Alice"));
        assert_eq!(event.is_admin, Some(true));
        assert_eq!(event.is_in_at_list, Some(true));
        assert_eq!(event.session_webhook_expires_at_millis, Some(1700000000000));
        assert_eq!(event.at_users.len(), 1);
    }

    #[test]
    fn parses_string_encoded_callback_scalars() {
        let event = BotEvent::from_value(serde_json::json!({
            "conversationType": "2",
            "isAdmin": "true",
            "isInAtList": "0",
            "sessionWebhookExpiredTime": "1700000000000"
        }));

        assert_eq!(event.is_admin, Some(true));
        assert_eq!(event.is_in_at_list, Some(false));
        assert_eq!(event.session_webhook_expires_at_millis, Some(1700000000000));
    }

    #[test]
    fn parses_callback_aliases_and_numeric_scope() {
        let event = BotEvent::from_value(serde_json::json!({
            "conversationType": 2,
            "conversation_title": "ops",
            "messageId": "message-1",
            "msg_type": " TEXT ",
            "sender_id": "sender-open-id",
            "sender-staff-id": "staff-1",
            "sender_nick": "Alice",
            "is_admin": 1,
            "is-in-at-list": "true",
            "open-conversation-id": "cid-example",
            "content": "/ping",
            "at-users": [
                {
                    "dingtalk_id": "$:LWCP_v1:$example",
                    "user-id": "staff-2"
                }
            ]
        }));

        assert_eq!(event.conversation_scope, ConversationScope::Group);
        assert_eq!(event.message_type, MessageType::Text);
        assert_eq!(
            event.text.as_ref().map(|text| text.content.as_str()),
            Some("/ping")
        );
        assert_eq!(event.message_id.as_deref(), Some("message-1"));
        assert_eq!(event.open_conversation_id.as_deref(), Some("cid-example"));
        assert_eq!(event.sender_id.as_deref(), Some("sender-open-id"));
        assert_eq!(event.sender_staff_id.as_deref(), Some("staff-1"));
        assert_eq!(event.sender_nick.as_deref(), Some("Alice"));
        assert_eq!(event.conversation_title.as_deref(), Some("ops"));
        assert_eq!(event.is_admin, Some(true));
        assert_eq!(event.is_in_at_list, Some(true));
        assert_eq!(event.at_users.len(), 1);
        assert_eq!(event.at_users[0].staff_id.as_deref(), Some("staff-2"));
    }

    #[test]
    fn normalizes_machine_string_fields_without_trimming_message_text() {
        let event = BotEvent::from_value(serde_json::json!({
            "conversationType": "2",
            "conversationTitle": " ops ",
            "messageId": " message-1 ",
            "msgtype": "text",
            "senderId": " sender-open-id ",
            "senderStaffId": " staff-1 ",
            "senderNick": " Alice ",
            "openConversationId": " cid-example ",
            "sessionWebhook": " https://oapi.dingtalk.com/robot/sendBySession?token=redacted ",
            "text": {
                "content": "  /ping keep text spaces  "
            },
            "atUsers": [
                {
                    "dingtalkId": " $:LWCP_v1:$example ",
                    "staffId": " staff-2 "
                }
            ],
            "content": {
                "downloadCode": " download-code ",
                "recognition": " hello ",
                "duration": "1200",
                "richText": [
                    {
                        "type": " picture ",
                        "downloadCode": " picture-download ",
                        "pictureDownloadCode": " picture-code "
                    }
                ]
            }
        }));

        assert_eq!(
            event.text.as_ref().map(|text| text.content.as_str()),
            Some("  /ping keep text spaces  ")
        );
        assert_eq!(event.message_id.as_deref(), Some("message-1"));
        assert_eq!(event.open_conversation_id.as_deref(), Some("cid-example"));
        assert_eq!(event.sender_id.as_deref(), Some("sender-open-id"));
        assert_eq!(event.sender_staff_id.as_deref(), Some("staff-1"));
        assert_eq!(event.sender_nick.as_deref(), Some("Alice"));
        assert_eq!(event.conversation_title.as_deref(), Some("ops"));
        assert_eq!(
            event.session_webhook.as_deref(),
            Some("https://oapi.dingtalk.com/robot/sendBySession?token=redacted")
        );
        assert_eq!(
            event
                .at_users
                .first()
                .and_then(|user| user.staff_id.as_deref()),
            Some("staff-2")
        );

        let audio = AudioMessage::from_content(event.content.as_ref());
        assert_eq!(audio.download_code.as_deref(), Some("download-code"));
        assert_eq!(audio.recognition.as_deref(), Some("hello"));

        let rich_text = RichTextMessage::from_content(event.content.as_ref());
        assert!(matches!(
            rich_text.items.first(),
            Some(RichTextItem::Picture {
                download_code: Some(download_code),
                picture_download_code: Some(picture_download_code),
            }) if download_code == "picture-download" && picture_download_code == "picture-code"
        ));
    }

    #[test]
    fn missing_msgtype_is_unknown_event_type() {
        let event = BotEvent::from_value(serde_json::json!({
            "conversationType": "2",
            "text": {
                "content": "/ping"
            }
        }));

        assert_eq!(
            event.message_type,
            MessageType::Unknown("missing".to_string())
        );
        assert!(matches!(
            event.message,
            IncomingMessage::Unknown {
                ref message_type,
                ..
            } if message_type == "missing"
        ));
    }

    #[test]
    fn parses_non_text_callback_content() {
        let event = BotEvent::from_value(serde_json::json!({
            "conversationType": "1",
            "msgtype": "picture",
            "content": {
                "downloadCode": "download-code"
            }
        }));

        assert_eq!(event.message_type, MessageType::Picture);
        assert!(matches!(
            &event.message,
            IncomingMessage::Picture(PictureMessage {
                download_code: Some(value),
                ..
            }) if value == "download-code"
        ));
        assert_eq!(
            event
                .content
                .as_ref()
                .and_then(|content| content.get("downloadCode"))
                .and_then(Value::as_str),
            Some("download-code")
        );
    }

    #[test]
    fn parses_string_encoded_callback_content() {
        let event = BotEvent::from_value(serde_json::json!({
            "conversationType": "1",
            "msgtype": "picture",
            "content": r#"{"downloadCode":"download-code","pictureDownloadCode":"picture-code"}"#
        }));

        assert_eq!(event.message_type, MessageType::Picture);
        assert_eq!(
            event
                .content
                .as_ref()
                .and_then(|content| content.get("downloadCode"))
                .and_then(Value::as_str),
            Some("download-code")
        );
        assert!(matches!(
            &event.message,
            IncomingMessage::Picture(PictureMessage {
                download_code: Some(download_code),
                picture_download_code: Some(picture_download_code),
            }) if download_code == "download-code" && picture_download_code == "picture-code"
        ));
    }

    #[test]
    fn parses_text_from_content_object_and_string_encoded_object() {
        let object_content = BotEvent::from_value(serde_json::json!({
            "conversationType": "2",
            "msgtype": "text",
            "content": {
                "content": "/ping object"
            }
        }));
        let string_content = BotEvent::from_value(serde_json::json!({
            "conversationType": "2",
            "msgtype": "text",
            "content": r#"{"content":"/ping string"}"#
        }));

        assert_eq!(
            object_content
                .text
                .as_ref()
                .map(|text| text.content.as_str()),
            Some("/ping object")
        );
        assert_eq!(
            string_content
                .text
                .as_ref()
                .map(|text| text.content.as_str()),
            Some("/ping string")
        );
    }

    #[test]
    fn parses_file_and_rich_text_payloads() {
        let file = BotEvent::from_value(serde_json::json!({
            "conversationType": "1",
            "msgtype": "file",
            "content": {
                "downloadCode": 3003,
                "fileName": "report.pdf",
                "fileId": 1001,
                "spaceId": 2002
            }
        }));
        assert!(matches!(
            &file.message,
            IncomingMessage::File(FileMessage {
                download_code: Some(download_code),
                file_name: Some(value),
                file_id: Some(file_id),
                space_id: Some(space_id),
            }) if download_code == "3003"
                && value == "report.pdf"
                && file_id == "1001"
                && space_id == "2002"
        ));
        assert_eq!(
            file.message
                .file()
                .and_then(|message| message.download_code.as_deref()),
            Some("3003")
        );
        assert_eq!(
            file.message
                .file()
                .and_then(|message| message.file_name.as_deref()),
            Some("report.pdf")
        );
        assert!(file.message.picture().is_none());

        let rich_text = BotEvent::from_value(serde_json::json!({
            "conversationType": "1",
            "msgtype": "richText",
            "content": {
                "richText": [
                    { "text": "hello" },
                    {
                        "type": "picture",
                        "downloadCode": 4004,
                        "pictureDownloadCode": 5005
                    }
                ]
            }
        }));
        let IncomingMessage::RichText(message) = &rich_text.message else {
            panic!("expected rich text");
        };

        assert_eq!(message.items.len(), 2);
        assert!(matches!(
            &message.items[0],
            RichTextItem::Text { text } if text == "hello"
        ));
        assert!(matches!(
            &message.items[1],
            RichTextItem::Picture {
                download_code: Some(download_code),
                picture_download_code: Some(picture_download_code),
            } if download_code == "4004" && picture_download_code == "5005"
        ));
        assert_eq!(
            rich_text
                .message
                .rich_text()
                .map(|message| message.items.len()),
            Some(2)
        );
    }

    #[test]
    fn parses_content_field_name_variants() {
        let file = BotEvent::from_value(serde_json::json!({
            "conversationType": "1",
            "msgtype": "file",
            "content": {
                "download-code": 3003,
                "file_name": "report.pdf",
                "file-id": 1001,
                "space_id": 2002
            }
        }));
        assert!(matches!(
            &file.message,
            IncomingMessage::File(FileMessage {
                download_code: Some(download_code),
                file_name: Some(file_name),
                file_id: Some(file_id),
                space_id: Some(space_id),
            }) if download_code == "3003"
                && file_name == "report.pdf"
                && file_id == "1001"
                && space_id == "2002"
        ));

        let rich_text = BotEvent::from_value(serde_json::json!({
            "conversation-type": "1",
            "msg-type": "richText",
            "content": {
                "rich-text": [
                    { "Text": "hello" },
                    {
                        "Type": "picture",
                        "download-code": 4004,
                        "picture_download_code": 5005
                    }
                ]
            }
        }));
        let IncomingMessage::RichText(message) = &rich_text.message else {
            panic!("expected rich text");
        };

        assert_eq!(message.items.len(), 2);
        assert!(matches!(
            &message.items[0],
            RichTextItem::Text { text } if text == "hello"
        ));
        assert!(matches!(
            &message.items[1],
            RichTextItem::Picture {
                download_code: Some(download_code),
                picture_download_code: Some(picture_download_code),
            } if download_code == "4004" && picture_download_code == "5005"
        ));
    }

    #[test]
    fn incoming_message_accessors_return_typed_payloads() {
        let text = BotEvent::text(ConversationScope::Private, "/ping");
        let markdown = BotEvent::from_value(serde_json::json!({
            "conversationType": "1",
            "msgtype": "markdown",
            "content": {
                "title": "deploy",
                "text": "**done**"
            }
        }));
        let audio = BotEvent::from_value(serde_json::json!({
            "conversationType": "1",
            "msgtype": "audio",
            "content": {
                "downloadCode": "audio-code",
                "recognition": "hello",
                "duration": "1200"
            }
        }));
        let video = BotEvent::from_value(serde_json::json!({
            "conversationType": "1",
            "msgtype": "video",
            "content": {
                "downloadCode": "video-code",
                "videoType": "mp4",
                "duration": 3000
            }
        }));
        let unknown = BotEvent::from_value(serde_json::json!({
            "conversationType": "1",
            "msgtype": "sticker",
            "content": {
                "id": "sticker-1"
            }
        }));

        assert_eq!(text.message.text(), Some("/ping"));
        assert_eq!(
            text.message
                .text_message()
                .map(|message| message.content.as_str()),
            Some("/ping")
        );
        assert_eq!(
            markdown
                .message
                .markdown()
                .and_then(|content| content.get("title"))
                .and_then(Value::as_str),
            Some("deploy")
        );
        assert_eq!(
            audio
                .message
                .audio()
                .and_then(|message| message.recognition.as_deref()),
            Some("hello")
        );
        assert_eq!(
            video
                .message
                .video()
                .and_then(|message| message.video_type.as_deref()),
            Some("mp4")
        );
        let (message_type, content) = unknown.message.unknown().expect("unknown message");
        assert_eq!(message_type, "sticker");
        assert_eq!(
            content
                .and_then(|content| content.get("id"))
                .and_then(Value::as_str),
            Some("sticker-1")
        );
    }

    #[test]
    fn verifies_callback_signature() {
        let timestamp = "1700000000000";
        let app_secret = "callback-secret";
        let now = UNIX_EPOCH + Duration::from_millis(1_700_000_000_000);
        let signature = test_callback_signature(timestamp, app_secret);
        let encoded_signature = urlencoding::encode(&signature);
        let verifier = CallbackVerifier::new(app_secret).expect("verifier");

        verifier
            .verify_at(timestamp, &encoded_signature, now)
            .expect("signature should verify");
    }

    #[test]
    fn rejects_untrimmed_callback_secret() {
        let error = CallbackVerifier::new(" callback-secret ")
            .expect_err("callback secret should not be rewritten");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn verifies_callback_signature_with_trimmed_header_values() {
        let timestamp = "1700000000000";
        let app_secret = "callback-secret";
        let now = UNIX_EPOCH + Duration::from_millis(1_700_000_000_000);
        let signature = test_callback_signature(timestamp, app_secret);
        let verifier = CallbackVerifier::new(app_secret).expect("verifier");

        verifier
            .verify_at(&format!(" {timestamp} "), &format!(" {signature} "), now)
            .expect("header whitespace should be ignored");
    }

    #[test]
    fn rejects_stale_callback_signature() {
        let timestamp = "1700000000000";
        let app_secret = "callback-secret";
        let now = UNIX_EPOCH + Duration::from_millis(1_700_001_000_000);
        let signature = test_callback_signature(timestamp, app_secret);
        let verifier = CallbackVerifier::new(app_secret).expect("verifier");
        let error = verifier
            .verify_at(timestamp, &signature, now)
            .expect_err("stale timestamp should fail");

        assert_eq!(error.kind(), crate::ErrorKind::Signature);
    }

    #[tokio::test]
    async fn handles_signed_callback_parts() {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_millis()
            .to_string();
        let app_secret = "callback-secret";
        let signature = test_callback_signature(&timestamp, app_secret);
        let client = DingTalk::builder().build().expect("client");
        let hit = Arc::new(AtomicBool::new(false));
        let seen = Arc::clone(&hit);
        let bot =
            Bot::new(client).on_text_command(ConversationScope::Group, "/ping", move |_ctx| {
                let seen = Arc::clone(&seen);
                async move {
                    seen.store(true, Ordering::SeqCst);
                }
            });
        let verifier = CallbackVerifier::new(app_secret).expect("verifier");
        let body = serde_json::json!({
            "conversationType": "2",
            "msgtype": "text",
            "text": {
                "content": "/ping"
            }
        })
        .to_string();

        let ack = bot
            .handle_callback_parts(&verifier, Some(&timestamp), Some(&signature), body)
            .await
            .expect("callback");

        assert_eq!(ack.as_str(), "OK");
        assert!(hit.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn rejects_callback_without_signature_headers() {
        let client = DingTalk::builder().build().expect("client");
        let bot = Bot::new(client);
        let verifier = CallbackVerifier::new("callback-secret").expect("verifier");
        let body = br#"{"conversationType":"2","msgtype":"text","text":{"content":"/ping"}}"#;
        let error = bot
            .handle_callback_parts(&verifier, None, None, body)
            .await
            .expect_err("missing headers should fail");

        assert_eq!(error.kind(), crate::ErrorKind::Signature);
    }

    #[tokio::test]
    async fn handles_callback_request_parts() {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_millis()
            .to_string();
        let app_secret = "callback-secret";
        let signature = test_callback_signature(&timestamp, app_secret);
        let client = DingTalk::builder().build().expect("client");
        let bot = Bot::new(client);
        let verifier = CallbackVerifier::new(app_secret).expect("verifier");
        let request = CallbackRequest::new(
            CallbackHeaders::new(Some(timestamp), Some(signature)),
            br#"{"conversationType":"2","msgtype":"text","text":{"content":"/ping"}}"#.to_vec(),
        );

        let ack = bot
            .handle_callback_request(&verifier, request)
            .await
            .expect("callback");

        assert_eq!(ack, BotAck::Ok);
    }

    fn test_callback_signature(timestamp_millis: &str, app_secret: &str) -> String {
        let string_to_sign = format!("{timestamp_millis}\n{app_secret}");
        let mut mac = Hmac::<Sha256>::new_from_slice(app_secret.as_bytes()).expect("hmac");
        mac.update(string_to_sign.as_bytes());
        STANDARD.encode(mac.finalize().into_bytes())
    }
}
