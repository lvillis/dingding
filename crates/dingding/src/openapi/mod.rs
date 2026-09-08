use std::{fmt, future::Future};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::{
    DingTalk, Error, Result,
    auth::AppCredentials,
    transport::{
        BodySnippetConfig, api_error_from_body, decode_json_response, parse_binary_response,
        parse_dingtalk_result, parse_standard_text_response, response_envelope_error,
    },
    util::{non_empty_trimmed, redact::redact_text},
};

mod lifecycle;
pub use lifecycle::{
    GroupMessageQuery, GroupMessageReader, GroupMessageStatus, MessageReadInfo,
    MessageRecallResponse, PrivateMessageStatus,
};

/// Minimal OpenAPI service.
#[derive(Clone)]
pub struct OpenApi {
    client: DingTalk,
    credentials: Option<AppCredentials>,
}

impl fmt::Debug for OpenApi {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenApi")
            .field("has_credentials", &self.credentials.is_some())
            .finish_non_exhaustive()
    }
}

impl OpenApi {
    pub(crate) fn new(client: DingTalk, credentials: Option<AppCredentials>) -> Self {
        Self {
            client,
            credentials,
        }
    }

    /// Returns app credentials configured for this OpenAPI service.
    #[must_use]
    pub fn credentials(&self) -> Option<&AppCredentials> {
        self.credentials.as_ref()
    }

    /// Returns a copy of this OpenAPI service with explicit app credentials.
    pub fn with_credentials(mut self, credentials: AppCredentials) -> Result<Self> {
        credentials.validate()?;
        self.credentials = Some(credentials);
        Ok(self)
    }

    /// Returns an access token, using the in-memory cache when possible.
    pub async fn access_token(&self) -> Result<String> {
        let credentials = self.credentials.as_ref().ok_or(Error::MissingCredentials)?;
        credentials.validate()?;

        if let Some(token) = self.client.cached_access_token(credentials) {
            return Ok(token);
        }

        let _refresh_guard = self.client.access_token_refresh_guard(credentials).await;
        if let Some(token) = self.client.cached_access_token(credentials) {
            return Ok(token);
        }

        let mut url = self.client.webhook_endpoint(&["gettoken"])?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("appkey", credentials.app_key());
            query.append_pair("appsecret", credentials.app_secret());
        }

        let (response, body) = decode_json_response::<AccessTokenResponse>(
            self.client.transport().get_webhook(&url).await?,
            self.client.transport().error_body_snippet(),
        )?;

        if let Some(error) = response_envelope_error(
            response.errcode,
            response.api_code.as_deref(),
            response.errmsg.as_deref(),
            response.success,
            response.request_id.as_deref(),
            &body,
            self.client.transport().error_body_snippet(),
        ) {
            return Err(error);
        }

        let token = response
            .access_token
            .as_deref()
            .and_then(|value| non_empty_trimmed(value, "access_token").ok())
            .ok_or_else(|| {
                api_error_from_body(
                    -1,
                    "missing access_token in DingTalk response",
                    response.request_id.clone(),
                    &body,
                    self.client.transport().error_body_snippet(),
                )
            })?;

        self.client
            .store_access_token(credentials.clone(), token.clone(), response.expires_in);

        Ok(token)
    }

    /// Posts a JSON body to a modern OpenAPI endpoint and extracts its `result` field.
    pub async fn post_json_result<T, B>(&self, segments: &[&str], body: &B) -> Result<T>
    where
        T: DeserializeOwned,
        B: Serialize + ?Sized,
    {
        let url = self.client.openapi_endpoint(segments)?;
        let url = &url;
        self.with_access_token(|access_token| async move {
            parse_dingtalk_result(
                self.client
                    .transport()
                    .post_openapi_json(url, Some(&access_token), body)
                    .await?,
                self.client.transport().error_body_snippet(),
            )
        })
        .await
    }

    /// Uploads a DingTalk media resource for robot image, voice, video, or file messages.
    pub async fn upload_media(&self, upload: MediaUpload) -> Result<UploadedMedia> {
        upload.validate()?;
        let upload = &upload;
        let media_type = upload.media_type().as_str();

        self.with_access_token(|access_token| async move {
            let mut url = self.client.webhook_endpoint(&["media", "upload"])?;
            url.query_pairs_mut()
                .append_pair("access_token", &access_token)
                .append_pair("type", media_type);
            let (content_type, body) = media_upload_multipart_body(upload)?;
            let response = self
                .client
                .transport()
                .post_webhook_body(&url, &content_type, body)
                .await?;
            parse_media_upload_response(
                response,
                self.client.transport().error_body_snippet(),
                upload.media_type(),
            )
        })
        .await
    }

    /// Creates a robot message helper for an app robot code.
    pub fn robot(&self, robot_code: impl Into<String>) -> Result<RobotApi> {
        let robot_code = robot_code.into();
        validate_machine_identifier(&robot_code, "robot_code")?;
        Ok(RobotApi {
            openapi: self.clone(),
            robot_code,
        })
    }
}

/// Application robot OpenAPI helper.
#[derive(Clone)]
pub struct RobotApi {
    openapi: OpenApi,
    robot_code: String,
}

impl fmt::Debug for RobotApi {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RobotApi")
            .field("robot_code", &self.robot_code)
            .field("openapi", &self.openapi)
            .finish()
    }
}

impl RobotApi {
    /// Returns the robot code used by this helper.
    #[must_use]
    pub fn robot_code(&self) -> &str {
        &self.robot_code
    }

    /// Returns a copy of this helper with another robot code.
    pub fn with_robot_code(mut self, robot_code: impl Into<String>) -> Result<Self> {
        let robot_code = robot_code.into();
        validate_machine_identifier(&robot_code, "robot_code")?;
        self.robot_code = robot_code;
        Ok(self)
    }

    /// Returns the underlying OpenAPI service.
    #[must_use]
    pub fn openapi(&self) -> &OpenApi {
        &self.openapi
    }

    /// Uploads a DingTalk media resource that can be used by robot messages.
    pub async fn upload_media(&self, upload: MediaUpload) -> Result<UploadedMedia> {
        self.openapi.upload_media(upload).await
    }

    /// Exchanges a robot message `downloadCode` for a temporary file download URL.
    pub async fn message_file_download_url(
        &self,
        download_code: impl Into<String>,
    ) -> Result<MessageFileDownload> {
        validate_machine_identifier(&self.robot_code, "robot_code")?;
        let robot_code = self.robot_code.clone();
        let download_code = download_code.into();
        validate_machine_identifier(&download_code, "download_code")?;

        let request = MessageFileDownloadRequest {
            robot_code,
            download_code,
        };
        let body = self
            .openapi
            .post_raw_text(&["v1.0", "robot", "messageFiles", "download"], &request)
            .await?;

        parse_message_file_download_response(
            &body,
            self.openapi.client.transport().error_body_snippet(),
        )
    }

    /// Downloads a robot-received image, voice, video, or file message by `downloadCode`.
    pub async fn download_message_file(
        &self,
        download_code: impl Into<String>,
    ) -> Result<DownloadedFile> {
        let download = self.message_file_download_url(download_code).await?;
        let url = parse_http_endpoint_url(download.download_url(), "download_url")?;
        let response = self.openapi.client.transport().get_url(&url).await?;
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned);
        let bytes = parse_binary_response(
            response,
            self.openapi.client.transport().error_body_snippet(),
        )?;

        Ok(DownloadedFile {
            download_url: download.into_download_url(),
            content_type,
            bytes,
        })
    }

    /// Sends a message to a group conversation.
    pub async fn send_group_message(
        &self,
        open_conversation_id: impl AsRef<str>,
        message: RobotMessage,
    ) -> Result<RobotMessageResponse> {
        validate_machine_identifier(&self.robot_code, "robot_code")?;
        validate_machine_identifier(open_conversation_id.as_ref(), "open_conversation_id")?;
        let robot_code = self.robot_code.clone();
        let open_conversation_id = open_conversation_id.as_ref().to_string();
        message.validate()?;

        let request = RobotGroupMessageRequest {
            msg_param: message.msg_param_json()?,
            msg_key: message.msg_key()?.to_string(),
            robot_code,
            open_conversation_id,
        };

        let body = self
            .openapi
            .post_raw_text(&["v1.0", "robot", "groupMessages", "send"], &request)
            .await?;

        parse_robot_message_response(&body, self.openapi.client.transport().error_body_snippet())
    }

    /// Sends a text message to a group conversation.
    pub async fn send_group_text(
        &self,
        open_conversation_id: impl AsRef<str>,
        content: impl Into<String>,
    ) -> Result<RobotMessageResponse> {
        self.send_group_message(open_conversation_id, RobotMessage::text(content))
            .await
    }

    /// Sends a markdown message to a group conversation.
    pub async fn send_group_markdown(
        &self,
        open_conversation_id: impl AsRef<str>,
        title: impl Into<String>,
        text: impl Into<String>,
    ) -> Result<RobotMessageResponse> {
        self.send_group_message(open_conversation_id, RobotMessage::markdown(title, text))
            .await
    }

    /// Sends a link message to a group conversation.
    pub async fn send_group_link(
        &self,
        open_conversation_id: impl AsRef<str>,
        title: impl Into<String>,
        text: impl Into<String>,
        message_url: impl Into<String>,
    ) -> Result<RobotMessageResponse> {
        self.send_group_message(
            open_conversation_id,
            RobotMessage::link(title, text, message_url),
        )
        .await
    }

    /// Sends a link message with an image to a group conversation.
    pub async fn send_group_link_with_image(
        &self,
        open_conversation_id: impl AsRef<str>,
        title: impl Into<String>,
        text: impl Into<String>,
        message_url: impl Into<String>,
        pic_url: impl Into<String>,
    ) -> Result<RobotMessageResponse> {
        self.send_group_message(
            open_conversation_id,
            RobotMessage::link_with_image(title, text, message_url, pic_url),
        )
        .await
    }

    /// Sends an image message to a group conversation.
    pub async fn send_group_image(
        &self,
        open_conversation_id: impl AsRef<str>,
        photo_url: impl Into<String>,
    ) -> Result<RobotMessageResponse> {
        self.send_group_message(open_conversation_id, RobotMessage::image(photo_url))
            .await
    }

    /// Sends an action-card message to a group conversation.
    pub async fn send_group_action_card(
        &self,
        open_conversation_id: impl AsRef<str>,
        card: RobotActionCard,
    ) -> Result<RobotMessageResponse> {
        self.send_group_message(open_conversation_id, RobotMessage::action_card(card))
            .await
    }

    /// Sends a voice message to a group conversation.
    pub async fn send_group_audio(
        &self,
        open_conversation_id: impl AsRef<str>,
        media_id: impl Into<String>,
        duration_millis: u64,
    ) -> Result<RobotMessageResponse> {
        self.send_group_message(
            open_conversation_id,
            RobotMessage::audio(media_id, duration_millis),
        )
        .await
    }

    /// Sends a file message to a group conversation.
    pub async fn send_group_file(
        &self,
        open_conversation_id: impl AsRef<str>,
        media_id: impl Into<String>,
        file_name: impl Into<String>,
        file_type: impl Into<String>,
    ) -> Result<RobotMessageResponse> {
        self.send_group_message(
            open_conversation_id,
            RobotMessage::file(media_id, file_name, file_type),
        )
        .await
    }

    /// Sends a video message to a group conversation.
    pub async fn send_group_video(
        &self,
        open_conversation_id: impl AsRef<str>,
        video: RobotVideo,
    ) -> Result<RobotMessageResponse> {
        self.send_group_message(open_conversation_id, RobotMessage::video(video))
            .await
    }

    /// Sends a custom template message to a group conversation.
    pub async fn send_group_custom<T>(
        &self,
        open_conversation_id: impl AsRef<str>,
        msg_key: impl Into<String>,
        msg_param: T,
    ) -> Result<RobotMessageResponse>
    where
        T: Serialize,
    {
        self.send_group_message(
            open_conversation_id,
            RobotMessage::custom(msg_key, msg_param)?,
        )
        .await
    }

    /// Sends a message to one or more users.
    pub async fn send_private_message<I, S>(
        &self,
        user_ids: I,
        message: RobotMessage,
    ) -> Result<RobotMessageResponse>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        validate_machine_identifier(&self.robot_code, "robot_code")?;
        let robot_code = self.robot_code.clone();
        let user_ids = normalize_user_ids(user_ids)?;
        message.validate()?;

        let request = RobotPrivateMessageRequest {
            msg_param: message.msg_param_json()?,
            msg_key: message.msg_key()?.to_string(),
            robot_code,
            user_ids,
        };

        let body = self
            .openapi
            .post_raw_text(&["v1.0", "robot", "oToMessages", "batchSend"], &request)
            .await?;

        parse_robot_message_response(&body, self.openapi.client.transport().error_body_snippet())
    }

    /// Sends a text message to one user.
    pub async fn send_private_text(
        &self,
        user_id: impl AsRef<str>,
        content: impl Into<String>,
    ) -> Result<RobotMessageResponse> {
        self.send_private_message([user_id], RobotMessage::text(content))
            .await
    }

    /// Sends a text message to one or more users.
    pub async fn send_private_text_to_many<I, S>(
        &self,
        user_ids: I,
        content: impl Into<String>,
    ) -> Result<RobotMessageResponse>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.send_private_message(user_ids, RobotMessage::text(content))
            .await
    }

    /// Sends a markdown message to one user.
    pub async fn send_private_markdown(
        &self,
        user_id: impl AsRef<str>,
        title: impl Into<String>,
        text: impl Into<String>,
    ) -> Result<RobotMessageResponse> {
        self.send_private_message([user_id], RobotMessage::markdown(title, text))
            .await
    }

    /// Sends a markdown message to one or more users.
    pub async fn send_private_markdown_to_many<I, S>(
        &self,
        user_ids: I,
        title: impl Into<String>,
        text: impl Into<String>,
    ) -> Result<RobotMessageResponse>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.send_private_message(user_ids, RobotMessage::markdown(title, text))
            .await
    }

    /// Sends a link message to one user.
    pub async fn send_private_link(
        &self,
        user_id: impl AsRef<str>,
        title: impl Into<String>,
        text: impl Into<String>,
        message_url: impl Into<String>,
    ) -> Result<RobotMessageResponse> {
        self.send_private_message([user_id], RobotMessage::link(title, text, message_url))
            .await
    }

    /// Sends a link message to one or more users.
    pub async fn send_private_link_to_many<I, S>(
        &self,
        user_ids: I,
        title: impl Into<String>,
        text: impl Into<String>,
        message_url: impl Into<String>,
    ) -> Result<RobotMessageResponse>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.send_private_message(user_ids, RobotMessage::link(title, text, message_url))
            .await
    }

    /// Sends an image message to one user.
    pub async fn send_private_image(
        &self,
        user_id: impl AsRef<str>,
        photo_url: impl Into<String>,
    ) -> Result<RobotMessageResponse> {
        self.send_private_message([user_id], RobotMessage::image(photo_url))
            .await
    }

    /// Sends an image message to one or more users.
    pub async fn send_private_image_to_many<I, S>(
        &self,
        user_ids: I,
        photo_url: impl Into<String>,
    ) -> Result<RobotMessageResponse>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.send_private_message(user_ids, RobotMessage::image(photo_url))
            .await
    }

    /// Sends an action-card message to one user.
    pub async fn send_private_action_card(
        &self,
        user_id: impl AsRef<str>,
        card: RobotActionCard,
    ) -> Result<RobotMessageResponse> {
        self.send_private_message([user_id], RobotMessage::action_card(card))
            .await
    }

    /// Sends an action-card message to one or more users.
    pub async fn send_private_action_card_to_many<I, S>(
        &self,
        user_ids: I,
        card: RobotActionCard,
    ) -> Result<RobotMessageResponse>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.send_private_message(user_ids, RobotMessage::action_card(card))
            .await
    }

    /// Sends a voice message to one user.
    pub async fn send_private_audio(
        &self,
        user_id: impl AsRef<str>,
        media_id: impl Into<String>,
        duration_millis: u64,
    ) -> Result<RobotMessageResponse> {
        self.send_private_message([user_id], RobotMessage::audio(media_id, duration_millis))
            .await
    }

    /// Sends a voice message to one or more users.
    pub async fn send_private_audio_to_many<I, S>(
        &self,
        user_ids: I,
        media_id: impl Into<String>,
        duration_millis: u64,
    ) -> Result<RobotMessageResponse>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.send_private_message(user_ids, RobotMessage::audio(media_id, duration_millis))
            .await
    }

    /// Sends a file message to one user.
    pub async fn send_private_file(
        &self,
        user_id: impl AsRef<str>,
        media_id: impl Into<String>,
        file_name: impl Into<String>,
        file_type: impl Into<String>,
    ) -> Result<RobotMessageResponse> {
        self.send_private_message(
            [user_id],
            RobotMessage::file(media_id, file_name, file_type),
        )
        .await
    }

    /// Sends a file message to one or more users.
    pub async fn send_private_file_to_many<I, S>(
        &self,
        user_ids: I,
        media_id: impl Into<String>,
        file_name: impl Into<String>,
        file_type: impl Into<String>,
    ) -> Result<RobotMessageResponse>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.send_private_message(user_ids, RobotMessage::file(media_id, file_name, file_type))
            .await
    }

    /// Sends a video message to one user.
    pub async fn send_private_video(
        &self,
        user_id: impl AsRef<str>,
        video: RobotVideo,
    ) -> Result<RobotMessageResponse> {
        self.send_private_message([user_id], RobotMessage::video(video))
            .await
    }

    /// Sends a video message to one or more users.
    pub async fn send_private_video_to_many<I, S>(
        &self,
        user_ids: I,
        video: RobotVideo,
    ) -> Result<RobotMessageResponse>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.send_private_message(user_ids, RobotMessage::video(video))
            .await
    }

    /// Sends a custom template message to one or more users.
    pub async fn send_private_custom<I, S, T>(
        &self,
        user_ids: I,
        msg_key: impl Into<String>,
        msg_param: T,
    ) -> Result<RobotMessageResponse>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
        T: Serialize,
    {
        self.send_private_message(user_ids, RobotMessage::custom(msg_key, msg_param)?)
            .await
    }

    /// Sends a standard interactive card through this application robot.
    pub async fn send_interactive_card(
        &self,
        card: InteractiveCard,
    ) -> Result<InteractiveCardResponse> {
        validate_machine_identifier(&self.robot_code, "robot_code")?;
        let robot_code = self.robot_code.clone();
        card.validate()?;

        let request = card.to_send_request(robot_code);
        let body = self
            .openapi
            .post_raw_text(
                &["v1.0", "im", "v1.0", "robot", "interactiveCards", "send"],
                &request,
            )
            .await?;

        parse_interactive_card_response(&body, self.openapi.client.transport().error_body_snippet())
    }

    /// Updates a standard interactive card previously sent by this application robot.
    pub async fn update_interactive_card(
        &self,
        update: InteractiveCardUpdate,
    ) -> Result<InteractiveCardResponse> {
        update.validate()?;

        let request = update.to_update_request();
        let body = self
            .openapi
            .put_raw_text(&["v1.0", "im", "robots", "interactiveCards"], &request)
            .await?;

        parse_interactive_card_response(&body, self.openapi.client.transport().error_body_snippet())
    }
}

/// DingTalk media resource type accepted by the legacy media upload API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MediaType {
    /// Image resources used by image messages and video covers.
    Image,
    /// Voice resources used by audio messages.
    Voice,
    /// Video resources used by video messages.
    Video,
    /// Generic files such as pdf, docx, xlsx, zip, or rar.
    File,
    /// Future DingTalk media type not yet modeled by this crate.
    Other(String),
}

impl MediaType {
    /// Returns DingTalk's wire value.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Image => "image",
            Self::Voice => "voice",
            Self::Video => "video",
            Self::File => "file",
            Self::Other(value) => value.as_str(),
        }
    }

    /// Creates a media type from DingTalk's wire value.
    pub fn from_raw(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        let value = normalize_machine_identifier(&value, "media_type")?;
        Ok(match value.to_ascii_lowercase().as_str() {
            "image" => Self::Image,
            "voice" | "audio" => Self::Voice,
            "video" => Self::Video,
            "file" => Self::File,
            _ => Self::Other(value),
        })
    }
}

impl fmt::Display for MediaType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Media upload request.
#[derive(Clone, PartialEq, Eq)]
pub struct MediaUpload {
    media_type: MediaType,
    file_name: String,
    content_type: Option<String>,
    bytes: Vec<u8>,
}

impl fmt::Debug for MediaUpload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MediaUpload")
            .field("media_type", &self.media_type)
            .field("file_name", &self.file_name)
            .field("content_type", &self.content_type)
            .field("byte_len", &self.bytes.len())
            .finish()
    }
}

impl MediaUpload {
    /// Creates a media upload from in-memory bytes.
    #[must_use]
    pub fn new(
        media_type: MediaType,
        file_name: impl Into<String>,
        bytes: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            media_type,
            file_name: file_name.into(),
            content_type: None,
            bytes: bytes.into(),
        }
    }

    /// Creates an image upload.
    #[must_use]
    pub fn image(file_name: impl Into<String>, bytes: impl Into<Vec<u8>>) -> Self {
        Self::new(MediaType::Image, file_name, bytes)
    }

    /// Creates a voice upload.
    #[must_use]
    pub fn voice(file_name: impl Into<String>, bytes: impl Into<Vec<u8>>) -> Self {
        Self::new(MediaType::Voice, file_name, bytes)
    }

    /// Creates a video upload.
    #[must_use]
    pub fn video(file_name: impl Into<String>, bytes: impl Into<Vec<u8>>) -> Self {
        Self::new(MediaType::Video, file_name, bytes)
    }

    /// Creates a generic file upload.
    #[must_use]
    pub fn file(file_name: impl Into<String>, bytes: impl Into<Vec<u8>>) -> Self {
        Self::new(MediaType::File, file_name, bytes)
    }

    /// Sets the part content type for the uploaded file.
    #[must_use]
    pub fn content_type(mut self, value: impl Into<String>) -> Self {
        self.content_type = Some(value.into());
        self
    }

    /// Returns the media type.
    #[must_use]
    pub fn media_type(&self) -> &MediaType {
        &self.media_type
    }

    /// Returns the uploaded file name.
    #[must_use]
    pub fn file_name(&self) -> &str {
        &self.file_name
    }

    /// Returns the optional file part content type.
    #[must_use]
    pub fn part_content_type(&self) -> Option<&str> {
        self.content_type.as_deref()
    }

    /// Returns the file bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    fn validate(&self) -> Result<()> {
        validate_machine_identifier(self.media_type.as_str(), "media_type")?;
        validate_file_name(&self.file_name, "file_name")?;
        if self.bytes.is_empty() {
            return Err(Error::invalid_input(
                "media",
                "file content must not be empty",
            ));
        }
        if let Some(content_type) = &self.content_type {
            validate_header_value(content_type, "content_type")?;
        }
        Ok(())
    }
}

/// Uploaded DingTalk media resource.
#[derive(Clone, PartialEq)]
pub struct UploadedMedia {
    media_type: MediaType,
    media_id: String,
    created_at_millis: Option<u64>,
    raw: Value,
}

impl fmt::Debug for UploadedMedia {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UploadedMedia")
            .field("media_type", &self.media_type)
            .field("media_id", &"<redacted>")
            .field("created_at_millis", &self.created_at_millis)
            .field("raw", &redacted_json_value(&self.raw))
            .finish()
    }
}

impl UploadedMedia {
    /// Returns DingTalk's media id.
    #[must_use]
    pub fn media_id(&self) -> &str {
        &self.media_id
    }

    /// Returns the uploaded media type.
    #[must_use]
    pub fn media_type(&self) -> &MediaType {
        &self.media_type
    }

    /// Returns DingTalk's creation timestamp in milliseconds when supplied.
    pub fn created_at_millis(&self) -> Option<u64> {
        self.created_at_millis
    }

    /// Returns the raw JSON response.
    #[must_use]
    pub fn raw(&self) -> &Value {
        &self.raw
    }
}

/// Temporary download URL for a file received by a robot.
#[derive(Clone, PartialEq)]
pub struct MessageFileDownload {
    download_url: String,
    raw: Value,
}

impl fmt::Debug for MessageFileDownload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MessageFileDownload")
            .field("download_url", &redact_text(&self.download_url))
            .field("raw", &redacted_json_value(&self.raw))
            .finish()
    }
}

impl MessageFileDownload {
    /// Returns DingTalk's temporary download URL.
    #[must_use]
    pub fn download_url(&self) -> &str {
        &self.download_url
    }

    /// Consumes this value and returns the temporary download URL.
    #[must_use]
    pub fn into_download_url(self) -> String {
        self.download_url
    }

    /// Returns the raw JSON response.
    #[must_use]
    pub fn raw(&self) -> &Value {
        &self.raw
    }
}

/// Downloaded bytes for a file received by a robot.
#[derive(Clone, PartialEq, Eq)]
pub struct DownloadedFile {
    download_url: String,
    content_type: Option<String>,
    bytes: Vec<u8>,
}

impl fmt::Debug for DownloadedFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DownloadedFile")
            .field("download_url", &redact_text(&self.download_url))
            .field("content_type", &self.content_type)
            .field("byte_len", &self.bytes.len())
            .finish()
    }
}

impl DownloadedFile {
    /// Returns the temporary URL used for the download.
    #[must_use]
    pub fn download_url(&self) -> &str {
        &self.download_url
    }

    /// Returns the response content type when supplied.
    #[must_use]
    pub fn content_type(&self) -> Option<&str> {
        self.content_type.as_deref()
    }

    /// Returns the downloaded bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Consumes this value and returns the downloaded bytes.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

/// Response returned by enterprise robot message send APIs.
#[derive(Debug, Clone, PartialEq)]
pub struct RobotMessageResponse {
    process_query_key: String,
    raw: Value,
}

impl RobotMessageResponse {
    /// Returns DingTalk's process query key for later query or recall APIs.
    #[must_use]
    pub fn process_query_key(&self) -> &str {
        &self.process_query_key
    }

    /// Consumes this value and returns the process query key.
    #[must_use]
    pub fn into_process_query_key(self) -> String {
        self.process_query_key
    }

    /// Returns the raw JSON response.
    #[must_use]
    pub fn raw(&self) -> &Value {
        &self.raw
    }
}

/// Standard interactive card message sent by an application robot.
#[derive(Clone, PartialEq, Eq)]
pub struct InteractiveCard {
    card_template_id: String,
    card_biz_id: String,
    card_data_json: String,
    open_conversation_id: Option<String>,
    single_chat_receiver: Option<String>,
    callback_url: Option<String>,
    user_id_private_data_map_json: Option<String>,
    union_id_private_data_map_json: Option<String>,
    send_options: InteractiveCardSendOptions,
    pull_strategy: Option<bool>,
}

impl fmt::Debug for InteractiveCard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let card_data_json = redact_text(&self.card_data_json);
        let single_chat_receiver = self.single_chat_receiver.as_deref().map(redact_text);
        let callback_url = self.callback_url.as_deref().map(redact_text);
        let user_id_private_data_map_json = self
            .user_id_private_data_map_json
            .as_deref()
            .map(redact_text);
        let union_id_private_data_map_json = self
            .union_id_private_data_map_json
            .as_deref()
            .map(redact_text);
        let send_options = redact_text(&format!("{:?}", self.send_options));

        f.debug_struct("InteractiveCard")
            .field("card_template_id", &self.card_template_id)
            .field("card_biz_id", &self.card_biz_id)
            .field("card_data_json", &card_data_json)
            .field("open_conversation_id", &self.open_conversation_id)
            .field("single_chat_receiver", &single_chat_receiver)
            .field("callback_url", &callback_url)
            .field(
                "user_id_private_data_map_json",
                &user_id_private_data_map_json,
            )
            .field(
                "union_id_private_data_map_json",
                &union_id_private_data_map_json,
            )
            .field("send_options", &send_options)
            .field("pull_strategy", &self.pull_strategy)
            .finish()
    }
}

impl InteractiveCard {
    /// Creates an interactive card without selecting a target conversation.
    ///
    /// Use [`Self::group`] or [`Self::private_user`] when the target is known at construction time.
    pub fn new<T>(
        card_template_id: impl Into<String>,
        card_biz_id: impl Into<String>,
        card_data: T,
    ) -> Result<Self>
    where
        T: Serialize,
    {
        let card_data_json = normalize_json_object("card_data", serde_json::to_value(card_data)?)?;
        Self::from_card_data_json(card_template_id, card_biz_id, card_data_json)
    }

    /// Creates a group interactive card.
    pub fn group<T>(
        open_conversation_id: impl Into<String>,
        card_template_id: impl Into<String>,
        card_biz_id: impl Into<String>,
        card_data: T,
    ) -> Result<Self>
    where
        T: Serialize,
    {
        Self::new(card_template_id, card_biz_id, card_data)?
            .open_conversation_id(open_conversation_id)
    }

    /// Creates a private-chat interactive card from DingTalk's raw `singleChatReceiver` JSON.
    pub fn private_receiver<T>(
        single_chat_receiver: impl Into<String>,
        card_template_id: impl Into<String>,
        card_biz_id: impl Into<String>,
        card_data: T,
    ) -> Result<Self>
    where
        T: Serialize,
    {
        Self::new(card_template_id, card_biz_id, card_data)?
            .single_chat_receiver_json(single_chat_receiver)
    }

    /// Creates a private-chat interactive card for a DingTalk user id.
    pub fn private_user<T>(
        user_id: impl Into<String>,
        card_template_id: impl Into<String>,
        card_biz_id: impl Into<String>,
        card_data: T,
    ) -> Result<Self>
    where
        T: Serialize,
    {
        Self::new(card_template_id, card_biz_id, card_data)?.single_chat_user_id(user_id)
    }

    /// Creates an interactive card from a raw JSON object string for `cardData`.
    pub fn from_card_data_json(
        card_template_id: impl Into<String>,
        card_biz_id: impl Into<String>,
        card_data_json: impl Into<String>,
    ) -> Result<Self> {
        let card_template_id = card_template_id.into();
        let card_biz_id = card_biz_id.into();
        let card_data_json = card_data_json.into();
        validate_machine_identifier(&card_template_id, "card_template_id")?;
        validate_machine_identifier(&card_biz_id, "card_biz_id")?;
        Ok(Self {
            card_template_id,
            card_biz_id,
            card_data_json: normalize_json_object_str("card_data", &card_data_json)?,
            open_conversation_id: None,
            single_chat_receiver: None,
            callback_url: None,
            user_id_private_data_map_json: None,
            union_id_private_data_map_json: None,
            send_options: InteractiveCardSendOptions::new(),
            pull_strategy: None,
        })
    }

    /// Sends this card to a group conversation.
    pub fn open_conversation_id(mut self, value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_machine_identifier(&value, "open_conversation_id")?;
        self.open_conversation_id = Some(value);
        Ok(self)
    }

    /// Sends this card to a private chat target using DingTalk's raw `singleChatReceiver` JSON.
    pub fn single_chat_receiver_json(mut self, value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        let value = normalize_json_object_str("single_chat_receiver", &value)?;
        self.single_chat_receiver = Some(value);
        Ok(self)
    }

    /// Sends this card to a private chat with the supplied DingTalk user id.
    pub fn single_chat_user_id(mut self, user_id: impl Into<String>) -> Result<Self> {
        let user_id = user_id.into();
        validate_machine_identifier(&user_id, "user_id")?;
        self.single_chat_receiver = Some(serde_json::json!({ "userId": user_id }).to_string());
        Ok(self)
    }

    /// Sets the card callback URL for HTTP callback mode.
    ///
    /// Stream mode card callbacks are received on `/v1.0/card/instances/callback`.
    pub fn callback_url(mut self, value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_http_endpoint_url(&value, "callback_url")?;
        self.callback_url = Some(value);
        Ok(self)
    }

    /// Sets whether DingTalk should use pull strategy for this card.
    #[must_use]
    pub fn pull_strategy(mut self, value: bool) -> Self {
        self.pull_strategy = Some(value);
        self
    }

    /// Replaces send options.
    #[must_use]
    pub fn send_options(mut self, options: InteractiveCardSendOptions) -> Self {
        self.send_options = options;
        self
    }

    /// Mentions users when sending this card.
    pub fn at_users<I, S>(mut self, user_ids: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.send_options = self.send_options.at_users(user_ids)?;
        Ok(self)
    }

    /// Mentions everyone when sending this card.
    #[must_use]
    pub fn at_all(mut self) -> Self {
        self.send_options = self.send_options.at_all(true);
        self
    }

    /// Restricts card receivers with DingTalk's `receiverListJson` option.
    pub fn receiver_users<I, S>(mut self, user_ids: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.send_options = self.send_options.receiver_users(user_ids)?;
        Ok(self)
    }

    /// Sets card property JSON from a serializable object.
    pub fn card_property<T>(mut self, value: T) -> Result<Self>
    where
        T: Serialize,
    {
        self.send_options = self.send_options.card_property(value)?;
        Ok(self)
    }

    /// Sets per-user private data keyed by DingTalk user id.
    pub fn user_private_data<T>(mut self, value: T) -> Result<Self>
    where
        T: Serialize,
    {
        self.user_id_private_data_map_json = Some(normalize_private_data_map(
            "user_id_private_data_map",
            serde_json::to_value(value)?,
        )?);
        Ok(self)
    }

    /// Sets per-user private data keyed by union id.
    pub fn union_private_data<T>(mut self, value: T) -> Result<Self>
    where
        T: Serialize,
    {
        self.union_id_private_data_map_json = Some(normalize_private_data_map(
            "union_id_private_data_map",
            serde_json::to_value(value)?,
        )?);
        Ok(self)
    }

    /// Returns the card template id.
    #[must_use]
    pub fn card_template_id(&self) -> &str {
        &self.card_template_id
    }

    /// Returns the caller-defined card business id.
    #[must_use]
    pub fn card_biz_id(&self) -> &str {
        &self.card_biz_id
    }

    /// Returns JSON-encoded `cardData`.
    #[must_use]
    pub fn card_data_json(&self) -> &str {
        &self.card_data_json
    }

    fn validate(&self) -> Result<()> {
        validate_machine_identifier(&self.card_template_id, "card_template_id")?;
        validate_machine_identifier(&self.card_biz_id, "card_biz_id")?;
        normalize_json_object_str("card_data", &self.card_data_json)?;

        match (
            self.open_conversation_id.as_deref(),
            self.single_chat_receiver.as_deref(),
        ) {
            (Some(open_conversation_id), None) => {
                validate_machine_identifier(open_conversation_id, "open_conversation_id")?;
            }
            (None, Some(single_chat_receiver)) => {
                normalize_json_object_str("single_chat_receiver", single_chat_receiver)?;
            }
            (None, None) => {
                return Err(Error::invalid_input(
                    "interactive_card.target",
                    "open_conversation_id or single_chat_receiver is required",
                ));
            }
            (Some(_), Some(_)) => {
                return Err(Error::invalid_input(
                    "interactive_card.target",
                    "provide only one of open_conversation_id or single_chat_receiver",
                ));
            }
        }

        if let Some(callback_url) = &self.callback_url {
            validate_http_endpoint_url(callback_url, "callback_url")?;
        }
        if let Some(value) = &self.user_id_private_data_map_json {
            normalize_private_data_map_str("user_id_private_data_map", value)?;
        }
        if let Some(value) = &self.union_id_private_data_map_json {
            normalize_private_data_map_str("union_id_private_data_map", value)?;
        }
        self.send_options.validate()
    }

    fn to_send_request(&self, robot_code: String) -> InteractiveCardSendRequest {
        InteractiveCardSendRequest {
            card_template_id: self.card_template_id.clone(),
            open_conversation_id: self.open_conversation_id.clone(),
            single_chat_receiver: self.single_chat_receiver.clone(),
            card_biz_id: self.card_biz_id.clone(),
            robot_code,
            callback_url: self.callback_url.clone(),
            card_data: self.card_data_json.clone(),
            user_id_private_data_map: self.user_id_private_data_map_json.clone(),
            union_id_private_data_map: self.union_id_private_data_map_json.clone(),
            send_options: (!self.send_options.is_empty()).then(|| self.send_options.clone()),
            pull_strategy: self.pull_strategy,
        }
    }
}

/// Send options for a standard interactive card.
#[derive(Clone, Default, PartialEq, Eq, Serialize)]
pub struct InteractiveCardSendOptions {
    #[serde(rename = "atUserListJson", skip_serializing_if = "Option::is_none")]
    at_user_list_json: Option<String>,
    #[serde(rename = "atAll", skip_serializing_if = "is_false")]
    at_all: bool,
    #[serde(rename = "receiverListJson", skip_serializing_if = "Option::is_none")]
    receiver_list_json: Option<String>,
    #[serde(rename = "cardPropertyJson", skip_serializing_if = "Option::is_none")]
    card_property_json: Option<String>,
}

impl fmt::Debug for InteractiveCardSendOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InteractiveCardSendOptions")
            .field("has_at_user_list", &self.at_user_list_json.is_some())
            .field("at_all", &self.at_all)
            .field("has_receiver_list", &self.receiver_list_json.is_some())
            .field(
                "card_property_json",
                &self.card_property_json.as_deref().map(redact_text),
            )
            .finish()
    }
}

impl InteractiveCardSendOptions {
    /// Creates empty send options.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets raw `atUserListJson`.
    pub fn at_user_list_json(mut self, value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        self.at_user_list_json = Some(normalize_json_string_array_str(
            "at_user_list_json",
            &value,
        )?);
        Ok(self)
    }

    /// Mentions users when sending the card.
    pub fn at_users<I, S>(mut self, user_ids: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.at_user_list_json = json_string_list("at_user_list_json", user_ids)?;
        Ok(self)
    }

    /// Sets `atAll`.
    #[must_use]
    pub fn at_all(mut self, value: bool) -> Self {
        self.at_all = value;
        self
    }

    /// Sets raw `receiverListJson`.
    pub fn receiver_list_json(mut self, value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        self.receiver_list_json = Some(normalize_json_string_array_str(
            "receiver_list_json",
            &value,
        )?);
        Ok(self)
    }

    /// Restricts receivers to the supplied user ids.
    pub fn receiver_users<I, S>(mut self, user_ids: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.receiver_list_json = json_string_list("receiver_list_json", user_ids)?;
        Ok(self)
    }

    /// Sets card property JSON from a serializable object.
    pub fn card_property<T>(mut self, value: T) -> Result<Self>
    where
        T: Serialize,
    {
        self.card_property_json = Some(normalize_json_object(
            "card_property",
            serde_json::to_value(value)?,
        )?);
        Ok(self)
    }

    /// Sets raw `cardPropertyJson`.
    pub fn card_property_json(mut self, value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        self.card_property_json = Some(normalize_json_object_str("card_property", &value)?);
        Ok(self)
    }

    /// Returns whether these options are empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.at_user_list_json.is_none()
            && !self.at_all
            && self.receiver_list_json.is_none()
            && self.card_property_json.is_none()
    }

    fn validate(&self) -> Result<()> {
        if let Some(value) = &self.at_user_list_json {
            normalize_json_string_array_str("at_user_list_json", value)?;
        }
        if let Some(value) = &self.receiver_list_json {
            normalize_json_string_array_str("receiver_list_json", value)?;
        }
        if let Some(value) = &self.card_property_json {
            normalize_json_object_str("card_property", value)?;
        }
        Ok(())
    }
}

/// Standard interactive card update request.
#[derive(Clone, PartialEq, Eq)]
pub struct InteractiveCardUpdate {
    card_biz_id: String,
    card_data_json: Option<String>,
    user_id_private_data_map_json: Option<String>,
    union_id_private_data_map_json: Option<String>,
    update_options: InteractiveCardUpdateOptions,
}

impl fmt::Debug for InteractiveCardUpdate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let card_data_json = self.card_data_json.as_deref().map(redact_text);
        let user_id_private_data_map_json = self
            .user_id_private_data_map_json
            .as_deref()
            .map(redact_text);
        let union_id_private_data_map_json = self
            .union_id_private_data_map_json
            .as_deref()
            .map(redact_text);

        f.debug_struct("InteractiveCardUpdate")
            .field("card_biz_id", &self.card_biz_id)
            .field("card_data_json", &card_data_json)
            .field(
                "user_id_private_data_map_json",
                &user_id_private_data_map_json,
            )
            .field(
                "union_id_private_data_map_json",
                &union_id_private_data_map_json,
            )
            .field("update_options", &self.update_options)
            .finish()
    }
}

impl InteractiveCardUpdate {
    /// Creates an update with replacement card data.
    pub fn card_data<T>(card_biz_id: impl Into<String>, card_data: T) -> Result<Self>
    where
        T: Serialize,
    {
        let card_biz_id = card_biz_id.into();
        validate_machine_identifier(&card_biz_id, "card_biz_id")?;
        Ok(Self {
            card_biz_id,
            card_data_json: Some(normalize_json_object(
                "card_data",
                serde_json::to_value(card_data)?,
            )?),
            user_id_private_data_map_json: None,
            union_id_private_data_map_json: None,
            update_options: InteractiveCardUpdateOptions::new(),
        })
    }

    /// Creates an update intended for per-user private data without global `cardData`.
    pub fn private_data(card_biz_id: impl Into<String>) -> Result<Self> {
        let card_biz_id = card_biz_id.into();
        validate_machine_identifier(&card_biz_id, "card_biz_id")?;
        Ok(Self {
            card_biz_id,
            card_data_json: None,
            user_id_private_data_map_json: None,
            union_id_private_data_map_json: None,
            update_options: InteractiveCardUpdateOptions::new(),
        })
    }

    /// Creates an update from raw JSON object string for `cardData`.
    pub fn from_card_data_json(
        card_biz_id: impl Into<String>,
        card_data_json: impl Into<String>,
    ) -> Result<Self> {
        let card_biz_id = card_biz_id.into();
        let card_data_json = card_data_json.into();
        validate_machine_identifier(&card_biz_id, "card_biz_id")?;
        Ok(Self {
            card_biz_id,
            card_data_json: Some(normalize_json_object_str("card_data", &card_data_json)?),
            user_id_private_data_map_json: None,
            union_id_private_data_map_json: None,
            update_options: InteractiveCardUpdateOptions::new(),
        })
    }

    /// Sets per-user private data keyed by DingTalk user id.
    pub fn user_private_data<T>(mut self, value: T) -> Result<Self>
    where
        T: Serialize,
    {
        self.user_id_private_data_map_json = Some(normalize_private_data_map(
            "user_id_private_data_map",
            serde_json::to_value(value)?,
        )?);
        Ok(self)
    }

    /// Sets per-user private data keyed by union id.
    pub fn union_private_data<T>(mut self, value: T) -> Result<Self>
    where
        T: Serialize,
    {
        self.union_id_private_data_map_json = Some(normalize_private_data_map(
            "union_id_private_data_map",
            serde_json::to_value(value)?,
        )?);
        Ok(self)
    }

    /// Replaces update options.
    #[must_use]
    pub fn update_options(mut self, options: InteractiveCardUpdateOptions) -> Self {
        self.update_options = options;
        self
    }

    /// Sets `updateCardDataByKey`.
    #[must_use]
    pub fn update_card_data_by_key(mut self, value: bool) -> Self {
        self.update_options = self.update_options.update_card_data_by_key(value);
        self
    }

    /// Sets `updatePrivateDataByKey`.
    #[must_use]
    pub fn update_private_data_by_key(mut self, value: bool) -> Self {
        self.update_options = self.update_options.update_private_data_by_key(value);
        self
    }

    /// Returns the caller-defined card business id.
    #[must_use]
    pub fn card_biz_id(&self) -> &str {
        &self.card_biz_id
    }

    fn validate(&self) -> Result<()> {
        validate_machine_identifier(&self.card_biz_id, "card_biz_id")?;

        if let Some(value) = &self.card_data_json {
            normalize_json_object_str("card_data", value)?;
        }
        if let Some(value) = &self.user_id_private_data_map_json {
            normalize_private_data_map_str("user_id_private_data_map", value)?;
        }
        if let Some(value) = &self.union_id_private_data_map_json {
            normalize_private_data_map_str("union_id_private_data_map", value)?;
        }
        if self.card_data_json.is_none()
            && self.user_id_private_data_map_json.is_none()
            && self.union_id_private_data_map_json.is_none()
        {
            return Err(Error::invalid_input(
                "interactive_card_update",
                "card_data or private data is required",
            ));
        }
        Ok(())
    }

    fn to_update_request(&self) -> InteractiveCardUpdateRequest {
        InteractiveCardUpdateRequest {
            card_biz_id: self.card_biz_id.clone(),
            card_data: self.card_data_json.clone(),
            user_id_private_data_map: self.user_id_private_data_map_json.clone(),
            union_id_private_data_map: self.union_id_private_data_map_json.clone(),
            update_options: (!self.update_options.is_empty()).then_some(self.update_options),
        }
    }
}

/// Update options for a standard interactive card.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct InteractiveCardUpdateOptions {
    #[serde(
        rename = "updateCardDataByKey",
        skip_serializing_if = "Option::is_none"
    )]
    update_card_data_by_key: Option<bool>,
    #[serde(
        rename = "updatePrivateDataByKey",
        skip_serializing_if = "Option::is_none"
    )]
    update_private_data_by_key: Option<bool>,
}

impl InteractiveCardUpdateOptions {
    /// Creates empty update options.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets `updateCardDataByKey`.
    #[must_use]
    pub fn update_card_data_by_key(mut self, value: bool) -> Self {
        self.update_card_data_by_key = Some(value);
        self
    }

    /// Sets `updatePrivateDataByKey`.
    #[must_use]
    pub fn update_private_data_by_key(mut self, value: bool) -> Self {
        self.update_private_data_by_key = Some(value);
        self
    }

    /// Returns whether these options are empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.update_card_data_by_key.is_none() && self.update_private_data_by_key.is_none()
    }
}

/// Response returned by interactive card send/update APIs.
#[derive(Debug, Clone, PartialEq)]
pub struct InteractiveCardResponse {
    process_query_key: String,
    raw: Value,
}

impl InteractiveCardResponse {
    /// Returns DingTalk's process query key.
    #[must_use]
    pub fn process_query_key(&self) -> &str {
        &self.process_query_key
    }

    /// Returns the raw JSON response.
    #[must_use]
    pub fn raw(&self) -> &Value {
        &self.raw
    }
}

/// Button used by robot action-card messages.
#[derive(Clone, PartialEq, Eq)]
pub struct RobotActionButton {
    title: String,
    url: String,
}

impl fmt::Debug for RobotActionButton {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RobotActionButton")
            .field("title", &redact_text(&self.title))
            .field("url", &redact_text(&self.url))
            .finish()
    }
}

impl RobotActionButton {
    /// Creates an action-card button.
    #[must_use]
    pub fn new(title: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            url: url.into(),
        }
    }

    /// Returns the button title.
    #[must_use]
    pub fn title(&self) -> &str {
        &self.title
    }

    /// Returns the button URL.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    fn validate(&self) -> Result<()> {
        non_empty_trimmed(&self.title, "button_title")?;
        validate_http_url(&self.url, "button_url")?;
        Ok(())
    }
}

/// Layout used by multi-button robot action cards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RobotActionCardLayout {
    /// Vertical buttons. DingTalk supports two or three buttons in this layout.
    Vertical,
    /// Horizontal buttons. DingTalk supports exactly two buttons in this layout.
    Horizontal,
}

/// Robot action-card message content.
#[derive(Clone, PartialEq, Eq)]
pub struct RobotActionCard {
    title: String,
    text: String,
    buttons: Vec<RobotActionButton>,
    layout: RobotActionCardLayout,
}

impl fmt::Debug for RobotActionCard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RobotActionCard")
            .field("title", &redact_text(&self.title))
            .field("text", &redact_text(&self.text))
            .field("buttons", &self.buttons)
            .field("layout", &self.layout)
            .finish()
    }
}

impl RobotActionCard {
    /// Creates an action card without buttons.
    ///
    /// Add buttons with [`Self::button`]. DingTalk supports one, two, or three buttons.
    #[must_use]
    pub fn new(title: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            text: text.into(),
            buttons: Vec::new(),
            layout: RobotActionCardLayout::Vertical,
        }
    }

    /// Creates a one-button action card.
    #[must_use]
    pub fn single(
        title: impl Into<String>,
        text: impl Into<String>,
        button_title: impl Into<String>,
        button_url: impl Into<String>,
    ) -> Self {
        Self::new(title, text).button(button_title, button_url)
    }

    /// Adds a button.
    #[must_use]
    pub fn button(mut self, title: impl Into<String>, url: impl Into<String>) -> Self {
        self.buttons.push(RobotActionButton::new(title, url));
        self
    }

    /// Replaces all buttons.
    #[must_use]
    pub fn with_buttons<I>(mut self, buttons: I) -> Self
    where
        I: IntoIterator<Item = RobotActionButton>,
    {
        self.buttons = buttons.into_iter().collect();
        self
    }

    /// Sets the layout.
    #[must_use]
    pub fn with_layout(mut self, layout: RobotActionCardLayout) -> Self {
        self.layout = layout;
        self
    }

    /// Uses the horizontal two-button DingTalk template.
    #[must_use]
    pub fn horizontal(self) -> Self {
        self.with_layout(RobotActionCardLayout::Horizontal)
    }

    /// Uses the vertical DingTalk templates.
    #[must_use]
    pub fn vertical(self) -> Self {
        self.with_layout(RobotActionCardLayout::Vertical)
    }

    /// Returns the title.
    #[must_use]
    pub fn title(&self) -> &str {
        &self.title
    }

    /// Returns the markdown text.
    #[must_use]
    pub fn text(&self) -> &str {
        &self.text
    }

    /// Returns the buttons.
    #[must_use]
    pub fn buttons(&self) -> &[RobotActionButton] {
        &self.buttons
    }

    /// Returns the layout.
    #[must_use]
    pub fn layout(&self) -> RobotActionCardLayout {
        self.layout
    }

    fn msg_key(&self) -> Result<&'static str> {
        self.validate()?;
        match (self.layout, self.buttons.len()) {
            (_, 1) => Ok("sampleActionCard"),
            (RobotActionCardLayout::Vertical, 2) => Ok("sampleActionCard2"),
            (RobotActionCardLayout::Vertical, 3) => Ok("sampleActionCard3"),
            (RobotActionCardLayout::Horizontal, 2) => Ok("sampleActionCard6"),
            _ => Err(Error::invalid_input(
                "buttons",
                "unsupported action-card layout",
            )),
        }
    }

    fn msg_param_json(&self) -> Result<String> {
        self.validate()?;

        let mut value = serde_json::Map::new();
        value.insert("title".to_string(), Value::String(self.title.clone()));
        value.insert("text".to_string(), Value::String(self.text.clone()));

        if self.buttons.len() == 1 {
            let button = &self.buttons[0];
            value.insert(
                "singleTitle".to_string(),
                Value::String(button.title.clone()),
            );
            value.insert("singleURL".to_string(), Value::String(button.url.clone()));
        } else if self.layout == RobotActionCardLayout::Horizontal {
            for (index, button) in self.buttons.iter().enumerate() {
                let number = index + 1;
                value.insert(
                    format!("buttonTitle{number}"),
                    Value::String(button.title.clone()),
                );
                value.insert(
                    format!("buttonUrl{number}"),
                    Value::String(button.url.clone()),
                );
            }
        } else {
            for (index, button) in self.buttons.iter().enumerate() {
                let number = index + 1;
                value.insert(
                    format!("actionTitle{number}"),
                    Value::String(button.title.clone()),
                );
                value.insert(
                    format!("actionURL{number}"),
                    Value::String(button.url.clone()),
                );
            }
        }

        Ok(serde_json::to_string(&Value::Object(value))?)
    }

    fn validate(&self) -> Result<()> {
        non_empty_trimmed(&self.title, "title")?;
        non_empty_trimmed(&self.text, "text")?;

        let button_count = self.buttons.len();
        if !(1..=3).contains(&button_count) {
            return Err(Error::invalid_input(
                "buttons",
                "action cards require one, two, or three buttons",
            ));
        }
        if self.layout == RobotActionCardLayout::Horizontal && button_count > 1 && button_count != 2
        {
            return Err(Error::invalid_input(
                "buttons",
                "horizontal action cards require exactly two buttons",
            ));
        }
        for button in &self.buttons {
            button.validate()?;
        }
        Ok(())
    }
}

/// Robot video message content.
#[derive(Clone, PartialEq, Eq)]
pub struct RobotVideo {
    video_media_id: String,
    duration_seconds: u64,
    video_type: Option<String>,
    pic_media_id: Option<String>,
    height: Option<u32>,
    width: Option<u32>,
}

impl fmt::Debug for RobotVideo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RobotVideo")
            .field("video_media_id", &"<redacted>")
            .field("duration_seconds", &self.duration_seconds)
            .field("video_type", &self.video_type)
            .field(
                "pic_media_id",
                &self.pic_media_id.as_ref().map(|_value| "<redacted>"),
            )
            .field("height", &self.height)
            .field("width", &self.width)
            .finish()
    }
}

impl RobotVideo {
    /// Creates a video message from an uploaded video media id.
    #[must_use]
    pub fn new(video_media_id: impl Into<String>, duration_seconds: u64) -> Self {
        Self {
            video_media_id: video_media_id.into(),
            duration_seconds,
            video_type: None,
            pic_media_id: None,
            height: None,
            width: None,
        }
    }

    /// Sets the video file type. DingTalk currently documents `mp4`.
    #[must_use]
    pub fn video_type(mut self, value: impl Into<String>) -> Self {
        self.video_type = Some(value.into());
        self
    }

    /// Sets the uploaded cover image media id.
    #[must_use]
    pub fn cover(mut self, pic_media_id: impl Into<String>) -> Self {
        self.pic_media_id = Some(pic_media_id.into());
        self
    }

    /// Sets display size in pixels.
    #[must_use]
    pub fn size(mut self, width: u32, height: u32) -> Self {
        self.width = Some(width);
        self.height = Some(height);
        self
    }

    /// Returns the video media id.
    #[must_use]
    pub fn video_media_id(&self) -> &str {
        &self.video_media_id
    }

    /// Returns the duration in seconds.
    #[must_use]
    pub fn duration_seconds(&self) -> u64 {
        self.duration_seconds
    }

    fn validate(&self) -> Result<()> {
        validate_machine_identifier(&self.video_media_id, "video_media_id")?;
        if self.duration_seconds == 0 {
            return Err(Error::invalid_input(
                "duration_seconds",
                "duration must be greater than zero",
            ));
        }
        if let Some(video_type) = &self.video_type {
            validate_machine_identifier(video_type, "video_type")?;
        }
        if let Some(pic_media_id) = &self.pic_media_id {
            validate_machine_identifier(pic_media_id, "pic_media_id")?;
        }
        if matches!(self.width, Some(0)) {
            return Err(Error::invalid_input(
                "width",
                "width must be greater than zero",
            ));
        }
        if matches!(self.height, Some(0)) {
            return Err(Error::invalid_input(
                "height",
                "height must be greater than zero",
            ));
        }
        Ok(())
    }
}

/// Application robot message payload.
#[derive(Clone, PartialEq, Eq)]
pub enum RobotMessage {
    /// Text message.
    Text {
        /// Message content.
        content: String,
    },
    /// Markdown message.
    Markdown {
        /// Notification title.
        title: String,
        /// Markdown body.
        text: String,
    },
    /// Link message.
    Link {
        /// Link title.
        title: String,
        /// Link summary.
        text: String,
        /// Target URL.
        message_url: String,
        /// Optional image URL.
        pic_url: Option<String>,
    },
    /// Image message.
    Image {
        /// DingTalk media id or HTTP image URL expected by DingTalk's `photoURL` field.
        photo_url: String,
    },
    /// Action-card message.
    ActionCard {
        /// Action-card content.
        card: RobotActionCard,
    },
    /// Voice message.
    Audio {
        /// Uploaded media id.
        media_id: String,
        /// Voice duration in milliseconds.
        duration_millis: u64,
    },
    /// File message.
    File {
        /// Uploaded media id.
        media_id: String,
        /// File name shown in DingTalk.
        file_name: String,
        /// File extension/type, for example `pdf`, `xlsx`, `docx`, `zip`, or `rar`.
        file_type: String,
    },
    /// Video message.
    Video {
        /// Video payload.
        video: RobotVideo,
    },
    /// Custom robot template message.
    Custom {
        /// DingTalk robot message template key.
        msg_key: String,
        /// JSON-encoded `msgParam` object.
        msg_param_json: String,
    },
}

impl fmt::Debug for RobotMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Text { content } => f
                .debug_struct("Text")
                .field("content", &redact_text(content))
                .finish(),
            Self::Markdown { title, text } => f
                .debug_struct("Markdown")
                .field("title", &redact_text(title))
                .field("text", &redact_text(text))
                .finish(),
            Self::Link {
                title,
                text,
                message_url,
                pic_url,
            } => f
                .debug_struct("Link")
                .field("title", &redact_text(title))
                .field("text", &redact_text(text))
                .field("message_url", &redact_text(message_url))
                .field("pic_url", &pic_url.as_deref().map(redact_text))
                .finish(),
            Self::Image { photo_url } => f
                .debug_struct("Image")
                .field("photo_url", &redact_text(photo_url))
                .finish(),
            Self::ActionCard { card } => f.debug_struct("ActionCard").field("card", card).finish(),
            Self::Audio {
                media_id,
                duration_millis,
            } => f
                .debug_struct("Audio")
                .field("media_id", &redact_secret(media_id))
                .field("duration_millis", duration_millis)
                .finish(),
            Self::File {
                media_id,
                file_name,
                file_type,
            } => f
                .debug_struct("File")
                .field("media_id", &redact_secret(media_id))
                .field("file_name", &file_name)
                .field("file_type", &file_type)
                .finish(),
            Self::Video { video } => f.debug_struct("Video").field("video", video).finish(),
            Self::Custom {
                msg_key,
                msg_param_json,
            } => f
                .debug_struct("Custom")
                .field("msg_key", msg_key)
                .field("msg_param_json", &redact_text(msg_param_json))
                .finish(),
        }
    }
}

impl RobotMessage {
    /// Creates a text message.
    #[must_use]
    pub fn text(content: impl Into<String>) -> Self {
        Self::Text {
            content: content.into(),
        }
    }

    /// Creates a markdown message.
    #[must_use]
    pub fn markdown(title: impl Into<String>, text: impl Into<String>) -> Self {
        Self::Markdown {
            title: title.into(),
            text: text.into(),
        }
    }

    /// Creates a link message without a picture.
    #[must_use]
    pub fn link(
        title: impl Into<String>,
        text: impl Into<String>,
        message_url: impl Into<String>,
    ) -> Self {
        Self::Link {
            title: title.into(),
            text: text.into(),
            message_url: message_url.into(),
            pic_url: None,
        }
    }

    /// Creates a link message with a picture.
    #[must_use]
    pub fn link_with_image(
        title: impl Into<String>,
        text: impl Into<String>,
        message_url: impl Into<String>,
        pic_url: impl Into<String>,
    ) -> Self {
        Self::Link {
            title: title.into(),
            text: text.into(),
            message_url: message_url.into(),
            pic_url: Some(pic_url.into()),
        }
    }

    /// Creates an image message.
    #[must_use]
    pub fn image(photo_url: impl Into<String>) -> Self {
        Self::Image {
            photo_url: photo_url.into(),
        }
    }

    /// Creates an action-card message.
    #[must_use]
    pub fn action_card(card: RobotActionCard) -> Self {
        Self::ActionCard { card }
    }

    /// Creates a one-button action-card message.
    #[must_use]
    pub fn single_action_card(
        title: impl Into<String>,
        text: impl Into<String>,
        button_title: impl Into<String>,
        button_url: impl Into<String>,
    ) -> Self {
        Self::action_card(RobotActionCard::single(
            title,
            text,
            button_title,
            button_url,
        ))
    }

    /// Creates a voice message from an uploaded media id.
    #[must_use]
    pub fn audio(media_id: impl Into<String>, duration_millis: u64) -> Self {
        Self::Audio {
            media_id: media_id.into(),
            duration_millis,
        }
    }

    /// Creates a file message from an uploaded media id.
    #[must_use]
    pub fn file(
        media_id: impl Into<String>,
        file_name: impl Into<String>,
        file_type: impl Into<String>,
    ) -> Self {
        Self::File {
            media_id: media_id.into(),
            file_name: file_name.into(),
            file_type: file_type.into(),
        }
    }

    /// Creates a file message and infers `fileType` from the file name extension.
    pub fn file_with_inferred_type(
        media_id: impl Into<String>,
        file_name: impl Into<String>,
    ) -> Result<Self> {
        let file_name = file_name.into();
        let file_type = infer_file_type(&file_name)?;
        Ok(Self::file(media_id, file_name, file_type))
    }

    /// Creates a video message.
    #[must_use]
    pub fn video(video: RobotVideo) -> Self {
        Self::Video { video }
    }

    /// Creates a custom template message from a serializable JSON object.
    pub fn custom<T>(msg_key: impl Into<String>, msg_param: T) -> Result<Self>
    where
        T: Serialize,
    {
        let msg_key = msg_key.into();
        validate_machine_identifier(&msg_key, "msg_key")?;
        let value = serde_json::to_value(msg_param)?;
        let msg_param_json = normalize_robot_msg_param_value(value)?;

        Ok(Self::Custom {
            msg_key,
            msg_param_json,
        })
    }

    /// Creates a custom template message from a raw JSON object string.
    pub fn custom_msg_param_json(
        msg_key: impl Into<String>,
        msg_param_json: impl Into<String>,
    ) -> Result<Self> {
        let msg_key = msg_key.into();
        validate_machine_identifier(&msg_key, "msg_key")?;
        let msg_param_json = msg_param_json.into();
        let msg_param_json = normalize_robot_msg_param_json(&msg_param_json)?;

        Ok(Self::Custom {
            msg_key,
            msg_param_json,
        })
    }

    /// Returns the DingTalk robot message template key.
    pub fn msg_key(&self) -> Result<&str> {
        Ok(match self {
            Self::Text { .. } => "sampleText",
            Self::Markdown { .. } => "sampleMarkdown",
            Self::Link { .. } => "sampleLink",
            Self::Image { .. } => "sampleImageMsg",
            Self::ActionCard { card } => card.msg_key()?,
            Self::Audio { .. } => "sampleAudio",
            Self::File { .. } => "sampleFile",
            Self::Video { .. } => "sampleVideo",
            Self::Custom { msg_key, .. } => {
                validate_machine_identifier(msg_key, "msg_key")?;
                msg_key
            }
        })
    }

    /// Returns the JSON-encoded `msgParam` object expected by DingTalk.
    pub fn msg_param_json(&self) -> Result<String> {
        self.validate()?;

        match self {
            Self::Text { content } => {
                let param = RobotTextParam { content };
                Ok(serde_json::to_string(&param)?)
            }
            Self::Markdown { title, text } => {
                let param = RobotMarkdownParam { title, text };
                Ok(serde_json::to_string(&param)?)
            }
            Self::Link {
                title,
                text,
                message_url,
                pic_url,
            } => {
                let param = RobotLinkParam {
                    title,
                    text,
                    message_url,
                    pic_url: pic_url.as_deref(),
                };
                Ok(serde_json::to_string(&param)?)
            }
            Self::Image { photo_url } => {
                let param = RobotImageParam { photo_url };
                Ok(serde_json::to_string(&param)?)
            }
            Self::ActionCard { card } => card.msg_param_json(),
            Self::Audio {
                media_id,
                duration_millis,
            } => {
                let duration = duration_millis.to_string();
                let param = RobotAudioParam {
                    media_id,
                    duration: &duration,
                };
                Ok(serde_json::to_string(&param)?)
            }
            Self::File {
                media_id,
                file_name,
                file_type,
            } => {
                let param = RobotFileParam {
                    media_id,
                    file_name,
                    file_type,
                };
                Ok(serde_json::to_string(&param)?)
            }
            Self::Video { video } => {
                video.validate()?;
                let duration = video.duration_seconds.to_string();
                let height = video.height.map(|value| value.to_string());
                let width = video.width.map(|value| value.to_string());
                let param = RobotVideoParam {
                    duration: &duration,
                    video_media_id: &video.video_media_id,
                    video_type: video.video_type.as_deref().unwrap_or("mp4"),
                    pic_media_id: video.pic_media_id.as_deref(),
                    height: height.as_deref(),
                    width: width.as_deref(),
                };
                Ok(serde_json::to_string(&param)?)
            }
            Self::Custom { msg_param_json, .. } => normalize_robot_msg_param_json(msg_param_json),
        }
    }

    /// Validates this message before sending.
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Text { content } => {
                non_empty_trimmed(content, "content")?;
            }
            Self::Markdown { title, text } => {
                non_empty_trimmed(title, "title")?;
                non_empty_trimmed(text, "text")?;
            }
            Self::Link {
                title,
                text,
                message_url,
                pic_url,
            } => {
                non_empty_trimmed(title, "title")?;
                non_empty_trimmed(text, "text")?;
                validate_http_url(message_url, "message_url")?;
                if let Some(pic_url) = pic_url {
                    validate_http_url(pic_url, "pic_url")?;
                }
            }
            Self::Image { photo_url } => {
                validate_robot_image_reference(photo_url, "photo_url")?;
            }
            Self::ActionCard { card } => {
                card.validate()?;
            }
            Self::Audio {
                media_id,
                duration_millis,
            } => {
                validate_machine_identifier(media_id, "media_id")?;
                if *duration_millis == 0 {
                    return Err(Error::invalid_input(
                        "duration_millis",
                        "duration must be greater than zero",
                    ));
                }
            }
            Self::File {
                media_id,
                file_name,
                file_type,
            } => {
                validate_machine_identifier(media_id, "media_id")?;
                validate_file_name(file_name, "file_name")?;
                validate_machine_identifier(file_type, "file_type")?;
            }
            Self::Video { video } => {
                video.validate()?;
            }
            Self::Custom {
                msg_key,
                msg_param_json,
            } => {
                validate_machine_identifier(msg_key, "msg_key")?;
                normalize_robot_msg_param_json(msg_param_json)?;
            }
        }
        Ok(())
    }
}

fn normalize_robot_msg_param_value(value: Value) -> Result<String> {
    normalize_json_object("msg_param", value)
}

fn normalize_robot_msg_param_json(value: &str) -> Result<String> {
    normalize_json_object_str("msg_param_json", value)
}

fn infer_file_type(file_name: &str) -> Result<String> {
    validate_file_name(file_name, "file_name")?;
    let (_stem, extension) = file_name
        .rsplit_once('.')
        .ok_or_else(|| Error::invalid_input("file_name", "file name must contain an extension"))?;
    normalize_machine_identifier(extension, "file_type").map(|value| value.to_ascii_lowercase())
}

fn normalize_json_object(field: &'static str, value: Value) -> Result<String> {
    if !value.is_object() {
        return Err(Error::invalid_input(
            field,
            "value must serialize to a JSON object",
        ));
    }

    Ok(serde_json::to_string(&value)?)
}

fn normalize_json_object_str(field: &'static str, value: &str) -> Result<String> {
    let value = non_empty_trimmed(value, field)?;
    let value = serde_json::from_str::<Value>(&value)
        .map_err(|source| Error::invalid_input(field, format!("invalid JSON: {source}")))?;
    normalize_json_object(field, value)
}

fn normalize_private_data_map(field: &'static str, value: Value) -> Result<String> {
    let Value::Object(object) = value else {
        return Err(Error::invalid_input(
            field,
            "value must serialize to a JSON object",
        ));
    };

    for (key, value) in object.iter() {
        validate_machine_identifier(key, field)?;
        if !value.is_object() {
            return Err(Error::invalid_input(
                field,
                "private data values must be JSON objects",
            ));
        }
    }

    Ok(serde_json::to_string(&Value::Object(object))?)
}

fn normalize_private_data_map_str(field: &'static str, value: &str) -> Result<String> {
    let value = non_empty_trimmed(value, field)?;
    let value = serde_json::from_str::<Value>(&value)
        .map_err(|source| Error::invalid_input(field, format!("invalid JSON: {source}")))?;
    normalize_private_data_map(field, value)
}

fn normalize_json_string_array_str(field: &'static str, value: &str) -> Result<String> {
    let value = non_empty_trimmed(value, field)?;
    let value = serde_json::from_str::<Value>(&value)
        .map_err(|source| Error::invalid_input(field, format!("invalid JSON: {source}")))?;
    let Value::Array(values) = value else {
        return Err(Error::invalid_input(field, "value must be a JSON array"));
    };

    let mut normalized = Vec::with_capacity(values.len());
    for value in values {
        let Value::String(value) = value else {
            return Err(Error::invalid_input(
                field,
                "array elements must be strings",
            ));
        };
        validate_machine_identifier(&value, field)?;
        normalized.push(Value::String(value));
    }
    if normalized.is_empty() {
        return Err(Error::invalid_input(
            field,
            "at least one user id is required",
        ));
    }
    Ok(Value::Array(normalized).to_string())
}

fn validate_http_url(value: &str, field: &'static str) -> Result<()> {
    parse_http_url(value, field).map(|_url| ())
}

fn validate_robot_image_reference(value: &str, field: &'static str) -> Result<()> {
    let trimmed = non_empty_trimmed(value, field)?;
    if trimmed != value {
        return Err(Error::invalid_input(
            field,
            "value must not contain leading or trailing whitespace",
        ));
    }

    if value.contains("://") || url::Url::parse(value).is_ok() {
        return validate_http_url(value, field);
    }

    if value.chars().any(char::is_whitespace) {
        return Err(Error::invalid_input(
            field,
            "media id must not contain whitespace",
        ));
    }

    validate_no_control_chars(value, field)
}

fn validate_http_endpoint_url(value: &str, field: &'static str) -> Result<()> {
    parse_http_endpoint_url(value, field).map(|_url| ())
}

fn parse_http_endpoint_url(value: &str, field: &'static str) -> Result<url::Url> {
    let url = parse_http_url(value, field)?;
    if url.fragment().is_some() {
        return Err(Error::invalid_input(
            field,
            "URL must not contain a fragment",
        ));
    }
    Ok(url)
}

fn parse_http_url(value: &str, field: &'static str) -> Result<url::Url> {
    let trimmed = non_empty_trimmed(value, field)?;
    if trimmed != value {
        return Err(Error::invalid_input(
            field,
            "value must not contain leading or trailing whitespace",
        ));
    }

    let url = url::Url::parse(value)
        .map_err(|source| Error::invalid_input(field, format!("invalid URL: {source}")))?;
    if !url.username().is_empty() || url.password().is_some() {
        return Err(Error::invalid_input(
            field,
            "URL must not contain username or password",
        ));
    }
    if matches!(url.scheme(), "http" | "https") {
        Ok(url)
    } else {
        Err(Error::invalid_input(
            field,
            "URL scheme must be http or https",
        ))
    }
}

fn validate_no_control_chars(value: &str, field: &'static str) -> Result<()> {
    normalize_no_control_chars(value, field).map(|_value| ())
}

fn validate_header_value(value: &str, field: &'static str) -> Result<()> {
    validate_no_control_chars(value, field)?;
    let trimmed = non_empty_trimmed(value, field)?;
    if trimmed != value {
        return Err(Error::invalid_input(
            field,
            "value must not contain leading or trailing whitespace",
        ));
    }
    Ok(())
}

fn validate_file_name(value: &str, field: &'static str) -> Result<()> {
    validate_header_value(value, field)?;
    if value.contains(['/', '\\']) {
        return Err(Error::invalid_input(
            field,
            "file name must not contain path separators",
        ));
    }
    Ok(())
}

fn normalize_no_control_chars(value: &str, field: &'static str) -> Result<String> {
    if value.chars().any(char::is_control) {
        return Err(Error::invalid_input(
            field,
            "value must not contain control characters",
        ));
    }
    let value = non_empty_trimmed(value, field)?;
    Ok(value)
}

fn validate_machine_identifier(value: &str, field: &'static str) -> Result<()> {
    let normalized = normalize_machine_identifier(value, field)?;
    if normalized != value {
        return Err(Error::invalid_input(
            field,
            "value must not contain leading or trailing whitespace",
        ));
    }
    Ok(())
}

fn normalize_machine_identifier(value: &str, field: &'static str) -> Result<String> {
    let value = normalize_no_control_chars(value, field)?;
    if value.chars().any(char::is_whitespace) {
        return Err(Error::invalid_input(
            field,
            "value must not contain whitespace",
        ));
    }
    Ok(value)
}

fn normalize_user_ids<I, S>(user_ids: I) -> Result<Vec<String>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut values = Vec::<String>::new();
    for value in user_ids {
        let value = value.as_ref();
        validate_machine_identifier(value, "user_id")?;
        if !values.iter().any(|existing| existing == value) {
            values.push(value.to_string());
        }
    }

    if values.is_empty() {
        return Err(Error::invalid_input(
            "user_ids",
            "at least one user id is required",
        ));
    }

    Ok(values)
}

fn json_string_list<I, S>(field: &'static str, values: I) -> Result<Option<String>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut normalized = Vec::<String>::new();
    for value in values {
        let value = value.as_ref();
        validate_machine_identifier(value, field)?;
        if !normalized.iter().any(|existing| existing == value) {
            normalized.push(value.to_string());
        }
    }
    if normalized.is_empty() {
        return Err(Error::invalid_input(
            field,
            "at least one user id is required",
        ));
    }

    Ok(Some(
        Value::Array(normalized.into_iter().map(Value::String).collect()).to_string(),
    ))
}

fn media_upload_multipart_body(upload: &MediaUpload) -> Result<(String, Vec<u8>)> {
    upload.validate()?;
    let media_type = upload.media_type().as_str();

    let boundary = media_upload_boundary(upload);
    let mut body = Vec::new();

    multipart_text_field(&mut body, &boundary, "type", media_type);
    multipart_file_field(
        &mut body,
        &boundary,
        "media",
        upload.file_name(),
        upload
            .part_content_type()
            .unwrap_or("application/octet-stream"),
        upload.bytes(),
    );
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

    Ok((format!("multipart/form-data; boundary={boundary}"), body))
}

fn multipart_text_field(body: &mut Vec<u8>, boundary: &str, name: &str, value: &str) {
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        format!(
            "Content-Disposition: form-data; name=\"{}\"\r\n\r\n",
            multipart_quote(name)
        )
        .as_bytes(),
    );
    body.extend_from_slice(value.as_bytes());
    body.extend_from_slice(b"\r\n");
}

fn multipart_file_field(
    body: &mut Vec<u8>,
    boundary: &str,
    name: &str,
    file_name: &str,
    content_type: &str,
    bytes: &[u8],
) {
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        format!(
            "Content-Disposition: form-data; name=\"{}\"; filename=\"{}\"\r\n",
            multipart_quote(name),
            multipart_quote(file_name)
        )
        .as_bytes(),
    );
    body.extend_from_slice(format!("Content-Type: {content_type}\r\n\r\n").as_bytes());
    body.extend_from_slice(bytes);
    body.extend_from_slice(b"\r\n");
}

fn multipart_quote(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn media_upload_boundary(upload: &MediaUpload) -> String {
    let media_type = upload
        .media_type()
        .as_str()
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>();
    let media_type = if media_type.is_empty() {
        "media"
    } else {
        media_type.as_str()
    };

    let mut suffix = 0_u128;
    loop {
        let boundary = format!(
            "----dingding-{media_type}-{}-{}-{suffix}",
            upload.file_name().len(),
            upload.bytes().len()
        );
        if !contains_subslice(upload.bytes(), boundary.as_bytes()) {
            return boundary;
        }
        suffix = suffix.saturating_add(1);
    }
}

fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

fn parse_media_upload_response(
    response: reqx::Response,
    error_body_snippet: crate::transport::BodySnippetConfig,
    requested_media_type: &MediaType,
) -> Result<UploadedMedia> {
    let (parsed, body) =
        decode_json_response::<RawMediaUploadResponse>(response, error_body_snippet)?;
    if let Some(error) = response_envelope_error(
        parsed.errcode,
        parsed.api_code.as_deref(),
        parsed.errmsg.as_deref(),
        parsed.success,
        parsed.request_id.as_deref(),
        &body,
        error_body_snippet,
    ) {
        return Err(error);
    }

    let raw = serde_json::from_str::<Value>(&body)?;
    let media_id = required_response_string(
        parsed.media_id.as_deref(),
        "media_id",
        "missing media_id in DingTalk response",
        parsed.request_id.clone(),
        &body,
        error_body_snippet,
    )?;
    let media_type = match parsed.media_type {
        Some(value) => MediaType::from_raw(value)?,
        None => requested_media_type.clone(),
    };
    let created_at_millis = parsed
        .created_at
        .and_then(|value| u64::try_from(value).ok());

    Ok(UploadedMedia {
        media_type,
        media_id,
        created_at_millis,
        raw,
    })
}

fn parse_message_file_download_response(
    body: &str,
    error_body_snippet: BodySnippetConfig,
) -> Result<MessageFileDownload> {
    let raw = serde_json::from_str::<Value>(body)?;
    let payload = raw
        .get("result")
        .filter(|value| value.is_object())
        .unwrap_or(&raw);
    let download_url = payload
        .get("downloadUrl")
        .or_else(|| payload.get("download_url"))
        .or_else(|| payload.get("url"))
        .and_then(Value::as_str);
    let download_url = required_response_string(
        download_url,
        "download_url",
        "missing downloadUrl in DingTalk response",
        response_request_id(&raw),
        body,
        error_body_snippet,
    )?;
    validate_http_endpoint_url(&download_url, "download_url")?;

    Ok(MessageFileDownload { download_url, raw })
}

fn parse_robot_message_response(
    body: &str,
    error_body_snippet: BodySnippetConfig,
) -> Result<RobotMessageResponse> {
    let (process_query_key, raw) = parse_process_query_key_response(body, error_body_snippet)?;

    Ok(RobotMessageResponse {
        process_query_key,
        raw,
    })
}

fn parse_interactive_card_response(
    body: &str,
    error_body_snippet: BodySnippetConfig,
) -> Result<InteractiveCardResponse> {
    let (process_query_key, raw) = parse_process_query_key_response(body, error_body_snippet)?;

    Ok(InteractiveCardResponse {
        process_query_key,
        raw,
    })
}

fn parse_process_query_key_response(
    body: &str,
    error_body_snippet: BodySnippetConfig,
) -> Result<(String, Value)> {
    let raw = serde_json::from_str::<Value>(body)?;
    let payload = raw
        .get("result")
        .filter(|value| value.is_object())
        .unwrap_or(&raw);
    let process_query_key = payload
        .get("processQueryKey")
        .or_else(|| payload.get("process_query_key"));
    let process_query_key = required_response_value_string(
        process_query_key,
        "process_query_key",
        "missing processQueryKey in DingTalk response",
        response_request_id(&raw),
        body,
        error_body_snippet,
    )?;

    Ok((process_query_key, raw))
}

fn required_response_value_string(
    value: Option<&Value>,
    field: &'static str,
    message: &'static str,
    request_id: Option<String>,
    body: &str,
    error_body_snippet: BodySnippetConfig,
) -> Result<String> {
    value
        .and_then(|value| response_value_string(value, field))
        .ok_or_else(|| api_error_from_body(-1, message, request_id, body, error_body_snippet))
}

fn required_response_string(
    value: Option<&str>,
    field: &'static str,
    message: &'static str,
    request_id: Option<String>,
    body: &str,
    error_body_snippet: BodySnippetConfig,
) -> Result<String> {
    value
        .and_then(|value| non_empty_trimmed(value, field).ok())
        .ok_or_else(|| api_error_from_body(-1, message, request_id, body, error_body_snippet))
}

fn response_value_string(value: &Value, field: &'static str) -> Option<String> {
    match value {
        Value::String(value) => non_empty_trimmed(value, field).ok(),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn response_request_id(value: &Value) -> Option<String> {
    value
        .get("requestId")
        .or_else(|| value.get("RequestId"))
        .or_else(|| value.get("requestid"))
        .or_else(|| value.get("request_id"))
        .and_then(|value| match value {
            Value::String(value) => non_empty_trimmed(value, "request_id").ok(),
            Value::Number(value) => Some(value.to_string()),
            _ => None,
        })
}

fn redacted_json_value(value: &Value) -> String {
    redact_text(&value.to_string())
}

fn redact_secret(_value: &str) -> &'static str {
    "<redacted>"
}

fn is_false(value: &bool) -> bool {
    !*value
}

impl OpenApi {
    async fn with_access_token<T, F, Fut>(&self, mut request: F) -> Result<T>
    where
        F: FnMut(String) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let token = self.access_token().await?;
        match request(token.clone()).await {
            Err(error) if is_rejected_access_token(&error) => {
                let credentials = self.credentials.as_ref().ok_or(Error::MissingCredentials)?;
                // A late rejection must not evict a token refreshed by another request.
                self.client.invalidate_access_token(credentials, &token);
                let token = self.access_token().await?;
                let result = request(token.clone()).await;
                if result.as_ref().is_err_and(is_rejected_access_token) {
                    self.client.invalidate_access_token(credentials, &token);
                }
                result
            }
            result => result,
        }
    }

    async fn post_raw_text<B>(&self, segments: &[&str], body: &B) -> Result<String>
    where
        B: Serialize + ?Sized,
    {
        let url = self.client.openapi_endpoint(segments)?;
        let url = &url;
        self.with_access_token(|access_token| async move {
            let response = self
                .client
                .transport()
                .post_openapi_json(url, Some(&access_token), body)
                .await?;
            parse_standard_text_response(response, self.client.transport().error_body_snippet())
        })
        .await
    }

    async fn put_raw_text<B>(&self, segments: &[&str], body: &B) -> Result<String>
    where
        B: Serialize + ?Sized,
    {
        let url = self.client.openapi_endpoint(segments)?;
        let url = &url;
        self.with_access_token(|access_token| async move {
            let response = self
                .client
                .transport()
                .put_openapi_json(url, Some(&access_token), body)
                .await?;
            parse_standard_text_response(response, self.client.transport().error_body_snippet())
        })
        .await
    }
}

fn is_rejected_access_token(error: &Error) -> bool {
    matches!(error.errcode(), Some(40014 | 42001))
        || matches!(
            error.api_code(),
            Some(
                "InvalidAuthentication"
                    | "InvalidAuthentication.AccessTokenInvalid"
                    | "InvalidAuthentication.AccessTokenExpired"
            )
        )
}

#[derive(Deserialize)]
struct AccessTokenResponse {
    #[serde(
        default,
        deserialize_with = "crate::transport::deserialize_optional_i64"
    )]
    errcode: Option<i64>,
    #[serde(
        rename = "code",
        default,
        alias = "Code",
        deserialize_with = "crate::transport::deserialize_optional_string"
    )]
    api_code: Option<String>,
    #[serde(
        default,
        alias = "message",
        alias = "errorMessage",
        alias = "ErrorMessage",
        alias = "error_message",
        deserialize_with = "crate::transport::deserialize_optional_string"
    )]
    errmsg: Option<String>,
    #[serde(
        default,
        deserialize_with = "crate::transport::deserialize_optional_bool"
    )]
    success: Option<bool>,
    #[serde(alias = "accessToken")]
    access_token: Option<String>,
    #[serde(alias = "expiresIn", alias = "expireIn")]
    #[serde(
        default,
        deserialize_with = "crate::transport::deserialize_optional_i64"
    )]
    expires_in: Option<i64>,
    #[serde(
        default,
        alias = "requestId",
        alias = "RequestId",
        alias = "requestid",
        deserialize_with = "crate::transport::deserialize_optional_string"
    )]
    request_id: Option<String>,
}

#[derive(Deserialize)]
struct RawMediaUploadResponse {
    #[serde(
        default,
        deserialize_with = "crate::transport::deserialize_optional_i64"
    )]
    errcode: Option<i64>,
    #[serde(
        rename = "code",
        default,
        alias = "Code",
        deserialize_with = "crate::transport::deserialize_optional_string"
    )]
    api_code: Option<String>,
    #[serde(
        default,
        alias = "message",
        alias = "errorMessage",
        alias = "ErrorMessage",
        alias = "error_message",
        deserialize_with = "crate::transport::deserialize_optional_string"
    )]
    errmsg: Option<String>,
    #[serde(
        default,
        deserialize_with = "crate::transport::deserialize_optional_bool"
    )]
    success: Option<bool>,
    #[serde(
        default,
        alias = "requestId",
        alias = "RequestId",
        alias = "requestid",
        deserialize_with = "crate::transport::deserialize_optional_string"
    )]
    request_id: Option<String>,
    #[serde(alias = "mediaId")]
    media_id: Option<String>,
    #[serde(rename = "type")]
    media_type: Option<String>,
    #[serde(alias = "createdAt")]
    #[serde(
        default,
        deserialize_with = "crate::transport::deserialize_optional_i64"
    )]
    created_at: Option<i64>,
}

#[derive(Serialize)]
struct RobotGroupMessageRequest {
    #[serde(rename = "msgParam")]
    msg_param: String,
    #[serde(rename = "msgKey")]
    msg_key: String,
    #[serde(rename = "robotCode")]
    robot_code: String,
    #[serde(rename = "openConversationId")]
    open_conversation_id: String,
}

#[derive(Serialize)]
struct RobotPrivateMessageRequest {
    #[serde(rename = "msgParam")]
    msg_param: String,
    #[serde(rename = "msgKey")]
    msg_key: String,
    #[serde(rename = "robotCode")]
    robot_code: String,
    #[serde(rename = "userIds")]
    user_ids: Vec<String>,
}

#[derive(Serialize)]
struct MessageFileDownloadRequest {
    #[serde(rename = "robotCode")]
    robot_code: String,
    #[serde(rename = "downloadCode")]
    download_code: String,
}

#[derive(Serialize)]
struct InteractiveCardSendRequest {
    #[serde(rename = "cardTemplateId")]
    card_template_id: String,
    #[serde(rename = "openConversationId", skip_serializing_if = "Option::is_none")]
    open_conversation_id: Option<String>,
    #[serde(rename = "singleChatReceiver", skip_serializing_if = "Option::is_none")]
    single_chat_receiver: Option<String>,
    #[serde(rename = "cardBizId")]
    card_biz_id: String,
    #[serde(rename = "robotCode")]
    robot_code: String,
    #[serde(rename = "callbackUrl", skip_serializing_if = "Option::is_none")]
    callback_url: Option<String>,
    #[serde(rename = "cardData")]
    card_data: String,
    #[serde(
        rename = "userIdPrivateDataMap",
        skip_serializing_if = "Option::is_none"
    )]
    user_id_private_data_map: Option<String>,
    #[serde(
        rename = "unionIdPrivateDataMap",
        skip_serializing_if = "Option::is_none"
    )]
    union_id_private_data_map: Option<String>,
    #[serde(rename = "sendOptions", skip_serializing_if = "Option::is_none")]
    send_options: Option<InteractiveCardSendOptions>,
    #[serde(rename = "pullStrategy", skip_serializing_if = "Option::is_none")]
    pull_strategy: Option<bool>,
}

#[derive(Serialize)]
struct InteractiveCardUpdateRequest {
    #[serde(rename = "cardBizId")]
    card_biz_id: String,
    #[serde(rename = "cardData", skip_serializing_if = "Option::is_none")]
    card_data: Option<String>,
    #[serde(
        rename = "userIdPrivateDataMap",
        skip_serializing_if = "Option::is_none"
    )]
    user_id_private_data_map: Option<String>,
    #[serde(
        rename = "unionIdPrivateDataMap",
        skip_serializing_if = "Option::is_none"
    )]
    union_id_private_data_map: Option<String>,
    #[serde(rename = "updateOptions", skip_serializing_if = "Option::is_none")]
    update_options: Option<InteractiveCardUpdateOptions>,
}

#[derive(Serialize)]
struct RobotTextParam<'a> {
    content: &'a str,
}

#[derive(Serialize)]
struct RobotMarkdownParam<'a> {
    title: &'a str,
    text: &'a str,
}

#[derive(Serialize)]
struct RobotLinkParam<'a> {
    title: &'a str,
    text: &'a str,
    #[serde(rename = "messageUrl")]
    message_url: &'a str,
    #[serde(rename = "picUrl", skip_serializing_if = "Option::is_none")]
    pic_url: Option<&'a str>,
}

#[derive(Serialize)]
struct RobotImageParam<'a> {
    #[serde(rename = "photoURL")]
    photo_url: &'a str,
}

#[derive(Serialize)]
struct RobotAudioParam<'a> {
    #[serde(rename = "mediaId")]
    media_id: &'a str,
    duration: &'a str,
}

#[derive(Serialize)]
struct RobotFileParam<'a> {
    #[serde(rename = "mediaId")]
    media_id: &'a str,
    #[serde(rename = "fileName")]
    file_name: &'a str,
    #[serde(rename = "fileType")]
    file_type: &'a str,
}

#[derive(Serialize)]
struct RobotVideoParam<'a> {
    duration: &'a str,
    #[serde(rename = "videoMediaId")]
    video_media_id: &'a str,
    #[serde(rename = "videoType")]
    video_type: &'a str,
    #[serde(rename = "picMediaId", skip_serializing_if = "Option::is_none")]
    pic_media_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    height: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    width: Option<&'a str>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn robot_text_message_uses_sample_text_key() {
        let message = RobotMessage::text("hello");

        assert_eq!(message.msg_key().expect("msg key"), "sampleText");
        assert_eq!(
            message.msg_param_json().expect("json"),
            r#"{"content":"hello"}"#
        );
    }

    #[test]
    fn robot_msg_param_json_rejects_invalid_messages() {
        let error = RobotMessage::text(" ")
            .msg_param_json()
            .expect_err("empty text should not serialize as a sendable message");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn robot_markdown_message_uses_sample_markdown_key() {
        let message = RobotMessage::markdown("title", "**body**");

        assert_eq!(message.msg_key().expect("msg key"), "sampleMarkdown");
        assert_eq!(
            message.msg_param_json().expect("json"),
            r#"{"title":"title","text":"**body**"}"#
        );
    }

    #[test]
    fn robot_rich_messages_use_typed_template_keys() {
        let link = RobotMessage::link_with_image(
            "docs",
            "read this",
            "https://example.com/docs",
            "https://example.com/docs.png",
        );
        let image = RobotMessage::image("MEDIA_IMAGE");
        let audio = RobotMessage::audio("MEDIA_AUDIO", 1_200);
        let file = RobotMessage::file("MEDIA_FILE", "report.pdf", "pdf");
        let video = RobotMessage::video(
            RobotVideo::new("MEDIA_VIDEO", 10)
                .cover("MEDIA_COVER")
                .size(640, 360),
        );

        assert_eq!(link.msg_key().expect("msg key"), "sampleLink");
        assert_eq!(image.msg_key().expect("msg key"), "sampleImageMsg");
        assert_eq!(audio.msg_key().expect("msg key"), "sampleAudio");
        assert_eq!(file.msg_key().expect("msg key"), "sampleFile");
        assert_eq!(video.msg_key().expect("msg key"), "sampleVideo");

        assert_eq!(
            serde_json::from_str::<Value>(&link.msg_param_json().expect("link")).expect("json"),
            serde_json::json!({
                "title": "docs",
                "text": "read this",
                "messageUrl": "https://example.com/docs",
                "picUrl": "https://example.com/docs.png"
            })
        );
        assert_eq!(
            serde_json::from_str::<Value>(&audio.msg_param_json().expect("audio")).expect("json"),
            serde_json::json!({
                "mediaId": "MEDIA_AUDIO",
                "duration": "1200"
            })
        );
        assert_eq!(
            serde_json::from_str::<Value>(&video.msg_param_json().expect("video")).expect("json"),
            serde_json::json!({
                "duration": "10",
                "height": "360",
                "picMediaId": "MEDIA_COVER",
                "videoMediaId": "MEDIA_VIDEO",
                "videoType": "mp4",
                "width": "640"
            })
        );
    }

    #[test]
    fn robot_message_builders_reject_untrimmed_identifier_and_url_fields() {
        let link = RobotMessage::link_with_image(
            "title",
            "body",
            " https://example.com/open ",
            " https://example.com/pic.png ",
        );
        let file = RobotMessage::file(" media-file ", " report.pdf ", " pdf ");

        assert_eq!(
            link.msg_param_json()
                .expect_err("URL whitespace should be rejected")
                .kind(),
            crate::ErrorKind::InvalidInput
        );
        assert_eq!(
            file.msg_param_json()
                .expect_err("media id whitespace should be rejected")
                .kind(),
            crate::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn robot_link_messages_validate_http_urls() {
        let message = RobotMessage::link("title", "body", "ftp://example.com/file");
        let error = message
            .validate()
            .expect_err("link message URL should require HTTP");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn robot_link_messages_reject_url_userinfo() {
        let message = RobotMessage::link("title", "body", "https://user:pass@example.com/file");
        let error = message
            .validate()
            .expect_err("link message URL should not contain credentials");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn robot_image_messages_validate_http_urls() {
        RobotMessage::image("@lADOpwk3K80C0M0C0A")
            .validate()
            .expect("media id image should be accepted");

        let non_http = RobotMessage::image("ftp://example.com/image.png")
            .validate()
            .expect_err("image URL should require HTTP");
        let userinfo = RobotMessage::image("https://user:pass@example.com/image.png")
            .validate()
            .expect_err("image URL should not contain credentials");
        let whitespace = RobotMessage::image("not a media id")
            .validate()
            .expect_err("media id must not contain whitespace");

        assert_eq!(non_http.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(userinfo.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(whitespace.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn robot_messages_validate_machine_identifiers_strictly() {
        let audio = RobotMessage::Audio {
            media_id: " media-1 ".to_string(),
            duration_millis: 1,
        }
        .validate()
        .expect_err("manual media id should not contain surrounding whitespace");
        let file_type = RobotMessage::File {
            media_id: "media-1".to_string(),
            file_name: "report.pdf".to_string(),
            file_type: "p df".to_string(),
        }
        .validate()
        .expect_err("manual file type should not contain whitespace");
        let file_name = RobotMessage::File {
            media_id: "media-1".to_string(),
            file_name: " report.pdf ".to_string(),
            file_type: "pdf".to_string(),
        }
        .validate()
        .expect_err("manual file name should not contain surrounding whitespace");
        let file_path = RobotMessage::File {
            media_id: "media-1".to_string(),
            file_name: "reports/report.pdf".to_string(),
            file_type: "pdf".to_string(),
        }
        .validate()
        .expect_err("manual file name should not contain path separators");
        let video = RobotMessage::video(RobotVideo::new("video media", 10))
            .validate()
            .expect_err("video media id should not contain whitespace");
        let custom = RobotMessage::Custom {
            msg_key: " sampleText ".to_string(),
            msg_param_json: r#"{"content":"hello"}"#.to_string(),
        }
        .msg_key()
        .expect_err("manual msg key should not contain surrounding whitespace");

        assert_eq!(audio.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(file_type.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(file_name.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(file_path.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(video.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(custom.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn inferred_file_type_rejects_untrimmed_file_names() {
        let error = RobotMessage::file_with_inferred_type("media-1", " report.pdf ")
            .expect_err("inference should not rewrite file names");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn robot_link_messages_reject_untrimmed_manual_urls() {
        let message = RobotMessage::Link {
            title: "title".to_string(),
            text: "body".to_string(),
            message_url: " https://example.com/open ".to_string(),
            pic_url: None,
        };
        let error = message
            .validate()
            .expect_err("serialized URL would contain whitespace");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn robot_action_card_selects_dingtalk_template() {
        let single =
            RobotMessage::single_action_card("title", "body", "open", "https://example.com/open");
        let vertical = RobotMessage::action_card(
            RobotActionCard::new("title", "body")
                .button("approve", "https://example.com/a")
                .button("reject", "https://example.com/r"),
        );
        let horizontal = RobotMessage::action_card(
            RobotActionCard::new("title", "body")
                .button("yes", "https://example.com/y")
                .button("no", "https://example.com/n")
                .horizontal(),
        );

        assert_eq!(single.msg_key().expect("msg key"), "sampleActionCard");
        assert_eq!(vertical.msg_key().expect("msg key"), "sampleActionCard2");
        assert_eq!(horizontal.msg_key().expect("msg key"), "sampleActionCard6");
        assert_eq!(
            serde_json::from_str::<Value>(&horizontal.msg_param_json().expect("json"))
                .expect("json"),
            serde_json::json!({
                "buttonTitle1": "yes",
                "buttonTitle2": "no",
                "buttonUrl1": "https://example.com/y",
                "buttonUrl2": "https://example.com/n",
                "text": "body",
                "title": "title"
            })
        );
    }

    #[test]
    fn robot_action_card_msg_key_rejects_invalid_layout() {
        let message = RobotMessage::action_card(RobotActionCard::new("title", "body"));

        let error = message
            .msg_key()
            .expect_err("action card without buttons should not produce a template key");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn robot_custom_message_uses_supplied_key_and_param() {
        let message = RobotMessage::custom(
            "sampleActionCard",
            serde_json::json!({
                "title": "title",
                "text": "body",
                "singleTitle": "open",
                "singleURL": "https://example.com"
            }),
        )
        .expect("custom message");

        assert_eq!(message.msg_key().expect("msg key"), "sampleActionCard");
        assert_eq!(
            message.msg_param_json().expect("json"),
            r#"{"singleTitle":"open","singleURL":"https://example.com","text":"body","title":"title"}"#
        );
    }

    #[test]
    fn robot_custom_param_json_is_normalized() {
        let message = RobotMessage::custom_msg_param_json(
            "sampleText",
            r#"{
                "content": "hello"
            }"#,
        )
        .expect("custom message");

        assert_eq!(message.msg_key().expect("msg key"), "sampleText");
        assert_eq!(
            message.msg_param_json().expect("json"),
            r#"{"content":"hello"}"#
        );
    }

    #[test]
    fn media_upload_builds_multipart_body() {
        let upload = MediaUpload::image("demo.png", b"PNG".to_vec()).content_type("image/png");
        let (content_type, body) = media_upload_multipart_body(&upload).expect("multipart");
        let body = String::from_utf8(body).expect("utf8 multipart");

        assert!(content_type.starts_with("multipart/form-data; boundary="));
        assert!(body.contains(r#"name="type""#));
        assert!(body.contains("\r\nimage\r\n"));
        assert!(body.contains(r#"name="media"; filename="demo.png""#));
        assert!(body.contains("Content-Type: image/png"));
        assert!(body.contains("PNG"));
    }

    #[test]
    fn media_upload_debug_does_not_dump_file_bytes() {
        let upload = MediaUpload::image("demo.png", b"secret-image-bytes".to_vec())
            .content_type("image/png");
        let debug = format!("{upload:?}");

        assert!(debug.contains("byte_len"));
        assert!(!debug.contains("secret-image-bytes"));
    }

    #[test]
    fn uploaded_media_debug_redacts_media_id() {
        let uploaded = UploadedMedia {
            media_type: MediaType::Image,
            media_id: "media-secret".to_string(),
            created_at_millis: Some(1_700_000_000_000),
            raw: serde_json::json!({
                "media_id": "media-secret",
                "type": "image"
            }),
        };
        let debug = format!("{uploaded:?}");

        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("media-secret"));
    }

    #[test]
    fn media_upload_normalizes_custom_media_type_wire_value() {
        let upload = MediaUpload::new(
            MediaType::from_raw(" custom ").expect("media type"),
            "demo.bin",
            b"BIN".to_vec(),
        );
        let (_content_type, body) = media_upload_multipart_body(&upload).expect("multipart");
        let body = String::from_utf8(body).expect("utf8 multipart");

        assert!(body.contains("\r\ncustom\r\n"));
        assert!(!body.contains("\r\n custom \r\n"));
    }

    #[test]
    fn media_upload_rejects_manual_whitespace_media_type() {
        let error = media_upload_multipart_body(&MediaUpload::new(
            MediaType::Other(" custom ".to_string()),
            "demo.bin",
            b"BIN".to_vec(),
        ))
        .expect_err("manual media type should be strict");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn media_upload_rejects_control_characters_in_multipart_headers() {
        let file_name_error = media_upload_multipart_body(&MediaUpload::file(
            "report.pdf\r\nx-injected: 1",
            b"PDF".to_vec(),
        ))
        .expect_err("file name must not inject headers");
        let content_type_error = media_upload_multipart_body(
            &MediaUpload::file("report.pdf", b"PDF".to_vec())
                .content_type("application/pdf\r\nx-injected: 1"),
        )
        .expect_err("content type must not inject headers");
        let media_type_error = media_upload_multipart_body(&MediaUpload::new(
            MediaType::Other("file\nbad".to_string()),
            "report.pdf",
            b"PDF".to_vec(),
        ))
        .expect_err("media type must not contain control characters");

        assert_eq!(file_name_error.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(content_type_error.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(media_type_error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn media_upload_rejects_untrimmed_or_path_file_names() {
        let untrimmed =
            media_upload_multipart_body(&MediaUpload::file(" report.pdf ", b"PDF".to_vec()))
                .expect_err("file names should not be rewritten");
        let path =
            media_upload_multipart_body(&MediaUpload::file("reports/report.pdf", b"PDF".to_vec()))
                .expect_err("file names should not include paths");

        assert_eq!(untrimmed.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(path.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn media_upload_boundary_avoids_file_content_collisions() {
        let upload = MediaUpload::file(
            "report.pdf",
            b"prefix ----dingding-file-10-39-0 suffix".to_vec(),
        );

        assert_eq!(media_upload_boundary(&upload), "----dingding-file-10-39-1");
    }

    #[test]
    fn parses_message_file_download_response() {
        let download = parse_message_file_download_response(
            r#"{"errcode":0,"result":{"downloadUrl":" https://example.com/file.bin?ticket=download-ticket "}}"#,
            BodySnippetConfig::default(),
        )
        .expect("download");

        assert_eq!(
            download.download_url(),
            "https://example.com/file.bin?ticket=download-ticket"
        );
        assert_eq!(
            download.raw()["result"]["downloadUrl"],
            " https://example.com/file.bin?ticket=download-ticket "
        );
    }

    #[test]
    fn download_types_debug_redacts_temporary_url_and_bytes() {
        let download = MessageFileDownload {
            download_url: "https://example.com/file.bin?ticket=download-ticket".to_string(),
            raw: serde_json::json!({
                "result": {
                    "downloadUrl": "https://example.com/file.bin?ticket=download-ticket"
                }
            }),
        };
        let file = DownloadedFile {
            download_url: download.download_url().to_string(),
            content_type: Some("application/octet-stream".to_string()),
            bytes: b"secret-file-bytes".to_vec(),
        };
        let download_debug = format!("{download:?}");
        let file_debug = format!("{file:?}");

        assert!(download_debug.contains("ticket=<redacted>"));
        assert!(file_debug.contains("byte_len"));
        assert!(!download_debug.contains("download-ticket"));
        assert!(!file_debug.contains("download-ticket"));
        assert!(!file_debug.contains("secret-file-bytes"));
    }

    #[test]
    fn message_file_download_rejects_fragment_url() {
        let error = parse_message_file_download_response(
            r#"{"errcode":0,"result":{"downloadUrl":"https://example.com/file.bin#token"}}"#,
            BodySnippetConfig::default(),
        )
        .expect_err("download URL should not contain a fragment");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn interactive_card_send_request_is_serialized() {
        let card = InteractiveCard::group(
            "open-cid",
            "template-id",
            "card-biz-id",
            serde_json::json!({ "title": "Deploy", "status": "ok" }),
        )
        .expect("card")
        .callback_url("https://example.com/card/callback")
        .expect("callback url")
        .at_users(["user-1", "user-1", "user-2"])
        .expect("at users")
        .receiver_users(["user-1"])
        .expect("receiver users")
        .pull_strategy(true)
        .card_property(serde_json::json!({ "theme": "blue" }))
        .expect("card property")
        .user_private_data(serde_json::json!({
            "user-1": { "status": "read" }
        }))
        .expect("private data");

        let request = card.to_send_request("robot-code".to_string());
        let value = serde_json::to_value(request).expect("serialize");

        assert_eq!(card.card_template_id(), "template-id");
        assert_eq!(card.card_biz_id(), "card-biz-id");
        assert_eq!(value["cardTemplateId"], "template-id");
        assert_eq!(value["openConversationId"], "open-cid");
        assert_eq!(value["robotCode"], "robot-code");
        assert_eq!(value["callbackUrl"], "https://example.com/card/callback");
        assert_eq!(
            value["cardData"].as_str(),
            Some(r#"{"status":"ok","title":"Deploy"}"#)
        );
        assert_eq!(
            value["sendOptions"]["atUserListJson"],
            r#"["user-1","user-2"]"#
        );
        assert_eq!(value["sendOptions"]["receiverListJson"], r#"["user-1"]"#);
        assert_eq!(
            value["sendOptions"]["cardPropertyJson"],
            r#"{"theme":"blue"}"#
        );
        assert_eq!(value["pullStrategy"], true);
    }

    #[test]
    fn interactive_card_debug_redacts_callback_and_private_data() {
        let card = InteractiveCard::group(
            "open-cid",
            "template-id",
            "card-biz-id",
            serde_json::json!({ "title": "Deploy", "token": "card-token" }),
        )
        .expect("card")
        .callback_url("https://example.com/card/callback?token=callback-token")
        .expect("callback url")
        .user_private_data(serde_json::json!({
            "user-1": { "token": "private-token" }
        }))
        .expect("private data");

        let debug = format!("{card:?}");

        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("card-token"));
        assert!(!debug.contains("callback-token"));
        assert!(!debug.contains("private-token"));
    }

    #[test]
    fn interactive_card_update_debug_redacts_card_and_private_data() {
        let update = InteractiveCardUpdate::card_data(
            "card-biz-id",
            serde_json::json!({ "token": "card-token" }),
        )
        .expect("update")
        .user_private_data(serde_json::json!({
            "user-1": { "token": "private-token" }
        }))
        .expect("private data");
        let debug = format!("{update:?}");

        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("card-token"));
        assert!(!debug.contains("private-token"));
    }

    #[test]
    fn robot_message_debug_redacts_urls_media_ids_and_custom_params() {
        let link = RobotMessage::link_with_image(
            "docs",
            "read token=body-token",
            "https://example.com/docs?token=link-token",
            "https://example.com/pic.png?token=pic-token",
        );
        let action = RobotMessage::single_action_card(
            "title",
            "body",
            "open",
            "https://example.com/open?token=button-token",
        );
        let audio = RobotMessage::audio("media-secret", 1_000);
        let custom = RobotMessage::custom(
            "sampleText",
            serde_json::json!({ "content": "hello", "token": "param-token" }),
        )
        .expect("custom");
        let debug = format!("{link:?} {action:?} {audio:?} {custom:?}");

        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("body-token"));
        assert!(!debug.contains("link-token"));
        assert!(!debug.contains("pic-token"));
        assert!(!debug.contains("button-token"));
        assert!(!debug.contains("media-secret"));
        assert!(!debug.contains("param-token"));
    }

    #[test]
    fn interactive_card_user_lists_reject_empty_inputs() {
        let card = InteractiveCard::group(
            "open-cid",
            "template-id",
            "card-biz-id",
            serde_json::json!({ "title": "Deploy" }),
        )
        .expect("card");

        let empty_at_users = card
            .clone()
            .at_users(Vec::<&str>::new())
            .expect_err("empty at user list should fail");
        let blank_at_users = card
            .clone()
            .at_users([" "])
            .expect_err("blank at user ids should fail");
        let empty_receivers = card
            .receiver_users(Vec::<&str>::new())
            .expect_err("empty receiver list should fail");

        assert_eq!(empty_at_users.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(blank_at_users.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(empty_receivers.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn interactive_card_raw_user_lists_are_canonicalized() {
        let options = InteractiveCardSendOptions::new()
            .at_user_list_json(r#" [ "user-1" , "user-2" ] "#)
            .expect("at users")
            .receiver_list_json(r#"["user-1"]"#)
            .expect("receivers");
        let value = serde_json::to_value(options).expect("serialize");

        assert_eq!(value["atUserListJson"], r#"["user-1","user-2"]"#);
        assert_eq!(value["receiverListJson"], r#"["user-1"]"#);
    }

    #[test]
    fn interactive_card_send_options_debug_redacts_receivers_and_properties() {
        let options = InteractiveCardSendOptions::new()
            .at_users(["at-user-secret"])
            .expect("at users")
            .receiver_users(["receiver-secret"])
            .expect("receivers")
            .card_property(serde_json::json!({ "token": "property-token" }))
            .expect("property");
        let debug = format!("{options:?}");

        assert!(debug.contains("has_at_user_list"));
        assert!(debug.contains("has_receiver_list"));
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("at-user-secret"));
        assert!(!debug.contains("receiver-secret"));
        assert!(!debug.contains("property-token"));
    }

    #[test]
    fn interactive_card_update_request_is_serialized() {
        let update = InteractiveCardUpdate::card_data(
            "card-biz-id",
            serde_json::json!({ "status": "done" }),
        )
        .expect("update")
        .update_card_data_by_key(true)
        .update_private_data_by_key(false);

        let value = serde_json::to_value(update.to_update_request()).expect("serialize");

        assert_eq!(update.card_biz_id(), "card-biz-id");
        assert_eq!(value["cardBizId"], "card-biz-id");
        assert_eq!(value["cardData"], r#"{"status":"done"}"#);
        assert_eq!(value["updateOptions"]["updateCardDataByKey"], true);
        assert_eq!(value["updateOptions"]["updatePrivateDataByKey"], false);
    }

    #[test]
    fn interactive_card_private_user_builds_receiver_json() {
        let card = InteractiveCard::private_user(
            "user-1",
            "template-id",
            "card-biz-id",
            serde_json::json!({ "title": "hello" }),
        )
        .expect("card");
        let request = card.to_send_request("robot-code".to_string());
        let value = serde_json::to_value(request).expect("serialize");

        assert_eq!(value["singleChatReceiver"], r#"{"userId":"user-1"}"#);
    }

    #[test]
    fn parses_interactive_card_response() {
        let direct = parse_interactive_card_response(
            r#"{"processQueryKey":" query-1 "}"#,
            BodySnippetConfig::default(),
        )
        .expect("direct");
        let wrapped = parse_interactive_card_response(
            r#"{"errcode":0,"errmsg":"ok","result":{"processQueryKey":" query-2 "}}"#,
            BodySnippetConfig::default(),
        )
        .expect("wrapped");

        assert_eq!(direct.process_query_key(), "query-1");
        assert_eq!(direct.raw()["processQueryKey"], " query-1 ");
        assert_eq!(wrapped.process_query_key(), "query-2");
    }

    #[test]
    fn parses_numeric_process_query_key_response() {
        let response = parse_robot_message_response(
            r#"{"errcode":0,"processQueryKey":12345}"#,
            BodySnippetConfig::default(),
        )
        .expect("response");

        assert_eq!(response.process_query_key(), "12345");
    }

    #[test]
    fn interactive_card_response_errors_preserve_numeric_request_id() {
        let error = parse_interactive_card_response(
            r#"{"requestId":12345,"result":{}}"#,
            BodySnippetConfig::default(),
        )
        .expect_err("missing processQueryKey should fail");

        assert_eq!(error.kind(), crate::ErrorKind::Api);
        assert_eq!(error.request_id(), Some("12345"));
    }

    #[test]
    fn rejects_invalid_interactive_card_inputs() {
        let invalid_data = InteractiveCard::group("cid", "template", "biz", ["not", "object"])
            .expect_err("card data must be object");
        let missing_target =
            InteractiveCard::new("template", "biz", serde_json::json!({ "title": "hello" }))
                .expect("card")
                .validate()
                .expect_err("target is required");
        let invalid_update = InteractiveCardUpdate::private_data("biz")
            .expect("update")
            .validate()
            .expect_err("some update data is required");
        let invalid_private_user =
            InteractiveCard::private_user(" ", "template", "biz", serde_json::json!({}))
                .expect_err("empty private user id should fail");
        let invalid_private_receiver =
            InteractiveCard::private_receiver("not json", "template", "biz", serde_json::json!({}))
                .expect_err("singleChatReceiver must be JSON object");
        let invalid_callback_url =
            InteractiveCard::group("cid", "template", "biz", serde_json::json!({}))
                .expect("card")
                .callback_url("ftp://example.com/callback")
                .expect_err("callback URL should require HTTP");
        let callback_url_fragment =
            InteractiveCard::group("cid", "template", "biz", serde_json::json!({}))
                .expect("card")
                .callback_url("https://example.com/callback#token")
                .expect_err("callback URL should not contain a fragment");
        let invalid_at_users = InteractiveCardSendOptions::new()
            .at_user_list_json(r#"[" user-1 "]"#)
            .expect_err("raw atUserListJson values should be strict user ids");
        let empty_raw_at_users = InteractiveCardSendOptions::new()
            .at_user_list_json(r#"[]"#)
            .expect_err("raw atUserListJson should not be empty");
        let invalid_receivers = InteractiveCardSendOptions::new()
            .receiver_list_json(r#"[123]"#)
            .expect_err("raw receiverListJson values should be strings");
        let empty_raw_receivers = InteractiveCardSendOptions::new()
            .receiver_list_json(r#"[]"#)
            .expect_err("raw receiverListJson should not be empty");
        let invalid_typed_at_users = InteractiveCardSendOptions::new()
            .at_users([" user-1 "])
            .expect_err("typed at users should be strict user ids");
        let invalid_card_id =
            InteractiveCard::group("cid", "template id", "biz", serde_json::json!({}))
                .expect_err("card template id should be a machine identifier");
        let untrimmed_card_id =
            InteractiveCard::group("cid", " template", "biz", serde_json::json!({}))
                .expect_err("card template id should not be rewritten");
        let untrimmed_conversation_id =
            InteractiveCard::group(" cid ", "template", "biz", serde_json::json!({}))
                .expect_err("conversation id should not be rewritten");
        let invalid_update_id = InteractiveCardUpdate::private_data(" biz ")
            .expect_err("card update id should not be rewritten");
        let invalid_private_data_key =
            InteractiveCard::group("cid", "template", "biz", serde_json::json!({}))
                .expect("card")
                .user_private_data(serde_json::json!({
                    " user-1 ": { "status": "ok" }
                }))
                .expect_err("private data keys should be strict user ids");
        let invalid_private_data_value =
            InteractiveCard::group("cid", "template", "biz", serde_json::json!({}))
                .expect("card")
                .user_private_data(serde_json::json!({
                    "user-1": "ok"
                }))
                .expect_err("private data values should be JSON objects");
        let invalid_update_private_data_value = InteractiveCardUpdate::private_data("biz")
            .expect("update")
            .user_private_data(serde_json::json!({
                "user-1": true
            }))
            .expect_err("update private data values should be JSON objects");

        assert_eq!(invalid_data.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(missing_target.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(invalid_update.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(invalid_private_user.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(
            invalid_private_receiver.kind(),
            crate::ErrorKind::InvalidInput
        );
        assert_eq!(invalid_callback_url.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(callback_url_fragment.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(invalid_at_users.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(empty_raw_at_users.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(invalid_receivers.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(empty_raw_receivers.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(
            invalid_typed_at_users.kind(),
            crate::ErrorKind::InvalidInput
        );
        assert_eq!(invalid_card_id.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(untrimmed_card_id.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(
            untrimmed_conversation_id.kind(),
            crate::ErrorKind::InvalidInput
        );
        assert_eq!(invalid_update_id.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(
            invalid_private_data_key.kind(),
            crate::ErrorKind::InvalidInput
        );
        assert_eq!(
            invalid_private_data_value.kind(),
            crate::ErrorKind::InvalidInput
        );
        assert_eq!(
            invalid_update_private_data_value.kind(),
            crate::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn openapi_and_robot_helpers_can_be_reconfigured() {
        let client = DingTalk::builder().build().expect("client");
        let openapi = client
            .openapi()
            .with_credentials(AppCredentials::new("app-key", "app-secret"))
            .expect("openapi credentials");
        let robot = openapi
            .robot("robot-a")
            .expect("robot")
            .with_robot_code("robot-b")
            .expect("robot code");

        assert_eq!(
            robot
                .openapi()
                .credentials()
                .map(|credentials| credentials.app_key()),
            Some("app-key")
        );
        assert_eq!(robot.robot_code(), "robot-b");
    }

    #[tokio::test]
    async fn custom_openapi_endpoint_segments_are_validated_before_credentials() {
        let client = DingTalk::builder().build().expect("client");

        let error = client
            .openapi()
            .post_json_result::<Value, _>(&[".."], &serde_json::json!({ "ping": true }))
            .await
            .expect_err("relative endpoint segments should fail before credentials");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn openapi_robot_helpers_validate_configuration_immediately() {
        let client = DingTalk::builder().build().expect("client");
        let openapi = client.openapi();
        let robot_error = openapi
            .robot(" robot-a ")
            .expect_err("robot code should not be rewritten");
        let credential_error = openapi
            .clone()
            .with_credentials(AppCredentials::new("app-key", " app-secret "))
            .expect_err("credentials should be validated immediately");
        let reconfigured_error = openapi
            .robot("robot-a")
            .expect("robot")
            .with_robot_code("robot b")
            .expect_err("new robot code should be validated immediately");

        assert_eq!(robot_error.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(credential_error.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(reconfigured_error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn rejects_custom_message_without_object_param() {
        let error = RobotMessage::custom("sampleText", ["not", "an", "object"])
            .expect_err("array msgParam should fail");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn custom_message_serializes_canonical_msg_param_json() {
        let message = RobotMessage::Custom {
            msg_key: "sampleText".to_string(),
            msg_param_json: r#" { "content" : "hello" } "#.to_string(),
        };

        assert_eq!(
            message.msg_param_json().expect("json"),
            r#"{"content":"hello"}"#
        );
    }

    #[test]
    fn rejects_empty_custom_msg_key() {
        let error = RobotMessage::custom(" ", serde_json::json!({"content": "hello"}))
            .expect_err("empty msgKey should fail");
        let whitespace =
            RobotMessage::custom("sample Text", serde_json::json!({"content": "hello"}))
                .expect_err("msgKey should be a machine identifier");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(whitespace.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn rejects_empty_private_user_ids() {
        let error =
            normalize_user_ids::<[&str; 0], &str>([]).expect_err("empty user list should fail");
        let whitespace =
            normalize_user_ids(["user 1"]).expect_err("user id must not contain whitespace");
        let surrounding_whitespace = normalize_user_ids([" user-1 "])
            .expect_err("user id must not contain surrounding whitespace");
        let control =
            normalize_user_ids(["user\n1"]).expect_err("user id must not contain control chars");
        let surrounding_control =
            normalize_user_ids(["\nuser-1"]).expect_err("user id must not contain control chars");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(whitespace.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(
            surrounding_whitespace.kind(),
            crate::ErrorKind::InvalidInput
        );
        assert_eq!(control.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(surrounding_control.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn private_user_ids_are_deduplicated_without_rewriting_values() {
        let values = normalize_user_ids(["user-1", "user-2", "user-1"]).expect("user ids");

        assert_eq!(values, ["user-1".to_string(), "user-2".to_string()]);
    }
}
