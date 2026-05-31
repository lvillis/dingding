use std::fmt;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::{
    DingTalk, Error, Result,
    auth::AppCredentials,
    transport::{
        api_error_from_body, decode_json_response, parse_binary_response, parse_dingtalk_result,
        parse_standard_text_response,
    },
    util::non_empty_trimmed,
};

/// Minimal OpenAPI service.
#[derive(Clone)]
pub struct OpenApi {
    client: DingTalk,
    credentials: Option<AppCredentials>,
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
    #[must_use]
    pub fn with_credentials(mut self, credentials: AppCredentials) -> Self {
        self.credentials = Some(credentials);
        self
    }

    /// Returns an access token, using the in-memory cache when possible.
    pub async fn access_token(&self) -> Result<String> {
        let credentials = self.credentials.as_ref().ok_or(Error::MissingCredentials)?;
        credentials.validate()?;

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

        if response.errcode != 0 {
            return Err(api_error_from_body(
                response.errcode,
                response.errmsg,
                response.request_id,
                &body,
                self.client.transport().error_body_snippet(),
            ));
        }

        let token = response.access_token.ok_or_else(|| {
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
        let access_token = self.access_token().await?;
        let url = self.client.openapi_endpoint(segments)?;
        parse_dingtalk_result(
            self.client
                .transport()
                .post_openapi_json(&url, Some(&access_token), body)
                .await?,
            self.client.transport().error_body_snippet(),
        )
    }

    /// Uploads a DingTalk media resource for robot image, voice, video, or file messages.
    pub async fn upload_media(&self, upload: MediaUpload) -> Result<UploadedMedia> {
        upload.validate()?;

        let access_token = self.access_token().await?;
        let mut url = self.client.webhook_endpoint(&["media", "upload"])?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("access_token", &access_token);
            query.append_pair("type", upload.media_type().as_str());
        }

        let (content_type, body) = media_upload_multipart_body(&upload)?;
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
    }

    /// Creates a robot message helper for an app robot code.
    #[must_use]
    pub fn robot(&self, robot_code: impl Into<String>) -> RobotApi {
        RobotApi {
            openapi: self.clone(),
            robot_code: normalize_robot_code(robot_code),
        }
    }
}

/// Application robot OpenAPI helper.
#[derive(Clone)]
pub struct RobotApi {
    openapi: OpenApi,
    robot_code: String,
}

impl RobotApi {
    /// Returns the robot code used by this helper.
    #[must_use]
    pub fn robot_code(&self) -> &str {
        &self.robot_code
    }

    /// Returns a copy of this helper with another robot code.
    #[must_use]
    pub fn with_robot_code(mut self, robot_code: impl Into<String>) -> Self {
        self.robot_code = normalize_robot_code(robot_code);
        self
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
        let robot_code = non_empty_trimmed(&self.robot_code, "robot_code")?;
        let download_code = download_code.into();
        let download_code = non_empty_trimmed(&download_code, "download_code")?;

        let request = MessageFileDownloadRequest {
            robot_code,
            download_code,
        };
        let body = self
            .openapi
            .post_raw_text(&["v1.0", "robot", "messageFiles", "download"], &request)
            .await?;

        parse_message_file_download_response(&body)
    }

    /// Downloads a robot-received image, voice, video, or file message by `downloadCode`.
    pub async fn download_message_file(
        &self,
        download_code: impl Into<String>,
    ) -> Result<DownloadedFile> {
        let download = self.message_file_download_url(download_code).await?;
        let url = url::Url::parse(download.download_url()).map_err(|source| {
            Error::invalid_input(
                "download_url",
                format!("invalid URL from DingTalk: {source}"),
            )
        })?;
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
    ) -> Result<String> {
        let robot_code = non_empty_trimmed(&self.robot_code, "robot_code")?;
        let open_conversation_id =
            non_empty_trimmed(open_conversation_id.as_ref(), "open_conversation_id")?;
        message.validate()?;

        let request = RobotGroupMessageRequest {
            msg_param: message.msg_param_json()?,
            msg_key: message.msg_key()?.to_string(),
            robot_code,
            open_conversation_id,
        };

        self.openapi
            .post_raw_text(&["v1.0", "robot", "groupMessages", "send"], &request)
            .await
    }

    /// Sends a text message to a group conversation.
    pub async fn send_group_text(
        &self,
        open_conversation_id: impl AsRef<str>,
        content: impl Into<String>,
    ) -> Result<String> {
        self.send_group_message(open_conversation_id, RobotMessage::text(content))
            .await
    }

    /// Sends a markdown message to a group conversation.
    pub async fn send_group_markdown(
        &self,
        open_conversation_id: impl AsRef<str>,
        title: impl Into<String>,
        text: impl Into<String>,
    ) -> Result<String> {
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
    ) -> Result<String> {
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
    ) -> Result<String> {
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
    ) -> Result<String> {
        self.send_group_message(open_conversation_id, RobotMessage::image(photo_url))
            .await
    }

    /// Sends an action-card message to a group conversation.
    pub async fn send_group_action_card(
        &self,
        open_conversation_id: impl AsRef<str>,
        card: RobotActionCard,
    ) -> Result<String> {
        self.send_group_message(open_conversation_id, RobotMessage::action_card(card))
            .await
    }

    /// Sends a voice message to a group conversation.
    pub async fn send_group_audio(
        &self,
        open_conversation_id: impl AsRef<str>,
        media_id: impl Into<String>,
        duration_millis: u64,
    ) -> Result<String> {
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
    ) -> Result<String> {
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
    ) -> Result<String> {
        self.send_group_message(open_conversation_id, RobotMessage::video(video))
            .await
    }

    /// Sends a custom template message to a group conversation.
    pub async fn send_group_custom<T>(
        &self,
        open_conversation_id: impl AsRef<str>,
        msg_key: impl Into<String>,
        msg_param: T,
    ) -> Result<String>
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
    ) -> Result<String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let robot_code = non_empty_trimmed(&self.robot_code, "robot_code")?;
        let user_ids = normalize_user_ids(user_ids)?;
        message.validate()?;

        let request = RobotPrivateMessageRequest {
            msg_param: message.msg_param_json()?,
            msg_key: message.msg_key()?.to_string(),
            robot_code,
            user_ids,
        };

        self.openapi
            .post_raw_text(&["v1.0", "robot", "oToMessages", "batchSend"], &request)
            .await
    }

    /// Sends a text message to one user.
    pub async fn send_private_text(
        &self,
        user_id: impl AsRef<str>,
        content: impl Into<String>,
    ) -> Result<String> {
        self.send_private_message([user_id], RobotMessage::text(content))
            .await
    }

    /// Sends a text message to one or more users.
    pub async fn send_private_text_to_many<I, S>(
        &self,
        user_ids: I,
        content: impl Into<String>,
    ) -> Result<String>
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
    ) -> Result<String> {
        self.send_private_message([user_id], RobotMessage::markdown(title, text))
            .await
    }

    /// Sends a markdown message to one or more users.
    pub async fn send_private_markdown_to_many<I, S>(
        &self,
        user_ids: I,
        title: impl Into<String>,
        text: impl Into<String>,
    ) -> Result<String>
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
    ) -> Result<String> {
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
    ) -> Result<String>
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
    ) -> Result<String> {
        self.send_private_message([user_id], RobotMessage::image(photo_url))
            .await
    }

    /// Sends an image message to one or more users.
    pub async fn send_private_image_to_many<I, S>(
        &self,
        user_ids: I,
        photo_url: impl Into<String>,
    ) -> Result<String>
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
    ) -> Result<String> {
        self.send_private_message([user_id], RobotMessage::action_card(card))
            .await
    }

    /// Sends an action-card message to one or more users.
    pub async fn send_private_action_card_to_many<I, S>(
        &self,
        user_ids: I,
        card: RobotActionCard,
    ) -> Result<String>
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
    ) -> Result<String> {
        self.send_private_message([user_id], RobotMessage::audio(media_id, duration_millis))
            .await
    }

    /// Sends a voice message to one or more users.
    pub async fn send_private_audio_to_many<I, S>(
        &self,
        user_ids: I,
        media_id: impl Into<String>,
        duration_millis: u64,
    ) -> Result<String>
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
    ) -> Result<String> {
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
    ) -> Result<String>
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
    ) -> Result<String> {
        self.send_private_message([user_id], RobotMessage::video(video))
            .await
    }

    /// Sends a video message to one or more users.
    pub async fn send_private_video_to_many<I, S>(
        &self,
        user_ids: I,
        video: RobotVideo,
    ) -> Result<String>
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
    ) -> Result<String>
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
        let robot_code = non_empty_trimmed(&self.robot_code, "robot_code")?;
        card.validate()?;

        let request = card.to_send_request(robot_code);
        let body = self
            .openapi
            .post_raw_text(
                &["v1.0", "im", "v1.0", "robot", "interactiveCards", "send"],
                &request,
            )
            .await?;

        parse_interactive_card_response(&body)
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

        parse_interactive_card_response(&body)
    }
}

fn normalize_robot_code(robot_code: impl Into<String>) -> String {
    robot_code.into().trim().to_string()
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
        let value = non_empty_trimmed(&value, "media_type")?;
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaUpload {
    media_type: MediaType,
    file_name: String,
    content_type: Option<String>,
    bytes: Vec<u8>,
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
            file_name: file_name.into().trim().to_string(),
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
        self.content_type = Some(value.into().trim().to_string());
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
        non_empty_trimmed(self.media_type.as_str(), "media_type")?;
        non_empty_trimmed(&self.file_name, "file_name")?;
        if self.bytes.is_empty() {
            return Err(Error::invalid_input(
                "media",
                "file content must not be empty",
            ));
        }
        if let Some(content_type) = &self.content_type {
            non_empty_trimmed(content_type, "content_type")?;
        }
        Ok(())
    }
}

/// Uploaded DingTalk media resource.
#[derive(Debug, Clone, PartialEq)]
pub struct UploadedMedia {
    media_type: MediaType,
    media_id: String,
    created_at_millis: Option<u64>,
    raw: Value,
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
    #[must_use]
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
#[derive(Debug, Clone, PartialEq)]
pub struct MessageFileDownload {
    download_url: String,
    raw: Value,
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadedFile {
    download_url: String,
    content_type: Option<String>,
    bytes: Vec<u8>,
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

/// Standard interactive card message sent by an application robot.
#[derive(Debug, Clone, PartialEq, Eq)]
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
        Ok(Self::new(card_template_id, card_biz_id, card_data)?
            .open_conversation_id(open_conversation_id))
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
        let card_data_json = card_data_json.into();
        Ok(Self {
            card_template_id: card_template_id.into().trim().to_string(),
            card_biz_id: card_biz_id.into().trim().to_string(),
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
    #[must_use]
    pub fn open_conversation_id(mut self, value: impl Into<String>) -> Self {
        self.open_conversation_id = Some(value.into().trim().to_string());
        self
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
        let user_id = non_empty_trimmed(&user_id, "user_id")?;
        self.single_chat_receiver = Some(serde_json::json!({ "userId": user_id }).to_string());
        Ok(self)
    }

    /// Sets the card callback URL for HTTP callback mode.
    ///
    /// Stream mode card callbacks are received on `/v1.0/card/instances/callback`.
    #[must_use]
    pub fn callback_url(mut self, value: impl Into<String>) -> Self {
        self.callback_url = Some(value.into().trim().to_string());
        self
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
    #[must_use]
    pub fn at_users<I, S>(mut self, user_ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.send_options = self.send_options.at_users(user_ids);
        self
    }

    /// Mentions everyone when sending this card.
    #[must_use]
    pub fn at_all(mut self) -> Self {
        self.send_options = self.send_options.at_all(true);
        self
    }

    /// Restricts card receivers with DingTalk's `receiverListJson` option.
    #[must_use]
    pub fn receiver_users<I, S>(mut self, user_ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.send_options = self.send_options.receiver_users(user_ids);
        self
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
        self.user_id_private_data_map_json = Some(normalize_json_object(
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
        self.union_id_private_data_map_json = Some(normalize_json_object(
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
        non_empty_trimmed(&self.card_template_id, "card_template_id")?;
        non_empty_trimmed(&self.card_biz_id, "card_biz_id")?;
        normalize_json_object_str("card_data", &self.card_data_json)?;

        match (
            self.open_conversation_id.as_deref(),
            self.single_chat_receiver.as_deref(),
        ) {
            (Some(open_conversation_id), None) => {
                non_empty_trimmed(open_conversation_id, "open_conversation_id")?;
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
            validate_http_url(callback_url, "callback_url")?;
        }
        if let Some(value) = &self.user_id_private_data_map_json {
            normalize_json_object_str("user_id_private_data_map", value)?;
        }
        if let Some(value) = &self.union_id_private_data_map_json {
            normalize_json_object_str("union_id_private_data_map", value)?;
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
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
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

impl InteractiveCardSendOptions {
    /// Creates empty send options.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets raw `atUserListJson`.
    #[must_use]
    pub fn at_user_list_json(mut self, value: impl Into<String>) -> Self {
        self.at_user_list_json = Some(value.into().trim().to_string());
        self
    }

    /// Mentions users when sending the card.
    #[must_use]
    pub fn at_users<I, S>(mut self, user_ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.at_user_list_json = Some(json_string_list(user_ids));
        self
    }

    /// Sets `atAll`.
    #[must_use]
    pub fn at_all(mut self, value: bool) -> Self {
        self.at_all = value;
        self
    }

    /// Sets raw `receiverListJson`.
    #[must_use]
    pub fn receiver_list_json(mut self, value: impl Into<String>) -> Self {
        self.receiver_list_json = Some(value.into().trim().to_string());
        self
    }

    /// Restricts receivers to the supplied user ids.
    #[must_use]
    pub fn receiver_users<I, S>(mut self, user_ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.receiver_list_json = Some(json_string_list(user_ids));
        self
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
            normalize_json_array_str("at_user_list_json", value)?;
        }
        if let Some(value) = &self.receiver_list_json {
            normalize_json_array_str("receiver_list_json", value)?;
        }
        if let Some(value) = &self.card_property_json {
            normalize_json_object_str("card_property", value)?;
        }
        Ok(())
    }
}

/// Standard interactive card update request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InteractiveCardUpdate {
    card_biz_id: String,
    card_data_json: Option<String>,
    user_id_private_data_map_json: Option<String>,
    union_id_private_data_map_json: Option<String>,
    update_options: InteractiveCardUpdateOptions,
}

impl InteractiveCardUpdate {
    /// Creates an update with replacement card data.
    pub fn card_data<T>(card_biz_id: impl Into<String>, card_data: T) -> Result<Self>
    where
        T: Serialize,
    {
        Ok(Self {
            card_biz_id: card_biz_id.into().trim().to_string(),
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
    #[must_use]
    pub fn private_data(card_biz_id: impl Into<String>) -> Self {
        Self {
            card_biz_id: card_biz_id.into().trim().to_string(),
            card_data_json: None,
            user_id_private_data_map_json: None,
            union_id_private_data_map_json: None,
            update_options: InteractiveCardUpdateOptions::new(),
        }
    }

    /// Creates an update from raw JSON object string for `cardData`.
    pub fn from_card_data_json(
        card_biz_id: impl Into<String>,
        card_data_json: impl Into<String>,
    ) -> Result<Self> {
        let card_data_json = card_data_json.into();
        Ok(Self {
            card_biz_id: card_biz_id.into().trim().to_string(),
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
        self.user_id_private_data_map_json = Some(normalize_json_object(
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
        self.union_id_private_data_map_json = Some(normalize_json_object(
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
        non_empty_trimmed(&self.card_biz_id, "card_biz_id")?;

        if let Some(value) = &self.card_data_json {
            normalize_json_object_str("card_data", value)?;
        }
        if let Some(value) = &self.user_id_private_data_map_json {
            normalize_json_object_str("user_id_private_data_map", value)?;
        }
        if let Some(value) = &self.union_id_private_data_map_json {
            normalize_json_object_str("union_id_private_data_map", value)?;
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RobotActionButton {
    title: String,
    url: String,
}

impl RobotActionButton {
    /// Creates an action-card button.
    #[must_use]
    pub fn new(title: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            title: title.into().trim().to_string(),
            url: url.into().trim().to_string(),
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
        non_empty_trimmed(&self.url, "button_url")?;
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RobotActionCard {
    title: String,
    text: String,
    buttons: Vec<RobotActionButton>,
    layout: RobotActionCardLayout,
}

impl RobotActionCard {
    /// Creates an action card without buttons.
    ///
    /// Add buttons with [`Self::button`]. DingTalk supports one, two, or three buttons.
    #[must_use]
    pub fn new(title: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            title: title.into().trim().to_string(),
            text: text.into().trim().to_string(),
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
        Ok(match (self.layout, self.buttons.len()) {
            (_, 1) => "sampleActionCard",
            (RobotActionCardLayout::Vertical, 2) => "sampleActionCard2",
            (RobotActionCardLayout::Vertical, 3) => "sampleActionCard3",
            (RobotActionCardLayout::Horizontal, 2) => "sampleActionCard6",
            _ => unreachable!("validated action-card layout"),
        })
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RobotVideo {
    video_media_id: String,
    duration_seconds: u64,
    video_type: Option<String>,
    pic_media_id: Option<String>,
    height: Option<u32>,
    width: Option<u32>,
}

impl RobotVideo {
    /// Creates a video message from an uploaded video media id.
    #[must_use]
    pub fn new(video_media_id: impl Into<String>, duration_seconds: u64) -> Self {
        Self {
            video_media_id: video_media_id.into().trim().to_string(),
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
        self.video_type = Some(value.into().trim().to_string());
        self
    }

    /// Sets the uploaded cover image media id.
    #[must_use]
    pub fn cover(mut self, pic_media_id: impl Into<String>) -> Self {
        self.pic_media_id = Some(pic_media_id.into().trim().to_string());
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
        non_empty_trimmed(&self.video_media_id, "video_media_id")?;
        if self.duration_seconds == 0 {
            return Err(Error::invalid_input(
                "duration_seconds",
                "duration must be greater than zero",
            ));
        }
        if let Some(video_type) = &self.video_type {
            non_empty_trimmed(video_type, "video_type")?;
        }
        if let Some(pic_media_id) = &self.pic_media_id {
            non_empty_trimmed(pic_media_id, "pic_media_id")?;
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
        /// DingTalk media id or image URL expected by DingTalk's `photoURL` field.
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
        let msg_key = non_empty_trimmed(&msg_key, "msg_key")?;
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
        let msg_key = non_empty_trimmed(&msg_key, "msg_key")?;
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
                non_empty_trimmed(msg_key, "msg_key")?;
                msg_key
            }
        })
    }

    /// Returns the JSON-encoded `msgParam` object expected by DingTalk.
    pub fn msg_param_json(&self) -> Result<String> {
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
            Self::Custom { msg_param_json, .. } => Ok(msg_param_json.clone()),
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
                non_empty_trimmed(message_url, "message_url")?;
                if let Some(pic_url) = pic_url {
                    non_empty_trimmed(pic_url, "pic_url")?;
                }
            }
            Self::Image { photo_url } => {
                non_empty_trimmed(photo_url, "photo_url")?;
            }
            Self::ActionCard { card } => {
                card.validate()?;
            }
            Self::Audio {
                media_id,
                duration_millis,
            } => {
                non_empty_trimmed(media_id, "media_id")?;
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
                non_empty_trimmed(media_id, "media_id")?;
                non_empty_trimmed(file_name, "file_name")?;
                non_empty_trimmed(file_type, "file_type")?;
            }
            Self::Video { video } => {
                video.validate()?;
            }
            Self::Custom {
                msg_key,
                msg_param_json,
            } => {
                non_empty_trimmed(msg_key, "msg_key")?;
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
    let file_name = non_empty_trimmed(file_name, "file_name")?;
    let (_stem, extension) = file_name
        .rsplit_once('.')
        .ok_or_else(|| Error::invalid_input("file_name", "file name must contain an extension"))?;
    non_empty_trimmed(extension, "file_type").map(|value| value.to_ascii_lowercase())
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

fn normalize_json_array_str(field: &'static str, value: &str) -> Result<String> {
    let value = non_empty_trimmed(value, field)?;
    let value = serde_json::from_str::<Value>(&value)
        .map_err(|source| Error::invalid_input(field, format!("invalid JSON: {source}")))?;
    if !value.is_array() {
        return Err(Error::invalid_input(field, "value must be a JSON array"));
    }
    Ok(serde_json::to_string(&value)?)
}

fn validate_http_url(value: &str, field: &'static str) -> Result<()> {
    let value = non_empty_trimmed(value, field)?;
    let url = url::Url::parse(&value)
        .map_err(|source| Error::invalid_input(field, format!("invalid URL: {source}")))?;
    if matches!(url.scheme(), "http" | "https") {
        Ok(())
    } else {
        Err(Error::invalid_input(
            field,
            "URL scheme must be http or https",
        ))
    }
}

fn normalize_user_ids<I, S>(user_ids: I) -> Result<Vec<String>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut values = Vec::new();
    for value in user_ids {
        let value = non_empty_trimmed(value.as_ref(), "user_id")?;
        if !values.contains(&value) {
            values.push(value);
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

fn json_string_list<I, S>(values: I) -> String
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut normalized = Vec::<String>::new();
    for value in values {
        let value = value.as_ref().trim();
        if !value.is_empty() && !normalized.iter().any(|existing| existing == value) {
            normalized.push(value.to_string());
        }
    }
    Value::Array(normalized.into_iter().map(Value::String).collect()).to_string()
}

fn media_upload_multipart_body(upload: &MediaUpload) -> Result<(String, Vec<u8>)> {
    upload.validate()?;

    let boundary = media_upload_boundary(upload);
    let mut body = Vec::new();

    multipart_text_field(&mut body, &boundary, "type", upload.media_type().as_str());
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

    for suffix in 0..100 {
        let boundary = format!(
            "----dingding-{media_type}-{}-{}-{suffix}",
            upload.file_name().len(),
            upload.bytes().len()
        );
        if !contains_subslice(upload.bytes(), boundary.as_bytes()) {
            return boundary;
        }
    }

    "----dingding-media-boundary".to_string()
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
    if let Some(code) = parsed.errcode
        && code != 0
    {
        return Err(api_error_from_body(
            code,
            parsed
                .errmsg
                .unwrap_or_else(|| "unknown dingtalk api error".to_string()),
            parsed.request_id,
            &body,
            error_body_snippet,
        ));
    }

    let raw = serde_json::from_str::<Value>(&body)?;
    let media_id = parsed
        .media_id
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| Error::invalid_input("media_id", "missing media_id in DingTalk response"))?;
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

fn parse_message_file_download_response(body: &str) -> Result<MessageFileDownload> {
    let raw = serde_json::from_str::<Value>(body)?;
    let payload = raw
        .get("result")
        .filter(|value| value.is_object())
        .unwrap_or(&raw);
    let download_url = payload
        .get("downloadUrl")
        .or_else(|| payload.get("download_url"))
        .or_else(|| payload.get("url"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            Error::invalid_input("download_url", "missing downloadUrl in DingTalk response")
        })?
        .to_string();

    Ok(MessageFileDownload { download_url, raw })
}

fn parse_interactive_card_response(body: &str) -> Result<InteractiveCardResponse> {
    let raw = serde_json::from_str::<Value>(body)?;
    let payload = raw
        .get("result")
        .filter(|value| value.is_object())
        .unwrap_or(&raw);
    let process_query_key = payload
        .get("processQueryKey")
        .or_else(|| payload.get("process_query_key"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            Error::invalid_input(
                "process_query_key",
                "missing processQueryKey in DingTalk response",
            )
        })?
        .to_string();

    Ok(InteractiveCardResponse {
        process_query_key,
        raw,
    })
}

fn is_false(value: &bool) -> bool {
    !*value
}

impl OpenApi {
    async fn post_raw_text<B>(&self, segments: &[&str], body: &B) -> Result<String>
    where
        B: Serialize + ?Sized,
    {
        let access_token = self.access_token().await?;
        let url = self.client.openapi_endpoint(segments)?;
        let response = self
            .client
            .transport()
            .post_openapi_json(&url, Some(&access_token), body)
            .await?;
        parse_standard_text_response(response, self.client.transport().error_body_snippet())
    }

    async fn put_raw_text<B>(&self, segments: &[&str], body: &B) -> Result<String>
    where
        B: Serialize + ?Sized,
    {
        let access_token = self.access_token().await?;
        let url = self.client.openapi_endpoint(segments)?;
        let response = self
            .client
            .transport()
            .put_openapi_json(&url, Some(&access_token), body)
            .await?;
        parse_standard_text_response(response, self.client.transport().error_body_snippet())
    }
}

#[derive(Debug, Deserialize)]
struct AccessTokenResponse {
    errcode: i64,
    #[serde(alias = "message")]
    errmsg: String,
    #[serde(alias = "accessToken")]
    access_token: Option<String>,
    #[serde(alias = "expiresIn", alias = "expireIn")]
    expires_in: Option<i64>,
    #[serde(default, alias = "requestId", alias = "RequestId")]
    request_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawMediaUploadResponse {
    errcode: Option<i64>,
    #[serde(alias = "message")]
    errmsg: Option<String>,
    #[serde(default, alias = "requestId", alias = "RequestId")]
    request_id: Option<String>,
    #[serde(alias = "mediaId")]
    media_id: Option<String>,
    #[serde(rename = "type")]
    media_type: Option<String>,
    #[serde(alias = "createdAt")]
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
    fn parses_message_file_download_response() {
        let download = parse_message_file_download_response(
            r#"{"errcode":0,"result":{"downloadUrl":"https://example.com/file.bin"}}"#,
        )
        .expect("download");

        assert_eq!(download.download_url(), "https://example.com/file.bin");
        assert_eq!(
            download.raw()["result"]["downloadUrl"],
            "https://example.com/file.bin"
        );
    }

    #[test]
    fn interactive_card_send_request_is_serialized() {
        let card = InteractiveCard::group(
            " open-cid ",
            " template-id ",
            " card-biz-id ",
            serde_json::json!({ "title": "Deploy", "status": "ok" }),
        )
        .expect("card")
        .callback_url(" https://example.com/card/callback ")
        .at_users([" user-1 ", "user-1", "user-2"])
        .receiver_users([" user-1 "])
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
    fn interactive_card_update_request_is_serialized() {
        let update = InteractiveCardUpdate::card_data(
            " card-biz-id ",
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
            " user-1 ",
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
        let direct =
            parse_interactive_card_response(r#"{"processQueryKey":"query-1"}"#).expect("direct");
        let wrapped = parse_interactive_card_response(
            r#"{"errcode":0,"errmsg":"ok","result":{"processQueryKey":"query-2"}}"#,
        )
        .expect("wrapped");

        assert_eq!(direct.process_query_key(), "query-1");
        assert_eq!(direct.raw()["processQueryKey"], "query-1");
        assert_eq!(wrapped.process_query_key(), "query-2");
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
                .validate()
                .expect_err("callback URL should require HTTP");

        assert_eq!(invalid_data.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(missing_target.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(invalid_update.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(invalid_private_user.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(
            invalid_private_receiver.kind(),
            crate::ErrorKind::InvalidInput
        );
        assert_eq!(invalid_callback_url.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn openapi_and_robot_helpers_can_be_reconfigured() {
        let client = DingTalk::builder().build().expect("client");
        let openapi = client
            .openapi()
            .with_credentials(AppCredentials::new("app-key", "app-secret"));
        let robot = openapi.robot(" robot-a ").with_robot_code(" robot-b ");

        assert_eq!(
            robot
                .openapi()
                .credentials()
                .map(|credentials| credentials.app_key()),
            Some("app-key")
        );
        assert_eq!(robot.robot_code(), "robot-b");
    }

    #[test]
    fn rejects_custom_message_without_object_param() {
        let error = RobotMessage::custom("sampleText", ["not", "an", "object"])
            .expect_err("array msgParam should fail");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn rejects_empty_custom_msg_key() {
        let error = RobotMessage::custom(" ", serde_json::json!({"content": "hello"}))
            .expect_err("empty msgKey should fail");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn rejects_empty_private_user_ids() {
        let error =
            normalize_user_ids::<[&str; 0], &str>([]).expect_err("empty user list should fail");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn private_user_ids_are_trimmed_and_deduplicated() {
        let values =
            normalize_user_ids([" user-1 ", "user-2", "user-1"]).expect("normalized user ids");

        assert_eq!(values, ["user-1".to_string(), "user-2".to_string()]);
    }
}
