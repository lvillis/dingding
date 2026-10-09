use std::{
    fmt,
    io::Cursor,
    path::{Path, PathBuf},
};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWrite},
};

use super::{
    MediaType, OpenApi, RobotApi, UploadedMedia, multipart_quote, multipart_text_field,
    parse_http_endpoint_url, parse_media_upload_response, validate_file_name,
    validate_header_value, validate_machine_identifier,
};
use crate::{Error, Result, util::redact::redact_text};

/// Streaming upload from a local file. No full-file buffer is allocated.
///
/// Token-rejection recovery reopens the file for a fresh attempt. Keep its
/// contents unchanged until the upload completes. HTTP retries cannot replay the
/// streaming request body.
#[derive(Clone)]
pub struct MediaFileUpload {
    media_type: MediaType,
    path: PathBuf,
    file_name: Option<String>,
    content_type: Option<String>,
    max_bytes: Option<u64>,
}

impl fmt::Debug for MediaFileUpload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MediaFileUpload")
            .field("media_type", &self.media_type)
            .field("max_bytes", &self.max_bytes)
            .finish_non_exhaustive()
    }
}

impl MediaFileUpload {
    /// Uses the path's final component as the uploaded filename unless overridden.
    #[must_use]
    pub fn new(media_type: MediaType, path: impl Into<PathBuf>) -> Self {
        Self {
            media_type,
            path: path.into(),
            file_name: None,
            content_type: None,
            max_bytes: None,
        }
    }

    /// Overrides the uploaded filename without changing the local path.
    #[must_use]
    pub fn file_name(mut self, value: impl Into<String>) -> Self {
        self.file_name = Some(value.into());
        self
    }

    /// Sets the MIME type of the file part.
    #[must_use]
    pub fn content_type(mut self, value: impl Into<String>) -> Self {
        self.content_type = Some(value.into());
        self
    }

    /// Rejects files exceeding this positive size before uploading.
    #[must_use]
    pub fn max_bytes(mut self, value: u64) -> Self {
        self.max_bytes = Some(value);
        self
    }

    /// Returns the local source path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn validated_file_name(&self) -> Result<&str> {
        validate_machine_identifier(self.media_type.as_str(), "media_type")?;
        let name = self
            .file_name
            .as_deref()
            .or_else(|| self.path.file_name()?.to_str())
            .ok_or_else(|| Error::invalid_input("file_name", "provide a UTF-8 filename"))?;
        validate_file_name(name, "file_name")?;
        if let Some(content_type) = &self.content_type {
            validate_header_value(content_type, "content_type")?;
        }
        if self.max_bytes == Some(0) {
            return Err(Error::invalid_input("max_bytes", "must be positive"));
        }
        Ok(name)
    }

    async fn open_file(&self) -> Result<(File, u64)> {
        if !tokio::fs::metadata(&self.path).await?.is_file() {
            return Err(Error::invalid_input(
                "media",
                "source must be a regular file",
            ));
        }
        let file = File::open(&self.path).await?;
        let metadata = file.metadata().await?;
        if !metadata.is_file() || metadata.len() == 0 {
            return Err(Error::invalid_input(
                "media",
                "source must be a nonempty regular file",
            ));
        }
        if self.max_bytes.is_some_and(|max| metadata.len() > max) {
            return Err(Error::invalid_input(
                "max_bytes",
                "media file exceeds upload limit",
            ));
        }
        Ok((file, metadata.len()))
    }
}

/// Metadata from a completed streaming download; contains no file-body buffer.
#[derive(Clone, PartialEq, Eq)]
pub struct DownloadedFileInfo {
    download_url: String,
    content_type: Option<String>,
    bytes_written: u64,
}

impl fmt::Debug for DownloadedFileInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DownloadedFileInfo")
            .field("download_url", &redact_text(&self.download_url))
            .field("content_type", &self.content_type)
            .field("bytes_written", &self.bytes_written)
            .finish()
    }
}

impl DownloadedFileInfo {
    /// Returns the temporary download URL, which may contain credentials.
    #[must_use]
    pub fn download_url(&self) -> &str {
        &self.download_url
    }
    /// Returns the response MIME type, when supplied.
    #[must_use]
    pub fn content_type(&self) -> Option<&str> {
        self.content_type.as_deref()
    }
    /// Returns the number of successfully written bytes.
    #[must_use]
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

impl OpenApi {
    /// Uploads a file with bounded memory and token-rejection recovery.
    ///
    /// Local I/O failures use [`crate::ErrorKind::Io`]. File contents must remain
    /// unchanged while this operation runs; do not upload a file still being written.
    ///
    /// ```no_run
    /// # use dingding::{Result, openapi::{OpenApi, MediaFileUpload, MediaType}};
    /// # async fn upload(api: &OpenApi) -> Result<()> {
    /// let media = api.upload_media_file(
    ///     MediaFileUpload::new(MediaType::File, "report.pdf")
    ///         .content_type("application/pdf").max_bytes(20 * 1024 * 1024),
    /// ).await?;
    /// let _id = media.media_id();
    /// # Ok(()) }
    /// ```
    pub async fn upload_media_file(&self, upload: MediaFileUpload) -> Result<UploadedMedia> {
        let file_name = upload.validated_file_name()?;
        // Validate local access and size before fetching credentials or sending requests.
        let (_, expected_length) = upload.open_file().await?;
        let upload = &upload;
        self.with_access_token(|token| async move {
            let (file, length) = upload.open_file().await?;
            if length != expected_length {
                return Err(Error::invalid_input("media", "file size changed during upload"));
            }
            let mut random = [0u8; 24];
            getrandom::fill(&mut random).map_err(std::io::Error::other)?;
            let boundary = format!("----dingding-{}", URL_SAFE_NO_PAD.encode(random));
            let mut prefix = Vec::new();
            multipart_text_field(&mut prefix, &boundary, "type", upload.media_type.as_str());
            prefix.extend_from_slice(format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"media\"; filename=\"{}\"\r\nContent-Type: {}\r\n\r\n",
                multipart_quote(file_name), upload.content_type.as_deref().unwrap_or("application/octet-stream")
            ).as_bytes());
            let suffix = format!("\r\n--{boundary}--\r\n").into_bytes();
            let content_length = length.checked_add(prefix.len() as u64)
                .and_then(|length| length.checked_add(suffix.len() as u64))
                .ok_or_else(|| Error::invalid_input("media", "multipart body is too large"))?;
            let body = Cursor::new(prefix).chain(file.take(length)).chain(Cursor::new(suffix));
            let mut url = self.client.webhook_endpoint(&["media", "upload"])?;
            url.query_pairs_mut().append_pair("access_token", &token)
                .append_pair("type", upload.media_type.as_str());
            let response = self.client.transport().post_webhook_reader(
                &url, &format!("multipart/form-data; boundary={boundary}"), body, content_length,
            ).await?;
            parse_media_upload_response(response, self.client.transport().error_body_snippet(), &upload.media_type)
        }).await
    }
}

impl RobotApi {
    /// Streams a local file using this robot's OpenAPI credentials.
    pub async fn upload_media_file(&self, upload: MediaFileUpload) -> Result<UploadedMedia> {
        self.openapi.upload_media_file(upload).await
    }

    /// Streams a received file to an async writer with a positive byte limit.
    ///
    /// Memory is bounded independently of file size. HTTP errors and DingTalk
    /// error envelopes fitting in the first 64 KiB are rejected before writing.
    /// Requests identity content encoding and rejects unexpected encoded responses.
    /// On failure or cancellation, the writer may contain partial data; use a
    /// temporary file and rename only after success. The writer is flushed but not
    /// closed. A total timeout, when configured on the client, also covers writes.
    ///
    /// ```no_run
    /// # use dingding::{Result, openapi::RobotApi};
    /// # async fn download(robot: &RobotApi, code: &str) -> Result<()> {
    /// let mut file = tokio::fs::OpenOptions::new().create_new(true).write(true)
    ///     .open("message.part").await?;
    /// let info = robot.download_message_file_to(code, &mut file, 20 * 1024 * 1024).await?;
    /// let _written = info.bytes_written();
    /// // Close and rename the temporary file after success; clean it up on error.
    /// # Ok(()) }
    /// ```
    pub async fn download_message_file_to<W>(
        &self,
        download_code: impl Into<String>,
        writer: &mut W,
        max_bytes: usize,
    ) -> Result<DownloadedFileInfo>
    where
        W: AsyncWrite + Unpin + Send + ?Sized,
    {
        if max_bytes == 0 {
            return Err(Error::invalid_input("max_bytes", "must be positive"));
        }
        let download = self.message_file_download_url(download_code).await?;
        let url = parse_http_endpoint_url(download.download_url(), "download_url")?;
        let (content_type, bytes_written) = self
            .openapi
            .client
            .transport()
            .download_to_writer(&url, writer, max_bytes)
            .await?;
        Ok(DownloadedFileInfo {
            download_url: download.into_download_url(),
            content_type,
            bytes_written,
        })
    }
}
