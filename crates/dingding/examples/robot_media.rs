//! Streams a local file to DingTalk, then sends it to a group or private recipient.
//!
//! Set app credentials, `DINGTALK_ROBOT_CODE`, `DINGTALK_MEDIA_PATH`, and
//! `DINGTALK_MEDIA_KIND=image|voice|video|file`. Choose exactly one of
//! `DINGTALK_OPEN_CONVERSATION_ID` or `DINGTALK_PRIVATE_USER_ID`.
//! `DINGTALK_MEDIA_MAX_BYTES` optionally limits the uploaded file size.

use std::{env, path::Path};

use dingding::{
    DingTalk, Error, Result,
    openapi::{MediaFileUpload, MediaType, RobotMessage, RobotReplyTarget, RobotVideo},
};

const ROBOT_CODE_ENV: &str = "DINGTALK_ROBOT_CODE";
const MEDIA_PATH_ENV: &str = "DINGTALK_MEDIA_PATH";
const MEDIA_KIND_ENV: &str = "DINGTALK_MEDIA_KIND";
const MEDIA_CONTENT_TYPE_ENV: &str = "DINGTALK_MEDIA_CONTENT_TYPE";
const MEDIA_MAX_BYTES_ENV: &str = "DINGTALK_MEDIA_MAX_BYTES";
const OPEN_CONVERSATION_ID_ENV: &str = "DINGTALK_OPEN_CONVERSATION_ID";
const PRIVATE_USER_ID_ENV: &str = "DINGTALK_PRIVATE_USER_ID";
const AUDIO_DURATION_MS_ENV: &str = "DINGTALK_AUDIO_DURATION_MS";
const VIDEO_DURATION_SECONDS_ENV: &str = "DINGTALK_VIDEO_DURATION_SECONDS";

#[tokio::main]
async fn main() -> Result<()> {
    let ding = DingTalk::builder().app_credentials_from_env()?.build()?;
    let robot = ding.openapi().robot(required_env(ROBOT_CODE_ENV)?)?;
    let target = target()?;
    let path = required_env(MEDIA_PATH_ENV)?;
    let media_type = media_type()?;
    let file_name = Path::new(&path)
        .file_name()
        .and_then(|value| value.to_str())
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            Error::InvalidConfig(format!("`{MEDIA_PATH_ENV}` must include a file name"))
        })?;
    let mut upload = MediaFileUpload::new(media_type.clone(), path);
    if let Some(content_type) = optional_env(MEDIA_CONTENT_TYPE_ENV) {
        upload = upload.content_type(content_type);
    }
    if let Some(max_bytes) = optional_u64_env(MEDIA_MAX_BYTES_ENV)? {
        upload = upload.max_bytes(max_bytes);
    }
    let media = robot.upload_media_file(upload).await?;
    let message = match media_type {
        MediaType::Image => RobotMessage::image(media.media_id()),
        MediaType::Voice => RobotMessage::audio(media.media_id(), audio_duration_ms()?),
        MediaType::Video => {
            RobotMessage::video(RobotVideo::new(media.media_id(), video_duration_seconds()?))
        }
        MediaType::File => RobotMessage::file_with_inferred_type(media.media_id(), file_name)?,
        MediaType::Other(value) => {
            return Err(Error::InvalidConfig(format!(
                "`{MEDIA_KIND_ENV}={value}` cannot be sent by this example"
            )));
        }
    };
    let response = target.send_message(&robot, message).await?;
    println!(
        "media sent: media_id={} process_query_key={}",
        media.media_id(),
        response.process_query_key()
    );
    Ok(())
}

fn media_type() -> Result<MediaType> {
    match required_env(MEDIA_KIND_ENV)?.to_ascii_lowercase().as_str() {
        "image" => Ok(MediaType::Image),
        "voice" | "audio" => Ok(MediaType::Voice),
        "video" => Ok(MediaType::Video),
        "file" => Ok(MediaType::File),
        other => Err(Error::InvalidConfig(format!(
            "`{MEDIA_KIND_ENV}` must be image, voice, video, or file; got `{other}`"
        ))),
    }
}

fn target() -> Result<RobotReplyTarget> {
    match (
        optional_env(OPEN_CONVERSATION_ID_ENV),
        optional_env(PRIVATE_USER_ID_ENV),
    ) {
        (Some(open_conversation_id), None) => RobotReplyTarget::group(open_conversation_id),
        (None, Some(user_id)) => RobotReplyTarget::private(user_id),
        (None, None) => Err(Error::InvalidConfig(format!(
            "set `{OPEN_CONVERSATION_ID_ENV}` or `{PRIVATE_USER_ID_ENV}` before running"
        ))),
        (Some(_), Some(_)) => Err(Error::InvalidConfig(format!(
            "set only one of `{OPEN_CONVERSATION_ID_ENV}` or `{PRIVATE_USER_ID_ENV}`"
        ))),
    }
}

fn audio_duration_ms() -> Result<u64> {
    optional_u64_env(AUDIO_DURATION_MS_ENV).map(|value| value.unwrap_or(1_000))
}

fn video_duration_seconds() -> Result<u64> {
    optional_u64_env(VIDEO_DURATION_SECONDS_ENV).map(|value| value.unwrap_or(1))
}

fn required_env(name: &'static str) -> Result<String> {
    optional_env(name).ok_or_else(|| Error::InvalidConfig(format!("set `{name}` before running")))
}

fn optional_env(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn optional_u64_env(name: &'static str) -> Result<Option<u64>> {
    optional_env(name)
        .map(|value| {
            value.parse::<u64>().map_err(|source| {
                Error::InvalidConfig(format!("`{name}` must be an unsigned integer: {source}"))
            })
        })
        .transpose()
}
