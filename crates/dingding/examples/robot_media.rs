use std::{env, fs, path::Path};

use dingding::{
    DingTalk, Error, Result,
    openapi::{MediaType, MediaUpload, RobotVideo},
};

const ROBOT_CODE_ENV: &str = "DINGTALK_ROBOT_CODE";
const MEDIA_PATH_ENV: &str = "DINGTALK_MEDIA_PATH";
const MEDIA_KIND_ENV: &str = "DINGTALK_MEDIA_KIND";
const MEDIA_CONTENT_TYPE_ENV: &str = "DINGTALK_MEDIA_CONTENT_TYPE";
const OPEN_CONVERSATION_ID_ENV: &str = "DINGTALK_OPEN_CONVERSATION_ID";
const PRIVATE_USER_ID_ENV: &str = "DINGTALK_PRIVATE_USER_ID";
const AUDIO_DURATION_MS_ENV: &str = "DINGTALK_AUDIO_DURATION_MS";
const VIDEO_DURATION_SECONDS_ENV: &str = "DINGTALK_VIDEO_DURATION_SECONDS";

#[tokio::main]
async fn main() -> Result<()> {
    let ding = DingTalk::builder().app_credentials_from_env()?.build()?;
    let robot = ding.openapi().robot(required_env(ROBOT_CODE_ENV)?);
    let target = target()?;
    let upload = media_upload()?;

    let media_type = upload.media_type().clone();
    let file_name = upload.file_name().to_string();
    let media = robot.upload_media(upload).await?;

    let process_query_key = match (target, media_type) {
        (Target::Group(open_conversation_id), MediaType::Image) => {
            robot
                .send_group_image(open_conversation_id, media.media_id())
                .await?
        }
        (Target::Private(user_id), MediaType::Image) => {
            robot.send_private_image(user_id, media.media_id()).await?
        }
        (Target::Group(open_conversation_id), MediaType::Voice) => {
            robot
                .send_group_audio(open_conversation_id, media.media_id(), audio_duration_ms()?)
                .await?
        }
        (Target::Private(user_id), MediaType::Voice) => {
            robot
                .send_private_audio(user_id, media.media_id(), audio_duration_ms()?)
                .await?
        }
        (Target::Group(open_conversation_id), MediaType::Video) => {
            robot
                .send_group_video(
                    open_conversation_id,
                    RobotVideo::new(media.media_id(), video_duration_seconds()?),
                )
                .await?
        }
        (Target::Private(user_id), MediaType::Video) => {
            robot
                .send_private_video(
                    user_id,
                    RobotVideo::new(media.media_id(), video_duration_seconds()?),
                )
                .await?
        }
        (Target::Group(open_conversation_id), MediaType::File) => {
            robot
                .send_group_file(
                    open_conversation_id,
                    media.media_id(),
                    &file_name,
                    file_type(&file_name)?,
                )
                .await?
        }
        (Target::Private(user_id), MediaType::File) => {
            robot
                .send_private_file(
                    user_id,
                    media.media_id(),
                    &file_name,
                    file_type(&file_name)?,
                )
                .await?
        }
        (_target, MediaType::Other(value)) => {
            return Err(Error::InvalidConfig(format!(
                "`{MEDIA_KIND_ENV}={value}` cannot be sent by this example"
            )));
        }
    };

    println!(
        "media sent: media_id={} process_query_key={}",
        media.media_id(),
        process_query_key
    );
    Ok(())
}

fn media_upload() -> Result<MediaUpload> {
    let path = required_env(MEDIA_PATH_ENV)?;
    let media_type = media_type()?;
    let file_name = Path::new(&path)
        .file_name()
        .and_then(|value| value.to_str())
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            Error::InvalidConfig(format!("`{MEDIA_PATH_ENV}` must include a file name"))
        })?;
    let bytes = fs::read(&path).map_err(|source| {
        Error::InvalidConfig(format!(
            "failed to read `{MEDIA_PATH_ENV}` at `{path}`: {source}"
        ))
    })?;

    let upload = MediaUpload::new(media_type, file_name, bytes);
    Ok(match optional_env(MEDIA_CONTENT_TYPE_ENV) {
        Some(content_type) => upload.content_type(content_type),
        None => upload,
    })
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

fn target() -> Result<Target> {
    match (
        optional_env(OPEN_CONVERSATION_ID_ENV),
        optional_env(PRIVATE_USER_ID_ENV),
    ) {
        (Some(open_conversation_id), None) => Ok(Target::Group(open_conversation_id)),
        (None, Some(user_id)) => Ok(Target::Private(user_id)),
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

fn file_type(file_name: &str) -> Result<String> {
    file_name
        .rsplit_once('.')
        .map(|(_stem, extension)| extension.trim().to_ascii_lowercase())
        .filter(|extension| !extension.is_empty())
        .ok_or_else(|| Error::InvalidConfig("file messages require a file extension".to_string()))
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

enum Target {
    Group(String),
    Private(String),
}
