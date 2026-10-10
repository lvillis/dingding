//! Sends one enterprise application robot message.
//!
//! Required environment:
//! - `DINGTALK_CLIENT_ID`
//! - `DINGTALK_CLIENT_SECRET`
//! - `DINGTALK_ROBOT_CODE`
//! - exactly one of `DINGTALK_OPEN_CONVERSATION_ID` or `DINGTALK_PRIVATE_USER_ID`
//!
//! Optional:
//! - `DINGTALK_ROBOT_MESSAGE_KIND=text|markdown|link|image|action_card|custom`

use std::env;

use dingding::{
    DingTalk, Error, Result,
    openapi::{RobotActionCard, RobotMessage, RobotReplyTarget},
};
use serde_json::Value;

const ROBOT_CODE_ENV: &str = "DINGTALK_ROBOT_CODE";
const OPEN_CONVERSATION_ID_ENV: &str = "DINGTALK_OPEN_CONVERSATION_ID";
const PRIVATE_USER_ID_ENV: &str = "DINGTALK_PRIVATE_USER_ID";
const MESSAGE_KIND_ENV: &str = "DINGTALK_ROBOT_MESSAGE_KIND";

const TEXT_ENV: &str = "DINGTALK_MESSAGE_TEXT";
const MARKDOWN_TITLE_ENV: &str = "DINGTALK_MARKDOWN_TITLE";
const MARKDOWN_TEXT_ENV: &str = "DINGTALK_MARKDOWN_TEXT";
const LINK_TITLE_ENV: &str = "DINGTALK_LINK_TITLE";
const LINK_TEXT_ENV: &str = "DINGTALK_LINK_TEXT";
const LINK_URL_ENV: &str = "DINGTALK_LINK_URL";
const LINK_PIC_URL_ENV: &str = "DINGTALK_LINK_PIC_URL";
const IMAGE_URL_ENV: &str = "DINGTALK_IMAGE_URL";
const ACTION_CARD_TITLE_ENV: &str = "DINGTALK_ACTION_CARD_TITLE";
const ACTION_CARD_TEXT_ENV: &str = "DINGTALK_ACTION_CARD_TEXT";
const ACTION_CARD_BUTTON_TITLE_ENV: &str = "DINGTALK_ACTION_CARD_BUTTON_TITLE";
const ACTION_CARD_BUTTON_URL_ENV: &str = "DINGTALK_ACTION_CARD_BUTTON_URL";
const CUSTOM_MSG_KEY_ENV: &str = "DINGTALK_CUSTOM_MSG_KEY";
const CUSTOM_MSG_PARAM_ENV: &str = "DINGTALK_CUSTOM_MSG_PARAM_JSON";

#[tokio::main]
async fn main() -> Result<()> {
    let ding = DingTalk::builder().app_credentials_from_env()?.build()?;
    let robot = ding.openapi().robot(required_env(ROBOT_CODE_ENV)?)?;
    let target = target()?;
    let message = message()?;
    let msg_key = message.msg_key()?.to_string();

    let response = target.send_message(&robot, message).await?;

    println!(
        "message sent: msg_key={msg_key} process_query_key={}",
        response.process_query_key()
    );
    Ok(())
}

fn message() -> Result<RobotMessage> {
    let kind = env_or(MESSAGE_KIND_ENV, "text");
    match kind.to_ascii_lowercase().as_str() {
        "text" => Ok(RobotMessage::text(env_or(
            TEXT_ENV,
            "hello from dingding enterprise robot",
        ))),
        "markdown" => Ok(RobotMessage::markdown(
            env_or(MARKDOWN_TITLE_ENV, "dingding"),
            env_or(MARKDOWN_TEXT_ENV, "### dingding\nhello from markdown"),
        )),
        "link" => link_message(),
        "image" => Ok(RobotMessage::image(required_env(IMAGE_URL_ENV)?)),
        "action_card" | "action-card" | "actioncard" => {
            Ok(RobotMessage::action_card(RobotActionCard::single(
                env_or(ACTION_CARD_TITLE_ENV, "dingding"),
                env_or(ACTION_CARD_TEXT_ENV, "### dingding\nopen the project page"),
                env_or(ACTION_CARD_BUTTON_TITLE_ENV, "Open"),
                env_or(
                    ACTION_CARD_BUTTON_URL_ENV,
                    "https://github.com/lvillis/dingding",
                ),
            )))
        }
        "custom" => custom_message(),
        other => Err(Error::InvalidConfig(format!(
            "`{MESSAGE_KIND_ENV}` must be text, markdown, link, image, action_card, or custom; got `{other}`"
        ))),
    }
}

fn link_message() -> Result<RobotMessage> {
    let title = env_or(LINK_TITLE_ENV, "dingding");
    let text = env_or(LINK_TEXT_ENV, "Rust SDK and bot framework for DingTalk");
    let message_url = env_or(LINK_URL_ENV, "https://github.com/lvillis/dingding");

    Ok(match optional_env(LINK_PIC_URL_ENV) {
        Some(pic_url) => RobotMessage::link_with_image(title, text, message_url, pic_url),
        None => RobotMessage::link(title, text, message_url),
    })
}

fn custom_message() -> Result<RobotMessage> {
    let msg_key = env_or(CUSTOM_MSG_KEY_ENV, "sampleText");
    let msg_param = optional_json_env(CUSTOM_MSG_PARAM_ENV)?.unwrap_or_else(|| {
        serde_json::json!({
            "content": env_or(TEXT_ENV, "hello from custom template")
        })
    });
    RobotMessage::custom(msg_key, msg_param)
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

fn required_env(name: &'static str) -> Result<String> {
    optional_env(name).ok_or_else(|| Error::InvalidConfig(format!("set `{name}` before running")))
}

fn env_or(name: &str, default: &str) -> String {
    optional_env(name).unwrap_or_else(|| default.to_string())
}

fn optional_env(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn optional_json_env(name: &str) -> Result<Option<Value>> {
    optional_env(name)
        .map(|value| serde_json::from_str::<Value>(&value).map_err(Error::from))
        .transpose()
}
