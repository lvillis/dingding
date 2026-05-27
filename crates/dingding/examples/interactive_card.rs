use std::env;

use dingding::{
    DingTalk, Error, Result,
    openapi::{InteractiveCard, InteractiveCardUpdate},
};
use serde_json::Value;

const ROBOT_CODE_ENV: &str = "DINGTALK_ROBOT_CODE";
const CARD_TEMPLATE_ID_ENV: &str = "DINGTALK_CARD_TEMPLATE_ID";
const CARD_BIZ_ID_ENV: &str = "DINGTALK_CARD_BIZ_ID";
const CARD_DATA_ENV: &str = "DINGTALK_CARD_DATA_JSON";
const OPEN_CONVERSATION_ID_ENV: &str = "DINGTALK_OPEN_CONVERSATION_ID";
const SINGLE_CHAT_RECEIVER_ENV: &str = "DINGTALK_SINGLE_CHAT_RECEIVER";
const SINGLE_CHAT_USER_ID_ENV: &str = "DINGTALK_SINGLE_CHAT_USER_ID";
const CALLBACK_URL_ENV: &str = "DINGTALK_CARD_CALLBACK_URL";
const UPDATE_CARD_DATA_ENV: &str = "DINGTALK_CARD_UPDATE_DATA_JSON";

#[tokio::main]
async fn main() -> Result<()> {
    let ding = DingTalk::builder().app_credentials_from_env()?.build()?;
    let robot = ding.openapi().robot(required_env(ROBOT_CODE_ENV)?);

    let card = build_card()?;
    let response = robot.send_interactive_card(card).await?;
    println!(
        "interactive card sent: process_query_key={}",
        response.process_query_key()
    );

    if let Some(update_data) = optional_json_env(UPDATE_CARD_DATA_ENV)? {
        let update = InteractiveCardUpdate::card_data(required_env(CARD_BIZ_ID_ENV)?, update_data)?
            .update_card_data_by_key(true);
        let response = robot.update_interactive_card(update).await?;
        println!(
            "interactive card updated: process_query_key={}",
            response.process_query_key()
        );
    }

    Ok(())
}

fn build_card() -> Result<InteractiveCard> {
    let card_template_id = required_env(CARD_TEMPLATE_ID_ENV)?;
    let card_biz_id = required_env(CARD_BIZ_ID_ENV)?;
    let card_data = optional_json_env(CARD_DATA_ENV)?
        .unwrap_or_else(|| serde_json::json!({ "title": "dingding", "text": "hello" }));

    let mut card = match (
        optional_env(OPEN_CONVERSATION_ID_ENV),
        optional_env(SINGLE_CHAT_RECEIVER_ENV),
        optional_env(SINGLE_CHAT_USER_ID_ENV),
    ) {
        (Some(open_conversation_id), None, None) => InteractiveCard::group(
            open_conversation_id,
            card_template_id,
            card_biz_id,
            card_data,
        )?,
        (None, Some(single_chat_receiver), None) => InteractiveCard::private_receiver(
            single_chat_receiver,
            card_template_id,
            card_biz_id,
            card_data,
        )?,
        (None, None, Some(user_id)) => {
            InteractiveCard::private_user(user_id, card_template_id, card_biz_id, card_data)?
        }
        (None, None, None) => {
            return Err(Error::InvalidConfig(format!(
                "set `{OPEN_CONVERSATION_ID_ENV}`, `{SINGLE_CHAT_RECEIVER_ENV}`, or `{SINGLE_CHAT_USER_ID_ENV}` before running"
            )));
        }
        _ => {
            return Err(Error::InvalidConfig(format!(
                "set only one of `{OPEN_CONVERSATION_ID_ENV}`, `{SINGLE_CHAT_RECEIVER_ENV}`, or `{SINGLE_CHAT_USER_ID_ENV}`"
            )));
        }
    };

    if let Some(callback_url) = optional_env(CALLBACK_URL_ENV) {
        card = card.callback_url(callback_url);
    }

    Ok(card)
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

fn optional_json_env(name: &str) -> Result<Option<Value>> {
    optional_env(name)
        .map(|value| serde_json::from_str::<Value>(&value).map_err(Error::from))
        .transpose()
}
