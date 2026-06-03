use std::env;

use dingding::{
    DingTalk, Error, Result,
    webhook::{At, WebhookResponse},
};

const ACCESS_TOKEN_ENV: &str = "DINGTALK_WEBHOOK_ACCESS_TOKEN";
const SECRET_ENV: &str = "DINGTALK_WEBHOOK_SECRET";
const MESSAGE_ENV: &str = "DINGTALK_WEBHOOK_MESSAGE";
const MARKDOWN_TITLE_ENV: &str = "DINGTALK_WEBHOOK_MARKDOWN_TITLE";
const AT_USER_IDS_ENV: &str = "DINGTALK_AT_USER_IDS";
const AT_MOBILES_ENV: &str = "DINGTALK_AT_MOBILES";
const AT_ALL_ENV: &str = "DINGTALK_AT_ALL";

#[tokio::main]
async fn main() -> Result<()> {
    let access_token = required_env(ACCESS_TOKEN_ENV)?;
    let message = optional_env(MESSAGE_ENV)
        .unwrap_or_else(|| "hello from dingding webhook robot".to_string());

    let ding = DingTalk::new()?;
    let mut webhook = ding.webhook(access_token)?;
    if let Some(secret) = optional_env(SECRET_ENV) {
        webhook = webhook.signing_secret(secret)?;
    }

    let response = if let Some(title) = optional_env(MARKDOWN_TITLE_ENV) {
        webhook.send_markdown(title, message).await?
    } else {
        let at = mention_metadata();
        if at.is_empty() {
            webhook.send_text(message).await?
        } else {
            webhook.send_text_with_at(message, at).await?
        }
    };

    print_response(&response);
    Ok(())
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

fn mention_metadata() -> At {
    let mut at = At::new()
        .user_ids(csv_env(AT_USER_IDS_ENV))
        .mobiles(csv_env(AT_MOBILES_ENV));

    if bool_env(AT_ALL_ENV) {
        at = at.all_users();
    }

    at
}

fn csv_env(name: &str) -> Vec<String> {
    optional_env(name)
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(ToOwned::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn bool_env(name: &str) -> bool {
    optional_env(name)
        .map(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "y"
            )
        })
        .unwrap_or(false)
}

fn print_response(response: &WebhookResponse) {
    println!(
        "sent: code={} message={} request_id={:?}",
        response.code(),
        response.message(),
        response.request_id()
    );
}
