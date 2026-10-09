use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    collections::{BTreeMap, HashSet},
    fmt,
};

use super::{RobotApi, validate_machine_identifier};
use crate::{Error, Result, transport::parse_openapi_response, util::redact::redact_text};

/// Pagination and identity for a group message read-status query.
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupMessageQuery {
    process_query_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_results: Option<u32>,
}

impl GroupMessageQuery {
    /// Queries the first page for a key returned by a group send operation.
    pub fn new(process_query_key: impl Into<String>) -> Result<Self> {
        let process_query_key = process_query_key.into();
        validate_machine_identifier(&process_query_key, "process_query_key")?;
        Ok(Self {
            process_query_key,
            next_token: None,
            max_results: None,
        })
    }

    /// Continues with the opaque cursor returned by the previous page.
    pub fn next_token(mut self, token: impl Into<String>) -> Result<Self> {
        let token = token.into();
        validate_machine_identifier(&token, "next_token")?;
        self.next_token = Some(token);
        Ok(self)
    }

    /// Sets a positive page size. DingTalk enforces the endpoint's maximum.
    pub fn max_results(mut self, value: u32) -> Result<Self> {
        if value == 0 {
            return Err(Error::invalid_input("max_results", "must be positive"));
        }
        self.max_results = Some(value);
        Ok(self)
    }
}

/// Lazy group-message reader pagination. Pages are not accumulated in memory.
///
/// Request errors leave the current query intact for an explicit retry. A repeated
/// cursor terminates pagination with an error instead of looping indefinitely.
pub struct GroupMessagePages {
    robot: RobotApi,
    conversation_id: String,
    query: Option<GroupMessageQuery>,
    seen_cursors: HashSet<String>,
}

impl fmt::Debug for GroupMessagePages {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GroupMessagePages")
            .field("finished", &self.query.is_none())
            .finish_non_exhaustive()
    }
}

impl GroupMessagePages {
    /// Requests the next page, or returns `None` once pagination has finished.
    ///
    /// Dropping this future before it completes does not advance the cursor.
    pub async fn next_page(&mut self) -> Result<Option<GroupMessageStatus>> {
        let Some(query) = self.query.as_ref() else {
            return Ok(None);
        };
        let page = self
            .robot
            .query_group_message(&self.conversation_id, query.clone())
            .await?;
        if page.has_more {
            let cursor = page.next_token.as_deref().ok_or_else(|| {
                Error::api_with_code(-1, None, "missing next-page cursor", None, None)
            })?;
            let next = query.clone().next_token(cursor)?;
            if !self.seen_cursors.insert(cursor.to_owned()) {
                self.query = None;
                return Err(Error::api_with_code(
                    -1,
                    None,
                    "repeated next-page cursor",
                    None,
                    None,
                ));
            }
            self.query = Some(next);
        } else {
            self.query = None;
        }
        Ok(Some(page))
    }
}

/// Group send status and one page of readers. Status strings retain future platform values.
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupMessageStatus {
    /// Platform send status, for example `SUCCESS`.
    #[serde(deserialize_with = "deserialize_status")]
    pub send_status: String,
    /// Whether another reader page is available.
    #[serde(default)]
    pub has_more: bool,
    /// Opaque cursor for the next reader page.
    pub next_token: Option<String>,
    /// Reader user ids returned by older response variants.
    #[serde(default)]
    pub read_user_ids: Vec<String>,
    /// Reader identities returned by newer response variants.
    #[serde(default)]
    pub read_users: Vec<GroupMessageReader>,
}

/// A user who read a group message.
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupMessageReader {
    /// Enterprise user id, when supplied.
    pub user_id: Option<String>,
    /// Union id, when supplied.
    pub union_id: Option<String>,
}

/// Send and read status for a message sent to one or more users.
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrivateMessageStatus {
    /// Platform send status, preserved without normalizing its spelling.
    #[serde(deserialize_with = "deserialize_status")]
    pub send_status: String,
    /// Per-user read information.
    #[serde(default)]
    pub message_read_info_list: Vec<MessageReadInfo>,
}

/// Read information for one private message recipient.
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageReadInfo {
    /// Recipient user id.
    pub user_id: String,
    /// Recipient display name, when supplied.
    pub name: Option<String>,
    /// Platform read status, for example `READ`.
    pub read_status: String,
    /// Read timestamp in milliseconds, when supplied.
    pub read_timestamp: Option<i64>,
}

/// Per-message results of a recall request. HTTP success can contain partial failures.
#[derive(Clone, Deserialize)]
#[serde(try_from = "RecallPayload")]
pub struct MessageRecallResponse {
    /// Successfully recalled process query keys.
    pub success_result: Vec<String>,
    /// Failed process query keys mapped to platform error details.
    pub failed_result: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RecallPayload {
    success_result: Option<Vec<String>>,
    failed_result: Option<BTreeMap<String, String>>,
}

impl TryFrom<RecallPayload> for MessageRecallResponse {
    type Error = &'static str;
    fn try_from(value: RecallPayload) -> std::result::Result<Self, Self::Error> {
        if value.success_result.is_none() && value.failed_result.is_none() {
            return Err("missing recall results");
        }
        Ok(Self {
            success_result: value.success_result.unwrap_or_default(),
            failed_result: value.failed_result.unwrap_or_default(),
        })
    }
}

fn deserialize_status<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<String, D::Error> {
    let status = String::deserialize(deserializer)?;
    if status.trim().is_empty() {
        return Err(serde::de::Error::custom("missing message status"));
    }
    Ok(status)
}

impl MessageRecallResponse {
    /// Returns true when the response reports at least one success and no failures.
    #[must_use]
    pub fn is_success(&self) -> bool {
        !self.success_result.is_empty() && self.failed_result.is_empty()
    }
}

impl fmt::Debug for GroupMessageQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GroupMessageQuery")
            .field("max_results", &self.max_results)
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for GroupMessageStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GroupMessageStatus")
            .field("send_status", &redact_text(&self.send_status))
            .field("has_more", &self.has_more)
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for GroupMessageReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GroupMessageReader").finish_non_exhaustive()
    }
}
impl fmt::Debug for PrivateMessageStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PrivateMessageStatus")
            .field("send_status", &redact_text(&self.send_status))
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for MessageReadInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MessageReadInfo")
            .field("read_status", &redact_text(&self.read_status))
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for MessageRecallResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MessageRecallResponse")
            .field("success_count", &self.success_result.len())
            .field("failure_count", &self.failed_result.len())
            .finish()
    }
}

impl RobotApi {
    /// Creates a lazy paginator without issuing a request.
    ///
    /// ```no_run
    /// # use dingding::{Result, openapi::{RobotApi, GroupMessageQuery}};
    /// # async fn query(robot: &RobotApi) -> Result<()> {
    /// let mut pages = robot.query_group_message_pages(
    ///     "conversation-id", GroupMessageQuery::new("process-query-key")?.max_results(50)?,
    /// )?;
    /// while let Some(page) = pages.next_page().await? {
    ///     for reader in page.read_users { /* process reader */ }
    /// }
    /// # Ok(()) }
    /// ```
    pub fn query_group_message_pages(
        &self,
        open_conversation_id: impl Into<String>,
        query: GroupMessageQuery,
    ) -> Result<GroupMessagePages> {
        let conversation_id = open_conversation_id.into();
        validate_machine_identifier(&conversation_id, "open_conversation_id")?;
        let seen_cursors = query.next_token.iter().cloned().collect();
        Ok(GroupMessagePages {
            robot: self.clone(),
            conversation_id,
            query: Some(query),
            seen_cursors,
        })
    }

    /// Queries a page of group message readers and the platform send status.
    ///
    /// ```no_run
    /// # use dingding::{Result, openapi::{RobotApi, GroupMessageQuery}};
    /// # async fn query(robot: &RobotApi, key: &str) -> Result<()> {
    /// let mut query = GroupMessageQuery::new(key)?.max_results(50)?;
    /// loop {
    ///     let page = robot.query_group_message("conversation-id", query).await?;
    ///     if !page.has_more { break; }
    ///     if let Some(cursor) = page.next_token {
    ///         query = GroupMessageQuery::new(key)?.max_results(50)?.next_token(cursor)?;
    ///     } else { break; }
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn query_group_message(
        &self,
        open_conversation_id: impl AsRef<str>,
        query: GroupMessageQuery,
    ) -> Result<GroupMessageStatus> {
        validate_machine_identifier(open_conversation_id.as_ref(), "open_conversation_id")?;
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Request<'a> {
            robot_code: &'a str,
            open_conversation_id: &'a str,
            #[serde(flatten)]
            query: GroupMessageQuery,
        }
        let request = Request {
            robot_code: &self.robot_code,
            open_conversation_id: open_conversation_id.as_ref(),
            query,
        };
        let response: GroupMessageStatus = self
            .post_lifecycle(&["v1.0", "robot", "groupMessages", "query"], &request)
            .await?;
        if response.send_status.trim().is_empty()
            || (response.has_more
                && response
                    .next_token
                    .as_deref()
                    .is_none_or(|s| s.trim().is_empty()))
        {
            return Err(Error::api_with_code(
                -1,
                None,
                "group status or next-page cursor is missing",
                None,
                None,
            ));
        }
        Ok(response)
    }

    /// Queries send/read status for a message returned by a private send operation.
    pub async fn query_private_message(
        &self,
        process_query_key: impl AsRef<str>,
    ) -> Result<PrivateMessageStatus> {
        validate_machine_identifier(process_query_key.as_ref(), "process_query_key")?;
        let mut url = self.openapi.client.openapi_endpoint(&[
            "v1.0",
            "robot",
            "oToMessages",
            "readStatus",
        ])?;
        url.query_pairs_mut()
            .append_pair("robotCode", &self.robot_code)
            .append_pair("processQueryKey", process_query_key.as_ref());
        let url = &url;
        self.openapi
            .with_access_token(|token| async move {
                parse_openapi_response(
                    self.openapi
                        .client
                        .transport()
                        .get_openapi(url, &token)
                        .await?,
                    self.openapi.client.transport().error_body_snippet(),
                )
            })
            .await
    }

    /// Recalls messages sent by this enterprise robot to an internal group.
    /// Inspect both success and failure collections in the returned batch result.
    pub async fn recall_group_messages<I, S>(
        &self,
        open_conversation_id: impl AsRef<str>,
        process_query_keys: I,
    ) -> Result<MessageRecallResponse>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        validate_machine_identifier(open_conversation_id.as_ref(), "open_conversation_id")?;
        let request = RecallRequest {
            robot_code: &self.robot_code,
            open_conversation_id: Some(open_conversation_id.as_ref()),
            process_query_keys: normalize_query_keys(process_query_keys)?,
        };
        self.post_lifecycle(&["v1.0", "robot", "groupMessages", "recall"], &request)
            .await
    }

    /// Recalls messages sent in person-to-robot private conversations.
    /// Inspect both success and failure collections in the returned batch result.
    pub async fn recall_private_messages<I, S>(
        &self,
        process_query_keys: I,
    ) -> Result<MessageRecallResponse>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let request = RecallRequest {
            robot_code: &self.robot_code,
            open_conversation_id: None,
            process_query_keys: normalize_query_keys(process_query_keys)?,
        };
        self.post_lifecycle(&["v1.0", "robot", "otoMessages", "batchRecall"], &request)
            .await
    }

    async fn post_lifecycle<T: DeserializeOwned, B: Serialize>(
        &self,
        segments: &[&str],
        body: &B,
    ) -> Result<T> {
        validate_machine_identifier(&self.robot_code, "robot_code")?;
        let url = self.openapi.client.openapi_endpoint(segments)?;
        let url = &url;
        self.openapi
            .with_access_token(|token| async move {
                parse_openapi_response(
                    self.openapi
                        .client
                        .transport()
                        .post_openapi_json(url, Some(&token), body)
                        .await?,
                    self.openapi.client.transport().error_body_snippet(),
                )
            })
            .await
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RecallRequest<'a> {
    robot_code: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    open_conversation_id: Option<&'a str>,
    process_query_keys: Vec<String>,
}

fn normalize_query_keys<I, S>(keys: I) -> Result<Vec<String>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut result = Vec::new();
    for key in keys {
        validate_machine_identifier(key.as_ref(), "process_query_key")?;
        if !result.iter().any(|existing| existing == key.as_ref()) {
            result.push(key.as_ref().to_owned());
        }
    }
    if result.is_empty() {
        return Err(Error::invalid_input(
            "process_query_keys",
            "must not be empty",
        ));
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn recall_results_accept_omitted_empty_collections_but_not_empty_envelopes() {
        let success: MessageRecallResponse =
            serde_json::from_value(json!({"successResult":["pq"]})).expect("success");
        assert!(success.is_success());
        let failure: MessageRecallResponse =
            serde_json::from_value(json!({"failedResult":{"pq":"not allowed"}})).expect("failure");
        assert!(!failure.is_success());
        assert!(serde_json::from_value::<MessageRecallResponse>(json!({})).is_err());
        assert!(
            serde_json::from_value::<MessageRecallResponse>(json!({"successResult":"pq"})).is_err()
        );
    }

    #[test]
    fn statuses_preserve_unknown_values_and_reject_missing_values() {
        let status: PrivateMessageStatus =
            serde_json::from_value(json!({"sendStatus":"FUTURE_STATUS"})).expect("status");
        assert_eq!(status.send_status, "FUTURE_STATUS");
        assert!(serde_json::from_value::<PrivateMessageStatus>(json!({"sendStatus":" "})).is_err());
        assert!(serde_json::from_value::<GroupMessageStatus>(json!({"hasMore":false})).is_err());
        assert!(GroupMessageQuery::new(" ").is_err());
        assert!(
            GroupMessageQuery::new("pq")
                .expect("query")
                .max_results(0)
                .is_err()
        );
        assert!(normalize_query_keys(Vec::<String>::new()).is_err());
        assert!(normalize_query_keys(["valid", " "]).is_err());
    }
}
