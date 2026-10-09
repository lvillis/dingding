use std::fmt;

use serde::{Deserialize, Serialize};

use super::{RobotApi, RobotMessage, RobotMessageResponse, validate_machine_identifier};
use crate::Result;

/// Serializable destination for an explicitly chosen proactive robot message.
///
/// Contains no session webhook or credentials. Store it with a background job and
/// supply the correct application's `RobotApi` when sending. Recipient identifiers
/// are sensitive; protect persisted values even though `Debug` redacts them.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "scope", rename_all = "snake_case")]
#[non_exhaustive]
pub enum RobotReplyTarget {
    /// Reply to the group, not privately to its sender.
    Group {
        /// DingTalk open conversation id.
        open_conversation_id: String,
    },
    /// Reply to the enterprise staff user in a private conversation.
    Private {
        /// Enterprise staff id, not the event's encrypted sender id.
        user_id: String,
    },
}

impl fmt::Debug for RobotReplyTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Group { .. } => f.debug_struct("Group").finish_non_exhaustive(),
            Self::Private { .. } => f.debug_struct("Private").finish_non_exhaustive(),
        }
    }
}

impl RobotReplyTarget {
    /// Creates a validated group destination.
    pub fn group(open_conversation_id: impl Into<String>) -> Result<Self> {
        let open_conversation_id = open_conversation_id.into();
        validate_machine_identifier(&open_conversation_id, "open_conversation_id")?;
        Ok(Self::Group {
            open_conversation_id,
        })
    }

    /// Creates a validated private destination from an enterprise staff id.
    pub fn private(user_id: impl Into<String>) -> Result<Self> {
        let user_id = user_id.into();
        validate_machine_identifier(&user_id, "user_id")?;
        Ok(Self::Private { user_id })
    }

    /// Sends through OpenAPI regardless of session webhook availability.
    ///
    /// Requires proactive-message permissions. No webhook fallback or automatic
    /// retry after ambiguous delivery is performed. Deserialized targets are
    /// validated by the send API before any request is issued.
    pub async fn send_message(
        &self,
        robot: &RobotApi,
        message: RobotMessage,
    ) -> Result<RobotMessageResponse> {
        match self {
            Self::Group {
                open_conversation_id,
            } => {
                robot
                    .send_group_message(open_conversation_id, message)
                    .await
            }
            Self::Private { user_id } => robot.send_private_message([user_id], message).await,
        }
    }

    /// Sends text through the explicitly selected application's robot.
    pub async fn send_text(
        &self,
        robot: &RobotApi,
        content: impl Into<String>,
    ) -> Result<RobotMessageResponse> {
        self.send_message(robot, RobotMessage::text(content)).await
    }

    /// Sends markdown through the explicitly selected application's robot.
    pub async fn send_markdown(
        &self,
        robot: &RobotApi,
        title: impl Into<String>,
        text: impl Into<String>,
    ) -> Result<RobotMessageResponse> {
        self.send_message(robot, RobotMessage::markdown(title, text))
            .await
    }
}
