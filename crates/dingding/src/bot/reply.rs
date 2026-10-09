use super::{BotContext, BotEvent, ConversationScope};
use crate::{
    Error, Result,
    openapi::{RobotApi, RobotMessage, RobotMessageResponse, RobotReplyTarget},
};

impl BotEvent {
    /// Extracts a destination for a proactive reply or a persisted background job.
    ///
    /// Group events require an open conversation id; private events require a
    /// sender staff id. Unknown scopes and missing identities fail without guessing.
    pub fn robot_reply_target(&self) -> Result<RobotReplyTarget> {
        match self.conversation_scope {
            ConversationScope::Group => {
                RobotReplyTarget::group(self.open_conversation_id.as_deref().ok_or_else(|| {
                    Error::invalid_input(
                        "open_conversation_id",
                        "event has no group conversation id",
                    )
                })?)
            }
            ConversationScope::Private => {
                RobotReplyTarget::private(self.sender_staff_id.as_deref().ok_or_else(|| {
                    Error::invalid_input("sender_staff_id", "event has no enterprise staff id")
                })?)
            }
            _ => Err(Error::invalid_input(
                "conversation_scope",
                "a group or private conversation is required",
            )),
        }
    }
}

impl<S> BotContext<S> {
    /// Captures the reply destination without retaining this context or its credentials.
    pub fn robot_reply_target(&self) -> Result<RobotReplyTarget> {
        self.event.robot_reply_target()
    }

    /// Explicitly replies through OpenAPI, including after session webhook expiry.
    ///
    /// Existing `reply_*` methods remain session-webhook-only. The supplied robot
    /// determines the credentials and permissions used for this proactive message.
    ///
    /// ```no_run
    /// # use dingding::{Result, bot::Context, openapi::RobotMessage};
    /// # async fn reply(ctx: Context) -> Result<()> {
    /// let robot = ctx.client().openapi().robot("robot-code")?;
    /// ctx.reply_via_robot(&robot, RobotMessage::text("Job completed")).await?;
    /// # Ok(()) }
    /// ```
    pub async fn reply_via_robot(
        &self,
        robot: &RobotApi,
        message: RobotMessage,
    ) -> Result<RobotMessageResponse> {
        self.robot_reply_target()?
            .send_message(robot, message)
            .await
    }
}
