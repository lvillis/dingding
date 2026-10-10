//! Common imports for DingTalk bot applications.

pub use crate::{
    BoxError, DingTalk, Error, HandlerResult, IntoHandlerResult, Result, auth::AppCredentials,
    handler_future,
};

#[cfg(feature = "macros")]
pub use crate::handler;

#[cfg(feature = "bot")]
pub use crate::bot::dedup::{
    Deduplication, DeduplicationFuture, DeduplicationLease, EventDeduplicator,
    MemoryEventDeduplicator,
};

#[cfg(feature = "bot")]
pub use crate::bot::{
    AnyContext, Bot, BotAck, BotContext, BotEvent, CallbackHeaders, CallbackRequest,
    CallbackVerifier, Context, ConversationScope, GroupContext, HandleOutcome, IncomingMessage,
    MessageType, Msg, PrivateContext, Route, Scope,
};

#[cfg(feature = "openapi")]
pub use crate::openapi::{
    DownloadedFile, DownloadedFileInfo, GroupMessagePages, GroupMessageQuery, GroupMessageReader,
    GroupMessageStatus, InteractiveCard, InteractiveCardResponse, InteractiveCardSendOptions,
    InteractiveCardUpdate, InteractiveCardUpdateOptions, MediaFileUpload, MediaType, MediaUpload,
    MessageFileDownload, MessageReadInfo, MessageRecallResponse, OpenApi, PrivateMessageStatus,
    RobotActionButton, RobotActionCard, RobotActionCardLayout, RobotApi, RobotMessage,
    RobotMessageResponse, RobotReplyTarget, RobotVideo, UploadedMedia,
};

#[cfg(feature = "stream")]
pub use crate::stream::{
    BOT_MESSAGE_TOPIC, CARD_CALLBACK_TOPIC, CardCallbackActionValue, CardCallbackContent,
    CardCallbackEvent, CardCallbackOperator, CardCallbackPayload, CardCallbackPrivateData,
    CardCallbackResponse, ReconnectPolicy, StreamBot, StreamBotBuilder, StreamCancellationReason,
    StreamClient, StreamClientBuilder, StreamContext, StreamError, StreamExit, StreamFrame,
    StreamFrameResponse, StreamFrameType, StreamHeaders, StreamProcessingPolicy, StreamRunEvent,
    StreamSubscription, StreamSubscriptionType,
};

#[cfg(feature = "webhook")]
pub use crate::webhook::{
    ActionCardButton, At, ButtonOrientation, FeedCardLink, WebhookMessage, WebhookResponse,
};
