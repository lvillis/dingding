//! Common imports for DingTalk bot applications.

pub use crate::{DingTalk, Error, Result, auth::AppCredentials};

#[cfg(feature = "macros")]
pub use crate::handler;

#[cfg(feature = "bot")]
pub use crate::bot::{
    AnyContext, Bot, BotAck, BotContext, BotEvent, CallbackHeaders, CallbackRequest,
    CallbackVerifier, Context, ConversationScope, GroupContext, HandleOutcome, IncomingMessage,
    MessageType, Msg, PrivateContext, Route, Scope,
};

#[cfg(feature = "openapi")]
pub use crate::openapi::{
    DownloadedFile, InteractiveCard, InteractiveCardResponse, InteractiveCardSendOptions,
    InteractiveCardUpdate, InteractiveCardUpdateOptions, MediaType, MediaUpload,
    MessageFileDownload, OpenApi, RobotActionButton, RobotActionCard, RobotActionCardLayout,
    RobotApi, RobotMessage, RobotMessageResponse, RobotVideo, UploadedMedia,
};

#[cfg(feature = "stream")]
pub use crate::stream::{
    CARD_CALLBACK_TOPIC, CardCallbackActionValue, CardCallbackContent, CardCallbackEvent,
    CardCallbackOperator, CardCallbackPayload, CardCallbackPrivateData, CardCallbackResponse,
    ReconnectPolicy, StreamBot, StreamBotBuilder, StreamClient, StreamClientBuilder, StreamExit,
    StreamFrame, StreamFrameResponse, StreamFrameType, StreamHeaders, StreamRunEvent,
    StreamSubscription, StreamSubscriptionType,
};

#[cfg(feature = "webhook")]
pub use crate::webhook::{
    ActionCardButton, At, ButtonOrientation, FeedCardLink, WebhookMessage, WebhookResponse,
};
