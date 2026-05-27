use std::{
    any::Any, collections::BTreeMap, fmt, future::Future, pin::Pin, sync::Arc, time::Duration,
};

use futures_util::{
    Sink, SinkExt, StreamExt,
    future::{Either, FutureExt, pending, select},
    pin_mut,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

use crate::{
    DingTalk, Error, Result,
    auth::AppCredentials,
    bot::{
        Bot, BotContext, BotEvent, BotState, ConversationScope, HandleOutcome, MessageType, Route,
    },
    transport::decode_json_response,
    util::non_empty_trimmed,
};

/// Stream topic for robot message callbacks.
pub const BOT_MESSAGE_TOPIC: &str = "/v1.0/im/bot/messages/get";
/// Stream topic for interactive card callbacks.
pub const CARD_CALLBACK_TOPIC: &str = "/v1.0/card/instances/callback";

type StreamEventHandler = Arc<dyn Fn(StreamRunEvent) + Send + Sync + 'static>;
type BoxStreamFrameFuture =
    Pin<Box<dyn Future<Output = Result<StreamFrameResponse>> + Send + 'static>>;
type StreamFrameHandler = Arc<dyn Fn(StreamFrame) -> BoxStreamFrameFuture + Send + Sync + 'static>;
type CardCallbackFuture =
    Pin<Box<dyn Future<Output = Result<StreamFrameResponse>> + Send + 'static>>;
type CardCallbackHandler =
    Arc<dyn Fn(CardCallbackEvent) -> CardCallbackFuture + Send + Sync + 'static>;

/// High-level Stream robot application.
#[derive(Clone)]
pub struct StreamBot {
    client: StreamClient,
}

impl StreamBot {
    /// Creates a high-level Stream bot builder.
    #[must_use]
    pub fn builder() -> StreamBotBuilder {
        StreamBotBuilder::new()
    }

    /// Creates a Stream bot builder using app credentials from environment variables.
    ///
    /// Reads `DINGTALK_CLIENT_ID` / `DINGTALK_CLIENT_SECRET`, falling back to
    /// `DINGTALK_APP_KEY` / `DINGTALK_APP_SECRET`.
    pub fn from_env() -> Result<StreamBotBuilder> {
        StreamBotBuilder::from_env()
    }

    /// Creates a Stream bot builder from an existing SDK client.
    #[must_use]
    pub fn from_client(client: DingTalk) -> StreamBotBuilder {
        StreamBotBuilder::from_client(client)
    }

    /// Runs the Stream bot forever with reconnect backoff.
    pub async fn run(&self) -> Result<()> {
        self.client.run().await
    }

    /// Runs the Stream bot with reconnect backoff until `shutdown` resolves.
    pub async fn run_until<F>(&self, shutdown: F) -> Result<()>
    where
        F: Future<Output = ()>,
    {
        self.client.run_until(shutdown).await
    }
}

/// Builder for [`StreamBot`].
pub struct StreamBotBuilder {
    client: Option<DingTalk>,
    credentials: Option<AppCredentials>,
    routes: Vec<Route>,
    fallback: Option<Route>,
    state: Option<BotState>,
    subscriptions: Vec<StreamSubscription>,
    local_ip: Option<String>,
    user_agent: Option<String>,
    reconnect: ReconnectPolicy,
    event_handler: Option<StreamEventHandler>,
    frame_handler: Option<StreamFrameHandler>,
    card_callback_handler: Option<CardCallbackHandler>,
}

impl StreamBotBuilder {
    fn new() -> Self {
        Self {
            client: None,
            credentials: None,
            routes: Vec::new(),
            fallback: None,
            state: None,
            subscriptions: vec![StreamSubscription::bot_messages()],
            local_ip: None,
            user_agent: None,
            reconnect: ReconnectPolicy::default(),
            event_handler: None,
            frame_handler: None,
            card_callback_handler: None,
        }
    }

    fn from_env() -> Result<Self> {
        Ok(Self::new().credentials(AppCredentials::from_env()?))
    }

    fn from_client(client: DingTalk) -> Self {
        Self {
            client: Some(client),
            ..Self::new()
        }
    }

    /// Uses an existing SDK client.
    #[must_use]
    pub fn client(mut self, client: DingTalk) -> Self {
        self.client = Some(client);
        self
    }

    /// Overrides Stream credentials.
    #[must_use]
    pub fn credentials(mut self, credentials: AppCredentials) -> Self {
        self.credentials = Some(credentials);
        self
    }

    /// Overrides Stream credentials.
    #[must_use]
    pub fn client_id_and_secret(
        mut self,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
    ) -> Self {
        self.credentials = Some(AppCredentials::new(client_id, client_secret));
        self
    }

    /// Registers a route.
    #[must_use]
    pub fn route(mut self, route: Route) -> Self {
        self.routes.push(route);
        self
    }

    /// Registers a text command route.
    #[must_use]
    pub fn on_text_command<F, Fut>(
        self,
        scope: ConversationScope,
        command: impl Into<String>,
        handler: F,
    ) -> Self
    where
        F: Fn(BotContext, BotEvent) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.route(
            Route::new(scope)
                .message_type(MessageType::Text)
                .command(command)
                .handle(handler),
        )
    }

    /// Registers a text command route with multiple command aliases.
    #[must_use]
    pub fn on_text_commands<I, S, F, Fut>(
        self,
        scope: ConversationScope,
        commands: I,
        handler: F,
    ) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
        F: Fn(BotContext, BotEvent) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.route(
            Route::new(scope)
                .message_type(MessageType::Text)
                .commands(commands)
                .handle(handler),
        )
    }

    /// Registers a message route.
    #[must_use]
    pub fn on_message<F, Fut>(
        self,
        scope: ConversationScope,
        message_type: MessageType,
        handler: F,
    ) -> Self
    where
        F: Fn(BotContext, BotEvent) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.route(Route::new(scope).message_type(message_type).handle(handler))
    }

    /// Registers a group text command route.
    #[must_use]
    pub fn on_group_text_command<F, Fut>(self, command: impl Into<String>, handler: F) -> Self
    where
        F: Fn(BotContext, BotEvent) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.on_text_command(ConversationScope::Group, command, handler)
    }

    /// Registers a group text command route with multiple command aliases.
    #[must_use]
    pub fn on_group_text_commands<I, S, F, Fut>(self, commands: I, handler: F) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
        F: Fn(BotContext, BotEvent) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.on_text_commands(ConversationScope::Group, commands, handler)
    }

    /// Registers a private-chat text command route.
    #[must_use]
    pub fn on_private_text_command<F, Fut>(self, command: impl Into<String>, handler: F) -> Self
    where
        F: Fn(BotContext, BotEvent) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.on_text_command(ConversationScope::Private, command, handler)
    }

    /// Registers a private-chat text command route with multiple command aliases.
    #[must_use]
    pub fn on_private_text_commands<I, S, F, Fut>(self, commands: I, handler: F) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
        F: Fn(BotContext, BotEvent) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.on_text_commands(ConversationScope::Private, commands, handler)
    }

    /// Registers a group message route.
    #[must_use]
    pub fn on_group_message<F, Fut>(self, message_type: MessageType, handler: F) -> Self
    where
        F: Fn(BotContext, BotEvent) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.on_message(ConversationScope::Group, message_type, handler)
    }

    /// Registers a private-chat message route.
    #[must_use]
    pub fn on_private_message<F, Fut>(self, message_type: MessageType, handler: F) -> Self
    where
        F: Fn(BotContext, BotEvent) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.on_message(ConversationScope::Private, message_type, handler)
    }

    /// Registers a fallback route that runs when no normal route matches.
    #[must_use]
    pub fn fallback_route(mut self, route: Route) -> Self {
        self.fallback = Some(route);
        self
    }

    /// Registers a fallback handler that runs when no normal route matches.
    #[must_use]
    pub fn fallback<F, Fut>(self, handler: F) -> Self
    where
        F: Fn(BotContext, BotEvent) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.fallback_route(Route::new(ConversationScope::Any).handle(handler))
    }

    /// Registers a fallback handler for unmatched text messages.
    #[must_use]
    pub fn on_unmatched_text<F, Fut>(self, scope: ConversationScope, handler: F) -> Self
    where
        F: Fn(BotContext, BotEvent) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.fallback_route(
            Route::new(scope)
                .message_type(MessageType::Text)
                .handle(handler),
        )
    }

    /// Configures shared application state available from [`BotContext::state`].
    #[must_use]
    pub fn state<T>(mut self, state: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        self.state = Some(Arc::new(state) as Arc<dyn Any + Send + Sync>);
        self
    }

    /// Replaces Stream subscriptions.
    #[must_use]
    pub fn subscriptions(mut self, subscriptions: Vec<StreamSubscription>) -> Self {
        self.subscriptions = subscriptions;
        self
    }

    /// Adds a Stream subscription.
    #[must_use]
    pub fn subscription(mut self, subscription: StreamSubscription) -> Self {
        self.subscriptions.push(subscription);
        self
    }

    /// Sets local IP metadata reported to DingTalk.
    #[must_use]
    pub fn local_ip(mut self, value: impl Into<String>) -> Self {
        self.local_ip = Some(value.into());
        self
    }

    /// Sets Stream client user-agent metadata.
    #[must_use]
    pub fn user_agent(mut self, value: impl Into<String>) -> Self {
        self.user_agent = Some(value.into());
        self
    }

    /// Sets reconnect policy.
    #[must_use]
    pub fn reconnect_policy(mut self, value: ReconnectPolicy) -> Self {
        self.reconnect = value;
        self
    }

    /// Registers a synchronous runtime event handler.
    #[must_use]
    pub fn on_event<F>(mut self, handler: F) -> Self
    where
        F: Fn(StreamRunEvent) + Send + Sync + 'static,
    {
        self.event_handler = Some(Arc::new(handler));
        self
    }

    /// Registers a handler for non-system Stream frames not handled by the bot router.
    ///
    /// Bot message callbacks are still routed through [`Bot`] first when a bot router is
    /// configured. Other callback/event topics, or bot message callbacks without a bot router,
    /// are acknowledged with `200 OK` after this handler completes.
    #[must_use]
    pub fn on_frame<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(StreamFrame) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        self.frame_handler = Some(Arc::new(move |frame| {
            let handler = Arc::clone(&handler);
            Box::pin(async move {
                handler(frame).await?;
                Ok(StreamFrameResponse::empty())
            })
        }));
        self
    }

    /// Registers a handler that can return a Stream callback response payload.
    #[must_use]
    pub fn on_frame_with_response<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(StreamFrame) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<StreamFrameResponse>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        self.frame_handler = Some(Arc::new(move |frame| {
            let handler = Arc::clone(&handler);
            Box::pin(async move { handler(frame).await })
        }));
        self
    }

    /// Registers a handler for interactive card callbacks.
    ///
    /// The card callback subscription is added automatically.
    #[must_use]
    pub fn on_card_callback<F, Fut>(self, handler: F) -> Self
    where
        F: Fn(CardCallbackEvent) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.on_card_callback_with_response(move |event| {
            let future = handler(event);
            async move {
                future.await?;
                Ok(StreamFrameResponse::empty())
            }
        })
    }

    /// Registers a card callback handler that can return a Stream callback response payload.
    ///
    /// The card callback subscription is added automatically.
    #[must_use]
    pub fn on_card_callback_with_response<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(CardCallbackEvent) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<StreamFrameResponse>> + Send + 'static,
    {
        self = self.subscription_once(StreamSubscription::card_callbacks());
        let handler = Arc::new(handler);
        self.card_callback_handler = Some(Arc::new(move |event| {
            let handler = Arc::clone(&handler);
            Box::pin(async move { handler(event).await })
        }));
        self
    }

    fn subscription_once(mut self, subscription: StreamSubscription) -> Self {
        if !self.subscriptions.iter().any(|existing| {
            existing.kind == subscription.kind && existing.topic == subscription.topic
        }) {
            self.subscriptions.push(subscription);
        }
        self
    }

    /// Builds the Stream bot.
    pub fn build(self) -> Result<StreamBot> {
        let client = match self.client {
            Some(client) => client,
            None => {
                let credentials = self.credentials.clone().ok_or(Error::MissingCredentials)?;
                DingTalk::builder().app_credentials(credentials).build()?
            }
        };

        let mut bot = Bot::new(client.clone());
        for route in self.routes {
            bot = bot.route(route);
        }
        if let Some(route) = self.fallback {
            bot = bot.fallback_route(route);
        }
        if let Some(state) = self.state {
            bot = bot.state_arc(state);
        }

        let mut stream = StreamClient::builder(client.clone())?
            .subscriptions(self.subscriptions)
            .reconnect_policy(self.reconnect)
            .bot(bot);
        if let Some(credentials) = self.credentials {
            stream = stream.credentials(credentials);
        }
        if let Some(local_ip) = self.local_ip {
            stream = stream.local_ip(local_ip);
        }
        if let Some(user_agent) = self.user_agent {
            stream = stream.user_agent(user_agent);
        }
        if let Some(handler) = self.event_handler {
            stream = stream.on_event_handler(handler);
        }
        if let Some(handler) = self.frame_handler {
            stream = stream.on_frame_handler(handler);
        }
        if let Some(handler) = self.card_callback_handler {
            stream = stream.on_card_callback_handler(handler);
        }

        Ok(StreamBot {
            client: stream.build()?,
        })
    }

    /// Builds and runs the Stream bot forever with reconnect backoff.
    pub async fn run(self) -> Result<()> {
        self.build()?.run().await
    }

    /// Builds and runs the Stream bot until `shutdown` resolves.
    pub async fn run_until<F>(self, shutdown: F) -> Result<()>
    where
        F: Future<Output = ()>,
    {
        self.build()?.run_until(shutdown).await
    }
}

/// DingTalk Stream client.
#[derive(Clone)]
pub struct StreamClient {
    client: DingTalk,
    credentials: AppCredentials,
    subscriptions: Vec<StreamSubscription>,
    local_ip: Option<String>,
    user_agent: String,
    reconnect: ReconnectPolicy,
    bot: Option<Bot>,
    event_handler: Option<StreamEventHandler>,
    frame_handler: Option<StreamFrameHandler>,
    card_callback_handler: Option<CardCallbackHandler>,
}

impl StreamClient {
    /// Creates a Stream client builder.
    pub fn builder(client: DingTalk) -> Result<StreamClientBuilder> {
        StreamClientBuilder::new(client)
    }

    /// Opens one Stream WebSocket connection and runs until the connection closes.
    pub async fn run_once(&self) -> Result<StreamExit> {
        self.run_once_with_attempt(1).await
    }

    async fn run_once_with_attempt(&self, attempt: u32) -> Result<StreamExit> {
        self.emit_event(StreamRunEvent::ConnectionOpening { attempt });
        let ticket = self.open_connection().await?;
        let url = websocket_url(&ticket)?;
        let (mut socket, _response) = connect_async(url.as_str())
            .await
            .map_err(|source| Error::Stream(format!("websocket connect failed: {source}")))?;
        self.emit_event(StreamRunEvent::ConnectionOpened { attempt });

        while let Some(message) = socket.next().await {
            let message = message
                .map_err(|source| Error::Stream(format!("websocket receive failed: {source}")))?;

            match message {
                Message::Text(text) => {
                    if self.handle_frame_and_ack(&mut socket, &text).await? {
                        self.emit_event(StreamRunEvent::ConnectionClosed {
                            attempt,
                            exit: StreamExit::Disconnect,
                        });
                        return Ok(StreamExit::Disconnect);
                    }
                }
                Message::Binary(bytes) => {
                    let text = String::from_utf8(bytes.to_vec()).map_err(|source| {
                        Error::Stream(format!("invalid utf-8 frame: {source}"))
                    })?;
                    if self.handle_frame_and_ack(&mut socket, &text).await? {
                        self.emit_event(StreamRunEvent::ConnectionClosed {
                            attempt,
                            exit: StreamExit::Disconnect,
                        });
                        return Ok(StreamExit::Disconnect);
                    }
                }
                Message::Ping(bytes) => {
                    socket.send(Message::Pong(bytes)).await.map_err(|source| {
                        Error::Stream(format!("websocket pong failed: {source}"))
                    })?;
                }
                Message::Pong(_) => {}
                Message::Close(_) => {
                    self.emit_event(StreamRunEvent::ConnectionClosed {
                        attempt,
                        exit: StreamExit::Closed,
                    });
                    return Ok(StreamExit::Closed);
                }
                Message::Frame(_) => {}
            }
        }

        self.emit_event(StreamRunEvent::ConnectionClosed {
            attempt,
            exit: StreamExit::Closed,
        });
        Ok(StreamExit::Closed)
    }

    async fn handle_frame_and_ack<S>(&self, socket: &mut S, text: &str) -> Result<bool>
    where
        S: Sink<Message> + Unpin,
        S::Error: std::fmt::Display,
    {
        let response = self.handle_text_frame(text).await;
        let handled = response.unwrap_or_else(StreamHandleResult::internal_error);
        self.emit_frame_error(&handled);

        let exit_after_ack = handled.exit_after_ack;
        let payload = serde_json::to_string(&handled.ack)?;
        socket
            .send(Message::Text(payload.into()))
            .await
            .map_err(|source| Error::Stream(format!("websocket send failed: {source}")))?;

        Ok(exit_after_ack)
    }

    /// Runs the Stream client forever with reconnect backoff.
    pub async fn run(&self) -> Result<()> {
        self.run_until(pending()).await
    }

    /// Runs the Stream client with reconnect backoff until `shutdown` resolves.
    pub async fn run_until<F>(&self, shutdown: F) -> Result<()>
    where
        F: Future<Output = ()>,
    {
        let shutdown = shutdown.fuse();
        pin_mut!(shutdown);
        let mut attempt = 1_u32;

        loop {
            let run_once = self.run_once_with_attempt(attempt).fuse();
            pin_mut!(run_once);

            let run_result = match select(run_once, shutdown.as_mut()).await {
                Either::Left((result, _shutdown)) => result,
                Either::Right(((), _run_once)) => {
                    self.emit_event(StreamRunEvent::Shutdown);
                    return Ok(());
                }
            };

            match run_result {
                Ok(StreamExit::Disconnect | StreamExit::Closed) => {}
                Err(error) => {
                    let retrying = self.reconnect.retry_on_error;
                    self.emit_event(StreamRunEvent::ConnectionError {
                        attempt,
                        retrying,
                        error: error.to_string(),
                    });
                    if !self.reconnect.retry_on_error {
                        return Err(error);
                    }
                }
            }

            let delay = self.reconnect.delay_for_attempt(attempt);
            attempt = attempt.saturating_add(1);
            self.emit_event(StreamRunEvent::ReconnectScheduled {
                next_attempt: attempt,
                delay,
            });

            let sleep = tokio::time::sleep(delay).fuse();
            pin_mut!(sleep);
            match select(sleep, shutdown.as_mut()).await {
                Either::Left((_done, _shutdown)) => {}
                Either::Right(((), _sleep)) => {
                    self.emit_event(StreamRunEvent::Shutdown);
                    return Ok(());
                }
            }
        }
    }

    async fn open_connection(&self) -> Result<OpenConnectionResponse> {
        self.credentials.validate()?;
        let url = self
            .client
            .openapi_endpoint(&["v1.0", "gateway", "connections", "open"])?;
        let request = OpenConnectionRequest {
            client_id: self.credentials.app_key(),
            client_secret: self.credentials.app_secret(),
            local_ip: self.local_ip.as_deref(),
            subscriptions: &self.subscriptions,
            user_agent: &self.user_agent,
        };
        let response = self
            .client
            .transport()
            .post_openapi_json(&url, None, &request)
            .await?;
        let (value, _body) = decode_json_response::<OpenConnectionResponse>(
            response,
            self.client.transport().error_body_snippet(),
        )?;
        value.validate()?;
        Ok(value)
    }

    async fn handle_text_frame(&self, text: &str) -> Result<StreamHandleResult> {
        let frame = match StreamFrame::from_text(text) {
            Ok(frame) => frame,
            Err(error) => {
                let message_id = StreamFrame::message_id_from_text(text);
                return Ok(StreamHandleResult::frame_error(
                    message_id.clone(),
                    StreamAck::internal_error(message_id.unwrap_or_default()),
                    error,
                ));
            }
        };

        if frame.frame_type == StreamFrameType::Callback
            && frame.headers.topic == BOT_MESSAGE_TOPIC
            && let Some(bot) = &self.bot
        {
            return self.handle_bot_frame(bot, frame).await;
        }

        if frame.is_card_callback()
            && let Some(handler) = &self.card_callback_handler
        {
            return self.handle_card_callback_frame(handler, frame).await;
        }

        if matches!(frame.frame_type(), StreamFrameType::System) {
            self.handle_system_frame(frame)
        } else {
            self.handle_unhandled_frame(frame).await
        }
    }

    async fn handle_bot_frame(&self, bot: &Bot, frame: StreamFrame) -> Result<StreamHandleResult> {
        let message_id = frame.headers.message_id.clone();
        let event = match frame.bot_event() {
            Ok(Some(event)) => event,
            Ok(None) => {
                return Ok(StreamHandleResult::ack(StreamAck::not_found(message_id)));
            }
            Err(error) => {
                return Ok(StreamHandleResult::frame_error(
                    Some(message_id.clone()),
                    StreamAck::internal_error(message_id),
                    error,
                ));
            }
        };

        let conversation_scope = event.conversation_scope.clone();
        let message_type = event.message_type.clone();
        match bot.handle_event(event).await {
            Ok(outcome) => {
                self.emit_event(StreamRunEvent::BotEventHandled {
                    message_id: message_id.clone(),
                    outcome,
                    conversation_scope,
                    message_type,
                });
                Ok(StreamHandleResult::ack(StreamAck::ok(
                    message_id,
                    StreamAckData::Response(Value::Null),
                )))
            }
            Err(error) => Ok(StreamHandleResult::frame_error(
                Some(message_id.clone()),
                StreamAck::internal_error(message_id),
                error,
            )),
        }
    }

    async fn handle_card_callback_frame(
        &self,
        handler: &CardCallbackHandler,
        frame: StreamFrame,
    ) -> Result<StreamHandleResult> {
        let message_id = frame.headers.message_id.clone();
        let event = match frame.card_callback() {
            Ok(Some(event)) => event,
            Ok(None) => {
                return Ok(StreamHandleResult::ack(StreamAck::not_found(message_id)));
            }
            Err(error) => {
                return Ok(StreamHandleResult::frame_error(
                    Some(message_id.clone()),
                    StreamAck::internal_error(message_id),
                    error,
                ));
            }
        };

        let payload = event.payload();
        let card_biz_id = payload.card_biz_id().map(ToOwned::to_owned);
        let action = payload.action().map(ToOwned::to_owned);
        match handler(event).await {
            Ok(response) => {
                self.emit_event(StreamRunEvent::CardCallbackHandled {
                    message_id: message_id.clone(),
                    card_biz_id,
                    action,
                });
                Ok(StreamHandleResult::ack(StreamAck::ok(
                    message_id,
                    StreamAckData::Response(response.into_value()),
                )))
            }
            Err(error) => Ok(StreamHandleResult::frame_error(
                Some(message_id.clone()),
                StreamAck::internal_error(message_id),
                error,
            )),
        }
    }

    async fn handle_unhandled_frame(&self, frame: StreamFrame) -> Result<StreamHandleResult> {
        let message_id = frame.headers.message_id.clone();
        let Some(handler) = &self.frame_handler else {
            return Ok(StreamHandleResult::ack(StreamAck::not_found(message_id)));
        };

        match handler(frame).await {
            Ok(response) => Ok(StreamHandleResult::ack(StreamAck::ok(
                message_id,
                StreamAckData::Response(response.into_value()),
            ))),
            Err(error) => Ok(StreamHandleResult::frame_error(
                Some(message_id.clone()),
                StreamAck::internal_error(message_id),
                error,
            )),
        }
    }

    fn handle_system_frame(&self, frame: StreamFrame) -> Result<StreamHandleResult> {
        match frame.headers.topic.as_str() {
            "ping" => {
                let data = frame.data_json().unwrap_or(Value::Null);
                Ok(StreamHandleResult::ack(StreamAck::ok(
                    frame.headers.message_id,
                    StreamAckData::Raw(data),
                )))
            }
            "disconnect" => Ok(StreamHandleResult::disconnect(StreamAck::disconnect(
                frame.headers.message_id,
            ))),
            _ => Ok(StreamHandleResult::ack(StreamAck::not_found(
                frame.headers.message_id,
            ))),
        }
    }

    fn emit_event(&self, event: StreamRunEvent) {
        if let Some(handler) = &self.event_handler {
            handler(event);
        }
    }

    fn emit_frame_error(&self, result: &StreamHandleResult) {
        let Some(error) = &result.error else {
            return;
        };
        self.emit_event(StreamRunEvent::FrameError {
            message_id: error.message_id.clone(),
            error: error.error.to_string(),
        });
    }
}

struct StreamFrameError {
    message_id: Option<String>,
    error: Error,
}

struct StreamHandleResult {
    ack: StreamAck,
    exit_after_ack: bool,
    error: Option<StreamFrameError>,
}

impl StreamHandleResult {
    fn ack(ack: StreamAck) -> Self {
        Self {
            ack,
            exit_after_ack: false,
            error: None,
        }
    }

    fn disconnect(ack: StreamAck) -> Self {
        Self {
            ack,
            exit_after_ack: true,
            error: None,
        }
    }

    fn internal_error(error: Error) -> Self {
        Self::frame_error(None, StreamAck::internal_error(String::new()), error)
    }

    fn frame_error(message_id: Option<String>, ack: StreamAck, error: Error) -> Self {
        Self {
            ack,
            exit_after_ack: false,
            error: Some(StreamFrameError { message_id, error }),
        }
    }
}

/// Builder for [`StreamClient`].
pub struct StreamClientBuilder {
    client: DingTalk,
    credentials: Option<AppCredentials>,
    subscriptions: Vec<StreamSubscription>,
    local_ip: Option<String>,
    user_agent: String,
    reconnect: ReconnectPolicy,
    bot: Option<Bot>,
    event_handler: Option<StreamEventHandler>,
    frame_handler: Option<StreamFrameHandler>,
    card_callback_handler: Option<CardCallbackHandler>,
}

impl StreamClientBuilder {
    fn new(client: DingTalk) -> Result<Self> {
        let credentials = client.app_credentials();
        Ok(Self {
            client,
            credentials,
            subscriptions: vec![StreamSubscription::bot_messages()],
            local_ip: None,
            user_agent: concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION")).to_string(),
            reconnect: ReconnectPolicy::default(),
            bot: None,
            event_handler: None,
            frame_handler: None,
            card_callback_handler: None,
        })
    }

    /// Overrides Stream credentials.
    #[must_use]
    pub fn credentials(mut self, credentials: AppCredentials) -> Self {
        self.credentials = Some(credentials);
        self
    }

    /// Overrides Stream credentials.
    #[must_use]
    pub fn client_id_and_secret(
        mut self,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
    ) -> Self {
        self.credentials = Some(AppCredentials::new(client_id, client_secret));
        self
    }

    /// Sets the bot router that handles robot message callbacks.
    #[must_use]
    pub fn bot(mut self, bot: Bot) -> Self {
        self.bot = Some(bot);
        self
    }

    /// Replaces Stream subscriptions.
    #[must_use]
    pub fn subscriptions(mut self, subscriptions: Vec<StreamSubscription>) -> Self {
        self.subscriptions = subscriptions;
        self
    }

    /// Adds a Stream subscription.
    #[must_use]
    pub fn subscription(mut self, subscription: StreamSubscription) -> Self {
        self.subscriptions.push(subscription);
        self
    }

    /// Sets local IP metadata reported to DingTalk.
    #[must_use]
    pub fn local_ip(mut self, value: impl Into<String>) -> Self {
        self.local_ip = Some(value.into());
        self
    }

    /// Sets Stream client user-agent metadata.
    #[must_use]
    pub fn user_agent(mut self, value: impl Into<String>) -> Self {
        self.user_agent = value.into();
        self
    }

    /// Sets reconnect policy.
    #[must_use]
    pub fn reconnect_policy(mut self, value: ReconnectPolicy) -> Self {
        self.reconnect = value;
        self
    }

    /// Registers a synchronous runtime event handler.
    ///
    /// Use this to bridge Stream lifecycle events into your logging or metrics system.
    #[must_use]
    pub fn on_event<F>(mut self, handler: F) -> Self
    where
        F: Fn(StreamRunEvent) + Send + Sync + 'static,
    {
        self.event_handler = Some(Arc::new(handler));
        self
    }

    fn on_event_handler(mut self, handler: StreamEventHandler) -> Self {
        self.event_handler = Some(handler);
        self
    }

    /// Registers a handler for callback/event frames not handled by a bot router.
    #[must_use]
    pub fn on_frame<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(StreamFrame) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        self.frame_handler = Some(Arc::new(move |frame| {
            let handler = Arc::clone(&handler);
            Box::pin(async move {
                handler(frame).await?;
                Ok(StreamFrameResponse::empty())
            })
        }));
        self
    }

    /// Registers a handler that can return a Stream callback response payload.
    #[must_use]
    pub fn on_frame_with_response<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(StreamFrame) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<StreamFrameResponse>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        self.frame_handler = Some(Arc::new(move |frame| {
            let handler = Arc::clone(&handler);
            Box::pin(async move { handler(frame).await })
        }));
        self
    }

    /// Registers a handler for interactive card callbacks.
    ///
    /// The card callback subscription is added automatically.
    #[must_use]
    pub fn on_card_callback<F, Fut>(self, handler: F) -> Self
    where
        F: Fn(CardCallbackEvent) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.on_card_callback_with_response(move |event| {
            let future = handler(event);
            async move {
                future.await?;
                Ok(StreamFrameResponse::empty())
            }
        })
    }

    /// Registers a card callback handler that can return a Stream callback response payload.
    ///
    /// The card callback subscription is added automatically.
    #[must_use]
    pub fn on_card_callback_with_response<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(CardCallbackEvent) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<StreamFrameResponse>> + Send + 'static,
    {
        self = self.subscription_once(StreamSubscription::card_callbacks());
        let handler = Arc::new(handler);
        self.card_callback_handler = Some(Arc::new(move |event| {
            let handler = Arc::clone(&handler);
            Box::pin(async move { handler(event).await })
        }));
        self
    }

    fn on_frame_handler(mut self, handler: StreamFrameHandler) -> Self {
        self.frame_handler = Some(handler);
        self
    }

    fn on_card_callback_handler(mut self, handler: CardCallbackHandler) -> Self {
        self.card_callback_handler = Some(handler);
        self
    }

    fn subscription_once(mut self, subscription: StreamSubscription) -> Self {
        if !self.subscriptions.iter().any(|existing| {
            existing.kind == subscription.kind && existing.topic == subscription.topic
        }) {
            self.subscriptions.push(subscription);
        }
        self
    }

    /// Builds a Stream client.
    pub fn build(self) -> Result<StreamClient> {
        let credentials = self.credentials.ok_or(Error::MissingCredentials)?;
        credentials.validate()?;
        if self.subscriptions.is_empty() {
            return Err(Error::invalid_input(
                "subscriptions",
                "at least one subscription is required",
            ));
        }
        for subscription in &self.subscriptions {
            subscription.validate()?;
        }
        self.reconnect.validate()?;
        let user_agent = non_empty_trimmed(&self.user_agent, "user_agent")?;
        let local_ip = self
            .local_ip
            .map(|value| non_empty_trimmed(&value, "local_ip"))
            .transpose()?;
        if self.bot.is_none()
            && self.frame_handler.is_none()
            && self.card_callback_handler.is_none()
        {
            return Err(Error::InvalidConfig(
                "stream bot router, frame handler, or card callback handler is required"
                    .to_string(),
            ));
        }

        Ok(StreamClient {
            client: self.client,
            credentials,
            subscriptions: self.subscriptions,
            local_ip,
            user_agent,
            reconnect: self.reconnect,
            bot: self.bot,
            event_handler: self.event_handler,
            frame_handler: self.frame_handler,
            card_callback_handler: self.card_callback_handler,
        })
    }
}

/// Stream reconnect policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconnectPolicy {
    /// Initial reconnect delay.
    pub initial_delay: Duration,
    /// Maximum reconnect delay.
    pub max_delay: Duration,
    /// Whether errors should be retried.
    pub retry_on_error: bool,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            retry_on_error: true,
        }
    }
}

impl ReconnectPolicy {
    /// Creates a reconnect policy with retry-on-error enabled.
    #[must_use]
    pub fn new(initial_delay: Duration, max_delay: Duration) -> Self {
        Self {
            initial_delay,
            max_delay,
            retry_on_error: true,
        }
    }

    /// Creates a reconnect policy that does not retry connection errors.
    #[must_use]
    pub fn no_retry() -> Self {
        Self {
            retry_on_error: false,
            ..Self::default()
        }
    }

    /// Sets the initial reconnect delay.
    #[must_use]
    pub fn initial_delay(mut self, value: Duration) -> Self {
        self.initial_delay = value;
        self
    }

    /// Sets the maximum reconnect delay.
    #[must_use]
    pub fn max_delay(mut self, value: Duration) -> Self {
        self.max_delay = value;
        self
    }

    /// Sets whether connection errors should be retried.
    #[must_use]
    pub fn retry_on_error(mut self, value: bool) -> Self {
        self.retry_on_error = value;
        self
    }

    /// Validates this reconnect policy.
    pub fn validate(&self) -> Result<()> {
        if self.initial_delay.is_zero() {
            return Err(Error::invalid_input(
                "reconnect.initial_delay",
                "value must be greater than zero",
            ));
        }
        if self.max_delay < self.initial_delay {
            return Err(Error::invalid_input(
                "reconnect.max_delay",
                "value must be greater than or equal to initial_delay",
            ));
        }
        Ok(())
    }

    /// Returns the reconnect delay for a one-based connection attempt number.
    #[must_use]
    pub fn delay_for_attempt(self, attempt: u32) -> Duration {
        let exponent = attempt.saturating_sub(1).min(10);
        let multiplier = 1_u32.checked_shl(exponent).unwrap_or(1024);
        self.initial_delay
            .saturating_mul(multiplier)
            .min(self.max_delay)
    }
}

/// Stream subscription.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StreamSubscription {
    topic: String,
    #[serde(rename = "type")]
    kind: StreamSubscriptionType,
}

impl StreamSubscription {
    /// Subscribes to robot message callbacks.
    #[must_use]
    pub fn bot_messages() -> Self {
        Self {
            topic: BOT_MESSAGE_TOPIC.to_string(),
            kind: StreamSubscriptionType::Callback,
        }
    }

    /// Subscribes to interactive card callbacks.
    #[must_use]
    pub fn card_callbacks() -> Self {
        Self {
            topic: CARD_CALLBACK_TOPIC.to_string(),
            kind: StreamSubscriptionType::Callback,
        }
    }

    /// Subscribes to all event topics selected in the DingTalk developer console.
    #[must_use]
    pub fn all_events() -> Self {
        Self {
            topic: "*".to_string(),
            kind: StreamSubscriptionType::Event,
        }
    }

    /// Creates a custom event subscription.
    #[must_use]
    pub fn event(topic: impl Into<String>) -> Self {
        Self::new(StreamSubscriptionType::Event, topic)
    }

    /// Creates a custom callback subscription.
    #[must_use]
    pub fn callback(topic: impl Into<String>) -> Self {
        Self::new(StreamSubscriptionType::Callback, topic)
    }

    /// Creates a custom Stream subscription.
    #[must_use]
    pub fn new(kind: StreamSubscriptionType, topic: impl Into<String>) -> Self {
        let topic = topic.into();
        Self {
            topic: topic.trim().to_string(),
            kind,
        }
    }

    /// Returns the subscribed topic.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the subscription type.
    #[must_use]
    pub fn kind(&self) -> StreamSubscriptionType {
        self.kind
    }

    /// Validates this subscription.
    pub fn validate(&self) -> Result<()> {
        non_empty_trimmed(&self.topic, "subscription.topic")?;
        Ok(())
    }
}

/// Stream subscription type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum StreamSubscriptionType {
    /// Callback topic.
    #[serde(rename = "CALLBACK")]
    Callback,
    /// Event topic.
    #[serde(rename = "EVENT")]
    Event,
}

impl StreamSubscriptionType {
    /// Returns the DingTalk wire value for this subscription type.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Callback => "CALLBACK",
            Self::Event => "EVENT",
        }
    }

    /// Returns whether this is a callback subscription.
    #[must_use]
    pub fn is_callback(self) -> bool {
        matches!(self, Self::Callback)
    }

    /// Returns whether this is an event subscription.
    #[must_use]
    pub fn is_event(self) -> bool {
        matches!(self, Self::Event)
    }
}

impl fmt::Display for StreamSubscriptionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Why a single Stream connection exited.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamExit {
    /// WebSocket closed.
    Closed,
    /// Server sent system disconnect.
    Disconnect,
}

impl StreamExit {
    /// Returns a stable lowercase label for logs and metrics.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::Disconnect => "disconnect",
        }
    }

    /// Returns whether the WebSocket closed normally.
    #[must_use]
    pub fn is_closed(self) -> bool {
        matches!(self, Self::Closed)
    }

    /// Returns whether DingTalk requested a disconnect.
    #[must_use]
    pub fn is_disconnect(self) -> bool {
        matches!(self, Self::Disconnect)
    }
}

impl fmt::Display for StreamExit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Runtime event emitted by [`StreamClient`] while running.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum StreamRunEvent {
    /// A WebSocket connection attempt is starting.
    ConnectionOpening {
        /// One-based connection attempt number.
        attempt: u32,
    },
    /// A WebSocket connection was established.
    ConnectionOpened {
        /// One-based connection attempt number.
        attempt: u32,
    },
    /// A WebSocket connection closed normally.
    ConnectionClosed {
        /// One-based connection attempt number.
        attempt: u32,
        /// Exit reason.
        exit: StreamExit,
    },
    /// A connection-level error occurred.
    ConnectionError {
        /// One-based connection attempt number.
        attempt: u32,
        /// Whether the client will retry this error.
        retrying: bool,
        /// Human-readable error text.
        error: String,
    },
    /// A reconnect sleep was scheduled.
    ReconnectScheduled {
        /// One-based number of the next connection attempt.
        next_attempt: u32,
        /// Delay before reconnecting.
        delay: Duration,
    },
    /// A frame was acknowledged with an internal error.
    FrameError {
        /// DingTalk message id, when the frame could be parsed.
        message_id: Option<String>,
        /// Human-readable error text.
        error: String,
    },
    /// A standard bot message callback frame was routed by the bot router.
    BotEventHandled {
        /// Stream frame message id.
        message_id: String,
        /// Bot routing outcome.
        outcome: HandleOutcome,
        /// Conversation scope parsed from the callback payload.
        conversation_scope: ConversationScope,
        /// Incoming message type parsed from the callback payload.
        message_type: MessageType,
    },
    /// An interactive card callback frame was handled.
    CardCallbackHandled {
        /// Stream frame message id.
        message_id: String,
        /// Card business id when supplied by DingTalk.
        card_biz_id: Option<String>,
        /// Card action when supplied by DingTalk.
        action: Option<String>,
    },
    /// The caller-provided shutdown signal resolved.
    Shutdown,
}

#[derive(Debug, Serialize)]
struct OpenConnectionRequest<'a> {
    #[serde(rename = "clientId")]
    client_id: &'a str,
    #[serde(rename = "clientSecret")]
    client_secret: &'a str,
    #[serde(rename = "localIp", skip_serializing_if = "Option::is_none")]
    local_ip: Option<&'a str>,
    subscriptions: &'a [StreamSubscription],
    #[serde(rename = "ua")]
    user_agent: &'a str,
}

#[derive(Debug, Deserialize)]
struct OpenConnectionResponse {
    endpoint: String,
    ticket: String,
}

impl OpenConnectionResponse {
    fn validate(&self) -> Result<()> {
        non_empty_trimmed(&self.endpoint, "endpoint")?;
        non_empty_trimmed(&self.ticket, "ticket")?;
        Ok(())
    }
}

fn websocket_url(ticket: &OpenConnectionResponse) -> Result<Url> {
    let mut url = Url::parse(&ticket.endpoint)
        .map_err(|source| Error::Stream(format!("invalid stream endpoint: {source}")))?;
    if !matches!(url.scheme(), "ws" | "wss") {
        return Err(Error::Stream(
            "stream endpoint scheme must be ws or wss".to_string(),
        ));
    }
    url.query_pairs_mut().append_pair("ticket", &ticket.ticket);
    Ok(url)
}

/// Incoming DingTalk Stream frame.
#[derive(Debug, Clone)]
pub struct StreamFrame {
    frame_type: StreamFrameType,
    headers: StreamHeaders,
    data: Value,
}

impl StreamFrame {
    /// Parses a raw Stream WebSocket text frame.
    pub fn from_text(text: &str) -> Result<Self> {
        let raw = serde_json::from_str::<RawStreamFrame>(text)?;
        Ok(Self {
            frame_type: StreamFrameType::from_raw(&raw.frame_type),
            headers: raw.headers,
            data: raw.data,
        })
    }

    /// Returns the Stream frame type.
    #[must_use]
    pub fn frame_type(&self) -> &StreamFrameType {
        &self.frame_type
    }

    /// Returns Stream frame headers.
    #[must_use]
    pub fn headers(&self) -> &StreamHeaders {
        &self.headers
    }

    /// Returns the subscribed topic that produced this frame.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.headers.topic
    }

    /// Returns the DingTalk Stream message id.
    #[must_use]
    pub fn message_id(&self) -> &str {
        &self.headers.message_id
    }

    /// Returns the frame content type when DingTalk supplied it.
    #[must_use]
    pub fn content_type(&self) -> Option<&str> {
        self.headers.content_type()
    }

    /// Returns an extra header value by name.
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&Value> {
        self.headers.get(name)
    }

    /// Returns the raw `data` value as supplied by DingTalk.
    #[must_use]
    pub fn data(&self) -> &Value {
        &self.data
    }

    /// Returns `data` as JSON, decoding string-encoded JSON frames when necessary.
    pub fn data_json(&self) -> Result<Value> {
        match &self.data {
            Value::String(data) => Ok(serde_json::from_str(data)?),
            data => Ok(data.clone()),
        }
    }

    /// Decodes `data` into a concrete type, handling string-encoded JSON frames.
    pub fn data_as<T>(&self) -> Result<T>
    where
        T: DeserializeOwned,
    {
        match &self.data {
            Value::String(data) => Ok(serde_json::from_str(data)?),
            data => Ok(serde_json::from_value(data.clone())?),
        }
    }

    /// Returns whether this is the standard app robot message callback frame.
    #[must_use]
    pub fn is_bot_message_callback(&self) -> bool {
        self.frame_type == StreamFrameType::Callback && self.topic() == BOT_MESSAGE_TOPIC
    }

    /// Returns whether this is the standard interactive card callback frame.
    #[must_use]
    pub fn is_card_callback(&self) -> bool {
        self.frame_type == StreamFrameType::Callback && self.topic() == CARD_CALLBACK_TOPIC
    }

    /// Converts a bot message callback frame into a normalized [`BotEvent`].
    ///
    /// Returns `Ok(None)` for other Stream topics.
    pub fn bot_event(&self) -> Result<Option<BotEvent>> {
        if self.is_bot_message_callback() {
            Ok(Some(BotEvent::from_value(self.data_json()?)))
        } else {
            Ok(None)
        }
    }

    /// Converts an interactive card callback frame into a [`CardCallbackEvent`].
    ///
    /// Returns `Ok(None)` for other Stream topics.
    pub fn card_callback(&self) -> Result<Option<CardCallbackEvent>> {
        if self.is_card_callback() {
            Ok(Some(CardCallbackEvent::from_value(self.data_json()?)))
        } else {
            Ok(None)
        }
    }

    fn message_id_from_text(text: &str) -> Option<String> {
        let value = serde_json::from_str::<Value>(text).ok()?;
        value
            .get("headers")?
            .get("messageId")?
            .as_str()
            .filter(|value| !value.trim().is_empty())
            .map(ToOwned::to_owned)
    }
}

/// Interactive card callback event delivered by DingTalk Stream.
#[derive(Debug, Clone, PartialEq)]
pub struct CardCallbackEvent {
    raw: Value,
}

impl CardCallbackEvent {
    /// Creates a card callback event from raw JSON.
    #[must_use]
    pub fn from_value(raw: Value) -> Self {
        Self { raw }
    }

    /// Returns the raw callback payload.
    #[must_use]
    pub fn raw(&self) -> &Value {
        &self.raw
    }

    /// Returns a normalized, strongly typed view of this callback.
    #[must_use]
    pub fn payload(&self) -> CardCallbackPayload {
        CardCallbackPayload::from_value(self.raw.clone())
    }

    /// Returns a top-level callback field by name.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.raw.get(name).or_else(|| {
            self.raw
                .as_object()?
                .iter()
                .find(|(key, _value)| field_name_matches(key, name))
                .map(|(_key, value)| value)
        })
    }

    /// Returns the card business id when DingTalk supplied one.
    #[must_use]
    pub fn card_biz_id(&self) -> Option<&str> {
        self.string_field(&["cardBizId", "card_biz_id", "outTrackId", "out_track_id"])
    }

    /// Returns the card instance id when DingTalk supplied one.
    #[must_use]
    pub fn card_instance_id(&self) -> Option<&str> {
        self.string_field(&["cardInstanceId", "card_instance_id"])
    }

    /// Returns the action name or callback type when DingTalk supplied one.
    #[must_use]
    pub fn action(&self) -> Option<&str> {
        self.string_field(&["action", "actionName", "actionType", "callbackType"])
    }

    /// Returns the open conversation id when DingTalk supplied one.
    #[must_use]
    pub fn open_conversation_id(&self) -> Option<&str> {
        self.string_field(&["openConversationId", "open_conversation_id"])
    }

    /// Returns the operator user id when DingTalk supplied one.
    #[must_use]
    pub fn user_id(&self) -> Option<&str> {
        self.string_field(&["userId", "user_id", "operatorUserId"])
    }

    /// Returns the operator union id when DingTalk supplied one.
    #[must_use]
    pub fn union_id(&self) -> Option<&str> {
        self.string_field(&["unionId", "union_id", "operatorUnionId"])
    }

    /// Returns action value/form data when DingTalk supplied one.
    #[must_use]
    pub fn action_value(&self) -> Option<&Value> {
        self.value_field(&[
            "actionValue",
            "action_value",
            "value",
            "formValue",
            "form_value",
        ])
    }

    fn string_field(&self, names: &[&str]) -> Option<&str> {
        self.value_field(names)
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
    }

    fn value_field(&self, names: &[&str]) -> Option<&Value> {
        names.iter().find_map(|name| self.get(name))
    }
}

/// Strongly typed view of an interactive card callback.
#[derive(Debug, Clone, PartialEq)]
pub struct CardCallbackPayload {
    raw: Value,
    callback_type: Option<String>,
    card_biz_id: Option<String>,
    card_instance_id: Option<String>,
    open_conversation_id: Option<String>,
    operator: CardCallbackOperator,
    content: Option<CardCallbackContent>,
    action: Option<String>,
    action_ids: Vec<String>,
    action_value: CardCallbackActionValue,
}

impl CardCallbackPayload {
    /// Creates a typed payload from raw DingTalk callback JSON.
    #[must_use]
    pub fn from_value(raw: Value) -> Self {
        let content = callback_content(&raw).map(CardCallbackContent::from_value);
        let action_ids = content
            .as_ref()
            .map(|content| content.action_ids().to_vec())
            .unwrap_or_default();
        let action = string_value(&raw, &["action", "actionName", "actionType"])
            .or_else(|| action_ids.first().cloned())
            .or_else(|| string_value(&raw, &["callbackType", "type"]));
        let action_value = callback_action_value(&raw)
            .or_else(|| {
                content
                    .as_ref()
                    .and_then(|content| content.action_value_value())
            })
            .map(CardCallbackActionValue::from_value)
            .unwrap_or_default();

        Self {
            callback_type: string_value(&raw, &["callbackType", "type"]),
            card_biz_id: string_value(
                &raw,
                &["cardBizId", "card_biz_id", "outTrackId", "out_track_id"],
            ),
            card_instance_id: string_value(&raw, &["cardInstanceId", "card_instance_id"]),
            open_conversation_id: string_value(
                &raw,
                &["openConversationId", "open_conversation_id"],
            ),
            operator: CardCallbackOperator::from_value(&raw),
            raw,
            content,
            action,
            action_ids,
            action_value,
        }
    }

    /// Returns the raw callback payload.
    #[must_use]
    pub fn raw(&self) -> &Value {
        &self.raw
    }

    /// Returns DingTalk's callback type when supplied.
    #[must_use]
    pub fn callback_type(&self) -> Option<&str> {
        self.callback_type.as_deref()
    }

    /// Returns the card business id or `outTrackId`.
    #[must_use]
    pub fn card_biz_id(&self) -> Option<&str> {
        self.card_biz_id.as_deref()
    }

    /// Returns the card instance id.
    #[must_use]
    pub fn card_instance_id(&self) -> Option<&str> {
        self.card_instance_id.as_deref()
    }

    /// Returns the open conversation id.
    #[must_use]
    pub fn open_conversation_id(&self) -> Option<&str> {
        self.open_conversation_id.as_deref()
    }

    /// Returns the operator metadata.
    #[must_use]
    pub fn operator(&self) -> &CardCallbackOperator {
        &self.operator
    }

    /// Returns the parsed `content` payload when supplied.
    #[must_use]
    pub fn content(&self) -> Option<&CardCallbackContent> {
        self.content.as_ref()
    }

    /// Returns the action name or primary action id.
    #[must_use]
    pub fn action(&self) -> Option<&str> {
        self.action.as_deref()
    }

    /// Returns all action ids supplied by card private data.
    #[must_use]
    pub fn action_ids(&self) -> &[String] {
        &self.action_ids
    }

    /// Returns normalized action or form values.
    #[must_use]
    pub fn action_value(&self) -> &CardCallbackActionValue {
        &self.action_value
    }
}

/// User metadata for an interactive card callback.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CardCallbackOperator {
    user_id: Option<String>,
    union_id: Option<String>,
    corp_id: Option<String>,
}

impl CardCallbackOperator {
    fn from_value(raw: &Value) -> Self {
        Self {
            user_id: string_value(raw, &["userId", "user_id", "operatorUserId"]),
            union_id: string_value(raw, &["unionId", "union_id", "operatorUnionId"]),
            corp_id: string_value(raw, &["corpId", "corp_id"]),
        }
    }

    /// Returns the DingTalk user id.
    #[must_use]
    pub fn user_id(&self) -> Option<&str> {
        self.user_id.as_deref()
    }

    /// Returns the DingTalk union id.
    #[must_use]
    pub fn union_id(&self) -> Option<&str> {
        self.union_id.as_deref()
    }

    /// Returns the DingTalk corp id.
    #[must_use]
    pub fn corp_id(&self) -> Option<&str> {
        self.corp_id.as_deref()
    }
}

/// Parsed interactive card callback `content`.
#[derive(Debug, Clone, PartialEq)]
pub struct CardCallbackContent {
    raw: Value,
    private_data: Option<CardCallbackPrivateData>,
}

impl CardCallbackContent {
    fn from_value(raw: Value) -> Self {
        let private_data = value_by_names(
            &raw,
            &[
                "cardPrivateData",
                "card_private_data",
                "privateData",
                "private_data",
            ],
        )
        .cloned()
        .map(CardCallbackPrivateData::from_value);

        Self { raw, private_data }
    }

    /// Returns raw parsed content JSON.
    #[must_use]
    pub fn raw(&self) -> &Value {
        &self.raw
    }

    /// Returns private card action data when DingTalk supplied it.
    #[must_use]
    pub fn private_data(&self) -> Option<&CardCallbackPrivateData> {
        self.private_data.as_ref()
    }

    /// Returns all action ids supplied by card private data.
    #[must_use]
    pub fn action_ids(&self) -> &[String] {
        self.private_data
            .as_ref()
            .map(CardCallbackPrivateData::action_ids)
            .unwrap_or(&[])
    }

    /// Returns normalized action or form values from private data.
    #[must_use]
    pub fn action_value(&self) -> CardCallbackActionValue {
        self.action_value_value()
            .map(CardCallbackActionValue::from_value)
            .unwrap_or_default()
    }

    fn action_value_value(&self) -> Option<Value> {
        self.private_data
            .as_ref()
            .and_then(|private_data| private_data.params().as_value().cloned())
            .or_else(|| callback_action_value(&self.raw))
    }
}

/// Parsed private data inside an interactive card callback.
#[derive(Debug, Clone, PartialEq)]
pub struct CardCallbackPrivateData {
    raw: Value,
    action_ids: Vec<String>,
    params: CardCallbackActionValue,
}

impl CardCallbackPrivateData {
    fn from_value(raw: Value) -> Self {
        let action_ids = value_by_names(&raw, &["actionIds", "action_ids"])
            .map(string_vec_from_value)
            .unwrap_or_default();
        let params = value_by_names(&raw, &["params", "formValue", "form_value"])
            .cloned()
            .map(CardCallbackActionValue::from_value)
            .unwrap_or_default();

        Self {
            raw,
            action_ids,
            params,
        }
    }

    /// Returns raw private data JSON.
    #[must_use]
    pub fn raw(&self) -> &Value {
        &self.raw
    }

    /// Returns DingTalk action ids.
    #[must_use]
    pub fn action_ids(&self) -> &[String] {
        &self.action_ids
    }

    /// Returns form parameters or action values.
    #[must_use]
    pub fn params(&self) -> &CardCallbackActionValue {
        &self.params
    }
}

/// Action value or form parameters from an interactive card callback.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CardCallbackActionValue {
    value: Option<Value>,
}

impl CardCallbackActionValue {
    /// Creates action values from raw JSON.
    #[must_use]
    pub fn from_value(value: Value) -> Self {
        Self { value: Some(value) }
    }

    /// Returns whether no action value was supplied.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.value.is_none()
    }

    /// Returns the raw value.
    #[must_use]
    pub fn as_value(&self) -> Option<&Value> {
        self.value.as_ref()
    }

    /// Returns the raw object map when the value is an object.
    #[must_use]
    pub fn as_object(&self) -> Option<&serde_json::Map<String, Value>> {
        self.value.as_ref().and_then(Value::as_object)
    }

    /// Returns a named field from an object action value.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&Value> {
        let object = self.as_object()?;
        object.get(name).or_else(|| {
            object
                .iter()
                .find(|(key, _value)| field_name_matches(key, name))
                .map(|(_key, value)| value)
        })
    }

    /// Returns a named string field from an object action value.
    #[must_use]
    pub fn string(&self, name: &str) -> Option<&str> {
        self.get(name)
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
    }

    /// Deserializes the action value into an application type.
    pub fn deserialize<T>(&self) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        self.value
            .as_ref()
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(Error::from)
    }
}

/// Response payload for interactive card callbacks.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct CardCallbackResponse {
    #[serde(rename = "cardData", skip_serializing_if = "Option::is_none")]
    card_data: Option<CardCallbackResponseData>,
    #[serde(rename = "userPrivateData", skip_serializing_if = "Option::is_none")]
    user_private_data: Option<CardCallbackResponseData>,
}

impl CardCallbackResponse {
    /// Creates an empty callback response.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets card-level parameters to update.
    #[must_use]
    pub fn card_data<I, K, V>(mut self, values: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.card_data = Some(CardCallbackResponseData::new(values));
        self
    }

    /// Sets user-private parameters to update.
    #[must_use]
    pub fn user_private_data<I, K, V>(mut self, values: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.user_private_data = Some(CardCallbackResponseData::new(values));
        self
    }

    /// Converts this value into a Stream ACK response.
    pub fn into_stream_response(self) -> Result<StreamFrameResponse> {
        StreamFrameResponse::json(self)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
struct CardCallbackResponseData {
    #[serde(rename = "cardParamMap")]
    card_param_map: BTreeMap<String, String>,
}

impl CardCallbackResponseData {
    fn new<I, K, V>(values: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        Self {
            card_param_map: values
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        }
    }
}

fn callback_content(raw: &Value) -> Option<Value> {
    let value = value_by_names(raw, &["content"])?;
    match value {
        Value::String(text) => serde_json::from_str::<Value>(text).ok(),
        other => Some(other.clone()),
    }
}

fn callback_action_value(raw: &Value) -> Option<Value> {
    value_by_names(
        raw,
        &[
            "actionValue",
            "action_value",
            "value",
            "formValue",
            "form_value",
        ],
    )
    .cloned()
}

fn value_by_names<'a>(raw: &'a Value, names: &[&str]) -> Option<&'a Value> {
    names.iter().find_map(|name| {
        raw.get(name).or_else(|| {
            raw.as_object()?
                .iter()
                .find(|(key, _value)| field_name_matches(key, name))
                .map(|(_key, value)| value)
        })
    })
}

fn string_value(raw: &Value, names: &[&str]) -> Option<String> {
    value_by_names(raw, names)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn string_vec_from_value(value: &Value) -> Vec<String> {
    match value {
        Value::Array(values) => values
            .iter()
            .filter_map(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect(),
        Value::String(text) => serde_json::from_str::<Vec<String>>(text)
            .unwrap_or_else(|_| vec![text.trim().to_string()])
            .into_iter()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .collect(),
        _ => Vec::new(),
    }
}

fn field_name_matches(left: &str, right: &str) -> bool {
    fn normalized(value: &str) -> impl Iterator<Item = char> + '_ {
        value
            .chars()
            .filter(|ch| !matches!(ch, '-' | '_'))
            .flat_map(char::to_lowercase)
    }

    normalized(left).eq(normalized(right))
}

#[derive(Debug, Deserialize)]
struct RawStreamFrame {
    #[serde(rename = "type")]
    frame_type: String,
    headers: StreamHeaders,
    data: Value,
}

/// DingTalk Stream frame headers.
#[derive(Debug, Clone, Deserialize)]
pub struct StreamHeaders {
    topic: String,
    #[serde(rename = "messageId")]
    message_id: String,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

impl StreamHeaders {
    /// Returns the subscribed topic that produced this frame.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the DingTalk Stream message id.
    #[must_use]
    pub fn message_id(&self) -> &str {
        &self.message_id
    }

    /// Returns the frame content type when DingTalk supplied it.
    #[must_use]
    pub fn content_type(&self) -> Option<&str> {
        self.get("contentType")
            .or_else(|| self.get("content-type"))
            .and_then(Value::as_str)
    }

    /// Returns an extra header value by name.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.extra.get(name).or_else(|| {
            self.extra
                .iter()
                .find(|(key, _value)| header_name_matches(key, name))
                .map(|(_key, value)| value)
        })
    }

    /// Returns all extra header values not modeled as first-class fields.
    #[must_use]
    pub fn extra(&self) -> &BTreeMap<String, Value> {
        &self.extra
    }
}

fn header_name_matches(left: &str, right: &str) -> bool {
    fn normalized(value: &str) -> impl Iterator<Item = char> + '_ {
        value
            .chars()
            .filter(|ch| !matches!(ch, '-' | '_'))
            .flat_map(char::to_lowercase)
    }

    normalized(left).eq(normalized(right))
}

/// DingTalk Stream frame type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamFrameType {
    /// System frame such as `ping` or `disconnect`.
    System,
    /// Callback frame.
    Callback,
    /// Event frame.
    Event,
    /// Unknown or future frame type.
    Unknown(String),
}

impl StreamFrameType {
    fn from_raw(value: &str) -> Self {
        match value.trim().to_ascii_uppercase().as_str() {
            "SYSTEM" => Self::System,
            "CALLBACK" => Self::Callback,
            "EVENT" => Self::Event,
            _ => Self::Unknown(value.to_string()),
        }
    }

    /// Returns the DingTalk wire value for known frame types.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::System => "SYSTEM",
            Self::Callback => "CALLBACK",
            Self::Event => "EVENT",
            Self::Unknown(value) => value.as_str(),
        }
    }

    /// Returns whether this is a system frame.
    #[must_use]
    pub fn is_system(&self) -> bool {
        matches!(self, Self::System)
    }

    /// Returns whether this is a callback frame.
    #[must_use]
    pub fn is_callback(&self) -> bool {
        matches!(self, Self::Callback)
    }

    /// Returns whether this is an event frame.
    #[must_use]
    pub fn is_event(&self) -> bool {
        matches!(self, Self::Event)
    }

    /// Returns whether this is an unknown DingTalk Stream frame type.
    #[must_use]
    pub fn is_unknown(&self) -> bool {
        matches!(self, Self::Unknown(_))
    }
}

impl fmt::Display for StreamFrameType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Response payload returned from a custom Stream frame handler.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamFrameResponse {
    value: Value,
}

impl StreamFrameResponse {
    /// Creates an empty `{"response": null}` ACK payload.
    #[must_use]
    pub fn empty() -> Self {
        Self { value: Value::Null }
    }

    /// Creates a response from a raw JSON value.
    #[must_use]
    pub fn from_value(value: Value) -> Self {
        Self { value }
    }

    /// Creates a response from a serializable value.
    pub fn json<T>(value: T) -> Result<Self>
    where
        T: Serialize,
    {
        Ok(Self {
            value: serde_json::to_value(value)?,
        })
    }

    /// Returns the raw response value.
    #[must_use]
    pub fn as_value(&self) -> &Value {
        &self.value
    }

    /// Consumes this response and returns the raw JSON value.
    #[must_use]
    pub fn into_value(self) -> Value {
        self.value
    }
}

impl Default for StreamFrameResponse {
    fn default() -> Self {
        Self::empty()
    }
}

#[derive(Debug, Serialize)]
struct StreamAck {
    code: u16,
    message: &'static str,
    headers: StreamAckHeaders,
    data: String,
}

impl StreamAck {
    fn ok(message_id: String, data: StreamAckData) -> Self {
        Self {
            code: 200,
            message: "OK",
            headers: StreamAckHeaders::new(message_id),
            data: data.to_json_string(),
        }
    }

    fn not_found(message_id: String) -> Self {
        Self {
            code: 404,
            message: "topic not found",
            headers: StreamAckHeaders::new(message_id),
            data: r#"{"response":null}"#.to_string(),
        }
    }

    fn disconnect(message_id: String) -> Self {
        Self {
            code: 200,
            message: "OK",
            headers: StreamAckHeaders::new(message_id),
            data: r#"{"response":null}"#.to_string(),
        }
    }

    fn internal_error(message_id: String) -> Self {
        Self {
            code: 500,
            message: "internal error",
            headers: StreamAckHeaders::new(message_id),
            data: r#"{"response":null}"#.to_string(),
        }
    }
}

#[derive(Debug, Serialize)]
struct StreamAckHeaders {
    #[serde(rename = "messageId")]
    message_id: String,
    #[serde(rename = "contentType")]
    content_type: &'static str,
}

impl StreamAckHeaders {
    fn new(message_id: String) -> Self {
        Self {
            message_id,
            content_type: "application/json",
        }
    }
}

enum StreamAckData {
    Response(Value),
    Raw(Value),
}

impl StreamAckData {
    fn to_json_string(&self) -> String {
        match self {
            Self::Response(value) => serde_json::to_string(&serde_json::json!({
                "response": value,
            }))
            .unwrap_or_else(|_error| r#"{"response":null}"#.to_string()),
            Self::Raw(value) => {
                serde_json::to_string(value).unwrap_or_else(|_error| "{}".to_string())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    #[test]
    fn websocket_url_appends_ticket() {
        let url = websocket_url(&OpenConnectionResponse {
            endpoint: "wss://example.com/connect".to_string(),
            ticket: "ticket-1".to_string(),
        })
        .expect("url");

        assert_eq!(url.as_str(), "wss://example.com/connect?ticket=ticket-1");
    }

    #[test]
    fn stream_ack_serializes_message_id() {
        let ack = StreamAck::ok(
            "message-1".to_string(),
            StreamAckData::Response(Value::Null),
        );
        let value = serde_json::to_value(ack).expect("json");

        assert_eq!(value["code"], 200);
        assert_eq!(value["headers"]["messageId"], "message-1");
        assert_eq!(value["data"], r#"{"response":null}"#);
    }

    #[test]
    fn stream_internal_error_ack_preserves_message_id() {
        let ack = StreamAck::internal_error("message-1".to_string());
        let value = serde_json::to_value(ack).expect("json");

        assert_eq!(value["code"], 500);
        assert_eq!(value["headers"]["messageId"], "message-1");
        assert_eq!(value["data"], r#"{"response":null}"#);
    }

    #[tokio::test]
    async fn invalid_bot_callback_data_ack_preserves_message_id() {
        let client = DingTalk::builder()
            .app_key_and_secret("client-id", "client-secret")
            .build()
            .expect("client");
        let bot = Bot::new(client.clone());
        let stream = StreamClient::builder(client)
            .expect("builder")
            .bot(bot)
            .build()
            .expect("stream");

        let handled = stream
            .handle_text_frame(
                r#"{
                    "specVersion":"1.0",
                    "type":"CALLBACK",
                    "headers":{
                        "topic":"/v1.0/im/bot/messages/get",
                        "messageId":"message-1",
                        "contentType":"application/json"
                    },
                    "data":"not-json"
                }"#,
            )
            .await
            .expect("handled");
        let value = serde_json::to_value(handled.ack).expect("json");

        assert_eq!(value["code"], 500);
        assert_eq!(value["headers"]["messageId"], "message-1");
        assert!(matches!(
            handled.error,
            Some(StreamFrameError {
                message_id: Some(message_id),
                ..
            }) if message_id == "message-1"
        ));
    }

    #[tokio::test]
    async fn malformed_stream_frame_ack_preserves_message_id() {
        let client = DingTalk::builder()
            .app_key_and_secret("client-id", "client-secret")
            .build()
            .expect("client");
        let bot = Bot::new(client.clone());
        let stream = StreamClient::builder(client)
            .expect("builder")
            .bot(bot)
            .build()
            .expect("stream");

        let handled = stream
            .handle_text_frame(
                r#"{
                    "specVersion":"1.0",
                    "type":"CALLBACK",
                    "headers":{
                        "topic":"/v1.0/im/bot/messages/get",
                        "messageId":"message-1",
                        "contentType":"application/json"
                    }
                }"#,
            )
            .await
            .expect("handled");
        let value = serde_json::to_value(handled.ack).expect("json");

        assert_eq!(value["code"], 500);
        assert_eq!(value["headers"]["messageId"], "message-1");
        assert!(matches!(
            handled.error,
            Some(StreamFrameError {
                message_id: Some(message_id),
                ..
            }) if message_id == "message-1"
        ));
    }

    #[test]
    fn reconnect_policy_uses_initial_delay_for_first_retry() {
        let policy = ReconnectPolicy::new(Duration::from_secs(2), Duration::from_secs(30));

        assert_eq!(policy.delay_for_attempt(0), Duration::from_secs(2));
        assert_eq!(policy.delay_for_attempt(1), Duration::from_secs(2));
        assert_eq!(policy.delay_for_attempt(2), Duration::from_secs(4));
        assert_eq!(policy.delay_for_attempt(10), Duration::from_secs(30));
    }

    #[test]
    fn reconnect_policy_builder_helpers_are_validated() {
        let policy = ReconnectPolicy::no_retry()
            .initial_delay(Duration::from_secs(3))
            .max_delay(Duration::from_secs(10));

        assert!(!policy.retry_on_error);
        assert_eq!(policy.delay_for_attempt(2), Duration::from_secs(6));
        assert!(policy.validate().is_ok());

        let invalid = policy.max_delay(Duration::from_secs(1));
        let error = invalid.validate().expect_err("invalid policy should fail");
        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn stream_subscription_builders_set_kind_and_topic() {
        let event = StreamSubscription::event(" /v1.0/example/events ");
        let callback =
            StreamSubscription::new(StreamSubscriptionType::Callback, "/v1.0/example/callbacks");
        let card = StreamSubscription::card_callbacks();

        assert_eq!(event.topic(), "/v1.0/example/events");
        assert_eq!(event.kind(), StreamSubscriptionType::Event);
        assert_eq!(event.kind().as_str(), "EVENT");
        assert_eq!(event.kind().to_string(), "EVENT");
        assert!(event.kind().is_event());
        assert_eq!(callback.kind(), StreamSubscriptionType::Callback);
        assert_eq!(callback.kind().as_str(), "CALLBACK");
        assert!(callback.kind().is_callback());
        assert_eq!(card.topic(), CARD_CALLBACK_TOPIC);
        assert_eq!(card.kind(), StreamSubscriptionType::Callback);
        assert!(event.validate().is_ok());
    }

    #[test]
    fn stream_exit_exposes_stable_labels() {
        assert_eq!(StreamExit::Closed.as_str(), "closed");
        assert_eq!(StreamExit::Closed.to_string(), "closed");
        assert_eq!(StreamExit::Disconnect.as_str(), "disconnect");
        assert!(StreamExit::Closed.is_closed());
        assert!(StreamExit::Disconnect.is_disconnect());
        assert!(!StreamExit::Closed.is_disconnect());
    }

    #[test]
    fn stream_builder_rejects_empty_subscription_topic() {
        let client = DingTalk::builder()
            .app_key_and_secret("client-id", "client-secret")
            .build()
            .expect("client");
        let bot = Bot::new(client.clone());

        let result = StreamClient::builder(client)
            .expect("builder")
            .subscriptions(vec![StreamSubscription::callback(" ")])
            .bot(bot)
            .build();
        let Err(error) = result else {
            panic!("empty topic should fail");
        };

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn stream_builder_requires_bot_or_frame_handler() {
        let client = DingTalk::builder()
            .app_key_and_secret("client-id", "client-secret")
            .build()
            .expect("client");

        let result = StreamClient::builder(client).expect("builder").build();
        let Err(error) = result else {
            panic!("handler should be required");
        };

        assert_eq!(error.kind(), crate::ErrorKind::InvalidConfig);
    }

    #[test]
    fn stream_bot_builder_builds_from_credentials() {
        let result = StreamBot::builder()
            .client_id_and_secret("client-id", "client-secret")
            .route(Route::new(ConversationScope::Any))
            .build();

        assert!(result.is_ok());
    }

    #[test]
    fn stream_bot_builder_registers_route_shortcuts() {
        let result = StreamBot::builder()
            .client_id_and_secret("client-id", "client-secret")
            .on_group_text_command("/ping", |_ctx, _event| async { Ok(()) })
            .on_private_text_commands(["/help", "help"], |_ctx, _event| async { Ok(()) })
            .on_private_message(MessageType::Picture, |_ctx, _event| async { Ok(()) })
            .build();

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn stream_bot_emits_bot_event_handled() {
        let seen = Arc::new(Mutex::new(Vec::<StreamRunEvent>::new()));
        let seen_events = Arc::clone(&seen);
        let stream_bot = StreamBot::builder()
            .client_id_and_secret("client-id", "client-secret")
            .on_group_text_command("/ping", |_ctx, _event| async { Ok(()) })
            .on_event(move |event| {
                seen_events.lock().expect("event lock").push(event);
            })
            .build()
            .expect("stream bot");

        let handled = stream_bot
            .client
            .handle_text_frame(
                r#"{
                    "specVersion":"1.0",
                    "type":"CALLBACK",
                    "headers":{
                        "topic":"/v1.0/im/bot/messages/get",
                        "messageId":"message-1",
                        "contentType":"application/json"
                    },
                    "data":"{\"conversationType\":\"2\",\"msgtype\":\"text\",\"text\":{\"content\":\"/ping\"}}"
                }"#,
            )
            .await
            .expect("handled");
        let value = serde_json::to_value(handled.ack).expect("json");

        assert_eq!(value["code"], 200);
        assert_eq!(
            seen.lock().expect("event lock").as_slice(),
            [StreamRunEvent::BotEventHandled {
                message_id: "message-1".to_string(),
                outcome: HandleOutcome::Matched,
                conversation_scope: ConversationScope::Group,
                message_type: MessageType::Text,
            }]
        );
    }

    #[test]
    fn stream_bot_builder_forwards_frame_handler() {
        let result = StreamBot::builder()
            .client_id_and_secret("client-id", "client-secret")
            .subscription(StreamSubscription::event("/v1.0/example/events"))
            .on_frame(|_frame| async { Ok(()) })
            .build();

        assert!(result.is_ok());
    }

    #[test]
    fn stream_bot_builder_forwards_response_frame_handler() {
        let result = StreamBot::builder()
            .client_id_and_secret("client-id", "client-secret")
            .subscription(StreamSubscription::event("/v1.0/example/events"))
            .on_frame_with_response(|_frame| async {
                StreamFrameResponse::json(serde_json::json!({ "accepted": true }))
            })
            .build();

        assert!(result.is_ok());
    }

    #[test]
    fn stream_bot_builder_registers_card_callback_handler() {
        let stream = StreamBot::builder()
            .client_id_and_secret("client-id", "client-secret")
            .on_card_callback(|_event| async { Ok(()) })
            .build()
            .expect("stream bot");

        assert!(
            stream
                .client
                .subscriptions
                .iter()
                .any(|subscription| subscription.topic() == CARD_CALLBACK_TOPIC)
        );
    }

    #[tokio::test]
    async fn stream_client_handles_card_callbacks() {
        let client = DingTalk::builder()
            .app_key_and_secret("client-id", "client-secret")
            .build()
            .expect("client");
        let seen = Arc::new(Mutex::new(None::<(String, String, Value)>));
        let seen_event = Arc::clone(&seen);
        let events = Arc::new(Mutex::new(Vec::<StreamRunEvent>::new()));
        let seen_events = Arc::clone(&events);
        let stream = StreamClient::builder(client)
            .expect("builder")
            .on_card_callback(move |event| {
                let seen_event = Arc::clone(&seen_event);
                async move {
                    *seen_event.lock().expect("card lock") = Some((
                        event.card_biz_id().unwrap_or_default().to_string(),
                        event.action().unwrap_or_default().to_string(),
                        event.action_value().cloned().unwrap_or(Value::Null),
                    ));
                    Ok(())
                }
            })
            .on_event(move |event| {
                seen_events.lock().expect("event lock").push(event);
            })
            .build()
            .expect("stream");

        let handled = stream
            .handle_text_frame(
                r#"{
                    "specVersion":"1.0",
                    "type":"CALLBACK",
                    "headers":{
                        "topic":"/v1.0/card/instances/callback",
                        "messageId":"message-1",
                        "contentType":"application/json"
                    },
                    "data":{
                        "cardBizId":"card-biz-id",
                        "action":"approve",
                        "actionValue":{"ok":true},
                        "openConversationId":"cid",
                        "userId":"user-1"
                    }
                }"#,
            )
            .await
            .expect("handled");
        let value = serde_json::to_value(handled.ack).expect("json");

        assert_eq!(value["code"], 200);
        assert_eq!(
            seen.lock().expect("card lock").as_ref(),
            Some(&(
                "card-biz-id".to_string(),
                "approve".to_string(),
                serde_json::json!({ "ok": true })
            ))
        );
        assert_eq!(
            events.lock().expect("event lock").as_slice(),
            [StreamRunEvent::CardCallbackHandled {
                message_id: "message-1".to_string(),
                card_biz_id: Some("card-biz-id".to_string()),
                action: Some("approve".to_string()),
            }]
        );
    }

    #[test]
    fn card_callback_payload_parses_content_private_data() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Form {
            env: String,
            approved: String,
        }

        let event = CardCallbackEvent::from_value(serde_json::json!({
            "type": "CALLBACK",
            "outTrackId": "card-biz-id",
            "corpId": "corp-1",
            "userId": "user-1",
            "content": {
                "cardPrivateData": {
                    "actionIds": ["approve"],
                    "params": {
                        "env": "prod",
                        "approved": "true"
                    }
                }
            }
        }));
        let payload = event.payload();

        assert_eq!(payload.callback_type(), Some("CALLBACK"));
        assert_eq!(payload.card_biz_id(), Some("card-biz-id"));
        assert_eq!(payload.operator().corp_id(), Some("corp-1"));
        assert_eq!(payload.operator().user_id(), Some("user-1"));
        assert_eq!(payload.action(), Some("approve"));
        assert_eq!(payload.action_ids(), ["approve".to_string()]);
        assert_eq!(payload.action_value().string("env"), Some("prod"));
        assert_eq!(
            payload
                .action_value()
                .deserialize::<Form>()
                .expect("typed form"),
            Some(Form {
                env: "prod".to_string(),
                approved: "true".to_string()
            })
        );
    }

    #[test]
    fn card_callback_payload_parses_json_string_content() {
        let event = CardCallbackEvent::from_value(serde_json::json!({
            "cardBizId": "card-biz-id",
            "content": "{\"cardPrivateData\":{\"actionIds\":\"[\\\"save\\\"]\",\"params\":{\"field\":\"value\"}}}"
        }));
        let payload = event.payload();

        assert_eq!(payload.card_biz_id(), Some("card-biz-id"));
        assert_eq!(payload.action(), Some("save"));
        assert_eq!(payload.action_ids(), ["save".to_string()]);
        assert_eq!(payload.action_value().string("field"), Some("value"));
    }

    #[test]
    fn card_callback_response_serializes_card_param_maps() {
        let response = CardCallbackResponse::new()
            .card_data([("status", "done")])
            .user_private_data([("clicked", "true")])
            .into_stream_response()
            .expect("response");

        assert_eq!(
            response.as_value()["cardData"]["cardParamMap"]["status"],
            "done"
        );
        assert_eq!(
            response.as_value()["userPrivateData"]["cardParamMap"]["clicked"],
            "true"
        );
    }

    #[tokio::test]
    async fn stream_client_handles_custom_event_frames() {
        let client = DingTalk::builder()
            .app_key_and_secret("client-id", "client-secret")
            .build()
            .expect("client");
        let seen = Arc::new(Mutex::new(None::<(StreamFrameType, String, Value)>));
        let seen_frame = Arc::clone(&seen);
        let stream = StreamClient::builder(client)
            .expect("builder")
            .subscriptions(vec![StreamSubscription::event("/v1.0/example/events")])
            .on_frame(move |frame| {
                let seen_frame = Arc::clone(&seen_frame);
                async move {
                    let data = frame.data_json()?;
                    *seen_frame.lock().expect("frame lock") =
                        Some((frame.frame_type().clone(), frame.topic().to_string(), data));
                    Ok(())
                }
            })
            .build()
            .expect("stream");

        let handled = stream
            .handle_text_frame(
                r#"{
                    "specVersion":"1.0",
                    "type":"EVENT",
                    "headers":{
                        "topic":"/v1.0/example/events",
                        "messageId":"message-1",
                        "contentType":"application/json"
                    },
                    "data":{
                        "eventId":"event-1"
                    }
                }"#,
            )
            .await
            .expect("handled");
        let value = serde_json::to_value(handled.ack).expect("json");

        assert_eq!(value["code"], 200);
        assert_eq!(value["headers"]["messageId"], "message-1");
        assert_eq!(value["data"], r#"{"response":null}"#);
        assert_eq!(
            seen.lock().expect("frame lock").as_ref(),
            Some(&(
                StreamFrameType::Event,
                "/v1.0/example/events".to_string(),
                serde_json::json!({ "eventId": "event-1" })
            ))
        );
    }

    #[tokio::test]
    async fn stream_client_custom_frame_handler_can_return_response() {
        let client = DingTalk::builder()
            .app_key_and_secret("client-id", "client-secret")
            .build()
            .expect("client");
        let stream = StreamClient::builder(client)
            .expect("builder")
            .subscriptions(vec![StreamSubscription::event("/v1.0/example/events")])
            .on_frame_with_response(|frame| async move {
                let data = frame.data_json()?;
                StreamFrameResponse::json(serde_json::json!({
                    "topic": frame.topic(),
                    "eventId": data["eventId"],
                }))
            })
            .build()
            .expect("stream");

        let handled = stream
            .handle_text_frame(
                r#"{
                    "specVersion":"1.0",
                    "type":"EVENT",
                    "headers":{
                        "topic":"/v1.0/example/events",
                        "messageId":"message-1",
                        "contentType":"application/json"
                    },
                    "data":{
                        "eventId":"event-1"
                    }
                }"#,
            )
            .await
            .expect("handled");
        let value = serde_json::to_value(handled.ack).expect("json");
        let data: Value =
            serde_json::from_str(value["data"].as_str().expect("data string")).expect("data json");

        assert_eq!(value["code"], 200);
        assert_eq!(data["response"]["topic"], "/v1.0/example/events");
        assert_eq!(data["response"]["eventId"], "event-1");
    }

    #[tokio::test]
    async fn bot_message_frame_without_bot_can_use_frame_handler() {
        let client = DingTalk::builder()
            .app_key_and_secret("client-id", "client-secret")
            .build()
            .expect("client");
        let seen = Arc::new(Mutex::new(None::<String>));
        let seen_topic = Arc::clone(&seen);
        let stream = StreamClient::builder(client)
            .expect("builder")
            .on_frame(move |frame| {
                let seen_topic = Arc::clone(&seen_topic);
                async move {
                    *seen_topic.lock().expect("topic lock") = Some(frame.topic().to_string());
                    Ok(())
                }
            })
            .build()
            .expect("stream");

        let handled = stream
            .handle_text_frame(
                r#"{
                    "specVersion":"1.0",
                    "type":"CALLBACK",
                    "headers":{
                        "topic":"/v1.0/im/bot/messages/get",
                        "messageId":"message-1",
                        "contentType":"application/json"
                    },
                    "data":"{\"conversationType\":\"2\",\"msgtype\":\"text\",\"text\":{\"content\":\"/ping\"}}"
                }"#,
            )
            .await
            .expect("handled");
        let value = serde_json::to_value(handled.ack).expect("json");

        assert_eq!(value["code"], 200);
        assert_eq!(
            seen.lock().expect("topic lock").as_deref(),
            Some(BOT_MESSAGE_TOPIC)
        );
    }

    #[test]
    fn stream_frame_parses_bot_callback() {
        let frame = StreamFrame::from_text(
            r#"{
                "specVersion":"1.0",
                "type":"CALLBACK",
                "headers":{
                    "topic":"/v1.0/im/bot/messages/get",
                    "messageId":"message-1",
                    "contentType":"application/json"
                },
                "data":"{\"conversationType\":\"2\",\"msgtype\":\"text\",\"text\":{\"content\":\"/ping\"}}"
            }"#,
        )
        .expect("frame");

        assert_eq!(frame.headers.topic, BOT_MESSAGE_TOPIC);
        assert!(matches!(frame.frame_type, StreamFrameType::Callback));
        assert_eq!(frame.data_json().expect("data")["msgtype"], "text");
        assert!(frame.is_bot_message_callback());
        let event = frame
            .bot_event()
            .expect("bot event")
            .expect("bot message topic");
        assert_eq!(event.conversation_scope, ConversationScope::Group);
        assert_eq!(
            event.text.as_ref().map(|text| text.content.as_str()),
            Some("/ping")
        );
    }

    #[test]
    fn stream_frame_exposes_accessors_and_preserves_unknown_type() {
        let frame = StreamFrame::from_text(
            r#"{
                "specVersion":"1.0",
                "type":"future",
                "headers":{
                    "topic":"/v1.0/example/future",
                    "messageId":"message-1",
                    "contentType":"application/json"
                },
                "data":"{\"ok\":true}"
            }"#,
        )
        .expect("frame");

        assert_eq!(frame.frame_type().as_str(), "future");
        assert_eq!(frame.frame_type().to_string(), "future");
        assert!(frame.frame_type().is_unknown());
        assert!(!frame.frame_type().is_callback());
        assert!(StreamFrameType::System.is_system());
        assert!(StreamFrameType::Callback.is_callback());
        assert!(StreamFrameType::Event.is_event());
        assert_eq!(frame.topic(), "/v1.0/example/future");
        assert_eq!(frame.message_id(), "message-1");
        assert_eq!(frame.headers().topic(), "/v1.0/example/future");
        assert_eq!(frame.headers().message_id(), "message-1");
        assert_eq!(frame.content_type(), Some("application/json"));
        assert_eq!(
            frame.header("content-type").and_then(Value::as_str),
            Some("application/json")
        );
        assert_eq!(
            frame.headers().get("CONTENTTYPE").and_then(Value::as_str),
            Some("application/json")
        );
        assert!(frame.headers().extra().contains_key("contentType"));
        assert_eq!(frame.data_json().expect("data")["ok"], true);
        assert!(!frame.is_bot_message_callback());
        assert!(frame.bot_event().expect("bot event").is_none());
    }

    #[test]
    fn stream_frame_decodes_typed_data() {
        #[derive(Debug, serde::Deserialize, PartialEq)]
        struct EventPayload {
            #[serde(rename = "eventId")]
            event_id: String,
        }

        let frame = StreamFrame::from_text(
            r#"{
                "specVersion":"1.0",
                "type":"EVENT",
                "headers":{
                    "topic":"/v1.0/example/events",
                    "messageId":"message-1",
                    "contentType":"application/json"
                },
                "data":"{\"eventId\":\"event-1\"}"
            }"#,
        )
        .expect("frame");

        let payload: EventPayload = frame.data_as().expect("typed payload");

        assert_eq!(
            payload,
            EventPayload {
                event_id: "event-1".to_string()
            }
        );
    }

    #[test]
    fn stream_frame_response_wraps_json_values() {
        let empty = StreamFrameResponse::default();
        let response =
            StreamFrameResponse::json(serde_json::json!({ "ok": true })).expect("response");

        assert_eq!(empty.as_value(), &Value::Null);
        assert_eq!(response.as_value()["ok"], true);
        assert_eq!(
            StreamFrameResponse::from_value(serde_json::json!("pong")).into_value(),
            serde_json::json!("pong")
        );
    }

    #[test]
    fn stream_frame_accepts_json_object_data() {
        let frame = StreamFrame::from_text(
            r#"{
                "specVersion":"1.0",
                "type":"CALLBACK",
                "headers":{
                    "topic":"/v1.0/im/bot/messages/get",
                    "messageId":"message-1",
                    "contentType":"application/json"
                },
                "data":{
                    "conversationType":"2",
                    "msgtype":"text",
                    "text":{"content":"/ping"}
                }
            }"#,
        )
        .expect("frame");

        assert_eq!(frame.headers.topic, BOT_MESSAGE_TOPIC);
        assert!(matches!(frame.frame_type, StreamFrameType::Callback));
        assert_eq!(frame.data_json().expect("data")["msgtype"], "text");
    }
}
