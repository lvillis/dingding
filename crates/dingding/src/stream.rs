//! Receive robot messages, interactive card callbacks, and other Stream frames.
//!
//! [`StreamBot`] combines routing and a connection. [`StreamClient`] accepts an
//! independently configured [`Bot`] for applications that need separate transports
//! or lower-level subscriptions. Both support [`StreamProcessingPolicy`].
//!
//! # Context and state
//!
//! Frame and card handlers receive `(StreamContext, event)`. Return `()` for an
//! empty acknowledgement, [`StreamFrameResponse`] for a raw payload, or
//! [`CardCallbackResponse`] for a validated card update. Any can be wrapped in an
//! SDK or application result. An empty `CardCallbackResponse` is invalid; use `()`
//! when acknowledging without an update.
//!
//! ```
//! use dingding::prelude::*;
//!
//! fn configure(client: DingTalk) -> StreamBotBuilder {
//!     StreamBot::from_client(client)
//!         .state(String::from("done"))
//!         .on_group_text_command("/ping", |ctx| handler_future(async move {
//!             let text = String::from_utf8(b"pong".to_vec())?;
//!             ctx.reply_text(text).await?;
//!             Ok(())
//!         }))
//!         .on_card_callback(|ctx, _| async move {
//!             CardCallbackResponse::new().card_data([
//!                 ("status", ctx.state_required::<String>()?),
//!             ])
//!         })
//! }
//! ```
//!
//! All business handlers share the same state allocation, even in card-only or
//! frame-only applications. [`StreamClientBuilder`] inherits a supplied Bot's
//! state; an explicit [`StreamClientBuilder::state`] overrides it for all handlers
//! regardless of setter order. Other clones of the supplied Bot keep their state.
//!
//! Explicit builder credentials override client defaults for the connection and
//! every handler's SDK client, including a separately supplied Bot. Transports,
//! base URLs, and proxy settings are preserved; other client clones are unchanged.
//! [`StreamContext::client`] uses the connection's SDK transport, while a supplied
//! Bot keeps its own. Use [`DingTalk::with_app_credentials`] to configure an
//! independently used client clone.
//!
//! # Diagnostics
//!
//! Stream emits structured `tracing` events under the `dingding::stream` target:
//! connection and frame/handler failures at WARN, connection lifecycle at INFO,
//! and attempts, reconnect scheduling, and handled events at DEBUG. Error messages
//! are redacted; message bodies, card contents, and original error source chains
//! are not logged. Install a subscriber in the application to collect these events.
//! The library never installs a global subscriber or writes directly to stdout/stderr.
//! [`StreamRunEvent::ConnectionError`] and [`StreamRunEvent::FrameError`] contain a
//! [`StreamError`] with `kind`, HTTP `status`, `errcode`, `api_code`, `request_id`,
//! `retry_after`, and retryability accessors. Missing metadata is `None`. Text fields
//! are redacted; response bodies and original sources are not included.
//!
//! [`StreamBotBuilder::on_event`] and [`StreamClientBuilder::on_event`] are
//! optional observers and do not disable diagnostics. Avoid logging the same
//! events twice. Handler errors produce failure ACKs without stopping the runner;
//! connection errors retry by default. Neither automatically sends an error message
//! to the conversation.
//!
//! Observers can request shutdown through an application-owned signal, for example
//! to stop reconnecting on an authentication error:
//!
//! ```no_run
//! use dingding::prelude::*;
//! # async fn run() -> Result<()> {
//! let (stop, mut stopped) = tokio::sync::watch::channel(false);
//! StreamBot::from_env()?
//!     .on_text_command(Scope::Any, "/ping", |ctx| async move {
//!         ctx.reply_text("pong").await
//!     })
//!     .on_event(move |event| {
//!         if let StreamRunEvent::ConnectionError { error, .. } = event
//!             && matches!(error.status(), Some(401 | 403))
//!         {
//!             let _ = stop.send(true);
//!         }
//!     })
//!     .run_until(async move { let _ = stopped.wait_for(|stop| *stop).await; })
//!     .await
//! # }
//! ```
//!
//! # Processing and shutdown
//!
//! Defaults: one concurrent handler, 64 waiting business frames, a 30-second
//! handler timeout, 5-second write timeout, and separate 30-second shutdown and
//! disconnect drain deadlines.
//! Concurrency greater than one allows out-of-order completion. Heartbeats and
//! ACK writes continue while asynchronous handlers run. Overload, handler errors,
//! panics, and timeouts produce failure ACKs. Handler deadlines also cover
//! deduplication storage operations.
//!
//! [`StreamBot::run_until`] and [`StreamClient::run_until`] stop accepting business
//! work when the shutdown future resolves, then drain accepted handlers and ACKs.
//! Work exceeding the shutdown deadline is cancelled and an error is returned.
//! Server disconnects also stop accepting business work and drain accepted frames,
//! including queued work, within [`StreamProcessingPolicy::disconnect_timeout`].
//! The same policy applies to WebSocket close, receive failure, and write failure.
//! After transport loss, local processing and deduplication completion continue,
//! but ACKs cannot be delivered. Reconnection starts only after drain finishes.
//! If shutdown overlaps disconnect, the earlier deadline wins; further disconnects
//! do not extend it. Per-handler deadlines still apply during drain.
//!
//! [`StreamRunEvent::FrameCancelled`] identifies every cancelled accepted frame and
//! its [`StreamCancellationReason`], including queued work that never started.
//! Handler timeouts also emit this event. Dropping the run future directly cancels
//! work without draining and emits `RunnerDropped` events. Process termination does
//! not guarantee notifications. Deadlines are cooperative: handlers must not block
//! the executor, and detached tasks spawned by handlers are not managed by the SDK.
//! `run()` continues until a terminal error or cancellation. Reconnect backoff
//! defaults to one through thirty seconds and counts consecutive failed attempts
//! or short-lived connections, independently of lifetime attempt numbers. A socket
//! open for [`ReconnectPolicy::reset_after`] (60 seconds by default) resets backoff;
//! setup and drain time do not count. [`ReconnectPolicy::no_retry`] returns connection
//! failures to the caller, but normal disconnects still reconnect.
//!
//! # Deduplication
//!
//! Successful results are retained in memory for five minutes, up to 10,000 entries.
//! Completed duplicates replay the saved response. In-flight duplicates do not
//! receive successful completion. Failed or cancelled work releases its reservation;
//! active reservations are not evicted for capacity, and a full cache fails closed.
//!
//! Multiple replicas can share an asynchronous [`EventDeduplicator`]. Use a separate
//! application namespace, renewable leases, stale-owner fencing, and atomic response
//! storage at completion. Business effects still need durable idempotency across
//! process crashes and retention expiry. A cancelled or failed handler may already
//! have submitted an external operation. An undelivered ACK may lead to redelivery,
//! but the SDK does not guarantee that DingTalk will redeliver. Cached successful
//! results can replay ACKs within their retention; they cannot provide exactly-once
//! business execution, recover unrecorded external results, or roll back side effects.
//! Persist jobs and external operation identifiers durably before acknowledging them,
//! and record/reconcile results outside the handler deadline when necessary.

use std::{
    any::Any, collections::BTreeMap, fmt, future::Future, pin::Pin, sync::Arc, time::Duration,
};

use futures_util::{
    future::{Either, FutureExt, pending, select},
    pin_mut,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio_tungstenite::connect_async;
use url::Url;

use crate::{
    DingTalk, Error, Result,
    auth::AppCredentials,
    bot::{Bot, BotEvent, BotState, ConversationScope, HandleOutcome, MessageType, Route},
    transport::{
        BodySnippetConfig, api_error_from_body, decode_json_response, response_envelope_error,
        with_response_metadata,
    },
    util::{non_empty_trimmed, redact::redact_text},
};

mod context;
mod diagnostics;
mod error;
#[cfg(test)]
mod handler_tests;
mod handlers;
mod runtime;
use crate::bot::dedup::{EventDeduplicator, MemoryEventDeduplicator};
pub use context::StreamContext;
pub use error::StreamError;
pub use runtime::{StreamCancellationReason, StreamProcessingPolicy};

/// Stream topic for robot message callbacks.
pub const BOT_MESSAGE_TOPIC: &str = "/v1.0/im/bot/messages/get";
/// Stream topic for interactive card callbacks.
pub const CARD_CALLBACK_TOPIC: &str = "/v1.0/card/instances/callback";

type StreamEventHandler = Arc<dyn Fn(StreamRunEvent) + Send + Sync + 'static>;
type BoxStreamFrameFuture =
    Pin<Box<dyn Future<Output = Result<StreamFrameResponse>> + Send + 'static>>;
type StreamFrameHandler =
    Arc<dyn Fn(StreamContext, StreamFrame) -> BoxStreamFrameFuture + Send + Sync + 'static>;
type CardCallbackFuture =
    Pin<Box<dyn Future<Output = Result<StreamFrameResponse>> + Send + 'static>>;
type CardCallbackHandler =
    Arc<dyn Fn(StreamContext, CardCallbackEvent) -> CardCallbackFuture + Send + Sync + 'static>;

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
    fallbacks: Vec<Route>,
    state: Option<BotState>,
    subscriptions: Vec<StreamSubscription>,
    subscriptions_replaced: bool,
    local_ip: Option<String>,
    user_agent: Option<String>,
    reconnect: ReconnectPolicy,
    websocket_connect_timeout: Option<Duration>,
    processing: StreamProcessingPolicy,
    deduplicator: Arc<dyn EventDeduplicator>,
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
            fallbacks: Vec::new(),
            state: None,
            subscriptions: Vec::new(),
            subscriptions_replaced: false,
            local_ip: None,
            user_agent: None,
            reconnect: ReconnectPolicy::default(),
            websocket_connect_timeout: None,
            processing: StreamProcessingPolicy::default(),
            deduplicator: Arc::new(MemoryEventDeduplicator::default()),
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

    /// Overrides credentials for the Stream connection and handler OpenAPI calls.
    #[must_use]
    pub fn credentials(mut self, credentials: AppCredentials) -> Self {
        self.credentials = Some(credentials);
        self
    }

    /// Overrides credentials for the Stream connection and handler OpenAPI calls.
    #[must_use]
    pub fn client_id_and_secret(
        mut self,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
    ) -> Self {
        self.credentials = Some(AppCredentials::new(client_id, client_secret));
        self
    }

    /// Appends a route. Conflicts and unreachable routes are rejected by [`Self::build`].
    #[must_use]
    pub fn route(mut self, route: Route) -> Self {
        self.routes.push(route);
        self
    }

    /// Appends a fallback route used when no normal route matches.
    ///
    /// Fallbacks accumulate in registration order. Put specific fallbacks first
    /// and catch-all handlers last. See [`Bot::fallback_route`].
    #[must_use]
    pub fn fallback_route(mut self, route: Route) -> Self {
        self.fallbacks.push(route);
        self
    }

    /// Configures shared state for bot, frame, and card handlers.
    ///
    /// Available through [`crate::bot::BotContext::state`] and [`StreamContext::state`].
    #[must_use]
    pub fn state<T>(mut self, state: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        self.state = Some(Arc::new(state) as Arc<dyn Any + Send + Sync>);
        self
    }

    /// Replaces inferred Stream subscriptions.
    ///
    /// Bot-message defaults are not added when this is used. Card callback handlers still add the
    /// required card callback topic automatically.
    #[must_use]
    pub fn subscriptions(mut self, subscriptions: Vec<StreamSubscription>) -> Self {
        self.subscriptions = subscriptions;
        self.subscriptions_replaced = true;
        self
    }

    /// Adds a Stream subscription in addition to inferred subscriptions.
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

    /// Sets the WebSocket connect timeout.
    #[must_use]
    pub fn websocket_connect_timeout(mut self, value: Duration) -> Self {
        self.websocket_connect_timeout = Some(value);
        self
    }

    /// Configures bounded business processing and graceful shutdown.
    #[must_use]
    pub fn processing_policy(mut self, value: StreamProcessingPolicy) -> Self {
        self.processing = value;
        self
    }

    /// Uses a shared deduplication backend for Stream frames and bot callbacks.
    #[must_use]
    pub fn deduplicator(mut self, store: Arc<dyn EventDeduplicator>) -> Self {
        self.deduplicator = store;
        self
    }

    /// Registers an optional synchronous runtime event observer.
    ///
    /// Use this for metrics or custom observers. Structured `tracing` diagnostics
    /// are emitted independently; avoid logging the same events twice.
    #[must_use]
    pub fn on_event<F>(mut self, handler: F) -> Self
    where
        F: Fn(StreamRunEvent) + Send + Sync + 'static,
    {
        self.event_handler = Some(Arc::new(handler));
        self
    }

    /// Builds the Stream bot.
    pub fn build(self) -> Result<StreamBot> {
        let Self {
            client,
            credentials,
            routes,
            fallbacks,
            state,
            subscriptions,
            subscriptions_replaced,
            local_ip,
            user_agent,
            reconnect,
            websocket_connect_timeout,
            processing,
            deduplicator,
            event_handler,
            frame_handler,
            card_callback_handler,
        } = self;

        let has_bot_route = !routes.is_empty() || !fallbacks.is_empty();
        let has_frame_handler = frame_handler.is_some() || card_callback_handler.is_some();
        if !has_bot_route && !has_frame_handler {
            return Err(Error::InvalidConfig(
                "stream bot route, frame handler, or card callback handler is required".to_string(),
            ));
        }
        validate_bot_routes(&routes, &fallbacks)?;

        let client = match client {
            Some(client) => client,
            None => {
                let credentials = credentials.clone().ok_or(Error::MissingCredentials)?;
                DingTalk::builder().app_credentials(credentials).build()?
            }
        };

        let mut stream = StreamClient::builder(client.clone())?
            .subscriptions(resolve_implicit_subscriptions(
                subscriptions,
                subscriptions_replaced,
                has_bot_route,
                frame_handler.is_some(),
                card_callback_handler.is_some(),
            ))
            .reconnect_policy(reconnect)
            .processing_policy(processing)
            .deduplicator(Arc::clone(&deduplicator));
        stream.state = state;
        if let Some(websocket_connect_timeout) = websocket_connect_timeout {
            stream = stream.websocket_connect_timeout(websocket_connect_timeout);
        }

        if has_bot_route {
            let mut bot = Bot::new(client.clone()).deduplicator(deduplicator);
            for route in routes {
                bot = bot.route(route);
            }
            for route in fallbacks {
                bot = bot.fallback_route(route);
            }
            stream = stream.bot(bot);
        }

        if let Some(credentials) = credentials {
            stream = stream.credentials(credentials);
        }
        if let Some(local_ip) = local_ip {
            stream = stream.local_ip(local_ip);
        }
        if let Some(user_agent) = user_agent {
            stream = stream.user_agent(user_agent);
        }
        if let Some(handler) = event_handler {
            stream = stream.on_event_handler(handler);
        }
        if let Some(handler) = frame_handler {
            stream = stream.on_frame_handler(handler);
        }
        if let Some(handler) = card_callback_handler {
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
    state: Option<BotState>,
    credentials: AppCredentials,
    subscriptions: Vec<StreamSubscription>,
    local_ip: Option<String>,
    user_agent: String,
    reconnect: ReconnectPolicy,
    websocket_connect_timeout: Duration,
    processing: StreamProcessingPolicy,
    deduplicator: Arc<dyn EventDeduplicator>,
    bot: Option<Bot>,
    event_handler: Option<StreamEventHandler>,
    frame_handler: Option<StreamFrameHandler>,
    card_callback_handler: Option<CardCallbackHandler>,
}

impl StreamClient {
    fn handler_context(&self) -> StreamContext {
        StreamContext {
            client: self.client.clone(),
            state: self.state.clone(),
        }
    }

    /// Creates a Stream client builder.
    pub fn builder(client: DingTalk) -> Result<StreamClientBuilder> {
        StreamClientBuilder::new(client)
    }

    /// Opens one Stream WebSocket connection and drains accepted work when it closes.
    ///
    /// Uses [`StreamProcessingPolicy::disconnect_timeout`]. Errors are returned to
    /// the caller without reconnecting; cancellation and frame events are still emitted.
    pub async fn run_once(&self) -> Result<StreamExit> {
        let mut shutdown = Box::pin(pending());
        let mut connected_for = Duration::ZERO;
        self.run_once_with_shutdown(1, shutdown.as_mut(), &mut connected_for)
            .await?
            .ok_or_else(|| Error::stream("unexpected shutdown"))
    }

    async fn run_once_with_shutdown<F>(
        &self,
        attempt: u32,
        mut shutdown: Pin<&mut F>,
        connected_for: &mut Duration,
    ) -> Result<Option<StreamExit>>
    where
        F: Future<Output = ()> + ?Sized,
    {
        self.emit_event(StreamRunEvent::ConnectionOpening { attempt });
        let connect = async {
            let ticket = self.open_connection().await?;
            let url = websocket_url(&ticket)?;
            tokio::time::timeout(self.websocket_connect_timeout, connect_async(url.as_str()))
                .await
                .map_err(|_| Error::stream("websocket connect timed out"))?
                .map_err(Error::websocket_connect)
        };
        let (socket, _response) = tokio::select! {
            biased;
            () = shutdown.as_mut() => return Ok(None),
            result = connect => result?,
        };
        self.emit_event(StreamRunEvent::ConnectionOpened { attempt });
        let exit = self.run_socket(socket, shutdown, connected_for).await?;
        if let Some(exit) = exit {
            self.emit_event(StreamRunEvent::ConnectionClosed { attempt, exit });
        }
        Ok(exit)
    }

    /// Runs the Stream client forever with reconnect backoff.
    pub async fn run(&self) -> Result<()> {
        self.run_until(pending()).await
    }

    /// Runs with reconnect backoff until `shutdown` resolves, then drains accepted work.
    ///
    /// Server disconnects drain before reconnecting. See the module's processing,
    /// cancellation, and deduplication contracts.
    pub async fn run_until<F>(&self, shutdown: F) -> Result<()>
    where
        F: Future<Output = ()>,
    {
        let shutdown = shutdown.fuse();
        pin_mut!(shutdown);
        let mut attempt = 1_u32;
        let mut consecutive_failures = 0_u32;

        loop {
            let mut connected_for = Duration::ZERO;
            let run_result = self
                .run_once_with_shutdown(attempt, shutdown.as_mut(), &mut connected_for)
                .await;
            match run_result {
                Ok(None) => {
                    self.emit_event(StreamRunEvent::Shutdown);
                    return Ok(());
                }
                Ok(Some(StreamExit::Disconnect | StreamExit::Closed)) => {}
                Err(error) => {
                    if futures_util::future::FusedFuture::is_terminated(&shutdown) {
                        self.emit_event(StreamRunEvent::Shutdown);
                        return Err(error);
                    }
                    let retrying = self.reconnect.retry_on_error;
                    self.emit_event(StreamRunEvent::ConnectionError {
                        attempt,
                        retrying,
                        error: StreamError::from(&error),
                    });
                    if !self.reconnect.retry_on_error {
                        return Err(error);
                    }
                }
            }

            consecutive_failures = self
                .reconnect
                .failures_after_connection(consecutive_failures, connected_for);
            let delay = self.reconnect.delay_for_failures(consecutive_failures);
            attempt = attempt.saturating_add(1);
            self.emit_event(StreamRunEvent::ReconnectScheduled {
                next_attempt: attempt,
                consecutive_failures,
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
        let error_body_snippet = self.client.transport().error_body_snippet();
        with_response_metadata(response, |response| {
            let (value, body) =
                decode_json_response::<RawOpenConnectionResponse>(response, error_body_snippet)?;
            value.into_connection(&body, error_body_snippet)
        })
    }

    #[cfg(test)]
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

        self.handle_parsed_frame(frame).await
    }

    async fn handle_parsed_frame(&self, frame: StreamFrame) -> Result<StreamHandleResult> {
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
        match handler(self.handler_context(), event).await {
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

        match handler(self.handler_context(), frame).await {
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
                let data = frame.data_json_or_raw();
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
        event.trace();
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
            error: StreamError::from(&error.error),
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
    state: Option<BotState>,
    credentials: Option<AppCredentials>,
    subscriptions: Vec<StreamSubscription>,
    subscriptions_replaced: bool,
    local_ip: Option<String>,
    user_agent: String,
    reconnect: ReconnectPolicy,
    websocket_connect_timeout: Duration,
    processing: StreamProcessingPolicy,
    deduplicator: Arc<dyn EventDeduplicator>,
    bot: Option<Bot>,
    event_handler: Option<StreamEventHandler>,
    frame_handler: Option<StreamFrameHandler>,
    card_callback_handler: Option<CardCallbackHandler>,
}

impl StreamClientBuilder {
    fn new(client: DingTalk) -> Result<Self> {
        let credentials = client.app_credentials();
        let websocket_connect_timeout = client.stream_connect_timeout();
        Ok(Self {
            client,
            state: None,
            credentials,
            subscriptions: Vec::new(),
            subscriptions_replaced: false,
            local_ip: None,
            user_agent: concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION")).to_string(),
            reconnect: ReconnectPolicy::default(),
            websocket_connect_timeout,
            processing: StreamProcessingPolicy::default(),
            deduplicator: Arc::new(MemoryEventDeduplicator::default()),
            bot: None,
            event_handler: None,
            frame_handler: None,
            card_callback_handler: None,
        })
    }

    /// Overrides credentials for the Stream connection and handler OpenAPI calls.
    #[must_use]
    pub fn credentials(mut self, credentials: AppCredentials) -> Self {
        self.credentials = Some(credentials);
        self
    }

    /// Overrides credentials for the Stream connection and handler OpenAPI calls.
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

    /// Configures shared state for the bot router, frame handlers, and card callbacks.
    ///
    /// Overrides state on a supplied bot regardless of builder call order. When omitted,
    /// the supplied bot's state is shared with Stream callbacks.
    #[must_use]
    pub fn state<T>(mut self, state: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        self.state = Some(Arc::new(state));
        self
    }

    /// Replaces inferred Stream subscriptions.
    ///
    /// Bot-message defaults are not added when this is used. Card callback handlers still add the
    /// required card callback topic automatically.
    #[must_use]
    pub fn subscriptions(mut self, subscriptions: Vec<StreamSubscription>) -> Self {
        self.subscriptions = subscriptions;
        self.subscriptions_replaced = true;
        self
    }

    /// Adds a Stream subscription in addition to inferred subscriptions.
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

    /// Sets the WebSocket connect timeout.
    #[must_use]
    pub fn websocket_connect_timeout(mut self, value: Duration) -> Self {
        self.websocket_connect_timeout = value;
        self
    }

    /// Configures bounded business processing and graceful shutdown.
    #[must_use]
    pub fn processing_policy(mut self, value: StreamProcessingPolicy) -> Self {
        self.processing = value;
        self
    }

    /// Uses a shared Stream frame deduplication backend.
    #[must_use]
    pub fn deduplicator(mut self, store: Arc<dyn EventDeduplicator>) -> Self {
        self.deduplicator = store;
        self
    }

    /// Registers an optional synchronous runtime event observer.
    ///
    /// Use this for metrics or custom observers. Structured `tracing` diagnostics
    /// are emitted independently; avoid logging the same events twice.
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

    fn on_frame_handler(mut self, handler: StreamFrameHandler) -> Self {
        self.frame_handler = Some(handler);
        self
    }

    fn on_card_callback_handler(mut self, handler: CardCallbackHandler) -> Self {
        self.card_callback_handler = Some(handler);
        self
    }

    /// Builds a Stream client.
    pub fn build(self) -> Result<StreamClient> {
        if self.bot.is_none()
            && self.frame_handler.is_none()
            && self.card_callback_handler.is_none()
        {
            return Err(Error::InvalidConfig(
                "stream bot router, frame handler, or card callback handler is required"
                    .to_string(),
            ));
        }
        if let Some(bot) = &self.bot {
            if !bot.has_routes() {
                return Err(Error::InvalidConfig(
                    "stream bot router requires at least one route or fallback".to_string(),
                ));
            }
            bot.validate()?;
        }
        let credentials = self.credentials.ok_or(Error::MissingCredentials)?;
        credentials.validate()?;
        let client = self.client.with_app_credentials(credentials.clone())?;
        let state = self
            .state
            .or_else(|| self.bot.as_ref().and_then(Bot::shared_state));
        let bot = self
            .bot
            .map(|bot| {
                let bot = match &state {
                    Some(state) => bot.state_arc(Arc::clone(state)),
                    None => bot,
                };
                bot.with_app_credentials(credentials.clone())
            })
            .transpose()?;

        let subscriptions = resolve_implicit_subscriptions(
            self.subscriptions,
            self.subscriptions_replaced,
            bot.is_some(),
            self.frame_handler.is_some(),
            self.card_callback_handler.is_some(),
        );
        let subscriptions = normalize_subscriptions(subscriptions)?;
        self.reconnect.validate()?;
        self.processing.validate()?;
        validate_stream_duration("websocket_connect_timeout", self.websocket_connect_timeout)?;
        validate_user_agent(&self.user_agent)?;
        let user_agent = self.user_agent;
        let local_ip = self
            .local_ip
            .map(|value| {
                validate_protocol_token(&value, "local_ip")?;
                Ok::<String, Error>(value)
            })
            .transpose()?;

        Ok(StreamClient {
            client,
            state,
            credentials,
            subscriptions,
            local_ip,
            user_agent,
            reconnect: self.reconnect,
            websocket_connect_timeout: self.websocket_connect_timeout,
            processing: self.processing,
            deduplicator: self.deduplicator,
            bot,
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
    /// Connection lifetime required to reset backoff, excluding connect and drain time.
    /// The default is 60 seconds. Brief connections continue to increase backoff.
    pub reset_after: Duration,
    /// Whether errors should be retried.
    pub retry_on_error: bool,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            reset_after: Duration::from_secs(60),
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
            ..Self::default()
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

    /// Sets how long a connection must stay open before its backoff is reset.
    #[must_use]
    pub fn reset_after(mut self, value: Duration) -> Self {
        self.reset_after = value;
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
        validate_stream_duration("reconnect.initial_delay", self.initial_delay)?;
        validate_stream_duration("reconnect.max_delay", self.max_delay)?;
        validate_stream_duration("reconnect.reset_after", self.reset_after)?;
        if self.max_delay < self.initial_delay {
            return Err(Error::invalid_input(
                "reconnect.max_delay",
                "value must be greater than or equal to initial_delay",
            ));
        }
        Ok(())
    }

    fn failures_after_connection(self, previous: u32, connected_for: Duration) -> u32 {
        if connected_for >= self.reset_after {
            0
        } else {
            previous.saturating_add(1)
        }
    }

    /// Returns the delay for consecutive failed attempts or short-lived connections.
    /// Zero (a stable connection) and one both use the initial delay.
    #[must_use]
    pub fn delay_for_failures(self, failures: u32) -> Duration {
        let mut delay = self.initial_delay.min(self.max_delay);
        for _ in 1..failures {
            if delay.is_zero() || delay == self.max_delay {
                break;
            }
            delay = delay.saturating_mul(2).min(self.max_delay);
        }
        delay
    }
}

fn validate_stream_duration(field: &'static str, value: Duration) -> Result<()> {
    if value.is_zero() || std::time::Instant::now().checked_add(value).is_none() {
        return Err(Error::invalid_input(
            field,
            "value must be positive and fit a monotonic deadline",
        ));
    }
    Ok(())
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
        Self {
            topic: topic.into(),
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
        validate_protocol_token(&self.topic, "subscription.topic")?;
        Ok(())
    }
}

fn normalize_subscriptions(
    subscriptions: Vec<StreamSubscription>,
) -> Result<Vec<StreamSubscription>> {
    if subscriptions.is_empty() {
        return Err(Error::invalid_input(
            "subscriptions",
            "at least one subscription is required",
        ));
    }

    let mut normalized = Vec::new();
    for subscription in subscriptions {
        subscription.validate()?;
        if !normalized.iter().any(|existing: &StreamSubscription| {
            existing.kind == subscription.kind && existing.topic == subscription.topic
        }) {
            normalized.push(subscription);
        }
    }

    Ok(normalized)
}

fn validate_bot_routes(routes: &[Route], fallbacks: &[Route]) -> Result<()> {
    for (kind, routes) in [("routes", routes), ("fallbacks", fallbacks)] {
        for (index, route) in routes.iter().enumerate() {
            route.validate_after(&routes[..index], kind)?;
        }
    }
    Ok(())
}

fn resolve_implicit_subscriptions(
    mut subscriptions: Vec<StreamSubscription>,
    subscriptions_replaced: bool,
    bot_messages: bool,
    frame_handler: bool,
    card_callbacks: bool,
) -> Vec<StreamSubscription> {
    if !subscriptions_replaced && (bot_messages || frame_handler) {
        push_subscription_once(&mut subscriptions, StreamSubscription::bot_messages());
    }
    if card_callbacks {
        push_subscription_once(&mut subscriptions, StreamSubscription::card_callbacks());
    }
    subscriptions
}

fn push_subscription_once(
    subscriptions: &mut Vec<StreamSubscription>,
    subscription: StreamSubscription,
) {
    if !subscriptions
        .iter()
        .any(|existing| existing.kind == subscription.kind && existing.topic == subscription.topic)
    {
        subscriptions.push(subscription);
    }
}

fn redacted_json_value(value: &Value) -> String {
    redact_text(&value.to_string())
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
        /// Redacted error text and structured SDK metadata.
        error: StreamError,
    },
    /// A reconnect sleep was scheduled.
    ReconnectScheduled {
        /// One-based number of the next connection attempt.
        next_attempt: u32,
        /// Consecutive failures or short connections; zero after a stable connection.
        consecutive_failures: u32,
        /// Delay before reconnecting.
        delay: Duration,
    },
    /// Frame processing failed. A failure ACK is attempted only while connected.
    FrameError {
        /// DingTalk message id, when the frame could be parsed.
        message_id: Option<String>,
        /// Redacted error text and structured SDK metadata.
        error: StreamError,
    },
    /// Accepted frame processing was cancelled, possibly before its handler started.
    /// External side effects may already have occurred; no successful ACK is implied.
    FrameCancelled {
        /// Stream frame message id.
        message_id: String,
        /// Why processing was cancelled.
        reason: StreamCancellationReason,
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

#[derive(Serialize)]
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

fn websocket_url(ticket: &OpenConnectionResponse) -> Result<Url> {
    let endpoint = non_empty_trimmed(&ticket.endpoint, "stream.endpoint")?;
    let ticket_value = normalize_protocol_token(&ticket.ticket, "stream.ticket")?;
    let mut url = Url::parse(&endpoint)
        .map_err(|source| Error::stream(format!("invalid stream endpoint: {source}")))?;
    if !matches!(url.scheme(), "ws" | "wss") {
        return Err(Error::stream(
            "stream endpoint scheme must be ws or wss".to_string(),
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(Error::stream(
            "stream endpoint must not contain username or password".to_string(),
        ));
    }
    if url.fragment().is_some() {
        return Err(Error::stream(
            "stream endpoint must not contain a fragment".to_string(),
        ));
    }

    let query_pairs = url
        .query_pairs()
        .filter(|(name, _value)| name != "ticket")
        .map(|(name, value)| (name.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    url.set_query(None);
    {
        let mut query = url.query_pairs_mut();
        for (name, value) in query_pairs {
            query.append_pair(&name, &value);
        }
        query.append_pair("ticket", &ticket_value);
    }
    Ok(url)
}

struct OpenConnectionResponse {
    endpoint: String,
    ticket: String,
}

impl fmt::Debug for OpenConnectionResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenConnectionResponse")
            .field("endpoint", &redact_text(&self.endpoint))
            .field("ticket", &"<redacted>")
            .finish()
    }
}

#[derive(Deserialize)]
struct RawOpenConnectionResponse {
    endpoint: Option<String>,
    ticket: Option<String>,
    #[serde(
        default,
        deserialize_with = "crate::transport::deserialize_optional_i64"
    )]
    errcode: Option<i64>,
    #[serde(
        rename = "code",
        default,
        alias = "Code",
        deserialize_with = "crate::transport::deserialize_optional_string"
    )]
    api_code: Option<String>,
    #[serde(
        default,
        alias = "message",
        alias = "errorMessage",
        alias = "ErrorMessage",
        alias = "error_message",
        deserialize_with = "crate::transport::deserialize_optional_string"
    )]
    errmsg: Option<String>,
    #[serde(
        default,
        deserialize_with = "crate::transport::deserialize_optional_bool"
    )]
    success: Option<bool>,
    #[serde(
        default,
        alias = "requestId",
        alias = "RequestId",
        alias = "requestid",
        deserialize_with = "crate::transport::deserialize_optional_string"
    )]
    request_id: Option<String>,
}

impl RawOpenConnectionResponse {
    fn into_connection(
        self,
        body: &str,
        error_body_snippet: BodySnippetConfig,
    ) -> Result<OpenConnectionResponse> {
        if let Some(error) = response_envelope_error(
            self.errcode,
            self.api_code.as_deref(),
            self.errmsg.as_deref(),
            self.success,
            self.request_id.as_deref(),
            body,
            error_body_snippet,
        ) {
            return Err(error);
        }

        let endpoint = self
            .endpoint
            .as_deref()
            .and_then(|value| non_empty_trimmed(value, "endpoint").ok())
            .ok_or_else(|| {
                api_error_from_body(
                    -1,
                    "missing endpoint in DingTalk Stream open response",
                    self.request_id.clone(),
                    body,
                    error_body_snippet,
                )
            })?;
        let ticket = self
            .ticket
            .as_deref()
            .and_then(|value| normalize_protocol_token(value, "ticket").ok())
            .ok_or_else(|| {
                api_error_from_body(
                    -1,
                    "missing ticket in DingTalk Stream open response",
                    self.request_id.clone(),
                    body,
                    error_body_snippet,
                )
            })?;

        Ok(OpenConnectionResponse { endpoint, ticket })
    }
}

/// Incoming DingTalk Stream frame.
#[derive(Clone)]
pub struct StreamFrame {
    frame_type: StreamFrameType,
    headers: StreamHeaders,
    data: Value,
}

impl fmt::Debug for StreamFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamFrame")
            .field("frame_type", &self.frame_type)
            .field("headers", &self.headers)
            .field("data", &redacted_json_value(&self.data))
            .finish()
    }
}

impl StreamFrame {
    /// Parses a raw Stream WebSocket text frame.
    pub fn from_text(text: &str) -> Result<Self> {
        let raw = serde_json::from_str::<RawStreamFrame>(text)?;
        Ok(Self {
            frame_type: StreamFrameType::from_raw(&raw.frame_type)?,
            headers: raw.headers.normalized()?,
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

    /// Returns `data` as JSON when string decoding succeeds, otherwise returns raw `data`.
    #[must_use]
    pub fn data_json_or_raw(&self) -> Value {
        self.data_json().unwrap_or_else(|_error| self.data.clone())
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
        normalized_string_value(value_by_names(
            value.get("headers")?,
            &["messageId", "message_id", "MessageId"],
        )?)
    }
}

/// Interactive card callback event delivered by DingTalk Stream.
#[derive(Clone, PartialEq)]
pub struct CardCallbackEvent {
    raw: Value,
}

impl fmt::Debug for CardCallbackEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CardCallbackEvent")
            .field("raw", &redacted_json_value(&self.raw))
            .finish()
    }
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
        self.value_field(names).and_then(trimmed_str_value)
    }

    fn value_field(&self, names: &[&str]) -> Option<&Value> {
        names.iter().find_map(|name| self.get(name))
    }
}

/// Strongly typed view of an interactive card callback.
#[derive(Clone, PartialEq)]
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

impl fmt::Debug for CardCallbackPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CardCallbackPayload")
            .field("raw", &redacted_json_value(&self.raw))
            .field("callback_type", &self.callback_type)
            .field("card_biz_id", &self.card_biz_id)
            .field("card_instance_id", &self.card_instance_id)
            .field(
                "has_open_conversation_id",
                &self.open_conversation_id.is_some(),
            )
            .field("operator", &self.operator)
            .field("content", &self.content)
            .field("action", &self.action)
            .field("action_ids", &self.action_ids)
            .field("action_value", &self.action_value)
            .finish()
    }
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
#[derive(Clone, Default, PartialEq, Eq)]
pub struct CardCallbackOperator {
    user_id: Option<String>,
    union_id: Option<String>,
    corp_id: Option<String>,
}

impl fmt::Debug for CardCallbackOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CardCallbackOperator")
            .field("has_user_id", &self.user_id.is_some())
            .field("has_union_id", &self.union_id.is_some())
            .field("has_corp_id", &self.corp_id.is_some())
            .finish()
    }
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
#[derive(Clone, PartialEq)]
pub struct CardCallbackContent {
    raw: Value,
    private_data: Option<CardCallbackPrivateData>,
}

impl fmt::Debug for CardCallbackContent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CardCallbackContent")
            .field("raw", &redacted_json_value(&self.raw))
            .field("private_data", &self.private_data)
            .finish()
    }
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
#[derive(Clone, PartialEq)]
pub struct CardCallbackPrivateData {
    raw: Value,
    action_ids: Vec<String>,
    params: CardCallbackActionValue,
}

impl fmt::Debug for CardCallbackPrivateData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CardCallbackPrivateData")
            .field("raw", &redacted_json_value(&self.raw))
            .field("action_ids", &self.action_ids)
            .field("params", &self.params)
            .finish()
    }
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
#[derive(Clone, Default, PartialEq)]
pub struct CardCallbackActionValue {
    value: Option<Value>,
}

impl fmt::Debug for CardCallbackActionValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CardCallbackActionValue")
            .field("value", &self.value.as_ref().map(redacted_json_value))
            .finish()
    }
}

impl CardCallbackActionValue {
    /// Creates action values from raw JSON.
    #[must_use]
    pub fn from_value(value: Value) -> Self {
        let value = normalize_action_value(value);
        Self {
            value: (!value.is_null()).then_some(value),
        }
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
        self.get(name).and_then(trimmed_str_value)
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

fn normalize_action_value(value: Value) -> Value {
    let Value::String(raw) = &value else {
        return value;
    };

    let trimmed = raw.trim();
    if !(trimmed.starts_with('{') || trimmed.starts_with('[')) {
        return value;
    }

    serde_json::from_str(trimmed).unwrap_or(value)
}

/// Response payload for interactive card callbacks.
#[derive(Clone, Default, PartialEq, Eq, Serialize)]
pub struct CardCallbackResponse {
    #[serde(rename = "cardData", skip_serializing_if = "Option::is_none")]
    card_data: Option<CardCallbackResponseData>,
    #[serde(rename = "userPrivateData", skip_serializing_if = "Option::is_none")]
    user_private_data: Option<CardCallbackResponseData>,
}

impl fmt::Debug for CardCallbackResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CardCallbackResponse")
            .field("card_data", &self.card_data)
            .field("user_private_data", &self.user_private_data)
            .finish()
    }
}

impl CardCallbackResponse {
    /// Creates an empty callback response.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets card-level parameters to update.
    pub fn card_data<I, K, V>(mut self, values: I) -> Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.card_data = Some(CardCallbackResponseData::new("card_data", values)?);
        Ok(self)
    }

    /// Sets user-private parameters to update.
    pub fn user_private_data<I, K, V>(mut self, values: I) -> Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.user_private_data = Some(CardCallbackResponseData::new("user_private_data", values)?);
        Ok(self)
    }

    /// Converts this value into a Stream ACK response.
    pub fn into_stream_response(self) -> Result<StreamFrameResponse> {
        self.validate()?;
        StreamFrameResponse::json(self)
    }

    fn validate(&self) -> Result<()> {
        if self.card_data.is_none() && self.user_private_data.is_none() {
            return Err(Error::invalid_input(
                "card_callback_response",
                "card_data or user_private_data is required",
            ));
        }
        if let Some(card_data) = &self.card_data {
            card_data.validate("card_data")?;
        }
        if let Some(user_private_data) = &self.user_private_data {
            user_private_data.validate("user_private_data")?;
        }
        Ok(())
    }
}

#[derive(Clone, Default, PartialEq, Eq, Serialize)]
struct CardCallbackResponseData {
    #[serde(rename = "cardParamMap")]
    card_param_map: BTreeMap<String, String>,
}

impl fmt::Debug for CardCallbackResponseData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let card_param_map = self
            .card_param_map
            .keys()
            .map(|key| (key.as_str(), "<redacted>"))
            .collect::<BTreeMap<_, _>>();
        f.debug_struct("CardCallbackResponseData")
            .field("card_param_map", &card_param_map)
            .finish()
    }
}

impl CardCallbackResponseData {
    fn new<I, K, V>(field: &'static str, values: I) -> Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let mut card_param_map = BTreeMap::new();
        for (key, value) in values {
            let key = key.into();
            validate_protocol_token(&key, field)?;
            if card_param_map.contains_key(&key) {
                return Err(Error::invalid_input(
                    field,
                    "card parameter keys must be unique",
                ));
            }
            card_param_map.insert(key, value.into());
        }
        let data = Self { card_param_map };
        data.validate(field)?;
        Ok(data)
    }

    fn validate(&self, field: &'static str) -> Result<()> {
        if self.card_param_map.is_empty() {
            return Err(Error::invalid_input(
                field,
                "at least one card parameter is required",
            ));
        }
        for key in self.card_param_map.keys() {
            validate_protocol_token(key, field)?;
        }
        Ok(())
    }
}

fn validate_protocol_token(value: &str, field: &'static str) -> Result<()> {
    let normalized = normalize_protocol_token(value, field)?;
    if normalized != value {
        return Err(Error::invalid_input(
            field,
            "value must not contain leading or trailing whitespace",
        ));
    }
    Ok(())
}

fn normalize_protocol_token(value: &str, field: &'static str) -> Result<String> {
    if value.chars().any(char::is_control) {
        return Err(Error::invalid_input(
            field,
            "value must not contain control characters",
        ));
    }
    let value = non_empty_trimmed(value, field)?;
    if value.chars().any(char::is_whitespace) {
        return Err(Error::invalid_input(
            field,
            "value must not contain whitespace",
        ));
    }
    Ok(value)
}

fn validate_user_agent(value: &str) -> Result<()> {
    if value.chars().any(char::is_control) {
        return Err(Error::invalid_input(
            "user_agent",
            "value must not contain control characters",
        ));
    }
    let trimmed = non_empty_trimmed(value, "user_agent")?;
    if trimmed != value {
        return Err(Error::invalid_input(
            "user_agent",
            "value must not contain leading or trailing whitespace",
        ));
    }
    Ok(())
}

fn callback_content(raw: &Value) -> Option<Value> {
    let value = value_by_names(raw, &["content"])?;
    match value {
        Value::String(text) => Some(
            serde_json::from_str::<Value>(text)
                .unwrap_or_else(|_error| Value::String(text.clone())),
        ),
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
    value_by_names(raw, names).and_then(normalized_string_value)
}

fn normalized_string_value(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => non_empty_trimmed(value, "value").ok(),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn trimmed_str_value(value: &Value) -> Option<&str> {
    let value = value.as_str()?.trim();
    (!value.is_empty()).then_some(value)
}

fn string_vec_from_value(value: &Value) -> Vec<String> {
    match value {
        Value::Array(values) => normalized_string_values(values),
        Value::String(text) => string_vec_from_text(text),
        Value::Number(_) => normalized_string_value(value).into_iter().collect(),
        _ => Vec::new(),
    }
}

fn string_vec_from_text(text: &str) -> Vec<String> {
    let text = text.trim();
    if text.is_empty() {
        return Vec::new();
    }

    match serde_json::from_str::<Value>(text) {
        Ok(Value::Array(values)) => normalized_string_values(&values),
        Ok(value) => normalized_string_value(&value).into_iter().collect(),
        Err(_error) => vec![text.to_string()],
    }
}

fn normalized_string_values(values: &[Value]) -> Vec<String> {
    values.iter().filter_map(normalized_string_value).collect()
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

#[derive(Deserialize)]
struct RawStreamFrame {
    #[serde(
        rename = "type",
        deserialize_with = "crate::transport::deserialize_string"
    )]
    frame_type: String,
    headers: StreamHeaders,
    data: Value,
}

/// DingTalk Stream frame headers.
#[derive(Clone, Deserialize)]
pub struct StreamHeaders {
    #[serde(
        alias = "Topic",
        deserialize_with = "crate::transport::deserialize_string"
    )]
    topic: String,
    #[serde(
        rename = "messageId",
        alias = "message_id",
        alias = "MessageId",
        deserialize_with = "crate::transport::deserialize_string"
    )]
    message_id: String,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

impl fmt::Debug for StreamHeaders {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let extra = Value::Object(self.extra.clone().into_iter().collect());
        f.debug_struct("StreamHeaders")
            .field("topic", &self.topic)
            .field("message_id", &self.message_id)
            .field("extra", &redacted_json_value(&extra))
            .finish()
    }
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
            .and_then(trimmed_str_value)
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

    fn normalized(mut self) -> Result<Self> {
        self.topic = non_empty_trimmed(&self.topic, "stream.headers.topic")?;
        self.message_id = non_empty_trimmed(&self.message_id, "stream.headers.message_id")?;
        Ok(self)
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
    fn from_raw(value: &str) -> Result<Self> {
        let value = non_empty_trimmed(value, "stream.type")?;
        match value.to_ascii_uppercase().as_str() {
            "SYSTEM" => Ok(Self::System),
            "CALLBACK" => Ok(Self::Callback),
            "EVENT" => Ok(Self::Event),
            _ => Ok(Self::Unknown(value)),
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
            Self::Response(value) => serde_json::json!({ "response": value }).to_string(),
            Self::Raw(value) => value.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    #[tokio::test]
    async fn stream_bot_credentials_apply_to_custom_client_and_context() {
        for original_credentials in [None, Some(AppCredentials::new("other-id", "other-secret"))] {
            let mut builder = DingTalk::builder()
                .openapi_base_url("http://localhost:19001/modern")
                .connect_timeout(Duration::from_secs(9));
            if let Some(credentials) = &original_credentials {
                builder = builder.app_credentials(credentials.clone());
            }
            let original = builder.build().expect("custom client");
            let stream = StreamBot::from_client(original.clone())
                .client_id_and_secret("stream-id", "stream-secret")
                .on_text_command(ConversationScope::Any, "/check", |ctx| async move {
                    let api = ctx.client().openapi();
                    let credentials = api.credentials().ok_or(Error::MissingCredentials)?;
                    assert_eq!(credentials.app_key(), "stream-id");
                    assert_eq!(credentials.app_secret(), "stream-secret");
                    assert_eq!(
                        ctx.client().stream_connect_timeout(),
                        Duration::from_secs(9)
                    );
                    assert_eq!(
                        ctx.client().openapi_endpoint(&["test"])?.as_str(),
                        "http://localhost:19001/modern/test"
                    );
                    Ok::<_, dingding::Error>(())
                })
                .build()
                .expect("Stream bot");
            assert_eq!(stream.client.credentials.app_key(), "stream-id");
            assert_eq!(
                stream
                    .client
                    .client
                    .openapi()
                    .credentials()
                    .map(AppCredentials::app_key),
                Some("stream-id")
            );
            assert_eq!(
                original.openapi().credentials(),
                original_credentials.as_ref()
            );
            stream
                .client
                .bot
                .as_ref()
                .expect("bot")
                .handle_event(BotEvent::text(ConversationScope::Group, "/check"))
                .await
                .expect("handler uses Stream credentials");
        }
    }

    #[tokio::test]
    async fn stream_credentials_apply_to_supplied_router_without_replacing_its_transport() {
        let connection_client = DingTalk::new().expect("connection client");
        let router_client = DingTalk::builder()
            .app_key_and_secret("other-id", "other-secret")
            .openapi_base_url("http://localhost:19002/router")
            .connect_timeout(Duration::from_secs(11))
            .build()
            .expect("router client");
        let bot = Bot::new(router_client.clone()).on_text_command(
            ConversationScope::Any,
            "/check",
            |ctx| async move {
                let api = ctx.client().openapi();
                assert_eq!(
                    api.credentials().map(AppCredentials::app_key),
                    Some("stream-id")
                );
                assert_eq!(
                    ctx.client().stream_connect_timeout(),
                    Duration::from_secs(11)
                );
                assert_eq!(
                    ctx.client().openapi_endpoint(&["test"])?.as_str(),
                    "http://localhost:19002/router/test"
                );
                Ok::<_, Error>(())
            },
        );
        let stream = StreamClient::builder(connection_client.clone())
            .expect("builder")
            .client_id_and_secret("stream-id", "stream-secret")
            .bot(bot)
            .build()
            .expect("stream");
        stream
            .bot
            .as_ref()
            .expect("bot")
            .handle_event(BotEvent::text(ConversationScope::Private, "/check"))
            .await
            .expect("handler credentials");
        assert!(connection_client.openapi().credentials().is_none());
        assert_eq!(
            router_client
                .openapi()
                .credentials()
                .map(AppCredentials::app_key),
            Some("other-id")
        );
    }

    #[tokio::test]
    async fn stream_callbacks_preserve_application_errors() {
        use std::error::Error as _;
        let client = DingTalk::builder()
            .app_key_and_secret("id", "secret")
            .build()
            .expect("client");
        let application_error = || std::io::Error::other("application callback failed");
        let streams = [
            StreamClient::builder(client.clone())
                .expect("builder")
                .on_frame(move |_ctx, _| async move { Err::<(), _>(application_error()) })
                .build()
                .expect("stream"),
            StreamClient::builder(client.clone())
                .expect("builder")
                .on_frame(move |_ctx, _| async move {
                    Err::<StreamFrameResponse, _>(application_error())
                })
                .build()
                .expect("stream"),
            StreamClient::builder(client.clone())
                .expect("builder")
                .on_card_callback(move |_ctx, _| async move { Err::<(), _>(application_error()) })
                .build()
                .expect("stream"),
            StreamClient::builder(client.clone())
                .expect("builder")
                .on_card_callback(move |_ctx, _| async move {
                    Err::<StreamFrameResponse, _>(application_error())
                })
                .build()
                .expect("stream"),
            StreamBot::from_client(client.clone())
                .on_frame(move |_ctx, _| async move { Err::<(), _>(application_error()) })
                .build()
                .expect("bot")
                .client,
            StreamBot::from_client(client.clone())
                .on_frame(move |_ctx, _| async move {
                    Err::<StreamFrameResponse, _>(application_error())
                })
                .build()
                .expect("bot")
                .client,
            StreamBot::from_client(client.clone())
                .on_card_callback(move |_ctx, _| async move { Err::<(), _>(application_error()) })
                .build()
                .expect("bot")
                .client,
            StreamBot::from_client(client)
                .on_card_callback(move |_ctx, _| async move {
                    Err::<StreamFrameResponse, _>(application_error())
                })
                .build()
                .expect("bot")
                .client,
        ];
        let frame = serde_json::json!({
            "specVersion":"1.0", "type":"CALLBACK",
            "headers":{"topic":CARD_CALLBACK_TOPIC, "messageId":"card-id", "contentType":"application/json"},
            "data":"{}",
        }).to_string();
        for stream in streams {
            let handled = stream
                .handle_text_frame(&frame)
                .await
                .expect("handled frame");
            assert_eq!(
                serde_json::to_value(&handled.ack).expect("ack")["code"],
                500
            );
            let error = handled.error.expect("handler error").error;
            assert_eq!(error.kind(), crate::ErrorKind::Handler);
            assert!(
                error
                    .source()
                    .and_then(|source| source.downcast_ref::<std::io::Error>())
                    .is_some()
            );
        }
    }

    #[tokio::test]
    async fn stream_response_keeps_payload_and_sdk_error_category() {
        let stream = StreamBot::builder()
            .client_id_and_secret("id", "secret")
            .on_card_callback(|_ctx, _| async {
                Ok::<_, std::io::Error>(StreamFrameResponse::from_value(
                    serde_json::json!({"accepted":true}),
                ))
            })
            .build()
            .expect("bot")
            .client;
        let response = stream.card_callback_handler.as_ref().expect("callback")(
            stream.handler_context(),
            CardCallbackEvent::from_value(serde_json::json!({})),
        )
        .await
        .expect("response");
        assert_eq!(response.as_value()["accepted"], true);
        let stream = StreamBot::builder()
            .client_id_and_secret("id", "secret")
            .on_frame(|_ctx, _| async { Err::<(), _>(Error::MissingCredentials) })
            .build()
            .expect("bot")
            .client;
        let frame = StreamFrame::from_text(
            r#"{"type":"EVENT","headers":{"topic":"test","messageId":"id"},"data":{}}"#,
        )
        .expect("frame");
        let error =
            stream.frame_handler.as_ref().expect("handler")(stream.handler_context(), frame)
                .await
                .expect_err("SDK error");
        assert_eq!(error.kind(), crate::ErrorKind::MissingCredentials);
    }

    #[tokio::test]
    async fn stream_callbacks_accept_unit_and_response_values_on_both_builders() {
        let client = DingTalk::builder()
            .app_key_and_secret("id", "secret")
            .build()
            .expect("client");
        let low = || StreamClient::builder(client.clone()).expect("builder");
        let high = || StreamBot::from_client(client.clone());
        let response = || StreamFrameResponse::from_value(serde_json::json!({"accepted":true}));
        let streams = [
            (
                low().on_frame(|_ctx, _| async {}).build().expect("stream"),
                false,
            ),
            (
                low()
                    .on_frame(move |_ctx, _| async move { response() })
                    .build()
                    .expect("stream"),
                true,
            ),
            (
                low()
                    .on_card_callback(|_ctx, _| async {})
                    .build()
                    .expect("stream"),
                false,
            ),
            (
                low()
                    .on_card_callback(move |_ctx, _| async move { response() })
                    .build()
                    .expect("stream"),
                true,
            ),
            (
                high()
                    .on_frame(|_ctx, _| async {})
                    .build()
                    .expect("bot")
                    .client,
                false,
            ),
            (
                high()
                    .on_frame(move |_ctx, _| async move { response() })
                    .build()
                    .expect("bot")
                    .client,
                true,
            ),
            (
                high()
                    .on_card_callback(|_ctx, _| async {})
                    .build()
                    .expect("bot")
                    .client,
                false,
            ),
            (
                high()
                    .on_card_callback(move |_ctx, _| async move { response() })
                    .build()
                    .expect("bot")
                    .client,
                true,
            ),
        ];
        for (stream, has_payload) in streams {
            let result = if let Some(handler) = &stream.card_callback_handler {
                assert!(
                    stream
                        .subscriptions
                        .iter()
                        .any(|subscription| subscription.topic() == CARD_CALLBACK_TOPIC)
                );
                handler(
                    stream.handler_context(),
                    CardCallbackEvent::from_value(serde_json::json!({})),
                )
                .await
            } else {
                let frame = StreamFrame::from_text(
                    r#"{"type":"EVENT","headers":{"topic":"test","messageId":"id"},"data":{}}"#,
                )
                .expect("frame");
                stream.frame_handler.as_ref().expect("handler")(stream.handler_context(), frame)
                    .await
            }
            .expect("response");
            assert_eq!(
                result.into_value(),
                if has_payload {
                    serde_json::json!({"accepted":true})
                } else {
                    Value::Null
                }
            );
        }
    }

    fn test_bot(client: DingTalk) -> Bot {
        Bot::new(client).route(Route::new(ConversationScope::Any).handle(|_ctx| async {}))
    }

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
    fn websocket_url_replaces_existing_ticket() {
        let url = websocket_url(&OpenConnectionResponse {
            endpoint: "wss://example.com/connect?tenant=ding&ticket=stale".to_string(),
            ticket: "fresh-ticket".to_string(),
        })
        .expect("url");

        assert_eq!(
            url.as_str(),
            "wss://example.com/connect?tenant=ding&ticket=fresh-ticket"
        );
    }

    #[test]
    fn websocket_url_rejects_credentials_and_fragments() {
        let userinfo = websocket_url(&OpenConnectionResponse {
            endpoint: "wss://user:pass@example.com/connect".to_string(),
            ticket: "ticket-1".to_string(),
        })
        .expect_err("userinfo should fail");
        let fragment = websocket_url(&OpenConnectionResponse {
            endpoint: "wss://example.com/connect#fragment".to_string(),
            ticket: "ticket-1".to_string(),
        })
        .expect_err("fragment should fail");

        assert_eq!(userinfo.kind(), crate::ErrorKind::Stream);
        assert_eq!(fragment.kind(), crate::ErrorKind::Stream);
    }

    #[test]
    fn websocket_url_rejects_blank_endpoint_or_ticket() {
        let endpoint = websocket_url(&OpenConnectionResponse {
            endpoint: " ".to_string(),
            ticket: "ticket-1".to_string(),
        })
        .expect_err("blank endpoint should fail");
        let ticket = websocket_url(&OpenConnectionResponse {
            endpoint: "wss://example.com/connect".to_string(),
            ticket: " ".to_string(),
        })
        .expect_err("blank ticket should fail");

        assert_eq!(endpoint.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(ticket.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn websocket_url_rejects_ticket_whitespace() {
        let error = websocket_url(&OpenConnectionResponse {
            endpoint: "wss://example.com/connect".to_string(),
            ticket: "ticket value".to_string(),
        })
        .expect_err("ticket should not contain whitespace");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn open_connection_response_normalizes_endpoint_and_ticket() {
        let response = RawOpenConnectionResponse {
            endpoint: Some(" wss://example.com/connect ".to_string()),
            ticket: Some(" ticket-1 ".to_string()),
            errcode: Some(0),
            api_code: None,
            errmsg: None,
            success: None,
            request_id: Some(" request-1 ".to_string()),
        }
        .into_connection(
            r#"{"errcode":0,"endpoint":" wss://example.com/connect ","ticket":" ticket-1 "}"#,
            BodySnippetConfig::default(),
        )
        .expect("connection response");

        assert_eq!(response.endpoint, "wss://example.com/connect");
        assert_eq!(response.ticket, "ticket-1");
    }

    #[test]
    fn open_connection_response_debug_redacts_ticket() {
        let response = OpenConnectionResponse {
            endpoint: "wss://example.com/connect?ticket=endpoint-ticket".to_string(),
            ticket: "ticket-secret".to_string(),
        };
        let debug = format!("{response:?}");

        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("endpoint-ticket"));
        assert!(!debug.contains("ticket-secret"));
    }

    #[test]
    fn open_connection_response_preserves_api_errors() {
        let error = RawOpenConnectionResponse {
            endpoint: None,
            ticket: None,
            errcode: Some(40001),
            api_code: None,
            errmsg: Some("invalid client".to_string()),
            success: None,
            request_id: Some(" request-1 ".to_string()),
        }
        .into_connection(
            r#"{"errcode":40001,"errmsg":"invalid client","requestId":" request-1 "}"#,
            BodySnippetConfig::default(),
        )
        .expect_err("business error should fail");

        assert_eq!(error.kind(), crate::ErrorKind::Api);
        assert_eq!(error.request_id(), Some("request-1"));
        assert!(
            error
                .error_body_snippet()
                .is_some_and(|snippet| snippet.contains("invalid client"))
        );
    }

    #[test]
    fn open_connection_response_rejects_success_false() {
        let body = r#"{"errcode":0,"success":false,"errorMessage":"denied","requestId":"request-1","endpoint":"wss://example.com/connect","ticket":"ticket-1"}"#;
        let response = serde_json::from_str::<RawOpenConnectionResponse>(body).expect("response");
        let error = response
            .into_connection(body, BodySnippetConfig::default())
            .expect_err("success=false should fail");

        assert_eq!(error.kind(), crate::ErrorKind::Api);
        assert_eq!(error.request_id(), Some("request-1"));
        assert!(error.to_string().contains("denied"));
    }

    #[test]
    fn open_connection_response_rejects_missing_ticket() {
        let error = RawOpenConnectionResponse {
            endpoint: Some("wss://example.com/connect".to_string()),
            ticket: Some(" ".to_string()),
            errcode: Some(0),
            api_code: None,
            errmsg: None,
            success: None,
            request_id: Some("request-1".to_string()),
        }
        .into_connection(
            r#"{"errcode":0,"endpoint":"wss://example.com/connect","ticket":" ","requestId":"request-1"}"#,
            BodySnippetConfig::default(),
        )
        .expect_err("missing ticket should fail");

        assert_eq!(error.kind(), crate::ErrorKind::Api);
        assert_eq!(error.request_id(), Some("request-1"));
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
        let bot = test_bot(client.clone());
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
        let bot = test_bot(client.clone());
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
                        "MessageId":12345,
                        "contentType":"application/json"
                    }
                }"#,
            )
            .await
            .expect("handled");
        let value = serde_json::to_value(handled.ack).expect("json");

        assert_eq!(value["code"], 500);
        assert_eq!(value["headers"]["messageId"], "12345");
        assert!(matches!(
            handled.error,
            Some(StreamFrameError {
                message_id: Some(message_id),
                ..
            }) if message_id == "12345"
        ));
    }

    #[tokio::test]
    async fn system_ping_ack_preserves_non_json_data() {
        let client = DingTalk::builder()
            .app_key_and_secret("client-id", "client-secret")
            .build()
            .expect("client");
        let bot = test_bot(client.clone());
        let stream = StreamClient::builder(client)
            .expect("builder")
            .bot(bot)
            .build()
            .expect("stream");

        let handled = stream
            .handle_text_frame(
                r#"{
                    "specVersion":"1.0",
                    "type":"SYSTEM",
                    "headers":{
                        "topic":"ping",
                        "messageId":"message-1",
                        "contentType":"application/json"
                    },
                    "data":"not-json"
                }"#,
            )
            .await
            .expect("handled");
        let value = serde_json::to_value(handled.ack).expect("json");

        assert_eq!(value["code"], 200);
        assert_eq!(value["headers"]["messageId"], "message-1");
        assert_eq!(value["data"], r#""not-json""#);
        assert!(handled.error.is_none());
    }

    #[test]
    fn reconnect_policy_uses_initial_delay_for_first_retry() {
        let policy = ReconnectPolicy::new(Duration::from_secs(2), Duration::from_secs(30));

        assert_eq!(policy.delay_for_failures(0), Duration::from_secs(2));
        assert_eq!(policy.delay_for_failures(1), Duration::from_secs(2));
        assert_eq!(policy.delay_for_failures(2), Duration::from_secs(4));
        assert_eq!(policy.delay_for_failures(10), Duration::from_secs(30));
    }

    #[test]
    fn reconnect_streak_counts_failures_and_short_connections_not_lifetime_attempts() {
        let policy = ReconnectPolicy::default();
        assert_eq!(policy.failures_after_connection(9, Duration::ZERO), 10);
        assert_eq!(
            policy.failures_after_connection(9, Duration::from_secs(59)),
            10
        );
        assert_eq!(
            policy.failures_after_connection(9, Duration::from_secs(60)),
            0
        );
        assert_eq!(
            policy.failures_after_connection(0, Duration::from_secs(1)),
            1
        );
        assert_eq!(
            policy.failures_after_connection(u32::MAX, Duration::ZERO),
            u32::MAX
        );
        assert!(policy.reset_after(Duration::ZERO).validate().is_err());
        assert!(policy.reset_after(Duration::MAX).validate().is_err());
    }

    #[test]
    fn reconnect_backoff_keeps_growing_until_the_configured_maximum() {
        let policy = ReconnectPolicy::new(Duration::from_millis(1), Duration::from_secs(30));

        assert_eq!(policy.delay_for_failures(11), Duration::from_millis(1024));
        assert_eq!(policy.delay_for_failures(12), Duration::from_millis(2048));
        assert_eq!(policy.delay_for_failures(16), Duration::from_secs(30));
        assert_eq!(policy.delay_for_failures(u32::MAX), Duration::from_secs(30));

        let policy = ReconnectPolicy::new(Duration::from_nanos(1), Duration::MAX);
        assert_eq!(
            policy.delay_for_failures(33),
            Duration::from_nanos(1_u64 << 32)
        );
        assert_eq!(policy.delay_for_failures(u32::MAX), Duration::MAX);
        assert_eq!(
            policy
                .initial_delay(Duration::ZERO)
                .delay_for_failures(u32::MAX),
            Duration::ZERO
        );
    }

    #[test]
    fn reconnect_policy_rejects_overflowing_deadlines() {
        for (policy, expected_field) in [
            (
                ReconnectPolicy::new(Duration::MAX, Duration::MAX),
                "reconnect.initial_delay",
            ),
            (
                ReconnectPolicy::default().max_delay(Duration::MAX),
                "reconnect.max_delay",
            ),
        ] {
            let error = policy.validate().expect_err("deadline must fit");
            assert!(matches!(error, Error::InvalidInput { field, .. } if field == expected_field));
        }
    }

    #[test]
    fn reconnect_policy_builder_helpers_are_validated() {
        let policy = ReconnectPolicy::no_retry()
            .initial_delay(Duration::from_secs(3))
            .max_delay(Duration::from_secs(10));

        assert!(!policy.retry_on_error);
        assert_eq!(policy.delay_for_failures(2), Duration::from_secs(6));
        assert!(policy.validate().is_ok());

        let invalid = policy.max_delay(Duration::from_secs(1));
        let error = invalid.validate().expect_err("invalid policy should fail");
        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn stream_subscription_builders_set_kind_and_topic() {
        let event = StreamSubscription::event("/v1.0/example/events");
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
    fn stream_builder_deduplicates_subscriptions() {
        let client = DingTalk::builder()
            .app_key_and_secret("client-id", "client-secret")
            .build()
            .expect("client");
        let bot = test_bot(client.clone());

        let stream = StreamClient::builder(client)
            .expect("builder")
            .subscriptions(vec![
                StreamSubscription::event("/v1.0/example/events"),
                StreamSubscription::event("/v1.0/example/events"),
                StreamSubscription::callback("/v1.0/example/events"),
            ])
            .bot(bot)
            .build()
            .expect("stream");

        assert_eq!(stream.subscriptions.len(), 2);
        assert_eq!(stream.subscriptions[0].topic(), "/v1.0/example/events");
        assert_eq!(
            stream.subscriptions[0].kind(),
            StreamSubscriptionType::Event
        );
        assert_eq!(
            stream.subscriptions[1].kind(),
            StreamSubscriptionType::Callback
        );
    }

    #[test]
    fn stream_builder_infers_only_card_subscription_for_card_callbacks() {
        let client = DingTalk::builder()
            .app_key_and_secret("client-id", "client-secret")
            .build()
            .expect("client");

        let stream = StreamClient::builder(client)
            .expect("builder")
            .on_card_callback(|_ctx, _event| async {})
            .build()
            .expect("stream");

        assert_eq!(stream.subscriptions.len(), 1);
        assert_eq!(stream.subscriptions[0].topic(), CARD_CALLBACK_TOPIC);
    }

    #[test]
    fn stream_builder_inherits_client_connect_timeout_for_websocket() {
        let client = DingTalk::builder()
            .app_key_and_secret("client-id", "client-secret")
            .connect_timeout(Duration::from_secs(7))
            .build()
            .expect("client");

        let stream = StreamClient::builder(client)
            .expect("builder")
            .on_frame(|_ctx, _frame| async {})
            .build()
            .expect("stream");

        assert_eq!(stream.websocket_connect_timeout, Duration::from_secs(7));
    }

    #[test]
    fn stream_builder_overrides_and_validates_websocket_connect_timeout() {
        let client = DingTalk::builder()
            .app_key_and_secret("client-id", "client-secret")
            .connect_timeout(Duration::from_secs(7))
            .build()
            .expect("client");
        let stream = StreamClient::builder(client.clone())
            .expect("builder")
            .websocket_connect_timeout(Duration::from_secs(9))
            .on_frame(|_ctx, _frame| async {})
            .build()
            .expect("stream");
        let invalid = StreamClient::builder(client)
            .expect("builder")
            .websocket_connect_timeout(Duration::ZERO)
            .on_frame(|_ctx, _frame| async {})
            .build()
            .err()
            .expect("zero websocket timeout should fail");

        assert_eq!(stream.websocket_connect_timeout, Duration::from_secs(9));
        assert_eq!(invalid.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn stream_bot_builder_forwards_websocket_connect_timeout() {
        let stream = StreamBot::builder()
            .client_id_and_secret("client-id", "client-secret")
            .websocket_connect_timeout(Duration::from_secs(9))
            .on_frame(|_ctx, _frame| async {})
            .build()
            .expect("stream bot");

        assert_eq!(
            stream.client.websocket_connect_timeout,
            Duration::from_secs(9)
        );
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

        let result = StreamClient::builder(client)
            .expect("builder")
            .subscriptions(vec![StreamSubscription::callback(" ")])
            .on_frame(|_ctx, _frame| async {})
            .build();
        let Err(error) = result else {
            panic!("empty topic should fail");
        };

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn stream_builder_rejects_subscription_topic_whitespace() {
        let client = DingTalk::builder()
            .app_key_and_secret("client-id", "client-secret")
            .build()
            .expect("client");

        let result = StreamClient::builder(client)
            .expect("builder")
            .subscriptions(vec![StreamSubscription::callback("/v1.0/example events")])
            .on_frame(|_ctx, _frame| async {})
            .build();
        let Err(error) = result else {
            panic!("topic should not contain whitespace");
        };

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn stream_builder_rejects_untrimmed_user_agent() {
        let client = DingTalk::builder()
            .app_key_and_secret("client-id", "client-secret")
            .build()
            .expect("client");

        let result = StreamClient::builder(client)
            .expect("builder")
            .user_agent(" dingding/0.1 ")
            .on_frame(|_ctx, _frame| async {})
            .build();
        let Err(error) = result else {
            panic!("user agent should not be rewritten");
        };

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn stream_builder_requires_bot_or_frame_handler() {
        let client = DingTalk::builder().build().expect("client");

        let result = StreamClient::builder(client).expect("builder").build();
        let Err(error) = result else {
            panic!("handler should be required");
        };

        assert_eq!(error.kind(), crate::ErrorKind::InvalidConfig);
    }

    #[test]
    fn stream_builder_reports_missing_credentials_after_handler_validation() {
        let client = DingTalk::builder().build().expect("client");

        let result = StreamClient::builder(client)
            .expect("builder")
            .on_frame(|_ctx, _frame| async {})
            .build();
        let Err(error) = result else {
            panic!("credentials should be required");
        };

        assert_eq!(error.kind(), crate::ErrorKind::MissingCredentials);
    }

    #[test]
    fn stream_builder_rejects_empty_bot_router() {
        let client = DingTalk::builder()
            .app_key_and_secret("client-id", "client-secret")
            .build()
            .expect("client");
        let bot = Bot::new(client.clone());

        let result = StreamClient::builder(client)
            .expect("builder")
            .bot(bot)
            .build();
        let Err(error) = result else {
            panic!("empty bot should fail");
        };

        assert_eq!(error.kind(), crate::ErrorKind::InvalidConfig);
    }

    #[test]
    fn stream_builder_rejects_invalid_bot_routes() {
        let client = DingTalk::builder()
            .app_key_and_secret("client-id", "client-secret")
            .build()
            .expect("client");
        let bot = Bot::new(client.clone()).route(Route::new(ConversationScope::Any));

        let result = StreamClient::builder(client)
            .expect("builder")
            .bot(bot)
            .build();
        let Err(error) = result else {
            panic!("invalid bot route should fail");
        };

        assert_eq!(error.kind(), crate::ErrorKind::InvalidConfig);
    }

    #[test]
    fn stream_bot_builder_builds_from_credentials() {
        let result = StreamBot::builder()
            .client_id_and_secret("client-id", "client-secret")
            .route(Route::new(ConversationScope::Any).handle(|_ctx| async {}))
            .build();

        assert!(result.is_ok());
    }

    #[test]
    fn stream_bot_builder_requires_route_or_frame_handler() {
        let result = StreamBot::builder()
            .client_id_and_secret("client-id", "client-secret")
            .build();

        let Err(error) = result else {
            panic!("handler should be required");
        };

        assert_eq!(error.kind(), crate::ErrorKind::InvalidConfig);
    }

    #[test]
    fn stream_bot_builder_validates_routes_before_credentials() {
        let result = StreamBot::builder()
            .route(Route::new(ConversationScope::Any))
            .build();

        let Err(error) = result else {
            panic!("invalid route should fail before credentials");
        };

        assert_eq!(error.kind(), crate::ErrorKind::InvalidConfig);
    }

    #[test]
    fn stream_bot_builder_registers_route_shortcuts() {
        let result = StreamBot::builder()
            .client_id_and_secret("client-id", "client-secret")
            .on_group_text_command("/ping", |_ctx| async {})
            .on_private_text_commands(["/help", "help"], |_ctx| async {})
            .on_private_message(MessageType::Picture, |_ctx| async {})
            .build();

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn stream_bot_emits_bot_event_handled() {
        let seen = Arc::new(Mutex::new(Vec::<StreamRunEvent>::new()));
        let seen_events = Arc::clone(&seen);
        let stream_bot = StreamBot::builder()
            .client_id_and_secret("client-id", "client-secret")
            .on_group_text_command("/ping", |_ctx| async {})
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

    #[tokio::test]
    async fn stream_bot_builder_frame_handler_receives_bot_messages_without_routes() {
        let seen = Arc::new(Mutex::new(None::<String>));
        let seen_topic = Arc::clone(&seen);
        let stream_bot = StreamBot::builder()
            .client_id_and_secret("client-id", "client-secret")
            .on_frame(move |_ctx, frame| {
                let seen_topic = Arc::clone(&seen_topic);
                async move {
                    *seen_topic.lock().expect("topic lock") = Some(frame.topic().to_string());
                }
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
            seen.lock().expect("topic lock").as_deref(),
            Some(BOT_MESSAGE_TOPIC)
        );
    }

    #[test]
    fn stream_frame_normalizes_headers_and_unknown_type() {
        let frame = StreamFrame::from_text(
            r#"{
                "specVersion":"1.0",
                "type":" FUTURE ",
                "headers":{
                    "topic":" /v1.0/example/events ",
                    "messageId":" message-1 ",
                    "contentType":"application/json"
                },
                "data":{"ok":true}
            }"#,
        )
        .expect("frame");

        assert_eq!(
            frame.frame_type(),
            &StreamFrameType::Unknown("FUTURE".to_string())
        );
        assert_eq!(frame.topic(), "/v1.0/example/events");
        assert_eq!(frame.message_id(), "message-1");
    }

    #[test]
    fn stream_frame_accepts_numeric_message_id() {
        let frame = StreamFrame::from_text(
            r#"{
                "specVersion":"1.0",
                "type":"EVENT",
                "headers":{
                    "topic":"/v1.0/example/events",
                    "MessageId":12345
                },
                "data":{"ok":true}
            }"#,
        )
        .expect("frame");

        assert_eq!(frame.message_id(), "12345");
        assert_eq!(frame.data_json().expect("data")["ok"], true);
    }

    #[test]
    fn stream_frame_rejects_blank_type() {
        let error = StreamFrame::from_text(
            r#"{
                "specVersion":"1.0",
                "type":" ",
                "headers":{
                    "topic":"/v1.0/example/events",
                    "messageId":"message-1"
                },
                "data":{"ok":true}
            }"#,
        )
        .expect_err("blank frame type should fail");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn stream_frame_rejects_blank_required_headers() {
        let error = StreamFrame::from_text(
            r#"{
                "specVersion":"1.0",
                "type":"EVENT",
                "headers":{
                    "topic":"/v1.0/example/events",
                    "messageId":" "
                },
                "data":{"ok":true}
            }"#,
        )
        .expect_err("blank message id should fail");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn stream_bot_builder_forwards_frame_handler() {
        let result = StreamBot::builder()
            .client_id_and_secret("client-id", "client-secret")
            .subscription(StreamSubscription::event("/v1.0/example/events"))
            .on_frame(|_ctx, _frame| async {})
            .build();

        assert!(result.is_ok());
    }

    #[test]
    fn stream_bot_builder_forwards_response_frame_handler() {
        let result = StreamBot::builder()
            .client_id_and_secret("client-id", "client-secret")
            .subscription(StreamSubscription::event("/v1.0/example/events"))
            .on_frame(|_ctx, _frame| async {
                StreamFrameResponse::json(serde_json::json!({ "accepted": true }))
            })
            .build();

        assert!(result.is_ok());
    }

    #[test]
    fn stream_bot_builder_registers_card_callback_handler() {
        let stream = StreamBot::builder()
            .client_id_and_secret("client-id", "client-secret")
            .on_card_callback(|_ctx, _event| async {})
            .build()
            .expect("stream bot");

        assert!(
            stream
                .client
                .subscriptions
                .iter()
                .any(|subscription| subscription.topic() == CARD_CALLBACK_TOPIC)
        );
        assert!(
            !stream
                .client
                .subscriptions
                .iter()
                .any(|subscription| subscription.topic() == BOT_MESSAGE_TOPIC)
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
            .on_card_callback(move |_ctx, event| {
                let seen_event = Arc::clone(&seen_event);
                async move {
                    *seen_event.lock().expect("card lock") = Some((
                        event.card_biz_id().unwrap_or_default().to_string(),
                        event.action().unwrap_or_default().to_string(),
                        event.action_value().cloned().unwrap_or(Value::Null),
                    ));
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
                    "actionIds": ["approve", 1001],
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
        assert_eq!(
            payload.action_ids(),
            ["approve".to_string(), "1001".to_string()]
        );
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
    fn card_callback_debug_redacts_raw_operator_and_private_values() {
        let event = CardCallbackEvent::from_value(serde_json::json!({
            "type": "CALLBACK",
            "outTrackId": "card-biz-id",
            "corpId": "corp-secret",
            "userId": "user-secret",
            "unionId": "union-secret",
            "actionValue": {
                "token": "action-token"
            },
            "content": {
                "cardPrivateData": {
                    "actionIds": ["approve"],
                    "params": {
                        "token": "private-token"
                    }
                }
            }
        }));
        let payload = event.payload();
        let response = CardCallbackResponse::new()
            .card_data([("token", "response-token")])
            .expect("response");

        let debug = format!("{event:?} {payload:?} {response:?}");

        assert!(debug.contains("<redacted>"));
        assert!(debug.contains("has_user_id"));
        assert!(!debug.contains("corp-secret"));
        assert!(!debug.contains("user-secret"));
        assert!(!debug.contains("union-secret"));
        assert!(!debug.contains("action-token"));
        assert!(!debug.contains("private-token"));
        assert!(!debug.contains("response-token"));
    }

    #[test]
    fn card_callback_payload_parses_json_string_content() {
        let event = CardCallbackEvent::from_value(serde_json::json!({
            "cardBizId": "card-biz-id",
            "content": "{\"cardPrivateData\":{\"actionIds\":\"[\\\"save\\\",1001]\",\"params\":{\"field\":\"value\"}}}"
        }));
        let payload = event.payload();

        assert_eq!(payload.card_biz_id(), Some("card-biz-id"));
        assert_eq!(payload.action(), Some("save"));
        assert_eq!(
            payload.action_ids(),
            ["save".to_string(), "1001".to_string()]
        );
        assert_eq!(payload.action_value().string("field"), Some("value"));
    }

    #[test]
    fn card_callback_accessors_return_normalized_strings() {
        let event = CardCallbackEvent::from_value(serde_json::json!({
            "cardBizId": " card-biz-id ",
            "operatorUserId": " user-1 ",
            "actionValue": {
                "field": " value "
            }
        }));
        let payload = event.payload();

        assert_eq!(event.card_biz_id(), Some("card-biz-id"));
        assert_eq!(event.user_id(), Some("user-1"));
        assert_eq!(
            event
                .action_value()
                .and_then(|value| value.get("field"))
                .and_then(Value::as_str),
            Some(" value ")
        );
        assert_eq!(payload.card_biz_id(), Some("card-biz-id"));
        assert_eq!(payload.operator().user_id(), Some("user-1"));
        assert_eq!(payload.action_value().string("field"), Some("value"));
    }

    #[test]
    fn card_callback_payload_preserves_unparseable_string_content() {
        let event = CardCallbackEvent::from_value(serde_json::json!({
            "cardBizId": "card-biz-id",
            "content": "not-json"
        }));
        let payload = event.payload();

        assert_eq!(
            payload.content().map(CardCallbackContent::raw),
            Some(&Value::String("not-json".to_string()))
        );
    }

    #[test]
    fn card_callback_action_value_treats_null_as_empty() {
        let value = CardCallbackActionValue::from_value(Value::Null);

        assert!(value.is_empty());
        assert_eq!(
            value
                .deserialize::<serde_json::Value>()
                .expect("deserialize"),
            None
        );
    }

    #[test]
    fn card_callback_action_value_decodes_string_encoded_json() {
        let value = CardCallbackActionValue::from_value(Value::String(
            r#"{"field":" value ","count":2}"#.to_string(),
        ));

        assert_eq!(value.string("field"), Some("value"));
        assert_eq!(value.get("count").and_then(Value::as_u64), Some(2));
    }

    #[test]
    fn card_callback_response_serializes_card_param_maps() {
        let response = CardCallbackResponse::new()
            .card_data([("status", "done")])
            .expect("card data")
            .user_private_data([("clicked", "true")])
            .expect("private data")
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

    #[test]
    fn card_callback_response_rejects_empty_param_maps() {
        let error = CardCallbackResponse::new()
            .card_data(Vec::<(&str, &str)>::new())
            .expect_err("empty param map should fail");
        let empty_response = CardCallbackResponse::new()
            .into_stream_response()
            .expect_err("empty card callback response should fail");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(empty_response.kind(), crate::ErrorKind::InvalidInput);
    }

    #[test]
    fn card_callback_response_rejects_invalid_param_keys() {
        let error = CardCallbackResponse::new()
            .card_data([("bad key", "done")])
            .expect_err("card parameter keys should be protocol tokens");
        let untrimmed = CardCallbackResponse::new()
            .card_data([(" status ", "done")])
            .expect_err("card parameter keys should not be rewritten");
        let duplicate = CardCallbackResponse::new()
            .card_data([("status", "done"), ("status", "again")])
            .expect_err("duplicate card parameter keys should fail");

        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(untrimmed.kind(), crate::ErrorKind::InvalidInput);
        assert_eq!(duplicate.kind(), crate::ErrorKind::InvalidInput);
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
            .on_frame(move |_ctx, frame| {
                let seen_frame = Arc::clone(&seen_frame);
                async move {
                    let data = frame.data_json()?;
                    *seen_frame.lock().expect("frame lock") =
                        Some((frame.frame_type().clone(), frame.topic().to_string(), data));
                    Ok::<_, dingding::Error>(())
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
            .on_frame(|_ctx, frame| async move {
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
            .on_frame(move |_ctx, frame| {
                let seen_topic = Arc::clone(&seen_topic);
                async move {
                    *seen_topic.lock().expect("topic lock") = Some(frame.topic().to_string());
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
    fn stream_frame_debug_redacts_string_encoded_json() {
        let data = serde_json::json!({
            "access_token": "frame-secret",
            "content": serde_json::json!({"downloadCode": "download-secret"}).to_string(),
        })
        .to_string();
        let text = serde_json::json!({
            "type": "CALLBACK",
            "headers": {
                "topic": BOT_MESSAGE_TOPIC,
                "messageId": "message-1",
                "metadata": serde_json::json!({"ticket":"header-secret"}).to_string(),
            },
            "data": data,
        })
        .to_string();
        let frame = StreamFrame::from_text(&text).expect("frame");
        let debug = format!("{frame:?}");
        assert!(!debug.contains("frame-secret"));
        assert!(!debug.contains("download-secret"));
        assert!(!debug.contains("header-secret"));
        assert!(debug.contains("<redacted>"));
        assert_eq!(frame.data(), &Value::String(data));
        assert_eq!(
            frame.header("metadata"),
            Some(&Value::String(r#"{"ticket":"header-secret"}"#.into()))
        );
    }

    #[test]
    fn stream_frame_debug_redacts_temporary_credentials() {
        let frame = StreamFrame::from_text(
            r#"{
                "specVersion":"1.0",
                "type":"CALLBACK",
                "headers":{
                    "topic":"/v1.0/im/bot/messages/get",
                    "messageId":"message-1",
                    "ticket":"header-ticket"
                },
                "data":{
                    "conversationType":"2",
                    "msgtype":"file",
                    "sessionWebhook":"https://oapi.dingtalk.com/robot/sendBySession?token=session-token",
                    "content":{
                        "downloadCode":"download-code"
                    }
                }
            }"#,
        )
        .expect("frame");
        let debug = format!("{frame:?}");

        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("header-ticket"));
        assert!(!debug.contains("session-token"));
        assert!(!debug.contains("download-code"));
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
                    "contentType":" application/json "
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
            Some(" application/json ")
        );
        assert_eq!(
            frame.headers().get("CONTENTTYPE").and_then(Value::as_str),
            Some(" application/json ")
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
