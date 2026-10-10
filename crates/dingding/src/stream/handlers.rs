use std::{future::Future, sync::Arc};

use super::{
    CardCallbackEvent, StreamBotBuilder, StreamClientBuilder, StreamContext, StreamFrame,
    StreamFrameResponse,
};
use crate::IntoHandlerResult;

macro_rules! impl_stream_handlers {
    ($target:ty) => {
        impl $target {
            /// Registers a callback/event handler for frames not handled by the bot router.
            ///
            /// Receives the shared context followed by the frame. Return `()` for an empty
            /// acknowledgement, or [`StreamFrameResponse`] / [`super::CardCallbackResponse`]
            /// for a response payload, optionally wrapped in an SDK or application result.
            /// Handler failures produce a failure acknowledgement, not a success response.
            #[must_use]
            pub fn on_frame<F, Fut>(mut self, handler: F) -> Self
            where
                F: Fn(StreamContext, StreamFrame) -> Fut + Send + Sync + 'static,
                Fut: Future + Send + 'static,
                Fut::Output: IntoHandlerResult<StreamFrameResponse>,
            {
                let handler = Arc::new(handler);
                self.frame_handler = Some(Arc::new(move |ctx, frame| {
                    let handler = Arc::clone(&handler);
                    Box::pin(async move { handler(ctx, frame).await.into_handler_result() })
                }));
                self
            }

            /// Registers an interactive-card callback and automatically adds its subscription.
            ///
            /// Receives the shared context followed by the event. Return `()`,
            /// [`super::CardCallbackResponse`], [`StreamFrameResponse`], or a result containing
            /// any of these values. Card responses are validated before acknowledgement.
            /// SDK errors retain their metadata; application errors retain their source.
            #[must_use]
            pub fn on_card_callback<F, Fut>(mut self, handler: F) -> Self
            where
                F: Fn(StreamContext, CardCallbackEvent) -> Fut + Send + Sync + 'static,
                Fut: Future + Send + 'static,
                Fut::Output: IntoHandlerResult<StreamFrameResponse>,
            {
                let handler = Arc::new(handler);
                self.card_callback_handler = Some(Arc::new(move |ctx, event| {
                    let handler = Arc::clone(&handler);
                    Box::pin(async move { handler(ctx, event).await.into_handler_result() })
                }));
                self
            }
        }
    };
}

impl_stream_handlers!(StreamBotBuilder);
impl_stream_handlers!(StreamClientBuilder);
