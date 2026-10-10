use std::future::Future;

use super::{Bot, BotContext, ConversationScope, GroupContext, MessageType, PrivateContext, Route};
use crate::IntoHandlerResult;

// Keep the Bot and StreamBotBuilder registration surfaces identical.
macro_rules! impl_registration {
    ($target:ty) => {
        impl $target {
            /// Registers a text command handler returning `()` or an SDK/application result.
            #[must_use]
            pub fn on_text_command<F, Fut>(
                self,
                scope: ConversationScope,
                command: impl Into<String>,
                handler: F,
            ) -> Self
            where
                F: Fn(BotContext) -> Fut + Send + Sync + 'static,
                Fut: Future + Send + 'static,
                Fut::Output: IntoHandlerResult,
            {
                self.route(
                    Route::new(scope)
                        .message_type(MessageType::Text)
                        .command(command)
                        .handle(handler),
                )
            }

            /// Registers command aliases with `()` or an SDK/application result.
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
                F: Fn(BotContext) -> Fut + Send + Sync + 'static,
                Fut: Future + Send + 'static,
                Fut::Output: IntoHandlerResult,
            {
                self.route(
                    Route::new(scope)
                        .message_type(MessageType::Text)
                        .commands(commands)
                        .handle(handler),
                )
            }

            /// Registers a message handler returning `()` or an SDK/application result.
            #[must_use]
            pub fn on_message<F, Fut>(
                self,
                scope: ConversationScope,
                message_type: MessageType,
                handler: F,
            ) -> Self
            where
                F: Fn(BotContext) -> Fut + Send + Sync + 'static,
                Fut: Future + Send + 'static,
                Fut::Output: IntoHandlerResult,
            {
                self.route(Route::new(scope).message_type(message_type).handle(handler))
            }

            /// Registers a group command returning `()` or an SDK/application result.
            /// The handler receives a [`GroupContext`], matching [`Route::handle_group`].
            #[must_use]
            pub fn on_group_text_command<F, Fut>(
                self,
                command: impl Into<String>,
                handler: F,
            ) -> Self
            where
                F: Fn(GroupContext) -> Fut + Send + Sync + 'static,
                Fut: Future + Send + 'static,
                Fut::Output: IntoHandlerResult,
            {
                self.route(
                    Route::new(ConversationScope::Group)
                        .message_type(MessageType::Text)
                        .command(command)
                        .handle_group(handler),
                )
            }

            /// Registers group command aliases returning `()` or an SDK/application result.
            /// The handler receives a [`GroupContext`].
            #[must_use]
            pub fn on_group_text_commands<I, S, F, Fut>(self, commands: I, handler: F) -> Self
            where
                I: IntoIterator<Item = S>,
                S: Into<String>,
                F: Fn(GroupContext) -> Fut + Send + Sync + 'static,
                Fut: Future + Send + 'static,
                Fut::Output: IntoHandlerResult,
            {
                self.route(
                    Route::new(ConversationScope::Group)
                        .message_type(MessageType::Text)
                        .commands(commands)
                        .handle_group(handler),
                )
            }

            /// Registers a private-chat command returning `()` or an SDK/application result.
            /// The handler receives a [`PrivateContext`], matching [`Route::handle_private`].
            #[must_use]
            pub fn on_private_text_command<F, Fut>(
                self,
                command: impl Into<String>,
                handler: F,
            ) -> Self
            where
                F: Fn(PrivateContext) -> Fut + Send + Sync + 'static,
                Fut: Future + Send + 'static,
                Fut::Output: IntoHandlerResult,
            {
                self.route(
                    Route::new(ConversationScope::Private)
                        .message_type(MessageType::Text)
                        .command(command)
                        .handle_private(handler),
                )
            }

            /// Registers private-chat command aliases returning `()` or an SDK/application result.
            /// The handler receives a [`PrivateContext`].
            #[must_use]
            pub fn on_private_text_commands<I, S, F, Fut>(self, commands: I, handler: F) -> Self
            where
                I: IntoIterator<Item = S>,
                S: Into<String>,
                F: Fn(PrivateContext) -> Fut + Send + Sync + 'static,
                Fut: Future + Send + 'static,
                Fut::Output: IntoHandlerResult,
            {
                self.route(
                    Route::new(ConversationScope::Private)
                        .message_type(MessageType::Text)
                        .commands(commands)
                        .handle_private(handler),
                )
            }

            /// Registers a group message handler returning `()` or an SDK/application result.
            /// The handler receives a [`GroupContext`].
            #[must_use]
            pub fn on_group_message<F, Fut>(self, message_type: MessageType, handler: F) -> Self
            where
                F: Fn(GroupContext) -> Fut + Send + Sync + 'static,
                Fut: Future + Send + 'static,
                Fut::Output: IntoHandlerResult,
            {
                self.route(
                    Route::new(ConversationScope::Group)
                        .message_type(message_type)
                        .handle_group(handler),
                )
            }

            /// Registers a private message handler returning `()` or an SDK/application result.
            /// The handler receives a [`PrivateContext`].
            #[must_use]
            pub fn on_private_message<F, Fut>(self, message_type: MessageType, handler: F) -> Self
            where
                F: Fn(PrivateContext) -> Fut + Send + Sync + 'static,
                Fut: Future + Send + 'static,
                Fut::Output: IntoHandlerResult,
            {
                self.route(
                    Route::new(ConversationScope::Private)
                        .message_type(message_type)
                        .handle_private(handler),
                )
            }

            /// Appends a catch-all fallback returning `()` or an SDK/application result.
            ///
            /// Register this last, after specific fallbacks. See [`Self::fallback_route`].
            #[must_use]
            pub fn fallback<F, Fut>(self, handler: F) -> Self
            where
                F: Fn(BotContext) -> Fut + Send + Sync + 'static,
                Fut: Future + Send + 'static,
                Fut::Output: IntoHandlerResult,
            {
                self.fallback_route(Route::new(ConversationScope::Any).handle(handler))
            }

            /// Appends an unmatched-text fallback returning `()` or an SDK/application result.
            ///
            /// Fallbacks accumulate in registration order. See [`Self::fallback_route`].
            #[must_use]
            pub fn on_unmatched_text<F, Fut>(self, scope: ConversationScope, handler: F) -> Self
            where
                F: Fn(BotContext) -> Fut + Send + Sync + 'static,
                Fut: Future + Send + 'static,
                Fut::Output: IntoHandlerResult,
            {
                self.fallback_route(
                    Route::new(scope)
                        .message_type(MessageType::Text)
                        .handle(handler),
                )
            }
        }
    };
}

impl_registration!(Bot);
#[cfg(feature = "stream")]
impl_registration!(crate::stream::StreamBotBuilder);
