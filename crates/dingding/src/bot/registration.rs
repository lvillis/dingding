use std::future::Future;

use super::{Bot, BotContext, BotEvent, ConversationScope, MessageType, Route};
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
                F: Fn(BotContext, BotEvent) -> Fut + Send + Sync + 'static,
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
                F: Fn(BotContext, BotEvent) -> Fut + Send + Sync + 'static,
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
                F: Fn(BotContext, BotEvent) -> Fut + Send + Sync + 'static,
                Fut: Future + Send + 'static,
                Fut::Output: IntoHandlerResult,
            {
                self.route(Route::new(scope).message_type(message_type).handle(handler))
            }

            /// Registers a group command returning `()` or an SDK/application result.
            #[must_use]
            pub fn on_group_text_command<F, Fut>(
                self,
                command: impl Into<String>,
                handler: F,
            ) -> Self
            where
                F: Fn(BotContext, BotEvent) -> Fut + Send + Sync + 'static,
                Fut: Future + Send + 'static,
                Fut::Output: IntoHandlerResult,
            {
                self.on_text_command(ConversationScope::Group, command, handler)
            }

            /// Registers group command aliases returning `()` or an SDK/application result.
            #[must_use]
            pub fn on_group_text_commands<I, S, F, Fut>(self, commands: I, handler: F) -> Self
            where
                I: IntoIterator<Item = S>,
                S: Into<String>,
                F: Fn(BotContext, BotEvent) -> Fut + Send + Sync + 'static,
                Fut: Future + Send + 'static,
                Fut::Output: IntoHandlerResult,
            {
                self.on_text_commands(ConversationScope::Group, commands, handler)
            }

            /// Registers a private-chat command returning `()` or an SDK/application result.
            #[must_use]
            pub fn on_private_text_command<F, Fut>(
                self,
                command: impl Into<String>,
                handler: F,
            ) -> Self
            where
                F: Fn(BotContext, BotEvent) -> Fut + Send + Sync + 'static,
                Fut: Future + Send + 'static,
                Fut::Output: IntoHandlerResult,
            {
                self.on_text_command(ConversationScope::Private, command, handler)
            }

            /// Registers private-chat command aliases returning `()` or an SDK/application result.
            #[must_use]
            pub fn on_private_text_commands<I, S, F, Fut>(self, commands: I, handler: F) -> Self
            where
                I: IntoIterator<Item = S>,
                S: Into<String>,
                F: Fn(BotContext, BotEvent) -> Fut + Send + Sync + 'static,
                Fut: Future + Send + 'static,
                Fut::Output: IntoHandlerResult,
            {
                self.on_text_commands(ConversationScope::Private, commands, handler)
            }

            /// Registers a group message handler returning `()` or an SDK/application result.
            #[must_use]
            pub fn on_group_message<F, Fut>(self, message_type: MessageType, handler: F) -> Self
            where
                F: Fn(BotContext, BotEvent) -> Fut + Send + Sync + 'static,
                Fut: Future + Send + 'static,
                Fut::Output: IntoHandlerResult,
            {
                self.on_message(ConversationScope::Group, message_type, handler)
            }

            /// Registers a private message handler returning `()` or an SDK/application result.
            #[must_use]
            pub fn on_private_message<F, Fut>(self, message_type: MessageType, handler: F) -> Self
            where
                F: Fn(BotContext, BotEvent) -> Fut + Send + Sync + 'static,
                Fut: Future + Send + 'static,
                Fut::Output: IntoHandlerResult,
            {
                self.on_message(ConversationScope::Private, message_type, handler)
            }

            /// Registers a fallback returning `()` or an SDK/application result.
            #[must_use]
            pub fn fallback<F, Fut>(self, handler: F) -> Self
            where
                F: Fn(BotContext, BotEvent) -> Fut + Send + Sync + 'static,
                Fut: Future + Send + 'static,
                Fut::Output: IntoHandlerResult,
            {
                self.fallback_route(Route::new(ConversationScope::Any).handle(handler))
            }

            /// Registers an unmatched-text fallback returning `()` or an SDK/application result.
            #[must_use]
            pub fn on_unmatched_text<F, Fut>(self, scope: ConversationScope, handler: F) -> Self
            where
                F: Fn(BotContext, BotEvent) -> Fut + Send + Sync + 'static,
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
