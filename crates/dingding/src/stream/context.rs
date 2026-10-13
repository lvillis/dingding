use std::sync::Arc;

use crate::DingTalk;

/// Shared SDK client and application state for Stream frame and card callbacks.
///
/// The client uses the effective Stream credentials. State is shared with the bot
/// router, including when only frame or card callbacks are registered.
pub struct StreamContext<T = ()> {
    pub(super) client: DingTalk,
    pub(super) state: Arc<T>,
}

impl<T> Clone for StreamContext<T> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            state: Arc::clone(&self.state),
        }
    }
}

impl<T> StreamContext<T> {
    /// Returns the SDK client with the effective Stream credentials.
    #[must_use]
    pub fn client(&self) -> &DingTalk {
        &self.client
    }

    /// Borrows the application state, checked at compile time when registering the handler.
    #[must_use]
    pub fn state(&self) -> &T {
        &self.state
    }

    /// Clones the state handle without requiring `T: Clone`.
    #[must_use]
    pub fn shared_state(&self) -> Arc<T> {
        Arc::clone(&self.state)
    }
}
