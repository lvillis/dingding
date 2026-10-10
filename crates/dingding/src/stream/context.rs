use std::any::type_name;

use crate::{DingTalk, Error, Result, bot::BotState};

/// Shared SDK client and application state for Stream frame and card callbacks.
///
/// The client uses the effective Stream credentials. State is shared with the bot
/// router, including when only frame or card callbacks are registered.
#[derive(Clone)]
pub struct StreamContext {
    pub(super) client: DingTalk,
    pub(super) state: Option<BotState>,
}

impl StreamContext {
    /// Returns the SDK client with the effective Stream credentials.
    #[must_use]
    pub fn client(&self) -> &DingTalk {
        &self.client
    }

    /// Returns shared state when it was configured with this type.
    #[must_use]
    pub fn state<T>(&self) -> Option<&T>
    where
        T: Send + Sync + 'static,
    {
        self.state.as_deref()?.downcast_ref::<T>()
    }

    /// Returns shared state or an invalid-configuration error.
    pub fn state_required<T>(&self) -> Result<&T>
    where
        T: Send + Sync + 'static,
    {
        self.state::<T>().ok_or_else(|| {
            Error::InvalidConfig(format!(
                "stream state `{}` is not configured",
                type_name::<T>()
            ))
        })
    }
}
