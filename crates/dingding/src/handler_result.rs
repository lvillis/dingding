use std::future::Future;

use crate::{BoxError, Error, HandlerResult, Result};

/// Gives an async block a [`HandlerResult`] output without boxing its future.
///
/// Use inside a handler closure to infer the error type of `?` and `Ok(...)`
/// while mixing SDK and application errors. This returns the future unchanged;
/// it does not spawn work or catch panics. Typed functions need no adapter.
///
/// ```
/// # #[cfg(feature = "bot")]
/// # fn example() {
/// use dingding::prelude::*;
/// let _ = Route::new(Scope::Any).handle(|ctx| handler_future(async move {
///     let content = String::from_utf8(vec![104, 105])?;
///     ctx.reply_text(content).await?;
///     Ok(())
/// }));
/// # }
/// ```
pub fn handler_future<T, F>(future: F) -> F
where
    F: Future<Output = HandlerResult<T>>,
{
    future
}

/// Normalizes a bot or Stream handler's return value.
///
/// Bot handlers can return `()` or a result with any error convertible into
/// [`BoxError`]. Stream callbacks additionally accept a
/// `StreamFrameResponse` or `CardCallbackResponse`, directly or inside a result. Returning `()` from a
/// Stream callback produces an empty acknowledgement payload.
///
/// Use [`crate::HandlerResult`] for named handlers combining SDK and application
/// errors, or [`handler_future`] inside a closure to infer that result's error type.
/// A handler that cannot fail can simply return `()`.
pub trait IntoHandlerResult<T = ()> {
    /// Converts the output, preserving SDK error metadata and application sources.
    fn into_handler_result(self) -> Result<T>;
}

impl IntoHandlerResult for () {
    fn into_handler_result(self) -> Result<()> {
        Ok(())
    }
}

impl<T, V, E> IntoHandlerResult<T> for std::result::Result<V, E>
where
    V: IntoHandlerResult<T>,
    E: Into<BoxError>,
{
    fn into_handler_result(self) -> Result<T> {
        self.map_err(Error::handler)?.into_handler_result()
    }
}

#[cfg(feature = "stream")]
impl IntoHandlerResult<crate::stream::StreamFrameResponse> for () {
    fn into_handler_result(self) -> Result<crate::stream::StreamFrameResponse> {
        Ok(crate::stream::StreamFrameResponse::empty())
    }
}

#[cfg(feature = "stream")]
impl IntoHandlerResult<crate::stream::StreamFrameResponse> for crate::stream::StreamFrameResponse {
    fn into_handler_result(self) -> Result<Self> {
        Ok(self)
    }
}

#[cfg(feature = "stream")]
impl IntoHandlerResult<crate::stream::StreamFrameResponse> for crate::stream::CardCallbackResponse {
    fn into_handler_result(self) -> Result<crate::stream::StreamFrameResponse> {
        self.into_stream_response()
    }
}
