use crate::{BoxError, Error, Result};

/// Normalizes a bot or Stream handler's return value.
///
/// Bot handlers can return `()` or a result with any error convertible into
/// [`BoxError`]. Stream callbacks additionally accept a
/// `StreamFrameResponse`, directly or inside a result. Returning `()` from a
/// Stream callback produces an empty acknowledgement payload.
///
/// Use [`crate::HandlerResult`] for named handlers combining SDK and application
/// errors. A result-only closure needs an explicit error type, for example
/// `Ok::<_, std::io::Error>(())`; a handler that cannot fail can simply return `()`.
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
