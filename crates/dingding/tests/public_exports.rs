#[cfg(feature = "openapi")]
#[test]
fn new_openapi_types_are_available_from_both_import_surfaces() {
    fn same<T>(_: Option<T>, _: Option<T>) {}
    use dingding::{prelude, types};
    same(
        None::<prelude::MediaFileUpload>,
        None::<types::MediaFileUpload>,
    );
    same(
        None::<prelude::DownloadedFileInfo>,
        None::<types::DownloadedFileInfo>,
    );
    same(
        None::<prelude::GroupMessagePages>,
        None::<types::GroupMessagePages>,
    );
    same(
        None::<prelude::RobotReplyTarget>,
        None::<types::RobotReplyTarget>,
    );
}

#[cfg(feature = "stream")]
#[test]
fn stream_context_is_available_from_both_import_surfaces() {
    let context: Option<dingding::prelude::StreamContext> = None;
    let _: Option<dingding::types::StreamContext> = context;
    let error: Option<dingding::prelude::StreamError> = None;
    let _: Option<dingding::types::StreamError> = error;
    let reason = dingding::prelude::StreamCancellationReason::DisconnectTimeout;
    let _: dingding::types::StreamCancellationReason = reason;
}
