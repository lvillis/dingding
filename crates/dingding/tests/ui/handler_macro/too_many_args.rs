#[dingding::handler(scope = any, msg = text)]
async fn ping(
    _ctx: dingding::bot::AnyContext,
    _event: dingding::bot::BotEvent,
    _extra: (),
) -> dingding::Result<()> {
    Ok(())
}

fn main() {}
