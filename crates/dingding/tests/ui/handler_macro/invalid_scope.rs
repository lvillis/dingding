#[dingding::handler(scope = channel, msg = text)]
async fn ping(_ctx: dingding::bot::AnyContext) -> dingding::Result<()> {
    Ok(())
}

fn main() {}
