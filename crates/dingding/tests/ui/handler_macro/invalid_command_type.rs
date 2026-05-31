#[dingding::handler(scope = any, msg = text, command = 123)]
async fn ping(_ctx: dingding::bot::AnyContext) -> dingding::Result<()> {
    Ok(())
}

fn main() {}
