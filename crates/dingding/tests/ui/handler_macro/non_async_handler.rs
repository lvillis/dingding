#[dingding::handler(scope = any, msg = text)]
fn ping(_ctx: dingding::bot::AnyContext) -> dingding::Result<()> {
    Ok(())
}

fn main() {}
