#[dingding::handler(scope = any, msg = text)]
async unsafe fn ping(_ctx: dingding::bot::AnyContext) -> dingding::Result<()> {
    Ok(())
}

fn main() {}
