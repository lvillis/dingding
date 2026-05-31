#[dingding::handler(scope = any, msg = tex)]
async fn ping(_ctx: dingding::bot::AnyContext) -> dingding::Result<()> {
    Ok(())
}

fn main() {}
