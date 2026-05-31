#[dingding::handler(scope = any, msg = text)]
async fn ping<T>(_ctx: dingding::bot::AnyContext) -> dingding::Result<()> {
    Ok(())
}

fn main() {}
