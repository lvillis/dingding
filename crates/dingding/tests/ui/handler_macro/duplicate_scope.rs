#[dingding::handler(scope = group, scope = any)]
async fn ping() -> dingding::Result<()> {
    Ok(())
}

fn main() {}
