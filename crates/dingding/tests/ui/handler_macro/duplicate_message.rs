#[dingding::handler(msg = text, msg = any)]
async fn ping() -> dingding::Result<()> {
    Ok(())
}

fn main() {}
