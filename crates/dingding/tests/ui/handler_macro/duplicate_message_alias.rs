#[dingding::handler(msg = text, message = any)]
async fn ping() -> dingding::Result<()> {
    Ok(())
}

fn main() {}
