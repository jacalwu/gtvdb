//! `gtv-server` binary entrypoint. See [`gtv_server`] for the library.

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let addr = std::env::var("GTV_ADDR")
        .unwrap_or_else(|_| gtv_server::DEFAULT_ADDR.to_string());
    gtv_server::serve(addr.parse()?).await
}
