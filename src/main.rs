use std::net::SocketAddr;

use message_brocker::{BrokerService, serve};
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let addr = std::env::var("BROKER_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:50051".to_owned())
        .parse::<SocketAddr>()?;

    let service = match std::env::var("BROKER_DATA_DIR") {
        Ok(path) => {
            info!(%path, "durable storage enabled");
            BrokerService::with_storage_path(path)?
        }
        Err(_) => BrokerService::new(),
    };

    info!("starting broker on {}", addr);
    serve(addr, service).await?;

    Ok(())
}
