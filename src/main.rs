use std::net::SocketAddr;

use tonic::{Request, Response, Status};
use tracing::info;
use uuid::Uuid;

pub mod broker {
    tonic::include_proto!("broker.v1");
}

use broker::broker_server::{Broker, BrokerServer};
use broker::{
    HealthRequest, HealthResponse,
    PublishRequest, PublishResponse,
};

#[derive(Default)]
struct BrokerService;

#[tonic::async_trait]
impl Broker for BrokerService {
    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            status: crate::broker::HealthStatus::HsServing as i32,
        }))
    }

    async fn publish(
        &self,
        request: Request<PublishRequest>,
    ) -> Result<Response<PublishResponse>, Status> {
        let req = request.into_inner();

        info!(
            queue = %req.queue,
            payload_len = req.payload.len(),
            "message published"
        );

        Ok(Response::new(PublishResponse {
            message_id: Uuid::new_v4().to_string(),
        }))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let addr: SocketAddr = "127.0.0.1:50051".parse()?;

    info!("starting broker on {}", addr);

    let service = BrokerService::default();

    tonic::transport::Server::builder()
        .add_service(BrokerServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
