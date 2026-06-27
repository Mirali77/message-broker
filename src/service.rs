use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::{Mutex, Notify};
use tonic::{Request, Response, Status};
use tracing::info;
use uuid::Uuid;

use crate::message::StoredMessage;
use crate::proto::broker_server::{Broker, BrokerServer};
use crate::proto::{
    self, AckRequest, AckResponse, BrokerMessage, HealthRequest, HealthResponse, NackRequest,
    NackResponse, PublishRequest, PublishResponse, PullRequest, PullResponse, QueueStatsRequest,
    QueueStatsResponse,
};
use crate::state::{BrokerState, InFlightMessage};
use crate::storage::DurableStore;

pub(crate) const DEFAULT_VISIBILITY_TIMEOUT: Duration = Duration::from_secs(30);
pub(crate) const DEFAULT_MAX_MESSAGES: usize = 1;
pub(crate) const MAX_BATCH_SIZE: usize = 100;

#[derive(Clone, Debug, Default)]
pub struct BrokerService {
    state: Arc<Mutex<BrokerState>>,
    notify: Arc<Notify>,
    storage: Option<Arc<DurableStore>>,
}

impl BrokerService {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_storage_path(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let storage = Arc::new(DurableStore::open(path)?);
        let state = storage.load()?;

        Ok(Self {
            state: Arc::new(Mutex::new(state)),
            notify: Arc::new(Notify::new()),
            storage: Some(storage),
        })
    }

    fn persist_state(&self, state: &BrokerState) -> Result<(), Status> {
        if let Some(storage) = &self.storage {
            storage.save(state).map_err(|error| {
                Status::internal(format!("failed to persist broker state: {error}"))
            })?;
        }

        Ok(())
    }

    async fn pull_available(
        &self,
        queue: &str,
        max_messages: usize,
        visibility_timeout: Duration,
    ) -> Result<Vec<BrokerMessage>, Status> {
        let mut state = self.state.lock().await;
        let expired_requeued = state.requeue_expired(tokio::time::Instant::now());

        let mut messages = Vec::new();

        while messages.len() < max_messages {
            let Some(mut message) = state.ready.entry(queue.to_owned()).or_default().pop_front()
            else {
                break;
            };

            message.attempts = message.attempts.saturating_add(1);
            let delivery_id = Uuid::new_v4().to_string();
            let response_message = BrokerMessage {
                message_id: message.message_id.clone(),
                delivery_id: delivery_id.clone(),
                queue: message.queue.clone(),
                payload: message.payload.clone(),
                attempts: message.attempts,
                created_at_unix_ms: message.created_at_unix_ms,
            };

            state.in_flight.insert(
                delivery_id,
                InFlightMessage {
                    message,
                    deadline: tokio::time::Instant::now() + visibility_timeout,
                },
            );
            messages.push(response_message);
        }

        if expired_requeued || !messages.is_empty() {
            self.persist_state(&state)?;
        }

        Ok(messages)
    }
}

#[tonic::async_trait]
impl Broker for BrokerService {
    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            status: proto::HealthStatus::HsServing as i32,
        }))
    }

    async fn publish(
        &self,
        request: Request<PublishRequest>,
    ) -> Result<Response<PublishResponse>, Status> {
        let req = request.into_inner();
        let queue = normalize_queue(req.queue)?;
        let message_id = Uuid::new_v4().to_string();
        let payload_len = req.payload.len();

        let message = StoredMessage {
            message_id: message_id.clone(),
            queue: queue.clone(),
            payload: req.payload,
            attempts: 0,
            created_at_unix_ms: unix_time_ms(),
        };

        {
            let mut state = self.state.lock().await;
            state
                .ready
                .entry(queue.clone())
                .or_default()
                .push_back(message);
            self.persist_state(&state)?;
        }

        self.notify.notify_waiters();
        info!(%queue, payload_len, %message_id, "message published");

        Ok(Response::new(PublishResponse { message_id }))
    }

    async fn pull(&self, request: Request<PullRequest>) -> Result<Response<PullResponse>, Status> {
        let req = request.into_inner();
        let queue = normalize_queue(req.queue)?;
        let max_messages = normalize_max_messages(req.max_messages);
        let visibility_timeout = normalize_visibility_timeout(req.visibility_timeout_ms);
        let wait_timeout = Duration::from_millis(req.wait_timeout_ms);
        let deadline = tokio::time::Instant::now() + wait_timeout;

        loop {
            let messages = self
                .pull_available(&queue, max_messages, visibility_timeout)
                .await?;

            if !messages.is_empty() || tokio::time::Instant::now() >= deadline {
                return Ok(Response::new(PullResponse { messages }));
            }

            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            tokio::select! {
                _ = self.notify.notified() => {}
                _ = tokio::time::sleep(remaining) => {}
            }
        }
    }

    async fn ack(&self, request: Request<AckRequest>) -> Result<Response<AckResponse>, Status> {
        let req = request.into_inner();
        let delivery_id = normalize_delivery_id(req.delivery_id)?;
        let acknowledged = {
            let mut state = self.state.lock().await;
            let expired_requeued = state.requeue_expired(tokio::time::Instant::now());
            let acknowledged = state.in_flight.remove(&delivery_id).is_some();
            if expired_requeued || acknowledged {
                self.persist_state(&state)?;
            }
            acknowledged
        };

        Ok(Response::new(AckResponse { acknowledged }))
    }

    async fn nack(&self, request: Request<NackRequest>) -> Result<Response<NackResponse>, Status> {
        let req = request.into_inner();
        let delivery_id = normalize_delivery_id(req.delivery_id)?;
        let accepted = {
            let mut state = self.state.lock().await;
            let expired_requeued = state.requeue_expired(tokio::time::Instant::now());

            if let Some(delivery) = state.in_flight.remove(&delivery_id) {
                if req.requeue {
                    state
                        .ready
                        .entry(delivery.message.queue.clone())
                        .or_default()
                        .push_front(delivery.message);
                }
                self.persist_state(&state)?;
                true
            } else {
                if expired_requeued {
                    self.persist_state(&state)?;
                }
                false
            }
        };

        if accepted && req.requeue {
            self.notify.notify_waiters();
        }

        Ok(Response::new(NackResponse { accepted }))
    }

    async fn queue_stats(
        &self,
        request: Request<QueueStatsRequest>,
    ) -> Result<Response<QueueStatsResponse>, Status> {
        let req = request.into_inner();
        let queue = normalize_queue(req.queue)?;
        let (ready, in_flight) = {
            let mut state = self.state.lock().await;
            if state.requeue_expired(tokio::time::Instant::now()) {
                self.persist_state(&state)?;
            }
            state.queue_stats(&queue)
        };

        Ok(Response::new(QueueStatsResponse {
            queue,
            ready,
            in_flight,
        }))
    }
}

pub async fn serve(
    addr: SocketAddr,
    service: BrokerService,
) -> Result<(), tonic::transport::Error> {
    tonic::transport::Server::builder()
        .add_service(BrokerServer::new(service))
        .serve(addr)
        .await
}

pub(crate) fn normalize_queue(queue: String) -> Result<String, Status> {
    let queue = queue.trim().to_owned();
    if queue.is_empty() {
        return Err(Status::invalid_argument("queue must not be empty"));
    }

    Ok(queue)
}

pub(crate) fn normalize_delivery_id(delivery_id: String) -> Result<String, Status> {
    let delivery_id = delivery_id.trim().to_owned();
    if delivery_id.is_empty() {
        return Err(Status::invalid_argument("delivery_id must not be empty"));
    }

    Ok(delivery_id)
}

pub(crate) fn normalize_max_messages(max_messages: u32) -> usize {
    if max_messages == 0 {
        DEFAULT_MAX_MESSAGES
    } else {
        (max_messages as usize).min(MAX_BATCH_SIZE)
    }
}

pub(crate) fn normalize_visibility_timeout(visibility_timeout_ms: u64) -> Duration {
    if visibility_timeout_ms == 0 {
        DEFAULT_VISIBILITY_TIMEOUT
    } else {
        Duration::from_millis(visibility_timeout_ms)
    }
}

fn unix_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests;
