use std::time::Duration;

use tonic::{Code, Request};
use uuid::Uuid;

use super::{
    BrokerService, DEFAULT_MAX_MESSAGES, DEFAULT_VISIBILITY_TIMEOUT, MAX_BATCH_SIZE,
    normalize_delivery_id, normalize_max_messages, normalize_queue, normalize_visibility_timeout,
};
use crate::proto::broker_server::Broker;
use crate::proto::{
    self, AckRequest, AckResponse, HealthRequest, NackRequest, NackResponse, PublishRequest,
    PullRequest, PullResponse, QueueStatsRequest, QueueStatsResponse,
};

async fn publish(service: &BrokerService, queue: &str, payload: &[u8]) -> String {
    service
        .publish(Request::new(PublishRequest {
            queue: queue.to_owned(),
            payload: payload.to_vec(),
        }))
        .await
        .unwrap()
        .into_inner()
        .message_id
}

async fn pull(
    service: &BrokerService,
    queue: &str,
    max_messages: u32,
    visibility_timeout_ms: u64,
    wait_timeout_ms: u64,
) -> PullResponse {
    service
        .pull(Request::new(PullRequest {
            queue: queue.to_owned(),
            max_messages,
            visibility_timeout_ms,
            wait_timeout_ms,
        }))
        .await
        .unwrap()
        .into_inner()
}

async fn pull_once(service: &BrokerService, queue: &str) -> PullResponse {
    pull(service, queue, 10, 50, 0).await
}

async fn ack(service: &BrokerService, delivery_id: impl Into<String>) -> AckResponse {
    service
        .ack(Request::new(AckRequest {
            delivery_id: delivery_id.into(),
        }))
        .await
        .unwrap()
        .into_inner()
}

async fn nack(
    service: &BrokerService,
    delivery_id: impl Into<String>,
    requeue: bool,
) -> NackResponse {
    service
        .nack(Request::new(NackRequest {
            delivery_id: delivery_id.into(),
            requeue,
        }))
        .await
        .unwrap()
        .into_inner()
}

async fn stats(service: &BrokerService, queue: &str) -> QueueStatsResponse {
    service
        .queue_stats(Request::new(QueueStatsRequest {
            queue: queue.to_owned(),
        }))
        .await
        .unwrap()
        .into_inner()
}

#[tokio::test]
async fn health_reports_serving() {
    let service = BrokerService::new();

    let health = service
        .health(Request::new(HealthRequest {}))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(health.status, proto::HealthStatus::HsServing as i32);
}

#[tokio::test]
async fn publish_pull_and_ack_removes_message() {
    let service = BrokerService::new();
    let message_id = publish(&service, "jobs", b"hello").await;

    let pulled = pull_once(&service, "jobs").await;
    assert_eq!(pulled.messages.len(), 1);
    assert_eq!(pulled.messages[0].message_id, message_id);
    assert_eq!(pulled.messages[0].payload, b"hello");
    assert_eq!(pulled.messages[0].attempts, 1);

    let ack = service
        .ack(Request::new(AckRequest {
            delivery_id: pulled.messages[0].delivery_id.clone(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(ack.acknowledged);

    let stats = stats(&service, "jobs").await;
    assert_eq!(stats.ready, 0);
    assert_eq!(stats.in_flight, 0);
}

#[tokio::test]
async fn queue_names_are_trimmed_on_publish_pull_and_stats() {
    let service = BrokerService::new();
    publish(&service, "  jobs  ", b"trimmed").await;

    let stats = stats(&service, " jobs ").await;
    assert_eq!(stats.queue, "jobs");
    assert_eq!(stats.ready, 1);
    assert_eq!(stats.in_flight, 0);

    let pulled = pull_once(&service, " jobs ").await;
    assert_eq!(pulled.messages.len(), 1);
    assert_eq!(pulled.messages[0].queue, "jobs");
    assert_eq!(pulled.messages[0].payload, b"trimmed");
}

#[tokio::test]
async fn queues_are_isolated() {
    let service = BrokerService::new();
    publish(&service, "alpha", b"a").await;
    publish(&service, "beta", b"b").await;

    let alpha = pull_once(&service, "alpha").await;
    assert_eq!(alpha.messages.len(), 1);
    assert_eq!(alpha.messages[0].payload, b"a");

    let beta_stats = stats(&service, "beta").await;
    assert_eq!(beta_stats.ready, 1);
    assert_eq!(beta_stats.in_flight, 0);
}

#[tokio::test]
async fn pull_moves_messages_to_in_flight_and_respects_batch_size() {
    let service = BrokerService::new();
    publish(&service, "jobs", b"one").await;
    publish(&service, "jobs", b"two").await;
    publish(&service, "jobs", b"three").await;

    let pulled = pull(&service, "jobs", 2, 1_000, 0).await;
    let payloads = pulled
        .messages
        .iter()
        .map(|message| message.payload.as_slice())
        .collect::<Vec<_>>();

    assert_eq!(payloads, vec![b"one".as_slice(), b"two".as_slice()]);

    let stats = stats(&service, "jobs").await;
    assert_eq!(stats.ready, 1);
    assert_eq!(stats.in_flight, 2);
}

#[tokio::test]
async fn zero_max_messages_defaults_to_one() {
    let service = BrokerService::new();
    publish(&service, "jobs", b"one").await;
    publish(&service, "jobs", b"two").await;

    let pulled = pull(&service, "jobs", 0, 1_000, 0).await;

    assert_eq!(pulled.messages.len(), 1);
    let stats = stats(&service, "jobs").await;
    assert_eq!(stats.ready, 1);
    assert_eq!(stats.in_flight, 1);
}

#[tokio::test]
async fn max_messages_is_capped() {
    let service = BrokerService::new();
    for i in 0..101 {
        publish(&service, "jobs", format!("message-{i}").as_bytes()).await;
    }

    let pulled = pull(&service, "jobs", 101, 1_000, 0).await;

    assert_eq!(pulled.messages.len(), MAX_BATCH_SIZE);
    let stats = stats(&service, "jobs").await;
    assert_eq!(stats.ready, 1);
    assert_eq!(stats.in_flight, MAX_BATCH_SIZE as u64);
}

#[tokio::test]
async fn empty_pull_returns_immediately_when_wait_timeout_is_zero() {
    let service = BrokerService::new();

    let started_at = tokio::time::Instant::now();
    let pulled = pull(&service, "jobs", 10, 50, 0).await;

    assert!(pulled.messages.is_empty());
    assert!(started_at.elapsed() < Duration::from_millis(25));
}

#[tokio::test]
async fn pull_waits_until_message_is_published() {
    let service = BrokerService::new();
    let consumer = service.clone();

    let pull_task = tokio::spawn(async move { pull(&consumer, "jobs", 10, 1_000, 500).await });
    tokio::time::sleep(Duration::from_millis(25)).await;
    publish(&service, "jobs", b"eventual").await;

    let pulled = pull_task.await.unwrap();
    assert_eq!(pulled.messages.len(), 1);
    assert_eq!(pulled.messages[0].payload, b"eventual");
}

#[tokio::test]
async fn pull_wait_timeout_expires_without_messages() {
    let service = BrokerService::new();

    let started_at = tokio::time::Instant::now();
    let pulled = pull(&service, "jobs", 10, 50, 30).await;

    assert!(pulled.messages.is_empty());
    assert!(started_at.elapsed() >= Duration::from_millis(25));
}

#[tokio::test]
async fn nack_requeues_message() {
    let service = BrokerService::new();
    let message_id = publish(&service, "jobs", b"retry").await;
    let first_pull = pull_once(&service, "jobs").await;

    let nack = nack(&service, first_pull.messages[0].delivery_id.clone(), true).await;
    assert!(nack.accepted);

    let second_pull = pull_once(&service, "jobs").await;
    assert_eq!(second_pull.messages.len(), 1);
    assert_eq!(second_pull.messages[0].message_id, message_id);
    assert_eq!(second_pull.messages[0].attempts, 2);
}

#[tokio::test]
async fn nack_without_requeue_drops_message() {
    let service = BrokerService::new();
    publish(&service, "jobs", b"drop").await;
    let pulled = pull_once(&service, "jobs").await;

    let nack = nack(&service, pulled.messages[0].delivery_id.clone(), false).await;
    assert!(nack.accepted);

    let stats = stats(&service, "jobs").await;
    assert_eq!(stats.ready, 0);
    assert_eq!(stats.in_flight, 0);
    assert!(pull_once(&service, "jobs").await.messages.is_empty());
}

#[tokio::test]
async fn ack_and_nack_unknown_delivery_id_return_false() {
    let service = BrokerService::new();

    assert!(!ack(&service, Uuid::new_v4().to_string()).await.acknowledged);
    assert!(
        !nack(&service, Uuid::new_v4().to_string(), true)
            .await
            .accepted
    );
}

#[tokio::test]
async fn ack_and_nack_reject_empty_delivery_id() {
    let service = BrokerService::new();

    let ack_error = service
        .ack(Request::new(AckRequest {
            delivery_id: " ".to_owned(),
        }))
        .await
        .unwrap_err();
    assert_eq!(ack_error.code(), Code::InvalidArgument);

    let nack_error = service
        .nack(Request::new(NackRequest {
            delivery_id: " ".to_owned(),
            requeue: true,
        }))
        .await
        .unwrap_err();
    assert_eq!(nack_error.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn expired_delivery_is_visible_again() {
    let service = BrokerService::new();
    publish(&service, "jobs", b"slow").await;
    let first_pull = pull_once(&service, "jobs").await;
    assert_eq!(first_pull.messages.len(), 1);

    tokio::time::sleep(Duration::from_millis(70)).await;

    let second_pull = pull_once(&service, "jobs").await;
    assert_eq!(second_pull.messages.len(), 1);
    assert_eq!(second_pull.messages[0].attempts, 2);
    assert_ne!(
        second_pull.messages[0].delivery_id,
        first_pull.messages[0].delivery_id
    );
}

#[tokio::test]
async fn queue_stats_requeues_expired_deliveries_before_counting() {
    let service = BrokerService::new();
    publish(&service, "jobs", b"slow").await;
    let pulled = pull(&service, "jobs", 1, 20, 0).await;
    assert_eq!(pulled.messages.len(), 1);

    tokio::time::sleep(Duration::from_millis(30)).await;

    let stats = stats(&service, "jobs").await;
    assert_eq!(stats.ready, 1);
    assert_eq!(stats.in_flight, 0);
}

#[tokio::test]
async fn all_queue_based_rpcs_reject_empty_queue() {
    let service = BrokerService::new();
    let publish_error = service
        .publish(Request::new(PublishRequest {
            queue: "   ".to_owned(),
            payload: Vec::new(),
        }))
        .await
        .unwrap_err();
    assert_eq!(publish_error.code(), Code::InvalidArgument);

    let pull_error = service
        .pull(Request::new(PullRequest {
            queue: "   ".to_owned(),
            max_messages: 1,
            visibility_timeout_ms: 1,
            wait_timeout_ms: 0,
        }))
        .await
        .unwrap_err();
    assert_eq!(pull_error.code(), Code::InvalidArgument);

    let stats_error = service
        .queue_stats(Request::new(QueueStatsRequest {
            queue: "   ".to_owned(),
        }))
        .await
        .unwrap_err();
    assert_eq!(stats_error.code(), Code::InvalidArgument);
}

#[test]
fn normalization_helpers_apply_defaults_and_limits() {
    assert_eq!(normalize_queue("  jobs  ".to_owned()).unwrap(), "jobs");
    assert_eq!(
        normalize_delivery_id("  delivery  ".to_owned()).unwrap(),
        "delivery"
    );
    assert_eq!(normalize_max_messages(0), DEFAULT_MAX_MESSAGES);
    assert_eq!(normalize_max_messages(1), 1);
    assert_eq!(
        normalize_max_messages((MAX_BATCH_SIZE + 1) as u32),
        MAX_BATCH_SIZE
    );
    assert_eq!(normalize_visibility_timeout(0), DEFAULT_VISIBILITY_TIMEOUT);
    assert_eq!(normalize_visibility_timeout(42), Duration::from_millis(42));
}
