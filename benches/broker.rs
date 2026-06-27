use std::time::{Duration, Instant};

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use message_brocker::BrokerService;
use message_brocker::proto::broker_server::Broker;
use message_brocker::proto::{AckRequest, NackRequest, PublishRequest, PullRequest, PullResponse};
use tokio::runtime::Runtime;
use tonic::Request;

const QUEUE: &str = "bench";
const PAYLOAD_SIZE: usize = 256;
const BATCH_SIZE: u32 = 100;

fn bench_publish(c: &mut Criterion) {
    let runtime = Runtime::new().expect("create tokio runtime");
    let payload = vec![42; PAYLOAD_SIZE];

    let mut group = c.benchmark_group("broker");
    group.throughput(Throughput::Elements(1));
    group.bench_function("publish", |b| {
        b.iter_custom(|iters| {
            let service = BrokerService::new();
            let payload = payload.clone();

            let started_at = Instant::now();
            runtime.block_on(async {
                for _ in 0..iters {
                    let response = service
                        .publish(Request::new(PublishRequest {
                            queue: QUEUE.to_owned(),
                            payload: black_box(payload.clone()),
                        }))
                        .await
                        .expect("publish")
                        .into_inner();
                    black_box(response.message_id);
                }
            });
            started_at.elapsed()
        });
    });
    group.finish();
}

fn bench_publish_pull_ack(c: &mut Criterion) {
    let runtime = Runtime::new().expect("create tokio runtime");
    let payload = vec![7; PAYLOAD_SIZE];

    let mut group = c.benchmark_group("broker");
    group.throughput(Throughput::Elements(1));
    group.bench_function("publish_pull_ack", |b| {
        b.iter_custom(|iters| {
            let service = BrokerService::new();
            let payload = payload.clone();

            let started_at = Instant::now();
            runtime.block_on(async {
                for _ in 0..iters {
                    service
                        .publish(Request::new(PublishRequest {
                            queue: QUEUE.to_owned(),
                            payload: black_box(payload.clone()),
                        }))
                        .await
                        .expect("publish");

                    let pulled = pull(&service, 1).await;
                    let delivery_id = pulled.messages[0].delivery_id.clone();

                    let ack = service
                        .ack(Request::new(AckRequest { delivery_id }))
                        .await
                        .expect("ack")
                        .into_inner();
                    black_box(ack.acknowledged);
                }
            });
            started_at.elapsed()
        });
    });
    group.finish();
}

fn bench_batch_pull_ack(c: &mut Criterion) {
    let runtime = Runtime::new().expect("create tokio runtime");
    let payload = vec![13; PAYLOAD_SIZE];

    let mut group = c.benchmark_group("broker");
    group.throughput(Throughput::Elements(BATCH_SIZE as u64));
    group.bench_function("batch_publish_pull_ack_100", |b| {
        b.iter_custom(|iters| {
            let service = BrokerService::new();
            let payload = payload.clone();

            let started_at = Instant::now();
            runtime.block_on(async {
                for _ in 0..iters {
                    for _ in 0..BATCH_SIZE {
                        service
                            .publish(Request::new(PublishRequest {
                                queue: QUEUE.to_owned(),
                                payload: black_box(payload.clone()),
                            }))
                            .await
                            .expect("publish");
                    }

                    let pulled = pull(&service, BATCH_SIZE).await;
                    black_box(pulled.messages.len());

                    for message in pulled.messages {
                        service
                            .ack(Request::new(AckRequest {
                                delivery_id: message.delivery_id,
                            }))
                            .await
                            .expect("ack");
                    }
                }
            });
            started_at.elapsed()
        });
    });
    group.finish();
}

fn bench_nack_requeue(c: &mut Criterion) {
    let runtime = Runtime::new().expect("create tokio runtime");
    let payload = vec![99; PAYLOAD_SIZE];

    let mut group = c.benchmark_group("broker");
    group.throughput(Throughput::Elements(1));
    group.bench_function("pull_nack_requeue", |b| {
        b.iter_custom(|iters| {
            let service = BrokerService::new();
            let payload = payload.clone();

            runtime.block_on(async {
                service
                    .publish(Request::new(PublishRequest {
                        queue: QUEUE.to_owned(),
                        payload,
                    }))
                    .await
                    .expect("seed publish");
            });

            let started_at = Instant::now();
            runtime.block_on(async {
                for _ in 0..iters {
                    let pulled = pull(&service, 1).await;
                    let delivery_id = pulled.messages[0].delivery_id.clone();

                    let nack = service
                        .nack(Request::new(NackRequest {
                            delivery_id,
                            requeue: true,
                        }))
                        .await
                        .expect("nack")
                        .into_inner();
                    black_box(nack.accepted);
                }
            });
            started_at.elapsed()
        });
    });
    group.finish();
}

async fn pull(service: &BrokerService, max_messages: u32) -> PullResponse {
    service
        .pull(Request::new(PullRequest {
            queue: QUEUE.to_owned(),
            max_messages,
            visibility_timeout_ms: Duration::from_secs(30).as_millis() as u64,
            wait_timeout_ms: 0,
        }))
        .await
        .expect("pull")
        .into_inner()
}

criterion_group!(
    benches,
    bench_publish,
    bench_publish_pull_ack,
    bench_batch_pull_ack,
    bench_nack_requeue
);
criterion_main!(benches);
