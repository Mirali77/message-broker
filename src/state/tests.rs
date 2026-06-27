use std::time::Duration;

use super::{BrokerState, InFlightMessage};
use crate::message::StoredMessage;

#[test]
fn requeues_all_expired_messages() {
    let now = tokio::time::Instant::now();
    let mut state = BrokerState::default();
    state.in_flight.insert(
        "expired".to_owned(),
        InFlightMessage {
            message: StoredMessage {
                message_id: "message-1".to_owned(),
                queue: "jobs".to_owned(),
                payload: b"payload".to_vec(),
                attempts: 1,
                created_at_unix_ms: 1,
            },
            deadline: now - Duration::from_millis(1),
        },
    );
    state.in_flight.insert(
        "live".to_owned(),
        InFlightMessage {
            message: StoredMessage {
                message_id: "message-2".to_owned(),
                queue: "jobs".to_owned(),
                payload: b"payload".to_vec(),
                attempts: 1,
                created_at_unix_ms: 1,
            },
            deadline: now + Duration::from_secs(1),
        },
    );

    state.requeue_expired(now);

    assert_eq!(state.ready.get("jobs").unwrap().len(), 1);
    assert!(state.in_flight.contains_key("live"));
    assert!(!state.in_flight.contains_key("expired"));
}

#[test]
fn queue_stats_counts_ready_and_in_flight_by_queue() {
    let now = tokio::time::Instant::now();
    let mut state = BrokerState::default();
    state
        .ready
        .entry("jobs".to_owned())
        .or_default()
        .push_back(StoredMessage {
            message_id: "ready".to_owned(),
            queue: "jobs".to_owned(),
            payload: Vec::new(),
            attempts: 0,
            created_at_unix_ms: 1,
        });
    state.in_flight.insert(
        "delivery".to_owned(),
        InFlightMessage {
            message: StoredMessage {
                message_id: "flight".to_owned(),
                queue: "jobs".to_owned(),
                payload: Vec::new(),
                attempts: 1,
                created_at_unix_ms: 1,
            },
            deadline: now + Duration::from_secs(1),
        },
    );
    state.in_flight.insert(
        "other-delivery".to_owned(),
        InFlightMessage {
            message: StoredMessage {
                message_id: "other".to_owned(),
                queue: "other".to_owned(),
                payload: Vec::new(),
                attempts: 1,
                created_at_unix_ms: 1,
            },
            deadline: now + Duration::from_secs(1),
        },
    );

    assert_eq!(state.queue_stats("jobs"), (1, 1));
}
