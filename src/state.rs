use std::collections::{HashMap, VecDeque};

use crate::message::StoredMessage;

#[derive(Clone, Debug)]
pub(crate) struct InFlightMessage {
    pub(crate) message: StoredMessage,
    pub(crate) deadline: tokio::time::Instant,
}

#[derive(Default, Debug)]
pub(crate) struct BrokerState {
    pub(crate) ready: HashMap<String, VecDeque<StoredMessage>>,
    pub(crate) in_flight: HashMap<String, InFlightMessage>,
}

impl BrokerState {
    pub(crate) fn requeue_expired(&mut self, now: tokio::time::Instant) {
        let expired_delivery_ids = self
            .in_flight
            .iter()
            .filter_map(|(delivery_id, delivery)| {
                (delivery.deadline <= now).then(|| delivery_id.clone())
            })
            .collect::<Vec<_>>();

        for delivery_id in expired_delivery_ids {
            if let Some(delivery) = self.in_flight.remove(&delivery_id) {
                self.ready
                    .entry(delivery.message.queue.clone())
                    .or_default()
                    .push_front(delivery.message);
            }
        }
    }

    pub(crate) fn queue_stats(&self, queue: &str) -> (u64, u64) {
        let ready = self.ready.get(queue).map_or(0, VecDeque::len) as u64;
        let in_flight = self
            .in_flight
            .values()
            .filter(|delivery| delivery.message.queue == queue)
            .count() as u64;

        (ready, in_flight)
    }
}

#[cfg(test)]
mod tests;
