use std::collections::{HashMap, VecDeque};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::message::StoredMessage;
use crate::state::{BrokerState, InFlightMessage};

const STATE_KEY: &[u8] = b"broker-state-v1";

#[derive(Clone, Debug)]
pub(crate) struct DurableStore {
    db: sled::Db,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct PersistentState {
    ready: HashMap<String, Vec<StoredMessage>>,
    in_flight: Vec<StoredMessage>,
}

impl DurableStore {
    pub(crate) fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Ok(Self {
            db: sled::open(path)?,
        })
    }

    pub(crate) fn load(&self) -> anyhow::Result<BrokerState> {
        let Some(bytes) = self.db.get(STATE_KEY)? else {
            return Ok(BrokerState::default());
        };

        let persistent = bincode::deserialize::<PersistentState>(&bytes)?;
        Ok(BrokerState::from_persistent(persistent))
    }

    pub(crate) fn save(&self, state: &BrokerState) -> anyhow::Result<()> {
        let bytes = bincode::serialize(&PersistentState::from_state(state))?;
        self.db.insert(STATE_KEY, bytes)?;
        self.db.flush()?;
        Ok(())
    }
}

impl PersistentState {
    fn from_state(state: &BrokerState) -> Self {
        Self {
            ready: state
                .ready
                .iter()
                .map(|(queue, messages)| (queue.clone(), messages.iter().cloned().collect()))
                .collect(),
            in_flight: state
                .in_flight
                .values()
                .map(|delivery| delivery.message.clone())
                .collect(),
        }
    }
}

impl BrokerState {
    fn from_persistent(persistent: PersistentState) -> Self {
        let mut ready = persistent
            .ready
            .into_iter()
            .map(|(queue, messages)| (queue, VecDeque::from(messages)))
            .collect::<HashMap<_, _>>();

        for message in persistent.in_flight {
            ready
                .entry(message.queue.clone())
                .or_default()
                .push_back(message);
        }

        Self {
            ready,
            in_flight: HashMap::<String, InFlightMessage>::new(),
        }
    }
}
