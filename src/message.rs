use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct StoredMessage {
    pub(crate) message_id: String,
    pub(crate) queue: String,
    pub(crate) payload: Vec<u8>,
    pub(crate) attempts: u32,
    pub(crate) created_at_unix_ms: i64,
}
