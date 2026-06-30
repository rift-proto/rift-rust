//! # Snapshot -- Topic State Snapshots (spec section 13.4)
//!
//! A snapshot is a complete state representation of a topic at a specific offset.
//!
//! ## Use Cases
//!
//! 1. **Reconnection recovery**: When a client's offset is too stale, it fetches a snapshot first,
//!    then consumes incremental messages.
//! 2. **New subscriber initialization**: In the `SnapshotThenLive` subscribe mode, the server
//!    sends a snapshot first.
//! 3. **Periodic compaction**: Periodically create snapshots and clean up historical messages
//!    before the snapshot's offset.
//!
//! Snapshots are set by the publisher and persistently stored by the server.
//! New subscribers can obtain them via
//! [`SubscribeIntent::SnapshotThenLive`](super::SubscribeIntent::SnapshotThenLive).

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::Result;

/// A state snapshot of a topic at a specific offset.
///
/// Each snapshot is bound to a topic and records the corresponding `base_offset`.
/// After obtaining a snapshot, a new subscriber starts consuming from `base_offset + 1`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// The topic name this snapshot belongs to.
    pub topic: String,

    /// Unique snapshot identifier (used for updating or deleting a specific snapshot).
    pub snapshot_id: String,

    /// The offset corresponding to this snapshot.
    ///
    /// Represents the aggregated state of all messages up to and including `base_offset`.
    /// Recovery begins consuming from `base_offset + 1`.
    pub base_offset: i64,

    /// Snapshot schema identifier, formatted as `{domain}.{name}@{major}.{minor}`.
    ///
    /// Defines the structure of `payload`, used for version compatibility checks.
    pub schema: String,

    /// Snapshot payload in JSON format.
    ///
    /// The structure is defined by `schema`, and is typically a complete representation
    /// of the topic's current state.
    pub payload: serde_json::Value,

    /// Creation timestamp (millisecond Unix timestamp).
    pub created_at: i64,

    /// Expiration timestamp (millisecond Unix timestamp, optional).
    ///
    /// After this time the snapshot is considered expired and may be cleaned up by the server.
    pub expires_at: Option<i64>,

    /// Optional checksum (e.g. a hexadecimal representation of SHA-256).
    ///
    /// Used to detect snapshot data corruption.
    pub checksum: Option<String>,
}

/// Serializes a Snapshot to JSON bytes.
pub fn encode_snapshot(s: &Snapshot) -> Result<Bytes> {
    Ok(Bytes::from(serde_json::to_vec(s)?))
}

/// Deserializes a Snapshot from JSON bytes.
pub fn decode_snapshot(bytes: &[u8]) -> Result<Snapshot> {
    Ok(serde_json::from_slice(bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let s = Snapshot {
            topic: "room/1".into(),
            snapshot_id: "snap-1".into(),
            base_offset: 42,
            schema: "room.snapshot@1.0".into(),
            payload: serde_json::json!({"messages": []}),
            created_at: 1000,
            expires_at: Some(2000),
            checksum: None,
        };
        let bytes = encode_snapshot(&s).unwrap();
        let back = decode_snapshot(&bytes).unwrap();
        assert_eq!(back.base_offset, 42);
        assert_eq!(back.snapshot_id, "snap-1");
    }
}
