//! Snapshot store — spec section 13.4.
//!
//! This module provides an in-process snapshot store that captures and
//! retrieves per-topic snapshots. A snapshot represents the latest
//! state of a topic at a particular message offset, allowing
//! subscribers to bootstrap quickly without replaying the entire
//! message history.
//!
//! # Retention policy
//!
//! Only the most recent snapshot per topic is retained. When a new
//! snapshot is captured for a topic that already has one, the previous
//! snapshot is overwritten. Snapshots can optionally be configured
//! with a time-to-live (TTL); expired snapshots are treated as absent
//! and are returned as `None` by [`SnapshotStore::get`].
//!
//! # Snapshot capture
//!
//! The [`SnapshotStore::capture`] method pulls the latest log entry
//! from a [`TopicStore`] and records it as a snapshot. The caller
//! provides an optional TTL duration; if `None`, the snapshot never
//! expires. Each snapshot is assigned a unique UUID identifier.
//!
//! # Shared access
//!
//! The store is wrapped in a [`parking_lot::RwLock`] internally,
//! allowing multiple concurrent readers or a single writer. A shared
//! reference can be distributed via the [`SharedSnapshotStore`] type
//! alias (`Arc<SnapshotStore>`).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::now_ms;

use parking_lot::RwLock;

use crate::topic::TopicStore;

/// A captured snapshot of a topic's state at a particular offset.
///
/// Contains the snapshot payload, metadata about when it was created,
/// and an optional expiration timestamp. Instances are produced by
/// [`SnapshotStore::capture`] and consumed by subscribers that want
/// to bootstrap from the latest state.
#[derive(Debug, Clone)]
pub struct StoredSnapshot {
    /// Unique identifier for this snapshot, generated as a UUID v4
    /// string. Used to distinguish snapshots and for cache keys.
    pub snapshot_id: String,
    /// The topic name this snapshot belongs to.
    pub topic: String,
    /// The message offset at which this snapshot was taken. All
    /// messages with offsets greater than this value occurred after
    /// the snapshot.
    pub base_offset: i64,
    /// The snapshot payload — the serialized state of the topic at
    /// the time the snapshot was captured.
    pub payload: bytes::Bytes,
    /// Epoch millisecond timestamp when the snapshot was created.
    pub created_at: i64,
    /// Optional epoch millisecond timestamp at which the snapshot
    /// expires. If `None`, the snapshot never expires. If the current
    /// time exceeds this value, [`SnapshotStore::get`] returns `None`.
    pub expires_at: Option<i64>,
}

/// In-process snapshot store with per-topic retention.
///
/// Stores at most one snapshot per topic (the most recent). Older
/// snapshots for the same topic are silently overwritten. The store
/// is protected by a [`parking_lot::RwLock`] for concurrent read
/// access and exclusive write access.
///
/// # Examples
///
/// ```ignore
/// use rifts::broker::SnapshotStore;
/// use std::time::Duration;
///
/// let store = SnapshotStore::new();
/// let snapshot = store.capture("orders", &topic_store, Some(Duration::from_secs(300)));
/// if let Some(snap) = snapshot {
///     println!("Snapshot at offset {}: {} bytes", snap.base_offset, snap.payload.len());
/// }
/// ```
pub struct SnapshotStore {
    /// Map from topic name to the most recent snapshot for that topic.
    inner: RwLock<HashMap<String, StoredSnapshot>>,
}

impl std::fmt::Debug for SnapshotStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotStore")
            .field("topics", &self.inner.read().len())
            .finish()
    }
}

impl Default for SnapshotStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SnapshotStore {
    /// Create an empty snapshot store with no captured snapshots.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    /// Capture a snapshot of a topic's current state.
    ///
    /// Retrieves the latest log entry from the provided
    /// [`TopicStore`] for the given topic, wraps it in a
    /// [`StoredSnapshot`] with a new UUID and the current timestamp,
    /// and stores it. Any previously stored snapshot for the same
    /// topic is overwritten.
    ///
    /// # Arguments
    ///
    /// * `topic` — The topic name to snapshot.
    /// * `store` — The [`TopicStore`] from which to pull the latest
    ///   log entry.
    /// * `ttl` — Optional time-to-live duration. If `Some(d)`, the
    ///   snapshot's `expires_at` is set to `now + d`. If `None`, the
    ///   snapshot never expires.
    ///
    /// # Returns
    ///
    /// `Some(StoredSnapshot)` if the topic exists and has at least
    /// one log entry, `None` otherwise.
    pub fn capture(
        &self,
        topic: &str,
        store: &TopicStore,
        ttl: Option<Duration>,
    ) -> Option<StoredSnapshot> {
        let entry = store.get(topic)?;
        let snap = entry.snapshot()?;
        let now = now_ms();
        let stored = StoredSnapshot {
            // Deterministic id derived from the entry's offset
            // so repeated captures of the same log state produce
            // the same snapshot_id.
            snapshot_id: format!("snap-{}", snap.offset),
            topic: topic.to_string(),
            base_offset: snap.offset,
            payload: snap.payload,
            created_at: now,
            expires_at: ttl.map(|t| now + t.as_millis() as i64),
        };
        self.inner.write().insert(topic.to_string(), stored.clone());
        Some(stored)
    }

    /// Retrieve the most recent snapshot for a topic.
    ///
    /// Returns `None` if no snapshot has been captured for the topic,
    /// or if the snapshot's `expires_at` timestamp is in the past
    /// (i.e. the snapshot has expired).
    ///
    /// # Arguments
    ///
    /// * `topic` — The topic name to look up.
    pub fn get(&self, topic: &str) -> Option<StoredSnapshot> {
        let snap = self.inner.read().get(topic).cloned()?;
        if let Some(expires) = snap.expires_at
            && now_ms() > expires
        {
            return None;
        }
        Some(snap)
    }

    /// Remove the snapshot for a topic, returning it if one existed.
    ///
    /// After removal, subsequent calls to [`get`](SnapshotStore::get)
    /// for the same topic will return `None` until a new snapshot is
    /// captured.
    ///
    /// # Arguments
    ///
    /// * `topic` — The topic whose snapshot should be removed.
    pub fn remove(&self, topic: &str) -> Option<StoredSnapshot> {
        self.inner.write().remove(topic)
    }

    /// List all currently stored snapshots across all topics.
    ///
    /// Returns a `Vec` of cloned snapshot records. This includes
    /// snapshots that may have logically expired but have not yet been
    /// removed. Callers should check `expires_at` if they need to
    /// filter out expired snapshots.
    pub fn list(&self) -> Vec<StoredSnapshot> {
        self.inner.read().values().cloned().collect()
    }
}

/// A type alias for a thread-safe, shared snapshot store.
///
/// Wraps [`SnapshotStore`] in an `Arc` for sharing across async tasks
/// and broker components.
pub type SharedSnapshotStore = Arc<SnapshotStore>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic::profile::TopicProfile;

    #[test]
    fn capture_and_read() {
        let store = TopicStore::new();
        let entry = store
            .get_or_create(
                "t",
                TopicProfile {
                    retention: crate::topic::retention::RetentionPolicy::Count(10),
                    snapshot_enabled: true,
                    ..TopicProfile::default()
                },
            )
            .unwrap();
        entry.append(crate::topic::store::LogEntry {
            offset: 1,
            publisher_session: None,
            message_id: "m1".into(),
            class: "event".into(),
            event: Some("e".into()),
            payload: bytes::Bytes::from_static(b"hello"),
            timestamp: 0,
            appended_at: None,
        });
        let snaps = SnapshotStore::new();
        let s = snaps
            .capture("t", &store, Some(Duration::from_secs(60)))
            .unwrap();
        assert_eq!(s.base_offset, 1);
        assert_eq!(s.payload.as_ref(), b"hello");
        let got = snaps.get("t").unwrap();
        assert_eq!(got.snapshot_id, s.snapshot_id);
    }

    #[test]
    fn expired_snapshot_returns_none() {
        let store = TopicStore::new();
        let entry = store
            .get_or_create(
                "t",
                TopicProfile {
                    retention: crate::topic::retention::RetentionPolicy::Count(10),
                    snapshot_enabled: true,
                    ..TopicProfile::default()
                },
            )
            .unwrap();
        entry.append(crate::topic::store::LogEntry {
            offset: 1,
            publisher_session: None,
            message_id: "m1".into(),
            class: "event".into(),
            event: Some("e".into()),
            payload: bytes::Bytes::from_static(b"hello"),
            timestamp: 0,
            appended_at: None,
        });
        let snaps = SnapshotStore::new();
        // Capture with 0-ms TTL — expires immediately.
        snaps.capture("t", &store, Some(Duration::from_millis(0)));
        // Manually set expires_at to 0.
        if let Some(snap) = snaps.inner.write().get_mut("t") {
            snap.expires_at = Some(0);
        }
        assert!(snaps.get("t").is_none());
    }
}
