//! # Topic State Snapshot Store
//!
//! This module provides the [`SnapshotStore`] trait and its implementations
//! for capturing and retrieving topic state snapshots. This corresponds to
//! **specification section 13.4**.
//!
//! ## What Is a Snapshot?
//!
//! A snapshot is a point-in-time capture of a topic's current state,
//! including a base offset (the offset at capture time) and a binary
//! payload containing the serialized topic state. Clients can use
//! snapshots to quickly synchronize with a topic without replaying the
//! entire message log.
//!
//! ## Implementations
//!
//! - [`MemorySnapshotStore`] -- an in-memory store backed by a
//!   `HashMap` protected by a `RwLock`. Each topic stores at most one
//!   snapshot; capturing a new snapshot replaces the previous one.
//!   Suitable for development and single-process deployments.
//! - [`SledSnapshotStore`] -- a durable store backed by
//!   [`SledEngine`](crate::storage::SledEngine). Requires the `sled`
//!   Cargo feature. Snapshots are serialized as JSON. Older snapshots
//!   for the same topic are pruned automatically when a new one is
//!   captured. Suitable for production use.
//!
//! ## Expiration
//!
//! Snapshots can optionally carry an `expires_at` timestamp. Both
//! implementations check expiration on retrieval and return `None` for
//! expired snapshots.

use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::RwLock;
use uuid::Uuid;

use crate::now_ms;
use crate::topic::TopicStore;

/// A stored snapshot of a topic's state at a point in time.
///
/// Snapshots are created by [`SnapshotStore::capture`] and retrieved by
/// [`SnapshotStore::get`]. They carry a unique identifier, the topic
/// name, the base offset at which the snapshot was taken, a binary
/// payload, and optional expiration metadata.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StoredSnapshot {
    /// Globally unique identifier for this snapshot (a UUID v4 string).
    pub snapshot_id: String,
    /// The topic whose state was captured.
    pub topic: String,
    /// The log offset at which the snapshot was taken. Clients can use
    /// this to determine which messages they need to replay after
    /// restoring the snapshot.
    pub base_offset: i64,
    /// The serialized topic state as raw bytes.
    pub payload: Bytes,
    /// Absolute timestamp (milliseconds since epoch) when the snapshot
    /// was created.
    pub created_at: i64,
    /// Optional absolute timestamp (milliseconds since epoch) after which
    /// the snapshot is considered expired and should not be returned by
    /// [`SnapshotStore::get`]. `None` means the snapshot never expires.
    pub expires_at: Option<i64>,
}

/// Trait for snapshot capture and retrieval.
///
/// Implementations manage the lifecycle of topic state snapshots, including
/// creation, retrieval (with expiration checking), deletion, and listing.
/// Each topic stores at most one active snapshot at a time.
pub trait SnapshotStore: Send + Sync {
    /// Capture a new snapshot of the topic's current state.
    ///
    /// Reads the latest state from `store` for the given topic, assigns
    /// a new unique snapshot ID, and records the snapshot. If the topic
    /// has no state or does not exist, returns `None`.
    ///
    /// # Parameters
    ///
    /// - `topic` -- the topic to snapshot.
    /// - `store` -- the [`TopicStore`] from which the current topic state
    ///   is read.
    /// - `ttl` -- optional time-to-live. If `Some(duration)`, the
    ///   snapshot's `expires_at` field is set to `now + duration`.
    ///
    /// # Returns
    ///
    /// The newly created [`StoredSnapshot`], or `None` if the topic has
    /// no capturable state.
    fn capture(
        &self,
        topic: &str,
        store: &TopicStore,
        ttl: Option<Duration>,
    ) -> Option<StoredSnapshot>;

    /// Retrieve the latest non-expired snapshot for a topic.
    ///
    /// Returns `None` if no snapshot exists for the topic or if the
    /// only available snapshot has expired.
    fn get(&self, topic: &str) -> Option<StoredSnapshot>;

    /// Delete the snapshot(s) for the given topic.
    ///
    /// This is a no-op if no snapshot exists for the topic.
    fn remove(&self, topic: &str);

    /// List all stored snapshots across all topics.
    ///
    /// Returns a `Vec` of all snapshots currently held in the store,
    /// including potentially expired ones. Callers that need only
    /// non-expired snapshots should check `expires_at` themselves.
    fn list(&self) -> Vec<StoredSnapshot>;
}

// ── Memory-backed ────────────────────────────────────────────

/// In-memory snapshot store backed by a `HashMap` protected by a
/// `RwLock`.
///
/// Each topic stores at most one snapshot. Capturing a new snapshot for a
/// topic replaces the previous one. Retrieval checks the `expires_at`
/// field and returns `None` for expired snapshots.
///
/// # Thread Safety
///
/// Read operations (`get`, `list`) acquire a shared read lock, while
/// write operations (`capture`, `remove`) acquire an exclusive write lock.
#[derive(Debug, Default)]
pub struct MemorySnapshotStore {
    /// Map from topic name to the most recently captured snapshot.
    inner: RwLock<HashMap<String, StoredSnapshot>>,
}

impl MemorySnapshotStore {
    /// Create a new, empty in-memory snapshot store.
    pub fn new() -> Self {
        Self::default()
    }
}

impl SnapshotStore for MemorySnapshotStore {
    fn capture(
        &self,
        topic: &str,
        store: &TopicStore,
        ttl: Option<Duration>,
    ) -> Option<StoredSnapshot> {
        let entry = store.get(topic)?;
        let snap = entry.snapshot()?;
        let now = now_ms();
        let stored = StoredSnapshot {
            // Deterministic id derived from the entry's offset.
            snapshot_id: format!("snap-{}", snap.offset),
            topic: topic.to_string(),
            base_offset: snap.offset,
            payload: snap.payload,
            created_at: now,
            expires_at: ttl.map(|t| now + t.as_millis() as i64),
        };
        let mut g = self.inner.write();
        g.insert(topic.to_string(), stored.clone());
        Some(stored)
    }

    fn get(&self, topic: &str) -> Option<StoredSnapshot> {
        let g = self.inner.read();
        let snap = g.get(topic)?;
        if let Some(expires) = snap.expires_at
            && now_ms() > expires
        {
            return None;
        }
        Some(snap.clone())
    }

    fn remove(&self, topic: &str) {
        self.inner.write().remove(topic);
    }

    fn list(&self) -> Vec<StoredSnapshot> {
        self.inner.read().values().cloned().collect()
    }
}

// ── Sled-backed ──────────────────────────────────────────────

#[cfg(feature = "sled")]
mod sled_impl {
    //! Sled-backed snapshot store implementation.
    //!
    //! This sub-module provides [`SledSnapshotStore`], a durable snapshot
    //! store that persists snapshots to disk via a
    //! [`SledEngine`](crate::storage::SledEngine). Snapshots are
    //! serialized as JSON and keyed using
    //! [`encode::snapshot_key`](crate::storage::encode::snapshot_key).
    //! When a new snapshot is captured for a topic, older snapshots for
    //! the same topic are automatically pruned.

    use super::*;
    use crate::storage::encode;
    use crate::storage::engine::SledEngine;
    use crate::storage::engine::StorageEngine;

    /// Sled-backed snapshot store that persists topic state snapshots to
    /// disk.
    ///
    /// Each snapshot is serialized as JSON and stored under a key produced
    /// by [`encode::snapshot_key`]. When a new snapshot is captured for a
    /// topic, all older snapshots for that topic are deleted, leaving only
    /// the most recent one.
    ///
    /// # Expiration
    ///
    /// Expired snapshots are filtered out at retrieval time by
    /// [`get`](SledSnapshotStore::get) but are not automatically deleted
    /// from the store. Call [`remove`](SledSnapshotStore::remove) or rely
    /// on the next `capture` call to clean them up.
    pub struct SledSnapshotStore {
        /// The underlying byte-oriented storage engine.
        engine: SledEngine,
    }

    impl SledSnapshotStore {
        /// Create a new sled-backed snapshot store from the given engine.
        ///
        /// The engine should be a dedicated tree for snapshot entries,
        /// opened from the same `sled::Db` instance used by other stores.
        ///
        /// # Parameters
        ///
        /// - `engine` -- a [`SledEngine`] instance (typically a dedicated
        ///   tree for snapshot entries).
        pub fn new(engine: SledEngine) -> Self {
            Self { engine }
        }
    }

    impl SnapshotStore for SledSnapshotStore {
        /// Capture a new snapshot and persist it to the sled tree.
        ///
        /// After writing the new snapshot, all older snapshots for the
        /// same topic are deleted from the engine, keeping only the
        /// latest one. The new snapshot is only deleted-along if its
        /// serialization actually succeeded; otherwise we keep the
        /// existing snapshot to avoid silent data loss.
        fn capture(
            &self,
            topic: &str,
            store: &TopicStore,
            ttl: Option<Duration>,
        ) -> Option<StoredSnapshot> {
            let entry = store.get(topic)?;
            let snap = entry.snapshot()?;
            let now = now_ms();
            let stored = StoredSnapshot {
                snapshot_id: format!("snap-{}", snap.offset),
                topic: topic.to_string(),
                base_offset: snap.offset,
                payload: snap.payload,
                created_at: now,
                expires_at: ttl.map(|t| now + t.as_millis() as i64),
            };
            let key = encode::snapshot_key(topic, &stored.snapshot_id);
            // Only evict older snapshots after we know the new one is
            // durably written; otherwise a serialization failure would
            // wipe the previous good snapshot.
            match serde_json::to_vec(&stored) {
                Ok(value) => {
                    self.engine.put(&key, &value);
                    let prefix = encode::snapshot_prefix(topic);
                    for (k, _) in self.engine.scan_prefix(&prefix) {
                        if k != key {
                            self.engine.delete(&k);
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "snapshot serialization failed; keeping previous snapshot");
                    return None;
                }
            }
            Some(stored)
        }

        /// Retrieve the latest non-expired snapshot for a topic.
        ///
        /// Scans all snapshot entries under the topic's prefix, filters
        /// out expired ones, sorts by `created_at` descending, and
        /// returns the most recent. Returns `None` if no valid snapshot
        /// exists.
        fn get(&self, topic: &str) -> Option<StoredSnapshot> {
            let prefix = encode::snapshot_prefix(topic);
            let now = now_ms();
            let mut latest: Option<StoredSnapshot> = None;
            for (_, v) in self.engine.scan_prefix(&prefix) {
                if let Ok(s) = serde_json::from_slice::<StoredSnapshot>(&v) {
                    if let Some(expires) = s.expires_at {
                        if now > expires {
                            continue;
                        }
                    }
                    match &latest {
                        None => latest = Some(s),
                        Some(prev) if s.created_at > prev.created_at => latest = Some(s),
                        _ => {}
                    }
                }
            }
            latest
        }

        /// Delete all snapshot entries for the given topic.
        fn remove(&self, topic: &str) {
            let prefix = encode::snapshot_prefix(topic);
            for (k, _) in self.engine.scan_prefix(&prefix) {
                self.engine.delete(&k);
            }
        }

        /// List all snapshots stored in the engine across all topics.
        ///
        /// This performs a full scan with an empty prefix, so it is
        /// intended for administrative or debugging use rather than
        /// hot-path operations.
        fn list(&self) -> Vec<StoredSnapshot> {
            self.engine
                .scan_prefix(&[])
                .into_iter()
                .filter_map(|(_, v)| serde_json::from_slice::<StoredSnapshot>(&v).ok())
                .collect()
        }
    }
}

#[cfg(feature = "sled")]
pub use sled_impl::SledSnapshotStore;

// ── Tests ────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic::profile::TopicProfile;
    use crate::topic::retention::RetentionPolicy;
    use crate::topic::store::LogEntry;

    #[test]
    fn memory_capture_and_get() {
        let store = TopicStore::new();
        let entry = store
            .get_or_create(
                "t",
                TopicProfile {
                    retention: RetentionPolicy::Count(10),
                    snapshot_enabled: true,
                    ..TopicProfile::default()
                },
            )
            .unwrap();
        entry.append(LogEntry {
            offset: 1,
            publisher_session: None,
            message_id: "m1".into(),
            class: "event".into(),
            event: Some("e".into()),
            payload: Bytes::from_static(b"hello"),
            timestamp: 0,
            appended_at: None,
        });
        let snaps = MemorySnapshotStore::new();
        let s = snaps
            .capture("t", &store, Some(Duration::from_secs(60)))
            .unwrap();
        assert_eq!(s.base_offset, 1);
        let got = snaps.get("t").unwrap();
        assert_eq!(got.snapshot_id, s.snapshot_id);
    }
}
