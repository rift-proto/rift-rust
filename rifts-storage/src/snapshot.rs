//! # Topic State Snapshot Store
//!
//! All trait methods are async so that Redis-backed implementations
//! can use async Redis commands without `block_on`.

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::RwLock;

use rifts_core::now_ms;
use rifts_core::topic::TopicStore;

/// A stored snapshot of a topic's state at a point in time.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StoredSnapshot {
    /// Unique identifier for this snapshot.
    pub snapshot_id: String,
    /// The topic this snapshot belongs to.
    pub topic: String,
    /// The log offset at which this snapshot was captured.
    pub base_offset: i64,
    /// The snapshot payload (typically CBOR-encoded topic state).
    pub payload: Bytes,
    /// Timestamp (ms since epoch) when the snapshot was created.
    pub created_at: i64,
    /// Optional expiration timestamp; `None` means the snapshot lives indefinitely.
    pub expires_at: Option<i64>,
}

/// Trait for snapshot capture and retrieval. All methods are async.
#[async_trait]
pub trait SnapshotStore: Send + Sync {
    /// Capture a new snapshot of the topic's current state.
    async fn capture(
        &self,
        topic: &str,
        store: &TopicStore,
        ttl: Option<Duration>,
    ) -> Option<StoredSnapshot>;

    /// Retrieve the latest non-expired snapshot for a topic.
    async fn get(&self, topic: &str) -> Option<StoredSnapshot>;

    /// Delete the snapshot(s) for the given topic.
    async fn remove(&self, topic: &str);

    /// List all stored snapshots across all topics.
    async fn list(&self) -> Vec<StoredSnapshot>;
}

// ── Memory-backed ────────────────────────────────────────────

/// In-memory snapshot store backed by a [`RwLock`]-protected [`HashMap`].
///
/// Snapshots are stored keyed by topic name.
#[derive(Debug, Default)]
pub struct MemorySnapshotStore {
    inner: RwLock<HashMap<String, StoredSnapshot>>,
}

impl MemorySnapshotStore {
    /// Create a new, empty [`MemorySnapshotStore`].
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl SnapshotStore for MemorySnapshotStore {
    async fn capture(
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
        let mut g = self.inner.write();
        g.insert(topic.to_string(), stored.clone());
        Some(stored)
    }

    async fn get(&self, topic: &str) -> Option<StoredSnapshot> {
        let g = self.inner.read();
        let snap = g.get(topic)?;
        if let Some(expires) = snap.expires_at
            && now_ms() > expires
        {
            return None;
        }
        Some(snap.clone())
    }

    async fn remove(&self, topic: &str) {
        self.inner.write().remove(topic);
    }

    async fn list(&self) -> Vec<StoredSnapshot> {
        self.inner.read().values().cloned().collect()
    }
}

// ── Sled-backed ──────────────────────────────────────────────

#[cfg(feature = "sled")]
mod sled_impl {
    use super::*;
    use crate::encode;
    use crate::engine::SledEngine;
    use crate::engine::StorageEngine;

    /// Sled-backed snapshot store.
    ///
    /// Snapshots are stored in the sled tree as CBOR-encoded [`StoredSnapshot`]
    /// values keyed by topic name.
    pub struct SledSnapshotStore {
        engine: SledEngine,
    }

    impl SledSnapshotStore {
        /// Create a new [`SledSnapshotStore`] backed by the given sled engine.
        pub fn new(engine: SledEngine) -> Self {
            Self { engine }
        }
    }

    #[async_trait]
    impl SnapshotStore for SledSnapshotStore {
        async fn capture(
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
            match serde_json::to_vec(&stored) {
                Ok(value) => {
                    let _ = self.engine.put(&key, &value);
                    let prefix = encode::snapshot_prefix(topic);
                    for (k, _) in self.engine.scan_prefix(&prefix) {
                        if k != key {
                            let _ = self.engine.delete(&k);
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

        async fn get(&self, topic: &str) -> Option<StoredSnapshot> {
            let prefix = encode::snapshot_prefix(topic);
            let now = now_ms();
            let mut latest: Option<StoredSnapshot> = None;
            for (_, v) in self.engine.scan_prefix(&prefix) {
                if let Ok(s) = serde_json::from_slice::<StoredSnapshot>(&v) {
                    if let Some(expires) = s.expires_at
                        && now > expires
                    {
                        continue;
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

        async fn remove(&self, topic: &str) {
            let prefix = encode::snapshot_prefix(topic);
            for (k, _) in self.engine.scan_prefix(&prefix) {
                let _ = self.engine.delete(&k);
            }
        }

        async fn list(&self) -> Vec<StoredSnapshot> {
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
    use bytes::Bytes;
    use rifts_core::topic::store::LogEntry;
    use rifts_core::topic::{TopicProfile, TopicStore};

    #[tokio::test]
    async fn memory_capture_and_get() {
        let store = TopicStore::new();
        let entry = store
            .get_or_create(
                "t",
                TopicProfile {
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
            payload: Bytes::from_static(b"x"),
            timestamp: 0,
            appended_at: None,
        });
        let snaps = MemorySnapshotStore::new();
        let s = snaps
            .capture("t", &store, Some(Duration::from_secs(60)))
            .await
            .unwrap();
        assert_eq!(s.base_offset, 1);
        let got = snaps.get("t").await.unwrap();
        assert_eq!(got.snapshot_id, s.snapshot_id);
    }
}
