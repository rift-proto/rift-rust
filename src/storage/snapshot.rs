//! Snapshot store — captures and retrieves topic state snapshots (spec §13.4).

use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::RwLock;
use uuid::Uuid;

use crate::now_ms;
use crate::topic::TopicStore;

/// A stored snapshot.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StoredSnapshot {
    pub snapshot_id: String,
    pub topic: String,
    pub base_offset: i64,
    pub payload: Bytes,
    pub created_at: i64,
    pub expires_at: Option<i64>,
}

/// Trait for snapshot capture and retrieval.
pub trait SnapshotStore: Send + Sync {
    /// Take a snapshot from the topic store's latest state.
    fn capture(
        &self,
        topic: &str,
        store: &TopicStore,
        ttl: Option<Duration>,
    ) -> Option<StoredSnapshot>;

    /// Get the latest snapshot for a topic, if any and not expired.
    fn get(&self, topic: &str) -> Option<StoredSnapshot>;

    /// Drop the snapshot for a topic.
    fn remove(&self, topic: &str);

    /// List all snapshots.
    fn list(&self) -> Vec<StoredSnapshot>;
}

// ── Memory-backed ────────────────────────────────────────────

/// In-memory snapshot store, backed by a `HashMap`.
#[derive(Debug, Default)]
pub struct MemorySnapshotStore {
    inner: RwLock<HashMap<String, StoredSnapshot>>,
}

impl MemorySnapshotStore {
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
            snapshot_id: Uuid::new_v4().to_string(),
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
    use super::*;
    use crate::storage::engine::SledEngine;

    /// Sled-backed snapshot store.
    pub struct SledSnapshotStore {
        engine: SledEngine,
    }

    impl SledSnapshotStore {
        pub fn new(engine: SledEngine) -> Self {
            Self { engine }
        }
    }

    impl SnapshotStore for SledSnapshotStore {
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
                snapshot_id: Uuid::new_v4().to_string(),
                topic: topic.to_string(),
                base_offset: snap.offset,
                payload: snap.payload,
                created_at: now,
                expires_at: ttl.map(|t| now + t.as_millis() as i64),
            };
            let key = encode::snapshot_key(topic, &stored.snapshot_id);
            if let Ok(value) = serde_json::to_vec(&stored) {
                self.engine.put(&key, &value);
            }
            // Remove old snapshots for this topic (keep only latest).
            let prefix = encode::snapshot_prefix(topic);
            for (k, _) in self.engine.scan_prefix(&prefix) {
                if k != key {
                    self.engine.delete(&k);
                }
            }
            Some(stored)
        }

        fn get(&self, topic: &str) -> Option<StoredSnapshot> {
            let prefix = encode::snapshot_prefix(topic);
            let snaps: Vec<StoredSnapshot> = self
                .engine
                .scan_prefix(&prefix)
                .into_iter()
                .filter_map(|(_, v)| serde_json::from_slice::<StoredSnapshot>(&v).ok())
                .collect();
            snaps.into_iter().find(|s| {
                if let Some(expires) = s.expires_at {
                    now_ms() <= expires
                } else {
                    true
                }
            })
        }

        fn remove(&self, topic: &str) {
            let prefix = encode::snapshot_prefix(topic);
            for (k, _) in self.engine.scan_prefix(&prefix) {
                self.engine.delete(&k);
            }
        }

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
