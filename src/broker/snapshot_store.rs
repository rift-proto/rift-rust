//! Snapshot store — spec §13.4.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::now_ms;

use parking_lot::RwLock;
use uuid::Uuid;

use crate::topic::TopicStore;

#[derive(Debug, Clone)]
pub struct StoredSnapshot {
    pub snapshot_id: String,
    pub topic: String,
    pub base_offset: i64,
    pub payload: bytes::Bytes,
    pub created_at: i64,
    pub expires_at: Option<i64>,
}

/// In-process snapshot store. One snapshot per topic is retained
/// (the most recent); older ones are overwritten.
pub struct SnapshotStore {
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
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    /// Take a snapshot of the topic's current state. Pulls the latest
    /// log entry from the `TopicStore` and records it.
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
            snapshot_id: Uuid::new_v4().to_string(),
            topic: topic.to_string(),
            base_offset: snap.offset,
            payload: snap.payload,
            created_at: now,
            expires_at: ttl.map(|t| now + t.as_millis() as i64),
        };
        self.inner.write().insert(topic.to_string(), stored.clone());
        Some(stored)
    }

    /// Get a snapshot, returning `None` if it has expired.
    pub fn get(&self, topic: &str) -> Option<StoredSnapshot> {
        let snap = self.inner.read().get(topic).cloned()?;
        if let Some(expires) = snap.expires_at
            && now_ms() > expires
        {
            return None;
        }
        Some(snap)
    }

    pub fn remove(&self, topic: &str) -> Option<StoredSnapshot> {
        self.inner.write().remove(topic)
    }

    pub fn list(&self) -> Vec<StoredSnapshot> {
        self.inner.read().values().cloned().collect()
    }
}

/// Shared snapshot store.
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
        });
        let snaps = SnapshotStore::new();
        // Capture with 0-ms TTL → expires immediately.
        snaps.capture("t", &store, Some(Duration::from_millis(0)));
        // Manually set expires_at to 0.
        if let Some(mut snap) = snaps.inner.write().get_mut("t") {
            snap.expires_at = Some(0);
        }
        assert!(snaps.get("t").is_none());
    }
}
