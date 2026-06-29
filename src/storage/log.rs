//! # Topic Message Log Store
//!
//! This module provides the [`LogStore`] trait and its implementations for
//! persisting, querying, and expiring topic message logs.
//!
//! All trait methods are async so that implementations can perform
//! network I/O (Redis) without blocking the Tokio runtime.

use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use parking_lot::RwLock;

use crate::now_ms;
use crate::topic::retention::RetentionPolicy;
use crate::topic::store::LogEntry;

/// Trait for a topic message log with append, range query, and
/// retention enforcement. All methods are async.
#[async_trait]
pub trait LogStore: Send + Sync {
    /// Append a log entry and enforce the given retention policy.
    async fn append(&self, topic: &str, entry: LogEntry, retention: RetentionPolicy);

    /// Retrieve entries in `[from, to]` (inclusive), ordered by offset.
    async fn range(&self, topic: &str, from: i64, to: i64) -> Vec<LogEntry>;

    /// Retrieve the most recently appended entry, if any.
    async fn latest(&self, topic: &str) -> Option<LogEntry>;

    /// Drop all log entries for the given topic.
    async fn remove(&self, topic: &str);
}

// ── Memory-backed ────────────────────────────────────────────

/// In-memory append-log store backed by a concurrent [`DashMap`].
///
/// Each topic maps to a `Vec<LogEntry>` protected by an `Arc<RwLock<>>`,
/// allowing concurrent reads during replay while writes are serialized.
#[derive(Debug, Default)]
pub struct MemoryLogStore {
    inner: DashMap<String, Arc<RwLock<Vec<LogEntry>>>>,
}

impl MemoryLogStore {
    /// Create a new, empty [`MemoryLogStore`].
    pub fn new() -> Self {
        Self::default()
    }

    fn get_or_create_log(&self, topic: &str) -> Arc<RwLock<Vec<LogEntry>>> {
        self.inner
            .entry(topic.to_string())
            .or_insert_with(|| Arc::new(RwLock::new(Vec::new())))
            .clone()
    }
}

#[async_trait]
impl LogStore for MemoryLogStore {
    async fn append(&self, topic: &str, entry: LogEntry, retention: RetentionPolicy) {
        let log = self.get_or_create_log(topic);
        let mut g = log.write();
        g.push(entry.clone());
        match retention {
            RetentionPolicy::None => g.clear(),
            RetentionPolicy::Count(n) => {
                if g.len() > n {
                    let drop = g.len() - n;
                    g.drain(0..drop);
                }
            }
            RetentionPolicy::Size(max_bytes) => {
                let mut total: usize = g.iter().map(|e| e.payload.len()).sum();
                let mut idx = 0;
                while total > max_bytes && idx < g.len() {
                    total -= g[idx].payload.len();
                    idx += 1;
                }
                if idx > 0 {
                    g.drain(0..idx);
                }
            }
            RetentionPolicy::Ttl(ttl) => {
                let now = now_ms();
                g.retain(|e| now - e.timestamp <= ttl.as_millis() as i64);
            }
            RetentionPolicy::Latest => {
                g.retain(|e| e.offset == entry.offset);
            }
            RetentionPolicy::Durable => {}
        }
    }

    async fn range(&self, topic: &str, from: i64, to: i64) -> Vec<LogEntry> {
        self.inner
            .get(topic)
            .map(|log| {
                log.read()
                    .iter()
                    .filter(|e| e.offset >= from && e.offset <= to)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    async fn latest(&self, topic: &str) -> Option<LogEntry> {
        self.inner
            .get(topic)
            .and_then(|log| log.read().last().cloned())
    }

    async fn remove(&self, topic: &str) {
        self.inner.remove(topic);
    }
}

// ── Sled-backed ──────────────────────────────────────────────

#[cfg(feature = "sled")]
mod sled_impl {
    use super::*;
    use crate::storage::encode;
    use crate::storage::engine::SledEngine;
    use crate::storage::engine::StorageEngine;

    /// Sled-backed message log store.
    ///
    /// Stores log entries in a sled tree keyed by `(topic, offset)`, supporting
    /// prefix scans for range queries during replay.
    pub struct SledLogStore {
        engine: SledEngine,
    }

    impl SledLogStore {
        /// Create a new [`SledLogStore`] backed by the given sled engine.
        pub fn new(engine: SledEngine) -> Self {
            Self { engine }
        }
    }

    #[async_trait]
    impl LogStore for SledLogStore {
        async fn append(&self, topic: &str, entry: LogEntry, retention: RetentionPolicy) {
            let key = encode::log_key(topic, entry.offset);
            let value = match serde_json::to_vec(&entry) {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!(error = %e, topic, offset = entry.offset,
                        "log entry serialization failed; skipping write");
                    return;
                }
            };
            let _ = self.engine.put(&key, &value);

            match retention {
                RetentionPolicy::Count(n) => {
                    let all = self.engine.scan_prefix(&encode::log_prefix(topic));
                    if all.len() > n {
                        let drop = all.len() - n;
                        for (k, _) in &all[..drop] {
                            let _ = self.engine.delete(k);
                        }
                    }
                }
                RetentionPolicy::Ttl(ttl) => {
                    let now = now_ms();
                    let ttl_ms = ttl.as_millis() as i64;
                    let all = self.engine.scan_prefix(&encode::log_prefix(topic));
                    for (k, _) in all.iter().filter(|(_, v)| {
                        if let Ok(e) = serde_json::from_slice::<LogEntry>(v) {
                            let ts = e.appended_at.unwrap_or(e.timestamp);
                            now - ts > ttl_ms
                        } else {
                            false
                        }
                    }) {
                        let _ = self.engine.delete(k);
                    }
                }
                RetentionPolicy::Latest => {
                    let all = self.engine.scan_prefix(&encode::log_prefix(topic));
                    for (k, _) in all.iter() {
                        if *k != key {
                            let _ = self.engine.delete(k);
                        }
                    }
                }
                RetentionPolicy::None => {
                    let all = self.engine.scan_prefix(&encode::log_prefix(topic));
                    for (k, _) in &all {
                        let _ = self.engine.delete(k);
                    }
                }
                RetentionPolicy::Size(_) | RetentionPolicy::Durable => {}
            }
        }

        async fn range(&self, topic: &str, from: i64, to: i64) -> Vec<LogEntry> {
            let prefix = encode::log_prefix(topic);
            self.engine
                .scan_prefix(&prefix)
                .into_iter()
                .filter_map(|(_, v)| serde_json::from_slice::<LogEntry>(&v).ok())
                .filter(|e| e.offset >= from && e.offset <= to)
                .collect()
        }

        async fn latest(&self, topic: &str) -> Option<LogEntry> {
            let prefix = encode::log_prefix(topic);
            let mut entries: Vec<_> = self
                .engine
                .scan_prefix(&prefix)
                .into_iter()
                .filter_map(|(_, v)| serde_json::from_slice::<LogEntry>(&v).ok())
                .collect();
            entries.sort_by_key(|e| e.offset);
            entries.pop()
        }

        async fn remove(&self, topic: &str) {
            let prefix = encode::log_prefix(topic);
            for (k, _) in self.engine.scan_prefix(&prefix) {
                let _ = self.engine.delete(&k);
            }
        }
    }
}

#[cfg(feature = "sled")]
pub use sled_impl::SledLogStore;

// ── Tests ────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic::store::LogEntry;
    use bytes::Bytes;

    fn sample_entry(offset: i64) -> LogEntry {
        LogEntry {
            offset,
            publisher_session: None,
            message_id: format!("m-{offset}"),
            class: "event".into(),
            event: Some("e".into()),
            payload: Bytes::from("x"),
            timestamp: 0,
            appended_at: None,
        }
    }

    async fn test_append_and_range(store: &dyn LogStore) {
        let rp = RetentionPolicy::Count(10);
        for i in 1..=5 {
            store.append("t", sample_entry(i), rp).await;
        }
        let got = store.range("t", 2, 4).await;
        assert_eq!(
            got.iter().map(|e| e.offset).collect::<Vec<_>>(),
            vec![2, 3, 4]
        );
    }

    async fn test_latest(store: &dyn LogStore) {
        let rp = RetentionPolicy::Count(10);
        store.append("t", sample_entry(1), rp).await;
        store.append("t", sample_entry(2), rp).await;
        assert_eq!(store.latest("t").await.unwrap().offset, 2);
    }

    async fn test_retention_count(store: &dyn LogStore) {
        let rp = RetentionPolicy::Count(2);
        for i in 1..=5 {
            store.append("t", sample_entry(i), rp).await;
        }
        let all = store.range("t", 1, 5).await;
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].offset, 4);
        assert_eq!(all[1].offset, 5);
    }

    #[tokio::test]
    async fn memory_append_and_range() {
        test_append_and_range(&MemoryLogStore::new()).await;
    }

    #[tokio::test]
    async fn memory_latest() {
        test_latest(&MemoryLogStore::new()).await;
    }

    #[tokio::test]
    async fn memory_retention_count() {
        test_retention_count(&MemoryLogStore::new()).await;
    }
}
