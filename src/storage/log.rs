//! # Topic Message Log Store
//!
//! This module provides the [`LogStore`] trait and its implementations for
//! persisting, querying, and expiring topic message logs. Each topic
//! maintains an ordered sequence of [`LogEntry`] values identified by a
//! monotonically increasing offset.
//!
//! ## Operations
//!
//! - **Append** -- write a new entry to a topic's log, immediately
//!   enforcing the configured [`RetentionPolicy`] to evict old entries.
//! - **Range query** -- retrieve entries whose offsets fall within an
//!   inclusive `[from, to]` range.
//! - **Latest** -- retrieve the most recently appended entry (highest
//!   offset), useful for catching up on missed messages.
//! - **Remove** -- drop all entries for a topic, typically when the topic
//!   itself is deleted.
//!
//! ## Implementations
//!
//! - [`MemoryLogStore`] -- an in-memory log backed by
//!   `DashMap<String, RwLock<Vec<LogEntry>>>`. Each topic gets its own
//!   `Vec` protected by a `RwLock`, allowing concurrent reads. Suitable
//!   for development and single-process deployments.
//! - [`SledLogStore`] -- a durable log backed by
//!   [`SledEngine`](crate::storage::SledEngine). Requires the `sled`
//!   Cargo feature. Log entries are serialized as JSON and keyed by
//!   [`encode::log_key`](crate::storage::encode::log_key). Suitable
//!   for production use where logs must survive broker restarts.
//!
//! ## Retention Policies
//!
//! The [`RetentionPolicy`](crate::topic::retention::RetentionPolicy) enum
//! controls how old entries are evicted after each append:
//!
//! | Policy | Behavior |
//! |--------|----------|
//! | `None` | Clear the entire log after every append. |
//! | `Count(n)` | Keep only the last `n` entries. |
//! | `Size(max)` | Evict oldest entries until total payload bytes <= `max`. |
//! | `Ttl(dur)` | Evict entries older than `dur`. |
//! | `Latest` | Keep only the most recently appended entry. |
//! | `Durable` | Never evict (log grows without bound). |

use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::RwLock;

use crate::now_ms;
use crate::topic::retention::RetentionPolicy;
use crate::topic::store::LogEntry;

/// Trait for a topic message log with append, range query, and
/// retention enforcement.
///
/// Implementations manage the lifecycle of log entries for a single
/// topic namespace. All methods take a `&str` topic name as the first
/// parameter; the store internally namespaces entries per topic.
pub trait LogStore: Send + Sync {
    /// Append a log entry to the topic's log and enforce the given
    /// retention policy.
    ///
    /// After inserting the entry, the implementation evicts older
    /// entries according to `retention`. The exact eviction strategy
    /// depends on the variant of [`RetentionPolicy`]:
    ///
    /// - `None` -- clears the entire log.
    /// - `Count(n)` -- drops oldest entries until at most `n` remain.
    /// - `Size(max_bytes)` -- drops oldest entries until total payload
    ///   size is at most `max_bytes`.
    /// - `Ttl(duration)` -- drops entries whose timestamp is older than
    ///   `duration` relative to the current time.
    /// - `Latest` -- removes all entries except the one just appended.
    /// - `Durable` -- no eviction (entries accumulate indefinitely).
    fn append(&self, topic: &str, entry: LogEntry, retention: RetentionPolicy);

    /// Retrieve all log entries whose offset is in the inclusive range
    /// `[from, to]`.
    ///
    /// Returns an empty `Vec` if the topic has no entries or if no
    /// entries fall within the requested range. The returned entries
    /// are ordered by offset in ascending order.
    fn range(&self, topic: &str, from: i64, to: i64) -> Vec<LogEntry>;

    /// Retrieve the most recently appended entry (highest offset) for
    /// the topic, if any.
    ///
    /// Returns `None` if the topic has no entries.
    fn latest(&self, topic: &str) -> Option<LogEntry>;

    /// Drop all log entries for the given topic.
    ///
    /// This is typically called when a topic is deleted. It is a no-op
    /// if the topic has no entries.
    fn remove(&self, topic: &str);
}

// ── Memory-backed ────────────────────────────────────────────

/// In-memory log store backed by a concurrent map of per-topic entry
/// vectors.
///
/// Internally uses `DashMap<String, Arc<RwLock<Vec<LogEntry>>>>` so that
/// each topic's log is independently lockable. Read operations acquire
/// shared `RwLock` access, while append operations acquire exclusive
/// access only for the single topic being written to.
///
/// # Thread Safety
///
/// Multiple threads may read from different topics concurrently without
/// contention. Writes to the same topic serialize via the `RwLock`.
#[derive(Debug, Default)]
pub struct MemoryLogStore {
    /// Map from topic name to a per-topic ordered log of entries.
    inner: DashMap<String, Arc<RwLock<Vec<LogEntry>>>>,
}

impl MemoryLogStore {
    /// Create a new, empty in-memory log store.
    ///
    /// Topics are created lazily on the first call to
    /// [`LogStore::append`] for that topic.
    pub fn new() -> Self {
        Self::default()
    }

    /// Retrieve the log vector for `topic`, creating it if it does not
    /// yet exist.
    ///
    /// This is an internal helper used by every `LogStore` method. It
    /// lazily inserts a new `Arc<RwLock<Vec<LogEntry>>>` into the
    /// `DashMap` on first access.
    fn get_or_create_log(&self, topic: &str) -> Arc<RwLock<Vec<LogEntry>>> {
        self.inner
            .entry(topic.to_string())
            .or_insert_with(|| Arc::new(RwLock::new(Vec::new())))
            .clone()
    }
}

impl LogStore for MemoryLogStore {
    fn append(&self, topic: &str, entry: LogEntry, retention: RetentionPolicy) {
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

    fn range(&self, topic: &str, from: i64, to: i64) -> Vec<LogEntry> {
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

    fn latest(&self, topic: &str) -> Option<LogEntry> {
        self.inner
            .get(topic)
            .and_then(|log| log.read().last().cloned())
    }

    fn remove(&self, topic: &str) {
        self.inner.remove(topic);
    }
}

// ── Sled-backed ──────────────────────────────────────────────

#[cfg(feature = "sled")]
mod sled_impl {
    //! Sled-backed log store implementation.
    //!
    //! This sub-module provides [`SledLogStore`], a durable log store
    //! that persists entries to disk via a
    //! [`SledEngine`](crate::storage::SledEngine). Each log entry is
    //! serialized as JSON and keyed using
    //! [`encode::log_key`](crate::storage::encode::log_key) so that
    //! lexicographic ordering matches numeric offset ordering.

    use super::*;
    use crate::storage::encode;
    use crate::storage::engine::SledEngine;
    use crate::storage::engine::StorageEngine;

    /// Sled-backed log store that persists topic message entries to disk.
    ///
    /// Each log entry is serialized as JSON via `serde_json` and stored
    /// under a key produced by [`encode::log_key`]. Because the keys are
    /// lexicographically ordered by offset, range scans and latest-entry
    /// lookups are efficient.
    ///
    /// # Retention
    ///
    /// The sled implementation enforces retention after every `append` by
    /// scanning the topic's prefix and deleting stale entries. This
    /// approach trades write amplification for simplicity; callers with
    /// very high write rates may prefer a background sweep strategy.
    pub struct SledLogStore {
        /// The underlying byte-oriented storage engine.
        engine: SledEngine,
    }

    impl SledLogStore {
        /// Create a new sled-backed log store from the given engine.
        ///
        /// The engine should be a dedicated tree for log entries, opened
        /// from the same `sled::Db` instance used by other stores.
        ///
        /// # Parameters
        ///
        /// - `engine` -- a [`SledEngine`] instance (typically a dedicated
        ///   tree for log entries).
        pub fn new(engine: SledEngine) -> Self {
            Self { engine }
        }
    }

    impl LogStore for SledLogStore {
        /// Append a log entry and enforce the given retention policy.
        ///
        /// The entry is serialized as JSON and written to the sled tree
        /// under the appropriate key. After writing, old entries are
        /// evicted according to `retention`:
        ///
        /// - `Count(n)` -- scan and delete excess entries.
        /// - `Ttl(duration)` -- scan and delete expired entries.
        /// - `Latest` -- delete all entries except the one just written.
        /// - `None` -- delete all entries for the topic.
        /// - `Size` and `Durable` -- no sled-side eviction (not
        ///   supported or deferred).
        fn append(&self, topic: &str, entry: LogEntry, retention: RetentionPolicy) {
            let key = encode::log_key(topic, entry.offset);
            // Propagate serialization failures as logs rather than
            // writing an empty Vec (which would corrupt the entry on
            // later read and be silently filtered out).
            let value = match serde_json::to_vec(&entry) {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!(error = %e, topic, offset = entry.offset,
                        "log entry serialization failed; skipping write");
                    return;
                }
            };
            self.engine.put(&key, &value);

            match retention {
                // Retention policies below run a full prefix scan on every
                // append to locate stale entries. This trades write
                // amplification for implementation simplicity; callers with
                // high-throughput topics should consider a background sweep
                // strategy or maintaining an in-memory entry count.
                RetentionPolicy::Count(n) => {
                    let all = self.engine.scan_prefix(&encode::log_prefix(topic));
                    if all.len() > n {
                        let drop = all.len() - n;
                        for (k, _) in &all[..drop] {
                            self.engine.delete(k);
                        }
                    }
                }
                RetentionPolicy::Ttl(ttl) => {
                    let now = now_ms();
                    let ttl_ms = ttl.as_millis() as i64;
                    let all = self.engine.scan_prefix(&encode::log_prefix(topic));
                    for (k, _) in all.iter().filter(|(_, v)| {
                        if let Ok(e) = serde_json::from_slice::<LogEntry>(v) {
                            // Use append time when present, falling back
                            // to the message timestamp for legacy entries
                            // that lack an `appended_at` field.
                            let ts = e.appended_at.unwrap_or(e.timestamp);
                            now - ts > ttl_ms
                        } else {
                            false
                        }
                    }) {
                        self.engine.delete(k);
                    }
                }
                RetentionPolicy::Latest => {
                    let all = self.engine.scan_prefix(&encode::log_prefix(topic));
                    for (k, _) in all.iter() {
                        if *k != key {
                            self.engine.delete(k);
                        }
                    }
                }
                RetentionPolicy::None => {
                    let all = self.engine.scan_prefix(&encode::log_prefix(topic));
                    for (k, _) in &all {
                        self.engine.delete(k);
                    }
                }
                // `Size` and `Durable` are not enforced on the sled
                // backend (deferred); see `LogStore` docstring.
                RetentionPolicy::Size(_) | RetentionPolicy::Durable => {}
            }
        }

        /// Retrieve entries in `[from, to]` (inclusive) by scanning the
        /// topic's prefix and filtering by offset.
        ///
        /// Note: this scans all entries for the topic and filters
        /// in-memory. For topics with very large logs, a more efficient
        /// approach using sled's `range` iterator with encoded start/end
        /// keys would be preferable.
        fn range(&self, topic: &str, from: i64, to: i64) -> Vec<LogEntry> {
            let prefix = encode::log_prefix(topic);
            self.engine
                .scan_prefix(&prefix)
                .into_iter()
                .filter_map(|(_, v)| serde_json::from_slice::<LogEntry>(&v).ok())
                .filter(|e| e.offset >= from && e.offset <= to)
                .collect()
        }

        /// Retrieve the most recently appended entry (highest offset).
        ///
        /// Scans all entries for the topic, sorts by offset, and returns
        /// the last one. Returns `None` if the topic has no entries.
        fn latest(&self, topic: &str) -> Option<LogEntry> {
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

        /// Drop all log entries for the given topic by scanning and
        /// deleting every entry under the topic's prefix.
        fn remove(&self, topic: &str) {
            let prefix = encode::log_prefix(topic);
            for (k, _) in self.engine.scan_prefix(&prefix) {
                self.engine.delete(&k);
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

    fn test_append_and_range(store: &dyn LogStore) {
        let rp = RetentionPolicy::Count(10);
        for i in 1..=5 {
            store.append("t", sample_entry(i), rp);
        }
        let got = store.range("t", 2, 4);
        assert_eq!(
            got.iter().map(|e| e.offset).collect::<Vec<_>>(),
            vec![2, 3, 4]
        );
    }

    fn test_latest(store: &dyn LogStore) {
        let rp = RetentionPolicy::Count(10);
        store.append("t", sample_entry(1), rp);
        store.append("t", sample_entry(2), rp);
        assert_eq!(store.latest("t").unwrap().offset, 2);
    }

    fn test_retention_count(store: &dyn LogStore) {
        let rp = RetentionPolicy::Count(2);
        for i in 1..=5 {
            store.append("t", sample_entry(i), rp);
        }
        let all = store.range("t", 1, 5);
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].offset, 4);
        assert_eq!(all[1].offset, 5);
    }

    #[test]
    fn memory_append_and_range() {
        test_append_and_range(&MemoryLogStore::new());
    }

    #[test]
    fn memory_latest() {
        test_latest(&MemoryLogStore::new());
    }

    #[test]
    fn memory_retention_count() {
        test_retention_count(&MemoryLogStore::new());
    }
}
