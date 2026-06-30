//! # Message Deduplication Store
//!
//! This module implements message deduplication, which detects and discards
//! duplicate messages within a configurable time window.
//!
//! All trait methods are async so that implementations can perform
//! network I/O (Redis) without blocking the Tokio runtime.

use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;

use rifts_core::now_ms;

/// Trait for message deduplication stores.
///
/// All methods are async so that Redis-backed implementations can
/// use async Redis commands without `block_on`.
#[async_trait]
pub trait DedupeStore: Send + Sync {
    /// Check whether `key` under `topic` is fresh and, if so, record it.
    /// Returns `true` for fresh, `false` for duplicate. Atomic.
    async fn check_and_record(&self, topic: &str, key: &str, window: Duration) -> bool;

    /// Remove expired entries. Returns count removed.
    async fn sweep(&self) -> usize;
}

// ── Memory-backed ────────────────────────────────────────────

/// In-memory deduplication store backed by a concurrent [`DashMap`].
///
/// Each entry maps a `(topic, message_id)` tuple to an expiry timestamp
/// (milliseconds since epoch). Entries past their expiry are pruned by
/// [`sweep`](DedupeStore::sweep).
#[derive(Debug, Default)]
pub struct MemoryDedupeStore {
    inner: DashMap<(String, String), i64>,
}

impl MemoryDedupeStore {
    /// Create a new, empty [`MemoryDedupeStore`].
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl DedupeStore for MemoryDedupeStore {
    async fn check_and_record(&self, topic: &str, key: &str, window: Duration) -> bool {
        let now = now_ms();
        let expires = now + window.as_millis() as i64;
        let k = (topic.to_string(), key.to_string());
        let mut is_fresh = false;
        self.inner
            .entry(k)
            .and_modify(|v| {
                if *v <= now {
                    *v = expires;
                    is_fresh = true;
                }
            })
            .or_insert_with(|| {
                is_fresh = true;
                expires
            });
        is_fresh
    }

    async fn sweep(&self) -> usize {
        let now = now_ms();
        let expired: Vec<(String, String)> = self
            .inner
            .iter()
            .filter(|kv| *kv.value() <= now)
            .map(|kv| (kv.key().0.clone(), kv.key().1.clone()))
            .collect();
        let mut removed = 0;
        for k in expired {
            if self.inner.remove(&k).is_some() {
                removed += 1;
            }
        }
        removed
    }
}

// ── Sled-backed ──────────────────────────────────────────────

#[cfg(feature = "sled")]
mod sled_impl {
    use super::*;
    use crate::encode;
    use crate::engine::SledEngine;
    use crate::engine::StorageEngine;

    /// Sled-backed deduplication store.
    ///
    /// Uses atomic compare-and-swap on the underlying sled tree for correct
    /// distributed deduplication semantics.
    pub struct SledDedupeStore {
        engine: SledEngine,
    }

    impl SledDedupeStore {
        /// Create a new [`SledDedupeStore`] backed by the given sled engine.
        pub fn new(engine: SledEngine) -> Self {
            Self { engine }
        }
    }

    #[async_trait]
    impl DedupeStore for SledDedupeStore {
        async fn check_and_record(&self, topic: &str, key: &str, window: Duration) -> bool {
            let now = now_ms();
            let expires = now + window.as_millis() as i64;
            let k = encode::dedupe_key(topic, key);
            let new_bytes = expires.to_be_bytes();

            loop {
                let expected: Option<Vec<u8>> = match self.engine.get(&k) {
                    None => None,
                    Some(v) if v.len() >= 8 => {
                        let prev = i64::from_be_bytes(v[..8].try_into().unwrap_or([0; 8]));
                        if prev > now {
                            return false;
                        }
                        Some(v)
                    }
                    Some(_) => None,
                };
                match self.engine.cas(k.clone(), expected, new_bytes.to_vec()) {
                    Ok(Ok(())) => return true,
                    Ok(Err(_)) => continue,
                    Err(e) => {
                        tracing::error!(error = %e, "sled CAS failed in dedupe");
                        return false;
                    }
                }
            }
        }

        async fn sweep(&self) -> usize {
            let now = now_ms();
            let mut total = 0;
            let all = self.engine.scan_prefix(&[]);
            let mut topics: std::collections::HashSet<String> = std::collections::HashSet::new();
            for (k, _) in &all {
                if let Some(sep_pos) = k.iter().position(|&b| b == encode::SEP)
                    && let Ok(t) = std::str::from_utf8(&k[..sep_pos])
                {
                    topics.insert(t.to_string());
                }
            }
            for topic in topics {
                let prefix = encode::dedupe_prefix(&topic);
                let expired: Vec<Vec<u8>> = self
                    .engine
                    .scan_prefix(&prefix)
                    .into_iter()
                    .filter(|(_, v)| {
                        v.len() >= 8
                            && i64::from_be_bytes(v[..8].try_into().unwrap_or([0; 8])) <= now
                    })
                    .map(|(k, _)| k)
                    .collect();
                for k in &expired {
                    let _ = self.engine.delete(k);
                }
                total += expired.len();
            }
            total
        }
    }
}

#[cfg(feature = "sled")]
pub use sled_impl::SledDedupeStore;

// ── Tests ────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    async fn test_fresh_then_duplicate(store: &dyn DedupeStore) {
        let w = Duration::from_secs(60);
        assert!(store.check_and_record("t", "k", w).await);
        assert!(!store.check_and_record("t", "k", w).await);
    }

    async fn test_different_topics(store: &dyn DedupeStore) {
        let w = Duration::from_secs(60);
        assert!(store.check_and_record("t1", "k", w).await);
        assert!(store.check_and_record("t2", "k", w).await);
    }

    #[tokio::test]
    async fn memory_fresh_then_duplicate() {
        test_fresh_then_duplicate(&MemoryDedupeStore::new()).await;
    }

    #[tokio::test]
    async fn memory_different_topics() {
        test_different_topics(&MemoryDedupeStore::new()).await;
    }

    #[tokio::test]
    async fn memory_sweep_removes_expired() {
        let d = MemoryDedupeStore::new();
        d.check_and_record("t", "k", Duration::from_millis(0)).await;
        d.inner.insert(("t".into(), "k".into()), 0);
        let removed = d.sweep().await;
        assert_eq!(removed, 1);
    }

    #[tokio::test]
    async fn concurrent_dedup_returns_one_fresh() {
        use std::sync::Arc;
        let store = Arc::new(MemoryDedupeStore::new());
        let w = Duration::from_secs(60);
        let mut handles = Vec::new();
        for _ in 0..8 {
            let s = store.clone();
            handles.push(tokio::spawn(async move {
                s.check_and_record("t", "k", w).await
            }));
        }
        let mut fresh_count = 0;
        for h in handles {
            if h.await.unwrap() {
                fresh_count += 1;
            }
        }
        assert_eq!(fresh_count, 1, "exactly one thread should see fresh");
    }
}
