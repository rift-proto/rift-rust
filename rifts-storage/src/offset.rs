//! # Per-Topic Monotonic Offset Store
//!
//! This module provides the [`OffsetStore`] trait and its implementations for
//! allocating monotonically increasing offsets on a per-topic basis.
//!
//! All trait methods are async so that implementations can perform
//! network I/O (Redis) without blocking the Tokio runtime.

use std::sync::atomic::{AtomicI64, Ordering};

use async_trait::async_trait;
use dashmap::DashMap;

/// Trait for per-topic monotonic offset allocation.
///
/// All methods are `async` so that Redis-backed implementations can
/// use async Redis commands without `block_on`.
#[async_trait]
pub trait OffsetStore: Send + Sync {
    /// Allocate the next offset for `topic` and return it.
    async fn alloc(&self, topic: &str) -> i64;

    /// Return the highest allocated offset for `topic`, or `0`.
    async fn head(&self, topic: &str) -> i64;

    /// Remove all offset state for the given topic.
    async fn remove(&self, topic: &str);
}

// ── Memory-backed ────────────────────────────────────────────

/// In-memory offset store backed by a concurrent [`DashMap`].
///
/// Each topic maps to an [`AtomicI64`] tracking the current head offset.
#[derive(Debug, Default)]
pub struct MemoryOffsetStore {
    inner: DashMap<String, AtomicI64>,
}

impl MemoryOffsetStore {
    /// Create a new, empty [`MemoryOffsetStore`].
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl OffsetStore for MemoryOffsetStore {
    async fn alloc(&self, topic: &str) -> i64 {
        self.inner
            .entry(topic.to_string())
            .or_insert_with(|| AtomicI64::new(1))
            .fetch_add(1, Ordering::SeqCst)
    }

    async fn head(&self, topic: &str) -> i64 {
        self.inner
            .get(topic)
            .map(|c| c.load(Ordering::SeqCst) - 1)
            .unwrap_or(0)
    }

    async fn remove(&self, topic: &str) {
        self.inner.remove(topic);
    }
}

// ── Sled-backed ──────────────────────────────────────────────

#[cfg(feature = "sled")]
mod sled_impl {
    use super::*;
    use crate::encode;
    use crate::engine::SledEngine;
    use crate::engine::StorageEngine;

    /// Sled-backed offset store with an in-memory write-through cache.
    ///
    /// Hot offset values are cached in memory for fast reads, with writes
    /// going through to the underlying sled tree for durability.
    pub struct SledOffsetStore {
        engine: SledEngine,
        cache: parking_lot::Mutex<std::collections::HashMap<String, i64>>,
    }

    impl SledOffsetStore {
        /// Create a new [`SledOffsetStore`], pre-loading the cache from existing
        /// offset keys in the sled tree.
        pub fn new(engine: SledEngine) -> Self {
            let mut cache = std::collections::HashMap::new();
            for (key, value) in engine
                .scan_prefix(&[])
                .iter()
                .filter(|(k, _)| k.ends_with(b"head"))
            {
                let topic = String::from_utf8_lossy(&key[..key.len() - 5]).to_string();
                if value.len() >= 8 {
                    let head = i64::from_be_bytes(value[..8].try_into().unwrap_or([0; 8]));
                    cache.insert(topic, head);
                }
            }
            Self {
                engine,
                cache: parking_lot::Mutex::new(cache),
            }
        }
    }

    #[async_trait]
    impl OffsetStore for SledOffsetStore {
        async fn alloc(&self, topic: &str) -> i64 {
            let mut cache = self.cache.lock();
            let next = if let Some(&h) = cache.get(topic) {
                h + 1
            } else {
                let key = encode::offset_key(topic);
                let real_head = self
                    .engine
                    .get(&key)
                    .and_then(|v| {
                        if v.len() >= 8 {
                            Some(i64::from_be_bytes(v[..8].try_into().unwrap_or([0; 8])))
                        } else {
                            None
                        }
                    })
                    .unwrap_or(0);
                real_head + 1
            };
            cache.insert(topic.to_string(), next);
            let key = encode::offset_key(topic);
            let _ = self.engine.put(&key, &next.to_be_bytes());
            next
        }

        async fn head(&self, topic: &str) -> i64 {
            self.cache.lock().get(topic).copied().unwrap_or_else(|| {
                let key = encode::offset_key(topic);
                self.engine
                    .get(&key)
                    .and_then(|v| {
                        if v.len() >= 8 {
                            Some(i64::from_be_bytes(v[..8].try_into().unwrap_or([0; 8])))
                        } else {
                            None
                        }
                    })
                    .unwrap_or(0)
            })
        }

        async fn remove(&self, topic: &str) {
            self.cache.lock().remove(topic);
            let _ = self.engine.delete(&encode::offset_key(topic));
        }
    }
}

#[cfg(feature = "sled")]
pub use sled_impl::SledOffsetStore;

// ── Tests ────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    async fn test_alloc_and_head(store: &dyn OffsetStore) {
        assert_eq!(store.head("t").await, 0);
        assert_eq!(store.alloc("t").await, 1);
        assert_eq!(store.alloc("t").await, 2);
        assert_eq!(store.head("t").await, 2);
        assert_eq!(store.alloc("u").await, 1);
        assert_eq!(store.head("u").await, 1);
    }

    async fn test_remove(store: &dyn OffsetStore) {
        store.alloc("t").await;
        store.alloc("t").await;
        store.remove("t").await;
        assert_eq!(store.head("t").await, 0);
    }

    #[tokio::test]
    async fn memory_alloc_and_head() {
        test_alloc_and_head(&MemoryOffsetStore::new()).await;
    }

    #[tokio::test]
    async fn memory_remove() {
        test_remove(&MemoryOffsetStore::new()).await;
    }
}
