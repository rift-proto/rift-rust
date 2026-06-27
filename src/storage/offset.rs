//! Offset store — per-topic monotonic offset allocation (spec §13.1).

use std::sync::atomic::{AtomicI64, Ordering};

use dashmap::DashMap;

/// Trait for per-topic monotonic offset allocation.
pub trait OffsetStore: Send + Sync {
    /// Allocate the next offset for `topic` and return it.
    /// First call returns 1.
    fn alloc(&self, topic: &str) -> i64;

    /// Return the highest allocated offset for `topic` (0 if none).
    fn head(&self, topic: &str) -> i64;

    /// Drop a topic's offset.
    fn remove(&self, topic: &str);
}

// ── Memory-backed ────────────────────────────────────────────

/// In-memory offset store, backed by a `DashMap`.
#[derive(Debug, Default)]
pub struct MemoryOffsetStore {
    inner: DashMap<String, AtomicI64>,
}

impl MemoryOffsetStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl OffsetStore for MemoryOffsetStore {
    fn alloc(&self, topic: &str) -> i64 {
        self.inner
            .entry(topic.to_string())
            .or_insert_with(|| AtomicI64::new(1))
            .fetch_add(1, Ordering::SeqCst)
    }

    fn head(&self, topic: &str) -> i64 {
        self.inner
            .get(topic)
            .map(|c| c.load(Ordering::SeqCst) - 1)
            .unwrap_or(0)
    }

    fn remove(&self, topic: &str) {
        self.inner.remove(topic);
    }
}

// ── Sled-backed ──────────────────────────────────────────────

#[cfg(feature = "sled")]
mod sled_impl {
    use super::*;
    use crate::storage::engine::SledEngine;
    use std::sync::Mutex;

    /// Sled-backed offset store.  One key per topic: `<topic>\x00head`.
    pub struct SledOffsetStore {
        engine: SledEngine,
        /// In-memory cache to avoid sled reads on every `alloc`.
        cache: Mutex<std::collections::HashMap<String, i64>>,
    }

    impl SledOffsetStore {
        pub fn new(engine: SledEngine) -> Self {
            let mut cache = std::collections::HashMap::new();
            // Warm cache from existing data.
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
                cache: Mutex::new(cache),
            }
        }
    }

    impl OffsetStore for SledOffsetStore {
        fn alloc(&self, topic: &str) -> i64 {
            let mut cache = self.cache.lock().unwrap();
            let next = cache.get(topic).map(|h| h + 1).unwrap_or(1);
            cache.insert(topic.to_string(), next);
            let key = encode::offset_key(topic);
            self.engine.put(&key, &next.to_be_bytes());
            next
        }

        fn head(&self, topic: &str) -> i64 {
            self.cache
                .lock()
                .unwrap()
                .get(topic)
                .copied()
                .unwrap_or_else(|| {
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

        fn remove(&self, topic: &str) {
            self.cache.lock().unwrap().remove(topic);
            self.engine.delete(&encode::offset_key(topic));
        }
    }
}

#[cfg(feature = "sled")]
pub use sled_impl::SledOffsetStore;

// ── Tests ────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_alloc_and_head(store: &dyn OffsetStore) {
        assert_eq!(store.head("t"), 0);
        assert_eq!(store.alloc("t"), 1);
        assert_eq!(store.alloc("t"), 2);
        assert_eq!(store.head("t"), 2);
        assert_eq!(store.alloc("u"), 1);
        assert_eq!(store.head("u"), 1);
    }

    fn test_remove(store: &dyn OffsetStore) {
        store.alloc("t");
        store.alloc("t");
        store.remove("t");
        assert_eq!(store.head("t"), 0);
    }

    #[test]
    fn memory_alloc_and_head() {
        test_alloc_and_head(&MemoryOffsetStore::new());
    }

    #[test]
    fn memory_remove() {
        test_remove(&MemoryOffsetStore::new());
    }
}
