//! Low-level byte store abstraction.
//!
//! Two implementations:
//! - [`MemoryEngine`] — `DashMap`-backed, no persistence.
//! - `SledEngine` — `sled::Tree`-backed, durable (feature `sled`).

use std::sync::Arc;

use dashmap::DashMap;

/// Low-level byte store abstraction.
///
/// All keys and values are opaque `Vec<u8>`.  Higher-level stores
/// (offset, log, dedupe, snapshot) build on top of this trait and
/// perform their own key encoding.
pub trait StorageEngine: Send + Sync + 'static {
    /// Get a value by key.
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;

    /// Put a value by key.
    fn put(&self, key: &[u8], value: &[u8]);

    /// Delete a key.
    fn delete(&self, key: &[u8]);

    /// Scan all entries whose keys start with `prefix`.
    fn scan_prefix(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)>;
}

/// In-memory storage engine, backed by a `DashMap`.
///
/// Suitable for development and single-process deployments.
#[derive(Debug, Default)]
pub struct MemoryEngine {
    inner: DashMap<Vec<u8>, Vec<u8>>,
}

impl MemoryEngine {
    pub fn new() -> Self {
        Self::default()
    }
}

impl StorageEngine for MemoryEngine {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.inner.get(key).map(|v| v.clone())
    }

    fn put(&self, key: &[u8], value: &[u8]) {
        self.inner.insert(key.to_vec(), value.to_vec());
    }

    fn delete(&self, key: &[u8]) {
        self.inner.remove(key);
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.inner
            .iter()
            .filter(|kv| kv.key().starts_with(prefix))
            .map(|kv| (kv.key().clone(), kv.value().clone()))
            .collect()
    }
}

/// Wrapper for an `Arc<dyn StorageEngine>`.
pub type SharedEngine = Arc<dyn StorageEngine>;

#[cfg(feature = "sled")]
pub mod sled_engine {
    //! Sled-backed storage engine.

    use super::StorageEngine;

    /// A storage engine backed by a single `sled::Tree`.
    ///
    /// Each higher-level store (offset, log, dedupe, snapshot) gets
    /// its own tree opened from the same `sled::Db` instance, giving
    /// independent key spaces without prefix encoding at the engine
    /// level.
    pub struct SledEngine {
        tree: sled::Tree,
    }

    impl SledEngine {
        pub fn new(tree: sled::Tree) -> Self {
            Self { tree }
        }

        pub fn flush(&self) -> Result<usize, sled::Error> {
            self.tree.flush()
        }
    }

    impl StorageEngine for SledEngine {
        fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
            self.tree.get(key).ok().flatten().map(|v| v.to_vec())
        }

        fn put(&self, key: &[u8], value: &[u8]) {
            let _ = self.tree.insert(key, value);
        }

        fn delete(&self, key: &[u8]) {
            let _ = self.tree.remove(key);
        }

        fn scan_prefix(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
            self.tree
                .scan_prefix(prefix)
                .filter_map(|r| r.ok())
                .map(|(k, v)| (k.to_vec(), v.to_vec()))
                .collect()
        }
    }
}

#[cfg(feature = "sled")]
pub use sled_engine::SledEngine;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_engine_basic_ops() {
        let e = MemoryEngine::new();
        assert!(e.get(b"k").is_none());
        e.put(b"k", b"v");
        assert_eq!(e.get(b"k"), Some(b"v".to_vec()));
        e.delete(b"k");
        assert!(e.get(b"k").is_none());
    }

    #[test]
    fn memory_engine_scan_prefix() {
        let e = MemoryEngine::new();
        e.put(b"a:1", b"x");
        e.put(b"a:2", b"y");
        e.put(b"b:1", b"z");
        let results = e.scan_prefix(b"a:");
        assert_eq!(results.len(), 2);
    }
}
