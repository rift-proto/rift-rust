//! # Low-Level Byte Store Engine Abstraction
//!
//! This module defines the [`StorageEngine`] trait -- the fundamental
//! key-value interface that every higher-level store (offset, log, dedupe,
//! snapshot) builds upon. Keys and values are opaque `Vec<u8>` byte slices;
//! all semantic interpretation (topic names, offsets, timestamps) is left to
//! the higher-level stores.
//!
//! ## Implementations
//!
//! - [`MemoryEngine`] -- a `DashMap`-backed engine with no persistence.
//!   Requires zero configuration and is always available. Ideal for
//!   development, testing, and single-process deployments.
//! - [`SledEngine`] -- a `sled::Tree`-backed engine that persists data to
//!   disk. Requires the `sled` Cargo feature. Each higher-level store
//!   opens its own tree from a shared `sled::Db` instance, giving each
//!   store an independent key space without prefix encoding at the engine
//!   level.

use std::sync::Arc;

use dashmap::DashMap;

use rifts_core::error::StorageError;

/// Low-level byte store abstraction.
///
/// All keys and values are opaque `Vec<u8>`.  Higher-level stores
/// (offset, log, dedupe, snapshot) build on top of this trait and
/// perform their own key encoding via the
/// [`encode`](crate::encode) module.
///
/// Implementations must be both `Send` and `Sync` so they can be shared
/// safely across threads (typically via an `Arc`).
pub trait StorageEngine: Send + Sync + 'static {
    /// Retrieve the value associated with `key`, if it exists.
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;

    /// Insert or overwrite the value for `key`.
    ///
    /// Returns the number of bytes written on success.
    fn put(&self, key: &[u8], value: &[u8]) -> Result<usize, StorageError>;

    /// Remove the entry for `key` from the store.
    fn delete(&self, key: &[u8]) -> Result<(), StorageError>;

    /// Scan all entries whose keys start with `prefix` and return them as
    /// `(key, value)` pairs.
    fn scan_prefix(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)>;

    /// Flush all pending writes to durable storage.
    ///
    /// Returns the number of bytes flushed on success.
    fn flush(&self) -> Result<usize, StorageError>;
}

/// In-memory storage engine, backed by a concurrent `DashMap`.
#[derive(Debug, Default)]
pub struct MemoryEngine {
    inner: DashMap<Vec<u8>, Vec<u8>>,
}

impl MemoryEngine {
    /// Create a new, empty [`MemoryEngine`].
    pub fn new() -> Self {
        Self::default()
    }
}

impl StorageEngine for MemoryEngine {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.inner.get(key).map(|v| v.clone())
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<usize, StorageError> {
        self.inner.insert(key.to_vec(), value.to_vec());
        Ok(value.len())
    }

    fn delete(&self, key: &[u8]) -> Result<(), StorageError> {
        self.inner.remove(key);
        Ok(())
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.inner
            .iter()
            .filter(|kv| kv.key().starts_with(prefix))
            .map(|kv| (kv.key().clone(), kv.value().clone()))
            .collect()
    }

    fn flush(&self) -> Result<usize, StorageError> {
        Ok(0)
    }
}

/// Shared, reference-counted handle to a [`StorageEngine`] implementation.
///
/// Used by storage backends that need to share engine access across multiple
/// components (e.g. log store, offset store, dedupe store).
pub type SharedEngine = Arc<dyn StorageEngine>;

/// Sled-backed storage engine module.
///
/// Provides [`SledEngine`], a persistent, embedded storage engine backed by
/// the `sled` database. Enabled via the `sled` feature flag.
#[cfg(feature = "sled")]
pub mod sled_engine {
    use super::StorageEngine;
    use rifts_core::error::StorageError;

    /// Persistent storage engine backed by a `sled` [`Tree`](sled::Tree).
    ///
    /// Supports atomic compare-and-swap operations via [`cas`](SledEngine::cas),
    /// used by the deduplication store for distributed consistency.
    pub struct SledEngine {
        tree: sled::Tree,
    }

    impl SledEngine {
        /// Open or create a new [`SledEngine`] backed by the given sled tree.
        pub fn new(tree: sled::Tree) -> Self {
            Self { tree }
        }

        /// Flush all pending writes to disk, returning the number of bytes flushed.
        pub fn flush(&self) -> Result<usize, sled::Error> {
            self.tree.flush()
        }

        /// Atomic compare-and-swap.
        ///
        /// If the current value matches `expected`, writes `new_value`. Returns
        /// `Ok(Ok(()))` on success, `Ok(Err(CompareAndSwapError))` if the
        /// expected value didn't match, or `Err(sled::Error)` on I/O failure.
        pub fn cas(
            &self,
            key: Vec<u8>,
            expected: Option<Vec<u8>>,
            new_value: Vec<u8>,
        ) -> Result<Result<(), sled::CompareAndSwapError>, sled::Error> {
            self.tree.compare_and_swap(key, expected, Some(new_value))
        }
    }

    impl StorageEngine for SledEngine {
        fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
            self.tree.get(key).ok().flatten().map(|v| v.to_vec())
        }

        fn put(&self, key: &[u8], value: &[u8]) -> Result<usize, StorageError> {
            let len = value.len();
            self.tree
                .insert(key, value)
                .map(|_| len)
                .map_err(StorageError::engine)
        }

        fn delete(&self, key: &[u8]) -> Result<(), StorageError> {
            self.tree
                .remove(key)
                .map(|_| ())
                .map_err(StorageError::engine)
        }

        fn scan_prefix(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
            self.tree
                .scan_prefix(prefix)
                .filter_map(|r| match r {
                    Ok(kv) => Some((kv.0.to_vec(), kv.1.to_vec())),
                    Err(e) => {
                        tracing::warn!(error = %e, "sled scan_prefix entry error");
                        None
                    }
                })
                .collect()
        }

        fn flush(&self) -> Result<usize, StorageError> {
            self.tree.flush().map_err(StorageError::engine)
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
        e.put(b"k", b"v").unwrap();
        assert_eq!(e.get(b"k"), Some(b"v".to_vec()));
        e.delete(b"k").unwrap();
        assert!(e.get(b"k").is_none());
    }

    #[test]
    fn memory_engine_scan_prefix() {
        let e = MemoryEngine::new();
        e.put(b"a:1", b"x").unwrap();
        e.put(b"a:2", b"y").unwrap();
        e.put(b"b:1", b"z").unwrap();
        let results = e.scan_prefix(b"a:");
        assert_eq!(results.len(), 2);
    }
}
