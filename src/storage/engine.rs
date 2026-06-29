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
//!
//! ## Shared Engine Pattern
//!
//! The [`SharedEngine`] type alias (`Arc<dyn StorageEngine>`) allows
//! multiple higher-level stores to share a single engine instance. In the
//! sled backend, each store gets its own `SledEngine` wrapping a distinct
//! `sled::Tree`, so no cross-contamination occurs between key namespaces.

use std::sync::Arc;

use dashmap::DashMap;

/// Low-level byte store abstraction.
///
/// All keys and values are opaque `Vec<u8>`.  Higher-level stores
/// (offset, log, dedupe, snapshot) build on top of this trait and
/// perform their own key encoding via the
/// [`encode`](crate::storage::encode) module.
///
/// Implementations must be both `Send` and `Sync` so they can be shared
/// safely across threads (typically via an `Arc`).
pub trait StorageEngine: Send + Sync + 'static {
    /// Retrieve the value associated with `key`, if it exists.
    ///
    /// Returns `None` when the key is not present in the store.
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;

    /// Insert or overwrite the value for `key`.
    ///
    /// If the key already exists, its value is replaced. If the key is
    /// new, a new entry is created.
    fn put(&self, key: &[u8], value: &[u8]);

    /// Remove the entry for `key` from the store.
    ///
    /// This is a no-op if the key does not exist.
    fn delete(&self, key: &[u8]);

    /// Scan all entries whose keys start with `prefix` and return them as
    /// `(key, value)` pairs.
    ///
    /// The order of the returned pairs is unspecified for unsorted backends
    /// (e.g. `MemoryEngine`) but is lexicographic by key for sorted
    /// backends (e.g. `SledEngine`).
    ///
    /// **Complexity**: O(n) in the total number of keys for `MemoryEngine`
    /// (full scan with a prefix filter). Callers with large key spaces
    /// should prefer `SledEngine` which supports efficient prefix scans
    /// via the underlying B+ tree.
    fn scan_prefix(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)>;
}

/// In-memory storage engine, backed by a concurrent `DashMap`.
///
/// Provides sub-microsecond latency and requires zero configuration.
/// Data is **not** persisted -- it is lost when the process exits.
/// Suitable for development, testing, and single-process deployments.
///
/// # Examples
///
/// ```ignore
/// use rifts::storage::{MemoryEngine, StorageEngine};
///
/// let engine = MemoryEngine::new();
/// engine.put(b"key", b"value");
/// assert_eq!(engine.get(b"key"), Some(b"value".to_vec()));
/// ```
#[derive(Debug, Default)]
pub struct MemoryEngine {
    /// The underlying concurrent hash map storing key-value byte pairs.
    inner: DashMap<Vec<u8>, Vec<u8>>,
}

impl MemoryEngine {
    /// Create a new, empty in-memory storage engine.
    ///
    /// The returned engine has no entries and will grow as data is
    /// written via [`StorageEngine::put`].
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

/// Type alias for a thread-safe, reference-counted storage engine.
///
/// This is the typical way to share an engine across multiple higher-level
/// stores. The `Arc` provides cheap cloning and the `dyn StorageEngine`
/// allows swapping implementations (e.g. from `MemoryEngine` to
/// `SledEngine`) without changing any downstream code.
pub type SharedEngine = Arc<dyn StorageEngine>;

#[cfg(feature = "sled")]
pub mod sled_engine {
    //! Sled-backed storage engine implementation.
    //!
    //! This sub-module provides [`SledEngine`], a durable key-value store
    //! backed by a single `sled::Tree`. Each higher-level store (offset,
    //! log, dedupe, snapshot) opens its own tree from the same `sled::Db`
    //! instance, giving each an isolated key space without cross-store
    //! prefix collisions.

    use super::StorageEngine;

    /// A storage engine backed by a single `sled::Tree`.
    ///
    /// Each higher-level store (offset, log, dedupe, snapshot) gets
    /// its own tree opened from the same `sled::Db` instance, giving
    /// independent key spaces without prefix encoding at the engine
    /// level.
    ///
    /// Because sled flushes data to disk, entries survive broker restarts.
    /// Call [`flush`](SledEngine::flush) to force a sync to the underlying
    /// filesystem.
    pub struct SledEngine {
        /// The sled B+ tree that holds all key-value entries.
        tree: sled::Tree,
    }

    impl SledEngine {
        /// Create a new sled-backed engine wrapping the given `sled::Tree`.
        ///
        /// Each higher-level store should open a separate tree from the
        /// same `sled::Db` to keep key spaces isolated.
        ///
        /// # Parameters
        ///
        /// - `tree` -- an opened `sled::Tree` instance.
        pub fn new(tree: sled::Tree) -> Self {
            Self { tree }
        }

        /// Flush all pending writes to the underlying filesystem.
        ///
        /// Returns the number of bytes flushed on success. This is
        /// useful for ensuring durability before acknowledging writes
        /// to clients.
        ///
        /// # Errors
        ///
        /// Returns a `sled::Error` if the flush fails.
        pub fn flush(&self) -> Result<usize, sled::Error> {
            self.tree.flush()
        }

        /// Atomically compare-and-swap: replace the current value at
        /// `key` with `new_value` only if the existing value equals
        /// `expected`.
        ///
        /// - `expected == None` means "key is absent" (insert path).
        /// - `expected == Some(_)` means "key is present with this value".
        ///
        /// On success returns `Ok(Ok(()))`. On a value mismatch
        /// returns `Ok(Err(sled::CompareAndSwapError))` and the caller
        /// should re-read and retry. On a sled I/O error returns
        /// `Err(sled::Error)`. This is the primitive used by
        /// [`SledDedupeStore`](crate::storage::SledDedupeStore) to
        /// eliminate the read-then-write race in deduplication.
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

        fn put(&self, key: &[u8], value: &[u8]) {
            if let Err(e) = self.tree.insert(key, value) {
                tracing::error!(error = %e, "sled put failed");
            }
        }

        fn delete(&self, key: &[u8]) {
            if let Err(e) = self.tree.remove(key) {
                tracing::error!(error = %e, "sled delete failed");
            }
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
