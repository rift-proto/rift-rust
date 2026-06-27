//! Persistence abstraction for Rift/1 broker stores.
//!
//! Every broker component — offset allocation, log append, dedupe
//! check, snapshot capture — is backed by a [`StorageEngine`].
//! Two engines ship with the crate:
//!
//! - [`MemoryEngine`] — in-process, zero-config, no persistence.
//! - [`SledEngine`](crate::storage::SledEngine) — embedded on-disk B+tree (feature `sled`).

pub mod dedupe;
pub mod encode;
pub mod engine;
pub mod log;
pub mod offset;
pub mod snapshot;

pub use dedupe::{DedupeStore, MemoryDedupeStore};
pub use encode::{
    dedupe_key, dedupe_prefix, log_key, log_prefix, log_range_end, log_range_start, offset_key,
    offset_prefix, snapshot_key, snapshot_prefix,
};
pub use engine::{MemoryEngine, SharedEngine, StorageEngine};
pub use log::{LogStore, MemoryLogStore};
pub use offset::{MemoryOffsetStore, OffsetStore};
pub use snapshot::{MemorySnapshotStore, SnapshotStore, StoredSnapshot};

#[cfg(feature = "sled")]
pub use dedupe::SledDedupeStore;
#[cfg(feature = "sled")]
pub use engine::SledEngine;
#[cfg(feature = "sled")]
pub use log::SledLogStore;
#[cfg(feature = "sled")]
pub use offset::SledOffsetStore;
#[cfg(feature = "sled")]
pub use snapshot::SledSnapshotStore;
