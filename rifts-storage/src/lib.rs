#![allow(rustdoc::broken_intra_doc_links, rustdoc::private_intra_doc_links)]
//! # Rifts Storage — Persistent Storage Engine
//!
//! This crate provides a unified abstraction layer for all storage subsystems
//! used by the Rift broker. Every broker component -- offset allocation, log
//! append, deduplication, and snapshot capture -- is backed by a single
//! [`StorageEngine`] that operates on opaque byte keys and values.
//!
//! ## Architecture Overview
//!
//! ```text
//! StorageEngine          -- low-level byte-oriented key-value store (get/put/delete/scan_prefix)
//!   +-- OffsetStore      -- per-topic monotonic offset allocation
//!   +-- LogStore         -- topic message log append, range query, and retention
//!   +-- DedupeStore      -- message deduplication
//!   +-- SnapshotStore    -- topic state snapshot capture and retrieval
//! ```
//!
//! ## Storage Engines
//!
//! This crate ships two engine implementations:
//!
//! - [`MemoryEngine`] -- an in-memory engine backed by `DashMap`. Requires no
//!   configuration and provides no persistence; ideal for development, testing,
//!   and single-process deployments.
//! - [`SledEngine`] -- a durable engine backed by the embedded B+ tree
//!   database [sled](https://docs.rs/sled). Requires the `sled` Cargo feature
//!   and is suitable for production use.
//!
//! ## Key Encoding
//!
//! Higher-level stores delegate key construction to the [`encode`] module.
//! Every key follows a two-level namespace of the form
//! `<topic_name>\x00<sub_key>`, where `\x00` is the [`encode::SEP`]
//! separator. This guarantees that scanning entries for topic `"room/5"`
//! never accidentally matches entries belonging to `"room/50"`.
//!
//! ## Choosing an Engine
//!
//! | Concern | `MemoryEngine` | `SledEngine` |
//! |---------|---------------|--------------|
//! | Persistence | None (lost on restart) | Disk-backed |
//! | Latency | Sub-microsecond | Low millisecond |
//! | Setup | Zero configuration | Requires a `sled::Db` path |
//! | Feature gate | Always available | `sled` Cargo feature |

#![forbid(unsafe_code)]
#![deny(unreachable_pub)]
#![deny(missing_docs)]
#![warn(rust_2018_idioms)]

// ── Module declarations ──────────────────────────────────────────────────────

/// The deduplication sub-module, which detects and discards duplicate
/// messages within a configurable time window.
pub mod dedupe;
/// Key-encoding helpers that construct byte-level keys for every
/// higher-level store (offset, log, deduplication, snapshot).
pub mod encode;
/// The low-level byte-oriented key-value storage engine abstraction.
///
/// Provides [`StorageEngine`] and the two built-in backends
/// ([`MemoryEngine`] and `SledEngine`).
pub mod engine;
/// The topic message log store, supporting append, range queries, and
/// retention enforcement.
pub mod log;
/// The per-topic monotonic offset allocation store.
pub mod offset;
/// The topic state snapshot store, capturing and retrieving snapshots.
pub mod snapshot;

// ── Memory-backed re-exports (always available) ──────────────────────────────

pub use dedupe::{DedupeStore, MemoryDedupeStore};
pub use encode::{
    dedupe_key, dedupe_prefix, log_key, log_prefix, log_range_end, log_range_start, offset_key,
    offset_prefix, snapshot_key, snapshot_prefix,
};
pub use engine::{MemoryEngine, SharedEngine, StorageEngine};
pub use log::{LogStore, MemoryLogStore};
pub use offset::{MemoryOffsetStore, OffsetStore};
pub use snapshot::{MemorySnapshotStore, SnapshotStore, StoredSnapshot};

// ── Sled-backed re-exports (feature-gated) ───────────────────────────────────

/// Sled-backed deduplication store. Requires the `sled` Cargo feature.
#[cfg(feature = "sled")]
pub use dedupe::SledDedupeStore;
/// Sled-backed storage engine. Requires the `sled` Cargo feature.
#[cfg(feature = "sled")]
pub use engine::SledEngine;
/// Sled-backed log store. Requires the `sled` Cargo feature.
#[cfg(feature = "sled")]
pub use log::SledLogStore;
/// Sled-backed offset store. Requires the `sled` Cargo feature.
#[cfg(feature = "sled")]
pub use offset::SledOffsetStore;
/// Sled-backed snapshot store. Requires the `sled` Cargo feature.
#[cfg(feature = "sled")]
pub use snapshot::SledSnapshotStore;
