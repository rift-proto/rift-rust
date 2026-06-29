//! The **broker** subsystem (spec section 22).
//!
//! This module contains all the components responsible for accepting
//! published messages, routing them to the correct topic, deduplicating
//! repeated submissions, maintaining per-topic offset cursors, persisting
//! message logs and snapshots, and fanning out live deliveries to
//! connected subscribers.
//!
//! # Architecture
//!
//! The central abstraction is the [`Broker`] trait, which defines the
//! full set of topic-level operations (publish, subscribe, replay, etc.)
//! as async methods. The primary implementation is [`InMemoryBroker`],
//! a single-process broker with pluggable storage backends.
//!
//! # Key components
//!
//! - **[`broker`]** — The [`Broker`] trait itself, plus the [`PublishOutcome`] type
//!   and the `serialize_frame_for_fanout` helper.
//! - **[`fanout`]** — The [`FanoutEngine`] that delivers serialized frames to all
//!   active subscribers of a topic.
//! - **[`dedupe`]** — The [`DedupeStore`] that suppresses duplicate messages within
//!   a configurable time window.
//! - **[`router`]** — The [`TopicRouter`] trait and its [`LocalRouter`] implementation
//!   that resolves topic names to [`TopicEntry`] handles.
//! - **[`offset_store`]** — A per-topic monotonic offset allocator.
//! - **[`snapshot_store`]** — Captures and retrieves per-topic snapshots.
//! - **[`wire`]** — The framed TCP wire protocol used between gateway and
//!   broker nodes ([`GatewayMsg`], [`GatewayCodec`]).
//! - **[`memory_broker`]** — The generic [`InMemoryBroker`] struct that wires
//!   all the above components together.
//!
//! [`TopicEntry`]: crate::topic::TopicEntry

pub mod broker;
pub mod dedupe;
pub mod fanout;
pub mod memory_broker;
pub mod offset_store;
pub mod router;
pub mod snapshot_store;
pub mod wire;

/// The core broker trait and supporting types.
pub use broker::{Broker, BrokerSubscription, PublishOutcome, serialize_frame_for_fanout};

/// Time-window-based message deduplication store.
pub use dedupe::DedupeStore;

/// Fanout engine, connection sinks, subscription management, and
/// related types.
pub use fanout::{
    ConnectionSink, FanoutEngine, FanoutError, FanoutSink, SubscribeIntent, Subscription,
    SubscriptionId,
};

/// Single-process broker with pluggable storage backends.
pub use memory_broker::InMemoryBroker;

/// Per-topic monotonic offset allocator.
pub use offset_store::OffsetStore;

/// Topic routing layer (local and future distributed).
pub use router::{LocalRouter, Route, TopicRouter};

/// Snapshot persistence types.
pub use snapshot_store::{SharedSnapshotStore, SnapshotStore, StoredSnapshot};

/// Gateway-to-broker wire protocol types.
pub use wire::{GatewayCodec, GatewayMsg};
