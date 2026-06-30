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
//! - **[`router`]** — The [`TopicRouter`] trait and its [`LocalRouter`] implementation
//!   that resolves topic names to [`TopicEntry`] handles.
//! - **[`memory_broker`]** — The generic [`InMemoryBroker`] struct that wires
//!   all the above components together.
//!
//! [`TopicEntry`]: crate::topic::TopicEntry

#[allow(clippy::module_inception)]
pub mod broker;
pub mod fanout;
pub mod memory_broker;
pub mod router;

/// The core broker trait and supporting types.
pub use broker::{Broker, BrokerSubscription, PublishOutcome, serialize_frame_for_fanout};

/// Fanout engine, connection sinks, subscription management, and related types.
pub use fanout::{
    ConnectionSink, FanoutEngine, FanoutError, FanoutSink, SubscribeIntent, Subscription,
    SubscriptionId,
};

/// Single-process broker with pluggable storage backends.
pub use memory_broker::InMemoryBroker;

/// Topic routing layer.
pub use router::{LocalRouter, Route, TopicRouter};
