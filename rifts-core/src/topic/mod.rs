//! Topic layer (Rift spec section 9).
//!
//! This module defines the topic abstraction that sits between the broker
//! and the transport. A *topic* is a named, policy-governed channel that
//! producers publish to and consumers subscribe from.
//!
//! # Submodules
//!
//! - [`ordering`] — message ordering policies (none, per-connection, global, etc.).
//! - [`profile`] — a [`TopicProfile`] bundles all configurable policies for a topic.
//! - [`retention`] — how long messages are kept in the replay log.
//! - [`store`] — the in-memory [`TopicStore`] and per-topic [`TopicEntry`]
//!   state, including replay logs, snapshots, and subscriber/publisher counts.

pub mod ordering;
pub mod profile;
pub mod retention;
pub mod store;

pub use ordering::OrderingPolicy;
pub use profile::TopicProfile;
pub use retention::RetentionPolicy;
pub use store::{LogEntry, SubscriberId, TopicEntry, TopicStore, validate_name};
