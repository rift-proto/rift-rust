//! # Redis-Backed Multi-Instance Broker
//!
//! This crate provides the Redis integration layer for the `rifts`
//! ecosystem, enabling multiple server instances to share topic state
//! and route messages via Redis.
//!
//! ## Architecture
//!
//! ```text
//! ┌──────────────────────┐     ┌──────────────────────┐
//! │     Instance A       │     │     Instance B       │
//! │  RedisActorBroker    │     │  RedisActorBroker    │
//! │  ┌────────────────┐  │     │  ┌────────────────┐  │
//! │  │ RedisStorage   │  │     │  │ RedisStorage   │  │
//! │  │ Offset/Log/    │  │     │  │ Offset/Log/    │  │
//! │  │ Dedupe/Snapshot│  │     │  │ Dedupe/Snapshot│  │
//! │  └───────┬────────┘  │     │  └───────┬────────┘  │
//! │  ┌───────▼────────┐  │     │  ┌───────▼────────┐  │
//! │  │ Redis Pub/Sub  │  │     │  │ Redis Pub/Sub  │  │
//! │  │ Fanout Bridge  │──┼─────┼──│ Fanout Bridge  │  │
//! │  └────────────────┘  │     │  └────────────────┘  │
//! └──────────┬───────────┘     └──────────┬───────────┘
//!            └───────────┬───────────────┘
//!                        │
//!                  ┌─────▼─────┐
//!                  │   Redis   │
//!                  │  Pub/Sub  │
//!                  │  Hashes   │
//!                  │  Sets     │
//!                  └───────────┘
//! ```
//!
//! ## Submodules
//!
//! | Module | Purpose |
//! |--------|---------|
//! | [`connection`] | Redis connection pool and key helpers |
//! | [`storage`] | [`OffsetStore`], [`LogStore`], [`DedupeStore`], [`SnapshotStore`] Redis implementations |
//! | [`fanout`] | Redis Pub/Sub -> local fanout bridge |
//! | [`broker`] | [`RedisActorBroker`] implementing [`Broker`](rifts_broker::broker::Broker) |

#![forbid(unsafe_code)]
#![deny(unreachable_pub)]
#![deny(missing_docs)]
#![warn(rust_2018_idioms)]

#[cfg(feature = "redis")]
pub mod broker;
#[cfg(feature = "redis")]
pub mod connection;
#[cfg(feature = "redis")]
pub mod fanout;
#[cfg(feature = "redis")]
pub mod storage;

#[cfg(feature = "redis")]
pub use broker::RedisActorBroker;
#[cfg(feature = "redis")]
pub use connection::RedisPool;
#[cfg(feature = "redis")]
pub use fanout::FanoutBridge;
#[cfg(feature = "redis")]
pub use storage::dedupe::RedisDedupeStore;
#[cfg(feature = "redis")]
pub use storage::log::RedisLogStore;
#[cfg(feature = "redis")]
pub use storage::offset::RedisOffsetStore;
#[cfg(feature = "redis")]
pub use storage::snapshot::RedisSnapshotStore;
