//! # Rifts — Rift Realtime Protocol / 1.0 Server Implementation
//!
//! This is the facade / umbrella crate that re-exports the public API surface
//! from the Rift ecosystem crates:
//!
//! - [`rifts_core`] — protocol fundamentals: frames, messages, codecs, error types, metrics, topics
//! - [`rifts_broker`] — message routing core: [`Broker`] trait, [`InMemoryBroker`], per-connection state
//! - [`rifts_session`] — session management: authentication, offset tracking, resume, session store
//! - [`rifts_storage`] — persistent storage engine: append log, offset index, dedupe, snapshot (optional, feature `sled`)
//! - [`rifts_transport`] — transport-layer abstraction: traits, WebSocket, and framework adapters (optional)
//! - [`rifts_redis`] — Redis-backed multi-instance broker and storage (optional, feature `redis`)
//! - [`rifts_client`] — async client SDK with auto-reconnect, heartbeat, and typed events (optional, feature `client`)
//!
//! ## Quick Start
//!
//! ```no_run
//! use rifts::RiftServer;
//! use std::sync::Arc;
//! use tokio::sync::Notify;
//!
//! # async fn run() -> rifts::Result<()> {
//! let shutdown = Arc::new(Notify::new());
//! let server = RiftServer::builder()
//!     .websocket_transport()
//!     .build()?;
//! server.run("127.0.0.1:9000".parse().unwrap(), shutdown).await?;
//! # Ok(()) }
//! ```

#![allow(rustdoc::broken_intra_doc_links, rustdoc::private_intra_doc_links)]
#![forbid(unsafe_code)]
#![deny(unreachable_pub)]
#![deny(missing_docs)]
#![warn(rust_2018_idioms)]

// ── Module declarations ──────────────────────────────────────────────────────

/// Server entry point — [`RiftServer`] and its [`RiftServerBuilder`].
pub mod server;

// ── Re-exports from rifts-core ───────────────────────────────────────────────

/// Message acknowledgement (ack / nack) semantics and tracking.
pub use rifts_core::ack;
/// Serialization codecs (JSON, CBOR).
pub use rifts_core::codec;
/// Server configuration structures and defaults.
pub use rifts_core::config;
/// Global error type hierarchy.
pub use rifts_core::error;
/// Flow control strategies — backpressure, rate limiting.
pub use rifts_core::flow;
/// Protocol frame (Frame) structure and codec helpers.
pub use rifts_core::frame;
/// Wire-format frame encoder and decoder shared by all transports.
pub use rifts_core::frame_codec;
/// Message semantic layer — Command, Event, Datagram, Stream, Snapshot, etc.
pub use rifts_core::message;
/// Process-local metric counters.
pub use rifts_core::metrics;
/// Protocol constants — handshake, heartbeat, error codes, versioning, close codes.
pub use rifts_core::protocol;
/// Topic profile — retention policy, ordering policy, storage.
pub use rifts_core::topic;

// Convenience re-exports from rifts-core
pub use rifts_core::ack::{AckManager, SharedAckManager};
pub use rifts_core::config::ServerConfig;
pub use rifts_core::error::{BoxedStdError, ConfigError, Result, RiftError, StorageError};
pub use rifts_core::frame::{EncodingFormat, Frame, FrameFlags, FrameType, Priority};
pub use rifts_core::frame_codec::DEFAULT_MAX_BINARY_PAYLOAD;
pub use rifts_core::frame_codec::{decode_binary_frame, decode_text_frame, encode_frame};
pub use rifts_core::message::{DeliveryMode, Message, MessageClass, SubscribeResult};
pub use rifts_core::metrics::Metrics;
pub use rifts_core::now_ms;
pub use rifts_core::protocol::close::CloseCode;
pub use rifts_core::protocol::error_code::ErrorCode;
pub use rifts_core::protocol::hello::{AuthMode, Hello, Ready, ResumeResult, SdkInfo, Welcome};
pub use rifts_core::topic::{OrderingPolicy, RetentionPolicy, TopicProfile, TopicStore};

// ── Re-exports from rifts-broker ─────────────────────────────────────────────

/// Message routing core — Broker trait and implementations.
pub use rifts_broker::broker;
/// Per-connection state machine.
pub use rifts_broker::connection;

pub use rifts_broker::broker::{Broker, InMemoryBroker, PublishOutcome};
pub use rifts_broker::broker::{FanoutEngine, LocalRouter, Route, SubscribeIntent, TopicRouter};

// ── Re-exports from rifts-session ────────────────────────────────────────────

/// Session management.
pub use rifts_session::session;

pub use rifts_session::session::{
    AllowAllAuth, AuthContext, AuthHints, AuthProvider, ClientId, OffsetTracker, ResumeDecision,
    ResumeManager, Session, SessionId, SessionState, SessionStore, TokenAuth,
};

// ── Re-exports from rifts-storage (optional) ─────────────────────────────────

/// Persistent storage engine (re-exported from rifts-storage crate).
#[cfg(feature = "sled")]
pub use rifts_storage as storage;

#[cfg(feature = "sled")]
pub use rifts_storage::{
    DedupeStore, LogStore, MemoryDedupeStore, MemoryEngine, MemoryLogStore, MemoryOffsetStore,
    MemorySnapshotStore, OffsetStore, SharedEngine, SledDedupeStore, SledEngine, SledLogStore,
    SledOffsetStore, SledSnapshotStore, SnapshotStore, StorageEngine, StoredSnapshot, dedupe_key,
    dedupe_prefix, encode, log_key, log_prefix, log_range_end, log_range_start, offset_key,
    offset_prefix, snapshot_key, snapshot_prefix,
};

// ── Re-exports from rifts-transport (optional) ───────────────────────────────

/// Transport-layer abstraction and framework adapters.
#[cfg(feature = "_transport")]
pub use rifts_transport::transport;

#[cfg(feature = "_transport")]
pub use rifts_transport::transport::{Transport, TransportConnection, TransportListener};

/// Standalone WebSocket transport.
#[cfg(feature = "websocket")]
pub use rifts_transport::transport::websocket::WebSocketTransport;

// ── Re-exports from rifts-redis (optional) ───────────────────────────────────

/// Redis-backed multi-instance broker and storage.
#[cfg(feature = "redis")]
pub use rifts_redis as redis;

#[cfg(feature = "redis")]
pub use rifts_redis::{
    FanoutBridge, RedisActorBroker, RedisDedupeStore, RedisLogStore, RedisOffsetStore, RedisPool,
    RedisSnapshotStore,
};

// ── Re-exports from rifts-client (optional) ──────────────────────────────────

/// Async client SDK.
#[cfg(feature = "client")]
pub use rifts_client as client;

#[cfg(feature = "client")]
pub use rifts_client::{
    ClientError, ClientEvent, CommandOpts, PublishOpts, RiftClient, RiftClientConfig, StateOpts,
};

// ── Server re-exports ────────────────────────────────────────────────────────

pub use server::{RiftServer, RiftServerBuilder};
