#![allow(rustdoc::broken_intra_doc_links, rustdoc::private_intra_doc_links)]
//! # Rifts — Rift Realtime Protocol / 1.0 Server Implementation
//!
//! This crate implements the server-side of the [Rift Realtime Protocol v1.0][spec],
//! providing an embeddable, high-performance real-time pub/sub engine.
//!
//! ## Core Concepts
//!
//! - **Broker** — the message routing core responsible for publishing,
//!   subscribing, fan-out delivery, and replay. Ships with
//!   [`InMemoryBroker`] (in-process, all storage in memory).
//! - **Frame / Message** — a [`Frame`] is the wire-level transport unit
//!   (JSON or CBOR encoded); a [`Message`] is the business-semantic layer
//!   carried inside a frame (commands, events, datagrams, snapshots, etc.).
//! - **Session** — each WebSocket connection maps to a [`Session`] that
//!   manages authentication state, offset tracking, and heartbeat.
//! - **Transport** — transport-layer abstraction with built-in adapters
//!   for `axum`, `actix-web`, `warp`, `ntex`, plus a standalone
//!   WebSocket listener.
//! - **Topic Profile** — each topic can carry its own retention policy,
//!   ordering policy, subscriber/publisher limits, snapshot toggle, etc.
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
//!
//! ## Module Overview (spec §30)
//!
//! | Module | Responsibility |
//! |--------|----------------|
//! | [`ack`] | Message acknowledgement (ack / nack) semantics and tracking |
//! | [`broker`] | Message routing core — publish, subscribe, fan-out, dedupe |
//! | [`codec`] | Serialization codecs (JSON / CBOR) |
//! | [`config`] | Server configuration (payload limits, heartbeat policy, etc.) |
//! | [`connection`] | Connection-level processing — frame parsing, dispatch, backpressure |
//! | [`error`] | Global error type hierarchy |
//! | [`flow`] | Flow control — backpressure, rate limiting |
//! | [`frame`] | Protocol frame structure and codec helpers |
//! | [`message`] | Message semantic layer (Command / Event / Datagram / Stream / Snapshot) |
//! | [`metrics`] | Process-local metric counters (exportable to Prometheus) |
//! | [`protocol`] | Protocol constants — handshake, heartbeat, error codes, versioning, close codes |
//! | [`session`] | Session management — authentication, offset tracking, resume |
//! | [`storage`] | Persistent storage engine — append log, offset index, dedupe, snapshot |
//! | [`topic`] | Topic profile — retention policy, ordering policy, storage binding |
//! | [`transport`] | Transport-layer abstraction and framework adapters |
//!
//! [spec]: https://github.com/rift-proto/rifts

#![forbid(unsafe_code)]
#![deny(unreachable_pub)]
#![deny(missing_docs)]
#![warn(rust_2018_idioms)]

// ── Module declarations ──────────────────────────────────────────────────────

/// Message acknowledgement (ack / nack) semantics and tracking.
pub mod ack;

/// Async client SDK with auto-reconnect, heartbeat, and typed events.
#[cfg(feature = "client")]
pub mod client;

/// Message routing core — Broker trait and implementations.
pub mod broker;

/// Serialization codecs (JSON, CBOR).
pub mod codec;

/// Server configuration structures and defaults.
pub mod config;

/// Connection-level processing — frame parsing, command dispatch, backpressure.
pub mod connection;

/// Global error type hierarchy.
pub mod error;

/// Flow control strategies — backpressure, rate limiting.
pub mod flow;

/// Protocol frame (Frame) structure and codec helpers.
pub mod frame;

/// Message semantic layer — Command, Event, Datagram, Stream, Snapshot, etc.
pub mod message;

/// Process-local metric counters.
pub mod metrics;

/// Protocol constants — handshake, heartbeat, error codes, versioning, close codes.
pub mod protocol;

/// Redis-backed multi-instance broker and storage (feature `redis`).
#[cfg(feature = "redis")]
pub mod redis;

/// Server entry point — `RiftServer` and its Builder.
pub mod server;

/// Session management — authentication, offset tracking, resume.
pub mod session;

/// Persistent storage engine.
pub mod storage;

/// Topic profile — retention policy, ordering policy, storage.
pub mod topic;

/// Transport-layer abstraction and framework adapters.
pub mod transport;

// ── Public API re-exports ────────────────────────────────────────────────────

pub use broker::{Broker, InMemoryBroker, PublishOutcome, SubscribeIntent};
pub use config::ServerConfig;
pub use error::{BoxedStdError, ConfigError, Result, RiftError};
pub use frame::{Codec, Frame, FrameFlags, FrameType, Priority};
pub use message::{DeliveryMode, Message, MessageClass, SubscribeResult};
pub use metrics::Metrics;
pub use protocol::close::CloseCode;
pub use protocol::error_code::ErrorCode;
pub use protocol::hello::{AuthMode, Hello, Ready, ResumeResult, SdkInfo, Welcome};
pub use server::{RiftServer, RiftServerBuilder};
pub use session::{
    AllowAllAuth, AuthContext, AuthHints, AuthProvider, ClientId, OffsetTracker, Session,
    SessionId, SessionState, SessionStore, TokenAuth,
};
pub use topic::{OrderingPolicy, RetentionPolicy, TopicProfile, TopicStore};
pub use transport::frame_codec::DEFAULT_MAX_BINARY_PAYLOAD;
pub use transport::frame_codec::{decode_binary_frame, decode_text_frame, encode_frame};
#[cfg(feature = "websocket")]
pub use transport::websocket::WebSocketTransport;
pub use transport::{Transport, TransportConnection, TransportListener};

// ── Shared utilities ─────────────────────────────────────────────────────────

/// Returns the current UTC time as milliseconds since the Unix epoch.
///
/// # Overflow Handling
///
/// Returns `i64::MIN` when the system clock is before the Unix epoch
/// (extremely rare but theoretically possible). `i64::MIN` is
/// unambiguously invalid as a real-world timestamp (the real epoch in
/// i64 ms is ~ year 292 million), so callers can treat it as a
/// sentinel. Any caller that checks freshness will treat it as
/// "expired", keeping behaviour safe.
///
/// # Use Cases
///
/// - Message timestamp fields (`created_at`)
/// - Session expiry checks
/// - Dedupe window checks
/// - Heartbeat timeout detection
///
/// **Important**: protocol-critical paths must not depend on the absolute
/// accuracy of this value — it reflects the server's local clock and may
/// drift from client clocks.
pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        // Return `i64::MIN` if the system clock is set before
        // the Unix epoch. `i64::MIN` is unambiguously invalid as
        // a real-world timestamp (the real epoch in i64 ms is
        // ~ year 292 million), so callers can treat it as a
        // sentinel.
        .unwrap_or(i64::MIN)
}
