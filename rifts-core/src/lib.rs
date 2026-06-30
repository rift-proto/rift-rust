#![allow(rustdoc::broken_intra_doc_links, rustdoc::private_intra_doc_links)]
//! # Rifts Core — Protocol Fundamentals
//!
//! This crate defines the foundational types, traits, and utilities shared across
//! the Rift Realtime Protocol ecosystem. It contains no transport backends,
//! no storage backends, and no broker implementations.
//!
//! ## Modules
//!
//! | Module | Responsibility |
//! |--------|----------------|
//! | [`ack`] | Message acknowledgement (ack / nack) semantics and tracking |
//! | [`codec`] | Serialization codecs (JSON / CBOR) |
//! | [`config`] | Server configuration (payload limits, heartbeat policy, etc.) |
//! | [`error`] | Global error type hierarchy |
//! | [`flow`] | Flow control — backpressure, rate limiting |
//! | [`frame`] | Protocol frame structure and codec helpers |
//! | [`frame_codec`] | Wire-format frame encoder and decoder |
//! | [`message`] | Message semantic layer (Command / Event / Datagram / Stream / Snapshot) |
//! | [`metrics`] | Process-local metric counters |
//! | [`protocol`] | Protocol constants — handshake, heartbeat, error codes, versioning, close codes |
//! | [`topic`] | Topic profile — retention policy, ordering policy, storage binding |

#![forbid(unsafe_code)]
#![deny(unreachable_pub)]
#![deny(missing_docs)]
#![warn(rust_2018_idioms)]

// Module declarations

/// Message acknowledgement (ack / nack) semantics and tracking.
pub mod ack;

/// Serialization codecs (JSON, CBOR).
pub mod codec;

/// Server configuration structures and defaults.
pub mod config;

/// Global error type hierarchy.
pub mod error;

/// Flow control strategies — backpressure, rate limiting.
pub mod flow;

/// Protocol frame (Frame) structure and codec helpers.
pub mod frame;

/// Wire-format frame encoder and decoder shared by all transports.
pub mod frame_codec;

/// Message semantic layer — Command, Event, Datagram, Stream, Snapshot, etc.
pub mod message;

/// Process-local metric counters.
pub mod metrics;

/// Protocol constants — handshake, heartbeat, error codes, versioning, close codes.
pub mod protocol;

/// Topic profile — retention policy, ordering policy, storage.
pub mod topic;

// Public API re-exports

pub use error::{BoxedStdError, ConfigError, Result, RiftError};
pub use frame::{EncodingFormat, Frame, FrameFlags, FrameType, Priority};

// Shared utilities

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
pub fn now_ms() -> i64 {
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
