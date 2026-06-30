//! # Close Code -- Spec §20
//!
//! This module defines the structured close codes used when a Rift/1
//! connection is shut down.  A close code is carried in a **Close frame**
//! and tells the remote peer *why* the connection was terminated.
//!
//! ## Close Code Range
//!
//! Close codes are `u16` values divided into the following ranges:
//!
//! | Range        | Meaning                                      |
//! |--------------|----------------------------------------------|
//! | 1000         | Normal closure                               |
//! | 1001 – 1015  | Rift-defined close reasons (see [`CloseCode`])|
//! | Other        | Reserved / undefined                         |
//!
//! ## Relationship to WebSocket Close Codes
//!
//! The numbering scheme is inspired by
//! [RFC 6455 §7.4](https://tools.ietf.org/html/rfc6455#section-7.4),
//! but the **semantics are entirely Rift-specific** and must not be
//! confused with WebSocket close codes.
//!
//! ## Usage Example
//!
//! ```rust
//! use rifts_core::protocol::close::CloseCode;
//!
//! let code = CloseCode::AuthFailed;
//! assert_eq!(code.as_u16(), 1004);
//! assert_eq!(code.name(), "auth_failed");
//! ```
//!
//! ## Round-Trip Guarantee
//!
//! Every defined close code satisfies the invariant
//! `CloseCode::from_u16(code.as_u16()) == Some(code)`.  Values outside the
//! defined range produce `None` from `from_u16`.

use std::fmt;

/// Structured close code for a Rift/1 connection.
///
/// When a connection is closed -- either gracefully or due to an error --
/// the initiating side attaches a `CloseCode` to the Close frame so the
/// remote peer can determine the reason and decide whether to reconnect.
///
/// Every variant maps to a specific `u16` value defined in spec §20.
/// The enum is `#[repr(u16)]` so that the numeric value can be extracted
/// with a zero-cost cast via [`as_u16`](Self::as_u16).
///
/// ## Reconnection Guidance
///
/// Certain close codes signal transient conditions where the client SHOULD
/// retry (e.g. [`Draining`](Self::Draining), [`ServerOverloaded`](Self::ServerOverloaded),
/// [`IdleTimeout`](Self::IdleTimeout)).  Others indicate permanent
/// conditions that require human intervention (e.g. [`AuthFailed`](Self::AuthFailed),
/// [`PolicyViolation`](Self::PolicyViolation)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum CloseCode {
    /// Normal closure -- the connection was terminated deliberately by one
    /// side after all pending work had been drained.
    Normal = 1000,

    /// Graceful draining -- the server is draining its outbound message
    /// queue before shutting down the connection.
    ///
    /// The client should wait briefly and then attempt to reconnect.
    Draining = 1001,

    /// Protocol error -- the remote peer sent a frame that does not
    /// conform to the Rift/1 specification.
    ProtocolError = 1002,

    /// Unsupported codec -- the client requested a codec that the server
    /// does not support.
    UnsupportedCodec = 1003,

    /// Authentication failed -- the supplied credentials were invalid,
    /// the signature did not match, or the identity could not be verified
    /// for any other reason.
    AuthFailed = 1004,

    /// Authentication expired -- the credentials (e.g. a bearer token)
    /// have passed their validity window.
    AuthExpired = 1005,

    /// Permission revoked -- the credentials are still structurally valid
    /// but have been explicitly revoked by an administrator.
    PermissionRevoked = 1006,

    /// Session conflict -- another connection is already bound to the
    /// session identifier requested by this client.
    SessionConflict = 1007,

    /// Rate limited -- the client is sending frames faster than the
    /// server's configured rate-limiting threshold allows.
    RateLimited = 1008,

    /// Payload too large -- a single frame's payload exceeds the maximum
    /// size the server is willing to accept.
    PayloadTooLarge = 1009,

    /// Slow consumer -- the client's outbound queue is backing up because
    /// the client is not reading frames quickly enough.
    SlowConsumer = 1010,

    /// Server overloaded -- the server is running low on one or more
    /// critical resources (CPU, memory, open connections).
    ServerOverloaded = 1011,

    /// Shard moved -- the topic partition requested by the client has
    /// been migrated to a different server node.
    ShardMoved = 1012,

    /// Idle timeout -- no frames were received on the connection within
    /// the configured idle-timeout window.
    IdleTimeout = 1013,

    /// Client upgrade required -- the server requires a newer SDK version
    /// and asks the client to upgrade before reconnecting.
    ClientUpgradeRequired = 1014,

    /// Policy violation -- the client's behavior violates a server-side
    /// policy (for example, using an illegal topic name).
    PolicyViolation = 1015,
}

impl CloseCode {
    /// Attempts to convert a raw `u16` value into a [`CloseCode`].
    ///
    /// Returns `None` if the value does not correspond to any defined
    /// close code.  This is the inverse of [`as_u16`](Self::as_u16):
    ///
    /// ```rust
    /// use rifts_core::protocol::close::CloseCode;
    ///
    /// assert_eq!(CloseCode::from_u16(1000), Some(CloseCode::Normal));
    /// assert_eq!(CloseCode::from_u16(0), None);
    /// ```
    pub fn from_u16(v: u16) -> Option<Self> {
        Some(match v {
            1000 => CloseCode::Normal,
            1001 => CloseCode::Draining,
            1002 => CloseCode::ProtocolError,
            1003 => CloseCode::UnsupportedCodec,
            1004 => CloseCode::AuthFailed,
            1005 => CloseCode::AuthExpired,
            1006 => CloseCode::PermissionRevoked,
            1007 => CloseCode::SessionConflict,
            1008 => CloseCode::RateLimited,
            1009 => CloseCode::PayloadTooLarge,
            1010 => CloseCode::SlowConsumer,
            1011 => CloseCode::ServerOverloaded,
            1012 => CloseCode::ShardMoved,
            1013 => CloseCode::IdleTimeout,
            1014 => CloseCode::ClientUpgradeRequired,
            1015 => CloseCode::PolicyViolation,
            _ => return None,
        })
    }

    /// Returns the numeric `u16` representation of this close code.
    ///
    /// This is the value transmitted on the wire inside a Close frame.
    /// Because the enum is `#[repr(u16)]`, this cast is zero-cost.
    pub fn as_u16(self) -> u16 {
        self as u16
    }

    /// Returns the human-readable, snake_case wire name of this close code.
    ///
    /// The name is stable and intended for logging and diagnostics; it is
    /// *not* the value sent on the wire (that is [`as_u16`](Self::as_u16)).
    ///
    /// ```rust
    /// use rifts_core::protocol::close::CloseCode;
    ///
    /// assert_eq!(CloseCode::ProtocolError.name(), "protocol_error");
    /// assert_eq!(CloseCode::ShardMoved.name(), "shard_moved");
    /// ```
    pub fn name(self) -> &'static str {
        match self {
            CloseCode::Normal => "normal",
            CloseCode::Draining => "draining",
            CloseCode::ProtocolError => "protocol_error",
            CloseCode::UnsupportedCodec => "unsupported_codec",
            CloseCode::AuthFailed => "auth_failed",
            CloseCode::AuthExpired => "auth_expired",
            CloseCode::PermissionRevoked => "permission_revoked",
            CloseCode::SessionConflict => "session_conflict",
            CloseCode::RateLimited => "rate_limited",
            CloseCode::PayloadTooLarge => "payload_too_large",
            CloseCode::SlowConsumer => "slow_consumer",
            CloseCode::ServerOverloaded => "server_overloaded",
            CloseCode::ShardMoved => "shard_moved",
            CloseCode::IdleTimeout => "idle_timeout",
            CloseCode::ClientUpgradeRequired => "client_upgrade_required",
            CloseCode::PolicyViolation => "policy_violation",
        }
    }
}

impl fmt::Display for CloseCode {
    /// Formats the close code as `"<name>(<numeric>)"`, e.g.
    /// `"auth_failed(1004)"`.
    ///
    /// This format is intended for log messages and error reports where
    /// both the human-readable name and the numeric wire value are useful.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}({})", self.name(), self.as_u16())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        for c in [
            CloseCode::Normal,
            CloseCode::Draining,
            CloseCode::ProtocolError,
            CloseCode::UnsupportedCodec,
            CloseCode::AuthFailed,
            CloseCode::AuthExpired,
            CloseCode::PermissionRevoked,
            CloseCode::SessionConflict,
            CloseCode::RateLimited,
            CloseCode::PayloadTooLarge,
            CloseCode::SlowConsumer,
            CloseCode::ServerOverloaded,
            CloseCode::ShardMoved,
            CloseCode::IdleTimeout,
            CloseCode::ClientUpgradeRequired,
            CloseCode::PolicyViolation,
        ] {
            assert_eq!(CloseCode::from_u16(c.as_u16()), Some(c));
        }
    }

    #[test]
    fn unknown() {
        assert_eq!(CloseCode::from_u16(0), None);
        assert_eq!(CloseCode::from_u16(9999), None);
    }
}
