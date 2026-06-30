//! # Structured Error Codes -- Spec §19.1
//!
//! This module defines the machine-readable error codes that can be
//! returned inside a `Frame::error()` response.  Each error code belongs
//! to one of several logical categories:
//!
//! | Category        | Variants                                        |
//! |-----------------|-------------------------------------------------|
//! | **Protocol**    | Version mismatches, invalid frames, codec and   |
//! |                 | payload issues, schema and field violations      |
//! | **Auth**        | Missing, invalid, expired, or revoked credentials|
//! | **Session**     | Not found, expired, conflicted, or rejected     |
//! |                 | resume sessions                                  |
//! | **Topic**       | Not found, closed, overloaded, or rate-limited  |
//! |                 | topics, subscriber/publisher limits              |
//! | **Message**     | Duplicate, expired, rejected, too large, ack    |
//! |                 | timeout, or delivery failure                     |
//! | **System**      | Overloaded, maintenance, shard moved, region    |
//! |                 | unavailable, or internal errors                  |
//!
//! ## Wire Format
//!
//! Error codes are transmitted as their stable `RIFT_*` string identifier
//! (see [`ErrorCode::as_str`]) so that older clients can gracefully
//! handle unknown codes without breaking.
//!
//! ## Retryability
//!
//! The [`ErrorCode::is_retryable`] method indicates whether the error is
//! **generally safe to retry** with back-off.  Transient conditions such
//! as system overload, shard migration, or message delivery failure are
//! retryable, while permanent failures such as authentication errors are
//! not.
//!
//! ## Category Summary
//!
//! * **Protocol** errors indicate a framing or encoding mistake by the
//!   remote peer.  They are never retryable.
//! * **Auth** errors indicate a credentials problem.  The client must
//!   obtain fresh credentials before retrying.
//! * **Session** errors relate to the session lifecycle, including
//!   resume and replay operations.
//! * **Topic** errors relate to topic-level resource limits and access
//!   control.
//! * **Message** errors relate to individual message validation,
//!   delivery, and acknowledgement.
//! * **System** errors indicate server-side or infrastructure issues;
//!   most are transient and retryable.

use std::fmt;

/// Stable, machine-readable error code returned in a `Frame::error()`.
///
/// Each variant represents a single, well-defined failure condition in
/// the Rift/1 protocol.  Variants are grouped by category (protocol,
/// auth, session, topic, message, system) for clarity.
///
/// ## Wire Representation
///
/// On the wire, an error code is transmitted as its `RIFT_*` string
/// identifier (obtained via [`as_str`](Self::as_str)).  This design
/// allows older peers to log or display unknown codes gracefully.
///
/// ## Retryability
///
/// Call [`is_retryable`](Self::is_retryable) to determine whether the
/// error is transient and can be retried with back-off.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    // ── Protocol (§19.1) ──────────────────────────────────────────
    /// The requested protocol version is not supported by the server.
    ///
    /// This typically means the client is too old (or too new) for the
    /// server's supported version range.  The client should upgrade or
    /// negotiate a compatible version.
    ProtocolVersionUnsupported,

    /// The received frame is structurally invalid (malformed header,
    /// missing required fields, etc.).
    ///
    /// This indicates a bug in the sender's frame encoder.
    ProtocolFrameInvalid,

    /// The requested content codec is not supported.
    ///
    /// The client should fall back to a codec listed in the server's
    /// `Welcome` capabilities or disconnect.
    ProtocolCodecUnsupported,

    /// The frame payload exceeds the server's configured maximum size.
    ///
    /// The client should split the payload across multiple frames or
    /// reduce the message size.
    ProtocolPayloadTooLarge,

    /// A required field is missing from the frame.
    ///
    /// This is a protocol-level validation error indicating that the
    /// sender omitted a field that the receiver requires.
    ProtocolRequiredFieldMissing,

    /// The frame contents do not match the expected schema.
    ///
    /// This may occur when the client and server disagree on the
    /// structure of a particular frame type.
    ProtocolSchemaMismatch,

    /// Frames were received out of the required order (e.g. a Publish
    /// before a Subscribe).
    ///
    /// The client must ensure it follows the frame sequencing rules
    /// defined in the Rift/1 specification.
    ProtocolOrderViolation,

    // ── Auth & permission ─────────────────────────────────────────
    /// Authentication is required but no credentials were provided.
    ///
    /// The client must include credentials in its `Hello` frame or
    /// subsequent authentication exchange.
    AuthRequired,

    /// The supplied credentials are syntactically or semantically invalid.
    ///
    /// This can happen when a token is malformed, a signature is
    /// incorrect, or the credential type is unrecognized.
    AuthInvalid,

    /// The supplied credentials have expired.
    ///
    /// The client must obtain fresh credentials (e.g. refresh the
    /// bearer token) before retrying.
    AuthExpired,

    /// The supplied credentials have been explicitly revoked.
    ///
    /// Unlike expiration, revocation is an active administrative action.
    /// The client should prompt the user to re-authenticate.
    AuthRevoked,

    /// The authenticated principal does not have permission for the
    /// requested operation.
    ///
    /// The client should not retry unless the user's permissions change.
    PermissionDenied,

    /// The authenticated principal is not allowed to access the
    /// requested topic.
    ///
    /// This is a topic-scoped variant of [`PermissionDenied`](Self::PermissionDenied).
    TopicForbidden,

    // ── Session & resume ──────────────────────────────────────────
    /// The referenced session could not be found.
    ///
    /// This typically means the session ID provided by the client is
    /// unknown to the server, possibly because the session has been
    /// garbage-collected.
    SessionNotFound,

    /// The referenced session has expired and is no longer valid.
    ///
    /// The client must establish a new session.
    SessionExpired,

    /// Another connection is already bound to the requested session.
    ///
    /// The server enforces single-connection-per-session semantics.
    /// The client should either wait or request a new session.
    SessionConflict,

    /// The server rejected the client's resume request.
    ///
    /// This may happen when the server cannot honour the resume for
    /// internal reasons (e.g. state migration in progress).
    ResumeRejected,

    /// The replay offset requested by the client has fallen outside the
    /// server's retention window.
    ///
    /// The client must perform a full snapshot catch-up instead of
    /// incremental replay.
    ReplayOffsetExpired,

    /// A full snapshot is required before the session can be resumed.
    ///
    /// The server cannot provide incremental replay and needs the client
    /// to fetch a complete state snapshot first.
    SnapshotRequired,

    // ── Topic ─────────────────────────────────────────────────────
    /// The requested topic does not exist.
    ///
    /// The client may need to create the topic first (if the server
    /// supports auto-creation) or use a different topic name.
    TopicNotFound,

    /// The requested topic has been closed for publishing.
    ///
    /// No new messages can be published to this topic.  Subscribers
    /// may still receive buffered messages.
    TopicClosed,

    /// The topic is temporarily overloaded and cannot accept more work.
    ///
    /// This is a transient condition; the client should retry with
    /// exponential back-off.
    TopicOverloaded,

    /// The topic has reached its maximum number of subscribers.
    ///
    /// The client should retry later or use a different topic.
    TopicSubscriberLimit,

    /// The topic has reached its maximum number of publishers.
    ///
    /// The client should retry later or use a different topic.
    TopicPublisherLimit,

    /// The topic's per-publisher rate limit has been exceeded.
    ///
    /// The client must reduce its publish rate or wait before retrying.
    TopicRateLimited,

    // ── Message ───────────────────────────────────────────────────
    /// The message is a duplicate of one already processed.
    ///
    /// The server uses message IDs to detect and reject duplicates.
    /// The client should not retry with the same message ID.
    MessageDuplicate,

    /// The message has expired (TTL reached zero).
    ///
    /// The message was queued but could not be delivered before its
    /// time-to-live window elapsed.
    MessageExpired,

    /// The message was rejected by the topic's validation rules.
    ///
    /// This may be due to schema validation, size constraints, or
    /// other topic-level admission policies.
    MessageRejected,

    /// The message payload exceeds the maximum allowed size.
    ///
    /// The client should compress or split the payload.
    MessageTooLarge,

    /// The server did not receive an acknowledgement for the message
    /// within the configured timeout window.
    ///
    /// This applies to messages that require at-least-once delivery
    /// confirmation.
    MessageAckTimeout,

    /// The message could not be delivered to its destination.
    ///
    /// This is a transient condition; the client should retry.
    MessageDeliveryFailed,

    // ── System ────────────────────────────────────────────────────
    /// The server is temporarily overloaded (CPU, memory, connections).
    ///
    /// The client should back off and retry with exponential delay.
    SystemOverloaded,

    /// The server is undergoing scheduled maintenance.
    ///
    /// The client should disconnect and reconnect after the
    /// maintenance window.
    SystemMaintenance,

    /// The relevant shard has moved to a different node.
    ///
    /// The client should reconnect; the new node will be discovered
    /// through the routing layer.
    SystemShardMoved,

    /// The target region is currently unavailable.
    ///
    /// The client should fail over to a different region if available.
    SystemRegionUnavailable,

    /// An unexpected internal error occurred.
    ///
    /// This indicates a server-side bug.  The client may retry but
    /// should report the error if it persists.
    SystemInternal,
}

impl ErrorCode {
    /// Returns the stable wire-format string identifier for this error code.
    ///
    /// All identifiers are prefixed with `RIFT_` and use `SCREAMING_SNAKE_CASE`.
    /// This string is what is transmitted inside an Error frame so that older
    /// peers can log or display unknown codes without panicking.
    ///
    /// ```rust
    /// use rifts_core::protocol::error_code::ErrorCode;
    ///
    /// assert_eq!(ErrorCode::AuthInvalid.as_str(), "RIFT_AUTH_INVALID");
    /// assert_eq!(ErrorCode::SystemInternal.as_str(), "RIFT_SYSTEM_INTERNAL");
    /// ```
    pub fn as_str(self) -> &'static str {
        match self {
            // Protocol
            ErrorCode::ProtocolVersionUnsupported => "RIFT_PROTOCOL_VERSION_UNSUPPORTED",
            ErrorCode::ProtocolFrameInvalid => "RIFT_PROTOCOL_FRAME_INVALID",
            ErrorCode::ProtocolCodecUnsupported => "RIFT_PROTOCOL_CODEC_UNSUPPORTED",
            ErrorCode::ProtocolPayloadTooLarge => "RIFT_PROTOCOL_PAYLOAD_TOO_LARGE",
            ErrorCode::ProtocolRequiredFieldMissing => "RIFT_PROTOCOL_REQUIRED_FIELD_MISSING",
            ErrorCode::ProtocolSchemaMismatch => "RIFT_PROTOCOL_SCHEMA_MISMATCH",
            ErrorCode::ProtocolOrderViolation => "RIFT_PROTOCOL_ORDER_VIOLATION",

            // Auth
            ErrorCode::AuthRequired => "RIFT_AUTH_REQUIRED",
            ErrorCode::AuthInvalid => "RIFT_AUTH_INVALID",
            ErrorCode::AuthExpired => "RIFT_AUTH_EXPIRED",
            ErrorCode::AuthRevoked => "RIFT_AUTH_REVOKED",
            ErrorCode::PermissionDenied => "RIFT_PERMISSION_DENIED",
            ErrorCode::TopicForbidden => "RIFT_TOPIC_FORBIDDEN",

            // Session
            ErrorCode::SessionNotFound => "RIFT_SESSION_NOT_FOUND",
            ErrorCode::SessionExpired => "RIFT_SESSION_EXPIRED",
            ErrorCode::SessionConflict => "RIFT_SESSION_CONFLICT",
            ErrorCode::ResumeRejected => "RIFT_RESUME_REJECTED",
            ErrorCode::ReplayOffsetExpired => "RIFT_REPLAY_OFFSET_EXPIRED",
            ErrorCode::SnapshotRequired => "RIFT_SNAPSHOT_REQUIRED",

            // Topic
            ErrorCode::TopicNotFound => "RIFT_TOPIC_NOT_FOUND",
            ErrorCode::TopicClosed => "RIFT_TOPIC_CLOSED",
            ErrorCode::TopicOverloaded => "RIFT_TOPIC_OVERLOADED",
            ErrorCode::TopicSubscriberLimit => "RIFT_TOPIC_SUBSCRIBER_LIMIT",
            ErrorCode::TopicPublisherLimit => "RIFT_TOPIC_PUBLISHER_LIMIT",
            ErrorCode::TopicRateLimited => "RIFT_TOPIC_RATE_LIMITED",

            // Message
            ErrorCode::MessageDuplicate => "RIFT_MESSAGE_DUPLICATE",
            ErrorCode::MessageExpired => "RIFT_MESSAGE_EXPIRED",
            ErrorCode::MessageRejected => "RIFT_MESSAGE_REJECTED",
            ErrorCode::MessageTooLarge => "RIFT_MESSAGE_TOO_LARGE",
            ErrorCode::MessageAckTimeout => "RIFT_MESSAGE_ACK_TIMEOUT",
            ErrorCode::MessageDeliveryFailed => "RIFT_MESSAGE_DELIVERY_FAILED",

            // System
            ErrorCode::SystemOverloaded => "RIFT_SYSTEM_OVERLOADED",
            ErrorCode::SystemMaintenance => "RIFT_SYSTEM_MAINTENANCE",
            ErrorCode::SystemShardMoved => "RIFT_SYSTEM_SHARD_MOVED",
            ErrorCode::SystemRegionUnavailable => "RIFT_SYSTEM_REGION_UNAVAILABLE",
            ErrorCode::SystemInternal => "RIFT_SYSTEM_INTERNAL",
        }
    }

    /// Returns `true` if this error condition is **generally safe to retry**
    /// with exponential back-off.
    ///
    /// Transient conditions -- such as system overload, shard migration,
    /// topic rate limiting, or message delivery failure -- are considered
    /// retryable.  Permanent failures -- such as invalid credentials or
    /// protocol violations -- are **not** retryable because retrying would
    /// not change the outcome.
    ///
    /// The following error codes are retryable:
    ///
    /// * [`SystemOverloaded`](Self::SystemOverloaded)
    /// * [`SystemShardMoved`](Self::SystemShardMoved)
    /// * [`SystemRegionUnavailable`](Self::SystemRegionUnavailable)
    /// * [`TopicOverloaded`](Self::TopicOverloaded)
    /// * [`TopicRateLimited`](Self::TopicRateLimited)
    /// * [`ReplayOffsetExpired`](Self::ReplayOffsetExpired)
    /// * [`MessageDeliveryFailed`](Self::MessageDeliveryFailed)
    /// * [`MessageAckTimeout`](Self::MessageAckTimeout)
    ///
    /// ```rust
    /// use rifts_core::protocol::error_code::ErrorCode;
    ///
    /// assert!(ErrorCode::SystemOverloaded.is_retryable());
    /// assert!(!ErrorCode::AuthInvalid.is_retryable());
    /// ```
    pub fn is_retryable(self) -> bool {
        matches!(
            self,
            ErrorCode::SystemOverloaded
                | ErrorCode::SystemShardMoved
                | ErrorCode::SystemRegionUnavailable
                | ErrorCode::TopicOverloaded
                | ErrorCode::TopicRateLimited
                | ErrorCode::ReplayOffsetExpired
                | ErrorCode::MessageDeliveryFailed
                | ErrorCode::MessageAckTimeout
        )
    }
}

impl fmt::Display for ErrorCode {
    /// Formats the error code by delegating to [`as_str`](Self::as_str),
    /// producing the stable `RIFT_*` wire identifier.
    ///
    /// This is equivalent to calling `as_str()` and is suitable for
    /// inclusion in log messages and user-facing error reports.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn str_round_trip() {
        for c in [
            ErrorCode::ProtocolVersionUnsupported,
            ErrorCode::AuthInvalid,
            ErrorCode::SessionConflict,
            ErrorCode::TopicNotFound,
            ErrorCode::MessageDuplicate,
            ErrorCode::SystemInternal,
        ] {
            assert!(!c.as_str().is_empty());
            assert!(c.as_str().starts_with("RIFT_"));
        }
    }

    #[test]
    fn retryable() {
        assert!(ErrorCode::SystemOverloaded.is_retryable());
        assert!(!ErrorCode::AuthInvalid.is_retryable());
        assert!(!ErrorCode::ProtocolFrameInvalid.is_retryable());
    }
}
