//! # Global Error Type Hierarchy
//!
//! This module defines the complete error classification system for the Rift server,
//! using a layered enum design:
//!
//! ```text
//! RiftError
//! ├── FrameReject      — Frame-level rejection (malformed structure, unsupported version, payload too large, etc.)
//! ├── SessionReject    — Session-level rejection (not found, expired, conflict, replay offset expired, etc.)
//! ├── TopicReject      — Topic-level rejection (not found, closed, overloaded, insufficient permissions, etc.)
//! ├── MessageReject    — Message-level rejection (duplicate, expired, ack timeout, etc.)
//! ├── AuthReject       — Authentication/authorization rejection (missing, invalid, expired, insufficient permissions, etc.)
//! ├── SystemReject     — System-level failures (overloaded, maintenance, shard migration, region unavailable, etc.)
//! ├── ConfigError      — Configuration errors
//! ├── Io               — I/O errors
//! ├── SerdeJson / Cbor* — Serialization errors
//! ├── WebSocket        — WebSocket transport errors
//! └── Other            — Catch-all fallback
//! ```
//!
//! Each sub-enum implements `thiserror::Error`, providing clear error messages and
//! automatic `From` conversions.  The top-level `RiftError` uses `#[from]` to derive
//! `From` impls for every sub-type.

use thiserror::Error;

/// Convenience `Result` alias used throughout the crate.
///
/// Equivalent to `std::result::Result<T, RiftError>`, simplifying function signatures.
pub type Result<T> = std::result::Result<T, RiftError>;

/// Frame-level rejection reasons.
///
/// Produced during frame decoding and validation in the [`frame`](crate::frame) module,
/// indicating that the client sent a frame that does not conform to the protocol
/// specification or exceeds server-imposed limits.
#[derive(Debug, Error)]
pub enum FrameReject {
    /// The client's protocol version is not supported by the server.
    ///
    /// The range of supported versions is determined by
    /// [`version::SUPPORTED`](crate::protocol::version).
    /// When this error is triggered the server should inform the client of
    /// the supported version range via an Error frame.
    #[error("protocol version unsupported: client={client}, server={server}")]
    ProtocolVersionUnsupported {
        /// The protocol version sent by the client.
        client: u16,
        /// The protocol version supported by the server.
        server: u16,
    },

    /// The frame structure is invalid: missing fields, wrong types, corrupted encoding, etc.
    ///
    /// Carries a human-readable description intended for logging and debugging.
    #[error("frame is malformed: {0}")]
    FrameInvalid(String),

    /// The codec requested by the client is not supported by the server.
    ///
    /// Typically occurs during codec negotiation in the Hello phase.
    #[error("codec unsupported: {0}")]
    CodecUnsupported(String),

    /// The payload exceeds the maximum size configured on the server.
    ///
    /// `actual` is the number of bytes received; `max` is
    /// [`ServerConfig::max_payload_bytes`](crate::config::ServerConfig).
    #[error("payload too large: {actual} > {max}")]
    PayloadTooLarge {
        /// The actual payload size in bytes.
        actual: usize,
        /// The maximum allowed payload size in bytes.
        max: usize,
    },

    /// A required field specified by the protocol is missing.
    ///
    /// The field name is a compile-time string literal for easy log correlation.
    #[error("required field missing: {0}")]
    RequiredFieldMissing(&'static str),

    /// The frame structure does not match the expected schema.
    ///
    /// For example: a Subscribe frame carrying a Publish-only field.
    #[error("schema mismatch: {0}")]
    SchemaMismatch(String),

    /// The frame violates a message ordering constraint.
    ///
    /// For example: sending messages out of order in stream mode.
    #[error("order violation: {0}")]
    OrderViolation(String),
}

/// Session/resume operation rejection reasons.
///
/// Produced during session lookup, resumption, and replay stages,
/// typically triggering a disconnect-reconnect flow.
#[derive(Debug, Error)]
pub enum SessionReject {
    /// The requested session does not exist.
    ///
    /// Possible causes: the session expired and was garbage-collected,
    /// the `session_id` is misspelled, or the session was never created.
    #[error("session not found: {0}")]
    NotFound(String),

    /// The session has expired.
    ///
    /// The expiration duration is controlled by
    /// [`ServerConfig::idle_timeout`](crate::config::ServerConfig).
    #[error("session expired")]
    Expired,

    /// The connection was closed by the remote peer (normal
    /// WebSocket close, not an idle-timeout). This is distinct
    /// from `Expired` so callers can avoid triggering session
    /// resumption logic in response to a clean shutdown.
    #[error("session closed by peer")]
    Closed,

    /// The client's epoch does not match the one recorded by the server.
    ///
    /// An epoch conflict indicates the server has restarted or undergone
    /// failover, making the old session state unreliable.
    #[error("session epoch conflict: incoming={incoming}, current={current}")]
    Conflict {
        /// The epoch value sent by the client.
        incoming: u32,
        /// The epoch value recorded by the server.
        current: u32,
    },

    /// The resume request was rejected.
    ///
    /// Carries a human-readable description of the rejection reason.
    #[error("resume rejected: {0}")]
    ResumeRejected(String),

    /// The requested replay offset has expired and is no longer within the
    /// server's retained replay window.
    ///
    /// The client should switch to snapshot mode to obtain the current state
    /// and then begin consuming from the latest offset.
    #[error("replay offset expired: topic={topic}, requested={requested}")]
    ReplayOffsetExpired {
        /// The topic for which the replay offset has expired.
        topic: String,
        /// The offset the client requested.
        requested: i64,
    },

    /// The topic requires a snapshot before consumption can continue.
    ///
    /// Triggered when the topic has snapshots enabled and the consumer's
    /// offset is too far behind.
    #[error("snapshot required for topic: {0}")]
    SnapshotRequired(String),

    /// The connection timed out due to inactivity — no frames were
    /// received within the configured `idle_timeout` window.
    #[error("connection idle timeout")]
    IdleTimeout,
}

/// Topic operation rejection reasons.
///
/// Produced during topic lookup, creation, publishing, and subscription operations.
#[derive(Debug, Error)]
pub enum TopicReject {
    /// The topic does not exist.
    #[error("topic not found: {0}")]
    NotFound(String),

    /// The topic is closed and no longer accepts new messages.
    #[error("topic closed: {0}")]
    Closed(String),

    /// The topic is currently overloaded and temporarily rejecting new requests.
    ///
    /// The client should retry after the interval indicated by the server.
    #[error("topic overloaded: {0}")]
    Overloaded(String),

    /// The topic has reached its subscriber limit.
    ///
    /// The limit is defined by
    /// [`TopicProfile::max_subscribers`](crate::topic::TopicProfile).
    #[error("topic subscriber limit reached: {0}")]
    SubscriberLimit(String),

    /// The topic has reached its publisher limit.
    ///
    /// The limit is defined by
    /// [`TopicProfile::max_publishers`](crate::topic::TopicProfile).
    #[error("topic publisher limit reached: {0}")]
    PublisherLimit(String),

    /// The current identity is not authorized to access this topic.
    ///
    /// Typically returned by [`AuthProvider`](crate::session::AuthProvider).
    #[error("topic forbidden: {0}")]
    Forbidden(String),

    /// Topic-level rate limiting has been triggered.
    ///
    /// The client should reduce its send rate.
    #[error("topic rate limited: {0}")]
    RateLimited(String),

    /// The topic name is invalid.
    ///
    /// Valid names must be non-empty, at most 256 characters, and contain
    /// only `[a-zA-Z0-9._/-]`.
    #[error("invalid topic name: {0}")]
    InvalidName(String),
}

/// Message-level rejection reasons.
///
/// Produced during message publishing, acknowledgment, and distribution.
#[derive(Debug, Error)]
pub enum MessageReject {
    /// The message's `dedupe_key` has already been seen within the deduplication window.
    ///
    /// The deduplication window is controlled by
    /// [`ServerConfig::dedupe_window`](crate::config::ServerConfig).
    #[error("duplicate message: id={0}")]
    Duplicate(String),

    /// The message has expired.
    ///
    /// The message's TTL or `expires_at` timestamp has elapsed; it will
    /// not be delivered.
    #[error("message expired")]
    Expired,

    /// The message was rejected by application-layer logic.
    ///
    /// For example: the message content does not satisfy business validation rules.
    #[error("message rejected: {0}")]
    Rejected(String),

    /// The message exceeds the size limit.
    ///
    /// `actual` is the size in bytes; `max` is
    /// [`ServerConfig::max_payload_bytes`](crate::config::ServerConfig).
    #[error("message too large: {actual} > {max}")]
    TooLarge {
        /// The actual message size in bytes.
        actual: usize,
        /// The maximum allowed message size in bytes.
        max: usize,
    },

    /// Waiting for the message acknowledgment (ack) timed out.
    ///
    /// The consumer did not send an ack or nack within the expected time,
    /// so the message is considered a delivery failure.
    #[error("ack timeout: id={0}")]
    AckTimeout(String),

    /// Delivery to at least one subscriber failed.
    ///
    /// Possible causes: the consumer's outbound queue is full,
    /// the connection was dropped, etc.
    #[error("delivery failed: {0}")]
    DeliveryFailed(String),
}

/// Authentication / authorization failure reasons.
///
/// Produced during the Hello-phase authentication or topic access authorization.
#[derive(Debug, Error)]
pub enum AuthReject {
    /// The server requires authentication but the client did not provide credentials.
    ///
    /// The server should include an `auth_required` hint in the Error frame.
    #[error("authentication required")]
    Required,

    /// The provided credentials are invalid (token not found, signature mismatch, etc.).
    #[error("authentication invalid: {0}")]
    Invalid(String),

    /// The credentials have expired.
    ///
    /// The client should refresh its token and reconnect.
    #[error("authentication expired")]
    Expired,

    /// The credentials have been revoked (e.g., by an administrator).
    #[error("authentication revoked")]
    Revoked,

    /// Authentication succeeded, but the current identity lacks permission
    /// to perform the requested operation.
    ///
    /// For example: a regular user attempting to publish to a protected topic.
    #[error("permission denied: {0}")]
    Denied(String),
}

/// System-level failure reasons.
///
/// Triggered by infrastructure-level issues that are typically not recoverable
/// through client-side action alone.
#[derive(Debug, Error)]
pub enum SystemReject {
    /// The server is globally overloaded (CPU, memory, connection count, etc.).
    ///
    /// The client should reconnect using exponential backoff.
    #[error("system overloaded")]
    Overloaded,

    /// The server is undergoing maintenance and is temporarily unavailable.
    #[error("system maintenance")]
    Maintenance,

    /// The shard responsible for the topic has moved to another node.
    ///
    /// In a distributed deployment the client should reconnect to the new address.
    #[error("shard moved: topic={0}")]
    ShardMoved(String),

    /// The current region is unavailable.
    ///
    /// The client should switch to a different available region.
    #[error("region unavailable: {0}")]
    RegionUnavailable(String),

    /// An internal error occurred.
    ///
    /// Carries a descriptive message that is typically recorded in server logs.
    /// Clients should not attempt to parse this text.
    #[error("internal error: {0}")]
    Internal(String),
}

/// Top-level error type that combines all error sub-types.
///
/// Uses `#[from]` to automatically implement `From` for each sub-type, allowing
/// the `?` operator to propagate errors across function boundaries.
///
/// # Examples
///
/// ```rust,no_run
/// use rifts::error::{RiftError, TopicReject, Result};
///
/// fn find_topic(name: &str) -> Result<()> {
///     if name.is_empty() {
///         return Err(TopicReject::InvalidName("empty".into()).into());
///     }
///     Ok(())
/// }
/// ```
#[derive(Debug, Error)]
pub enum RiftError {
    /// Frame-level error (malformed structure, unsupported version, etc.).
    #[error(transparent)]
    Frame(#[from] FrameReject),

    /// Session-level error (not found, expired, conflict, etc.).
    #[error(transparent)]
    Session(#[from] SessionReject),

    /// Topic-level error (not found, overloaded, insufficient permissions, etc.).
    #[error(transparent)]
    Topic(#[from] TopicReject),

    /// Message-level error (duplicate, expired, ack timeout, etc.).
    #[error(transparent)]
    Message(#[from] MessageReject),

    /// Authentication / authorization error.
    #[error(transparent)]
    Auth(#[from] AuthReject),

    /// System-level failure.
    #[error(transparent)]
    System(#[from] SystemReject),

    /// Configuration error.
    #[error("config error: {0}")]
    Config(#[from] ConfigError),

    /// Operating-system or transport-layer I/O error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// `serde_json` serialization / deserialization error.
    ///
    /// Wrapped in `BoxedStdError` to avoid exposing `serde_json::Error` in the
    /// public API.
    #[error("serde_json error: {0}")]
    SerdeJson(BoxedStdError),

    /// CBOR serialization error (`ciborium::ser`).
    #[error("ciborium error: {0}")]
    Cbor(BoxedStdError),

    /// CBOR deserialization error (`ciborium::de`).
    #[error("ciborium de error: {0}")]
    CborDe(BoxedStdError),

    /// WebSocket transport-layer error.
    ///
    /// Only available when the `websocket` feature is enabled.
    #[error("websocket error: {0}")]
    WebSocket(BoxedStdError),

    /// Catch-all variant for wrapping errors that cannot be classified
    /// into a more specific variant.
    ///
    /// Prefer a more specific variant whenever possible; use this only
    /// when the error type cannot be enumerated in advance.
    #[error("other: {0}")]
    Other(BoxedStdError),
}

impl From<serde_json::Error> for RiftError {
    fn from(e: serde_json::Error) -> Self {
        Self::SerdeJson(BoxedStdError(Box::new(e)))
    }
}

impl From<ciborium::ser::Error<std::io::Error>> for RiftError {
    fn from(e: ciborium::ser::Error<std::io::Error>) -> Self {
        Self::Cbor(BoxedStdError(Box::new(e)))
    }
}

impl From<ciborium::de::Error<std::io::Error>> for RiftError {
    fn from(e: ciborium::de::Error<std::io::Error>) -> Self {
        Self::CborDe(BoxedStdError(Box::new(e)))
    }
}

#[cfg(feature = "websocket")]
impl From<tokio_tungstenite::tungstenite::Error> for RiftError {
    fn from(e: tokio_tungstenite::tungstenite::Error) -> Self {
        Self::WebSocket(BoxedStdError(Box::new(e)))
    }
}

/// Convenience wrapper that lifts any `std::error::Error` into [`RiftError::Other`].
///
/// Avoids adding a dedicated variant for every third-party error type.
///
/// # Internal Layout
///
/// Holds a `Box<dyn std::error::Error + Send + Sync>`, supporting any
/// heap-allocated error type.
#[derive(Debug, Error)]
#[error("{0}")]
pub struct BoxedStdError(pub Box<dyn std::error::Error + Send + Sync>);

impl RiftError {
    /// Wraps an arbitrary error into [`RiftError::Other`].
    ///
    /// Suitable for third-party library errors or custom error types.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use rifts::RiftError;
    /// let err: RiftError = RiftError::other(std::io::Error::new(
    ///     std::io::ErrorKind::Other, "something broke"
    /// ));
    /// ```
    pub fn other<E: std::error::Error + Send + Sync + 'static>(e: E) -> Self {
        Self::Other(BoxedStdError(Box::new(e)))
    }

    /// Constructs a `RiftError` from a `serde_json::Error` without relying on
    /// the `From` impl.
    ///
    /// Use this instead of `e.into()` when the compiler cannot infer the
    /// target type (e.g., in deeply nested closures) to avoid type ambiguity.
    pub fn from_serde_json(e: serde_json::Error) -> Self {
        Self::SerdeJson(BoxedStdError(Box::new(e)))
    }

    /// Constructs a `RiftError` from a CBOR serialization error.
    pub fn from_cbor_ser<E: std::error::Error + Send + Sync + 'static>(e: E) -> Self {
        Self::Cbor(BoxedStdError(Box::new(e)))
    }

    /// Constructs a `RiftError` from a CBOR deserialization error.
    pub fn from_cbor_de<E: std::error::Error + Send + Sync + 'static>(e: E) -> Self {
        Self::CborDe(BoxedStdError(Box::new(e)))
    }
}

/// Configuration error.
///
/// Produced by [`ServerConfig`](crate::config::ServerConfig) validation logic.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// A configuration field has an invalid value.
    ///
    /// `field` is the field name; `message` is a human-readable explanation.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use rifts::error::ConfigError;
    /// let err = ConfigError::Invalid {
    ///     field: "max_payload_bytes",
    ///     message: "must be > 0".into(),
    /// };
    /// ```
    #[error("invalid value for {field}: {message}")]
    Invalid {
        /// The name of the invalid field.
        field: &'static str,
        /// A human-readable description of the error.
        message: String,
    },
}
