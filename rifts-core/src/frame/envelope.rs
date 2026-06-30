//! # Frame Envelope — Rift/1 Wire-Level Message Container
//!
//! `Frame` is the unified container for all content exchanged between client and server.
//! Whether it is a handshake (Hello), subscription (Subscribe), message publish (Publish),
//! acknowledgment (Ack), flow control (Flow Control), or error (Error), everything is
//! encapsulated within the same `Frame` structure.
//!
//! ## Field Categories
//!
//! | Category | Fields |
//! |----------|--------|
//! | **Routing** | `topic`, `event`, `stream_id` |
//! | **Correlation** | `message_id`, `correlation_id`, `session_id` |
//! | **Tracing** | `trace_id`, `timestamp`, `ttl_ms` |
//! | **Control** | `frame_type`, `flags`, `codec`, `priority` |
//! | **Payload** | `payload` |
//!
//! Fields are aligned with spec section 6.1.

use std::fmt;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::frame::{EncodingFormat, FrameFlags, FrameType, Priority};

/// Rift/1 protocol frame — the fundamental unit of exchange between client and server.
///
/// Each `Frame` carries routing, correlation, tracing metadata and an optional payload.
/// When transmitted on the wire, frames are encoded as JSON text or CBOR binary
/// depending on the chosen [`EncodingFormat`].
///
/// # Usage
///
/// ```rust
/// use rifts_core::frame::{Frame, FrameType};
///
/// let mut frame = Frame::control();
/// frame.topic = Some("chat/room1".into());
/// frame.event = Some("message".into());
/// ```
///
/// # Field Completeness
///
/// `frame_id`, `frame_type`, and `timestamp` are required; other fields are set
/// as needed by semantic context.
/// The [`Default`] impl sets all fields to zero/None and is not suitable for direct
/// transmission — use convenience constructors like [`Frame::control()`] instead.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Frame {
    /// Protocol version number.
    ///
    /// Required by spec section 6.1, used for version negotiation and compatibility checks.
    pub version: u16,

    /// Monotonically increasing frame sequence number within the current connection.
    ///
    /// Assigned by the sender, used for frame ordering, deduplication, and ack correlation.
    pub frame_id: u64,

    /// Frame category (Control, Data, Ack, Flow, Error).
    ///
    /// Determines the semantics of the frame; see [`FrameType`].
    pub frame_type: FrameType,

    /// Bit-flag set describing additional frame attributes.
    ///
    /// Examples: compression, encryption, fragmentation, requires-ack, replayed, etc.
    /// See [`FrameFlags`].
    pub flags: FrameFlags,

    /// Payload encoding format (JSON or CBOR).
    ///
    /// Negotiated during the Hello handshake and remains consistent for the
    /// entire lifetime of the connection.
    pub codec: EncodingFormat,

    /// Logical session identifier.
    ///
    /// Assigned by the server after the connection is established, used to locate
    /// a previous session during disconnect-resume.
    /// `None` indicates not yet assigned (before the Hello phase).
    pub session_id: Option<String>,

    /// Stream identifier (spec section 6.1).
    ///
    /// Used to group frames into logical streams, e.g., correlating requests and
    /// responses for the same operation.
    pub stream_id: Option<String>,

    /// Target topic name.
    ///
    /// For publish/subscribe operations, specifies the topic the message belongs to.
    /// For control frames (subscribe, unsubscribe, etc.), specifies the target of the operation.
    pub topic: Option<String>,

    /// Event name.
    ///
    /// In Data frames, identifies the business message type (e.g., "message", "update").
    /// In Control frames, identifies the control operation type (e.g., "hello", "subscribe").
    pub event: Option<String>,

    /// Globally unique message identifier.
    ///
    /// Used for deduplication (`dedupe_key`), ack correlation, and message tracing.
    /// Typically generated using ULID or UUID v7.
    pub message_id: Option<String>,

    /// Request-response correlation identifier.
    ///
    /// Set by the sender in a request frame; the corresponding response frame
    /// carries the same value to match the response to its request.
    /// This is the foundation for RPC semantics.
    pub correlation_id: Option<String>,

    /// Distributed tracing identifier (corresponds to spec section 23.1).
    ///
    /// End-to-end trace chain identifier.
    pub trace_id: Option<String>,

    /// Sender timestamp (millisecond-precision Unix timestamp).
    ///
    /// Used for message expiration checks, latency measurement, and replay ordering.
    pub timestamp: i64,

    /// Message time-to-live in milliseconds.
    ///
    /// `None` means no TTL is set; the message never expires (subject to retention policy).
    /// Messages exceeding their TTL are discarded before delivery and counted
    /// in the `messages_expired_total` metric.
    pub ttl_ms: Option<u32>,

    /// Message priority, used to determine send order and backpressure drop strategy.
    ///
    /// See [`Priority`]. When `None`, equivalent to `Normal`.
    pub priority: Option<Priority>,

    /// Business or control payload.
    ///
    /// The body of Data frames (business messages) and Control frames.
    /// The encoding format is determined by the `codec` field.
    /// Ack frames, Flow frames, and Error frames typically do not carry a payload.
    pub payload: Option<Bytes>,
}

impl Default for Frame {
    fn default() -> Self {
        Self {
            version: 0,
            frame_id: 0,
            frame_type: FrameType::Control,
            flags: FrameFlags::empty(),
            codec: EncodingFormat::Json,
            session_id: None,
            stream_id: None,
            topic: None,
            event: None,
            message_id: None,
            correlation_id: None,
            trace_id: None,
            timestamp: 0,
            ttl_ms: None,
            priority: None,
            payload: None,
        }
    }
}

impl Frame {
    /// Creates a minimal Control frame.
    ///
    /// Control frames carry protocol interactions: Hello/Welcome/Ready,
    /// Subscribe/Unsubscribe, Resume, Ping/Pong, etc.
    /// The specific control operation is distinguished by the `event` field.
    pub fn control() -> Self {
        Self {
            frame_type: FrameType::Control,
            ..Self::default()
        }
    }

    /// Creates a minimal Data frame.
    ///
    /// Data frames carry business messages: Events, State updates, Commands,
    /// Replies, Datagrams, etc.
    /// The specific message type is distinguished by the `event` field and payload content.
    pub fn data() -> Self {
        Self {
            frame_type: FrameType::Data,
            ..Self::default()
        }
    }

    /// Creates a minimal Ack frame.
    ///
    /// Ack frames provide reliable delivery semantics. When a client receives a
    /// message that requires acknowledgment, it replies with an Ack frame,
    /// allowing the server to advance the offset accordingly.
    pub fn ack() -> Self {
        Self {
            frame_type: FrameType::Ack,
            ..Self::default()
        }
    }

    /// Creates a minimal Flow Control frame.
    ///
    /// Flow frames carry backpressure notifications, window adjustments,
    /// and degradation alerts.
    pub fn flow() -> Self {
        Self {
            frame_type: FrameType::Flow,
            ..Self::default()
        }
    }

    /// Creates a minimal Error frame.
    ///
    /// Error frames carry protocol errors, authorization errors, business errors,
    /// or system errors. Structured error details are conveyed via the `payload`.
    pub fn error() -> Self {
        Self {
            frame_type: FrameType::Error,
            ..Self::default()
        }
    }

    /// Returns whether this frame requires the receiver to send an acknowledgment (Ack).
    ///
    /// Corresponds to the `FrameFlags::REQUIRES_ACK` flag.
    /// Messages with this flag set wait for client acknowledgment after delivery;
    /// if the ack times out, it is counted in `ack_timeout_total`.
    pub fn requires_ack(&self) -> bool {
        self.flags.contains(FrameFlags::REQUIRES_ACK)
    }

    /// Returns whether this frame is a replay frame (spec section 13.1).
    ///
    /// Corresponds to the `FrameFlags::REPLAYED` flag.
    /// Replay frames are historical messages re-sent by the server after a
    /// disconnect-resume. Clients should update their offset accordingly
    /// rather than reprocessing the business logic.
    pub fn is_replay(&self) -> bool {
        self.flags.contains(FrameFlags::REPLAYED)
    }

    /// Marks this frame as a replay frame.
    ///
    /// Called by the server when replaying historical messages; sets the `REPLAYED` flag.
    pub fn mark_replay(&mut self) {
        self.flags.set(FrameFlags::REPLAYED);
    }

    /// Returns whether this frame is a snapshot frame (spec section 6.3).
    ///
    /// Corresponds to the `FrameFlags::SNAPSHOT` flag.
    /// Snapshot frames carry the current state snapshot of a topic,
    /// used to quickly initialize new subscribers.
    pub fn is_snapshot(&self) -> bool {
        self.flags.contains(FrameFlags::SNAPSHOT)
    }

    /// Marks this frame as a snapshot frame.
    ///
    /// Called by the server when sending a topic snapshot; sets the `SNAPSHOT` flag.
    pub fn mark_snapshot(&mut self) {
        self.flags.set(FrameFlags::SNAPSHOT);
    }
}

impl fmt::Display for Frame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Frame[id={} type={} codec={} topic={} event={} msg_id={} corr_id={} flags={} payload={}B]",
            self.frame_id,
            self.frame_type,
            self.codec,
            self.topic.as_deref().unwrap_or("-"),
            self.event.as_deref().unwrap_or("-"),
            self.message_id.as_deref().unwrap_or("-"),
            self.correlation_id.as_deref().unwrap_or("-"),
            self.flags,
            self.payload.as_ref().map(|p| p.len()).unwrap_or(0),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flag_round_trip() {
        let mut f = FrameFlags::empty();
        f.set(FrameFlags::COMPRESSED);
        f.set(FrameFlags::REQUIRES_ACK);
        assert!(f.contains(FrameFlags::COMPRESSED));
        assert!(f.contains(FrameFlags::REQUIRES_ACK));
        assert!(!f.contains(FrameFlags::ENCRYPTED));
        assert_eq!(f.bits(), FrameFlags::COMPRESSED | FrameFlags::REQUIRES_ACK);
    }

    #[test]
    fn priority_default() {
        assert_eq!(Priority::default(), Priority::Normal);
    }

    #[test]
    fn frame_type_tag() {
        for t in [
            FrameType::Control,
            FrameType::Data,
            FrameType::Ack,
            FrameType::Flow,
            FrameType::Error,
        ] {
            assert_eq!(FrameType::from_tag(t.tag()), Some(t));
        }
        assert_eq!(FrameType::from_tag(b'X'), None);
    }

    #[test]
    fn encoding_format_tag() {
        assert_eq!(
            EncodingFormat::from_tag(EncodingFormat::Json.tag()),
            Some(EncodingFormat::Json)
        );
        assert_eq!(
            EncodingFormat::from_tag(EncodingFormat::Cbor.tag()),
            Some(EncodingFormat::Cbor)
        );
        assert_eq!(EncodingFormat::from_tag(b'?'), None);
    }

    #[test]
    fn frame_constructors() {
        assert_eq!(Frame::control().frame_type, FrameType::Control);
        assert_eq!(Frame::data().frame_type, FrameType::Data);
        assert_eq!(Frame::ack().frame_type, FrameType::Ack);
        assert_eq!(Frame::flow().frame_type, FrameType::Flow);
        assert_eq!(Frame::error().frame_type, FrameType::Error);
    }
}
