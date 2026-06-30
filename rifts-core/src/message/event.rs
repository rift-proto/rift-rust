//! # Event -- Business Event Messages (spec section 8)
//!
//! Events are the most commonly used message type in the publish/subscribe model.
//! A publisher sends an event to a topic, and all subscribers receive it according to the delivery semantics.
//!
//! ## Typical Use Cases
//!
//! - Chat messages: `event = "chat.message.created"`
//! - Data changes: `event = "user.profile.updated"`
//! - Notification pushes: `event = "notification.push"`

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::{MessageReject, Result, RiftError};

/// A business event published to a topic.
///
/// Events are the core data unit of the Rift protocol. Each event carries a business payload,
/// an optional deduplication key, an ordering key, and a TTL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// Event name that identifies the business type of the message.
    ///
    /// A dotted hierarchical format is recommended, e.g. `"chat.message.created"`.
    pub event: String,

    /// Globally unique message ID.
    ///
    /// ULID or UUID v7 (time-ordered) is recommended; used for deduplication and acknowledgment correlation.
    pub message_id: String,

    /// Schema identifier, formatted as `{domain}.{name}@{major}.{minor}`.
    ///
    /// Used for version management and payload validation. E.g. `"chat.message.created@1.0"`.
    pub schema: String,

    /// Business payload in JSON format, whose structure is defined by `schema`.
    pub payload: serde_json::Value,

    /// Deduplication key (optional).
    ///
    /// When set, the server rejects duplicate messages with the same `dedupe_key` within
    /// the deduplication window (see [`ServerConfig::dedupe_window`](crate::config::ServerConfig)).
    /// Suitable for idempotent publish scenarios.
    pub dedupe_key: Option<String>,

    /// Ordering key (optional).
    ///
    /// Within the same topic, messages sharing the same `ordering_key` are strictly ordered.
    /// Messages with different `ordering_key` values have no ordering guarantees.
    /// Suitable for partitioned-ordering scenarios.
    pub ordering_key: Option<String>,

    /// Message time-to-live in milliseconds (optional).
    ///
    /// Messages that exceed their TTL are discarded before delivery. `None` means no TTL.
    pub ttl_ms: Option<u32>,
}

impl Event {
    /// Creates a new Event instance.
    ///
    /// `dedupe_key`, `ordering_key`, and `ttl_ms` default to `None`.
    pub fn new(
        event: impl Into<String>,
        message_id: impl Into<String>,
        schema: impl Into<String>,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            event: event.into(),
            message_id: message_id.into(),
            schema: schema.into(),
            payload,
            dedupe_key: None,
            ordering_key: None,
            ttl_ms: None,
        }
    }

    /// Estimates the encoded byte size for queue space accounting.
    ///
    /// **Note**: This value is approximate, based on a JSON encoding estimate;
    /// actual CBOR encoding may be smaller. Used for admission control in
    /// [`ServerConfig::max_send_queue_bytes`](crate::config::ServerConfig).
    pub fn size_hint(&self) -> usize {
        self.event.len()
            + self.message_id.len()
            + self.schema.len()
            + serde_json::to_vec(&self.payload)
                .map(|v| v.len())
                .unwrap_or(0)
    }
}

/// Serializes an Event to JSON bytes for use in a frame's `payload` field.
pub fn encode_event_body(e: &Event) -> Result<Bytes> {
    Ok(Bytes::from(serde_json::to_vec(e)?))
}

/// Deserializes an Event from JSON bytes.
///
/// An empty byte sequence returns a `MessageReject::Rejected` error.
pub fn decode_event_body(bytes: &[u8]) -> Result<Event> {
    if bytes.is_empty() {
        return Err(RiftError::Message(MessageReject::Rejected(
            "empty event payload".into(),
        )));
    }
    Ok(serde_json::from_slice(bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let e = Event::new(
            "chat.message.created",
            "01HZZZZZZZZZZZZZZZZZZZZZZ",
            "chat.message.created@1.0",
            serde_json::json!({"text": "hi"}),
        );
        let bytes = encode_event_body(&e).unwrap();
        let back = decode_event_body(&bytes).unwrap();
        assert_eq!(back.event, e.event);
        assert_eq!(back.message_id, e.message_id);
    }

    #[test]
    fn decode_empty_fails() {
        let r = decode_event_body(&[]);
        assert!(r.is_err());
    }
}
