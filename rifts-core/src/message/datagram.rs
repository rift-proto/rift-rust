//! # Datagram -- High-Frequency Datagrams (spec section 8)
//!
//! Datagrams are message types designed for high-frequency, loss-tolerant scenarios.
//! Typical use cases include:
//! - Mouse / cursor position updates
//! - Typing indicators
//! - Game position synchronization
//! - Real-time sensor readings
//!
//! Characteristics: no delivery guarantee, no retries, no acknowledgment, preferred drop under backpressure.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::Result;

/// High-frequency, discardable datagram message.
///
/// Unlike [`Event`](super::event::Event), datagrams:
/// - Are not persisted and are not written to the append log
/// - Do not produce acknowledgments (Acks)
/// - Are preferentially dropped under backpressure
/// - Use `BestEffort` as the default delivery mode
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Datagram {
    /// Schema identifier, formatted as `{domain}.{name}@{major}.{minor}`.
    pub schema: String,

    /// Optional event name used for routing at the receiver.
    ///
    /// Examples: `"move"`, `"typing"`, `"position"`.
    pub event: Option<String>,

    /// Datagram payload in JSON format.
    pub payload: serde_json::Value,
}

/// Serializes a Datagram to JSON bytes.
pub fn encode_datagram(d: &Datagram) -> Result<Bytes> {
    Ok(Bytes::from(serde_json::to_vec(d)?))
}

/// Deserializes a Datagram from JSON bytes.
pub fn decode_datagram(bytes: &[u8]) -> Result<Datagram> {
    Ok(serde_json::from_slice(bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let d = Datagram {
            schema: "game.position@1.0".into(),
            event: Some("move".into()),
            payload: serde_json::json!({"x": 10, "y": 20}),
        };
        let bytes = encode_datagram(&d).unwrap();
        let back = decode_datagram(&bytes).unwrap();
        assert_eq!(back.payload["x"], 10);
    }
}
