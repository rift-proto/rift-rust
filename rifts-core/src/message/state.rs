//! # State and Presence -- State Messages (spec section 14)
//!
//! State messages differ from regular events: **only the latest value per `state_key` is valid**.
//! Old values are overwritten by new values; no history is retained.
//!
//! ## Typical Use Cases
//!
//! - Collaborative cursor positions: `state_key = "cursor"`
//! - Typing indicators: `state_key = "typing:user-42"`
//! - Document properties: `state_key = "title"`
//!
//! ## Presence
//!
//! Presence is a special form of State (spec section 14.3) used to represent a user's online status.
//! Typical status values: `"online"`, `"away"`, `"busy"`, `"offline"`.
//! Presence supports TTL; after timeout it automatically falls back to `"offline"`.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::Result;

/// State message -- only the latest value per `state_key` is retained.
///
/// Unlike events, state messages use the `LatestOnly` delivery mode:
/// under backpressure, old states are overwritten by newer ones, and not every
/// intermediate state is guaranteed to be delivered.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct State {
    /// State key -- identifies the unique dimension of the state.
    ///
    /// Within the same topic, only the latest value per `state_key` is retained.
    /// Examples: `"cursor"`, `"typing:user-42"`, `"document.title"`.
    pub state_key: String,

    /// Human-readable name for the state (optional).
    ///
    /// Used for UI display; does not affect state lookup logic.
    pub name: Option<String>,

    /// Current state value in JSON format.
    pub value: serde_json::Value,

    /// State time-to-live in milliseconds (optional).
    ///
    /// After the TTL expires, the state is automatically cleared.
    /// Typical use: typing indicators.
    pub ttl_ms: Option<u32>,

    /// Subject that owns the state (optional).
    ///
    /// Identifies the user, device, or connection that initiated the state update.
    /// Examples: `"user-42"`, `"device-abc"`.
    pub subject: Option<String>,

    /// Update timestamp (millisecond Unix timestamp).
    pub updated_at: i64,
}

/// Presence -- a user's online status (spec section 14.3).
///
/// A specialized form of State dedicated to representing a user's online / offline / busy status.
/// The server automatically reverts the status to `"offline"` after the TTL expires.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Presence {
    /// The subject this Presence belongs to (typically a user ID).
    pub subject: String,

    /// Status string.
    ///
    /// Standard values: `"online"`, `"away"`, `"busy"`, `"offline"`.
    /// Applications may define custom statuses.
    pub status: String,

    /// Session ID this Presence belongs to (optional).
    pub session_id: Option<String>,

    /// Connection ID this Presence belongs to (optional).
    pub connection_id: Option<String>,

    /// Presence time-to-live in milliseconds (optional).
    ///
    /// After the TTL expires, the status automatically falls back to `"offline"`.
    /// Typical value: 30000 (30 seconds).
    pub ttl_ms: Option<u32>,

    /// Free-form metadata (optional).
    ///
    /// Carries additional information such as a user's avatar URL, current activity, etc.
    pub metadata: Option<serde_json::Value>,

    /// Update timestamp (millisecond Unix timestamp).
    pub updated_at: i64,
}

/// Serializes a State to JSON bytes.
pub fn encode_state(s: &State) -> Result<Bytes> {
    Ok(Bytes::from(serde_json::to_vec(s)?))
}

/// Deserializes a State from JSON bytes.
pub fn decode_state(bytes: &[u8]) -> Result<State> {
    Ok(serde_json::from_slice(bytes)?)
}

/// Serializes a Presence to JSON bytes.
pub fn encode_presence(p: &Presence) -> Result<Bytes> {
    Ok(Bytes::from(serde_json::to_vec(p)?))
}

/// Deserializes a Presence from JSON bytes.
pub fn decode_presence(bytes: &[u8]) -> Result<Presence> {
    Ok(serde_json::from_slice(bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_round_trip() {
        let s = State {
            state_key: "cursor".into(),
            name: Some("cursor position".into()),
            value: serde_json::json!({"x": 1, "y": 2}),
            ttl_ms: None,
            subject: Some("user-1".into()),
            updated_at: 1000,
        };
        let bytes = encode_state(&s).unwrap();
        let back = decode_state(&bytes).unwrap();
        assert_eq!(back.state_key, "cursor");
    }

    #[test]
    fn presence_round_trip() {
        let p = Presence {
            subject: "user-1".into(),
            status: "online".into(),
            session_id: Some("s-1".into()),
            connection_id: Some("c-1".into()),
            ttl_ms: Some(30_000),
            metadata: None,
            updated_at: 1000,
        };
        let bytes = encode_presence(&p).unwrap();
        let back = decode_presence(&bytes).unwrap();
        assert_eq!(back.status, "online");
    }
}
