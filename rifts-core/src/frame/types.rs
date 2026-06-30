//! # Frame-Level Foundational Types
//!
//! This module defines the fundamental enumerations and flag types used by protocol frames:
//!
//! - [`FrameType`]: Frame category (Control, Data, Ack, Flow, Error)
//! - [`EncodingFormat`]: Payload encoding format (JSON, CBOR)
//! - [`Priority`]: Message priority (Background, Volatile, Low, Normal, High, Critical)
//! - [`FrameFlags`]: Bit-flag set (compressed, encrypted, fragmented, requires-ack, etc.)

use std::fmt;

/// Frame category (spec section 6.2).
///
/// Each `Frame` has exactly one `FrameType`, which determines the frame's high-level
/// semantics. Transmitted on the wire as a single-byte tag (`C`/`D`/`A`/`F`/`E`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum FrameType {
    /// Control frame: carries protocol-level interactions.
    ///
    /// Includes: Hello/Welcome/Ready, Ping/Pong, Subscribe/Unsubscribe, Resume, etc.
    /// The specific operation is distinguished by the `Frame::event` field.
    Control,

    /// Data frame: carries business messages.
    ///
    /// Includes: Event, State, Command, Reply, Datagram, etc.
    Data,

    /// Acknowledgment frame: confirms receipt of a previously received frame.
    ///
    /// Used to implement reliable delivery semantics. When a client receives a
    /// message marked with the `REQUIRES_ACK` flag, it must reply with an Ack frame.
    Ack,

    /// Flow control frame: backpressure, window adjustment, degradation notifications.
    ///
    /// The server uses Flow frames to inform the client of the current load status.
    /// The client adjusts its send rate or pauses accordingly.
    Flow,

    /// Error frame: protocol error, authorization error, business error, or system error.
    ///
    /// The `payload` carries structured error details (error code + description).
    Error,
}

impl FrameType {
    /// Returns the single-byte tag used for compact encoding.
    ///
    /// | Frame Type | Tag |
    /// |------------|-----|
    /// | Control | `C` |
    /// | Data | `D` |
    /// | Ack | `A` |
    /// | Flow | `F` |
    /// | Error | `E` |
    pub fn tag(self) -> u8 {
        match self {
            FrameType::Control => b'C',
            FrameType::Data => b'D',
            FrameType::Ack => b'A',
            FrameType::Flow => b'F',
            FrameType::Error => b'E',
        }
    }

    /// Restores a `FrameType` from a single-byte tag.
    ///
    /// Returns `None` if the tag is invalid.
    pub fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            b'C' => Some(FrameType::Control),
            b'D' => Some(FrameType::Data),
            b'A' => Some(FrameType::Ack),
            b'F' => Some(FrameType::Flow),
            b'E' => Some(FrameType::Error),
            _ => None,
        }
    }
}

impl fmt::Display for FrameType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            FrameType::Control => "control",
            FrameType::Data => "data",
            FrameType::Ack => "ack",
            FrameType::Flow => "flow",
            FrameType::Error => "error",
        })
    }
}

/// Payload encoding format (spec section 7).
///
/// Negotiated between client and server during the Hello handshake phase and
/// remains consistent for the entire lifetime of the connection.
/// Transmitted on the wire as a single-byte tag (`J`/`B`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EncodingFormat {
    /// JSON text encoding.
    ///
    /// Human-readable and convenient for debugging and development.
    /// In production environments, CBOR is recommended for smaller size and
    /// faster parsing.
    Json,

    /// CBOR binary encoding (default).
    ///
    /// The default format recommended by spec section 7, balancing compactness
    /// and parsing efficiency. Defined in [RFC 7049](https://datatracker.ietf.org/doc/html/rfc7049).
    Cbor,
}

impl EncodingFormat {
    /// Returns the single-byte tag used for compact encoding.
    ///
    /// | Encoding Format | Tag |
    /// |-----------------|-----|
    /// | Json | `J` |
    /// | Cbor | `B` |
    pub fn tag(self) -> u8 {
        match self {
            EncodingFormat::Json => b'J',
            EncodingFormat::Cbor => b'B',
        }
    }

    /// Restores an `EncodingFormat` from a single-byte tag.
    ///
    /// Returns `None` if the tag is invalid.
    pub fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            b'J' => Some(EncodingFormat::Json),
            b'B' => Some(EncodingFormat::Cbor),
            _ => None,
        }
    }

    /// Returns the lowercase name of the encoding format (`"json"` or `"cbor"`).
    pub fn name(self) -> &'static str {
        match self {
            EncodingFormat::Json => "json",
            EncodingFormat::Cbor => "cbor",
        }
    }
}

impl fmt::Display for EncodingFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Message priority (spec section 18.3).
///
/// Determines the send order (higher priority first) and the drop order under
/// backpressure (lower priority dropped first).
///
/// # Priority Levels (Lowest to Highest)
///
/// | Value | Name | Use Case |
/// |-------|------|----------|
/// | 0 | Background | Background tasks (e.g., metrics reporting) |
/// | 1 | Volatile | Volatile messages (first to be dropped under backpressure) |
/// | 2 | Low | Low-priority business messages |
/// | 3 | Normal | Normal business messages (default) |
/// | 4 | High | High-priority business messages |
/// | 5 | Critical | Critical messages (e.g., system alerts, never dropped) |
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Default,
    serde::Serialize,
    serde::Deserialize,
)]
#[repr(u8)]
pub enum Priority {
    /// Background task priority (lowest).
    Background = 0,
    /// Volatile message — first to be dropped under backpressure.
    Volatile = 1,
    /// Low priority.
    Low = 2,
    /// Normal priority (default).
    #[default]
    Normal = 3,
    /// High priority.
    High = 4,
    /// Critical priority (highest, never dropped due to backpressure).
    Critical = 5,
}

impl Priority {
    /// Restores a `Priority` from a `u8` value.
    ///
    /// Returns `None` if the value is outside the 0..=5 range.
    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0 => Priority::Background,
            1 => Priority::Volatile,
            2 => Priority::Low,
            3 => Priority::Normal,
            4 => Priority::High,
            5 => Priority::Critical,
            _ => return None,
        })
    }

    /// Converts to a `u8` value.
    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

impl fmt::Display for Priority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Priority::Critical => "critical",
            Priority::High => "high",
            Priority::Normal => "normal",
            Priority::Low => "low",
            Priority::Volatile => "volatile",
            Priority::Background => "background",
        })
    }
}

/// Frame flag bit-set (spec section 6.3).
///
/// Internally stored as a `u16` bitmap, where each bit corresponds to an
/// independent flag. Supports bitwise combination (`with`/`without`), detection
/// (`contains`), and set/clear operations (`set`/`clear`).
///
/// # Flag Reference
///
/// | Bit | Name | Meaning |
/// |-----|------|---------|
/// | 0 | `COMPRESSED` | Payload is compressed |
/// | 1 | `ENCRYPTED` | Payload is encrypted |
/// | 2 | `FRAGMENTED` | Frame is fragmented (not the final fragment) |
/// | 3 | `FINAL_FRAGMENT` | Last fragment of a fragmented frame |
/// | 4 | `REQUIRES_ACK` | Receiver must acknowledge |
/// | 5 | `REPLAYED` | Replay frame (re-sent after disconnect-resume) |
/// | 6 | `SNAPSHOT` | Snapshot frame (current topic state) |
/// | 7 | `DEGRADED` | Degraded-mode frame |
/// | 8 | `DUPLICATE` | Duplicate frame (server detected a duplicate) |
/// | 9 | `TRACE` | Frame carries distributed tracing context |
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Default, serde::Serialize, serde::Deserialize,
)]
pub struct FrameFlags(u16);

impl FrameFlags {
    /// Payload is compressed (e.g., gzip, zstd).
    pub const COMPRESSED: u16 = 1 << 0;
    /// Payload is encrypted (end-to-end encryption scenarios).
    pub const ENCRYPTED: u16 = 1 << 1;
    /// Frame is fragmented; the current fragment is not the final one.
    pub const FRAGMENTED: u16 = 1 << 2;
    /// Last fragment of a fragmented frame.
    pub const FINAL_FRAGMENT: u16 = 1 << 3;
    /// Receiver must send an Ack frame upon receipt.
    pub const REQUIRES_ACK: u16 = 1 << 4;
    /// This frame is a replay frame (re-sent by the server after disconnect-resume).
    pub const REPLAYED: u16 = 1 << 5;
    /// This frame is a snapshot frame (carries the current state of the topic).
    pub const SNAPSHOT: u16 = 1 << 6;
    /// Degraded-mode frame (sent when some features are unavailable).
    pub const DEGRADED: u16 = 1 << 7;
    /// Duplicate frame (server detected that this frame has already been sent).
    pub const DUPLICATE: u16 = 1 << 8;
    /// Frame carries distributed tracing context.
    pub const TRACE: u16 = 1 << 9;

    /// Creates a new empty flag set (all bits zero).
    pub const fn empty() -> Self {
        Self(0)
    }

    /// Constructs a flag set from a raw `u16` bitmap.
    ///
    /// Allows direct reconstruction from a wire-decoded bitmap without validation.
    pub const fn from_bits(bits: u16) -> Self {
        Self(bits)
    }

    /// Returns the raw `u16` bitmap value.
    pub const fn bits(self) -> u16 {
        self.0
    }

    /// Checks whether the specified flag bit is set.
    pub fn contains(self, flag: u16) -> bool {
        self.0 & flag == flag
    }

    /// Sets the specified flag bit.
    pub fn set(&mut self, flag: u16) {
        self.0 |= flag;
    }

    /// Clears the specified flag bit.
    pub fn clear(&mut self, flag: u16) {
        self.0 &= !flag;
    }

    /// Returns a new copy with the specified flag bit set (for chaining).
    pub fn with(mut self, flag: u16) -> Self {
        self.set(flag);
        self
    }

    /// Returns a new copy with the specified flag bit cleared (for chaining).
    pub fn without(mut self, flag: u16) -> Self {
        self.clear(flag);
        self
    }
}

impl fmt::Display for FrameFlags {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        let mut emit = |name: &str, flag: u16| -> fmt::Result {
            if self.contains(flag) {
                if !first {
                    f.write_str("|")?;
                }
                first = false;
                f.write_str(name)?;
            }
            Ok(())
        };
        emit("compressed", FrameFlags::COMPRESSED)?;
        emit("encrypted", FrameFlags::ENCRYPTED)?;
        emit("fragmented", FrameFlags::FRAGMENTED)?;
        emit("final_fragment", FrameFlags::FINAL_FRAGMENT)?;
        emit("requires_ack", FrameFlags::REQUIRES_ACK)?;
        emit("replayed", FrameFlags::REPLAYED)?;
        emit("snapshot", FrameFlags::SNAPSHOT)?;
        emit("degraded", FrameFlags::DEGRADED)?;
        emit("duplicate", FrameFlags::DUPLICATE)?;
        emit("trace", FrameFlags::TRACE)?;
        if first {
            f.write_str("none")?;
        }
        Ok(())
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
}
