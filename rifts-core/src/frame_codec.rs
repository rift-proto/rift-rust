//! Wire-format frame encoder and decoder shared by all transports.
//!
//! This module handles the lowest-level serialization: converting between
//! [`Frame`] values and the byte sequences that travel
//! over the network. Every transport adapter (WebSocket, bridge, etc.)
//! delegates to the functions in this module.
//!
//! # Binary wire format
//!
//! The binary layout is fixed-size header followed by a variable-length
//! payload:
//!
//! ```text
//! 1 byte   — frame_type tag  (C=control, D=data, A=ack, F=flow, E=error)
//! 1 byte   — codec tag       (J=JSON, B=CBOR)
//! 2 bytes  — flags, big-endian u16 bitmask
//! 8 bytes  — frame_id, big-endian u64
//! 8 bytes  — timestamp, big-endian i64 (milliseconds since epoch)
//! 4 bytes  — payload length, big-endian u32
//! N bytes  — payload
//! ```
//!
//! Total fixed header overhead: 24 bytes. The payload length is always
//! included even when the payload is empty (length = 0).
//!
//! # Text / JSON envelope format
//!
//! When the transport uses a text WebSocket frame the frame is serialized
//! as a JSON object with the following fields:
//!
//! - `type` — `"control"`, `"data"`, `"ack"`, `"flow"`, or `"error"`.
//! - `codec` — `"json"` or `"cbor"`.
//! - `frame_id`, `flags`, `timestamp`, `payload` — as in the binary format.
//! - `session_id`, `stream_id`, `topic`, `event`, `message_id`,
//!   `correlation_id`, `trace_id` — optional string metadata fields.
//! - `ttl_ms` — optional TTL in milliseconds.

use bytes::{Bytes, BytesMut};

use crate::error::{FrameReject, Result, RiftError};
use crate::frame::{EncodingFormat as FrameEncodingFormat, Frame, FrameFlags, FrameType};

/// Encode a [`Frame`] into the binary wire format.
///
/// The resulting [`Bytes`] buffer is 24 + `payload.len()` bytes long
/// and can be sent directly over any byte-oriented transport (WebSocket
/// binary frame, TCP, etc.).
///
/// ## Optimization opportunity (Phase 3 — Link layer)
///
/// This function currently allocates a fresh [`BytesMut`] buffer on every call
/// and clones the payload [`Bytes`] via [`Option::cloned`]. For high-throughput
/// scenarios this can be improved:
///
///  1. Provide an `encode_frame_into(buf: &mut BytesMut, frame: &Frame)`
///     method that writes the header + payload directly into a caller-owned
///     buffer, allowing buffer reuse across frames.
///  2. Take `Frame` by value or accept `Bytes` directly so the payload can be
///     moved instead of cloned.
///  3. If the payload is `None`, avoid the `BytesMut::with_capacity` allocation
///     entirely by writing into a stack buffer.
///
/// These are deferred to Phase 3 because they require coordinated changes in
/// the transport layer (Link) to manage buffer lifecycle efficiently.
pub fn encode_frame(frame: &Frame) -> Result<Bytes> {
    let payload = frame.payload.as_ref().cloned().unwrap_or_default();
    let mut buf = BytesMut::with_capacity(24 + payload.len());
    buf.extend_from_slice(&[frame.frame_type.tag(), frame.codec.tag()]);
    buf.extend_from_slice(&frame.flags.bits().to_be_bytes());
    buf.extend_from_slice(&frame.frame_id.to_be_bytes());
    buf.extend_from_slice(&frame.timestamp.to_be_bytes());
    buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    buf.extend_from_slice(&payload);
    Ok(buf.freeze())
}

/// Default maximum binary-frame payload size (16 MiB). Frames
/// declaring a larger payload are rejected with
/// `FrameReject::PayloadTooLarge` before allocation to prevent a
/// malicious peer from forcing huge allocations.
pub const DEFAULT_MAX_BINARY_PAYLOAD: usize = 16 * 1024 * 1024;

/// Decode a binary frame from the wire format.
///
/// The input buffer must be at least 24 bytes long (the fixed header).
/// The payload length is read from the header and the buffer must
/// contain the full payload. Returns a structured [`FrameReject`] error
/// on malformed or truncated input.
///
/// `max_payload_len` caps the declared payload size; pass
/// `DEFAULT_MAX_BINARY_PAYLOAD` for the default.
pub fn decode_binary_frame(buf: &[u8], max_payload_len: usize) -> Result<Frame> {
    if buf.len() < 24 {
        return Err(RiftError::Frame(FrameReject::FrameInvalid(format!(
            "binary frame too short: {}",
            buf.len()
        ))));
    }
    let frame_type = FrameType::from_tag(buf[0]).ok_or_else(|| {
        RiftError::Frame(FrameReject::FrameInvalid("unknown frame type tag".into()))
    })?;
    let codec = FrameEncodingFormat::from_tag(buf[1])
        .ok_or_else(|| RiftError::Frame(FrameReject::FrameInvalid("unknown codec tag".into())))?;
    let flags = u16::from_be_bytes([buf[2], buf[3]]);
    let frame_id = u64::from_be_bytes([
        buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10], buf[11],
    ]);
    let timestamp = i64::from_be_bytes([
        buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18], buf[19],
    ]);
    let payload_len = u32::from_be_bytes([buf[20], buf[21], buf[22], buf[23]]) as usize;
    if payload_len > max_payload_len {
        return Err(RiftError::Frame(FrameReject::PayloadTooLarge {
            actual: payload_len,
            max: max_payload_len,
        }));
    }
    if buf.len() < 24 + payload_len {
        return Err(RiftError::Frame(FrameReject::FrameInvalid(format!(
            "payload truncated: want {}, have {}",
            payload_len,
            buf.len() - 24
        ))));
    }
    let payload = Bytes::copy_from_slice(&buf[24..24 + payload_len]);
    Ok(Frame {
        version: 0x0100,
        frame_id,
        frame_type,
        flags: FrameFlags::from_bits(flags),
        codec,
        session_id: None,
        stream_id: None,
        topic: None,
        event: None,
        message_id: None,
        correlation_id: None,
        trace_id: None,
        timestamp,
        ttl_ms: None,
        priority: None,
        payload: Some(payload),
    })
}

/// Decode a frame from a JSON text envelope.
///
/// This is used when the transport delivers text (non-binary) WebSocket
/// frames. The envelope is a JSON object whose fields correspond to the
/// [`Frame`] struct fields. Unknown or missing fields are ignored or set
/// to their default values.
pub fn decode_text_frame(buf: &[u8]) -> Result<Frame> {
    let value: serde_json::Value = serde_json::from_slice(buf)
        .map_err(|e| RiftError::Frame(FrameReject::FrameInvalid(format!("json envelope: {e}"))))?;
    let obj = value
        .as_object()
        .ok_or_else(|| RiftError::Frame(FrameReject::FrameInvalid("expected object".into())))?;
    let frame_type = match obj.get("type").and_then(|v| v.as_str()) {
        Some("control") => FrameType::Control,
        Some("data") => FrameType::Data,
        Some("ack") => FrameType::Ack,
        Some("flow") => FrameType::Flow,
        Some("error") => FrameType::Error,
        Some(other) => {
            return Err(RiftError::Frame(FrameReject::FrameInvalid(format!(
                "unknown type: {other}"
            ))));
        }
        None => FrameType::Data,
    };
    let codec = match obj.get("codec").and_then(|v| v.as_str()) {
        Some("json") => FrameEncodingFormat::Json,
        Some("cbor") => FrameEncodingFormat::Cbor,
        _ => FrameEncodingFormat::Json,
    };
    let frame_id = obj.get("frame_id").and_then(|v| v.as_u64()).unwrap_or(0);
    let timestamp = obj.get("timestamp").and_then(|v| v.as_i64()).unwrap_or(0);
    let flags = obj.get("flags").and_then(|v| v.as_u64()).unwrap_or(0) as u16;
    let payload = obj.get("payload").map(|v| {
        serde_json::to_vec(v).map(Bytes::from).unwrap_or_else(|e| {
            tracing::warn!(error = %e, "text frame payload serialization failed");
            Bytes::new()
        })
    });
    Ok(Frame {
        version: 0x0100,
        frame_id,
        frame_type,
        flags: FrameFlags::from_bits(flags),
        codec,
        session_id: obj
            .get("session_id")
            .and_then(|v| v.as_str())
            .map(String::from),
        stream_id: obj
            .get("stream_id")
            .and_then(|v| v.as_str())
            .map(String::from),
        topic: obj.get("topic").and_then(|v| v.as_str()).map(String::from),
        event: obj.get("event").and_then(|v| v.as_str()).map(String::from),
        message_id: obj
            .get("message_id")
            .and_then(|v| v.as_str())
            .map(String::from),
        correlation_id: obj
            .get("correlation_id")
            .and_then(|v| v.as_str())
            .map(String::from),
        trace_id: obj
            .get("trace_id")
            .and_then(|v| v.as_str())
            .map(String::from),
        timestamp,
        ttl_ms: obj.get("ttl_ms").and_then(|v| v.as_u64()).map(|v| v as u32),
        priority: None,
        payload,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_round_trip() {
        let f = Frame {
            version: 0x0100,
            frame_id: 42,
            frame_type: FrameType::Data,
            flags: FrameFlags::empty().with(FrameFlags::COMPRESSED),
            codec: FrameEncodingFormat::Cbor,
            session_id: Some("s".into()),
            stream_id: None,
            topic: Some("t".into()),
            event: Some("e".into()),
            message_id: Some("m".into()),
            correlation_id: None,
            trace_id: None,
            timestamp: 1000,
            ttl_ms: None,
            priority: None,
            payload: Some(Bytes::from_static(b"hi")),
        };
        let bytes = encode_frame(&f).unwrap();
        let back = decode_binary_frame(&bytes, DEFAULT_MAX_BINARY_PAYLOAD).unwrap();
        assert_eq!(back.frame_id, 42);
        assert_eq!(back.frame_type, FrameType::Data);
        assert_eq!(back.codec, FrameEncodingFormat::Cbor);
        assert!(back.flags.contains(FrameFlags::COMPRESSED));
        assert_eq!(back.payload.as_deref(), Some(&b"hi"[..]));
    }

    #[test]
    fn binary_too_short() {
        let r = decode_binary_frame(&[0u8; 5], DEFAULT_MAX_BINARY_PAYLOAD);
        assert!(r.is_err());
    }

    #[test]
    fn text_envelope() {
        let json = serde_json::json!({
            "type": "data",
            "codec": "json",
            "frame_id": 1,
            "timestamp": 0,
            "flags": 0,
            "payload": {"x": 1},
        });
        let bytes = serde_json::to_vec(&json).unwrap();
        let f = decode_text_frame(&bytes).unwrap();
        assert_eq!(f.frame_type, FrameType::Data);
        assert_eq!(f.codec, FrameEncodingFormat::Json);
    }
}
