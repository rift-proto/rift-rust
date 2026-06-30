//! # CBOR Codec
//!
//! Implements the [`PayloadCodec`] trait for the
//! [CBOR (Concise Binary Object Representation)](https://cbor.io/) wire format.
//!
//! The protocol specification (section 7) designates CBOR as the default
//! binary codec. CBOR produces smaller payloads than JSON and is faster
//! to parse, making it the preferred encoding for production deployments.
//!
//! ## Implementation Details
//!
//! * Encoding uses [`ciborium::into_writer`] to serialize a
//!   [`serde_json::Value`] into a CBOR byte buffer.
//! * Decoding uses [`ciborium::from_reader`] to parse CBOR bytes back
//!   into a [`serde_json::Value`].
//! * The codec is stateless and zero-cost -- [`CborCodec`] is a unit struct
//!   that can be freely copied.

use bytes::{BufMut, Bytes, BytesMut};

use crate::codec::codec::PayloadCodec;
use crate::error::Result;
use crate::frame::EncodingFormat as FrameEncodingFormat;

/// The default CBOR binary codec.
///
/// CBOR is a binary data format whose design goals include the
/// possibility of extremely small code size, fairly small message
/// size, and extensibility without the need for version negotiation.
///
/// This struct implements the [`PayloadCodec`] trait and carries the
/// [`FrameEncodingFormat::Cbor`] tag used during Hello-phase codec negotiation.
///
/// # Examples
///
/// ```rust,no_run
/// use rifts_core::codec::{CborCodec, PayloadCodec, PayloadCodecExt};
///
/// let codec = CborCodec;
/// let bytes = codec.encode(&42u32).unwrap();
/// let value: u32 = codec.decode(&bytes).unwrap();
/// assert_eq!(value, 42);
/// ```
#[derive(Debug, Default, Clone, Copy)]
pub struct CborCodec;

impl PayloadCodec for CborCodec {
    /// Returns [`FrameEncodingFormat::Cbor`], identifying this codec during
    /// Hello-phase negotiation.
    fn frame_codec(&self) -> FrameEncodingFormat {
        FrameEncodingFormat::Cbor
    }

    /// Encode a [`serde_json::Value`] into CBOR binary bytes.
    ///
    /// Internally delegates to [`ciborium::into_writer`] which writes
    /// RFC 7049-compliant CBOR into a growable byte buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if `ciborium` encounters an I/O or serialization
    /// failure (unlikely for in-memory buffers but possible for certain
    /// value types).
    fn encode_value(&self, value: &serde_json::Value) -> Result<Bytes> {
        let mut buf = Vec::new();
        ciborium::into_writer(value, &mut buf)?;
        Ok(Bytes::from(buf))
    }

    /// Zero-copy encode: writes directly into the [`BytesMut`] buffer via
    /// [`ciborium::into_writer`], avoiding the intermediate `Vec` allocation
    /// that [`encode_value`](PayloadCodec::encode_value) requires.
    fn encode_value_to(&self, value: &serde_json::Value, buf: &mut BytesMut) -> Result<()> {
        let mut writer = buf.writer();
        ciborium::into_writer(value, &mut writer)?;
        Ok(())
    }

    /// Decode CBOR binary bytes into a [`serde_json::Value`].
    ///
    /// Internally delegates to [`ciborium::from_reader`] which parses
    /// RFC 7049-compliant CBOR from the given byte slice.
    ///
    /// # Errors
    ///
    /// Returns an error if the input is not valid CBOR or if `ciborium`
    /// encounters a deserialization failure.
    fn decode_value(&self, bytes: &[u8]) -> Result<serde_json::Value> {
        Ok(ciborium::from_reader(bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::codec::codec::PayloadCodecExt;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Sample {
        name: String,
        count: u32,
    }

    #[test]
    fn round_trip() {
        let c = CborCodec;
        let s = Sample {
            name: "rift".to_string(),
            count: 42,
        };
        let bytes = c.encode(&s).unwrap();
        let back: Sample = c.decode(&bytes).unwrap();
        assert_eq!(back, s);
    }

    #[test]
    fn frame_codec_tag() {
        assert_eq!(CborCodec.frame_codec(), FrameEncodingFormat::Cbor);
    }

    #[test]
    fn encode_value_to_matches_encode_value() {
        let codec = CborCodec;
        let value = serde_json::json!({"key": "value", "nested": {"arr": [1, 2, 3]}});

        let encoded = codec.encode_value(&value).unwrap();

        let mut buf = BytesMut::new();
        codec.encode_value_to(&value, &mut buf).unwrap();

        assert_eq!(buf, encoded);
    }

    #[test]
    fn decode_value_owned_matches_decode_value() {
        let codec = CborCodec;
        let value = serde_json::json!({"key": "value", "nested": {"arr": [1, 2, 3]}});
        // Encode via ciborium into Bytes
        let mut raw = Vec::new();
        ciborium::into_writer(&value, &mut raw).unwrap();
        let bytes = Bytes::from(raw);

        let from_slice = codec.decode_value(&bytes).unwrap();
        let from_owned = codec.decode_value_owned(bytes.clone()).unwrap();

        assert_eq!(from_slice, from_owned);
    }
}
