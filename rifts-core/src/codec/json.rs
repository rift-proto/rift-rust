//! # JSON Codec
//!
//! Implements the [`PayloadCodec`] trait for the JSON
//! wire format.
//!
//! The protocol specification (section 7) lists JSON as the
//! debug and development codec. JSON payloads are human-readable,
//! making them ideal for troubleshooting, interactive testing, and
//! environments where binary tooling is unavailable. For production
//! use, prefer the CBOR codec ([`CborCodec`](crate::codec::CborCodec)).
//!
//! ## Implementation Details
//!
//! * Encoding uses [`serde_json::to_vec`] to serialize a
//!   [`serde_json::Value`] into a UTF-8 JSON byte vector.
//! * Decoding uses [`serde_json::from_slice`] to parse JSON bytes back
//!   into a [`serde_json::Value`].
//! * The codec is stateless and zero-cost -- [`JsonCodec`] is a unit struct
//!   that can be freely copied.

use bytes::{BufMut, Bytes, BytesMut};

use crate::codec::codec::PayloadCodec;
use crate::error::Result;
use crate::frame::EncodingFormat as FrameEncodingFormat;

/// JSON text codec for debugging and development.
///
/// This struct implements the [`PayloadCodec`] trait and carries the
/// [`FrameEncodingFormat::Json`] tag used during Hello-phase codec negotiation.
/// JSON encoding produces larger payloads than CBOR but is trivially
/// inspectable with standard tools (`curl`, `jq`, browser devtools, etc.).
///
/// # Examples
///
/// ```rust,no_run
/// use rifts_core::codec::{JsonCodec, PayloadCodec, PayloadCodecExt};
///
/// let codec = JsonCodec;
/// let bytes = codec.encode(&"hello").unwrap();
/// let value: String = codec.decode(&bytes).unwrap();
/// assert_eq!(value, "hello");
/// ```
#[derive(Debug, Default, Clone, Copy)]
pub struct JsonCodec;

impl PayloadCodec for JsonCodec {
    /// Returns [`FrameEncodingFormat::Json`], identifying this codec during
    /// Hello-phase negotiation.
    fn frame_codec(&self) -> FrameEncodingFormat {
        FrameEncodingFormat::Json
    }

    /// Encode a [`serde_json::Value`] into JSON text bytes.
    ///
    /// The output is compact (no pretty-printing or indentation)
    /// to minimize wire overhead.
    ///
    /// # Errors
    ///
    /// Returns an error if `serde_json::to_vec` encounters a
    /// serialization failure (extremely rare for valid `Value` inputs).
    fn encode_value(&self, value: &serde_json::Value) -> Result<Bytes> {
        Ok(Bytes::from(serde_json::to_vec(value)?))
    }

    /// Zero-copy encode: writes directly into the [`BytesMut`] buffer via
    /// [`serde_json::to_writer`], avoiding the intermediate `Vec` allocation
    /// that [`encode_value`](PayloadCodec::encode_value) requires.
    fn encode_value_to(&self, value: &serde_json::Value, buf: &mut BytesMut) -> Result<()> {
        let mut writer = buf.writer();
        serde_json::to_writer(&mut writer, value)?;
        Ok(())
    }

    /// Decode JSON text bytes into a [`serde_json::Value`].
    ///
    /// Rejects input larger than `max_bytes` to mitigate DoS attacks
    /// via deeply-nested or excessively large JSON documents.
    ///
    /// # Errors
    ///
    /// Returns an error if the input is not valid JSON or if
    /// `serde_json::from_slice` encounters a deserialization failure.
    fn decode_value(&self, bytes: &[u8]) -> Result<serde_json::Value> {
        const MAX_JSON_BYTES: usize = 1_048_576; // 1 MiB
        if bytes.len() > MAX_JSON_BYTES {
            return Err(crate::error::FrameReject::PayloadTooLarge {
                actual: bytes.len(),
                max: MAX_JSON_BYTES,
            }
            .into());
        }
        Ok(serde_json::from_slice(bytes)?)
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
        let c = JsonCodec;
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
        assert_eq!(JsonCodec.frame_codec(), FrameEncodingFormat::Json);
    }

    #[test]
    fn encode_value_to_matches_encode_value() {
        let codec = JsonCodec;
        let value = serde_json::json!({"key": "value", "nested": {"arr": [1, 2, 3]}});

        let encoded = codec.encode_value(&value).unwrap();

        let mut buf = BytesMut::new();
        codec.encode_value_to(&value, &mut buf).unwrap();

        assert_eq!(buf, encoded);
    }

    #[test]
    fn decode_value_owned_matches_decode_value() {
        let codec = JsonCodec;
        let value = serde_json::json!({"key": "value", "nested": {"arr": [1, 2, 3]}});
        let bytes = Bytes::from(serde_json::to_vec(&value).unwrap());

        let from_slice = codec.decode_value(&bytes).unwrap();
        let from_owned = codec.decode_value_owned(bytes.clone()).unwrap();

        assert_eq!(from_slice, from_owned);
    }
}
