//! # Codec Trait and Negotiation Helper
//!
//! This module defines the core [`PayloadCodec`] trait that all encoding backends
//! must implement, along with the [`PayloadCodecExt`] extension trait that adds
//! generic convenience methods. The standalone [`negotiate`] function
//! performs server-side codec selection from the client's preference list.
//!
//! ## Trait Design
//!
//! [`PayloadCodec`] intentionally uses non-generic `encode_value` / `decode_value`
//! signatures so the trait remains dyn-compatible (object-safe). Generic
//! wrappers that serialize and deserialize Rust types via `serde_json::Value`
//! are provided by [`PayloadCodecExt`], which is blanket-implemented for every
//! `PayloadCodec` implementor.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use serde::Serialize;

use crate::error::Result;
use crate::frame::EncodingFormat as FrameEncodingFormat;

/// Trait for a single named encoding format.
///
/// Codecs are expected to be stateless and cheaply cloneable. Each codec
/// is identified by a [`FrameEncodingFormat`] tag so that both peers can agree on
/// which encoding to use during the Hello handshake.
///
/// The trait is dyn-compatible (object-safe) because `encode_value` and
/// `decode_value` operate on `serde_json::Value` rather than generic types.
/// For generic convenience methods, see [`PayloadCodecExt`].
pub trait PayloadCodec: Send + Sync {
    /// Returns the [`FrameEncodingFormat`] enum variant that identifies this encoding
    /// format on the wire.
    ///
    /// This value is sent inside Hello frames during codec negotiation.
    fn frame_codec(&self) -> FrameEncodingFormat;

    /// Encode a [`serde_json::Value`] into wire-format bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the value cannot be serialized into this codec's
    /// wire format.
    fn encode_value(&self, value: &serde_json::Value) -> Result<Bytes>;

    /// Decode wire-format bytes back into a [`serde_json::Value`].
    ///
    /// # Errors
    ///
    /// Returns an error if the byte slice does not represent a valid
    /// payload in this codec's wire format.
    fn decode_value(&self, bytes: &[u8]) -> Result<serde_json::Value>;

    /// Decode an owned [`Bytes`] buffer back into a [`serde_json::Value`].
    ///
    /// The default implementation delegates to [`decode_value`](PayloadCodec::decode_value).
    /// Implementations may override this to avoid an extra copy when the caller
    /// already holds an owned buffer (e.g. via zero-copy parsing that borrows
    /// from the original `Bytes` allocation).
    ///
    /// # Errors
    ///
    /// Returns an error if the byte slice does not represent a valid
    /// payload in this codec's wire format.
    fn decode_value_owned(&self, data: Bytes) -> Result<serde_json::Value> {
        self.decode_value(&data)
    }

    /// Encode a [`serde_json::Value`] directly into a [`BytesMut`] buffer,
    /// avoiding an intermediate allocation.
    ///
    /// The default implementation calls [`encode_value`](PayloadCodec::encode_value)
    /// and extends `buf` with the result. Implementations should override this
    /// to write directly into `buf` for zero-copy encoding.
    ///
    /// # Errors
    ///
    /// Returns an error if the value cannot be serialized into this codec's
    /// wire format.
    fn encode_value_to(&self, value: &serde_json::Value, buf: &mut BytesMut) -> Result<()> {
        let bytes = self.encode_value(value)?;
        buf.extend_from_slice(&bytes);
        Ok(())
    }
}

/// Blanket implementation of [`PayloadCodecExt`] for every type that implements
/// [`PayloadCodec`], providing generic encode/decode helpers.
impl<T> PayloadCodecExt for T
where
    T: PayloadCodec + ?Sized,
{
    fn encode<T2: Serialize + ?Sized>(&self, value: &T2) -> Result<Bytes> {
        let v = serde_json::to_value(value)?;
        self.encode_value(&v)
    }

    fn decode<T2: serde::de::DeserializeOwned>(&self, bytes: &[u8]) -> Result<T2> {
        let v = self.decode_value(bytes)?;
        Ok(serde_json::from_value(v)?)
    }
}

/// Extension methods available on every [`PayloadCodec`] implementation.
///
/// These generic helpers serialize Rust types into [`serde_json::Value`]
/// (via `serde_json::to_value`) before delegating to the non-generic
/// [`PayloadCodec::encode_value`], and perform the reverse path for decoding.
///
/// This trait is blanket-implemented for all `PayloadCodec + ?Sized`, so there
/// is no need to implement it manually.
pub trait PayloadCodecExt: PayloadCodec {
    /// Serialize a Rust value and encode it into wire-format bytes.
    ///
    /// This is a convenience wrapper that first converts the value to a
    /// [`serde_json::Value`] and then calls [`PayloadCodec::encode_value`].
    ///
    /// # Errors
    ///
    /// Returns an error if `serde_json::to_value` fails or if the codec
    /// cannot encode the resulting JSON value.
    fn encode<T: Serialize + ?Sized>(&self, value: &T) -> Result<Bytes>;

    /// Decode wire-format bytes and deserialize into a Rust type.
    ///
    /// This is a convenience wrapper that first calls [`PayloadCodec::decode_value`]
    /// and then deserializes the resulting [`serde_json::Value`] into `T`.
    ///
    /// # Errors
    ///
    /// Returns an error if decoding fails or if the JSON value cannot be
    /// deserialized into the target type.
    fn decode<T: serde::de::DeserializeOwned>(&self, bytes: &[u8]) -> Result<T>;
}

/// Negotiate a codec given a list of server-registered codecs and the
/// client's ordered preferences.
///
/// The function iterates through the client's preference list in order
/// and returns the first codec whose [`FrameEncodingFormat`] tag matches one of
/// the server's registered codecs.
///
/// # Arguments
///
/// * `server` -- The codecs the server has registered, wrapped in [`Arc`].
/// * `client` -- The client's ordered list of preferred [`FrameEncodingFormat`] tags.
///
/// # Returns
///
/// The first mutually supported codec, or a
/// [`FrameReject::CodecUnsupported`](crate::error::FrameReject::CodecUnsupported)
/// error if no common codec is found.
///
/// # Errors
///
/// Returns [`RiftError::Frame`](crate::error::RiftError::Frame) when no
/// overlap exists between the server and client codec sets.
pub fn negotiate(
    server: &[Arc<dyn PayloadCodec>],
    client: &[FrameEncodingFormat],
) -> Result<Arc<dyn PayloadCodec>> {
    for want in client {
        if let Some(c) = server.iter().find(|c| c.frame_codec() == *want) {
            return Ok(c.clone());
        }
    }
    Err(crate::error::RiftError::Frame(
        crate::error::FrameReject::CodecUnsupported(format!("client offered {:?}", client)),
    ))
}
