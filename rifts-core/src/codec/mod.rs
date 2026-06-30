//! # Codec Layer
//!
//! This module provides pluggable payload encoding and decoding capabilities,
//! converting application-layer data between `serde_json::Value` and wire-format
//! bytes. The protocol specification (section 7) defines the supported encoding
//! formats.
//!
//! ## Architecture Overview
//!
//! ```text
//! Codec (trait)
//! ├── CborCodec   -- CBOR binary encoding (default, compact and fast to parse)
//! └── JsonCodec   -- JSON text encoding (human-readable, widest compatibility)
//! ```
//!
//! ## Codec Negotiation
//!
//! During the Hello handshake phase, the client submits an ordered list of
//! preferred codecs. The server then calls [`negotiate`] to match the first
//! codec that both sides support. If no common codec is found, negotiation
//! fails with
//! [`FrameReject::CodecUnsupported`](crate::error::FrameReject::CodecUnsupported).
//!
//! ## Usage Example
//!
//! ```rust,no_run
//! use rifts_core::codec::{PayloadCodec, CborCodec, JsonCodec, negotiate};
//! use std::sync::Arc;
//!
//! // Register the codecs the server supports, in preference order.
//! let server_codecs: Vec<Arc<dyn PayloadCodec>> = vec![
//!     Arc::new(CborCodec),
//!     Arc::new(JsonCodec),
//! ];
//! ```
//!
//! ## Extending with Custom Codecs
//!
//! To add a new codec, implement the [`PayloadCodec`] trait for a zero-sized type
//! (or a struct carrying configuration) and register it alongside the
//! built-in codecs. The trait is dyn-compatible, so codecs can be stored
//! as `Arc<dyn PayloadCodec>` in collections.

pub mod cbor;
#[allow(clippy::module_inception)]
pub mod codec;
pub mod json;

pub use cbor::CborCodec;
pub use codec::{PayloadCodec, PayloadCodecExt, negotiate};
pub use json::JsonCodec;
