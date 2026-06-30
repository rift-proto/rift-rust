//! # Protocol Frame (Frame) Module
//!
//! This module defines the wire-level message container for the Rift/1 protocol — [`Frame`].
//!
//! As specified in section 6 of the spec, **all information** exchanged between client and
//! server — control frames, data frames, acknowledgment frames, flow-control frames, and
//! error frames — is encapsulated within a `Frame`.
//!
//! ## Submodules
//!
//! - [`envelope`]: The [`Frame`] struct definition, including all fields and convenience constructors.
//! - [`types`]: Frame-level foundational types: [`FrameType`], [`EncodingFormat`], [`Priority`], [`FrameFlags`].

pub mod envelope;
pub mod types;

pub use envelope::Frame;
pub use types::{EncodingFormat, FrameFlags, FrameType, Priority};
