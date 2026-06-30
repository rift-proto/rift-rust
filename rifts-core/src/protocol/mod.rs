//! # Protocol Layer Type Definitions (Protocol)
//!
//! This module aggregates all **protocol-level** types and constants defined
//! by the Rift/1 specification. Each sub-module covers a distinct phase of
//! the connection lifecycle:
//!
//! | Sub-module      | Responsibility                                  | Spec section |
//! |-----------------|-------------------------------------------------|--------------|
//! | [`version`]     | Protocol version number and negotiation rules   | §25          |
//! | [`close`]       | Structured close codes sent in Close frames     | §20          |
//! | [`error_code`]  | Structured error codes returned in Error frames | §19.1        |
//! | [`heartbeat`]   | Heartbeat policy (ping/pong interval & timeout) | §21          |
//! | [`hello`]       | Hello / Welcome / Ready handshake flow          | §5.2 – §5.5 |
//!
//! ## Design Principles
//!
//! * **Data-only** -- all types are pure data structures with no I/O logic,
//!   keeping the protocol layer free of transport concerns.
//! * **Zero-copy-friendly** -- every enum implements `Copy` and `Eq`, enabling
//!   cheap comparisons on the hot frame-decode path without heap allocation.
//! * **Compile-time constants** -- version and wire-protocol constants use
//!   `const` rather than `static` so the compiler can inline them at every
//!   call site.
//!
//! ## Wire Format Summary
//!
//! All protocol types in this module are **pure in-memory representations**.
//! Serialization to and from the binary wire format is handled by the
//! [`frame`](crate::frame) module.  The types here are intentionally
//! transport-agnostic so they can be shared between client and server
//! implementations without pulling in I/O dependencies.
//!
//! ## Versioning
//!
//! The protocol uses a two-part version number (`major.minor`) encoded as a
//! single `u16` (`major << 8 | minor`).  Major version bumps indicate
//! breaking wire-format changes; minor bumps indicate backwards-compatible
//! additions.  See [`version`] for the negotiation logic.

pub mod close;
pub mod error_code;
pub mod heartbeat;
pub mod hello;
pub mod version;
