//! # Rifts Session — Session Management Layer
//!
//! This crate implements the session management subsystem for the Rift
//! Realtime Protocol. It provides authentication, offset tracking, session
//! resumption, and session store capabilities.
//!
//! ## Modules
//!
//! * [`session::auth`] — Pluggable authentication via the `AuthProvider` trait.
//! * [`session::offset_tracker`] — Per-topic offset tracking for resume decisions.
//! * [`session::resume`] — Resume orchestration and epoch validation.
//! * [`session::session`] — Core `Session`, `SessionId`, `ClientId`, and lifecycle state.
//! * [`session::store`] — Thread-safe session store with offset tracking.

#![forbid(unsafe_code)]
#![deny(unreachable_pub)]
#![deny(missing_docs)]
#![warn(rust_2018_idioms)]

pub mod session;
