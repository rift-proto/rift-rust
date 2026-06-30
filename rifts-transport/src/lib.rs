//! # rifts-transport — Transport-layer abstraction and framework adapters
//!
//! This crate defines the Transport/TransportListener/TransportConnection trait
//! hierarchy and provides concrete implementations for several web frameworks.
//!
//! ## Feature flags
//!
//! | Feature flag  | Module       | Description                          |
//! |---------------|-------------|--------------------------------------|
//! | `websocket`   | [`transport::websocket`] | Standalone WebSocket via tungstenite |
//! | `axum`        | [`transport::axum`]      | Axum WebSocket adapter               |
//! | `actix-web`   | [`transport::actix`]     | Actix-web WebSocket adapter          |
//! | `warp`        | [`transport::warp`]      | Warp WebSocket adapter               |
//! | `ntex`        | [`transport::ntex`]      | Ntex WebSocket adapter               |
//!
//! The [`transport::bridge`] module (used internally by actix-web and ntex
//! adapters) provides a channel-based bridge for frameworks whose WebSocket
//! types are `!Send`.

#![forbid(unsafe_code)]
#![deny(unreachable_pub)]
#![deny(missing_docs)]
#![warn(rust_2018_idioms)]

pub mod transport;
