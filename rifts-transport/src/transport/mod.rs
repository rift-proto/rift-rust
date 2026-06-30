//! Transport abstraction layer.
//!
//! This module defines the trait hierarchy that decouples the Rift protocol
//! implementation from any particular network transport. The server works
//! exclusively with these traits, while framework-specific adapters (axum,
//! actix-web, warp, ntex) provide concrete implementations.
//!
//! # Trait hierarchy
//!
//! - [`Transport`] — a transport binding that can listen on a socket address.
//! - [`TransportListener`] — a bound listener that accepts incoming connections.
//! - [`TransportConnection`] — a single bidirectional connection that can read
//!   and write [`Frame`](rifts_core::Frame)s.
//!
//! # Built-in adapters
//!
//! | Feature flag  | Module       | Description                          |
//! |---------------|-------------|--------------------------------------|
//! | `websocket`   | [`websocket`] | Standalone WebSocket via tungstenite |
//! | `axum`        | [`axum`]      | Axum WebSocket adapter               |
//! | `actix-web`   | [`actix`]     | Actix-web WebSocket adapter          |
//! | `warp`        | [`warp`]      | Warp WebSocket adapter               |
//! | `ntex`        | [`ntex`]      | Ntex WebSocket adapter               |
//!
//! The [`bridge`] module (used by actix-web and ntex) provides a channel-based
//! bridge for frameworks whose WebSocket types are `!Send`.

// Channel bridge for non-Send WS types (actix-web, ntex).
#[cfg(any(feature = "actix-web", feature = "ntex"))]
pub mod bridge;

#[cfg(feature = "actix-web")]
pub mod actix;
#[cfg(feature = "axum")]
pub mod axum;
#[cfg(feature = "ntex")]
pub mod ntex;
#[cfg(feature = "warp")]
pub mod warp;
#[cfg(feature = "websocket")]
pub mod websocket;

use async_trait::async_trait;
use std::net::SocketAddr;

use rifts_core::protocol::close::CloseCode;
use rifts_core::{Frame, Result};

/// A transport binding — the entry point for listening on a network address.
///
/// Implementations are lightweight and cloneable. The server holds one
/// `Transport` and calls [`bind`](Transport::bind) to create a
/// [`TransportListener`] when it starts accepting connections.
///
/// Built-in implementations: [`WebSocketTransport`](websocket::WebSocketTransport).
/// Additional implementations can be added for WebTransport, TCP, Unix
/// sockets, or any other bidirectional byte-stream transport.
#[async_trait]
pub trait Transport: Send + Sync {
    /// Bind a listener on the given socket address.
    ///
    /// Returns a boxed [`TransportListener`] that can accept incoming
    /// connections. The address is typically `0.0.0.0:<port>` or
    /// `127.0.0.1:<port>`.
    async fn bind(&self, addr: SocketAddr) -> Result<Box<dyn TransportListener>>;

    /// Human-readable name of this transport (e.g. `"websocket"`,
    /// `"webtransport"`, `"tcp"`).
    fn name(&self) -> &'static str;
}

/// A bound transport listener that accepts incoming connections.
///
/// Created by [`Transport::bind`]. Each call to [`accept`](TransportListener::accept)
/// yields the next incoming [`TransportConnection`], or an error if the
/// listener has failed.
#[async_trait]
pub trait TransportListener: Send {
    /// Accept the next incoming connection.
    ///
    /// This method blocks (asynchronously) until a connection is
    /// available or an error occurs.
    async fn accept(&mut self) -> Result<Box<dyn TransportConnection>>;

    /// The local address the listener is bound to.
    ///
    /// Useful for logging and for discovering the ephemeral port when
    /// the server is bound to port 0.
    fn local_addr(&self) -> Result<SocketAddr>;
}

/// A single bidirectional transport connection.
///
/// Implementations read and write [`Frame`]s using
/// the binary or text wire format defined in
/// [`frame_codec`](rifts_core::frame_codec). The connection is
/// consumed by `Connection::run`.
#[async_trait]
pub trait TransportConnection: Send {
    /// Read the next frame from the remote peer.
    ///
    /// Returns an error if the connection has been closed or the frame
    /// is malformed. Implementations must handle WebSocket control
    /// frames (ping/pong/close) transparently and only surface data
    /// frames to the caller.
    async fn read_frame(&mut self) -> Result<Frame>;

    /// Write a frame to the remote peer.
    ///
    /// The frame is encoded using the transport's wire format (binary
    /// or text envelope) before being sent. Returns an error if the
    /// write fails.
    async fn write_frame(&mut self, frame: &Frame) -> Result<()>;

    /// Close the connection with a structured close code and human-readable
    /// reason string.
    ///
    /// The close code is mapped to the transport's native close mechanism
    /// (e.g. a WebSocket close frame). After this call the connection
    /// should not be used for further reads or writes.
    async fn close(&mut self, code: CloseCode, reason: &str) -> Result<()>;

    /// The peer's socket address, if known.
    ///
    /// Returns `None` when the transport does not expose peer addressing
    /// (e.g. some proxied configurations).
    fn peer_addr(&self) -> Option<SocketAddr>;
}
