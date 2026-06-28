//! Axum WebSocket adapter.
//!
//! This module wraps an [`axum::extract::ws::WebSocket`] as a Rift
//! [`TransportConnection`], allowing the Rift server to be embedded into
//! an Axum application.
//!
//! # Usage
//!
//! Call [`into_connection`] from within the `ws.on_upgrade()` callback:
//!
//! ```ignore
//! use axum::extract::ws::{WebSocket, WebSocketUpgrade};
//!
//! async fn handler(ws: WebSocketUpgrade) -> impl axum::response::IntoResponse {
//!     ws.on_upgrade(|socket| async move {
//!         let conn = rift::transport::axum::into_connection(socket, None);
//!         rift_server.accept_and_spawn(conn);
//!     })
//! }
//! ```
//!
//! The resulting [`Box<dyn TransportConnection>`] is `Send` and can be
//! passed directly to [`RiftServer::accept_and_spawn`](crate::server::RiftServer::accept_and_spawn).

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::ws::{Message, WebSocket};

use crate::error::{Result, RiftError};
use crate::frame::Frame;
use crate::protocol::close::CloseCode;
use crate::transport::TransportConnection;
use crate::transport::frame_codec::{
    DEFAULT_MAX_BINARY_PAYLOAD, decode_binary_frame, decode_text_frame, encode_frame,
};

/// An Axum WebSocket connection adapted for the Rift protocol.
///
/// Internally the axum `WebSocket` is wrapped in an `Arc<Mutex<_>>` so
/// that the read and write methods can share it without requiring
/// `&mut self` access to the underlying socket.
pub struct AxumWsConnection {
    /// The underlying axum WebSocket, shared behind an async mutex.
    ws: Arc<tokio::sync::Mutex<WebSocket>>,
    /// Peer socket address, if provided by the caller.
    peer: Option<SocketAddr>,
}

/// Wrap an axum `WebSocket` into a boxed [`TransportConnection`].
///
/// The `peer` parameter is the caller's opportunity to pass the peer's
/// socket address (typically extracted from the request), or `None` if
/// unknown.
///
/// # Example
///
/// ```ignore
/// use axum::extract::ws::{WebSocket, WebSocketUpgrade};
///
/// async fn handler(ws: WebSocketUpgrade) -> impl axum::response::IntoResponse {
///     ws.on_upgrade(|socket| async move {
///         let conn = rift::transport::axum::into_connection(socket, None);
///         rift_server.accept_and_spawn(conn);
///     })
/// }
/// ```
pub fn into_connection(ws: WebSocket, peer: Option<SocketAddr>) -> Box<dyn TransportConnection> {
    Box::new(AxumWsConnection {
        ws: Arc::new(tokio::sync::Mutex::new(ws)),
        peer,
    })
}

#[async_trait]
impl TransportConnection for AxumWsConnection {
    /// Read the next data frame from the axum WebSocket.
    ///
    /// Ping and pong frames are consumed silently. Close frames produce a
    /// [`SessionReject::Expired`](crate::error::SessionReject::Expired) error.
    /// Text frames are decoded via [`decode_text_frame`] and binary frames
    /// via [`decode_binary_frame`].
    async fn read_frame(&mut self) -> Result<Frame> {
        loop {
            let msg = self
                .ws
                .lock()
                .await
                .recv()
                .await
                .ok_or_else(|| {
                    RiftError::other(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "axum websocket closed",
                    ))
                })?
                .map_err(RiftError::other)?;
            match msg {
                Message::Text(text) => return decode_text_frame(text.as_bytes()),
                Message::Binary(bin) => {
                    return decode_binary_frame(&bin, DEFAULT_MAX_BINARY_PAYLOAD);
                }
                Message::Ping(_) | Message::Pong(_) => continue,
                Message::Close(_) => {
                    return Err(RiftError::Session(crate::error::SessionReject::Expired));
                }
            }
        }
    }

    /// Encode a frame to binary wire format and send it as an axum
    /// binary WebSocket message.
    async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        let payload = encode_frame(frame)?;
        self.ws
            .lock()
            .await
            .send(Message::Binary(payload))
            .await
            .map_err(RiftError::other)?;
        Ok(())
    }

    /// Send a WebSocket close frame with the given code and reason.
    async fn close(&mut self, code: CloseCode, reason: &str) -> Result<()> {
        let frame = axum::extract::ws::CloseFrame {
            code: code.as_u16(),
            reason: axum::extract::ws::Utf8Bytes::from(reason),
        };
        let _ = self.ws.lock().await.send(Message::Close(Some(frame))).await;
        Ok(())
    }

    /// Return the peer socket address, if provided at construction time.
    fn peer_addr(&self) -> Option<SocketAddr> {
        self.peer
    }
}
