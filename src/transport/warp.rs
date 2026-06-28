//! Warp WebSocket adapter.
//!
//! This module wraps a [`warp::ws::WebSocket`] as a Rift
//! [`TransportConnection`], allowing the Rift server to be embedded into
//! a Warp application.
//!
//! Unlike actix-web and ntex, warp's WebSocket type is `Send`, so no
//! channel bridge is needed — the adapter directly holds the split
//! read/write halves behind `Arc<Mutex<_>>` wrappers.
//!
//! # Usage
//!
//! ```ignore
//! warp::ws::ws()
//!     .map(|ws: warp::ws::Ws| {
//!         ws.on_upgrade(|socket| async move {
//!             let conn = rift::transport::warp::into_connection(socket, None);
//!             rift_server.accept_and_spawn(conn);
//!         })
//!     });
//! ```

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};

use crate::error::{Result, RiftError};
use crate::frame::Frame;
use crate::protocol::close::CloseCode;
use crate::transport::TransportConnection;
use crate::transport::frame_codec::{
    DEFAULT_MAX_BINARY_PAYLOAD, decode_binary_frame, decode_text_frame, encode_frame,
};

/// A Warp WebSocket connection adapted for the Rift protocol.
///
/// The underlying WebSocket is split into a reader half (`rx`) and a
/// writer half (`tx`), each wrapped in an `Arc<tokio::sync::Mutex<_>>`
/// so they can be accessed from async tasks without requiring `&mut self`.
pub struct WarpWsConnection {
    /// Reader half of the split warp WebSocket.
    rx: Arc<tokio::sync::Mutex<futures_util::stream::SplitStream<warp::ws::WebSocket>>>,

    /// Writer half of the split warp WebSocket.
    tx: Arc<
        tokio::sync::Mutex<futures_util::stream::SplitSink<warp::ws::WebSocket, warp::ws::Message>>,
    >,

    /// Peer socket address, if provided by the caller.
    peer: Option<SocketAddr>,
}

/// Wrap a warp `WebSocket` into a boxed [`TransportConnection`].
///
/// The WebSocket is split into independent read and write halves. The
/// `peer` parameter is the caller's opportunity to pass the peer's socket
/// address, or `None` if unknown.
///
/// # Example
///
/// ```ignore
/// warp::ws::ws()
///     .map(|ws: warp::ws::Ws| {
///         ws.on_upgrade(|socket| async move {
///             let conn = rift::transport::warp::into_connection(socket, None);
///             rift_server.accept_and_spawn(conn);
///         })
///     });
/// ```
pub fn into_connection(
    ws: warp::ws::WebSocket,
    peer: Option<SocketAddr>,
) -> Box<dyn TransportConnection> {
    let (tx, rx) = ws.split();
    Box::new(WarpWsConnection {
        rx: Arc::new(tokio::sync::Mutex::new(rx)),
        tx: Arc::new(tokio::sync::Mutex::new(tx)),
        peer,
    })
}

#[async_trait]
impl TransportConnection for WarpWsConnection {
    /// Read the next data frame from the warp WebSocket.
    ///
    /// Ping and pong frames are consumed silently. Close frames produce a
    /// [`SessionReject::Expired`](crate::error::SessionReject::Expired) error.
    /// Text frames are decoded via [`decode_text_frame`]; binary frames
    /// via [`decode_binary_frame`].
    async fn read_frame(&mut self) -> Result<Frame> {
        loop {
            let msg = self
                .rx
                .lock()
                .await
                .next()
                .await
                .ok_or_else(|| {
                    RiftError::other(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "warp websocket closed",
                    ))
                })?
                .map_err(|e| RiftError::other(std::io::Error::other(format!("{e:?}"))))?;
            if msg.is_text() {
                let text = msg
                    .to_str()
                    .map_err(|e| RiftError::other(std::io::Error::other(format!("{e:?}"))))?;
                return decode_text_frame(text.as_bytes());
            }
            if msg.is_binary() {
                return decode_binary_frame(&msg.into_bytes(), DEFAULT_MAX_BINARY_PAYLOAD);
            }
            if msg.is_ping() || msg.is_pong() {
                continue;
            }
            if msg.is_close() {
                return Err(RiftError::Session(crate::error::SessionReject::Expired));
            }
        }
    }

    /// Encode a frame to binary wire format and send it as a warp
    /// binary WebSocket message.
    async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        let payload = encode_frame(frame)?;
        self.tx
            .lock()
            .await
            .send(warp::ws::Message::binary(payload.to_vec()))
            .await
            .map_err(RiftError::other)?;
        Ok(())
    }

    /// Send a warp close message.
    ///
    /// Note: warp's `Message::close()` does not carry a code or reason;
    /// the parameters are accepted for trait conformance but are not
    /// forwarded to the peer.
    async fn close(&mut self, _code: CloseCode, _reason: &str) -> Result<()> {
        let msg = warp::ws::Message::close();
        let _ = self.tx.lock().await.send(msg).await;
        Ok(())
    }

    /// Return the peer socket address, if provided at construction time.
    fn peer_addr(&self) -> Option<SocketAddr> {
        self.peer
    }
}
