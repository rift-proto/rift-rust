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
//!             let conn = rifts_transport::transport::warp::into_connection(socket, None);
//!             rift_server.accept_and_spawn(conn);
//!         })
//!     });
//! ```

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};

use rifts_core::Frame;
use rifts_core::error::{FrameReject, Result, RiftError, SessionReject};
use rifts_core::frame_codec::{
    DEFAULT_MAX_BINARY_PAYLOAD, decode_binary_frame, decode_text_frame, encode_frame,
};
use rifts_core::protocol::close::CloseCode;

use crate::transport::TransportConnection;

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
///             let conn = rifts_transport::transport::warp::into_connection(socket, None);
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
    /// [`SessionReject::Closed`] error.
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
                .ok_or(RiftError::Session(SessionReject::Closed))?
                .map_err(|e| RiftError::WebSocket(Box::new(e)))?;
            if msg.is_text() {
                let text = msg.to_str().map_err(|_e| {
                    RiftError::Frame(FrameReject::FrameInvalid(
                        "warp text frame not valid UTF-8".into(),
                    ))
                })?;
                return decode_text_frame(text.as_bytes());
            }
            if msg.is_binary() {
                return decode_binary_frame(&msg.into_bytes(), DEFAULT_MAX_BINARY_PAYLOAD);
            }
            if msg.is_ping() || msg.is_pong() {
                continue;
            }
            if msg.is_close() {
                return Err(RiftError::Session(SessionReject::Closed));
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
            .map_err(|e| RiftError::WebSocket(Box::new(e)))?;
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
