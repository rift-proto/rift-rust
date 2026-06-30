//! Standalone WebSocket transport (Rift spec section 3.1, default transport).
//!
//! This module provides the built-in WebSocket transport used when the server
//! runs in standalone mode (without a web framework). It is gated behind the
//! `websocket` Cargo feature flag.
//!
//! The [`WebSocketConnection`] is split into independent read and write halves
//! (via `futures_util::StreamExt::split`) so that the server's reader and
//! writer tasks can operate concurrently without contending on a single mutex.
//!
//! # Message size limits
//!
//! An optional `max_message_size` can be configured via
//! [`WebSocketTransport::with_max_message_size`]. When set, the underlying
//! tungstenite connection rejects frames larger than the limit at the transport
//! level, before they reach the frame decoder. This is recommended for
//! production deployments to guard against memory exhaustion.

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::protocol::{
    CloseFrame as WsCloseFrame, Message as WsMessage, WebSocketConfig,
};
use tokio_tungstenite::{WebSocketStream, accept_async, accept_async_with_config};

use rifts_core::Frame;
use rifts_core::error::{Result, RiftError, SessionReject};
use rifts_core::frame_codec::{
    DEFAULT_MAX_BINARY_PAYLOAD, decode_binary_frame, decode_text_frame, encode_frame,
};
use rifts_core::protocol::close::CloseCode;

use crate::transport::{Transport, TransportConnection, TransportListener};

/// The standalone WebSocket transport, backed by `tokio-tungstenite`.
///
/// Create an instance with [`WebSocketTransport::new`] and optionally
/// configure a maximum message size with
/// [`with_max_message_size`](Self::with_max_message_size). Pass the
/// transport to `RiftServerBuilder::websocket_transport`
/// or use it directly via the [`Transport`] trait.
#[derive(Debug, Clone)]
pub struct WebSocketTransport {
    /// Maximum WebSocket message size in bytes. Messages exceeding
    /// this will be rejected at the transport level before frame
    /// decoding. `None` means no limit is enforced.
    max_message_size: Option<usize>,
}

impl Default for WebSocketTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl WebSocketTransport {
    /// Create a new WebSocket transport with no message size limit.
    ///
    /// For production use, call
    /// [`with_max_message_size`](Self::with_max_message_size) to set a
    /// limit that matches `ServerConfig::max_payload_bytes`.
    pub fn new() -> Self {
        Self {
            max_message_size: None,
        }
    }

    /// Set the maximum WebSocket message size in bytes.
    ///
    /// The limit is enforced by the underlying tungstenite connection
    /// before frames reach the application layer. Recommended value:
    /// `ServerConfig::max_payload_bytes`.
    pub fn with_max_message_size(mut self, limit: usize) -> Self {
        self.max_message_size = Some(limit);
        self
    }

    /// Access the configured max message size, if any.
    pub fn max_message_size(&self) -> Option<usize> {
        self.max_message_size
    }
}

#[async_trait]
impl Transport for WebSocketTransport {
    /// Bind a TCP listener on `addr` and return a [`WebSocketListener`].
    ///
    /// Each accepted TCP connection is upgraded to a WebSocket connection
    /// with the configured `max_message_size`.
    async fn bind(&self, addr: SocketAddr) -> Result<Box<dyn TransportListener>> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Box::new(WebSocketListener {
            inner: Arc::new(listener),
            max_message_size: self.max_message_size,
        }))
    }

    fn name(&self) -> &'static str {
        "websocket"
    }
}

/// A WebSocket listener that accepts incoming TCP connections and upgrades
/// them to WebSocket streams.
struct WebSocketListener {
    /// The underlying TCP listener, wrapped in an `Arc` so it can be
    /// shared across tasks if needed.
    inner: Arc<TcpListener>,
    /// Maximum message size to pass to the WebSocket config.
    max_message_size: Option<usize>,
}

#[async_trait]
impl TransportListener for WebSocketListener {
    /// Accept the next TCP connection and upgrade it to a WebSocket.
    async fn accept(&mut self) -> Result<Box<dyn TransportConnection>> {
        let (stream, _addr) = self.inner.accept().await?;
        let ws = accept_with_config(stream, self.max_message_size)
            .await
            .map_err(|e| RiftError::WebSocket(Box::new(e)))?;
        Ok(Box::new(WebSocketConnection::new(ws)))
    }

    /// Return the local address the TCP listener is bound to.
    fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.inner.local_addr()?)
    }
}

/// Accept a WebSocket connection with an optional `max_message_size`
/// configuration.
///
/// When a limit is set the underlying tungstenite connection will reject
/// frames larger than the limit before they reach the application layer.
/// When no limit is set the default tungstenite configuration is used.
async fn accept_with_config(
    stream: TcpStream,
    max_message_size: Option<usize>,
) -> std::result::Result<WebSocketStream<TcpStream>, tokio_tungstenite::tungstenite::Error> {
    if let Some(limit) = max_message_size {
        let config = WebSocketConfig::default().max_message_size(Some(limit));
        accept_async_with_config(stream, Some(config)).await
    } else {
        accept_async(stream).await
    }
}

/// A WebSocket-backed transport connection.
///
/// Internally the WebSocket stream is split into a reader half
/// ([`SplitStream`](futures_util::stream::SplitStream)) and a writer half
/// ([`SplitSink`](futures_util::stream::SplitSink)), each wrapped in its own
/// async-aware container. This allows the server's reader and writer tasks
/// to operate concurrently without mutex contention.
pub struct WebSocketConnection {
    /// Reader half of the split WebSocket stream.
    reader: futures_util::stream::SplitStream<WebSocketStream<TcpStream>>,

    /// Writer half of the split WebSocket stream, protected by a tokio
    /// `Mutex` for safe concurrent access from the writer task.
    writer: Arc<
        tokio::sync::Mutex<futures_util::stream::SplitSink<WebSocketStream<TcpStream>, WsMessage>>,
    >,

    /// Peer socket address, extracted from the TCP stream at accept time.
    peer: Option<SocketAddr>,
}

impl WebSocketConnection {
    /// Create a new connection from a completed WebSocket handshake.
    ///
    /// Extracts the peer address from the underlying TCP stream and splits
    /// the WebSocket into independent read and write halves.
    fn new(ws: WebSocketStream<TcpStream>) -> Self {
        let peer = ws.get_ref().peer_addr().ok();
        let (writer, reader) = ws.split();
        Self {
            reader,
            writer: Arc::new(tokio::sync::Mutex::new(writer)),
            peer,
        }
    }
}

#[async_trait]
impl TransportConnection for WebSocketConnection {
    /// Read the next data frame from the WebSocket.
    ///
    /// WebSocket control frames (ping, pong) are consumed silently and
    /// do not surface to the caller. Text frames are decoded via
    /// [`decode_text_frame`]; binary frames via [`decode_binary_frame`].
    /// A close frame produces a [`SessionReject::Closed`] error.
    async fn read_frame(&mut self) -> Result<Frame> {
        loop {
            let msg = self
                .reader
                .next()
                .await
                .ok_or(RiftError::Session(SessionReject::Closed))?
                .map_err(|e| RiftError::WebSocket(Box::new(e)))?;
            match msg {
                WsMessage::Text(text) => {
                    return decode_text_frame(text.as_bytes());
                }
                WsMessage::Binary(bin) => {
                    return decode_binary_frame(&bin, DEFAULT_MAX_BINARY_PAYLOAD);
                }
                WsMessage::Ping(_) | WsMessage::Pong(_) => continue,
                WsMessage::Close(_close) => {
                    return Err(RiftError::Session(SessionReject::Closed));
                }
                _ => continue,
            }
        }
    }

    /// Encode a frame to binary wire format and send it as a WebSocket
    /// binary message.
    ///
    /// The writer half is locked only for the duration of the send and
    /// flush operations.
    async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        let payload = encode_frame(frame)?;
        let mut w = self.writer.lock().await;
        w.send(WsMessage::Binary(payload))
            .await
            .map_err(|e| RiftError::WebSocket(Box::new(e)))?;
        w.flush()
            .await
            .map_err(|e| RiftError::WebSocket(Box::new(e)))?;
        Ok(())
    }

    /// Send a WebSocket close frame with the given code and reason, then
    /// close the underlying sink.
    async fn close(&mut self, code: CloseCode, reason: &str) -> Result<()> {
        let frame = WsCloseFrame {
            code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::from(
                code.as_u16(),
            ),
            reason: reason.to_string().into(),
        };
        let mut w = self.writer.lock().await;
        let _ = w.send(WsMessage::Close(Some(frame))).await;
        let _ = w.close().await;
        Ok(())
    }

    /// Return the peer socket address, if available.
    fn peer_addr(&self) -> Option<SocketAddr> {
        self.peer
    }
}
