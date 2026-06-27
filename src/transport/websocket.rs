//! WebSocket transport — spec §3.1 (default transport).
//!
//! The `WebSocketConnection` is split into independent read/write
//! halves so that the server can read and write concurrently without
//! contending on a single mutex.

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::protocol::{
    CloseFrame as WsCloseFrame, Message as WsMessage, WebSocketConfig,
};
use tokio_tungstenite::{WebSocketStream, accept_async, accept_async_with_config};

use crate::error::{BoxedStdError, Result, RiftError};
use crate::frame::Frame;
use crate::protocol::close::CloseCode;
use crate::transport::frame_codec::{decode_binary_frame, decode_text_frame, encode_frame};
use crate::transport::{Transport, TransportConnection, TransportListener};

/// WebSocket transport — uses `tokio-tungstenite` under the hood.
#[derive(Debug, Clone)]
pub struct WebSocketTransport {
    /// Maximum WebSocket message size in bytes. Messages exceeding
    /// this will be rejected at the transport level before frame
    /// decoding.
    max_message_size: Option<usize>,
}

impl Default for WebSocketTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl WebSocketTransport {
    /// Create a new WebSocket transport with no message size limit
    /// (use `with_max_message_size` for production).
    pub fn new() -> Self {
        Self {
            max_message_size: None,
        }
    }

    /// Set the maximum WebSocket message size in bytes.
    /// Recommended: set this to `ServerConfig::max_payload_bytes`.
    pub fn with_max_message_size(mut self, limit: usize) -> Self {
        self.max_message_size = Some(limit);
        self
    }

    /// Access the max message size, if set.
    pub fn max_message_size(&self) -> Option<usize> {
        self.max_message_size
    }
}

#[async_trait]
impl Transport for WebSocketTransport {
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

struct WebSocketListener {
    inner: Arc<TcpListener>,
    max_message_size: Option<usize>,
}

#[async_trait]
impl TransportListener for WebSocketListener {
    async fn accept(&mut self) -> Result<Box<dyn TransportConnection>> {
        let (stream, _addr) = self.inner.accept().await?;
        let ws = accept_with_config(stream, self.max_message_size).await?;
        Ok(Box::new(WebSocketConnection::new(ws)))
    }

    fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.inner.local_addr()?)
    }
}

/// Accept a WebSocket connection with an optional `max_message_size`
/// configuration. When set, the underlying tungstenite connection
/// will reject frames larger than the limit before they reach the
/// application layer.
async fn accept_with_config(
    stream: TcpStream,
    max_message_size: Option<usize>,
) -> std::result::Result<WebSocketStream<TcpStream>, tokio_tungstenite::tungstenite::Error> {
    if let Some(limit) = max_message_size {
        let config = WebSocketConfig {
            max_message_size: Some(limit),
            ..WebSocketConfig::default()
        };
        accept_async_with_config(stream, Some(config)).await
    } else {
        accept_async(stream).await
    }
}

/// A WebSocket-backed transport. Internally split into read/write
/// halves.
pub struct WebSocketConnection {
    reader: futures_util::stream::SplitStream<WebSocketStream<TcpStream>>,
    writer: Arc<
        tokio::sync::Mutex<futures_util::stream::SplitSink<WebSocketStream<TcpStream>, WsMessage>>,
    >,
    peer: Option<SocketAddr>,
}

impl WebSocketConnection {
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
    async fn read_frame(&mut self) -> Result<Frame> {
        loop {
            let msg = self
                .reader
                .next()
                .await
                .ok_or_else(|| {
                    RiftError::other(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "websocket closed",
                    ))
                })?
                .map_err(|e| RiftError::WebSocket(BoxedStdError(Box::new(e))))?;
            match msg {
                WsMessage::Text(text) => {
                    return decode_text_frame(text.as_bytes());
                }
                WsMessage::Binary(bin) => {
                    return decode_binary_frame(&bin);
                }
                WsMessage::Ping(_) | WsMessage::Pong(_) => continue,
                WsMessage::Close(_close) => {
                    return Err(RiftError::Session(crate::error::SessionReject::Expired));
                }
                _ => continue,
            }
        }
    }

    async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        let payload = encode_frame(frame)?;
        let mut w = self.writer.lock().await;
        w.send(WsMessage::Binary(payload.to_vec()))
            .await
            .map_err(|e| RiftError::WebSocket(BoxedStdError(Box::new(e))))?;
        w.flush()
            .await
            .map_err(|e| RiftError::WebSocket(BoxedStdError(Box::new(e))))?;
        Ok(())
    }

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

    fn peer_addr(&self) -> Option<SocketAddr> {
        self.peer
    }
}
