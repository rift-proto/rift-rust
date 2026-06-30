//! Channel bridge for non-`Send` WebSocket types (actix-web, ntex).
//!
//! Some web frameworks (notably actix-web and ntex) use `Rc`-based internals
//! that make their WebSocket types `!Send`. This module provides a
//! [`BridgeConnection`] that uses tokio mpsc channels to shuttle frame data
//! between the framework's own runtime (actix-rt / ntex-rt) and the tokio
//! runtime.
//!
//! # Architecture
//!
//! Two mpsc channels are created:
//!
//! 1. **Inbound channel** — the framework-side reader task pushes raw bytes
//!    (prefixed with a 1-byte type tag) into this channel.
//!    The `BridgeConnection` reads from it on the tokio side.
//!
//! 2. **Outbound channel** — the tokio side pushes encoded frames into this
//!    channel. The framework-side writer task reads from it and forwards
//!    the bytes to the actual WebSocket sink.
//!
//! The 1-byte tag prefix distinguishes frame types:
//!
//! - `b'B'` — binary frame (decoded via [`decode_binary_frame`]).
//! - `b'T'` — text/JSON frame (decoded via [`decode_text_frame`]).
//! - `b'C'` — close signal.

use std::net::SocketAddr;

use async_trait::async_trait;
use tokio::sync::mpsc;

use rifts_core::Frame;
use rifts_core::error::{FrameReject, Result, RiftError};
use rifts_core::frame_codec::{
    DEFAULT_MAX_BINARY_PAYLOAD, decode_binary_frame, decode_text_frame, encode_frame,
};
use rifts_core::protocol::close::CloseCode;

use crate::transport::TransportConnection;

/// A transport connection backed by tokio mpsc channels.
///
/// The other ends of the channels are driven by framework-specific
/// reader/writer tasks that stay on the framework's local runtime.
/// From the tokio side, `BridgeConnection` behaves like any other
/// [`TransportConnection`].
pub struct BridgeConnection {
    /// Receiver for frames coming from the framework WebSocket (read path).
    /// Wrapped in a tokio `Mutex` because `read_frame` takes `&mut self`
    /// but the receiver only needs exclusive access during the recv call.
    inbox: tokio::sync::Mutex<mpsc::Receiver<Vec<u8>>>,

    /// Sender for frames to be written to the framework WebSocket (write path).
    outbox: mpsc::Sender<Vec<u8>>,

    /// Peer socket address, if known.
    peer: Option<SocketAddr>,
}

/// Spawn framework-side bridge tasks and return a [`BridgeConnection`] for the
/// tokio side.
///
/// This function creates the two mpsc channels, invokes the provided closures
/// to spawn the framework-side reader and writer tasks (which **must** be
/// `Send`), and returns a boxed `BridgeConnection` ready for use.
///
/// # Parameters
///
/// - `peer` — the peer's socket address, if known.
/// - `capacity` — the bounded capacity of each mpsc channel.
/// - `spawn_reader` — a closure that receives the inbound sender and spawns
///   a task that reads from the WebSocket and pushes tagged bytes into it.
/// - `spawn_writer` — a closure that receives the outbound receiver and spawns
///   a task that reads tagged bytes from it and writes them to the WebSocket.
pub fn spawn_bridge(
    peer: Option<SocketAddr>,
    capacity: usize,
    spawn_reader: impl FnOnce(mpsc::Sender<Vec<u8>>) + Send + 'static,
    spawn_writer: impl FnOnce(mpsc::Receiver<Vec<u8>>) + Send + 'static,
) -> Box<dyn TransportConnection> {
    let (inbox_tx, inbox_rx) = mpsc::channel::<Vec<u8>>(capacity);
    let (outbox_tx, outbox_rx) = mpsc::channel::<Vec<u8>>(capacity);

    spawn_reader(inbox_tx);
    spawn_writer(outbox_rx);

    Box::new(BridgeConnection {
        inbox: tokio::sync::Mutex::new(inbox_rx),
        outbox: outbox_tx,
        peer,
    })
}

/// Like [`spawn_bridge`], but does **not** require the reader/writer closures
/// to be `Send`.
///
/// Use this variant for actix-web and ntex where the framework WebSocket types
/// contain `Rc` and are `!Send`. The closures are invoked on the current
/// (framework-local) thread, and the spawned tasks remain on the framework's
/// runtime. The returned `BridgeConnection` is still `Send` because it only
/// holds tokio channels.
pub fn spawn_bridge_local(
    peer: Option<SocketAddr>,
    capacity: usize,
    spawn_reader: impl FnOnce(mpsc::Sender<Vec<u8>>) + 'static,
    spawn_writer: impl FnOnce(mpsc::Receiver<Vec<u8>>) + 'static,
) -> Box<dyn TransportConnection> {
    let (inbox_tx, inbox_rx) = mpsc::channel::<Vec<u8>>(capacity);
    let (outbox_tx, outbox_rx) = mpsc::channel::<Vec<u8>>(capacity);

    spawn_reader(inbox_tx);
    spawn_writer(outbox_rx);

    Box::new(BridgeConnection {
        inbox: tokio::sync::Mutex::new(inbox_rx),
        outbox: outbox_tx,
        peer,
    })
}

#[async_trait]
impl TransportConnection for BridgeConnection {
    /// Read the next frame from the inbound channel.
    ///
    /// Raw bytes are expected to be prefixed with a 1-byte tag:
    /// `b'B'` for binary, `b'T'` for text/JSON, `b'C'` for close.
    async fn read_frame(&mut self) -> Result<Frame> {
        let raw = self
            .inbox
            .lock()
            .await
            .recv()
            .await
            .ok_or(RiftError::Session(rifts_core::error::SessionReject::Closed))?;
        // The raw bytes were stored with a 1-byte tag prefix:
        // b'B' → binary frame (decode_binary_frame)
        // b'T' → text frame   (decode_text_frame)
        // b'C' → close
        match raw.first() {
            Some(b'B') => decode_binary_frame(&raw[1..], DEFAULT_MAX_BINARY_PAYLOAD),
            Some(b'T') => decode_text_frame(&raw[1..]),
            Some(b'C') => Err(RiftError::Session(rifts_core::error::SessionReject::Closed)),
            _ => Err(RiftError::Frame(FrameReject::FrameInvalid(
                "invalid bridge frame tag".into(),
            ))),
        }
    }

    /// Encode a frame into the binary wire format and send it through the
    /// outbound channel, prefixed with the `b'B'` tag.
    async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        let payload = encode_frame(frame)?;
        // Prefix with 'B' for binary.
        let mut buf = Vec::with_capacity(1 + payload.len());
        buf.push(b'B');
        buf.extend_from_slice(&payload);
        self.outbox.send(buf).await.map_err(|_| {
            RiftError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "bridge write channel closed",
            ))
        })
    }

    /// Send a close signal through the outbound channel.
    ///
    /// The wire format is `b'C'` followed by the close code as a 2-byte
    /// big-endian `u16` and the UTF-8 reason bytes. The framework-side
    /// writer task interprets this as a request to close the underlying
    /// WebSocket connection with the given code and reason.
    async fn close(&mut self, code: CloseCode, reason: &str) -> Result<()> {
        let mut buf = Vec::with_capacity(3 + reason.len());
        buf.push(b'C');
        buf.extend_from_slice(&code.as_u16().to_be_bytes());
        buf.extend_from_slice(reason.as_bytes());
        let _ = self.outbox.send(buf).await;
        Ok(())
    }

    /// Return the peer socket address, if known at bridge creation time.
    fn peer_addr(&self) -> Option<SocketAddr> {
        self.peer
    }
}
