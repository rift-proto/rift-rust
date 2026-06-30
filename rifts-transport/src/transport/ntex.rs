//! Ntex WebSocket adapter.
//!
//! This module wraps an ntex WebSocket pair ([`ntex::ws::WsSink`] + message
//! stream) as a Rift [`TransportConnection`] via a channel bridge.
//!
//! # Why a bridge?
//!
//! Like actix-web, ntex uses `Rc`-based internals that make its WebSocket
//! types `!Send`. The bridge spawns reader and writer tasks on the ntex
//! runtime that shuttle raw bytes through tokio mpsc channels. The returned
//! `BridgeConnection` is `Send` and can be passed to
//! `RiftServer::accept_and_spawn`.
//!
//! # Usage
//!
//! ```ignore
//! use ntex::web;
//! use ntex::ws;
//!
//! async fn handler(req: web::HttpRequest, stream: web::Payload)
//!     -> Result<web::HttpResponse, web::Error>
//! {
//!     ws::start(req, stream, |msg_stream, sink| async move {
//!         let conn = rifts_transport::transport::ntex::into_connection(sink, msg_stream, None);
//!         tokio::spawn(async move {
//!             rift_server.accept_and_spawn(conn);
//!         });
//!     })
//! }
//! ```

use std::net::SocketAddr;

use crate::transport::TransportConnection;
use crate::transport::bridge::spawn_bridge_local;

/// Wrap an ntex WebSocket pair into a boxed [`TransportConnection`].
///
/// This function spawns two tasks on the ntex runtime:
///
/// 1. A **reader task** that pulls messages from the stream, prefixes each
///    with a 1-byte tag (`b'B'` for binary, `b'T'` for text, `b'C'` for
///    close), and pushes the tagged bytes into the inbound mpsc channel.
///
/// 2. A **writer task** that reads tagged bytes from the outbound mpsc
///    channel and forwards them to the `WsSink` (stripping the tag prefix
///    before sending).
///
/// The returned `BridgeConnection` is `Send` and can be moved to the
/// tokio runtime where `Connection::run` operates.
///
/// # Type parameters
///
/// - `S` — the message stream type, typically obtained from
///   `ntex::web::ws::start()`.
/// - `E` — the stream's error type.
///
/// # Parameters
///
/// - `sink` — the ntex `WsSink` for sending outbound messages.
/// - `stream` — the ntex message stream for receiving inbound messages.
/// - `peer` — the peer socket address, or `None` if unknown.
pub fn into_connection<S, E>(
    sink: ntex::ws::WsSink,
    mut stream: S,
    peer: Option<SocketAddr>,
) -> Box<dyn TransportConnection>
where
    S: futures_util::Stream<Item = Result<ntex::ws::Message, E>> + Unpin + 'static,
    E: std::fmt::Debug,
{
    spawn_bridge_local(
        peer,
        256,
        // Reader: pull from ntex message stream → tokio channel.
        move |tx| {
            ntex::rt::spawn(async move {
                use futures_util::StreamExt;
                while let Some(msg) = stream.next().await {
                    let raw = match msg {
                        Ok(ntex::ws::Message::Binary(bin)) => {
                            let mut v = Vec::with_capacity(1 + bin.len());
                            v.push(b'B');
                            v.extend_from_slice(&bin);
                            v
                        }
                        Ok(ntex::ws::Message::Text(text)) => {
                            let mut v = Vec::with_capacity(1 + text.len());
                            v.push(b'T');
                            v.extend_from_slice(text.as_bytes());
                            v
                        }
                        Ok(ntex::ws::Message::Close(_)) => vec![b'C'],
                        Ok(_) => continue,
                        Err(e) => {
                            tracing::warn!(error = ?e, "ntex reader stream error");
                            break;
                        }
                    };
                    if tx.send(raw).await.is_err() {
                        break;
                    }
                }
            });
        },
        // Writer: receive from tokio channel → ntex WsSink.
        move |mut rx| {
            ntex::rt::spawn(async move {
                while let Some(raw) = rx.recv().await {
                    if raw.first() == Some(&b'C') {
                        let _ = sink.send(ntex::ws::Message::Close(None)).await;
                        break;
                    }
                    if raw.len() > 1 {
                        let _ = sink
                            .send(ntex::ws::Message::Binary(raw[1..].to_vec().into()))
                            .await;
                    }
                }
            });
        },
    )
}
