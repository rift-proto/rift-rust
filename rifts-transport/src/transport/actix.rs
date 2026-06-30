//! Actix-web WebSocket adapter.
//!
//! This module wraps an actix-web WebSocket pair ([`actix_ws::Session`] +
//! [`actix_ws::MessageStream`]) as a Rift [`TransportConnection`] via a
//! channel bridge.
//!
//! # Why a bridge?
//!
//! Actix-web uses `Rc`-based internals, making its WebSocket types `!Send`.
//! The bridge spawns reader and writer tasks on the actix runtime that
//! shuttle raw bytes through tokio mpsc channels. The returned
//! `BridgeConnection` is `Send` and can be passed to
//! `RiftServer::accept_and_spawn`.
//!
//! # Usage
//!
//! ```ignore
//! use actix_web::{web, HttpRequest, HttpResponse, Error};
//!
//! async fn handler(req: HttpRequest, stream: web::Payload) -> Result<HttpResponse, Error> {
//!     let (res, session, msg_stream) = actix_ws::handle(&req, stream)?;
//!     let peer = req.peer_addr().ok();
//!     let conn = rifts_transport::transport::actix::into_connection(session, msg_stream, peer);
//!     tokio::spawn(async move {
//!         rift_server.accept_and_spawn(conn);
//!     });
//!     Ok(res)
//! }
//! ```

use std::net::SocketAddr;

use crate::transport::TransportConnection;
use crate::transport::bridge::spawn_bridge_local;

/// Wrap an actix-web WebSocket session and message stream into a boxed
/// [`TransportConnection`].
///
/// This function spawns two tasks on the actix runtime:
///
/// 1. A **reader task** that pulls messages from the `MessageStream`,
///    prefixes each with a 1-byte tag (`b'B'` for binary, `b'T'` for
///    text, `b'C'` for close), and pushes the tagged bytes into the
///    inbound mpsc channel.
///
/// 2. A **writer task** that reads tagged bytes from the outbound mpsc
///    channel and forwards them to the `Session` (stripping the tag
///    prefix before sending).
///
/// The returned `BridgeConnection` is `Send` and can be moved to the
/// tokio runtime where `Connection::run` operates.
///
/// # Parameters
///
/// - `session` — the actix-web WebSocket session for sending messages.
/// - `stream` — the actix-web WebSocket message stream for receiving messages.
/// - `peer` — the peer socket address, or `None` if unknown.
pub fn into_connection(
    session: actix_ws::Session,
    mut stream: actix_ws::MessageStream,
    peer: Option<SocketAddr>,
) -> Box<dyn TransportConnection> {
    spawn_bridge_local(
        peer,
        256,
        // Reader task: pull messages from actix stream → send to tokio channel.
        move |tx| {
            actix_web::rt::spawn(async move {
                use futures_util::StreamExt;
                while let Some(msg) = stream.next().await {
                    let raw = match msg {
                        Ok(actix_ws::Message::Binary(bin)) => {
                            let mut v = Vec::with_capacity(1 + bin.len());
                            v.push(b'B');
                            v.extend_from_slice(&bin);
                            v
                        }
                        Ok(actix_ws::Message::Text(text)) => {
                            let mut v = Vec::with_capacity(1 + text.len());
                            v.push(b'T');
                            v.extend_from_slice(text.as_bytes());
                            v
                        }
                        Ok(actix_ws::Message::Close(_)) => vec![b'C'],
                        Ok(_) => continue, // skip ping/pong/nop/continuation
                        Err(e) => {
                            tracing::warn!(error = ?e, "actix reader stream error");
                            break;
                        }
                    };
                    if tx.send(raw).await.is_err() {
                        break;
                    }
                }
            });
        },
        // Writer task: receive from tokio channel → write to actix session.
        move |mut rx| {
            let mut session = session;
            actix_web::rt::spawn(async move {
                while let Some(raw) = rx.recv().await {
                    if raw.first() == Some(&b'C') {
                        drop(session.close(None));
                        break;
                    }
                    // Skip the tag byte; write the payload.
                    if raw.len() > 1 {
                        drop(session.binary(raw[1..].to_vec()));
                    }
                }
            });
        },
    )
}
