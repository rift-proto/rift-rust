//! Remote broker — spec section 22 (distributed broker, Phase 2a).
//!
//! This module provides a [`Broker`] implementation that connects to an
//! external broker node over a single TCP connection. The external node
//! must speak the wire protocol defined in [`crate::broker::wire`] —
//! a length-prefixed CBOR-framed protocol that serializes [`WireMsg`]
//! variants for request/response and push delivery.
//!
//! # Connection model
//!
//! The [`RemoteBroker`] opens a single TCP connection and splits it into
//! a framed writer half and a framed reader half. The writer half is
//! protected by a Tokio [`tokio::sync::Mutex`] for serialized access,
//! while a background reader task continuously parses incoming frames
//! and dispatches them:
//!
//! - **Push messages** (`WireMsg::Deliver`) are routed to a local
//!   sink map (sink ID to an mpsc channel), which feeds a background
//!   task per subscription that delivers to the actual [`FanoutSink`].
//! - **Response messages** are matched to pending requests via a
//!   first-in-first-out queue of oneshot sender channels.
//!
//! # FIFO response matching
//!
//! The current [`WireMsg`] design does not carry a `request_id` field
//! on response variants. As a result, the reader task dispatches
//! responses to the oldest pending request (the first entry in the
//! pending map). This is safe under the current design where only one
//! in-flight request is supported per `RemoteBroker`, but concurrent
//! requests will require protocol changes to add request correlation.
//!
//! # Timeout and error handling
//!
//! Each request has a 30-second timeout. If the broker node does not
//! respond within this window, the pending entry is removed and a
//! [`RiftError::System`] is returned. If the TCP connection breaks,
//! the reader task exits and pending oneshot senders are dropped,
//! causing waiting requests to receive a "connection closed" error.
//!
//! # Sink lifecycle
//!
//! On `subscribe`, the `RemoteBroker` allocates a local sink ID (via
//! [`crate::broker::fanout::new_sink_id`]), registers an mpsc channel
//! in the sinks map, and spawns a background task that reads from the
//! channel and delivers to the actual [`FanoutSink`]. When the sink
//! is dropped (via `drop_sink`), the channel is closed and the entry
//! is removed from the sinks map.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_util::codec::Framed;

use crate::broker::broker::{Broker, PublishOutcome};
use crate::broker::fanout::{ConnectionSink, SubscribeIntent, SubscriptionId};
use crate::broker::wire::{WireCodec, WireMsg};
use crate::error::{Result, RiftError, SystemReject};

/// A `Broker` that talks to an external broker node over framed TCP.
///
/// Opens a single TCP connection for both request/response operations
/// and push delivery from the broker. Push messages (`Deliver`) are
/// routed to a local fanout map of sink IDs to mpsc channels, which
/// feed dedicated Tokio tasks that deliver to the actual connection
/// sinks.
///
/// # Thread safety
///
/// The remote broker is [`Send`] and [`Sync`] and can be shared across
/// async tasks via `Arc<RemoteBroker>`. All shared state uses
/// concurrent data structures ([`DashMap`], [`AtomicU32`], Tokio
/// channels).
///
/// # Examples
///
/// ```ignore
/// use std::net::SocketAddr;
/// use rifts::broker::RemoteBroker;
///
/// let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
/// let broker = RemoteBroker::connect(addr).await.unwrap();
/// ```
pub struct RemoteBroker {
    /// Writer half of the framed TCP connection, protected by a Tokio
    /// mutex so that only one task writes at a time. The framed
    /// transport encodes each [`WireMsg`] as a 4-byte big-endian
    /// length prefix followed by CBOR data.
    framed: Arc<Mutex<futures_util::stream::SplitSink<Framed<TcpStream, WireCodec>, WireMsg>>>,
    /// Map of pending requests: `request_id` to a oneshot reply
    /// channel. The reader task removes and fires the matching sender
    /// when a response arrives.
    pending: Arc<DashMap<u32, oneshot::Sender<WireMsg>>>,
    /// Monotonic counter for allocating unique request IDs.
    next_id: AtomicU32,
    /// Local sink map: `sink_id` to an mpsc channel. Push messages
    /// from the broker node are dispatched to the matching channel,
    /// which a dedicated background task forwards to the actual
    /// [`FanoutSink`].
    sinks: Arc<DashMap<u64, mpsc::Sender<Bytes>>>,
    /// Handle for the background reader task. When the remote broker
    /// is dropped, this handle will be cancelled by Tokio, stopping
    /// the reader.
    _reader_handle: tokio::task::JoinHandle<()>,
}

impl RemoteBroker {
    /// Connect to a broker node at the given socket address.
    ///
    /// Establishes a TCP connection to `addr`, wraps it in a
    /// [`Framed`] transport using the [`WireCodec`], and spawns a
    /// background reader task that continuously parses incoming
    /// [`WireMsg`] frames and dispatches them:
    ///
    /// - `Deliver` push messages are routed to the matching local
    ///   sink's mpsc channel (identified by `sink_id`).
    /// - All response variants are dispatched to the oldest pending
    ///   request's oneshot channel.
    ///
    /// # Arguments
    ///
    /// * `addr` — The socket address of the external broker node.
    ///
    /// # Errors
    ///
    /// Returns an error if the TCP connection cannot be established.
    pub async fn connect(addr: SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| RiftError::System(SystemReject::Internal(e.to_string())))?;
        let (framed_writer, mut framed_reader) =
            Framed::new(stream, WireCodec::default()).split::<WireMsg>();
        let framed = Arc::new(Mutex::new(framed_writer));
        let pending: Arc<DashMap<u32, oneshot::Sender<WireMsg>>> = Arc::new(DashMap::new());
        let sinks: Arc<DashMap<u64, mpsc::Sender<Bytes>>> = Arc::new(DashMap::new());

        // Spawn reader task.
        let pending_r = pending.clone();
        let sinks_r = sinks.clone();
        let reader_handle = tokio::spawn(async move {
            while let Some(msg_result) = framed_reader.next().await {
                let Ok(msg) = msg_result else {
                    break;
                };
                match msg {
                    WireMsg::Deliver {
                        sink_id, payload, ..
                    } => {
                        // Use `try_send` to avoid blocking the reader
                        // when a downstream sink is slow. A full
                        // channel means the subscriber is backpressured
                        // and we drop the payload rather than stall
                        // all request/response traffic on the same
                        // connection.
                        if let Some(tx) = sinks_r.get(&sink_id)
                            && let Err(e) = tx.try_send(payload)
                        {
                            tracing::warn!(sink_id, error = %e,
                                    "dropping deliver payload: sink backpressured");
                        }
                    }
                    // All response variants now carry a `request_id` field,
                    // enabling correct correlation of concurrent requests.
                    // Extract the request_id from whichever response variant
                    // arrived and dispatch to the matching pending oneshot.
                    response => {
                        let req_id = extract_request_id(&response);
                        if let Some((_, tx)) = pending_r.remove(&req_id) {
                            let _ = tx.send(response);
                        }
                    }
                }
            }
        });

        Ok(Self {
            framed,
            pending,
            next_id: AtomicU32::new(1),
            sinks,
            _reader_handle: reader_handle,
        })
    }

    /// Send a [`WireMsg`] request to the broker node and wait for the
    /// matching response.
    ///
    /// Allocates a unique request ID, registers a oneshot channel in
    /// the pending map, writes the framed request to the TCP
    /// connection, and awaits the response on the oneshot receiver.
    ///
    /// The request has a 30-second timeout. If the broker does not
    /// respond in time, the pending entry is removed and a system
    /// error is returned.
    ///
    /// # Arguments
    ///
    /// * `msg` — The request message to send.
    ///
    /// # Errors
    ///
    /// Returns an error if the write fails, the connection is closed
    /// before a response arrives, or the request times out.
    async fn request(&self, mut msg: WireMsg) -> Result<WireMsg> {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        // Stamp the request_id on the outgoing message so the broker
        // node can echo it back in the response.
        set_request_id(&mut msg, id);
        let (tx, rx) = oneshot::channel();
        self.pending.insert(id, tx);

        // Write the framed request.
        let mut framed = self.framed.lock().await;
        framed
            .send(msg)
            .await
            .map_err(|e| RiftError::System(SystemReject::Internal(format!("send: {e}"))))?;
        drop(framed);

        // Wait for response.
        match tokio::time::timeout(std::time::Duration::from_secs(30), rx).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(_)) => Err(RiftError::System(SystemReject::Internal(
                "broker connection closed".into(),
            ))),
            Err(_) => {
                // Remove the pending entry so it does not leak.
                self.pending.remove(&id);
                Err(RiftError::System(SystemReject::Internal(
                    "broker request timed out".into(),
                )))
            }
        }
    }
}

#[async_trait]
impl Broker for RemoteBroker {
    async fn publish(&self, frame: &crate::frame::Frame) -> Result<PublishOutcome> {
        let resp = self
            .request(WireMsg::Publish {
                request_id: 0,
                frame: frame.clone(),
            })
            .await?;
        match resp {
            WireMsg::PublishResult { outcome, .. } => Ok(outcome),
            WireMsg::Error { code, message, .. } => Err(RiftError::System(SystemReject::Internal(
                format!("broker error: {code} — {message}"),
            ))),
            _ => Err(RiftError::System(SystemReject::Internal(
                "unexpected broker response".into(),
            ))),
        }
    }

    async fn subscribe(
        &self,
        topic: &str,
        intent: SubscribeIntent,
        sink: ConnectionSink,
    ) -> Result<SubscriptionId> {
        let raw_id = crate::broker::fanout::new_sink_id();
        // Register local sink for push delivery.
        let (tx, mut rx) = mpsc::channel::<Bytes>(256);
        self.sinks.insert(raw_id, tx);

        // Background task to route push messages to the actual fanout sink.
        let sink_clone = sink.clone();
        let bg_task = tokio::spawn(async move {
            while let Some(payload) = rx.recv().await {
                let _ = sink_clone.deliver(payload);
            }
        });

        let resp = self
            .request(WireMsg::Subscribe {
                request_id: 0,
                topic: topic.to_string(),
                intent,
                sink_id: raw_id,
            })
            .await;
        match resp {
            Ok(WireMsg::SubscribeResult { id, .. }) => Ok(SubscriptionId(id)),
            Ok(WireMsg::Error { code, message, .. }) => {
                // Subscribe failed: clean up the local sink entry
                // and abort the background task so it does not
                // run forever.
                self.sinks.remove(&raw_id);
                bg_task.abort();
                Err(RiftError::System(SystemReject::Internal(format!(
                    "broker error: {code} — {message}"
                ))))
            }
            Ok(_) => {
                self.sinks.remove(&raw_id);
                bg_task.abort();
                Err(RiftError::System(SystemReject::Internal(
                    "unexpected broker response".into(),
                )))
            }
            Err(e) => {
                self.sinks.remove(&raw_id);
                bg_task.abort();
                Err(e)
            }
        }
    }

    async fn unsubscribe(&self, id: SubscriptionId) -> Result<bool> {
        let resp = self
            .request(WireMsg::Unsubscribe {
                request_id: 0,
                id: id.0,
            })
            .await?;
        match resp {
            WireMsg::UnsubscribeResult { ok, .. } => Ok(ok),
            WireMsg::Error { code, message, .. } => Err(RiftError::System(SystemReject::Internal(
                format!("broker error: {code} — {message}"),
            ))),
            _ => Err(RiftError::System(SystemReject::Internal(
                "unexpected unsubscribe response".into(),
            ))),
        }
    }

    async fn drop_sink(&self, sink_id: u64) -> usize {
        self.sinks.remove(&sink_id);
        match self
            .request(WireMsg::DropSink {
                request_id: 0,
                sink_id,
            })
            .await
        {
            Ok(WireMsg::DropSinkResult { count, .. }) => count,
            Ok(_) | Err(_) => 0,
        }
    }

    async fn replay(&self, topic: &str, from: i64, to: i64) -> Result<Vec<Bytes>> {
        let resp = self
            .request(WireMsg::Replay {
                request_id: 0,
                topic: topic.to_string(),
                from,
                to,
            })
            .await?;
        match resp {
            WireMsg::ReplayResult { entries, .. } => Ok(entries),
            WireMsg::Error { code, message, .. } => Err(RiftError::System(SystemReject::Internal(
                format!("broker error: {code} — {message}"),
            ))),
            _ => Err(RiftError::System(SystemReject::Internal(
                "unexpected broker response".into(),
            ))),
        }
    }

    async fn snapshot(&self, topic: &str) -> Result<Option<crate::storage::StoredSnapshot>> {
        let resp = self
            .request(WireMsg::Snapshot {
                request_id: 0,
                topic: topic.to_string(),
            })
            .await?;
        match resp {
            WireMsg::SnapshotResult { snapshot, .. } => Ok(snapshot),
            _ => Err(RiftError::System(SystemReject::Internal(
                "unexpected broker response".into(),
            ))),
        }
    }

    async fn subscriber_count(&self, topic: &str) -> usize {
        let resp = self
            .request(WireMsg::SubscriberCount {
                request_id: 0,
                topic: topic.to_string(),
            })
            .await;
        match resp {
            Ok(WireMsg::SubscriberCountResult { count, .. }) => count,
            _ => 0,
        }
    }

    async fn head_offset(&self, topic: &str) -> i64 {
        let resp = self
            .request(WireMsg::HeadOffset {
                request_id: 0,
                topic: topic.to_string(),
            })
            .await;
        match resp {
            Ok(WireMsg::HeadOffsetResult { offset, .. }) => offset,
            _ => 0,
        }
    }

    async fn dec_publisher(&self, _topic: &str) {
        // The remote broker node manages its own publisher tracking
        // state. The local side does not maintain a publisher count.
    }
}

// ── request_id helpers ─────────────────────────────────────────────────

/// Stamp a `request_id` onto an outgoing [`WireMsg`] request variant.
///
/// This is called by [`RemoteBroker::request`] just before sending a
/// message so the remote broker node can echo the id back in its
/// response.
fn set_request_id(msg: &mut WireMsg, id: u32) {
    match msg {
        WireMsg::Publish { request_id, .. }
        | WireMsg::Subscribe { request_id, .. }
        | WireMsg::Unsubscribe { request_id, .. }
        | WireMsg::DropSink { request_id, .. }
        | WireMsg::Replay { request_id, .. }
        | WireMsg::Snapshot { request_id, .. }
        | WireMsg::SubscriberCount { request_id, .. }
        | WireMsg::HeadOffset { request_id, .. } => {
            *request_id = id;
        }
        // Deliver and response variants are never sent as requests.
        _ => {}
    }
}

/// Extract the `request_id` from an incoming [`WireMsg`] response variant.
///
/// The id is used by the reader task to correlate the response with the
/// correct pending request. Returns `0` for non-response variants (which
/// should never reach this code path).
fn extract_request_id(msg: &WireMsg) -> u32 {
    match msg {
        WireMsg::PublishResult { request_id, .. }
        | WireMsg::SubscribeResult { request_id, .. }
        | WireMsg::UnsubscribeResult { request_id, .. }
        | WireMsg::DropSinkResult { request_id, .. }
        | WireMsg::ReplayResult { request_id, .. }
        | WireMsg::SnapshotResult { request_id, .. }
        | WireMsg::SubscriberCountResult { request_id, .. }
        | WireMsg::HeadOffsetResult { request_id, .. }
        | WireMsg::Error { request_id, .. } => *request_id,
        _ => 0,
    }
}
