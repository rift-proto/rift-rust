//! Remote broker — a `Broker` implementation that connects to an
//! external broker node over TCP.
//!
//! The external broker node must speak the wire protocol defined in
//! [`crate::broker::wire`].  This crate does not provide the broker
//! node; users write their own in any language / stack.

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
/// Opens a single TCP connection for both request/response and push
/// delivery.  Push messages (`Deliver`) are routed to a local fanout
/// map of sink IDs → channels.
pub struct RemoteBroker {
    /// Writer half of the framed TCP connection (serialised).
    framed: Arc<Mutex<futures_util::stream::SplitSink<Framed<TcpStream, WireCodec>, WireMsg>>>,
    /// Pending requests: request_id → reply channel.
    pending: Arc<DashMap<u32, oneshot::Sender<WireMsg>>>,
    /// Monotonic request id.
    next_id: AtomicU32,
    /// Local sink map: sink_id → channel.
    sinks: Arc<DashMap<u64, mpsc::Sender<Bytes>>>,
    /// Handle for the background reader task.
    _reader_handle: tokio::task::JoinHandle<()>,
}

impl RemoteBroker {
    /// Connect to a broker node at `addr`.
    ///
    /// Spawns a background reader task that parses `WireMsg` frames
    /// from the connection and dispatches:
    /// - `Deliver` push messages to the matching local sink's channel
    /// - response messages to the matching pending request's oneshot
    pub async fn connect(addr: SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| RiftError::System(SystemReject::Internal(e.to_string())))?;
        let (framed_writer, mut framed_reader) = Framed::new(stream, WireCodec).split::<WireMsg>();
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
                        if let Some(tx) = sinks_r.get(&sink_id) {
                            let _ = tx.send(payload).await;
                        }
                    }
                    // All response variants dispatch to pending. The
                    // current WireMsg design does not carry a
                    // request_id on response variants (see wire.rs),
                    // so we use a FIFO assumption: pop the first
                    // pending entry. This is safe under a single
                    // in-flight request per RemoteBroker; concurrent
                    // requests are not supported until a request_id
                    // is added to all response variants.
                    response => {
                        let first_key = pending_r.iter().next().map(|e| *e.key());
                        if let Some(id) = first_key
                            && let Some((_, tx)) = pending_r.remove(&id)
                        {
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

    /// Send a request and wait for the response.
    async fn request(&self, msg: WireMsg) -> Result<WireMsg> {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
                frame: frame.clone(),
            })
            .await?;
        match resp {
            WireMsg::PublishResult { outcome } => Ok(outcome),
            WireMsg::Error { code, message } => Err(RiftError::System(SystemReject::Internal(
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
        tokio::spawn(async move {
            while let Some(payload) = rx.recv().await {
                let _ = sink_clone.deliver(payload);
            }
        });

        let resp = self
            .request(WireMsg::Subscribe {
                topic: topic.to_string(),
                intent,
                sink_id: raw_id,
            })
            .await?;
        match resp {
            WireMsg::SubscribeResult { id } => Ok(SubscriptionId(id)),
            WireMsg::Error { code, message } => Err(RiftError::System(SystemReject::Internal(
                format!("broker error: {code} — {message}"),
            ))),
            _ => Err(RiftError::System(SystemReject::Internal(
                "unexpected broker response".into(),
            ))),
        }
    }

    async fn unsubscribe(&self, _id: SubscriptionId) -> Result<bool> {
        // Remote broker handles subscriber tracking.
        Ok(true)
    }

    async fn drop_sink(&self, sink_id: u64) -> usize {
        self.sinks.remove(&sink_id);
        let _ = self.request(WireMsg::DropSink { sink_id }).await;
        1
    }

    async fn replay(&self, topic: &str, from: i64, to: i64) -> Result<Vec<Bytes>> {
        let resp = self
            .request(WireMsg::Replay {
                topic: topic.to_string(),
                from,
                to,
            })
            .await?;
        match resp {
            WireMsg::ReplayResult { entries } => Ok(entries),
            WireMsg::Error { code, message } => Err(RiftError::System(SystemReject::Internal(
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
                topic: topic.to_string(),
            })
            .await?;
        match resp {
            WireMsg::SnapshotResult { snapshot } => Ok(snapshot),
            _ => Err(RiftError::System(SystemReject::Internal(
                "unexpected broker response".into(),
            ))),
        }
    }

    async fn subscriber_count(&self, topic: &str) -> usize {
        let resp = self
            .request(WireMsg::SubscriberCount {
                topic: topic.to_string(),
            })
            .await;
        match resp {
            Ok(WireMsg::SubscriberCountResult { count }) => count,
            _ => 0,
        }
    }

    async fn head_offset(&self, topic: &str) -> i64 {
        let resp = self
            .request(WireMsg::HeadOffset {
                topic: topic.to_string(),
            })
            .await;
        match resp {
            Ok(WireMsg::HeadOffsetResult { offset }) => offset,
            _ => 0,
        }
    }

    async fn dec_publisher(&self, _topic: &str) {
        // Remote broker manages its own state.
    }
}
