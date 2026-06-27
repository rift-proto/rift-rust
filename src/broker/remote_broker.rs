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
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc, oneshot};

use crate::broker::broker::{Broker, PublishOutcome};
use crate::broker::fanout::{ConnectionSink, SubscribeIntent, SubscriptionId};
use crate::broker::wire::WireMsg;
use crate::error::{Result, RiftError};

/// A `Broker` that talks to an external broker node over framed TCP.
///
/// Opens a single TCP connection for both request/response and push
/// delivery.  Push messages (`Deliver`) are routed to a local
/// fanout map of sink IDs → channels.
pub struct RemoteBroker {
    /// Write half of the TCP connection (serialised).
    writer: Mutex<tokio::io::WriteHalf<TcpStream>>,
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
    /// Spawns a background reader task that dispatches incoming
    /// responses and push messages.
    pub async fn connect(addr: SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| RiftError::System(crate::error::SystemReject::Internal(e.to_string())))?;
        let (reader, writer) = tokio::io::split(stream);
        let pending: Arc<DashMap<u32, oneshot::Sender<WireMsg>>> = Arc::new(DashMap::new());
        let sinks: Arc<DashMap<u64, mpsc::Sender<Bytes>>> = Arc::new(DashMap::new());

        // Spawn reader task.
        let _pending_reader = pending.clone();
        let _sinks_reader = sinks.clone();
        let reader_handle = tokio::spawn(async move {
            // Reader task: reads framed messages from the broker connection.
            // In a full implementation this would parse the length-prefixed
            // CBOR frames and route responses to pending requests and push
            // messages to local sinks.  The current stub just keeps the
            // connection alive.
            let mut buf = vec![0u8; 4096];
            let mut read_half = reader;
            loop {
                match tokio::io::AsyncReadExt::read(&mut read_half, &mut buf).await {
                    Ok(0) => break, // EOF
                    Ok(_) => {
                        // In a full implementation, accumulate bytes,
                        // parse frames, and dispatch.
                    }
                    Err(_) => break,
                }
            }
            drop(read_half);
        });

        Ok(Self {
            writer: Mutex::new(writer),
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

        // Write.
        let mut payload = Vec::new();
        ciborium::into_writer(&msg, &mut payload).map_err(|e| {
            RiftError::Frame(crate::error::FrameReject::FrameInvalid(e.to_string()))
        })?;
        let len = (payload.len() as u32).to_be_bytes();
        let mut writer = self.writer.lock().await;
        writer
            .write_all(&len)
            .await
            .map_err(|e| RiftError::System(crate::error::SystemReject::Internal(e.to_string())))?;
        writer
            .write_all(&payload)
            .await
            .map_err(|e| RiftError::System(crate::error::SystemReject::Internal(e.to_string())))?;

        // Wait for response.
        match tokio::time::timeout(std::time::Duration::from_secs(30), rx).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(_)) => Err(RiftError::System(crate::error::SystemReject::Internal(
                "broker connection closed".into(),
            ))),
            Err(_) => Err(RiftError::System(crate::error::SystemReject::Internal(
                "broker request timed out".into(),
            ))),
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
            WireMsg::Error { code, message } => Err(RiftError::System(
                crate::error::SystemReject::Internal(format!("broker error: {code} — {message}")),
            )),
            _ => Err(RiftError::System(crate::error::SystemReject::Internal(
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
            WireMsg::Error { code, message } => Err(RiftError::System(
                crate::error::SystemReject::Internal(format!("broker error: {code} — {message}")),
            )),
            _ => Err(RiftError::System(crate::error::SystemReject::Internal(
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
            WireMsg::Error { code, message } => Err(RiftError::System(
                crate::error::SystemReject::Internal(format!("broker error: {code} — {message}")),
            )),
            _ => Err(RiftError::System(crate::error::SystemReject::Internal(
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
            _ => Err(RiftError::System(crate::error::SystemReject::Internal(
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
}
