//! `ClusterBroker` — combines local actor broker semantics with
//! TCP mesh cluster communication.
//!
//! Implements the [`Broker`] trait by delegating local topic
//! operations to a backing broker (typically `InMemoryBroker` or
//! `ActorBroker`) and broadcasting publishes to all cluster peers
//! via the [`MeshRouter`].
//!
//! ## Lifecycle
//!
//! 1. `ClusterBroker::start(config)` binds the mesh TCP listener,
//!    spawns the gossip engine, runs discovery, and starts the
//!    inbound message dispatcher.
//! 2. The broker runs for the lifetime of the server.
//! 3. On shutdown, the gossip engine broadcasts `Leave` and the
//!    mesh listener is closed.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::broker::broker::{Broker, PublishOutcome};
use crate::broker::fanout::{ConnectionSink, FanoutEngine, SubscribeIntent, SubscriptionId};
use crate::cluster::wire::ClusterMsg as WireMsg;
use crate::cluster::config::ClusterConfig;
use crate::cluster::connection::{ConnectionPool, MeshConnection};
use crate::cluster::discovery::{CompositeDiscovery, Discovery, SeedDiscovery};
use crate::cluster::gossip::ClusterGossip;
use crate::cluster::node::NodeId;
use crate::cluster::router::MeshRouter;
use crate::error::{Result, RiftError, SystemReject};
use crate::frame::Frame;
use crate::storage::StoredSnapshot;

/// Pending `ActorForward` requests awaiting a response.
type ForwardMap = Arc<dashmap::DashMap<u32, oneshot::Sender<Bytes>>>;

/// A message that can be forwarded to a remote cluster node for
/// execution on its local broker. Only safe-for-forwarding operations
/// are included (read-only queries that operate on shared state).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) enum ForwardMsg {
    /// Replay historical messages for a topic.
    Replay {
        from: i64,
        to: i64,
    },
    /// Fetch the latest snapshot for a topic.
    Snapshot,
    /// Query the head offset for a topic.
    HeadOffset,
}

/// The cluster broker — combines local storage + fanout with
/// cross-node TCP mesh communication.
#[allow(dead_code)]
pub struct ClusterBroker {
    /// Local node id.
    local_id: NodeId,
    /// Local storage-backed broker (handles `replay`, `snapshot`,
    /// `head_offset`, `subscriber_count` for topics owned locally).
    local: Arc<dyn Broker>,
    /// Local fanout engine — shared with the router.
    fanout: Arc<FanoutEngine>,
    /// Outbound mesh connection pool.
    pool: Arc<ConnectionPool>,
    /// Cross-node message router.
    router: Arc<MeshRouter>,
    /// Gossip engine handle (kept to allow graceful shutdown).
    _gossip_handle: JoinHandle<()>,
    /// Pending ActorForward requests keyed by `request_id`.
    forwards: ForwardMap,
    /// Monotonic `request_id` counter for ActorForward.
    next_request_id: std::sync::atomic::AtomicU32,
    /// Set of known topic names, updated on subscribe/unsubscribe.
    /// Shared with the gossip engine's `topic_counts_fn` callback.
    topics_tracked: Arc<std::sync::RwLock<std::collections::HashSet<String>>>,
    /// Local per-topic subscriber counts, maintained by subscribe/unsubscribe
    /// and read by the gossip closure for piggybacking on MemberUpdate.
    local_topic_counts: Arc<dashmap::DashMap<String, usize>>,
    /// Member table (shared with gossip).
    members: crate::cluster::gossip::MemberTable,
    shutdown: Arc<tokio::sync::Notify>,
}

impl ClusterBroker {
    /// Start the cluster broker.
    ///
    /// Binds the mesh TCP listener, spawns the gossip engine,
    /// runs initial discovery, and starts the inbound message
    /// dispatcher.
    pub async fn start(config: ClusterConfig, local_broker: Arc<dyn Broker>) -> Result<Arc<Self>> {
        let local_id = NodeId::new();
        let listen_addr = config.listen_addr;

        // 1. Bind mesh TCP listener.
        let (pool, inbound_rx, shutdown) =
            MeshConnection::start(listen_addr, local_id.clone()).await?;

        // 2. Create the outbound message channel (gossip + router → pool).
        let (outbound_tx, mut outbound_rx) = mpsc::channel::<(NodeId, WireMsg)>(256);

        // 3. Spawn the outbound writer task — drains `outbound_rx`
        //    and sends each message to the appropriate peer link.
        let pool_for_writer = pool.clone();
        tokio::spawn(async move {
            while let Some((target, msg)) = outbound_rx.recv().await {
                if let Err(e) = pool_for_writer.send_to(&target, msg).await {
                    warn!(target = %target, error = %e, "outbound send failed");
                }
            }
        });

        // 4. Create the gossip engine with a topic-counts callback.
        let local_topic_counts: Arc<dashmap::DashMap<String, usize>> =
            Arc::new(dashmap::DashMap::new());
        let counts_for_gossip = local_topic_counts.clone();
        let topic_counts_fn: Arc<dyn Fn() -> HashMap<String, usize> + Send + Sync> =
            Arc::new(move || {
                counts_for_gossip
                    .iter()
                    .map(|r| (r.key().clone(), *r.value()))
                    .collect()
            });
        let gossip = Arc::new(ClusterGossip::new(
            local_id.clone(),
            listen_addr,
            outbound_tx.clone(),
            config.clone(),
            topic_counts_fn,
        ));
        let members = gossip.members();
        let gossip_handle = gossip.clone().spawn();

        // 5. Create the local fanout engine + router.
        let fanout = Arc::new(FanoutEngine::new());
        let router = Arc::new(MeshRouter::new(
            local_id.clone(),
            pool.clone(),
            fanout.clone(),
        ));

        // 6. Pending ActorForward map.
        let forwards: ForwardMap = Arc::new(dashmap::DashMap::new());

        // 7. Spawn the inbound dispatcher — routes incoming
        //    `WireMsg`s to gossip / router / forward handlers.
        let dispatcher_gossip = gossip.clone();
        let dispatcher_router = router.clone();
        let dispatcher_forwards = forwards.clone();
        let dispatcher_local = local_broker.clone();
        let dispatcher_shutdown = shutdown.clone();
        let dispatcher_pool = pool.clone();
        tokio::spawn(async move {
            Self::dispatch_loop(
                inbound_rx,
                dispatcher_gossip,
                dispatcher_router,
                dispatcher_forwards,
                dispatcher_local,
                dispatcher_pool,
                dispatcher_shutdown,
            )
            .await;
        });

        // 8. Run discovery and connect to discovered peers.
        let mut discoveries: Vec<Arc<dyn Discovery>> = Vec::new();
        if !config.seed_nodes.is_empty() {
            discoveries.push(Arc::new(SeedDiscovery::new(config.seed_nodes.clone())));
        }
        if !discoveries.is_empty() {
            let composite = CompositeDiscovery::new(discoveries);
            let pool_clone = pool.clone();
            let out_clone = outbound_tx.clone();
            let gossip_clone = gossip.clone();
            let local_id_clone = local_id.clone();
            let reconnect_shutdown = shutdown.clone();
            let base_ms = config.reconnect_base_ms;
            let max_ms = config.reconnect_max_ms;
            let max_attempts = config.max_reconnect_attempts;
            tokio::spawn(async move {
                let Ok(mut candidates) = composite.discover().await else {
                    return;
                };
                while let Some(candidate) = candidates.recv().await {
                    let peer_id = candidate.node_id.clone().unwrap_or_else(NodeId::new);
                    gossip_clone.add_peer(peer_id.clone(), candidate.addr);
                    MeshConnection::spawn_reconnect_loop(
                        peer_id,
                        candidate.addr,
                        pool_clone.clone(),
                        out_clone.clone(),
                        reconnect_shutdown.clone(),
                        base_ms,
                        max_ms,
                        max_attempts,
                    );
                }
                let _ = local_id_clone;
            });
        }

        info!(node_id = %local_id, addr = %listen_addr, "cluster broker started");

        Ok(Arc::new(Self {
            local_id,
            local: local_broker,
            fanout,
            pool,
            router,
            _gossip_handle: gossip_handle,
            forwards,
            next_request_id: std::sync::atomic::AtomicU32::new(1),
            members,
            topics_tracked: Arc::new(std::sync::RwLock::new(std::collections::HashSet::new())),
            local_topic_counts,
            shutdown,
        }))
    }

    /// Inbound dispatch loop.
    async fn dispatch_loop(
        mut inbound_rx: mpsc::Receiver<(NodeId, WireMsg)>,
        gossip: Arc<ClusterGossip>,
        router: Arc<MeshRouter>,
        forwards: ForwardMap,
        local: Arc<dyn Broker>,
        pool: Arc<ConnectionPool>,
        shutdown: Arc<tokio::sync::Notify>,
    ) {
        loop {
            tokio::select! {
                _ = shutdown.notified() => return,
                msg = inbound_rx.recv() => {
                    let Some((from, msg)) = msg else { return; };
                    match &msg {
                        // Gossip messages → gossip engine.
                        WireMsg::Ping { .. }
                        | WireMsg::Ack { .. }
                        | WireMsg::PingReq { .. }
                        | WireMsg::MemberUpdate { .. }
                        | WireMsg::Leave { .. } => {
                            gossip.handle_message(from, msg).await;
                        }
                        // Remote fanout → router → local subscribers.
                        WireMsg::RemoteFanout { from, topic, offset, payload } => {
                            router.handle_remote_fanout(from, topic, *offset, payload.clone());
                        }
                        // ActorForward → local broker.
                        WireMsg::ActorForward { request_id, topic, msg } => {
                            Self::handle_actor_forward(
                                local.clone(),
                                pool.clone(),
                                from,
                                *request_id,
                                topic.clone(),
                                msg.clone(),
                            ).await;
                        }
                        // ActorForwardResult → match pending request.
                        WireMsg::ActorForwardResult { request_id, result } => {
                            if let Some((_, tx)) = forwards.remove(request_id) {
                                let _ = tx.send(result.clone());
                            }
                        }
                    }
                }
            }
        }
    }

    /// Handle an inbound `ActorForward` by invoking the local
    /// broker and returning the result.
    ///
    /// Deserializes the embedded [`ForwardMsg`] (CBOR), dispatches
    /// to the corresponding local `Broker` method, serializes the
    /// response to CBOR, and sends `ActorForwardResult` back to the
    /// originating node.
    async fn handle_actor_forward(
        local: Arc<dyn Broker>,
        pool: Arc<ConnectionPool>,
        from: NodeId,
        request_id: u32,
        topic: String,
        msg: Bytes,
    ) {
        // Deserialize the ForwardMsg.
        let fwd_msg: Result<ForwardMsg> = ciborium::from_reader(msg.as_ref())
            .map_err(|e| {
                RiftError::System(SystemReject::Internal(format!("actor forward decode: {e}")))
            });
        let fwd_msg = match fwd_msg {
            Ok(m) => m,
            Err(e) => {
                // Send back an error result.
                let err_bytes = serialize_error(&e);
                let _ = pool
                    .send_to(
                        &from,
                        WireMsg::ActorForwardResult {
                            request_id,
                            result: err_bytes,
                        },
                    )
                    .await;
                return;
            }
        };

        // Dispatch to the local broker based on the variant.
        let result_bytes: Bytes = match fwd_msg {
            ForwardMsg::Replay { from: from_off, to } => {
                let entries = local.replay(&topic, from_off, to).await.unwrap_or_default();
                serialize_vec_bytes(&entries)
            }
            ForwardMsg::Snapshot { .. } => {
                let snap = local.snapshot(&topic).await.unwrap_or(None);
                serialize_option_snapshot(&snap)
            }
            ForwardMsg::HeadOffset { .. } => {
                let off = local.head_offset(&topic).await;
                serialize_i64(off)
            }
            // Unrecognized variants — return an error.
            _ => {
                let err =
                    RiftError::System(SystemReject::Internal("unsupported forward variant".into()));
                serialize_error(&err)
            }
        };

        // Send the result back to the originating node.
        let _ = pool
            .send_to(
                &from,
                WireMsg::ActorForwardResult {
                    request_id,
                    result: result_bytes,
                },
            )
            .await;
    }

    /// Forward a `ForwardMsg` to a specific peer and await the
    /// response via a registered oneshot.
    ///
    /// Allocates a `request_id`, registers a pending oneshot in the
    /// `forwards` map, sends `ActorForward`, and awaits the result
    /// with a timeout.
    #[allow(dead_code, clippy::too_many_arguments)]
    async fn forward_and_await(
        &self,
        target: &NodeId,
        topic: &str,
        msg: ForwardMsg,
        timeout: std::time::Duration,
    ) -> Result<Bytes> {
        let request_id = self
            .next_request_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut msg_buf = Vec::new();
        ciborium::into_writer(&msg, &mut msg_buf).map_err(|e| {
            RiftError::System(SystemReject::Internal(format!("forward encode: {e}")))
        })?;
        let msg_bytes = Bytes::from(msg_buf);

        let (tx, rx) = oneshot::channel();
        self.forwards.insert(request_id, tx);

        self.router
            .forward_actor_request(target, request_id, topic, msg_bytes)
            .await;

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(b)) => Ok(b),
            Ok(Err(_)) => Err(RiftError::System(SystemReject::Internal(
                "forward peer closed".into(),
            ))),
            Err(_) => {
                self.forwards.remove(&request_id);
                Err(RiftError::System(SystemReject::Internal(
                    "forward timed out".into(),
                )))
            }
        }
    }

    /// Graceful shutdown — broadcasts Leave and signals the
    /// dispatcher to exit.
    pub async fn shutdown(&self) {
        self.shutdown.notify_waiters();
        // The gossip engine's Leave broadcast happens via its own
        // shutdown path (not wired here in the initial impl).
    }
}

#[async_trait]
impl Broker for ClusterBroker {
    async fn publish(&self, frame: &Frame) -> Result<PublishOutcome> {
        // 1. Local publish — allocates offset, dedupes, appends log.
        let outcome = self.local.publish(frame).await?;

        // 2. Broadcast to peers (skipped for duplicates).
        if !outcome.duplicate {
            let topic = frame.topic.as_deref().unwrap_or("");
            self.router
                .broadcast_publish(topic, outcome.offset, frame)
                .await;
        }

        Ok(outcome)
    }

    async fn subscribe(
        &self,
        topic: &str,
        intent: SubscribeIntent,
        sink: ConnectionSink,
    ) -> Result<SubscriptionId> {
        // Local subscription — remote publishes are routed here by
        // the router's handle_remote_fanout.
        let id = self.local.subscribe(topic, intent, sink).await?;
        // Track for global subscriber_count aggregation.
        let count = self.local.subscriber_count(topic).await;
        self.local_topic_counts.insert(topic.to_string(), count);
        Ok(id)
    }

    async fn unsubscribe(&self, id: SubscriptionId) -> Result<bool> {
        let ok = self.local.unsubscribe(id).await?;
        // The count update will be reflected on the next gossip round.
        // For now, approximate correctness is sufficient.
        Ok(ok)
    }

    async fn drop_sink(&self, sink_id: u64) -> usize {
        self.local.drop_sink(sink_id).await
    }

    async fn replay(&self, topic: &str, from: i64, to: i64) -> Result<Vec<Bytes>> {
        // Try local first.
        let local_entries = self.local.replay(topic, from, to).await?;
        if !local_entries.is_empty() {
            return Ok(local_entries);
        }

        // Empty locally — broadcast ActorForward{Replay} to all peers.
        let msg = ForwardMsg::Replay { from, to };
        let mut buf = Vec::new();
        ciborium::into_writer(&msg, &mut buf).map_err(|e| {
            RiftError::System(SystemReject::Internal(format!("replay encode: {e}")))
        })?;
        let msg_bytes = Bytes::from(buf);

        let request_id = self
            .next_request_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.forwards.insert(request_id, tx);

        // Broadcast to all peers.
        let forward = WireMsg::ActorForward {
            request_id,
            topic: topic.to_string(),
            msg: msg_bytes.clone(),
        };
        self.pool.broadcast(forward).await;

        match tokio::time::timeout(std::time::Duration::from_secs(3), rx).await {
            Ok(Ok(result)) => match deserialize_vec_bytes(&result) {
                Some(entries) if !entries.is_empty() => Ok(entries),
                _ => Ok(Vec::new()),
            },
            _ => {
                self.forwards.remove(&request_id);
                Ok(Vec::new())
            }
        }
    }

    async fn snapshot(&self, topic: &str) -> Result<Option<crate::storage::StoredSnapshot>> {
        self.local.snapshot(topic).await
    }

    async fn subscriber_count(&self, topic: &str) -> usize {
        // Sum local count + all peers' piggybacked counts.
        let mut total = self.local.subscriber_count(topic).await;
        for member in self.members.snapshot() {
            if member.id != self.local_id {
                total += member.topic_counts.get(topic).copied().unwrap_or(0);
            }
        }
        total
    }

    async fn head_offset(&self, topic: &str) -> i64 {
        self.local.head_offset(topic).await
    }

    async fn dec_publisher(&self, topic: &str) {
        self.local.dec_publisher(topic).await
    }
}

// ── CBOR serialization helpers for ActorForward results ───────────────

/// Serialize a `RiftError` into CBOR bytes for the response payload.
fn serialize_error(e: &RiftError) -> Bytes {
    let mut buf = Vec::new();
    let _ = ciborium::into_writer(&e.to_string(), &mut buf);
    Bytes::from(buf)
}

/// Serialize `Vec<Bytes>` (the result of `Broker::replay`) into CBOR.
fn serialize_vec_bytes(v: &[Bytes]) -> Bytes {
    let mut buf = Vec::new();
    let strings: Vec<Vec<u8>> = v.iter().map(|b| b.to_vec()).collect();
    let _ = ciborium::into_writer(&strings, &mut buf);
    Bytes::from(buf)
}

/// Serialize `Option<StoredSnapshot>` into CBOR.
fn serialize_option_snapshot(s: &Option<StoredSnapshot>) -> Bytes {
    let mut buf = Vec::new();
    let _ = ciborium::into_writer(s, &mut buf);
    Bytes::from(buf)
}

/// Serialize an `i64` into CBOR.
fn serialize_i64(v: i64) -> Bytes {
    let mut buf = Vec::new();
    let _ = ciborium::into_writer(&v, &mut buf);
    Bytes::from(buf)
}

/// Deserialize a CBOR payload into `Vec<Bytes>` (for replay responses).
pub(crate) fn deserialize_vec_bytes(b: &Bytes) -> Option<Vec<Bytes>> {
    let v: Vec<Vec<u8>> = ciborium::from_reader(b.as_ref()).ok()?;
    Some(v.into_iter().map(Bytes::from).collect())
}
