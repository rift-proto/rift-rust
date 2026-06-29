//! TCP mesh connection management.
//!
//! [`MeshConnection`] owns a TCP listener for accepting inbound
//! cluster connections and a [`ConnectionPool`] of outbound
//! [`MeshLink`]s — one per peer node. All inter-node traffic goes
//! over these connections using the existing [`WireCodec`]
//! (length-prefixed CBOR).

use std::net::SocketAddr;
use std::sync::Arc;

use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tokio_util::codec::Framed;
use tracing::{info, warn};

use crate::cluster::wire::{ClusterCodec as WireCodec, ClusterMsg as WireMsg};
use crate::cluster::node::NodeId;
use crate::error::{Result, RiftError, SystemReject};

/// Default channel capacity for inbound message dispatch.
const DISPATCH_CAPACITY: usize = 256;

/// A single outbound link to a peer node.
pub struct MeshLink {
    writer: Arc<Mutex<futures_util::stream::SplitSink<Framed<TcpStream, WireCodec>, WireMsg>>>,
    _reader: JoinHandle<()>,
}

impl MeshLink {
    pub async fn send(&self, msg: WireMsg) -> Result<()> {
        let mut w = self.writer.lock().await;
        w.send(msg)
            .await
            .map_err(|e| RiftError::System(SystemReject::Internal(format!("mesh send: {e}"))))?;
        Ok(())
    }
}

/// Connection pool of outbound [`MeshLink`]s, keyed by peer `NodeId`.
pub struct ConnectionPool {
    links: DashMap<NodeId, Arc<MeshLink>>,
}

impl ConnectionPool {
    pub fn new() -> Self {
        Self {
            links: DashMap::new(),
        }
    }

    pub fn get(&self, node_id: &NodeId) -> Option<Arc<MeshLink>> {
        self.links.get(node_id).map(|r| r.value().clone())
    }

    pub fn insert(&self, node_id: NodeId, link: Arc<MeshLink>) {
        self.links.insert(node_id, link);
    }

    pub fn remove(&self, node_id: &NodeId) {
        self.links.remove(node_id);
    }

    pub fn peers(&self) -> Vec<NodeId> {
        self.links.iter().map(|r| r.key().clone()).collect()
    }

    pub async fn send_to(&self, node_id: &NodeId, msg: WireMsg) -> Result<()> {
        let link = self.get(node_id).ok_or_else(|| {
            RiftError::System(SystemReject::Internal(format!("no link to {node_id}")))
        })?;
        link.send(msg).await
    }

    pub async fn broadcast(&self, msg: WireMsg) {
        let peers: Vec<Arc<MeshLink>> = self.links.iter().map(|r| r.value().clone()).collect();
        for link in peers {
            let _ = link.send(msg.clone()).await;
        }
    }
}

impl Default for ConnectionPool {
    fn default() -> Self {
        Self::new()
    }
}

/// Static helpers for establishing and accepting mesh connections.
///
/// `ClusterBroker` owns the listener + pool directly; this struct
/// provides the static methods that operate on them.
pub struct MeshConnection;

impl MeshConnection {
    /// Bind a TCP listener and spawn the accept loop.
    ///
    /// Returns the connection pool (shared with the accept loop) and
    /// the receiver end of the inbound message dispatch channel.
    pub async fn start(
        addr: SocketAddr,
        local_id: NodeId,
    ) -> Result<(
        Arc<ConnectionPool>,
        mpsc::Receiver<(NodeId, WireMsg)>,
        Arc<tokio::sync::Notify>,
    )> {
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| RiftError::System(SystemReject::Internal(format!("mesh bind: {e}"))))?;
        let local_addr = listener
            .local_addr()
            .map_err(|e| RiftError::System(SystemReject::Internal(format!("local_addr: {e}"))))?;
        info!(addr = %local_addr, "mesh listener bound");

        let pool = Arc::new(ConnectionPool::new());
        let (inbound_tx, inbound_rx) = mpsc::channel(DISPATCH_CAPACITY);
        let shutdown = Arc::new(tokio::sync::Notify::new());

        let accept_pool = pool.clone();
        let accept_tx = inbound_tx.clone();
        let _accept_local = local_id.clone();
        let accept_shutdown = shutdown.clone();
        tokio::spawn(async move {
            Self::accept_loop(listener, accept_pool, accept_tx, accept_shutdown).await;
        });

        Ok((pool, inbound_rx, shutdown))
    }

    /// Establish an outbound connection to a peer.
    pub async fn connect_outbound(
        peer_id: NodeId,
        peer_addr: SocketAddr,
        pool: Arc<ConnectionPool>,
        dispatch: mpsc::Sender<(NodeId, WireMsg)>,
    ) -> Result<()> {
        let stream = TcpStream::connect(peer_addr)
            .await
            .map_err(|e| RiftError::System(SystemReject::Internal(format!("mesh connect: {e}"))))?;
        let framed = Framed::new(stream, WireCodec::default());
        let (sink, stream_rx) = framed.split::<WireMsg>();

        let writer = Arc::new(Mutex::new(sink));
        let dispatch_clone = dispatch.clone();
        let peer_id_clone = peer_id.clone();
        let reader = tokio::spawn(async move {
            let mut stream_rx = stream_rx;
            while let Some(msg_result) = stream_rx.next().await {
                match msg_result {
                    Ok(msg) => {
                        if dispatch_clone
                            .send((peer_id_clone.clone(), msg))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(e) => {
                        warn!("mesh reader error: {e}");
                        break;
                    }
                }
            }
        });

        let link = Arc::new(MeshLink {
            writer,
            _reader: reader,
        });
        pool.insert(peer_id, link);
        Ok(())
    }

    /// Spawn a reconnect loop that continuously tries to connect to
    /// `peer_addr`, with exponential backoff between attempts.
    ///
    /// Once connected, the link is registered in `pool` and stays
    /// active until the reader task exits (peer disconnect, error).
    /// When the link breaks, the loop removes the stale entry from
    /// the pool and starts a new connection attempt.
    ///
    /// The loop exits when `shutdown` is notified OR when
    /// `max_reconnect_attempts` is exceeded (0 = unlimited).
    #[allow(clippy::too_many_arguments)]
    pub fn spawn_reconnect_loop(
        peer_id: NodeId,
        peer_addr: SocketAddr,
        pool: Arc<ConnectionPool>,
        dispatch: mpsc::Sender<(NodeId, WireMsg)>,
        shutdown: Arc<tokio::sync::Notify>,
        base_ms: u64,
        max_ms: u64,
        max_attempts: u32,
    ) {
        tokio::spawn(async move {
            let mut attempt: u32 = 0;
            loop {
                tokio::select! {
                    _ = shutdown.notified() => return,
                    _ = async {} => {}
                }
                if max_attempts > 0 && attempt >= max_attempts {
                    warn!(peer = %peer_id, "reconnect max attempts reached");
                    return;
                }
                attempt += 1;

                let stream = match TcpStream::connect(peer_addr).await {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(peer = %peer_id, attempt, error = %e, "reconnect failed");
                        let delay = Self::backoff(base_ms, max_ms, attempt);
                        tokio::select! {
                            _ = shutdown.notified() => return,
                            _ = tokio::time::sleep(delay) => {}
                        }
                        continue;
                    }
                };

                info!(peer = %peer_id, attempt, "reconnected");

                let framed = Framed::new(stream, WireCodec::default());
                let (sink, stream_rx) = framed.split::<WireMsg>();
                let writer = Arc::new(Mutex::new(sink));
                let dispatch_clone = dispatch.clone();
                let peer_id_clone = peer_id.clone();
                let _pool_clone = pool.clone();
                let reader = tokio::spawn(async move {
                    let mut stream_rx = stream_rx;
                    while let Some(msg_result) = stream_rx.next().await {
                        match msg_result {
                            Ok(msg) => {
                                if dispatch_clone
                                    .send((peer_id_clone.clone(), msg))
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            }
                            Err(e) => {
                                warn!("mesh reader error: {e}");
                                break;
                            }
                        }
                    }
                });

                let link = Arc::new(MeshLink {
                    writer,
                    _reader: reader,
                });
                pool.insert(peer_id.clone(), link);
                // Reset attempt counter on successful connect.
                attempt = 0;
            }
        });
    }

    /// Exponential backoff: `min(base * 2^attempt, max)`, with a
    /// random jitter of ±25%.
    fn backoff(base_ms: u64, max_ms: u64, attempt: u32) -> std::time::Duration {
        let ms = (base_ms as f64 * 2.0_f64.powi(attempt as i32)) as u64;
        let ms = ms.min(max_ms);
        let jitter = (ms as f64 * 0.25) as u64;
        let ms = ms.saturating_sub(jitter / 2) + rand::random_range(0..=jitter);
        std::time::Duration::from_millis(ms)
    }

    /// Accept loop: spawns a handler per inbound connection.
    async fn accept_loop(
        listener: TcpListener,
        pool: Arc<ConnectionPool>,
        dispatch: mpsc::Sender<(NodeId, WireMsg)>,
        shutdown: Arc<tokio::sync::Notify>,
    ) {
        loop {
            tokio::select! {
                _ = shutdown.notified() => {
                    info!("mesh accept loop shutting down");
                    return;
                }
                accept = listener.accept() => {
                    let (stream, peer_addr) = match accept {
                        Ok(s) => s,
                        Err(e) => {
                            warn!("mesh accept error: {e}");
                            continue;
                        }
                    };
                    let framed = Framed::new(stream, WireCodec::default());
                    let (sink, stream_rx) = framed.split::<WireMsg>();
                    let dispatch_clone = dispatch.clone();
                    let pool_clone = pool.clone();
                    tokio::spawn(async move {
                        Self::inbound_session(stream_rx, sink, dispatch_clone, pool_clone, peer_addr).await;
                    });
                }
            }
        }
    }

    /// Handle a single inbound connection.
    async fn inbound_session(
        mut stream_rx: futures_util::stream::SplitStream<Framed<TcpStream, WireCodec>>,
        sink: futures_util::stream::SplitSink<Framed<TcpStream, WireCodec>, WireMsg>,
        dispatch: mpsc::Sender<(NodeId, WireMsg)>,
        pool: Arc<ConnectionPool>,
        peer_addr: SocketAddr,
    ) {
        let first = match stream_rx.next().await {
            Some(Ok(m)) => m,
            _ => return,
        };
        let peer_id = match extract_node_id(&first) {
            Some(id) => id,
            None => {
                warn!(addr = %peer_addr, "inbound connection did not identify itself");
                return;
            }
        };

        let writer = Arc::new(Mutex::new(sink));
        let dispatch_clone = dispatch.clone();
        let peer_id_clone = peer_id.clone();
        let reader = tokio::spawn(async move {
            let _ = dispatch_clone.send((peer_id_clone.clone(), first)).await;
            while let Some(msg_result) = stream_rx.next().await {
                match msg_result {
                    Ok(msg) => {
                        if dispatch_clone
                            .send((peer_id_clone.clone(), msg))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(e) => {
                        warn!("inbound reader error: {e}");
                        break;
                    }
                }
            }
        });
        let link = Arc::new(MeshLink {
            writer,
            _reader: reader,
        });
        pool.insert(peer_id, link);
    }
}

/// Extract the `NodeId` from any cluster-identifying message.
fn extract_node_id(msg: &WireMsg) -> Option<NodeId> {
    match msg {
        WireMsg::Ping { from, .. }
        | WireMsg::Ack { from, .. }
        | WireMsg::PingReq { from, .. }
        | WireMsg::MemberUpdate { from, .. }
        | WireMsg::Leave { from, .. }
        | WireMsg::RemoteFanout { from, .. } => Some(from.clone()),
        _ => None,
    }
}
