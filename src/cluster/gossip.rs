//! SWIM-based gossip protocol for cluster membership management.
//!
//! ## Protocol overview
//!
//! Each node maintains a [`MemberTable`] of all known peers. Every
//! `gossip_interval`, the node picks `gossip_fanout` random alive
//! members and sends them a [`WireMsg::Ping`]. If a direct ping
//! times out, the node sends a [`WireMsg::PingReq`] to `k` other
//! peers asking them to probe the suspect indirectly. If the
//! indirect probe also fails, the member is marked `Suspect`, then
//! `Dead` after `suspect_timeout`, and finally removed after
//! `dead_timeout`.
//!
//! Membership changes are propagated piggybacked on every gossip
//! message via [`WireMsg::MemberUpdate`].

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::RwLock;
use rand::Rng;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::cluster::wire::ClusterMsg as WireMsg;
use crate::cluster::config::ClusterConfig;
use crate::cluster::node::{NodeId, NodeInfo, NodeState};
use crate::now_ms;

/// Thread-safe member table.
///
/// Stores one [`NodeInfo`] per known node id. The local node is
/// always present with `NodeState::Alive`.
#[derive(Clone)]
pub struct MemberTable {
    inner: Arc<RwLock<HashMap<NodeId, NodeInfo>>>,
    /// Monotonic version counter — bumped on every change.
    version: Arc<std::sync::atomic::AtomicU64>,
}

impl MemberTable {
    /// Create an empty table.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            version: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Insert or update a member. Bumps the table version if the
    /// entry actually changed.
    pub fn upsert(&self, info: NodeInfo) {
        let mut g = self.inner.write();
        let prev = g.insert(info.id.clone(), info);
        if prev.is_none_or(|p| p.state != g.get(&p.id).map(|n| n.state).unwrap_or(NodeState::Alive))
        {
            self.version
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// Look up a member by id.
    pub fn get(&self, id: &NodeId) -> Option<NodeInfo> {
        self.inner.read().get(id).cloned()
    }

    /// Remove a member.
    pub fn remove(&self, id: &NodeId) {
        if self.inner.write().remove(id).is_some() {
            self.version
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// Return a snapshot of all members.
    pub fn snapshot(&self) -> Vec<NodeInfo> {
        self.inner.read().values().cloned().collect()
    }

    /// Return only alive members.
    pub fn alive_members(&self) -> Vec<NodeInfo> {
        self.inner
            .read()
            .values()
            .filter(|m| m.state == NodeState::Alive)
            .cloned()
            .collect()
    }

    /// Return the current table version.
    pub fn version(&self) -> u64 {
        self.version.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Number of known members (any state).
    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }
}

impl Default for MemberTable {
    fn default() -> Self {
        Self::new()
    }
}

/// Pending ping tracker — records which nodes have outstanding
/// direct pings so their `Ack` can clear the "alive" timer.
#[derive(Default)]
struct PingTracker {
    /// Maps target NodeId → (sent_at_ms, incarnation sent).
    pending: DashMap<NodeId, (i64, u64)>,
}

impl PingTracker {
    fn record(&self, target: &NodeId, incarnation: u64) {
        self.pending.insert(target.clone(), (now_ms(), incarnation));
    }

    fn clear(&self, target: &NodeId) -> Option<(i64, u64)> {
        self.pending.remove(target).map(|(_, v)| v)
    }

    fn outstanding(&self, timeout_ms: i64) -> Vec<NodeId> {
        let cutoff = now_ms() - timeout_ms;
        self.pending
            .iter()
            .filter(|r| r.value().0 < cutoff)
            .map(|r| r.key().clone())
            .collect()
    }
}

/// The gossip engine — owns the member table, runs the periodic
/// ping loop, and handles inbound gossip messages.
#[allow(dead_code)]
pub struct ClusterGossip {
    /// Local node id.
    local_id: NodeId,
    /// Local listen address (advertised to peers).
    local_addr: SocketAddr,
    /// Local incarnation counter.
    incarnation: Arc<std::sync::atomic::AtomicU64>,
    /// Shared member table.
    members: MemberTable,
    /// Outbound channel to the mesh connection pool (for sending
    /// gossip messages to peers).
    out_tx: mpsc::Sender<(NodeId, WireMsg)>,
    /// Pending ping tracker.
    pings: PingTracker,
    /// Cluster configuration.
    config: ClusterConfig,
    /// Callback to collect local topic→subscriber_count map for
    /// piggybacking on MemberUpdate.
    topic_counts_fn: Arc<dyn Fn() -> HashMap<String, usize> + Send + Sync>,
}

impl ClusterGossip {
    /// Create a new gossip engine.
    ///
    /// The local node is inserted into the member table as `Alive`.
    pub fn new(
        local_id: NodeId,
        local_addr: SocketAddr,
        out_tx: mpsc::Sender<(NodeId, WireMsg)>,
        config: ClusterConfig,
        topic_counts_fn: Arc<dyn Fn() -> HashMap<String, usize> + Send + Sync>,
    ) -> Self {
        let members = MemberTable::new();
        members.upsert(NodeInfo {
            id: local_id.clone(),
            addr: local_addr,
            state: NodeState::Alive,
            incarnation: 1,
            last_heartbeat: now_ms(),
            topic_counts: HashMap::new(),
        });
        Self {
            local_id,
            local_addr,
            incarnation: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            members,
            out_tx,
            pings: PingTracker::default(),
            config,
            topic_counts_fn,
        }
    }

    /// Return a clone of the member table for sharing with the router.
    pub fn members(&self) -> MemberTable {
        self.members.clone()
    }

    /// Return the local node id.
    pub fn local_id(&self) -> &NodeId {
        &self.local_id
    }

    /// Spawn the gossip event loop. Returns a shutdown handle.
    pub fn spawn(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            self.run().await;
        })
    }

    /// The main gossip loop.
    async fn run(&self) {
        let mut interval = tokio::time::interval(self.config.gossip_interval);
        loop {
            interval.tick().await;
            self.gossip_round().await;
            self.reap_suspects().await;
            self.reap_dead().await;
        }
    }

    /// One gossip round: ping `fanout` random alive members.
    async fn gossip_round(&self) {
        let alive = self.members.alive_members();
        if alive.is_empty() {
            return;
        }
        let fanout = self.config.gossip_fanout.min(alive.len());
        let mut chosen: Vec<NodeInfo> = Vec::with_capacity(fanout);
        {
            let mut pool = alive.clone();
            for _ in 0..fanout {
                if pool.is_empty() {
                    break;
                }
                let idx = rand::rng().random_range(0..pool.len());
                chosen.push(pool.swap_remove(idx));
            }
        }

        for target in &chosen {
            if target.id == self.local_id {
                continue;
            }
            let incarnation = self.incarnation.load(std::sync::atomic::Ordering::SeqCst);
            self.pings.record(&target.id, incarnation);
            let msg = WireMsg::Ping {
                from: self.local_id.clone(),
                incarnation,
            };
            if self.out_tx.send((target.id.clone(), msg)).await.is_err() {
                warn!(target = %target.id, "mesh out channel closed");
                return;
            }
            // Piggyback a MemberUpdate so peers learn our view.
            self.send_member_update(&target.id).await;
        }

        // Check for timed-out pings.
        let timed_out = self
            .pings
            .outstanding(self.config.ping_timeout.as_millis() as i64);
        for target_id in timed_out {
            self.handle_ping_timeout(&target_id).await;
        }
    }

    /// Handle a ping timeout: mark suspect, request indirect probe.
    async fn handle_ping_timeout(&self, target_id: &NodeId) {
        self.pings.clear(target_id);
        if let Some(info) = self.members.get(target_id)
            && info.state == NodeState::Alive
        {
            let mut updated = info.clone();
            updated.state = NodeState::Suspect;
            self.members.upsert(updated);
            debug!(node = %target_id, "node marked suspect");

            // Send PingReq to a few other alive members.
            let peers: Vec<NodeInfo> = self
                .members
                .alive_members()
                .into_iter()
                .filter(|m| m.id != *target_id && m.id != self.local_id)
                .take(3)
                .collect();
            for peer in peers {
                let incarnation = self.incarnation.load(std::sync::atomic::Ordering::SeqCst);
                let msg = WireMsg::PingReq {
                    from: self.local_id.clone(),
                    target: target_id.clone(),
                    incarnation,
                };
                let _ = self.out_tx.send((peer.id, msg)).await;
            }
        }
    }

    /// Promote suspects to dead after `suspect_timeout`.
    async fn reap_suspects(&self) {
        let now = now_ms();
        let cutoff = now - self.config.suspect_timeout.as_millis() as i64;
        for m in self.members.snapshot() {
            if m.state == NodeState::Suspect && m.last_heartbeat < cutoff {
                let id = m.id.clone();
                let mut updated = m;
                updated.state = NodeState::Dead;
                self.members.upsert(updated);
                info!(node = %id, "node marked dead");
            }
        }
    }

    /// Remove dead members after `dead_timeout`.
    async fn reap_dead(&self) {
        let now = now_ms();
        let cutoff = now - self.config.dead_timeout.as_millis() as i64;
        let to_remove: Vec<NodeId> = self
            .members
            .snapshot()
            .into_iter()
            .filter(|m| m.state == NodeState::Dead && m.last_heartbeat < cutoff)
            .map(|m| m.id)
            .collect();
        for id in to_remove {
            self.members.remove(&id);
        }
    }

    /// Send a `MemberUpdate` to a specific peer.
    async fn send_member_update(&self, target_id: &NodeId) {
        // Refresh the local node's topic counts in the member table.
        {
            let counts = (self.topic_counts_fn)();
            if let Some(mut self_info) = self.members.get(&self.local_id) {
                self_info.topic_counts = counts;
                self.members.upsert(self_info);
            }
        }
        let msg = WireMsg::MemberUpdate {
            from: self.local_id.clone(),
            members: self.members.snapshot(),
            version: self.members.version(),
        };
        let _ = self.out_tx.send((target_id.clone(), msg)).await;
    }

    /// Handle an inbound gossip message.
    ///
    /// Called by the cluster's inbound dispatcher when a gossip
    /// message arrives from a peer.
    pub async fn handle_message(&self, from: NodeId, msg: WireMsg) {
        match msg {
            WireMsg::Ping {
                from: peer_id,
                incarnation,
            } => {
                // Record the peer as alive (refresh heartbeat).
                self.refresh_heartbeat(&peer_id, incarnation);
                // Reply with Ack.
                let my_inc = self.incarnation.load(std::sync::atomic::Ordering::SeqCst);
                let ack = WireMsg::Ack {
                    from: self.local_id.clone(),
                    incarnation: my_inc,
                };
                let _ = self.out_tx.send((peer_id, ack)).await;
            }
            WireMsg::Ack {
                from: peer_id,
                incarnation,
            } => {
                self.pings.clear(&peer_id);
                self.refresh_heartbeat(&peer_id, incarnation);
            }
            WireMsg::PingReq {
                from: requester,
                target,
                incarnation,
            } => {
                // Probe `target` on behalf of `requester`.
                self.refresh_heartbeat(&requester, incarnation);
                if let Some(_target_info) = self.members.get(&target) {
                    let my_inc = self.incarnation.load(std::sync::atomic::Ordering::SeqCst);
                    let ping = WireMsg::Ping {
                        from: self.local_id.clone(),
                        incarnation: my_inc,
                    };
                    // Send ping to target. The Ack from target will
                    // be forwarded to the requester via a follow-up
                    // MemberUpdate.
                    let _ = self.out_tx.send((target, ping)).await;
                }
            }
            WireMsg::MemberUpdate {
                from: _,
                members,
                version,
            } => {
                // Merge: only accept updates with higher version.
                if version > self.members.version() {
                    for m in members {
                        self.members.upsert(m);
                    }
                }
            }
            WireMsg::Leave { from: peer_id } => {
                if let Some(info) = self.members.get(&peer_id) {
                    let mut updated = info;
                    updated.state = NodeState::Left;
                    self.members.upsert(updated);
                    info!(node = %peer_id, "node left cluster");
                }
            }
            _ => {
                // Non-gossip messages are handled by the router.
            }
        }
        let _ = from; // already used inside each arm
    }

    /// Refresh a peer's heartbeat and incarnation.
    fn refresh_heartbeat(&self, peer_id: &NodeId, incarnation: u64) {
        if let Some(info) = self.members.get(peer_id) {
            let mut updated = info;
            updated.last_heartbeat = now_ms();
            if incarnation > updated.incarnation {
                updated.incarnation = incarnation;
            }
            if updated.state == NodeState::Suspect || updated.state == NodeState::Dead {
                updated.state = NodeState::Alive;
            }
            self.members.upsert(updated);
        } else {
            // Unknown peer — we don't have its address, so we can't
            // add it. The peer's address comes via MemberUpdate or
            // discovery.
        }
    }

    /// Add a newly-discovered peer to the member table.
    pub fn add_peer(&self, id: NodeId, addr: SocketAddr) {
        self.members.upsert(NodeInfo {
            id,
            addr,
            state: NodeState::Alive,
            incarnation: 1,
            last_heartbeat: now_ms(),
            topic_counts: HashMap::new(),
        });
    }

    /// Graceful shutdown — broadcast Leave to all peers.
    pub async fn shutdown(&self) {
        let leave = WireMsg::Leave {
            from: self.local_id.clone(),
        };
        let alive = self.members.alive_members();
        for peer in alive {
            if peer.id != self.local_id {
                let _ = self.out_tx.send((peer.id.clone(), leave.clone())).await;
            }
        }
    }
}
