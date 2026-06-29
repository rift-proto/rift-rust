//! Cluster node identity and state types.
//!
//! Every node in the cluster has a unique [`NodeId`], a current
//! [`NodeState`], and associated metadata captured in [`NodeInfo`].

use std::net::SocketAddr;
use uuid::Uuid;

/// Unique identifier for a cluster node.
///
/// Generated on first start from a random UUID v4.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeId(pub String);

impl NodeId {
    /// Generate a new random node identifier.
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// Borrow as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl serde::Serialize for NodeId {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(s)
    }
}

impl<'de> serde::Deserialize<'de> for NodeId {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        String::deserialize(d).map(NodeId)
    }
}

/// Lifecycle state of a cluster member.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum NodeState {
    /// The node is reachable and participating in the cluster.
    Alive,
    /// The node failed a direct ping; an indirect probe is in progress.
    Suspect,
    /// The node has been confirmed dead (failed indirect probe or timeout).
    Dead,
    /// The node voluntarily left the cluster.
    Left,
}

/// Information about a cluster member, exchanged during gossip.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NodeInfo {
    /// Unique node identifier.
    pub id: NodeId,
    /// Address the node listens on for cluster-internal TCP connections.
    pub addr: SocketAddr,
    /// Current lifecycle state.
    pub state: NodeState,
    /// Monotonically increasing incarnation counter.
    pub incarnation: u64,
    /// Timestamp (milliseconds since epoch) of the last heartbeat
    /// received from this node.
    pub last_heartbeat: i64,
    /// Per-topic subscriber counts piggybacked on gossip. This is
    /// populated by the node's own `ClusterBroker` before sending
    /// a `MemberUpdate`, enabling approximate global aggregation.
    #[serde(default)]
    pub topic_counts: std::collections::HashMap<String, usize>,
}
