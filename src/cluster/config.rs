//! Cluster configuration.
//!
//! Defines the tunable parameters for a `rifts` node operating in
//! TCP mesh cluster mode.

use std::time::Duration;

/// Configuration for TCP mesh cluster mode (requires feature `cluster`).
///
/// Set via [`RiftServer::builder()`](crate::RiftServer::builder) when
/// the `cluster` feature is enabled.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// Address this node listens on for inter-node TCP connections.
    /// Other nodes will connect to this address for cluster communication.
    pub listen_addr: std::net::SocketAddr,

    /// Static list of seed node addresses for initial cluster bootstrap.
    /// Format: `"host:port"`. At least one seed is required to join an
    /// existing cluster; a node with no seeds starts a new single-node
    /// cluster and waits for other nodes to connect to it.
    pub seed_nodes: Vec<String>,

    /// Interval between gossip rounds. The node pings one random peer
    /// every `gossip_interval`.
    /// Default: 1 second.
    pub gossip_interval: Duration,

    /// Number of peers to ping per gossip round for faster failure
    /// detection in larger clusters.
    /// Default: 3.
    pub gossip_fanout: usize,

    /// Time to wait for a direct ping response before initiating
    /// an indirect probe.
    /// Default: 1 second.
    pub ping_timeout: Duration,

    /// Time after which a suspected node is marked dead if no
    /// alive confirmation arrives.
    /// Default: 5 seconds.
    pub suspect_timeout: Duration,

    /// Time after which a dead node is removed from the member list.
    /// Default: 60 seconds.
    pub dead_timeout: Duration,

    /// Maximum number of reconnect attempts to a peer.
    /// Default: 10.
    pub max_reconnect_attempts: u32,

    /// Base delay for exponential backoff on reconnect.
    /// Default: 500 ms.
    pub reconnect_base_ms: u64,

    /// Maximum delay for exponential backoff on reconnect.
    /// Default: 30 seconds.
    pub reconnect_max_ms: u64,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:9100".parse().unwrap(),
            seed_nodes: Vec::new(),
            gossip_interval: Duration::from_secs(1),
            gossip_fanout: 3,
            ping_timeout: Duration::from_secs(1),
            suspect_timeout: Duration::from_secs(5),
            dead_timeout: Duration::from_secs(60),
            max_reconnect_attempts: 10,
            reconnect_base_ms: 500,
            reconnect_max_ms: 30_000,
        }
    }
}
