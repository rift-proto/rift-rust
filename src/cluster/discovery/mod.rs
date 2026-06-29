//! Node discovery mechanisms.
//!
//! [`Discovery`] is the trait that yields candidate peer addresses
//! for the local node to connect to. The cluster ships two
//! implementations:
//!
//! - [`SeedDiscovery`] — connects to a static list of seed node
//!   addresses from the configuration.
//! - [`MdnsDiscovery`] — broadcasts a `_rifts-cluster._tcp` mDNS
//!   service and listens for other nodes on the local network.

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::cluster::node::NodeId;

/// A discovered peer candidate.
#[derive(Debug, Clone)]
pub struct PeerCandidate {
    /// The peer's advertised cluster listen address.
    pub addr: SocketAddr,
    /// The peer's node id, if known (mDNS responses include it;
    /// seed-node entries do not until handshake).
    pub node_id: Option<NodeId>,
}

/// The discovery trait — yields a stream of peer candidates.
#[async_trait]
pub trait Discovery: Send + Sync {
    /// Start discovery. Returns a receiver that yields peer
    /// candidates as they are discovered.
    async fn discover(&self) -> crate::error::Result<mpsc::Receiver<PeerCandidate>>;
}

/// Static seed-node discovery.
pub struct SeedDiscovery {
    seeds: Vec<String>,
}

impl SeedDiscovery {
    pub fn new(seeds: Vec<String>) -> Self {
        Self { seeds }
    }

    pub fn resolve(&self) -> Vec<PeerCandidate> {
        self.seeds
            .iter()
            .filter_map(|s| s.parse().ok())
            .map(|addr: SocketAddr| PeerCandidate {
                addr,
                node_id: None,
            })
            .collect()
    }
}

#[async_trait]
impl Discovery for SeedDiscovery {
    async fn discover(&self) -> crate::error::Result<mpsc::Receiver<PeerCandidate>> {
        let (tx, rx) = mpsc::channel(self.seeds.len().max(1));
        for candidate in self.resolve() {
            let _ = tx.send(candidate).await;
        }
        Ok(rx)
    }
}

/// mDNS-based LAN discovery — **removed**.
///
/// mDNS was judged over-engineered for typical deployments. Seed
/// nodes cover cross-subnet bootstrapping; for LAN-only zero-config
/// discovery, users can run a small seed-node list service or use
/// environment variables. This type is kept as a documentation
/// marker only.
#[deprecated(note = "mDNS discovery removed; use SeedDiscovery instead")]
pub struct MdnsDiscovery;

/// Composite discovery — combines multiple discovery sources.
pub struct CompositeDiscovery {
    inner: Vec<Arc<dyn Discovery>>,
}

impl CompositeDiscovery {
    pub fn new(inner: Vec<Arc<dyn Discovery>>) -> Arc<Self> {
        Arc::new(Self { inner })
    }
}

#[async_trait]
impl Discovery for CompositeDiscovery {
    async fn discover(&self) -> crate::error::Result<mpsc::Receiver<PeerCandidate>> {
        let (tx, rx) = mpsc::channel(32);
        for src in &self.inner {
            let src = src.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                let Ok(mut child) = src.discover().await else {
                    return;
                };
                while let Some(c) = child.recv().await {
                    if tx.send(c).await.is_err() {
                        break;
                    }
                }
            });
        }
        Ok(rx)
    }
}
