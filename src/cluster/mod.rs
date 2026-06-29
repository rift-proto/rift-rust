//! TCP mesh cluster — multi-node, zero-dependency (optional Redis) cluster
//! with automatic topology discovery (mDNS + seed nodes) and cross-node
//! message routing.
//!
//! ## Submodules
//!
//! | Module | Purpose |
//! |--------|---------|
//! | [`config`] | Cluster configuration (listen address, seed nodes, intervals) |
//! | [`node`] | Node identity, state, and member info types |
//! | [`connection`] | Mesh TCP connection pool and link state machine |
//! | [`gossip`] | SWIM-based gossip protocol for member management |
//! | [`discovery`] | Node discovery via mDNS and/or seed node list |
//! | [`router`] | Cross-node message routing (fanout + actor forwarding) |
//! | [`broker`] | `ClusterBroker` implementing the `Broker` trait |

pub mod broker;
pub mod config;
pub mod connection;
pub mod discovery;
pub mod gossip;
pub mod node;
pub mod router;
pub mod wire;
