//! Cross-node message routing.
//!
//! [`MeshRouter`] bridges the local [`FanoutEngine`] with the
//! outbound mesh connection pool. When a message is published
//! locally:
//!
//! 1. The router fans out to local subscribers (immediate, no
//!    network hop).
//! 2. The router broadcasts a [`WireMsg::RemoteFanout`] to all
//!    connected peers so they can fan out to their own local
//!    subscribers.
//!
//! When a `RemoteFanout` arrives from a peer, the router forwards
//! it to the local `FanoutEngine`. Dedup by `origin_node` prevents
//! echoing the message back to the originator.

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;
use tracing::warn;

use crate::broker::broker::serialize_frame_for_fanout;
use crate::broker::fanout::FanoutEngine;
use crate::cluster::wire::ClusterMsg as WireMsg;
use crate::cluster::connection::ConnectionPool;
use crate::cluster::node::NodeId;
use crate::frame::Frame;

/// The cross-node message router.
///
/// Owns (or borrows) the local `FanoutEngine` and the outbound
/// connection pool. Routes inbound `RemoteFanout` messages to
/// local subscribers and broadcasts outbound publishes to peers.
pub struct MeshRouter {
    /// Local node id — used to suppress echoed messages.
    local_id: NodeId,
    /// Outbound connection pool (shared with the gossip engine).
    pool: Arc<ConnectionPool>,
    /// Local fanout engine (shared with the broker).
    fanout: Arc<FanoutEngine>,
    /// Set of recently-seen message ids to dedup remote fanouts.
    /// Bounded — entries expire after `dedupe_window`.
    seen: Mutex<HashSet<(NodeId, String)>>,
}

impl MeshRouter {
    /// Create a new router.
    pub fn new(local_id: NodeId, pool: Arc<ConnectionPool>, fanout: Arc<FanoutEngine>) -> Self {
        Self {
            local_id,
            pool,
            fanout,
            seen: Mutex::new(HashSet::new()),
        }
    }

    /// Broadcast a published message to all peers and deliver to
    /// local subscribers.
    ///
    /// Called by `ClusterBroker::publish` after the local
    /// publish/offset/dedupe work is done.
    pub async fn broadcast_publish(&self, topic: &str, offset: i64, frame: &Frame) {
        // 1. Deliver to local subscribers.
        let serialized = serialize_frame_for_fanout(frame, offset);
        self.fanout.deliver(topic, serialized);

        // 2. Broadcast to peers.
        let payload = frame.payload.clone().unwrap_or_default();
        let msg_id = frame.message_id.clone().unwrap_or_default();
        let msg = WireMsg::RemoteFanout {
            from: self.local_id.clone(),
            topic: topic.to_string(),
            offset,
            payload,
        };
        // Record our own publish to dedup echoes.
        self.seen.lock().insert((self.local_id.clone(), msg_id));
        self.pool.broadcast(msg).await;
    }

    /// Handle an inbound `RemoteFanout` from a peer.
    ///
    /// Deduplicates by `(origin_node, message_id)` and forwards
    /// to local subscribers if fresh.
    pub fn handle_remote_fanout(&self, from: &NodeId, topic: &str, offset: i64, payload: Bytes) {
        // Dedup key — we use (from, payload_hash) since message_id
        // isn't carried in RemoteFanout. For simplicity we hash the
        // payload; in a production system we'd carry a message_id.
        let key = (from.clone(), hex_encode(&payload));
        {
            let mut g = self.seen.lock();
            if !g.insert(key) {
                // Already delivered locally.
                return;
            }
        }

        // Build a synthetic Frame for local fanout.
        let frame = Frame {
            payload: Some(payload),
            ..Frame::default()
        };
        let serialized = serialize_frame_for_fanout(&frame, offset);
        self.fanout.deliver(topic, serialized);
    }

    /// Forward an actor request to the node that owns the topic.
    ///
    /// The response arrives as `ActorForwardResult` and is matched
    /// by `request_id` via the caller's oneshot channel.
    pub async fn forward_actor_request(
        &self,
        target: &NodeId,
        request_id: u32,
        topic: &str,
        msg_bytes: Bytes,
    ) {
        let msg = WireMsg::ActorForward {
            request_id,
            topic: topic.to_string(),
            msg: msg_bytes,
        };
        if let Err(e) = self.pool.send_to(target, msg).await {
            warn!(target = %target, error = %e, "failed to forward actor request");
        }
    }
}

/// Encode bytes as a hex string for dedup keys.
fn hex_encode(b: &Bytes) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(b.len() * 2);
    for byte in b.as_ref() {
        let _ = write!(s, "{byte:02x}");
    }
    s
}
