//! Framed TCP protocol for cluster inter-node communication.
//!
//! This module defines the wire protocol used between cluster nodes.
//! The protocol uses a simple length-prefixed CBOR framing scheme:
//!
//! ```text
//! [u32 big-endian byte length][CBOR-encoded ClusterMsg payload]
//! ```
//!
//! Messages fall into two categories:
//!
//! - **Gossip messages** — SWIM protocol messages (Ping, Ack, PingReq,
//!   MemberUpdate, Leave).
//! - **RPC messages** — cross-node fanout and actor forwarding
//!   (RemoteFanout, ActorForward, ActorForwardResult).

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use tokio_util::codec::{Decoder, Encoder};

use crate::error::{FrameReject, Result, RiftError};

/// Maximum frame size for cluster messages.
pub const DEFAULT_MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

/// A message in the cluster inter-node wire protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterMsg {
    // -- SWIM gossip protocol ------------------------------------------
    /// SWIM gossip ping (direct probe).
    Ping {
        from: crate::cluster::node::NodeId,
        incarnation: u64,
    },

    /// Gossip ping response.
    Ack {
        from: crate::cluster::node::NodeId,
        incarnation: u64,
    },

    /// Indirect probe request: "please ping `target` on my behalf."
    PingReq {
        from: crate::cluster::node::NodeId,
        target: crate::cluster::node::NodeId,
        incarnation: u64,
    },

    /// Membership update (full or delta member list).
    MemberUpdate {
        from: crate::cluster::node::NodeId,
        members: Vec<crate::cluster::node::NodeInfo>,
        version: u64,
    },

    /// A node is voluntarily leaving the cluster.
    Leave {
        from: crate::cluster::node::NodeId,
    },

    // -- Cross-node RPC ------------------------------------------------
    /// Cross-node fanout: deliver a published message to subscribers
    /// on the receiving node.
    RemoteFanout {
        from: crate::cluster::node::NodeId,
        topic: String,
        offset: i64,
        payload: Bytes,
    },

    /// Forward an actor request to the owning node.
    ActorForward {
        request_id: u32,
        topic: String,
        /// Serialized actor message (CBOR-encoded).
        msg: Bytes,
    },

    /// Response to an `ActorForward` request.
    ActorForwardResult {
        request_id: u32,
        result: Bytes,
    },
}

// -- Tokio codec --------------------------------------------------------

/// Length-prefixed CBOR codec for [`ClusterMsg`] frames.
pub struct ClusterCodec {
    pub max_frame_size: usize,
}

impl Default for ClusterCodec {
    fn default() -> Self {
        Self {
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
        }
    }
}

impl Decoder for ClusterCodec {
    type Item = ClusterMsg;
    type Error = RiftError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<ClusterMsg>> {
        if buf.len() < 4 {
            return Ok(None);
        }
        let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        if len > self.max_frame_size {
            return Err(RiftError::Frame(FrameReject::PayloadTooLarge {
                actual: len,
                max: self.max_frame_size,
            }));
        }
        if buf.len() < 4 + len {
            return Ok(None);
        }
        buf.advance(4);
        let payload = buf.split_to(len);
        let msg = ciborium::from_reader(payload.as_ref())
            .map_err(|e| RiftError::Frame(FrameReject::FrameInvalid(e.to_string())))?;
        Ok(Some(msg))
    }
}

impl Encoder<ClusterMsg> for ClusterCodec {
    type Error = RiftError;

    fn encode(&mut self, msg: ClusterMsg, buf: &mut BytesMut) -> Result<()> {
        let mut payload = Vec::new();
        ciborium::into_writer(&msg, &mut payload)
            .map_err(|e| RiftError::Frame(FrameReject::FrameInvalid(e.to_string())))?;
        let len = payload.len() as u32;
        buf.reserve(4 + payload.len());
        buf.put_u32(len);
        buf.extend_from_slice(&payload);
        Ok(())
    }
}
