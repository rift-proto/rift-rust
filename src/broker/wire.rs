//! Framed TCP protocol for gateway-to-broker communication.
//!
//! This module defines the wire protocol used between a gateway and an
//! external broker node. The protocol uses a simple length-prefixed
//! CBOR framing scheme:
//!
//! ```text
//! [u32 big-endian byte length][CBOR-encoded GatewayMsg payload]
//! ```
//!
//! Each message is a [`GatewayMsg`] variant that falls into one of three
//! categories:
//!
//! - **Requests** — sent from the gateway to the broker node (e.g.
//!   `Publish`, `Subscribe`, `Replay`).
//! - **Responses** — sent from the broker node back to the gateway in
//!   reply to a request (e.g. `PublishResult`, `ReplayResult`, `Error`).
//! - **Push messages** — sent from the broker to the gateway without a
//!   preceding request (e.g. `Deliver` for live subscriber push).
//!
//! # Tokio codec integration
//!
//! The [`GatewayCodec`] struct implements Tokio's [`Decoder`] and
//! [`Encoder`] traits, allowing it to be used directly with
//! [`tokio_util::codec::Framed`] for non-blocking, async I/O.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use tokio_util::codec::{Decoder, Encoder};

use crate::broker::broker::PublishOutcome;
use crate::broker::fanout::SubscribeIntent;
use crate::error::{FrameReject, Result, RiftError};
use crate::frame::Frame;
use crate::storage::StoredSnapshot;

/// Maximum frame size for gateway-broker messages.
pub const DEFAULT_MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

/// A message in the gateway-to-broker wire protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GatewayMsg {
    // -- Requests (Gateway to Broker) ----------------------------------
    Publish {
        request_id: u32,
        frame: Frame,
    },

    Subscribe {
        request_id: u32,
        topic: String,
        intent: SubscribeIntent,
        sink_id: u64,
    },

    Unsubscribe {
        request_id: u32,
        id: u64,
    },

    DropSink {
        request_id: u32,
        sink_id: u64,
    },

    Replay {
        request_id: u32,
        topic: String,
        from: i64,
        to: i64,
    },

    Snapshot {
        request_id: u32,
        topic: String,
    },

    SubscriberCount {
        request_id: u32,
        topic: String,
    },

    HeadOffset {
        request_id: u32,
        topic: String,
    },

    // -- Responses (Broker to Gateway) ---------------------------------
    PublishResult {
        request_id: u32,
        outcome: PublishOutcome,
    },

    SubscribeResult {
        request_id: u32,
        id: u64,
    },

    UnsubscribeResult {
        request_id: u32,
        ok: bool,
    },

    DropSinkResult {
        request_id: u32,
        count: usize,
    },

    ReplayResult {
        request_id: u32,
        entries: Vec<Bytes>,
    },

    SnapshotResult {
        request_id: u32,
        snapshot: Option<StoredSnapshot>,
    },

    SubscriberCountResult {
        request_id: u32,
        count: usize,
    },

    HeadOffsetResult {
        request_id: u32,
        offset: i64,
    },

    Error {
        request_id: u32,
        code: String,
        message: String,
    },

    // -- Push (Broker to Gateway) --------------------------------------
    Deliver {
        sink_id: u64,
        topic: String,
        payload: Bytes,
    },
}

// -- Tokio codec --------------------------------------------------------

/// Length-prefixed CBOR codec for [`GatewayMsg`] frames.
pub struct GatewayCodec {
    pub max_frame_size: usize,
}

impl Default for GatewayCodec {
    fn default() -> Self {
        Self {
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
        }
    }
}

impl Decoder for GatewayCodec {
    type Item = GatewayMsg;
    type Error = RiftError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<GatewayMsg>> {
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

impl Encoder<GatewayMsg> for GatewayCodec {
    type Error = RiftError;

    fn encode(&mut self, msg: GatewayMsg, buf: &mut BytesMut) -> Result<()> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_publish() {
        let mut codec = GatewayCodec::default();
        let msg = GatewayMsg::Publish {
            request_id: 42,
            frame: Frame {
                topic: Some("t".into()),
                message_id: Some("m1".into()),
                payload: Some(Bytes::from_static(b"hello")),
                ..Frame::default()
            },
        };

        let mut buf = BytesMut::new();
        codec.encode(msg.clone(), &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap().unwrap();

        match decoded {
            GatewayMsg::Publish { request_id, frame } => {
                assert_eq!(request_id, 42);
                assert_eq!(frame.topic.as_deref(), Some("t"));
                assert_eq!(frame.message_id.as_deref(), Some("m1"));
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn round_trip_publish_result() {
        let mut codec = GatewayCodec::default();
        let msg = GatewayMsg::PublishResult {
            request_id: 7,
            outcome: PublishOutcome {
                offset: 42,
                duplicate: false,
            },
        };

        let mut buf = BytesMut::new();
        codec.encode(msg, &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap().unwrap();

        match decoded {
            GatewayMsg::PublishResult {
                request_id,
                outcome,
            } => {
                assert_eq!(request_id, 7);
                assert_eq!(outcome.offset, 42);
                assert!(!outcome.duplicate);
            }
            _ => panic!("wrong variant"),
        }
    }
}
