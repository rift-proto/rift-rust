//! Framed TCP protocol for Gateway ↔ Broker communication.
//!
//! Wire format: `[u32 BE len][CBOR-encoded WireMsg]`
//!
//! Used by [`RemoteBroker`](crate::broker::remote_broker::RemoteBroker) to
//! communicate with an external broker node over TCP.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use tokio_util::codec::{Decoder, Encoder};

use crate::broker::broker::PublishOutcome;
use crate::broker::fanout::SubscribeIntent;
use crate::error::{FrameReject, Result, RiftError};
use crate::frame::Frame;
use crate::storage::StoredSnapshot;

/// A message sent from Gateway to Broker (request) or Broker to Gateway (response/push).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WireMsg {
    // ── Requests (Gateway → Broker) ──────────────────────
    /// Publish a frame to a topic.
    Publish {
        frame: Frame,
    },
    /// Subscribe a sink to a topic.
    Subscribe {
        topic: String,
        intent: SubscribeIntent,
        sink_id: u64,
    },
    /// Unsubscribe by subscription id.
    Unsubscribe {
        id: u64,
    },
    /// Drop all subscriptions for a sink.
    DropSink {
        sink_id: u64,
    },
    /// Replay messages in a range.
    Replay {
        topic: String,
        from: i64,
        to: i64,
    },
    /// Fetch a snapshot.
    Snapshot {
        topic: String,
    },
    /// Get subscriber count.
    SubscriberCount {
        topic: String,
    },
    /// Get head offset.
    HeadOffset {
        topic: String,
    },

    // ── Responses (Broker → Gateway) ────────────────────
    PublishResult {
        outcome: PublishOutcome,
    },
    SubscribeResult {
        id: u64,
    },
    UnsubscribeResult {
        ok: bool,
    },
    DropSinkResult {
        count: usize,
    },
    ReplayResult {
        entries: Vec<Bytes>,
    },
    SnapshotResult {
        snapshot: Option<StoredSnapshot>,
    },
    SubscriberCountResult {
        count: usize,
    },
    HeadOffsetResult {
        offset: i64,
    },
    Error {
        code: String,
        message: String,
    },

    // ── Push (Broker → Gateway) ─────────────────────────
    Deliver {
        sink_id: u64,
        topic: String,
        payload: Bytes,
    },
}

// ── Tokio codec ───────────────────────────────────────────

/// Length-prefixed CBOR codec for `WireMsg`.
pub struct WireCodec;

impl Decoder for WireCodec {
    type Item = WireMsg;
    type Error = RiftError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<WireMsg>> {
        if buf.len() < 4 {
            return Ok(None);
        }
        let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
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

impl Encoder<WireMsg> for WireCodec {
    type Error = RiftError;

    fn encode(&mut self, msg: WireMsg, buf: &mut BytesMut) -> Result<()> {
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
        let mut codec = WireCodec;
        let msg = WireMsg::Publish {
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
            WireMsg::Publish { frame } => {
                assert_eq!(frame.topic.as_deref(), Some("t"));
                assert_eq!(frame.message_id.as_deref(), Some("m1"));
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn round_trip_publish_result() {
        let mut codec = WireCodec;
        let msg = WireMsg::PublishResult {
            outcome: PublishOutcome {
                offset: 42,
                duplicate: false,
            },
        };

        let mut buf = BytesMut::new();
        codec.encode(msg, &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap().unwrap();

        match decoded {
            WireMsg::PublishResult { outcome } => {
                assert_eq!(outcome.offset, 42);
                assert!(!outcome.duplicate);
            }
            _ => panic!("wrong variant"),
        }
    }
}
