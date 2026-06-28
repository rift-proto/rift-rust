//! Framed TCP protocol for gateway-to-broker communication.
//!
//! This module defines the wire protocol used between a gateway (client)
//! and an external broker node. The protocol uses a simple length-prefixed
//! CBOR framing scheme:
//!
//! ```text
//! [u32 big-endian byte length][CBOR-encoded WireMsg payload]
//! ```
//!
//! Each message is a [`WireMsg`] variant that falls into one of three
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
//! The [`WireCodec`] struct implements Tokio's [`Decoder`] and
//! [`Encoder`] traits, allowing it to be used directly with
//! [`tokio_util::codec::Framed`] for non-blocking, async I/O. The
//! codec handles buffering, partial reads, and frame boundary
//! detection transparently.
//!
//! # Serialization
//!
//! Messages are serialized to CBOR using the [`ciborium`] crate, which
//! provides a `serde`-based CBOR encoder/decoder. All [`WireMsg`]
//! variants derive [`Serialize`] and [`Deserialize`] for this purpose.
//!
//! # Usage
//!
//! Used by [`RemoteBroker`](crate::broker::remote_broker::RemoteBroker)
//! to communicate with an external broker node over TCP.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use tokio_util::codec::{Decoder, Encoder};

use crate::broker::broker::PublishOutcome;
use crate::broker::fanout::SubscribeIntent;
use crate::error::{FrameReject, Result, RiftError};
use crate::frame::Frame;
use crate::storage::StoredSnapshot;

/// A message in the gateway-to-broker wire protocol.
///
/// Variants are split into three groups:
///
/// - **Requests** (gateway to broker): `Publish`, `Subscribe`,
///   `Unsubscribe`, `DropSink`, `Replay`, `Snapshot`,
///   `SubscriberCount`, `HeadOffset`.
/// - **Responses** (broker to gateway): `PublishResult`,
///   `SubscribeResult`, `UnsubscribeResult`, `DropSinkResult`,
///   `ReplayResult`, `SnapshotResult`, `SubscriberCountResult`,
///   `HeadOffsetResult`, `Error`.
/// - **Push messages** (broker to gateway): `Deliver`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WireMsg {
    // -- Requests (Gateway to Broker) ----------------------------------
    /// Publish a frame to the topic specified within the frame.
    ///
    /// The broker validates, deduplicates, offsets, logs, and fans
    /// out the message. The response is a [`PublishResult`](WireMsg::PublishResult)
    /// or an [`Error`](WireMsg::Error).
    Publish {
        /// The frame to publish, containing topic, message ID,
        /// payload, and metadata.
        frame: Frame,
    },

    /// Subscribe a sink to a topic.
    ///
    /// Requests the broker to begin delivering messages for the given
    /// topic to the identified sink. The response is a
    /// [`SubscribeResult`](WireMsg::SubscribeResult) with the
    /// allocated subscription ID, or an [`Error`](WireMsg::Error).
    Subscribe {
        /// The topic name to subscribe to.
        topic: String,
        /// The subscriber's delivery preference (live, replay,
        /// snapshot-then-live, etc.).
        intent: SubscribeIntent,
        /// Identifier for the sink that will receive pushed messages.
        /// The broker uses this ID in subsequent [`Deliver`](WireMsg::Deliver)
        /// messages.
        sink_id: u64,
    },

    /// Unsubscribe by subscription ID.
    ///
    /// Cancels a previously created subscription. The response is an
    /// [`UnsubscribeResult`](WireMsg::UnsubscribeResult) indicating
    /// whether the subscription existed.
    Unsubscribe {
        /// The subscription ID to cancel, as returned by
        /// [`SubscribeResult`](WireMsg::SubscribeResult).
        id: u64,
    },

    /// Drop all subscriptions belonging to a particular sink.
    ///
    /// Typically sent when a client connection is closed and all of
    /// its subscriptions should be cleaned up. The response is a
    /// [`DropSinkResult`](WireMsg::DropSinkResult) with the count of
    /// removed subscriptions.
    DropSink {
        /// The sink identifier whose subscriptions should be removed.
        sink_id: u64,
    },

    /// Replay historical messages for a topic within an offset range.
    ///
    /// Returns messages with offsets in the inclusive range
    /// `[from, to]`. The response is a
    /// [`ReplayResult`](WireMsg::ReplayResult) containing the payload
    /// bytes, or an [`Error`](WireMsg::Error).
    Replay {
        /// The topic name to replay from.
        topic: String,
        /// Inclusive start offset.
        from: i64,
        /// Inclusive end offset.
        to: i64,
    },

    /// Fetch the most recent snapshot for a topic.
    ///
    /// The response is a [`SnapshotResult`](WireMsg::SnapshotResult)
    /// with an optional snapshot, or an [`Error`](WireMsg::Error).
    Snapshot {
        /// The topic name to snapshot.
        topic: String,
    },

    /// Query the number of active subscribers for a topic.
    ///
    /// The response is a
    /// [`SubscriberCountResult`](WireMsg::SubscriberCountResult).
    SubscriberCount {
        /// The topic name to query.
        topic: String,
    },

    /// Query the current head (highest allocated) offset for a topic.
    ///
    /// The response is a
    /// [`HeadOffsetResult`](WireMsg::HeadOffsetResult).
    HeadOffset {
        /// The topic name to query.
        topic: String,
    },

    // -- Responses (Broker to Gateway) ---------------------------------
    /// Response to a [`Publish`](WireMsg::Publish) request.
    ///
    /// Contains the outcome of the publish operation, including the
    /// assigned offset and whether the message was a duplicate.
    PublishResult {
        /// The publish outcome with offset and duplicate flag.
        outcome: PublishOutcome,
    },

    /// Response to a [`Subscribe`](WireMsg::Subscribe) request.
    ///
    /// Contains the newly allocated subscription ID.
    SubscribeResult {
        /// The subscription ID for the newly created subscription.
        id: u64,
    },

    /// Response to an [`Unsubscribe`](WireMsg::Unsubscribe) request.
    ///
    /// Indicates whether the subscription was found and removed.
    UnsubscribeResult {
        /// `true` if the subscription existed and was removed, `false`
        /// if it was not found.
        ok: bool,
    },

    /// Response to a [`DropSink`](WireMsg::DropSink) request.
    ///
    /// Contains the number of subscriptions that were removed.
    DropSinkResult {
        /// The number of subscriptions that were removed.
        count: usize,
    },

    /// Response to a [`Replay`](WireMsg::Replay) request.
    ///
    /// Contains the payload bytes for each message in the requested
    /// offset range.
    ReplayResult {
        /// The payload bytes for each replayed message, in offset
        /// order.
        entries: Vec<Bytes>,
    },

    /// Response to a [`Snapshot`](WireMsg::Snapshot) request.
    ///
    /// Contains the snapshot if one exists (and has not expired), or
    /// `None` if no snapshot is available.
    SnapshotResult {
        /// The requested snapshot, or `None` if unavailable.
        snapshot: Option<StoredSnapshot>,
    },

    /// Response to a [`SubscriberCount`](WireMsg::SubscriberCount)
    /// request.
    ///
    /// Contains the number of active subscribers for the topic.
    SubscriberCountResult {
        /// The subscriber count for the queried topic.
        count: usize,
    },

    /// Response to a [`HeadOffset`](WireMsg::HeadOffset) request.
    ///
    /// Contains the highest allocated offset for the topic.
    HeadOffsetResult {
        /// The head offset for the queried topic.
        offset: i64,
    },

    /// A generic error response for any request that fails.
    ///
    /// Contains a machine-readable error code and a human-readable
    /// error message.
    Error {
        /// A short, machine-readable error code (e.g. "TOPIC_NOT_FOUND").
        code: String,
        /// A human-readable description of the error.
        message: String,
    },

    // -- Push (Broker to Gateway) --------------------------------------
    /// Push a message to a connected subscriber.
    ///
    /// Sent by the broker when a new message is published to a topic
    /// that the gateway has active subscriptions for. The `sink_id`
    /// identifies which subscriber should receive the payload.
    Deliver {
        /// The sink identifier that should receive this push.
        sink_id: u64,
        /// The topic the message was published to.
        topic: String,
        /// The serialized message payload to deliver.
        payload: Bytes,
    },
}

// -- Tokio codec --------------------------------------------------------

/// Length-prefixed CBOR codec for [`WireMsg`] frames.
///
/// Implements Tokio's [`Decoder`] and [`Encoder`] traits for use with
/// [`tokio_util::codec::Framed`]. The wire format is:
///
/// ```text
/// [u32 big-endian byte count][CBOR-encoded WireMsg]
/// ```
///
/// The codec handles partial reads (buffering until a full frame is
/// available) and frame boundary detection. It returns
/// [`RiftError::Frame`] with [`FrameReject::FrameInvalid`] if the
/// CBOR payload cannot be deserialized.
pub const DEFAULT_MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;
pub struct WireCodec {
    /// Maximum allowed payload size in bytes. Frames declaring a
    /// length greater than this are rejected with `PayloadTooLarge`.
    pub max_frame_size: usize,
}

impl Default for WireCodec {
    fn default() -> Self {
        Self {
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
        }
    }
}

impl Decoder for WireCodec {
    type Item = WireMsg;
    type Error = RiftError;

    /// Decode a single [`WireMsg`] from the buffer.
    ///
    /// Returns `Ok(None)` if insufficient data is available to
    /// determine the frame length or to read the full payload.
    /// Returns `Ok(Some(msg))` when a complete frame has been decoded.
    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<WireMsg>> {
        if buf.len() < 4 {
            return Ok(None);
        }
        let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        // Reject oversized frames before allocating. A malicious peer
        // could otherwise send a length prefix up to 4 GiB and force a
        // large allocation once the data arrived.
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

impl Encoder<WireMsg> for WireCodec {
    type Error = RiftError;

    /// Encode a [`WireMsg`] into the buffer with a 4-byte length
    /// prefix.
    ///
    /// Serializes the message to CBOR, writes the byte count as a
    /// big-endian `u32`, then writes the CBOR payload. The buffer is
    /// reserved to the exact required size before writing.
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
        let mut codec = WireCodec::default();
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
        let mut codec = WireCodec::default();
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
