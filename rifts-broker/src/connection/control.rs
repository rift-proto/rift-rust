//! Control frame dispatch — subscribe, unsubscribe, ping/pong, and
//! error/ack reply helpers.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use tokio::sync::mpsc;
use tracing::warn;

use crate::broker::fanout::{FanoutError, FanoutSink, SubscriptionId, new_sink_id};
use crate::broker::{Broker, SubscribeIntent};
use rifts_core::codec::PayloadCodec;
use rifts_core::error::{Result, RiftError, SystemReject};
use rifts_core::frame::{EncodingFormat as FrameEncodingFormat, Frame, FrameFlags, FrameType};
use rifts_core::metrics::Metrics;
use rifts_session::session::Session;

/// Handle a control frame (ping, subscribe, unsubscribe).
///
/// The control body is expected to be a JSON object with a `"type"` field.
/// Unknown control types are logged and ignored.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_control(
    out_tx: &mpsc::Sender<Frame>,
    broker: &Arc<dyn Broker>,
    frame: &Frame,
    _session: &Arc<Session>,
    codec: &Arc<dyn PayloadCodec>,
    _metrics: &Arc<Metrics>,
    in_flight_bytes: &Arc<AtomicUsize>,
    max_bytes: usize,
    subscription_sinks: &Arc<parking_lot::Mutex<HashSet<u64>>>,
) -> Result<()> {
    let body = frame
        .payload
        .as_ref()
        .and_then(|p| std::str::from_utf8(p).ok())
        .unwrap_or("{}");

    // Propagate JSON parse errors rather than silently swallowing them.
    let v: serde_json::Value = serde_json::from_str(body).map_err(|e| {
        RiftError::Frame(rifts_core::error::FrameReject::FrameInvalid(format!(
            "invalid control body: {e}"
        )))
    })?;
    let ctrl_type = v.get("type").and_then(|x| x.as_str()).unwrap_or("");

    match ctrl_type {
        "ping" => {
            let pong = Frame {
                frame_type: FrameType::Control,
                codec: codec.frame_codec(),
                correlation_id: frame.correlation_id.clone(),
                ..Frame::default()
            };
            out_tx
                .try_send(pong)
                .map_err(|_| RiftError::System(SystemReject::Overloaded))?;
            Ok(())
        }
        "subscribe" => {
            let topic = v
                .get("topic")
                .and_then(|x| x.as_str())
                .ok_or_else(|| {
                    RiftError::Frame(rifts_core::error::FrameReject::RequiredFieldMissing(
                        "topic",
                    ))
                })?
                .to_string();
            let intent_str = v.get("intent").and_then(|x| x.as_str()).unwrap_or("live");
            let intent = match intent_str {
                "live" => SubscribeIntent::Live,
                "passive" => SubscribeIntent::Passive,
                "ephemeral" => SubscribeIntent::Ephemeral,
                "latest" => SubscribeIntent::Latest,
                "snapshot_then_live" => SubscribeIntent::SnapshotThenLive,
                "replay" => {
                    let from = v.get("from_offset").and_then(|x| x.as_i64()).unwrap_or(0);
                    SubscribeIntent::Replay { from }
                }
                other => {
                    return Err(RiftError::Frame(
                        rifts_core::error::FrameReject::FrameInvalid(format!(
                            "unknown subscribe intent: {other}"
                        )),
                    ));
                }
            };
            let sink_id = new_sink_id();
            let sink: Arc<dyn FanoutSink> = Arc::new(MpscSink {
                tx: out_tx.clone(),
                in_flight_bytes: in_flight_bytes.clone(),
                max_bytes,
                id: sink_id,
                codec: codec.frame_codec(),
            });
            let id = broker.subscribe(&topic, intent, sink).await?;
            // Track this per-subscription sink so teardown can clean
            // it up and prevent the FanoutEngine subscription leak.
            subscription_sinks.lock().insert(sink_id);
            let reply = serde_json::json!({
                "type": "subscribe_ack",
                "topic": topic,
                "subscription_id": id.0,
                "result": "accepted",
            });
            out_tx
                .try_send(Frame {
                    frame_type: FrameType::Control,
                    codec: codec.frame_codec(),
                    correlation_id: frame.correlation_id.clone(),
                    payload: Some(Bytes::from(reply.to_string())),
                    ..Frame::default()
                })
                .map_err(|_| RiftError::System(SystemReject::Overloaded))?;
            Ok(())
        }
        "unsubscribe" => {
            let sub_id = v
                .get("subscription_id")
                .and_then(|x| x.as_u64())
                .ok_or_else(|| {
                    RiftError::Frame(rifts_core::error::FrameReject::RequiredFieldMissing(
                        "subscription_id",
                    ))
                })?;
            let removed = broker.unsubscribe(SubscriptionId(sub_id)).await?;
            let reply = serde_json::json!({
                "type": "unsubscribe_ack",
                "subscription_id": sub_id,
                "result": if removed { "removed" } else { "not_found" },
            });
            out_tx
                .try_send(Frame {
                    frame_type: FrameType::Control,
                    codec: codec.frame_codec(),
                    correlation_id: frame.correlation_id.clone(),
                    payload: Some(Bytes::from(reply.to_string())),
                    ..Frame::default()
                })
                .map_err(|_| RiftError::System(SystemReject::Overloaded))?;
            Ok(())
        }
        _ => {
            warn!("unknown control type: {}", ctrl_type);
            Ok(())
        }
    }
}

/// A minimal fanout sink used by the subscribe control handler.
///
/// Unlike [`ConnSink`](crate::broker::fanout::ConnSink) (which is created
/// once per connection and tracks metrics), `MpscSink` is created per-subscription
/// and only enforces backpressure without incrementing metrics.
struct MpscSink {
    /// Sender half of the connection's outbound frame channel.
    tx: mpsc::Sender<Frame>,
    /// Atomic counter of bytes currently in the outbound queue.
    in_flight_bytes: Arc<AtomicUsize>,
    /// Maximum allowed bytes in the outbound queue.
    max_bytes: usize,
    /// Unique sink id for this subscription.
    id: u64,
    /// Codec negotiated during the Hello handshake.
    codec: FrameEncodingFormat,
}

impl FanoutSink for MpscSink {
    /// Attempt to deliver a serialized message frame to the connection's
    /// outbound queue.
    ///
    /// Returns `Err(FanoutError::Backpressured)` if the queue is at capacity
    /// or if the mpsc channel is full. Returns `Err(FanoutError::Closed)` if
    /// the channel has been closed.
    fn deliver(&self, frame: bytes::Bytes) -> std::result::Result<(), FanoutError> {
        let len = frame.len();
        // Atomic CAS reserve, same as ConnSink.
        let mut prev = self.in_flight_bytes.load(Ordering::Acquire);
        loop {
            if prev + len > self.max_bytes {
                return Err(FanoutError::Backpressured {
                    queue_bytes: prev,
                    max_bytes: self.max_bytes,
                });
            }
            match self.in_flight_bytes.compare_exchange_weak(
                prev,
                prev + len,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(current) => prev = current,
            }
        }
        let f = Frame {
            frame_type: FrameType::Data,
            codec: self.codec,
            payload: Some(frame),
            flags: FrameFlags::empty(),
            ..Frame::default()
        };
        match self.tx.try_send(f) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.in_flight_bytes.fetch_sub(len, Ordering::AcqRel);
                Err(FanoutError::Backpressured {
                    queue_bytes: self.in_flight_bytes.load(Ordering::SeqCst),
                    max_bytes: self.max_bytes,
                })
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.in_flight_bytes.fetch_sub(len, Ordering::AcqRel);
                Err(FanoutError::Closed)
            }
        }
    }
    fn id(&self) -> u64 {
        self.id
    }
}

/// Send an error frame to the client in response to a failed control
/// frame.
///
/// The error frame contains a JSON body with the error code, message,
/// and the original frame's `correlation_id`.
pub(crate) async fn send_error_frame(
    out_tx: &mpsc::Sender<Frame>,
    original: &Frame,
    err: RiftError,
) -> Result<()> {
    let code = rift_error_to_code(&err);
    let body = serde_json::json!({
        "code": code,
        "message": err.to_string(),
        "correlation_id": original.correlation_id,
    });
    out_tx
        .try_send(Frame {
            frame_type: FrameType::Error,
            codec: FrameEncodingFormat::Json,
            correlation_id: original.correlation_id.clone(),
            payload: Some(Bytes::from(body.to_string())),
            ..Frame::default()
        })
        .map_err(|_| RiftError::System(SystemReject::Overloaded))
}

/// Map a [`RiftError`] variant to the closest wire-level error code
/// identifier string.
///
/// This is used when constructing error frames to send to the client.
/// The mapping follows the error code table defined in the Rift protocol
/// specification.
fn rift_error_to_code(err: &RiftError) -> &'static str {
    use rifts_core::protocol::error_code::ErrorCode;
    match err {
        RiftError::Frame(fe) => match fe {
            rifts_core::error::FrameReject::ProtocolVersionUnsupported { .. } => {
                ErrorCode::ProtocolVersionUnsupported.as_str()
            }
            rifts_core::error::FrameReject::FrameInvalid(_) => {
                ErrorCode::ProtocolFrameInvalid.as_str()
            }
            rifts_core::error::FrameReject::CodecUnsupported(_) => {
                ErrorCode::ProtocolCodecUnsupported.as_str()
            }
            rifts_core::error::FrameReject::PayloadTooLarge { .. } => {
                ErrorCode::ProtocolPayloadTooLarge.as_str()
            }
            rifts_core::error::FrameReject::RequiredFieldMissing(_) => {
                ErrorCode::ProtocolRequiredFieldMissing.as_str()
            }
            rifts_core::error::FrameReject::SchemaMismatch(_) => {
                ErrorCode::ProtocolSchemaMismatch.as_str()
            }
            rifts_core::error::FrameReject::OrderViolation(_) => {
                ErrorCode::ProtocolOrderViolation.as_str()
            }
        },
        RiftError::Session(se) => match se {
            rifts_core::error::SessionReject::NotFound(_) => ErrorCode::SessionNotFound.as_str(),
            rifts_core::error::SessionReject::Expired => ErrorCode::SessionExpired.as_str(),
            rifts_core::error::SessionReject::Closed => ErrorCode::SessionExpired.as_str(),
            rifts_core::error::SessionReject::Conflict { .. } => {
                ErrorCode::SessionConflict.as_str()
            }
            rifts_core::error::SessionReject::ResumeRejected(_) => {
                ErrorCode::ResumeRejected.as_str()
            }
            rifts_core::error::SessionReject::ReplayOffsetExpired { .. } => {
                ErrorCode::ReplayOffsetExpired.as_str()
            }
            rifts_core::error::SessionReject::SnapshotRequired(_) => {
                ErrorCode::SnapshotRequired.as_str()
            }
            rifts_core::error::SessionReject::IdleTimeout => ErrorCode::SessionExpired.as_str(),
        },
        RiftError::Topic(te) => match te {
            rifts_core::error::TopicReject::NotFound(_) => ErrorCode::TopicNotFound.as_str(),
            rifts_core::error::TopicReject::Closed(_) => ErrorCode::TopicClosed.as_str(),
            rifts_core::error::TopicReject::Overloaded(_) => ErrorCode::TopicOverloaded.as_str(),
            rifts_core::error::TopicReject::SubscriberLimit(_) => {
                ErrorCode::TopicSubscriberLimit.as_str()
            }
            rifts_core::error::TopicReject::PublisherLimit(_) => {
                ErrorCode::TopicPublisherLimit.as_str()
            }
            rifts_core::error::TopicReject::Forbidden(_) => ErrorCode::TopicForbidden.as_str(),
            rifts_core::error::TopicReject::RateLimited(_) => ErrorCode::TopicRateLimited.as_str(),
            rifts_core::error::TopicReject::InvalidName(_) => {
                ErrorCode::ProtocolFrameInvalid.as_str()
            }
        },
        RiftError::Auth(ae) => match ae {
            rifts_core::error::AuthReject::Required => ErrorCode::AuthRequired.as_str(),
            rifts_core::error::AuthReject::Invalid(_) => ErrorCode::AuthInvalid.as_str(),
            rifts_core::error::AuthReject::Expired => ErrorCode::AuthExpired.as_str(),
            rifts_core::error::AuthReject::Revoked => ErrorCode::AuthRevoked.as_str(),
            rifts_core::error::AuthReject::Denied(_) => ErrorCode::PermissionDenied.as_str(),
        },
        RiftError::Message(me) => match me {
            rifts_core::error::MessageReject::Duplicate(_) => ErrorCode::MessageDuplicate.as_str(),
            rifts_core::error::MessageReject::Expired => ErrorCode::MessageExpired.as_str(),
            rifts_core::error::MessageReject::Rejected(_) => ErrorCode::MessageRejected.as_str(),
            rifts_core::error::MessageReject::TooLarge { .. } => {
                ErrorCode::MessageTooLarge.as_str()
            }
            rifts_core::error::MessageReject::AckTimeout(_) => {
                ErrorCode::MessageAckTimeout.as_str()
            }
            rifts_core::error::MessageReject::DeliveryFailed(_) => {
                ErrorCode::MessageDeliveryFailed.as_str()
            }
        },
        RiftError::System(se) => match se {
            rifts_core::error::SystemReject::Overloaded => ErrorCode::SystemOverloaded.as_str(),
            rifts_core::error::SystemReject::Maintenance => ErrorCode::SystemMaintenance.as_str(),
            rifts_core::error::SystemReject::ShardMoved(_) => ErrorCode::SystemShardMoved.as_str(),
            rifts_core::error::SystemReject::RegionUnavailable(_) => {
                ErrorCode::SystemRegionUnavailable.as_str()
            }
            rifts_core::error::SystemReject::Internal(_) => ErrorCode::SystemInternal.as_str(),
        },
        _ => ErrorCode::SystemInternal.as_str(),
    }
}

/// Build and send an acknowledgement frame to the client.
///
/// The ack body is a JSON object containing the ack id, message id,
/// status, offset, reason, and server timestamp.
pub(crate) async fn send_ack_frame(
    out_tx: &mpsc::Sender<Frame>,
    ack: rifts_core::ack::Ack,
    codec: &Arc<dyn PayloadCodec>,
) -> Result<()> {
    let body = serde_json::to_vec(&serde_json::json!({
        "ack_id": ack.ack_id,
        "message_id": ack.message_id,
        "status": ack.status.as_str(),
        "offset": ack.offset,
        "reason": ack.reason,
        "server_time": ack.server_time,
    }))?;
    out_tx
        .try_send(Frame {
            frame_type: FrameType::Ack,
            codec: codec.frame_codec(),
            payload: Some(Bytes::from(body)),
            ..Frame::default()
        })
        .map_err(|_| RiftError::System(SystemReject::Overloaded))
}
