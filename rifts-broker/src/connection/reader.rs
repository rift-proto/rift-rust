//! Reader task — reads frames from the transport and dispatches them by
//! frame type.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tracing::{debug, warn};

use crate::broker::Broker;
use rifts_core::ack::{Ack, AckStatus, SharedAckManager};
use rifts_core::codec::PayloadCodec;
use rifts_core::error::{Result, RiftError};
use rifts_core::frame::{Frame, FrameType};
use rifts_core::metrics::Metrics;
use rifts_core::now_ms;
use rifts_session::session::Session;
use rifts_transport::transport::TransportConnection;

use super::control::{handle_control, send_ack_frame, send_error_frame};

/// Reader task — reads frames from the transport and dispatches them to
/// the appropriate handler based on frame type.
///
/// - **Control frames** are routed to [`handle_control`].
/// - **Data frames** are published to the broker, with TTL checks and
///   acknowledgement generation.
/// - **Ack frames** complete outstanding ack tracking.
/// - **Flow frames** are logged (currently a no-op beyond logging).
/// - **Error frames** are logged as warnings.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn reader_task(
    transport_slot: Arc<AsyncMutex<Option<Box<dyn TransportConnection>>>>,
    session: Arc<Session>,
    broker: Arc<dyn Broker>,
    ack_manager: SharedAckManager,
    metrics: Arc<Metrics>,
    codec: Arc<dyn PayloadCodec>,
    out_tx: mpsc::Sender<Frame>,
    in_flight_bytes: Arc<AtomicUsize>,
    max_bytes: usize,
    published_topics: Arc<parking_lot::Mutex<HashSet<String>>>,
    subscription_sinks: Arc<parking_lot::Mutex<HashSet<u64>>>,
    conn_id: u64,
    idle_timeout: Duration,
) -> Result<()> {
    loop {
        let frame = {
            let read_fut = async {
                let mut guard = transport_slot.lock().await;
                let Some(transport) = guard.as_mut() else {
                    // Writer released the transport — connection is done.
                    return Err(RiftError::System(
                        rifts_core::error::SystemReject::Internal("transport released".into()),
                    ));
                };
                transport.read_frame().await
            };

            match tokio::time::timeout(idle_timeout, read_fut).await {
                Ok(Ok(f)) => f,
                Ok(Err(RiftError::Session(rifts_core::error::SessionReject::Expired))) => {
                    debug!(conn = conn_id, "peer closed (session expired)");
                    return Ok(());
                }
                Ok(Err(e)) => return Err(e),
                Err(_elapsed) => {
                    debug!(
                        conn = conn_id,
                        timeout_secs = idle_timeout.as_secs(),
                        "connection idle timeout"
                    );
                    metrics.inc(&metrics.connection_close_total);
                    return Err(RiftError::Session(
                        rifts_core::error::SessionReject::IdleTimeout,
                    ));
                }
            }
        };
        metrics.inc(&metrics.messages_in_total);
        session.touch();

        match frame.frame_type {
            FrameType::Control => {
                if let Err(e) = handle_control(
                    &out_tx,
                    &broker,
                    &frame,
                    &session,
                    &codec,
                    &metrics,
                    &in_flight_bytes,
                    max_bytes,
                    &subscription_sinks,
                )
                .await
                {
                    warn!(conn = conn_id, "control error: {}", e);
                    let _ = send_error_frame(&out_tx, &frame, e).await;
                }
            }
            FrameType::Data => {
                // TTL check before dispatching to broker.
                if let Some(ttl) = frame.ttl_ms
                    && frame.timestamp > 0
                    && now_ms() - frame.timestamp > ttl as i64
                {
                    debug!(conn = conn_id, "message expired (TTL)");
                    metrics.inc(&metrics.messages_expired_total);
                    continue;
                }
                let requires_ack = frame.requires_ack();
                let msg_id = frame.message_id.clone();
                match broker.publish(&frame).await {
                    Ok(outcome) => {
                        // Track this topic so the connection can release
                        // its publisher slot on close.
                        if let Some(t) = frame.topic.as_ref() {
                            published_topics.lock().insert(t.clone());
                        }
                        if requires_ack {
                            let status = if outcome.duplicate {
                                AckStatus::Duplicate
                            } else {
                                AckStatus::Persisted
                            };
                            let ack = Ack::new(msg_id.unwrap_or_default(), status)
                                .with_offset(outcome.offset);
                            let _ = send_ack_frame(&out_tx, ack, &codec).await;
                        }
                    }
                    Err(e) => {
                        if requires_ack {
                            let ack = Ack::new(msg_id.unwrap_or_default(), AckStatus::Failed)
                                .with_reason(e.to_string());
                            let _ = send_ack_frame(&out_tx, ack, &codec).await;
                        }
                    }
                }
            }
            FrameType::Ack => {
                let msg = frame.message_id.as_deref().unwrap_or("");
                let _ = ack_manager.complete(session.id.as_str(), msg);
            }
            FrameType::Flow => {
                debug!(conn = conn_id, "flow frame: {:?}", frame.payload);
            }
            FrameType::Error => {
                warn!(conn = conn_id, "peer error: {:?}", frame.payload);
            }
        }
    }
}
