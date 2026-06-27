//! Per-connection state machine — drives the Rift/1 connection
//! lifecycle (spec §5).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tracing::{debug, warn};

use crate::ack::{Ack, AckStatus, SharedAckManager};
use crate::broker::fanout::{FanoutError, FanoutSink};
use crate::broker::{Broker, SubscribeIntent};
use crate::codec::{CborCodec, Codec, JsonCodec, negotiate};
use crate::config::ServerConfig;
use crate::error::{Result, RiftError, SystemReject};
use crate::flow::BackpressureController;
use crate::frame::{Codec as FrameCodec, Frame, FrameFlags, FrameType};
use crate::metrics::Metrics;
use crate::now_ms;
use crate::protocol::close::CloseCode;
use crate::protocol::hello::{AuthMode, Hello};
use crate::session::resume::ResumeManager;
use crate::session::{AuthProvider, Session, SessionId, SessionState};
use crate::transport::TransportConnection;

/// Lightweight fanout sink that writes inbound frames to the
/// connection's outbound mpsc.
struct ConnSink {
    tx: mpsc::Sender<Frame>,
    in_flight_bytes: Arc<AtomicUsize>,
    max_bytes: usize,
    metrics: Arc<Metrics>,
    sink_id: u64,
}

impl FanoutSink for ConnSink {
    fn deliver(&self, frame: bytes::Bytes) -> std::result::Result<(), FanoutError> {
        let len = frame.len();
        if self.in_flight_bytes.load(Ordering::SeqCst) + len > self.max_bytes {
            return Err(FanoutError::Backpressured {
                queue_bytes: self.in_flight_bytes.load(Ordering::SeqCst),
                max_bytes: self.max_bytes,
            });
        }
        let f = Frame {
            frame_type: FrameType::Data,
            codec: FrameCodec::Json,
            payload: Some(frame),
            flags: FrameFlags::empty(),
            ..Frame::default()
        };
        match self.tx.try_send(f) {
            Ok(()) => {
                self.in_flight_bytes.fetch_add(len, Ordering::SeqCst);
                self.metrics.inc(&self.metrics.messages_out_total);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(_)) => Err(FanoutError::Backpressured {
                queue_bytes: self.in_flight_bytes.load(Ordering::SeqCst),
                max_bytes: self.max_bytes,
            }),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(FanoutError::Closed),
        }
    }
    fn id(&self) -> u64 {
        self.sink_id
    }
}

/// Per-connection state.
pub struct Connection {
    /// Unique connection id within this process.
    pub conn_id: u64,
    /// Reference to the broker.
    pub broker: Arc<dyn Broker>,
    /// Authentication provider.
    pub auth: Arc<dyn AuthProvider>,
    /// Server configuration.
    pub config: ServerConfig,
    /// Metrics counters.
    pub metrics: Arc<Metrics>,
    /// Acknowledgement manager.
    pub ack_manager: SharedAckManager,
    /// Resume manager.
    pub resume: Arc<ResumeManager>,
    /// Backpressure controller.
    pub backpressure: BackpressureController,
    out_tx: mpsc::Sender<Frame>,
    out_rx: parking_lot::Mutex<Option<mpsc::Receiver<Frame>>>,
    /// Unique sink id for this connection.
    pub sink_id: u64,
    /// Negotiated codec.
    pub codec: Arc<dyn Codec>,
    in_flight_bytes: Arc<AtomicUsize>,
}

impl Connection {
    #[allow(clippy::too_many_arguments)]
    /// Create a new connection with the given parameters.
    pub fn new(
        conn_id: u64,
        broker: Arc<dyn Broker>,
        auth: Arc<dyn AuthProvider>,
        config: ServerConfig,
        metrics: Arc<Metrics>,
        ack_manager: SharedAckManager,
        resume: Arc<ResumeManager>,
    ) -> Self {
        let (out_tx, out_rx) = mpsc::channel::<Frame>(1024);
        let max_send_queue_bytes = config.max_send_queue_bytes;
        Self {
            conn_id,
            broker,
            auth,
            config,
            metrics,
            ack_manager,
            resume,
            backpressure: BackpressureController::new(max_send_queue_bytes),
            out_tx,
            out_rx: parking_lot::Mutex::new(Some(out_rx)),
            sink_id: crate::broker::fanout::new_sink_id(),
            codec: Arc::new(JsonCodec),
            in_flight_bytes: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Return a `FanoutSink` attached to this connection's outbound
    /// channel.
    pub fn sink(&self) -> Arc<dyn FanoutSink> {
        Arc::new(ConnSink {
            tx: self.out_tx.clone(),
            in_flight_bytes: self.in_flight_bytes.clone(),
            max_bytes: self.backpressure.max_bytes(),
            metrics: self.metrics.clone(),
            sink_id: self.sink_id,
        })
    }

    /// Run the full connection lifecycle.
    pub async fn run(self, mut transport: Box<dyn TransportConnection>) -> Result<()> {
        self.metrics.inc(&self.metrics.connection_open_total);
        self.metrics.inc(&self.metrics.active_connections);

        // Drive the handshake synchronously using the same transport.
        let session = match self.handshake(&mut transport).await {
            Ok(s) => s,
            Err(e) => {
                self.metrics.inc(&self.metrics.connection_close_total);
                self.metrics
                    .active_connections
                    .fetch_sub(1, Ordering::SeqCst);
                let code = match &e {
                    RiftError::Auth(_) => CloseCode::AuthFailed,
                    RiftError::Frame(crate::error::FrameReject::ProtocolVersionUnsupported {
                        ..
                    }) => CloseCode::ProtocolError,
                    _ => CloseCode::ProtocolError,
                };
                let _ = transport.close(code, &e.to_string()).await;
                return Err(e);
            }
        };

        // Shared transport slot for reader/writer tasks.
        let transport_slot: Arc<AsyncMutex<Option<Box<dyn TransportConnection>>>> =
            Arc::new(AsyncMutex::new(Some(transport)));

        // Writer task.
        let rx = self.out_rx.lock().take().ok_or_else(|| {
            RiftError::System(SystemReject::Internal("writer already started".into()))
        })?;
        let transport_slot_w = transport_slot.clone();
        let in_flight_w = self.in_flight_bytes.clone();
        let writer_handle = tokio::spawn(async move {
            writer_task(rx, transport_slot_w, in_flight_w).await;
        });

        // Reader task.
        let transport_slot_r = transport_slot.clone();
        let broker_r = self.broker.clone();
        let ack_r = self.ack_manager.clone();
        let metrics_r = self.metrics.clone();
        let session_r = session.clone();
        let codec_r = self.codec.clone();
        let out_tx_r = self.out_tx.clone();
        let conn_id = self.conn_id;
        let reader_handle = tokio::spawn(async move {
            reader_task(
                transport_slot_r,
                session_r,
                broker_r,
                ack_r,
                metrics_r,
                codec_r,
                out_tx_r,
                conn_id,
            )
            .await
        });

        // Wait for the reader to finish.
        let res = match reader_handle.await {
            Ok(r) => r,
            Err(e) => Err(RiftError::System(SystemReject::Internal(format!(
                "reader task panicked: {e}"
            )))),
        };

        // Close the transport.
        if let Some(mut t) = transport_slot.lock().await.take() {
            let _ = t.close(CloseCode::Normal, "bye").await;
        }
        self.broker.drop_sink(self.sink_id);
        self.ack_manager.forget(session.id.as_str());
        self.resume.tracker.forget(&session.id);
        self.metrics.inc(&self.metrics.connection_close_total);
        self.metrics
            .active_connections
            .fetch_sub(1, Ordering::SeqCst);
        // Closing the channel signals the writer to stop.
        drop(self.out_tx);
        let _ = writer_handle.await;
        res
    }

    /// Drive the hello → auth → resume/start → ready handshake.
    async fn handshake(
        &self,
        transport: &mut Box<dyn TransportConnection>,
    ) -> Result<Arc<Session>> {
        let hello_frame = transport.read_frame().await?;
        if hello_frame.frame_type != FrameType::Control {
            return Err(RiftError::Frame(crate::error::FrameReject::FrameInvalid(
                "expected control frame".into(),
            )));
        }
        let (hello, token) = decode_hello_payload(&hello_frame.payload)?;

        if hello.protocol != crate::protocol::version::PROTOCOL_NAME {
            return Err(RiftError::Frame(crate::error::FrameReject::FrameInvalid(
                format!("unknown protocol: {}", hello.protocol),
            )));
        }
        let client_major = hello.version >> 8;
        if crate::protocol::version::negotiate_major(client_major).is_none() {
            return Err(RiftError::Frame(
                crate::error::FrameReject::ProtocolVersionUnsupported {
                    client: hello.version,
                    server: crate::protocol::version::encoded_version(),
                },
            ));
        }

        let server_codecs: Vec<Arc<dyn Codec>> = vec![Arc::new(JsonCodec), Arc::new(CborCodec)];
        let codec = negotiate(&server_codecs, &hello.codecs)?;

        let auth_mode = hello
            .auth_modes
            .first()
            .copied()
            .unwrap_or(AuthMode::Anonymous);
        let auth_ctx = self.auth.authenticate(auth_mode, token.as_deref()).await?;
        let client_id = auth_ctx.client_id.clone();

        let session = Arc::new(Session::new(SessionId::new(), client_id));

        // Build server-side topic offsets for resume evaluation.
        let last_offsets: std::collections::HashMap<String, i64> = hello
            .last_offsets
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        let mut topic_offsets = std::collections::HashMap::new();
        for topic in last_offsets.keys() {
            topic_offsets.insert(topic.clone(), self.broker.head_offset(topic));
        }
        match self.resume.evaluate(
            &session,
            session.current_epoch(),
            &last_offsets,
            &topic_offsets,
        ) {
            Ok(outcome) => match outcome {
                crate::session::resume::ResumeOutcome::Resumed
                | crate::session::resume::ResumeOutcome::Partial
                | crate::session::resume::ResumeOutcome::Replaying => {
                    self.metrics.inc(&self.metrics.resume_success_total);
                }
                _ => {
                    self.metrics.inc(&self.metrics.resume_failed_total);
                }
            },
            Err(_) => {
                self.metrics.inc(&self.metrics.resume_failed_total);
            }
        }

        session.set_state(SessionState::Ready);

        // Send welcome + ready synchronously.
        let welcome = build_welcome_frame(&session, codec.frame_codec());
        transport.write_frame(&welcome).await?;
        let ready = build_ready_frame(&session, &self.config);
        transport.write_frame(&ready).await?;
        session.set_state(SessionState::Active);

        // The negotiated codec will be used by the reader task via
        // self.codec (the field is independently cloned when building
        // the reader).
        Ok(session)
    }
}

async fn writer_task(
    mut rx: mpsc::Receiver<Frame>,
    transport_slot: Arc<AsyncMutex<Option<Box<dyn TransportConnection>>>>,
    in_flight_bytes: Arc<AtomicUsize>,
) {
    while let Some(frame) = rx.recv().await {
        let bytes = frame.payload.as_ref().map(|p| p.len()).unwrap_or(0);
        let result = {
            let mut guard = transport_slot.lock().await;
            let Some(transport) = guard.as_mut() else {
                break;
            };
            transport.write_frame(&frame).await
        };
        match result {
            Ok(()) => {
                in_flight_bytes.fetch_sub(bytes, Ordering::SeqCst);
            }
            Err(e) => {
                warn!("write error: {}", e);
                // Release the transport so the reader stops too.
                *transport_slot.lock().await = None;
                break;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn reader_task(
    transport_slot: Arc<AsyncMutex<Option<Box<dyn TransportConnection>>>>,
    session: Arc<Session>,
    broker: Arc<dyn Broker>,
    ack_manager: SharedAckManager,
    metrics: Arc<Metrics>,
    codec: Arc<dyn Codec>,
    out_tx: mpsc::Sender<Frame>,
    conn_id: u64,
) -> Result<()> {
    loop {
        let frame = {
            let mut guard = transport_slot.lock().await;
            let Some(transport) = guard.as_mut() else {
                // Writer released the transport — connection is done.
                return Ok(());
            };
            match transport.read_frame().await {
                Ok(f) => f,
                Err(RiftError::Session(crate::error::SessionReject::Expired)) => {
                    debug!(conn = conn_id, "peer closed (session expired)");
                    return Ok(());
                }
                Err(e) => return Err(e),
            }
        };
        metrics.inc(&metrics.messages_in_total);
        session.touch();

        match frame.frame_type {
            FrameType::Control => {
                if let Err(e) =
                    handle_control(&out_tx, &broker, &frame, &session, &codec, &metrics).await
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
                match broker.publish(&frame) {
                    Ok(outcome) => {
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

async fn handle_control(
    out_tx: &mpsc::Sender<Frame>,
    broker: &Arc<dyn Broker>,
    frame: &Frame,
    _session: &Arc<Session>,
    codec: &Arc<dyn Codec>,
    _metrics: &Arc<Metrics>,
) -> Result<()> {
    let body = frame
        .payload
        .as_ref()
        .and_then(|p| std::str::from_utf8(p).ok())
        .unwrap_or("{}");

    // Propagate JSON parse errors rather than silently swallowing them.
    let v: serde_json::Value = serde_json::from_str(body).map_err(|e| {
        RiftError::Frame(crate::error::FrameReject::FrameInvalid(format!(
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
                    RiftError::Frame(crate::error::FrameReject::RequiredFieldMissing("topic"))
                })?
                .to_string();
            let intent_str = v.get("intent").and_then(|x| x.as_str()).unwrap_or("live");
            let intent = match intent_str {
                "live" => SubscribeIntent::Live,
                "passive" => SubscribeIntent::Passive,
                "ephemeral" => SubscribeIntent::Ephemeral,
                "latest" => SubscribeIntent::Latest,
                other => {
                    return Err(RiftError::Frame(crate::error::FrameReject::FrameInvalid(
                        format!("unknown subscribe intent: {other}"),
                    )));
                }
            };
            let sink: Arc<dyn FanoutSink> = Arc::new(MpscSink {
                tx: out_tx.clone(),
                id: crate::broker::fanout::new_sink_id(),
            });
            let id = broker.subscribe(&topic, intent, sink)?;
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
                    RiftError::Frame(crate::error::FrameReject::RequiredFieldMissing(
                        "subscription_id",
                    ))
                })?;
            let removed = broker.unsubscribe(crate::broker::fanout::SubscriptionId(sub_id))?;
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

struct MpscSink {
    tx: mpsc::Sender<Frame>,
    id: u64,
}

impl FanoutSink for MpscSink {
    fn deliver(&self, frame: bytes::Bytes) -> std::result::Result<(), FanoutError> {
        let f = Frame {
            frame_type: FrameType::Data,
            codec: FrameCodec::Json,
            payload: Some(frame),
            flags: FrameFlags::empty(),
            ..Frame::default()
        };
        self.tx.try_send(f).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => FanoutError::Backpressured {
                queue_bytes: 0,
                max_bytes: 0,
            },
            mpsc::error::TrySendError::Closed(_) => FanoutError::Closed,
        })
    }
    fn id(&self) -> u64 {
        self.id
    }
}

async fn send_error_frame(
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
            codec: FrameCodec::Json,
            correlation_id: original.correlation_id.clone(),
            payload: Some(Bytes::from(body.to_string())),
            ..Frame::default()
        })
        .map_err(|_| RiftError::System(SystemReject::Overloaded))
}

/// Map a `RiftError` variant to the closest `ErrorCode` wire identifier.
fn rift_error_to_code(err: &RiftError) -> &'static str {
    use crate::protocol::error_code::ErrorCode;
    match err {
        RiftError::Frame(fe) => match fe {
            crate::error::FrameReject::ProtocolVersionUnsupported { .. } => {
                ErrorCode::ProtocolVersionUnsupported.as_str()
            }
            crate::error::FrameReject::FrameInvalid(_) => ErrorCode::ProtocolFrameInvalid.as_str(),
            crate::error::FrameReject::CodecUnsupported(_) => {
                ErrorCode::ProtocolCodecUnsupported.as_str()
            }
            crate::error::FrameReject::PayloadTooLarge { .. } => {
                ErrorCode::ProtocolPayloadTooLarge.as_str()
            }
            crate::error::FrameReject::RequiredFieldMissing(_) => {
                ErrorCode::ProtocolRequiredFieldMissing.as_str()
            }
            crate::error::FrameReject::SchemaMismatch(_) => {
                ErrorCode::ProtocolSchemaMismatch.as_str()
            }
            crate::error::FrameReject::OrderViolation(_) => {
                ErrorCode::ProtocolOrderViolation.as_str()
            }
        },
        RiftError::Session(se) => match se {
            crate::error::SessionReject::NotFound(_) => ErrorCode::SessionNotFound.as_str(),
            crate::error::SessionReject::Expired => ErrorCode::SessionExpired.as_str(),
            crate::error::SessionReject::Conflict { .. } => ErrorCode::SessionConflict.as_str(),
            crate::error::SessionReject::ResumeRejected(_) => ErrorCode::ResumeRejected.as_str(),
            crate::error::SessionReject::ReplayOffsetExpired { .. } => {
                ErrorCode::ReplayOffsetExpired.as_str()
            }
            crate::error::SessionReject::SnapshotRequired(_) => {
                ErrorCode::SnapshotRequired.as_str()
            }
        },
        RiftError::Topic(te) => match te {
            crate::error::TopicReject::NotFound(_) => ErrorCode::TopicNotFound.as_str(),
            crate::error::TopicReject::Closed(_) => ErrorCode::TopicClosed.as_str(),
            crate::error::TopicReject::Overloaded(_) => ErrorCode::TopicOverloaded.as_str(),
            crate::error::TopicReject::SubscriberLimit(_) => {
                ErrorCode::TopicSubscriberLimit.as_str()
            }
            crate::error::TopicReject::PublisherLimit(_) => ErrorCode::TopicPublisherLimit.as_str(),
            crate::error::TopicReject::Forbidden(_) => ErrorCode::TopicForbidden.as_str(),
            crate::error::TopicReject::RateLimited(_) => ErrorCode::TopicRateLimited.as_str(),
            crate::error::TopicReject::InvalidName(_) => ErrorCode::ProtocolFrameInvalid.as_str(),
        },
        RiftError::Auth(ae) => match ae {
            crate::error::AuthReject::Required => ErrorCode::AuthRequired.as_str(),
            crate::error::AuthReject::Invalid(_) => ErrorCode::AuthInvalid.as_str(),
            crate::error::AuthReject::Expired => ErrorCode::AuthExpired.as_str(),
            crate::error::AuthReject::Revoked => ErrorCode::AuthRevoked.as_str(),
            crate::error::AuthReject::Denied(_) => ErrorCode::PermissionDenied.as_str(),
        },
        RiftError::Message(me) => match me {
            crate::error::MessageReject::Duplicate(_) => ErrorCode::MessageDuplicate.as_str(),
            crate::error::MessageReject::Expired => ErrorCode::MessageExpired.as_str(),
            crate::error::MessageReject::Rejected(_) => ErrorCode::MessageRejected.as_str(),
            crate::error::MessageReject::TooLarge { .. } => ErrorCode::MessageTooLarge.as_str(),
            crate::error::MessageReject::AckTimeout(_) => ErrorCode::MessageAckTimeout.as_str(),
            crate::error::MessageReject::DeliveryFailed(_) => {
                ErrorCode::MessageDeliveryFailed.as_str()
            }
        },
        RiftError::System(se) => match se {
            crate::error::SystemReject::Overloaded => ErrorCode::SystemOverloaded.as_str(),
            crate::error::SystemReject::Maintenance => ErrorCode::SystemMaintenance.as_str(),
            crate::error::SystemReject::ShardMoved(_) => ErrorCode::SystemShardMoved.as_str(),
            crate::error::SystemReject::RegionUnavailable(_) => {
                ErrorCode::SystemRegionUnavailable.as_str()
            }
            crate::error::SystemReject::Internal(_) => ErrorCode::SystemInternal.as_str(),
        },
        _ => ErrorCode::SystemInternal.as_str(),
    }
}

async fn send_ack_frame(
    out_tx: &mpsc::Sender<Frame>,
    ack: Ack,
    codec: &Arc<dyn Codec>,
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

// --- handshake helpers ---

fn build_welcome_frame(session: &Session, codec: FrameCodec) -> Frame {
    let body = serde_json::json!({
        "type": "welcome",
        "session_id": session.id.as_str(),
        "epoch": session.current_epoch(),
        "server_time": now_ms(),
    });
    Frame {
        frame_type: FrameType::Control,
        codec,
        session_id: Some(session.id.as_str().to_string()),
        payload: Some(Bytes::from(body.to_string())),
        ..Frame::default()
    }
}

fn build_ready_frame(session: &Session, config: &ServerConfig) -> Frame {
    let body = serde_json::json!({
        "type": "ready",
        "session_id": session.id.as_str(),
        "epoch": session.current_epoch(),
        "ping_interval_ms": config.heartbeat.ping_interval.as_millis() as u32,
        "pong_timeout_ms": config.heartbeat.pong_timeout.as_millis() as u32,
        "max_missed_pongs": config.heartbeat.max_missed_pongs,
        "idle_timeout_ms": config.heartbeat.idle_timeout.as_millis() as u32,
        "jitter_ms": config.heartbeat.jitter.as_millis() as u32,
        "max_payload_bytes": config.max_payload_bytes as u32,
        "max_topics_per_connection": config.max_topics_per_connection as u32,
        "max_send_queue_bytes": config.max_send_queue_bytes as u32,
        "server_time": now_ms(),
    });
    Frame {
        frame_type: FrameType::Control,
        codec: FrameCodec::Json,
        session_id: Some(session.id.as_str().to_string()),
        payload: Some(Bytes::from(body.to_string())),
        ..Frame::default()
    }
}

fn decode_hello_payload(payload: &Option<Bytes>) -> Result<(Hello, Option<String>)> {
    let p = payload.as_ref().ok_or_else(|| {
        RiftError::Frame(crate::error::FrameReject::RequiredFieldMissing("payload"))
    })?;
    let v: serde_json::Value = serde_json::from_slice(p)
        .map_err(|e| RiftError::Frame(crate::error::FrameReject::FrameInvalid(e.to_string())))?;
    let obj = v.as_object().ok_or_else(|| {
        RiftError::Frame(crate::error::FrameReject::FrameInvalid(
            "hello must be object".into(),
        ))
    })?;
    let mut h = Hello {
        protocol: obj
            .get("protocol")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string(),
        version: obj.get("version").and_then(|x| x.as_u64()).unwrap_or(0) as u16,
        client_id: obj
            .get("client_id")
            .and_then(|x| x.as_str())
            .map(String::from),
        session_id: obj
            .get("session_id")
            .and_then(|x| x.as_str())
            .map(String::from),
        epoch: obj.get("epoch").and_then(|x| x.as_u64()).map(|x| x as u32),
        ..Hello::default()
    };
    if let Some(arr) = obj.get("codecs").and_then(|x| x.as_array()) {
        for c in arr {
            match c.as_str() {
                Some("json") => h.codecs.push(FrameCodec::Json),
                Some("cbor") => h.codecs.push(FrameCodec::Cbor),
                _ => {}
            }
        }
    }
    if let Some(arr) = obj.get("auth_modes").and_then(|x| x.as_array()) {
        for c in arr {
            match c.as_str() {
                Some("bearer") => h.auth_modes.push(AuthMode::Bearer),
                Some("cookie") => h.auth_modes.push(AuthMode::Cookie),
                Some("mtls") => h.auth_modes.push(AuthMode::Mtls),
                Some("signed_challenge") => h.auth_modes.push(AuthMode::SignedChallenge),
                Some("anonymous") => h.auth_modes.push(AuthMode::Anonymous),
                _ => {}
            }
        }
    }
    if let Some(arr) = obj.get("last_offsets").and_then(|x| x.as_object()) {
        for (k, v) in arr {
            if let Some(n) = v.as_i64() {
                h.last_offsets.insert(k.clone(), n);
            }
        }
    }
    let token = obj.get("token").and_then(|x| x.as_str()).map(String::from);
    Ok((h, token))
}
