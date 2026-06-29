//! Per-connection state machine — drives the Rift/1 connection lifecycle (spec section 5).
//!
//! This module implements the full lifecycle of a single Rift connection:
//!
//! 1. **Handshake** — hello, auth, codec negotiation, resume evaluation.
//! 2. **Active phase** — concurrent reader and writer tasks processing frames.
//! 3. **Teardown** — drain the outbound queue, release resources, close the transport.
//!
//! The [`Connection`] owns all per-connection state (broker ref, auth ref,
//! config, metrics, ack manager, backpressure controller, codec) and drives
//! the lifecycle via [`Connection::run`].
//!
//! # Architecture
//!
//! After the handshake completes, two tokio tasks are spawned:
//!
//! - **Writer task** — reads [`Frame`](crate::frame::Frame)s from an mpsc
//!   channel and writes them to the transport. Tracks in-flight bytes for
//!   backpressure accounting.
//!
//! - **Reader task** — reads frames from the transport, dispatches them
//!   to the broker (for data frames), the ack manager (for ack frames),
//!   or the control handler (for subscribe/unsubscribe/ping).
//!
//! The two tasks share the transport behind an `Arc<AsyncMutex<Option<...>>>`
//! so that either task can release it (e.g. on write error) and the other
//! will observe the `None` and shut down.

use std::collections::HashSet;
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
use crate::error::{Result, RiftError, SessionReject, SystemReject};
use crate::flow::BackpressureController;
use crate::frame::{Codec as FrameCodec, Frame, FrameFlags, FrameType};
use crate::metrics::Metrics;
use crate::now_ms;
use crate::protocol::close::CloseCode;
use crate::protocol::hello::{AuthMode, Hello};
use crate::session::resume::ResumeManager;
use crate::session::store::SessionStore;
use crate::session::{AuthProvider, OffsetTracker, Session, SessionId, SessionState};
use crate::transport::TransportConnection;

/// Lightweight fanout sink that writes inbound frames to the connection's
/// outbound mpsc channel.
///
/// Created by [`Connection::sink`] and handed to the broker so that
/// subscription fanout can push frames directly into this connection's
/// write queue without going through the connection object itself.
struct ConnSink {
    /// Sender half of the connection's outbound frame channel.
    tx: mpsc::Sender<Frame>,

    /// Atomic counter of bytes currently in the outbound queue.
    /// Shared with the writer task which decrements it on successful writes.
    in_flight_bytes: Arc<AtomicUsize>,

    /// Maximum allowed bytes in the outbound queue (from
    /// `ServerConfig::max_send_queue_bytes`).
    max_bytes: usize,

    /// Metrics counter for outgoing messages.
    metrics: Arc<Metrics>,

    /// Unique id for this sink, used by the broker to track subscriptions.
    sink_id: u64,

    /// Codec negotiated during the Hello handshake. All frames
    /// delivered via this sink are tagged with this codec so
    /// clients that negotiated CBOR receive correctly-encoded
    /// payloads.
    codec: FrameCodec,
}

impl FanoutSink for ConnSink {
    /// Attempt to deliver a serialized message frame to this connection's
    /// outbound queue.
    ///
    /// Returns `Err(FanoutError::Backpressured)` if the queue is at capacity
    /// or if the mpsc channel is full. Returns `Err(FanoutError::Closed)` if
    /// the channel has been closed (connection is shutting down).
    fn deliver(&self, frame: bytes::Bytes) -> std::result::Result<(), FanoutError> {
        let len = frame.len();
        // Atomic reserve: compare-and-swap loop so the byte-count
        // check and the increment are performed as a single atomic
        // operation. Without this, multiple concurrent deliveries
        // can each see the counter below the limit and all proceed,
        // collectively exceeding `max_bytes`.
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
            // Use the negotiated codec, not a hardcoded value, so
            // CBOR-negociated clients receive correctly-tagged frames.
            codec: self.codec,
            payload: Some(frame),
            flags: FrameFlags::empty(),
            ..Frame::default()
        };
        match self.tx.try_send(f) {
            Ok(()) => {
                self.metrics.inc(&self.metrics.messages_out_total);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                // Roll back the reservation we made above.
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
        self.sink_id
    }
}

/// Per-connection state.
///
/// Each accepted transport connection is wrapped in a `Connection` which
/// holds every resource the connection needs during its lifetime: the
/// broker, auth provider, server config, metrics, ack/resume managers,
/// backpressure controller, and the negotiated codec.
///
/// The connection is consumed by [`run`](Self::run) which drives the full
/// lifecycle (handshake, active phase, teardown).
pub struct Connection {
    /// Unique connection id within this process, assigned by the server.
    pub conn_id: u64,

    /// Reference to the broker that routes messages between publishers
    /// and subscribers.
    pub broker: Arc<dyn Broker>,

    /// Authentication provider used during the hello handshake.
    pub auth: Arc<dyn AuthProvider>,

    /// Server configuration (heartbeat, limits, topic defaults, etc.).
    pub config: ServerConfig,

    /// Metrics counters for connection, message, and error tracking.
    pub metrics: Arc<Metrics>,

    /// Acknowledgement manager for tracking outstanding (sent-but-not-acked)
    /// frames.
    pub ack_manager: SharedAckManager,

    /// Shared resume manager for evaluating session resume requests.
    pub resume: Arc<ResumeManager>,

    /// Shared offset tracker for recording per-session per-topic offset
    /// progress. Persists across connections for cross-connection resume.
    pub offset_tracker: Arc<OffsetTracker>,

    /// Shared session store for cross-connection session resumption.
    /// The server holds one store for all connections so that a
    /// reconnecting client can find its previous session.
    session_store: SessionStore,

    /// Backpressure controller for monitoring the outbound queue.
    pub backpressure: BackpressureController,

    /// Sender half of the outbound frame channel, shared with the fanout
    /// sink and the control handler.
    out_tx: mpsc::Sender<Frame>,

    /// Receiver half of the outbound frame channel, taken by the writer
    /// task at startup.
    out_rx: parking_lot::Mutex<Option<mpsc::Receiver<Frame>>>,

    /// Unique sink id for this connection's fanout sink.
    pub sink_id: u64,

    /// Negotiated codec, defaulting to JSON until the handshake completes.
    pub codec: Arc<dyn Codec>,

    /// Atomic counter of bytes currently in the outbound queue. Shared
    /// between the fanout sink and the writer task.
    in_flight_bytes: Arc<AtomicUsize>,

    /// Topics this connection has published to. Used on connection close
    /// to release per-topic publisher counts back to the broker (so the
    /// slots can be reused by future publishers).
    /// Topics this connection has published to. Drained on
    /// teardown to release per-topic publisher slots.
    published_topics: Arc<parking_lot::Mutex<HashSet<String>>>,
}

impl Connection {
    /// Create a new connection with the given parameters.
    ///
    /// A bounded mpsc channel (capacity 1024) is created for the outbound
    /// frame queue. The backpressure controller is initialized with the
    /// `max_send_queue_bytes` from the server config.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        conn_id: u64,
        broker: Arc<dyn Broker>,
        auth: Arc<dyn AuthProvider>,
        config: ServerConfig,
        metrics: Arc<Metrics>,
        ack_manager: SharedAckManager,
        resume: Arc<ResumeManager>,
        offset_tracker: Arc<OffsetTracker>,
        session_store: SessionStore,
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
            offset_tracker,
            session_store,
            backpressure: BackpressureController::new(max_send_queue_bytes),
            out_tx,
            out_rx: parking_lot::Mutex::new(Some(out_rx)),
            sink_id: crate::broker::fanout::new_sink_id(),
            codec: Arc::new(JsonCodec),
            in_flight_bytes: Arc::new(AtomicUsize::new(0)),
            published_topics: Arc::new(parking_lot::Mutex::new(HashSet::new())),
        }
    }

    /// Create a [`FanoutSink`] attached to this connection's outbound channel.
    ///
    /// The sink can be handed to the broker so that subscription fanout can
    /// push frames directly into the connection's write queue. The sink
    /// enforces the backpressure limit and increments the `messages_out_total`
    /// metric on each successful delivery.
    pub fn sink(&self) -> Arc<dyn FanoutSink> {
        Arc::new(ConnSink {
            tx: self.out_tx.clone(),
            in_flight_bytes: self.in_flight_bytes.clone(),
            max_bytes: self.backpressure.max_bytes(),
            metrics: self.metrics.clone(),
            sink_id: self.sink_id,
            codec: self.codec.frame_codec(),
        })
    }

    /// Run the full connection lifecycle.
    ///
    /// This method:
    ///
    /// 1. Increments connection metrics.
    /// 2. Performs the hello handshake (protocol negotiation, auth, resume).
    /// 3. Spawns the writer and reader tasks.
    /// 4. Waits for the reader to finish.
    /// 5. Closes the transport, releases resources (sink, acks, resume,
    ///    per-topic publisher slots), and decrements connection metrics.
    /// 6. Returns the reader's result.
    pub async fn run(mut self, mut transport: Box<dyn TransportConnection>) -> Result<()> {
        self.metrics.inc(&self.metrics.connection_open_total);
        self.metrics.inc(&self.metrics.active_connections);

        // Drive the handshake synchronously using the same transport.
        // Pass the session_store by value-clone so we don't borrow
        // `self` twice (once &mut for handshake, once &self for the store).
        let store = self.session_store.clone();
        let session = match self.handshake(&mut transport, &store).await {
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
        let in_flight_r = self.in_flight_bytes.clone();
        let max_bytes_r = self.backpressure.max_bytes();
        // Share the published_topics set with the reader task so
        // teardown can release every publisher slot the connection
        // actually used. Previously a separate set was created in
        // the reader, leaving the connection's set empty and
        // leaking publisher slots.
        let published_topics_r = self.published_topics.clone();
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
                in_flight_r,
                max_bytes_r,
                published_topics_r,
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
        self.broker.drop_sink(self.sink_id).await;
        self.ack_manager.forget(session.id.as_str());
        // Do NOT forget the offset tracker on every disconnect: the
        // SessionStore/OffsetTracker are specifically designed to
        // support cross-connection session resumption. Forgetting
        // here would defeat that feature. The offset history is
        // eventually cleaned up when the session is explicitly
        // removed from the store or expires.
        // Release per-topic publisher slots so the limit isn't
        // permanently consumed by this connection.
        let topics: Vec<String> = self.published_topics.lock().drain().collect();
        for topic in topics {
            self.broker.dec_publisher(&topic).await;
        }
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
    ///
    /// This method reads the hello control frame from the transport,
    /// validates the protocol name and version, negotiates a codec,
    /// authenticates the client, evaluates the resume request, and
    /// sends the welcome and ready frames.
    ///
    /// ## Session resumption
    ///
    /// If the client provides a `session_id` in its Hello, the server
    /// looks up the session in the [`SessionStore`]. If found and the
    /// epoch matches, the server evaluates the client's last offsets
    /// against the broker's current head offsets and replays any missed
    /// messages before the client begins processing live frames.
    ///
    /// If the session is not found or the epoch mismatches, the
    /// handshake fails with a session error and the connection is
    /// closed.
    async fn handshake(
        &mut self,
        transport: &mut Box<dyn TransportConnection>,
        session_store: &SessionStore,
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

        // Build the server's codec list from the configured offer.
        // An empty `codec_offer` means all compiled-in codecs are offered.
        let server_codecs: Vec<Arc<dyn Codec>> = if self.config.codec_offer.is_empty() {
            vec![Arc::new(JsonCodec), Arc::new(CborCodec)]
        } else {
            self.config
                .codec_offer
                .iter()
                .map(|offer| -> Arc<dyn Codec> {
                    match offer {
                        FrameCodec::Json => Arc::new(JsonCodec),
                        FrameCodec::Cbor => Arc::new(CborCodec),
                    }
                })
                .collect()
        };
        let codec = negotiate(&server_codecs, &hello.codecs)?;
        // Store the negotiated codec on the connection so the reader
        // task uses the same encoding for all subsequent frames.
        self.codec = codec.clone();

        let auth_mode = hello
            .auth_modes
            .first()
            .copied()
            .unwrap_or(AuthMode::Anonymous);
        let auth_ctx = self.auth.authenticate(auth_mode, token.as_deref()).await?;
        let client_id = auth_ctx.client_id.clone();

        // --- Session lookup / creation ----------------------------------

        let last_offsets: std::collections::HashMap<String, i64> = hello
            .last_offsets
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();

        let (session, resume_result) = if let Some(ref sid) = hello.session_id {
            // Client wants to resume an existing session.
            match session_store.get(sid) {
                Some(existing) => {
                    // Validate epoch.
                    let incoming_epoch = hello.epoch.unwrap_or(0);
                    if incoming_epoch != existing.current_epoch() {
                        return Err(RiftError::Session(SessionReject::Conflict {
                            incoming: incoming_epoch,
                            current: existing.current_epoch(),
                        }));
                    }
                    // Check for a conflicting active connection.
                    match existing.state() {
                        SessionState::Active | SessionState::Ready => {
                            return Err(RiftError::Session(SessionReject::Conflict {
                                incoming: incoming_epoch,
                                current: existing.current_epoch(),
                            }));
                        }
                        _ => {}
                    }
                    // Bump epoch for this new incarnation.
                    existing.bump_epoch();
                    existing.set_state(SessionState::Resuming);

                    // Evaluate resume decision against broker head offsets.
                    let mut topic_offsets = std::collections::HashMap::new();
                    for topic in last_offsets.keys() {
                        topic_offsets.insert(topic.clone(), self.broker.head_offset(topic).await);
                    }
                    let outcome = match self.resume.evaluate(
                        &existing,
                        &last_offsets,
                        &topic_offsets,
                    ) {
                        Ok(o) => o,
                        Err(e) => {
                            self.metrics.inc(&self.metrics.resume_failed_total);
                            return Err(e);
                        }
                    };
                    (existing, Some(outcome))
                }
                None => {
                    return Err(RiftError::Session(SessionReject::NotFound(sid.clone())));
                }
            }
        } else {
            // Cold start — create a brand new session.
            let s = Arc::new(Session::new(SessionId::new(), client_id));
            session_store.insert(s.clone());
            (s, None)
        };

        // Record resume metrics.
        match resume_result {
            Some(
                crate::session::ResumeDecision::FullResume
                | crate::session::ResumeDecision::PartialResume
                | crate::session::ResumeDecision::Replaying,
            ) => {
                self.metrics.inc(&self.metrics.resume_success_total);
            }
            Some(_) => {
                self.metrics.inc(&self.metrics.resume_failed_total);
            }
            None => {}
        }

        session.set_state(SessionState::Ready);

        // Send welcome (with resume_result + negotiated_codec).
        let resume_result_str = resume_result.map(|o| match o {
            crate::session::ResumeDecision::FullResume => "resumed",
            crate::session::ResumeDecision::PartialResume => "partial",
            crate::session::ResumeDecision::Replaying => "replaying",
            crate::session::ResumeDecision::SnapshotRequired => "snapshot_required",
            crate::session::ResumeDecision::ColdStart => "cold_start",
            crate::session::ResumeDecision::Rejected => "rejected",
        });
        let welcome = build_welcome_frame(
            &session,
            codec.frame_codec(),
            resume_result_str,
            &self.config,
        );
        transport.write_frame(&welcome).await?;

        // Replay missed messages for Partial / Replaying outcomes.
        if matches!(
            resume_result,
            Some(crate::session::ResumeDecision::PartialResume)
                | Some(crate::session::ResumeDecision::Replaying)
        ) {
            for (topic, &last_offset) in &last_offsets {
                let head = self.broker.head_offset(topic).await;
                if last_offset < head {
                    match self.broker.replay(topic, last_offset + 1, head).await {
                        Ok(frames) => {
                            for data in frames {
                                let mut replay_frame = Frame {
                                    frame_type: FrameType::Data,
                                    codec: FrameCodec::Json,
                                    payload: Some(data),
                                    flags: FrameFlags::empty(),
                                    topic: Some(topic.clone()),
                                    ..Frame::default()
                                };
                                replay_frame.mark_replay();
                                let _ = transport.write_frame(&replay_frame).await;
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                conn = self.conn_id,
                                topic = %topic,
                                "replay failed: {}",
                                e
                            );
                        }
                    }
                }
            }
        }

        // Send ready.
        let ready = build_ready_frame(&session, &self.config, codec.frame_codec());
        transport.write_frame(&ready).await?;
        session.set_state(SessionState::Active);

        Ok(session)
    }
}

/// Writer task — drains the outbound frame channel and writes each frame
/// to the transport.
///
/// The task exits when the channel is closed (all senders dropped) or
/// when a write error occurs. On write failure the transport slot is set
/// to `None` so that the reader task observes the shutdown and stops.
/// The `in_flight_bytes` counter is zeroed defensively on exit.
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
                // Release the bytes for the frame that failed to write;
                // without this, in_flight_bytes grows monotonically and
                // eventually triggers spurious backpressure on a
                // reconnect using a new Connection instance.
                in_flight_bytes.fetch_sub(bytes, Ordering::SeqCst);
                // Release the transport so the reader stops too.
                *transport_slot.lock().await = None;
                break;
            }
        }
    }
    // Defensive: when the writer exits (channel closed or transport
    // dropped) zero the counter. At this point the Connection is
    // about to be dropped, so no other code can be mutating the
    // counter.
    in_flight_bytes.store(0, Ordering::SeqCst);
}

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
async fn reader_task(
    transport_slot: Arc<AsyncMutex<Option<Box<dyn TransportConnection>>>>,
    session: Arc<Session>,
    broker: Arc<dyn Broker>,
    ack_manager: SharedAckManager,
    metrics: Arc<Metrics>,
    codec: Arc<dyn Codec>,
    out_tx: mpsc::Sender<Frame>,
    in_flight_bytes: Arc<AtomicUsize>,
    max_bytes: usize,
    published_topics: Arc<parking_lot::Mutex<HashSet<String>>>,
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
                if let Err(e) = handle_control(
                    &out_tx,
                    &broker,
                    &frame,
                    &session,
                    &codec,
                    &metrics,
                    &in_flight_bytes,
                    max_bytes,
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

/// Handle a control frame (ping, subscribe, unsubscribe).
///
/// The control body is expected to be a JSON object with a `"type"` field.
/// Unknown control types are logged and ignored.
#[allow(clippy::too_many_arguments)]
async fn handle_control(
    out_tx: &mpsc::Sender<Frame>,
    broker: &Arc<dyn Broker>,
    frame: &Frame,
    _session: &Arc<Session>,
    codec: &Arc<dyn Codec>,
    _metrics: &Arc<Metrics>,
    in_flight_bytes: &Arc<AtomicUsize>,
    max_bytes: usize,
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
                "snapshot_then_live" => SubscribeIntent::SnapshotThenLive,
                "replay" => {
                    let from = v.get("from_offset").and_then(|x| x.as_i64()).unwrap_or(0);
                    SubscribeIntent::Replay { from }
                }
                other => {
                    return Err(RiftError::Frame(crate::error::FrameReject::FrameInvalid(
                        format!("unknown subscribe intent: {other}"),
                    )));
                }
            };
            let sink: Arc<dyn FanoutSink> = Arc::new(MpscSink {
                tx: out_tx.clone(),
                in_flight_bytes: in_flight_bytes.clone(),
                max_bytes,
                id: crate::broker::fanout::new_sink_id(),
                codec: codec.frame_codec(),
            });
            let id = broker.subscribe(&topic, intent, sink).await?;
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
            let removed = broker
                .unsubscribe(crate::broker::fanout::SubscriptionId(sub_id))
                .await?;
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
/// Unlike [`ConnSink`] (which is created once per connection and tracks
/// metrics), `MpscSink` is created per-subscription and only enforces
/// backpressure without incrementing metrics.
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
    codec: FrameCodec,
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

/// Map a [`RiftError`] variant to the closest wire-level error code
/// identifier string.
///
/// This is used when constructing error frames to send to the client.
/// The mapping follows the error code table defined in the Rift protocol
/// specification.
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
            crate::error::SessionReject::Closed => ErrorCode::SessionExpired.as_str(),
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

/// Build and send an acknowledgement frame to the client.
///
/// The ack body is a JSON object containing the ack id, message id,
/// status, offset, reason, and server timestamp.
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

/// Build the "welcome" control frame sent immediately after successful
/// authentication.
///
/// Contains the assigned session id, epoch, negotiated codec,
/// resume result, and server timestamp.
fn build_welcome_frame(
    session: &Session,
    codec: FrameCodec,
    resume_result: Option<&str>,
    config: &ServerConfig,
) -> Frame {
    let mut body = serde_json::json!({
        "type": "welcome",
        "session_id": session.id.as_str(),
        "epoch": session.current_epoch(),
        "negotiated_codec": codec.name(),
        // `resume_window_ms` advertises how long the server will
        // retain a session's offset history for cross-connection
        // resumption. Surfacing it lets clients make informed
        // decisions about reconnect timing.
        "resume_window_ms": config.replay_window.as_millis() as u32,
        // Advertise the set of optional features the server
        // supports so clients can negotiate capability-aware
        // behavior on connect.
        "features": config.supported_features(),
        "server_time": now_ms(),
    });
    if let Some(r) = resume_result {
        body["resume_result"] = serde_json::Value::String(r.to_string());
    }
    Frame {
        frame_type: FrameType::Control,
        codec,
        session_id: Some(session.id.as_str().to_string()),
        payload: Some(Bytes::from(body.to_string())),
        timestamp: now_ms(),
        ..Frame::default()
    }
}

/// Build the "ready" control frame sent after the welcome frame.
///
/// Contains the negotiated session parameters: ping interval, pong
/// timeout, max missed pongs, idle timeout, jitter, max payload bytes,
/// max topics per connection, and max send queue bytes.
fn build_ready_frame(session: &Session, config: &ServerConfig, codec: FrameCodec) -> Frame {
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
        codec,
        session_id: Some(session.id.as_str().to_string()),
        payload: Some(Bytes::from(body.to_string())),
        timestamp: now_ms(),
        ..Frame::default()
    }
}

/// Decode the hello control frame payload into a [`Hello`] struct and
/// an optional authentication token.
///
/// The payload is expected to be a JSON object. Missing or malformed
/// fields are handled gracefully with defaults or descriptive errors.
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
        version: match obj.get("version").and_then(|x| x.as_u64()) {
            Some(v) if v <= u16::MAX as u64 => v as u16,
            Some(v) => {
                return Err(RiftError::Frame(crate::error::FrameReject::FrameInvalid(
                    format!("hello version out of range: {v}"),
                )));
            }
            None => 0,
        },
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
