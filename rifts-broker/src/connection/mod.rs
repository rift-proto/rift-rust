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
//! - **Writer task** — reads [`Frame`](rifts_core::frame::Frame)s from an mpsc
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

// ── Sub-modules ────────────────────────────────────────────────────────────────

pub(crate) mod control;
pub(crate) mod reader;
pub(crate) mod writer;

use reader::reader_task;
use writer::writer_task;

// ── Imports ────────────────────────────────────────────────────────────────────

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tracing::instrument;

use crate::broker::Broker;
use crate::broker::fanout::{ConnSink, FanoutSink};
use rifts_core::ack::SharedAckManager;
use rifts_core::codec::{CborCodec, JsonCodec, PayloadCodec, negotiate};
use rifts_core::config::ServerConfig;
use rifts_core::error::{Result, RiftError, SessionReject, SystemReject};
use rifts_core::flow::BackpressureController;
use rifts_core::frame::{EncodingFormat as FrameEncodingFormat, Frame, FrameFlags, FrameType};
use rifts_core::metrics::Metrics;
use rifts_core::now_ms;
use rifts_core::protocol::close::CloseCode;
use rifts_core::protocol::hello::{AuthMode, Hello};
use rifts_session::session::resume::ResumeManager;
use rifts_session::session::store::SessionStore;
use rifts_session::session::{AuthProvider, OffsetTracker, Session, SessionId, SessionState};
use rifts_transport::transport::TransportConnection;

// ── Connection ─────────────────────────────────────────────────────────────────

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
    pub codec: Arc<dyn PayloadCodec>,

    /// Atomic counter of bytes currently in the outbound queue. Shared
    /// between the fanout sink and the writer task.
    in_flight_bytes: Arc<AtomicUsize>,

    /// Topics this connection has published to. Used on connection close
    /// to release per-topic publisher counts back to the broker (so the
    /// slots can be reused by future publishers).
    /// Topics this connection has published to. Drained on
    /// teardown to release per-topic publisher slots.
    published_topics: Arc<parking_lot::Mutex<HashSet<String>>>,

    /// Subscription sink IDs created via the subscribe control handler.
    /// Tracked so teardown can clean up per-subscription MpscSink
    /// entries from the FanoutEngine — without this, every disconnect
    /// leaks N fanout subscription entries.
    subscription_sinks: Arc<parking_lot::Mutex<HashSet<u64>>>,
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
            subscription_sinks: Arc::new(parking_lot::Mutex::new(HashSet::new())),
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
    #[instrument(skip(self, transport), fields(conn_id = self.conn_id))]
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
                    RiftError::Frame(
                        rifts_core::error::FrameReject::ProtocolVersionUnsupported { .. },
                    ) => CloseCode::ProtocolError,
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
            writer_task(rx, transport_slot_w, in_flight_w, self.config.write_timeout).await;
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
        let subscription_sinks_r = self.subscription_sinks.clone();
        let conn_id = self.conn_id;
        let idle_timeout = self.config.idle_timeout;
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
                subscription_sinks_r,
                conn_id,
                idle_timeout,
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
        // Clean up per-subscription MpscSink entries from the fanout
        // engine. Without this, every disconnect leaks N subscription
        // entries (one per topic the connection subscribed to via the
        // subscribe control handler).
        let sub_sinks: Vec<u64> = self.subscription_sinks.lock().drain().collect();
        for sid in sub_sinks {
            self.broker.drop_sink(sid).await;
        }
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
            return Err(RiftError::Frame(
                rifts_core::error::FrameReject::FrameInvalid("expected control frame".into()),
            ));
        }
        let (hello, token) = decode_hello_payload(&hello_frame.payload)?;

        if hello.protocol != rifts_core::protocol::version::PROTOCOL_NAME {
            return Err(RiftError::Frame(
                rifts_core::error::FrameReject::FrameInvalid(format!(
                    "unknown protocol: {}",
                    hello.protocol
                )),
            ));
        }
        let client_major = hello.version >> 8;
        if rifts_core::protocol::version::negotiate_major(client_major).is_none() {
            return Err(RiftError::Frame(
                rifts_core::error::FrameReject::ProtocolVersionUnsupported {
                    client: hello.version,
                    server: rifts_core::protocol::version::encoded_version(),
                },
            ));
        }

        // Build the server's codec list from the configured offer.
        // An empty `codec_offer` means all compiled-in codecs are offered.
        let server_codecs: Vec<Arc<dyn PayloadCodec>> = if self.config.codec_offer.is_empty() {
            vec![Arc::new(JsonCodec), Arc::new(CborCodec)]
        } else {
            self.config
                .codec_offer
                .iter()
                .map(|offer| -> Arc<dyn PayloadCodec> {
                    match offer {
                        FrameEncodingFormat::Json => Arc::new(JsonCodec),
                        FrameEncodingFormat::Cbor => Arc::new(CborCodec),
                    }
                })
                .collect()
        };
        let codec = negotiate(&server_codecs, &hello.codecs)?;
        // Store the negotiated codec on the connection so the reader
        // task uses the same encoding for all subsequent frames.
        self.codec = codec.clone();

        // Match the client's offered auth modes against the server's
        // supported modes. Pick the first mode that appears in both
        // lists. If no overlap, fall back to Anonymous.
        let server_modes = self.auth.supported_modes();
        let auth_mode = hello
            .auth_modes
            .iter()
            .find(|m| server_modes.contains(m))
            .copied()
            .unwrap_or(AuthMode::Anonymous);
        let auth_ctx = self.auth.authenticate(auth_mode, token.as_deref()).await?;

        // Validate Hello frame fields before proceeding.
        if hello.codecs.is_empty() {
            return Err(RiftError::Frame(
                rifts_core::error::FrameReject::FrameInvalid(
                    "hello must include at least one codec".into(),
                ),
            ));
        }
        // Limit token size to 8 KiB to prevent resource exhaustion.
        if let Some(ref t) = token
            && t.len() > 8192
        {
            return Err(RiftError::Frame(
                rifts_core::error::FrameReject::PayloadTooLarge {
                    actual: t.len(),
                    max: 8192,
                },
            ));
        }
        // Limit last_offsets to 1024 entries.
        if hello.last_offsets.len() > 1024 {
            return Err(RiftError::Frame(
                rifts_core::error::FrameReject::FrameInvalid(
                    "hello last_offsets exceeds maximum 1024 entries".into(),
                ),
            ));
        }
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
                    let outcome =
                        match self
                            .resume
                            .evaluate(&existing, &last_offsets, &topic_offsets)
                        {
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
                rifts_session::session::ResumeDecision::FullResume
                | rifts_session::session::ResumeDecision::PartialResume
                | rifts_session::session::ResumeDecision::Replaying,
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
            rifts_session::session::ResumeDecision::FullResume => "resumed",
            rifts_session::session::ResumeDecision::PartialResume => "partial",
            rifts_session::session::ResumeDecision::Replaying => "replaying",
            rifts_session::session::ResumeDecision::SnapshotRequired => "snapshot_required",
            rifts_session::session::ResumeDecision::ColdStart => "cold_start",
            rifts_session::session::ResumeDecision::Rejected => "rejected",
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
            Some(rifts_session::session::ResumeDecision::PartialResume)
                | Some(rifts_session::session::ResumeDecision::Replaying)
        ) {
            for (topic, &last_offset) in &last_offsets {
                let head = self.broker.head_offset(topic).await;
                if last_offset < head {
                    match self.broker.replay(topic, last_offset + 1, head).await {
                        Ok(frames) => {
                            for data in frames {
                                let mut replay_frame = Frame {
                                    frame_type: FrameType::Data,
                                    codec: FrameEncodingFormat::Json,
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

// ── Handshake helpers ──────────────────────────────────────────────────────────

/// Build the "welcome" control frame sent immediately after successful
/// authentication.
///
/// Contains the assigned session id, epoch, negotiated codec,
/// resume result, and server timestamp.
fn build_welcome_frame(
    session: &Session,
    codec: FrameEncodingFormat,
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
fn build_ready_frame(
    session: &Session,
    config: &ServerConfig,
    codec: FrameEncodingFormat,
) -> Frame {
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
        RiftError::Frame(rifts_core::error::FrameReject::RequiredFieldMissing(
            "payload",
        ))
    })?;
    let v: serde_json::Value = serde_json::from_slice(p).map_err(|e| {
        RiftError::Frame(rifts_core::error::FrameReject::FrameInvalid(e.to_string()))
    })?;
    let obj = v.as_object().ok_or_else(|| {
        RiftError::Frame(rifts_core::error::FrameReject::FrameInvalid(
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
                return Err(RiftError::Frame(
                    rifts_core::error::FrameReject::FrameInvalid(format!(
                        "hello version out of range: {v}"
                    )),
                ));
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
                Some("json") => h.codecs.push(FrameEncodingFormat::Json),
                Some("cbor") => h.codecs.push(FrameEncodingFormat::Cbor),
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
