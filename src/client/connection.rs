use std::collections::HashMap;
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value as JsonValue;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Notify, RwLock, broadcast, oneshot};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use crate::ack::AckStatus;
use crate::broker::SubscribeIntent;
use crate::frame::{Codec, Frame, FrameType};
use crate::message::command::Reply;
use crate::protocol::hello::{Ready, Welcome};
use crate::transport::frame_codec::{
    DEFAULT_MAX_BINARY_PAYLOAD, decode_binary_frame, encode_frame,
};

use super::config::RiftClientConfig;
use super::error::{ClientError, Result};
use super::events::ClientEvent;
use super::frame_builder::{FrameIdCounter, hello_frame};
use super::heartbeat::{self, HeartbeatState};
use super::subscriber::SubscriptionTracker;

type WsWriter =
    futures_util::stream::SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, WsMessage>;
type WsReader = futures_util::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

/// Per-connection state. Dropped on disconnect and recreated on reconnect.
pub(crate) struct ConnectionInner {
    pub(crate) writer: Arc<Mutex<WsWriter>>,
    pub(crate) session_id: String,
    pub(crate) epoch: u32,
    #[allow(dead_code)]
    pub(crate) ready: Ready,
    #[allow(dead_code)]
    pub(crate) pending_replies: Arc<Mutex<HashMap<String, oneshot::Sender<Reply>>>>,
    pub(crate) disconnect_notify: Arc<Notify>,
    #[allow(dead_code)]
    pub(crate) heartbeat_state: Arc<HeartbeatState>,
}

/// Perform the full connect sequence: open WebSocket, Hello/Welcome/Ready,
/// spawn reader and heartbeat background tasks.
pub(crate) async fn connect(
    url: &str,
    config: &RwLock<RiftClientConfig>,
    event_tx: &broadcast::Sender<ClientEvent>,
    subscriptions: Arc<Mutex<SubscriptionTracker>>,
    frame_ids: FrameIdCounter,
) -> Result<Arc<ConnectionInner>> {
    // 1. Open WebSocket
    let (ws_stream, _resp) = connect_async(url)
        .await
        .map_err(|e| ClientError::Other(format!("WebSocket connect failed: {e}")))?;
    let (mut writer, mut reader) = ws_stream.split();

    // 2. Send Hello
    let cfg = config.read().await;
    let hello = hello_frame(
        frame_ids.next(),
        &cfg.client_id,
        cfg.session_id.as_deref(),
        cfg.epoch,
        &cfg.codecs,
        &cfg.token,
        &cfg.last_offsets,
        &cfg.features,
    );
    drop(cfg);
    let hello_bytes = encode_frame(&hello)?;
    writer.send(WsMessage::Binary(hello_bytes)).await?;

    // 3. Await Welcome and Ready
    let (welcome, ready) = await_handshake(&mut reader).await?;

    // 4. Update config with session info
    {
        let mut cfg = config.write().await;
        cfg.session_id = Some(welcome.session_id.clone());
        cfg.epoch = ready.epoch;
    }

    // 5. Build inner state
    let writer = Arc::new(Mutex::new(writer));
    let pending_replies: Arc<Mutex<HashMap<String, oneshot::Sender<Reply>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let disconnect_notify = Arc::new(Notify::new());

    // 6. Spawn heartbeat
    let (heartbeat_handle, heartbeat_state) =
        heartbeat::spawn_heartbeat(Arc::clone(&writer), &ready, frame_ids.clone());

    // 7. Spawn reader task
    let reader_event_tx = event_tx.clone();
    let reader_pending = Arc::clone(&pending_replies);
    let reader_notify = Arc::clone(&disconnect_notify);
    let reader_hb_state = Arc::clone(&heartbeat_state);
    let heartbeat_handle_arc = Arc::new(Mutex::new(Some(heartbeat_handle)));

    tokio::spawn(async move {
        reader_task(
            reader,
            reader_event_tx,
            reader_pending,
            reader_notify,
            reader_hb_state,
        )
        .await;
        // Clean up heartbeat
        if let Some(h) = heartbeat_handle_arc.lock().await.take() {
            h.abort();
        }
    });

    let inner = Arc::new(ConnectionInner {
        writer,
        session_id: welcome.session_id,
        epoch: ready.epoch,
        ready,
        pending_replies,
        disconnect_notify,
        heartbeat_state,
    });

    // 8. Re-subscribe all tracked topics
    let subs = subscriptions.lock().await;
    let mut frames = Vec::new();
    for (topic, mode) in subs.iter() {
        let f =
            super::frame_builder::subscribe_frame(frame_ids.next(), topic, mode_str(*mode), None);
        frames.push(f);
    }
    drop(subs);

    // Re-subscribe each tracked subscription. If a single frame fails
    // to send, log a warning and continue so a single broken topic
    // does not block the rest of the resubscribe set.
    for f in &frames {
        let bytes = match encode_frame(f) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(error = %e, "failed to encode resubscribe frame");
                continue;
            }
        };
        if let Err(e) = inner
            .writer
            .lock()
            .await
            .send(WsMessage::Binary(bytes))
            .await
        {
            tracing::warn!(error = %e, "failed to send resubscribe frame");
            continue;
        }
    }

    // 9. Emit connected
    let _ = event_tx.send(ClientEvent::Connected {
        session_id: inner.session_id.clone(),
        epoch: inner.epoch,
    });

    Ok(inner)
}

/// Wait for Welcome then Ready control frames over the reader.
async fn await_handshake(reader: &mut WsReader) -> Result<(Welcome, Ready)> {
    let mut welcome: Option<Welcome> = None;
    let mut ready: Option<Ready> = None;

    loop {
        let msg = reader
            .next()
            .await
            .ok_or_else(|| ClientError::Handshake("connection closed before Ready".into()))?
            .map_err(|e| ClientError::Other(format!("read error: {e}")))?;

        match msg {
            WsMessage::Binary(data) => {
                let frame = decode_binary_frame(&data, DEFAULT_MAX_BINARY_PAYLOAD)?;
                if frame.frame_type != FrameType::Control {
                    continue;
                }
                if let Some(ref payload) = frame.payload {
                    let obj: JsonValue = serde_json::from_slice(payload)?;
                    parse_handshake(&obj, &mut welcome, &mut ready);
                }
            }
            WsMessage::Text(text) => {
                let obj: JsonValue = serde_json::from_str(&text)?;
                parse_handshake(&obj, &mut welcome, &mut ready);
            }
            _ => {}
        }

        if let (Some(w), Some(r)) = (welcome.clone(), ready.clone()) {
            return Ok((w, r));
        }
    }
}

fn parse_handshake(obj: &JsonValue, welcome: &mut Option<Welcome>, ready: &mut Option<Ready>) {
    match obj.get("type").and_then(|v| v.as_str()) {
        Some("welcome") => {
            let w = Welcome {
                session_id: obj
                    .get("session_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
                epoch: obj.get("epoch").and_then(|v| v.as_u64()).unwrap_or(0) as u32,
                negotiated_codec: match obj.get("negotiated_codec").and_then(|v| v.as_str()) {
                    Some("cbor") | Some("Cbor") => Codec::Cbor,
                    _ => Codec::Json,
                },
                negotiated_compression: obj
                    .get("negotiated_compression")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                server_time: obj.get("server_time").and_then(|v| v.as_i64()).unwrap_or(0),
                resume_window_ms: obj
                    .get("resume_window_ms")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0) as u32,
                features: obj
                    .get("features")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default(),
            };
            *welcome = Some(w);
        }
        Some("ready") => {
            let r = Ready {
                session_id: obj
                    .get("session_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
                epoch: obj.get("epoch").and_then(|v| v.as_u64()).unwrap_or(0) as u32,
                ping_interval_ms: obj
                    .get("ping_interval_ms")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(25_000) as u32,
                pong_timeout_ms: obj
                    .get("pong_timeout_ms")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(10_000) as u32,
                max_missed_pongs: obj
                    .get("max_missed_pongs")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(2) as u32,
                idle_timeout_ms: obj
                    .get("idle_timeout_ms")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(300_000) as u32,
                jitter_ms: obj
                    .get("jitter_ms")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(2_500) as u32,
                max_payload_bytes: obj
                    .get("max_payload_bytes")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(65_536) as u32,
                max_topics_per_connection: obj
                    .get("max_topics_per_connection")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(128) as u32,
                max_send_queue_bytes: obj
                    .get("max_send_queue_bytes")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(1_048_576) as u32,
                server_time: obj.get("server_time").and_then(|v| v.as_i64()).unwrap_or(0),
            };
            *ready = Some(r);
        }
        _ => {}
    }
}

/// Background task that continuously reads WebSocket messages and dispatches
/// them as `ClientEvent`s through the broadcast channel.
async fn reader_task(
    mut reader: WsReader,
    event_tx: broadcast::Sender<ClientEvent>,
    pending_replies: Arc<Mutex<HashMap<String, oneshot::Sender<Reply>>>>,
    disconnect_notify: Arc<Notify>,
    heartbeat_state: Arc<HeartbeatState>,
) {
    loop {
        let msg = match reader.next().await {
            Some(Ok(m)) => m,
            Some(Err(e)) => {
                let _ = event_tx.send(ClientEvent::Error(format!("ws read: {e}")));
                break;
            }
            None => break,
        };

        match msg {
            WsMessage::Binary(data) => {
                let frame = match decode_binary_frame(&data, DEFAULT_MAX_BINARY_PAYLOAD) {
                    Ok(f) => f,
                    Err(e) => {
                        // Use tracing::warn rather than broadcasting a
                        // ClientEvent::Error, which would let a
                        // misbehaving server flood the broadcast
                        // channel (capacity 1024) and cause legitimate
                        // events to be dropped.
                        tracing::warn!(error = %e, "frame decode failed");
                        continue;
                    }
                };
                dispatch_frame(frame, &event_tx, &pending_replies, &heartbeat_state).await;
            }
            WsMessage::Text(text) => {
                match serde_json::from_str::<JsonValue>(&text) {
                    Ok(obj) => {
                        dispatch_text_envelope(&obj, &event_tx, &pending_replies, &heartbeat_state)
                            .await;
                    }
                    Err(_) => {
                        // Try JSON envelope as a frame
                        match crate::transport::frame_codec::decode_text_frame(text.as_bytes()) {
                            Ok(frame) => {
                                dispatch_frame(
                                    frame,
                                    &event_tx,
                                    &pending_replies,
                                    &heartbeat_state,
                                )
                                .await;
                            }
                            Err(e) => {
                                let _ =
                                    event_tx.send(ClientEvent::Error(format!("text decode: {e}")));
                            }
                        }
                    }
                }
            }
            WsMessage::Close(_) => {
                break;
            }
            _ => {}
        }
    }

    // Connection lost — notify reconnect logic
    disconnect_notify.notify_one();
}

async fn dispatch_text_envelope(
    obj: &JsonValue,
    event_tx: &broadcast::Sender<ClientEvent>,
    pending_replies: &Arc<Mutex<HashMap<String, oneshot::Sender<Reply>>>>,
    heartbeat_state: &Arc<HeartbeatState>,
) {
    let msg_type = obj.get("type").and_then(|v| v.as_str());

    match msg_type {
        Some("pong") => {
            heartbeat_state.reset_missed();
            let ts = obj.get("timestamp").and_then(|v| v.as_i64()).unwrap_or(0);
            let _ = event_tx.send(ClientEvent::Pong { timestamp: ts });
        }
        Some("welcome") | Some("ready") | Some("subscribe_ack") | Some("unsubscribe_ack") => {
            // Handled elsewhere; no event for now
        }
        _ => {
            // Try decoding as a data frame from a JSON envelope
            if let Ok(frame) = crate::transport::frame_codec::decode_text_frame(
                &serde_json::to_vec(obj).unwrap_or_default(),
            ) {
                dispatch_frame(frame, event_tx, pending_replies, heartbeat_state).await;
            }
        }
    }
}

async fn dispatch_frame(
    frame: Frame,
    event_tx: &broadcast::Sender<ClientEvent>,
    pending_replies: &Arc<Mutex<HashMap<String, oneshot::Sender<Reply>>>>,
    heartbeat_state: &Arc<HeartbeatState>,
) {
    match frame.frame_type {
        FrameType::Flow => {
            if let Some(ref payload) = frame.payload
                && let Ok(obj) = serde_json::from_slice::<JsonValue>(payload)
                && obj.get("type").and_then(|v| v.as_str()) == Some("pong")
            {
                heartbeat_state.reset_missed();
                let ts = obj.get("timestamp").and_then(|v| v.as_i64()).unwrap_or(0);
                let _ = event_tx.send(ClientEvent::Pong { timestamp: ts });
            }
        }
        FrameType::Ack => {
            if let Some(ref payload) = frame.payload
                && let Ok(obj) = serde_json::from_slice::<JsonValue>(payload)
            {
                let message_id = obj
                    .get("message_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let status_str = obj
                    .get("status")
                    .and_then(|v| v.as_str())
                    .unwrap_or("received");
                let status = match status_str {
                    "received" => AckStatus::Received,
                    "accepted" => AckStatus::Accepted,
                    "persisted" => AckStatus::Persisted,
                    "delivered" => AckStatus::Delivered,
                    "processed" => AckStatus::Processed,
                    "rejected" => AckStatus::Rejected,
                    "expired" => AckStatus::Expired,
                    "duplicate" => AckStatus::Duplicate,
                    "failed" => AckStatus::Failed,
                    _ => AckStatus::Received,
                };
                let _ = event_tx.send(ClientEvent::AckReceived { message_id, status });
            }
        }
        FrameType::Data | FrameType::Control => {
            if let Some(ref payload) = frame.payload
                && let Ok(obj) = serde_json::from_slice::<JsonValue>(payload)
            {
                let msg_class = obj.get("class").and_then(|v| v.as_str());
                let topic = frame
                    .topic
                    .as_deref()
                    .or_else(|| obj.get("topic").and_then(|v| v.as_str()))
                    .map(String::from);

                match msg_class {
                    Some("event") => {
                        if let Ok(event) =
                            serde_json::from_value::<crate::message::event::Event>(obj)
                        {
                            let _ = event_tx.send(ClientEvent::EventReceived {
                                topic: topic.unwrap_or_default(),
                                event,
                            });
                        }
                    }
                    Some("reply") => {
                        if let Ok(reply) = serde_json::from_value::<Reply>(obj.clone()) {
                            let corr_id = reply.correlation_id.clone();
                            let mut pending = pending_replies.lock().await;
                            if let Some(sender) = pending.remove(&corr_id) {
                                let _ = sender.send(reply.clone());
                            }
                            let _ = event_tx.send(ClientEvent::ReplyReceived { reply });
                        }
                    }
                    Some("state") => {
                        if let Ok(state) =
                            serde_json::from_value::<crate::message::state::State>(obj)
                        {
                            let _ = event_tx.send(ClientEvent::StateReceived {
                                topic: topic.unwrap_or_default(),
                                state,
                            });
                        }
                    }
                    Some("datagram") => {
                        if let Ok(datagram) =
                            serde_json::from_value::<crate::message::datagram::Datagram>(obj)
                        {
                            let _ = event_tx.send(ClientEvent::DatagramReceived {
                                topic: topic.unwrap_or_default(),
                                datagram,
                            });
                        }
                    }
                    Some("stream") => {
                        if let Ok(segment) =
                            serde_json::from_value::<crate::message::stream::StreamSegment>(obj)
                        {
                            let _ = event_tx.send(ClientEvent::StreamReceived {
                                topic: topic.unwrap_or_default(),
                                segment,
                            });
                        }
                    }
                    Some("snapshot") => {
                        if let Ok(snapshot) =
                            serde_json::from_value::<crate::message::snapshot::Snapshot>(obj)
                        {
                            let _ = event_tx.send(ClientEvent::SnapshotReceived {
                                topic: topic.unwrap_or_default(),
                                snapshot,
                            });
                        }
                    }
                    Some("system") => {
                        let event_name = obj
                            .get("event")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        let payload = obj.get("payload").cloned().unwrap_or(JsonValue::Null);
                        let _ = event_tx.send(ClientEvent::System {
                            event_name,
                            payload,
                        });
                    }
                    _ => {}
                }
            }
        }
        FrameType::Error => {
            let msg = format!("{}", frame);
            let _ = event_tx.send(ClientEvent::Error(msg));
        }
    }
}

fn mode_str(mode: SubscribeIntent) -> &'static str {
    match mode {
        SubscribeIntent::Live => "live",
        SubscribeIntent::Replay { .. } => "replay",
        SubscribeIntent::SnapshotThenLive => "snapshot_then_live",
        SubscribeIntent::Latest => "latest",
        SubscribeIntent::Passive => "passive",
        SubscribeIntent::Ephemeral => "ephemeral",
    }
}
