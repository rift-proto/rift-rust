use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures_util::SinkExt;
use tokio::sync::{Mutex, RwLock, broadcast, oneshot};
use tokio_tungstenite::tungstenite::Message as WsMessage;

use crate::ack::AckStatus;
use crate::broker::SubscribeIntent;
use crate::frame::{Frame, Priority};
use crate::message::command::Reply;
use crate::transport::frame_codec::encode_frame;

use super::config::RiftClientConfig;
use super::connection::ConnectionInner;
use super::error::{ClientError, Result};
use super::events::ClientEvent;
use super::frame_builder::{self, FrameIdCounter};
use super::subscriber::SubscriptionTracker;

/// Async Rift realtime client.
///
/// Create with [`RiftClient::new`], connect with [`RiftClient::connect`],
/// then use the publish/subscribe methods to interact with topics.
///
/// Obtain a stream of incoming events via [`RiftClient::subscribe_events`].
pub struct RiftClient {
    url: String,
    config: Arc<RwLock<RiftClientConfig>>,
    inner: Arc<RwLock<Option<Arc<ConnectionInner>>>>,
    event_tx: broadcast::Sender<ClientEvent>,
    subscriptions: Arc<Mutex<SubscriptionTracker>>,
    closed: Arc<AtomicBool>,
    frame_ids: FrameIdCounter,
}

/// Options for [`RiftClient::publish`].
#[derive(Debug, Default, Clone)]
pub struct PublishOpts {
    /// Optional deduplication key.
    pub dedupe_key: Option<String>,
    /// Optional ordering key for partition ordering.
    pub ordering_key: Option<String>,
    /// Time-to-live in milliseconds.
    pub ttl_ms: Option<u32>,
    /// Message priority.
    pub priority: Option<Priority>,
}

/// Options for [`RiftClient::command`].
#[derive(Debug, Default, Clone)]
pub struct CommandOpts {
    /// Command timeout in milliseconds. Defaults to 5000.
    pub timeout_ms: Option<u64>,
    /// Optional idempotency key.
    pub idempotency_key: Option<String>,
    /// Message priority.
    pub priority: Option<Priority>,
}

/// Options for [`RiftClient::publish_state`].
#[derive(Debug, Default, Clone)]
pub struct StateOpts {
    /// Optional state name.
    pub name: Option<String>,
    /// Time-to-live in milliseconds.
    pub ttl_ms: Option<u32>,
    /// Optional subject identifier.
    pub subject: Option<String>,
}

impl RiftClient {
    /// Create a new client. Does **not** connect yet.
    pub fn new(url: impl Into<String>, config: RiftClientConfig) -> Self {
        let (event_tx, _) = broadcast::channel(1024);
        Self {
            url: url.into(),
            config: Arc::new(RwLock::new(config)),
            inner: Arc::new(RwLock::new(None)),
            event_tx,
            subscriptions: Arc::new(Mutex::new(SubscriptionTracker::new())),
            closed: Arc::new(AtomicBool::new(false)),
            frame_ids: FrameIdCounter::new(),
        }
    }

    // ── Connection lifecycle ─────────────────────────────────────────────

    /// Connect to the Rift server, perform the Hello/Welcome/Ready handshake,
    /// and start the heartbeat loop. Returns after the Ready frame is received.
    pub async fn connect(&self) -> Result<()> {
        // Atomic check-and-install: take the write lock once to read the
        // current state and insert the new connection without a window
        // where two concurrent `connect()` calls could each see "empty"
        // and both open a WebSocket.
        let mut guard = self.inner.write().await;
        if guard.is_some() {
            return Err(ClientError::AlreadyConnected);
        }
        self.closed.store(false, Ordering::SeqCst);

        let inner = super::connection::connect(
            &self.url,
            &self.config,
            &self.event_tx,
            Arc::clone(&self.subscriptions),
            self.frame_ids.clone(),
        )
        .await?;

        let conn = inner.clone();
        let disconnect_notify = conn.disconnect_notify.clone();
        *guard = Some(inner);
        drop(guard);
        let event_tx = self.event_tx.clone();
        let config = Arc::clone(&self.config);
        let subscriptions = Arc::clone(&self.subscriptions);
        let closed = Arc::clone(&self.closed);
        let url = self.url.clone();
        let frame_ids = self.frame_ids.clone();
        let inner_slot = self.inner.clone();
        // Hold the inner Arc inside the monitor task; releasing
        // it after disconnect frees the connection resources.
        let _hold_arc = conn;

        tokio::spawn(async move {
            disconnect_notify.notified().await;
            // Dropping the captured inner Arc releases the
            // connection resources held by the reader / heartbeat
            // tasks once they exit.
            drop(_hold_arc);
            // Emit disconnect
            let _ = event_tx.send(ClientEvent::Disconnected {
                code: 1006,
                reason: "connection lost".into(),
            });
            // Auto-reconnect
            if !closed.load(Ordering::SeqCst) {
                let cfg = config.read().await;
                let auto = cfg.auto_reconnect;
                let max_attempts = cfg.max_reconnect_attempts;
                let base_delay = cfg.reconnect_delay;
                drop(cfg);

                if auto {
                    try_reconnect(
                        &url,
                        &config,
                        &event_tx,
                        inner_slot.as_ref(),
                        subscriptions,
                        closed,
                        frame_ids,
                        max_attempts,
                        base_delay,
                    )
                    .await;
                }
            }
        });

        Ok(())
    }

    /// Gracefully close the connection and stop auto-reconnect.
    pub async fn close(&self) -> Result<()> {
        self.closed.store(true, Ordering::SeqCst);
        let mut guard = self.inner.write().await;
        if let Some(inner) = guard.take() {
            let mut writer = inner.writer.lock().await;
            let _ = writer.send(WsMessage::Close(None)).await;
            let _ = writer.flush().await;
        }
        Ok(())
    }

    /// Returns `true` if connected and the handshake is complete.
    pub async fn is_connected(&self) -> bool {
        self.inner.read().await.is_some()
    }

    /// Returns the current session ID, or `None` if not connected.
    pub async fn session_id(&self) -> Option<String> {
        self.inner
            .read()
            .await
            .as_ref()
            .map(|i| i.session_id.clone())
    }

    /// Returns the current epoch.
    pub async fn epoch(&self) -> u32 {
        self.inner
            .read()
            .await
            .as_ref()
            .map(|i| i.epoch)
            .unwrap_or(1)
    }

    // ── Event subscription ───────────────────────────────────────────────

    /// Obtain a broadcast receiver for [`ClientEvent`]s emitted by this client.
    pub fn subscribe_events(&self) -> broadcast::Receiver<ClientEvent> {
        self.event_tx.subscribe()
    }

    // ── Subscribe / Unsubscribe ──────────────────────────────────────────

    /// Subscribe to a topic. The subscription is tracked for auto-resubscribe
    /// on reconnect.
    pub async fn subscribe(
        &self,
        topic: &str,
        mode: SubscribeIntent,
        from_offset: Option<i64>,
    ) -> Result<()> {
        self.require_connected().await?;
        {
            let mut subs = self.subscriptions.lock().await;
            subs.add(topic, mode);
        }
        let frame = frame_builder::subscribe_frame(
            self.frame_ids.next(),
            topic,
            mode_str(mode),
            from_offset,
        );
        self.send_frame(frame).await
    }

    /// Unsubscribe from a topic.
    pub async fn unsubscribe(&self, topic: &str) -> Result<()> {
        self.require_connected().await?;
        {
            let mut subs = self.subscriptions.lock().await;
            subs.remove(topic);
        }
        let frame = frame_builder::unsubscribe_frame(self.frame_ids.next(), topic);
        self.send_frame(frame).await
    }

    // ── Publish ──────────────────────────────────────────────────────────

    /// Publish an event to a topic.
    pub async fn publish(
        &self,
        topic: &str,
        event: &str,
        schema: &str,
        payload: serde_json::Value,
        opts: Option<PublishOpts>,
    ) -> Result<()> {
        self.require_connected().await?;
        let opts = opts.unwrap_or_default();
        let message_id = ulid::Ulid::new().to_string();
        let frame = frame_builder::event_frame(
            self.frame_ids.next(),
            topic,
            event,
            &message_id,
            schema,
            payload,
            opts.dedupe_key.as_deref(),
            opts.ordering_key.as_deref(),
            opts.ttl_ms,
            opts.priority,
        );
        self.send_frame(frame).await
    }

    /// Send a command and await the reply. Times out after `timeout_ms`
    /// (default 5 000 ms).
    pub async fn command(
        &self,
        topic: &str,
        cmd: &str,
        schema: &str,
        payload: serde_json::Value,
        opts: Option<CommandOpts>,
    ) -> Result<Reply> {
        self.require_connected().await?;
        let opts = opts.unwrap_or_default();
        let timeout_ms = opts.timeout_ms.unwrap_or(5_000);
        let correlation_id = uuid::Uuid::now_v7().to_string();

        let frame = frame_builder::command_frame(
            self.frame_ids.next(),
            topic,
            cmd,
            &correlation_id,
            timeout_ms,
            schema,
            payload,
            opts.idempotency_key.as_deref(),
            opts.priority,
        );

        // Register the pending reply and pre-compute the cleanup
        // closure so every error / drop path removes the entry.
        let (tx, rx) = oneshot::channel::<Reply>();
        let pending_cid = correlation_id.clone();
        let inner_for_cleanup = Arc::clone(&self.inner);
        let cleanup = move || {
            let inner_slot = inner_for_cleanup;
            let cid = pending_cid;
            tokio::spawn(async move {
                let inner = inner_slot.read().await;
                if let Some(conn) = inner.as_ref() {
                    conn.pending_replies.lock().await.remove(&cid);
                }
            });
        };
        // Tracks whether the reply has been received. The match
        // arms below either run `cleanup()` (on every error /
        // timeout path) or succeed (on the `Ok(Ok(reply))` path);
        // we never fall back to a guard.
        {
            let inner = self.inner.read().await;
            let conn = inner.as_ref().ok_or(ClientError::NotConnected)?;
            let mut pending = conn.pending_replies.lock().await;
            pending.insert(correlation_id.clone(), tx);
        }

        // Send the command frame. On any error here, the reply is
        // never awaited, so clean up the entry immediately.
        if let Err(e) = self.send_frame(frame).await {
            cleanup();
            return Err(e);
        }

        // Await reply or timeout.
        match tokio::time::timeout(Duration::from_millis(timeout_ms), rx).await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(_)) => {
                // Sender dropped -- connection closed. Clean up
                // proactively (the reader task may have dropped
                // the sender without removing the entry).
                cleanup();
                Err(ClientError::NotConnected)
            }
            Err(_) => {
                // Timeout. The reply may still arrive later; the
                // entry will be removed when the reader task
                // attempts to dispatch the late reply to a
                // dropped oneshot.
                cleanup();
                Err(ClientError::CommandTimeout(timeout_ms))
            }
        }
    }

    /// Publish a state message to a topic.
    pub async fn publish_state(
        &self,
        topic: &str,
        state_key: &str,
        value: serde_json::Value,
        opts: Option<StateOpts>,
    ) -> Result<()> {
        self.require_connected().await?;
        let opts = opts.unwrap_or_default();
        let frame = frame_builder::state_frame(
            self.frame_ids.next(),
            topic,
            state_key,
            value,
            opts.name.as_deref(),
            opts.ttl_ms,
            opts.subject.as_deref(),
        );
        self.send_frame(frame).await
    }

    /// Send a high-frequency datagram to a topic.
    pub async fn send_datagram(
        &self,
        topic: &str,
        schema: &str,
        payload: serde_json::Value,
        event: Option<&str>,
    ) -> Result<()> {
        self.require_connected().await?;
        let frame =
            frame_builder::datagram_frame(self.frame_ids.next(), topic, schema, event, payload);
        self.send_frame(frame).await
    }

    /// Send a stream segment to a topic.
    pub async fn send_stream_segment(
        &self,
        topic: &str,
        stream_id: &str,
        seq: u64,
        schema: &str,
        payload: serde_json::Value,
        final_segment: bool,
    ) -> Result<()> {
        self.require_connected().await?;
        let frame = frame_builder::stream_frame(
            self.frame_ids.next(),
            topic,
            stream_id,
            seq,
            schema,
            payload,
            final_segment,
        );
        self.send_frame(frame).await
    }

    // ── Ack ──────────────────────────────────────────────────────────────

    /// Send an acknowledgement for a received message.
    pub async fn ack(&self, message_id: &str, status: AckStatus) -> Result<()> {
        self.require_connected().await?;
        let frame = frame_builder::ack_frame(self.frame_ids.next(), message_id, status.as_str());
        self.send_frame(frame).await
    }

    // ── Internal helpers ─────────────────────────────────────────────────

    async fn require_connected(&self) -> Result<()> {
        if self.inner.read().await.is_none() {
            return Err(ClientError::NotConnected);
        }
        Ok(())
    }

    async fn send_frame(&self, frame: Frame) -> Result<()> {
        let inner = self.inner.read().await;
        let conn = inner.as_ref().ok_or(ClientError::NotConnected)?;
        let bytes = encode_frame(&frame)?;
        let mut writer = conn.writer.lock().await;
        writer.send(WsMessage::Binary(bytes)).await?;
        writer.flush().await?;
        Ok(())
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

#[allow(clippy::too_many_arguments)]
async fn try_reconnect(
    url: &str,
    config: &Arc<RwLock<RiftClientConfig>>,
    event_tx: &broadcast::Sender<ClientEvent>,
    inner_slot: &RwLock<Option<Arc<ConnectionInner>>>,
    subscriptions: Arc<Mutex<SubscriptionTracker>>,
    closed: Arc<AtomicBool>,
    frame_ids: FrameIdCounter,
    max_attempts: u32,
    base_delay: Duration,
) {
    for attempt in 1..=max_attempts {
        if closed.load(Ordering::SeqCst) {
            return;
        }
        let _ = event_tx.send(ClientEvent::Reconnecting { attempt });
        // Exponential backoff with jitter: base * 2^(attempt-1),
        // bounded by 32x base, plus up to base of random jitter.
        let exp = base_delay.saturating_mul(1u32 << (attempt - 1).min(5));
        let jitter =
            Duration::from_millis(rand::random::<u64>() % base_delay.as_millis().max(1) as u64);
        let delay = exp + jitter;
        tokio::time::sleep(delay).await;
        if closed.load(Ordering::SeqCst) {
            return;
        }
        // Bump epoch
        {
            let mut cfg = config.write().await;
            cfg.epoch += 1;
        }
        match super::connection::connect(
            url,
            config.as_ref(),
            event_tx,
            Arc::clone(&subscriptions),
            frame_ids.clone(),
        )
        .await
        {
            Ok(new_inner) => {
                tracing::info!(attempt, "reconnected");
                let sid = new_inner.session_id.clone();
                let ep = new_inner.epoch;
                *inner_slot.write().await = Some(new_inner);
                let _ = event_tx.send(ClientEvent::Connected {
                    session_id: sid,
                    epoch: ep,
                });
                return;
            }
            Err(e) => {
                tracing::warn!(attempt, "reconnect failed: {e}");
            }
        }
    }
    let _ = event_tx.send(ClientEvent::Error(format!(
        "max reconnect attempts ({max_attempts}) exceeded"
    )));
}
