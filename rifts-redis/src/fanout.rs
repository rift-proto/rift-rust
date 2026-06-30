//! Redis Pub/Sub -> local fanout bridge.
//!
//! The [`FanoutBridge`] manages cross-instance message delivery.
//! Each `rifts` instance that uses [`RedisActorBroker`] starts a
//! background tokio task that subscribes to Redis Pub/Sub channels
//! for every topic that has local subscribers.
//!
//! When a message arrives on `rift:fanout:{topic}`, the bridge
//! decodes it and delivers it to all local subscribers via the
//! local [`FanoutEngine`](rifts_broker::broker::FanoutEngine).
//!
//! ## Wire format
//!
//! Messages published to Redis Pub/Sub carry a JSON-encoded Frame
//! envelope so that cross-instance subscribers receive the full
//! topic/event/message_id/session_id/timestamp metadata, not just
//! the raw payload.
//!
//! ## Lifecycle
//!
//! - **Subscribe** -- when the first local subscriber joins a topic,
//!   the broker calls [`FanoutBridge::ensure_topic`] to open a
//!   Redis Pub/Sub subscription for that topic.
//! - **Unsubscribe** -- when the last local subscriber leaves, the
//!   broker calls [`FanoutBridge::drop_topic`] to unsubscribe from
//!   the Redis channel.
//! - **Shutdown** -- when the broker is dropped, the bridge task
//!   exits and all Pub/Sub subscriptions are cleaned up.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::Notify;
use tracing::{error, info, warn};

use rifts_broker::broker::{ConnectionSink, FanoutEngine, SubscribeIntent};
use rifts_core::Frame;

use crate::connection::RedisPool;

/// Per-topic subscriber count tracked by the fanout bridge.
struct TopicState {
    /// How many local subscribers are active for this topic.
    count: usize,
}

/// Bridge between Redis Pub/Sub and local fanout.
///
/// Spawns a background task that listens for Redis Pub/Sub messages
/// on channels matching `{prefix}:fanout:*` and delivers them to
/// local subscribers via the [`FanoutEngine`].
pub struct FanoutBridge {
    /// Redis connection pool for Pub/Sub.
    pool: RedisPool,
    /// Local fanout engine shared with the broker.
    fanout: FanoutEngine,
    /// Active topic subscriptions and their local subscriber counts.
    topics: DashMap<String, TopicState>,
    /// Shutdown signal for the background listener task.
    shutdown: Arc<Notify>,
}

impl FanoutBridge {
    /// Create a new fanout bridge and spawn the background listener.
    ///
    /// The background task will run until `shutdown` is notified.
    pub fn new(pool: RedisPool) -> Arc<Self> {
        let shutdown = Arc::new(Notify::new());
        let bridge = Arc::new(Self {
            pool,
            fanout: FanoutEngine::new(),
            topics: DashMap::new(),
            shutdown: shutdown.clone(),
        });

        // Spawn the background Pub/Sub listener.
        let bridge_clone = bridge.clone();
        tokio::spawn(async move {
            bridge_clone.listen_loop().await;
        });

        bridge
    }

    /// Return a reference to the local fanout engine.
    pub fn fanout(&self) -> &FanoutEngine {
        &self.fanout
    }

    /// Register a local subscriber for a topic.
    pub fn ensure_topic(&self, topic: &str) {
        let mut entry = self
            .topics
            .entry(topic.to_string())
            .or_insert(TopicState { count: 0 });
        entry.count += 1;
    }

    /// Remove a local subscriber from a topic.
    pub fn drop_topic(&self, topic: &str) {
        self.topics.remove(topic);
    }

    /// Deliver a cross-instance message to all local subscribers.
    ///
    /// The payload is a JSON-encoded [`Frame`] envelope. The offset
    /// is extracted from the fanout wire format header produced by
    /// [`serialize_frame_for_fanout`].
    fn deliver_to_local(&self, topic: &str, frame: Frame) {
        let framed = rifts_broker::broker::serialize_frame_for_fanout(&frame, 0);
        self.fanout.deliver(topic, framed);
    }

    /// Subscribe a connection sink to a topic and return the subscription ID.
    pub fn subscribe(
        &self,
        topic: &str,
        intent: SubscribeIntent,
        sink: ConnectionSink,
    ) -> rifts_broker::broker::SubscriptionId {
        self.ensure_topic(topic);
        self.fanout.subscribe(topic, intent, sink)
    }

    /// Unsubscribe by subscription ID.
    pub fn unsubscribe(&self, id: rifts_broker::broker::SubscriptionId) -> Option<String> {
        self.fanout.unsubscribe(id)
    }

    /// Drop all subscriptions for a given sink.
    pub fn drop_sink(&self, sink_id: u64) -> Vec<String> {
        self.fanout.drop_sink(sink_id)
    }

    /// Count subscribers for a topic.
    pub fn topic_subscriber_count(&self, topic: &str) -> usize {
        self.fanout.topic_subscriber_count(topic)
    }

    /// Background loop: listen on Redis Pub/Sub for cross-instance
    /// messages and forward them to local subscribers.
    async fn listen_loop(&self) {
        use futures_util::StreamExt;
        let prefix = self.pool.prefix().to_string();
        let channel_pattern = format!("{prefix}:fanout:*");

        // Retry loop -- reconnects on Pub/Sub connection failure.
        let backoff_base = Duration::from_secs(1);
        let backoff_max = Duration::from_secs(30);
        let mut retry_count = 0u32;

        loop {
            let client = match redis::Client::open(self.pool.url()) {
                Ok(c) => c,
                Err(e) => {
                    error!(error = %e, "redis fanout: failed to open client");
                    retry_count += 1;
                    let delay = backoff_base
                        .saturating_mul(retry_count.min(5))
                        .min(backoff_max);
                    tokio::select! {
                        _ = self.shutdown.notified() => return,
                        _ = tokio::time::sleep(delay) => {}
                    }
                    continue;
                }
            };

            let mut pubsub = match client.get_async_pubsub().await {
                Ok(ps) => ps,
                Err(e) => {
                    warn!(error = %e, "redis fanout: failed to get pubsub connection");
                    retry_count += 1;
                    let delay = backoff_base
                        .saturating_mul(retry_count.min(5))
                        .min(backoff_max);
                    tokio::select! {
                        _ = self.shutdown.notified() => return,
                        _ = tokio::time::sleep(delay) => {}
                    }
                    continue;
                }
            };

            if let Err(e) = pubsub.psubscribe(&channel_pattern).await {
                warn!(error = %e, pattern = %channel_pattern, "redis fanout: psubscribe failed");
                retry_count += 1;
                let delay = backoff_base
                    .saturating_mul(retry_count.min(5))
                    .min(backoff_max);
                tokio::select! {
                    _ = self.shutdown.notified() => return,
                    _ = tokio::time::sleep(delay) => {}
                }
                continue;
            }

            info!(pattern = %channel_pattern, "redis fanout: subscribed");
            retry_count = 0;

            let mut stream = pubsub.into_on_message();

            loop {
                tokio::select! {
                    _ = self.shutdown.notified() => {
                        return;
                    }
                    msg = stream.next() => {
                        let Some(msg) = msg else {
                            // Stream ended -- reconnect.
                            warn!("redis fanout: Pub/Sub stream ended, reconnecting");
                            break;
                        };
                        let payload: Bytes = Bytes::from(msg.get_payload_bytes().to_vec());
                        let channel: String = msg.get_channel_name().into();
                        let topic = channel
                            .strip_prefix(&format!("{}:fanout:", prefix))
                            .unwrap_or(&channel);

                        // Try to decode the payload as a JSON Frame envelope.
                        // If decoding fails, fall back to treating the payload
                        // as raw bytes in a minimal frame.
                        let frame = match serde_json::from_slice::<Frame>(&payload) {
                            Ok(mut f) => {
                                f.topic = Some(topic.to_string());
                                f
                            }
                            Err(_) => Frame {
                                topic: Some(topic.to_string()),
                                payload: Some(payload),
                                ..Frame::default()
                            },
                        };
                        self.deliver_to_local(topic, frame);
                    }
                }
            }
        }
    }
}
