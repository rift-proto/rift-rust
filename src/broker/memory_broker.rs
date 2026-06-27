//! In-memory broker — orchestrates topic store, dedupe, offsets,
//! log, snapshots, and fanout (spec §22).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;

use crate::broker::broker::{Broker, PublishOutcome};
use crate::broker::fanout::{ConnectionSink, FanoutEngine, SubscribeIntent, SubscriptionId};
use crate::broker::router::{LocalRouter, Route, TopicRouter};
use crate::error::{MessageReject, Result, RiftError, TopicReject};
use crate::frame::Frame;
use crate::message::MessageClass;
use crate::now_ms;
use crate::storage::{
    DedupeStore, LogStore, MemoryDedupeStore, MemoryLogStore, MemoryOffsetStore,
    MemorySnapshotStore, OffsetStore, SnapshotStore,
};
use crate::topic::TopicStore;
use crate::topic::store::LogEntry;

/// Single-process broker, generic over storage backends.
///
/// Type parameters correspond to the four persistence traits:
/// - `O`: [`OffsetStore`](crate::storage::OffsetStore) — per-topic offset allocation
/// - `L`: [`LogStore`](crate::storage::LogStore) — append + range-query message log
/// - `D`: [`DedupeStore`](crate::storage::DedupeStore) — deduplication
/// - `S`: [`SnapshotStore`](crate::storage::SnapshotStore) — snapshot capture and retrieval
///
/// Use the type aliases for common configurations:
/// - [`DefaultBroker`] — all memory-backed (development)
/// - [`SledBroker`] — all sled-backed (production, feature `sled`)
pub struct InMemoryBroker<O, L, D, S> {
    /// In-memory topic metadata store.
    pub store: TopicStore,
    /// Per-topic offset allocator.
    pub offsets: O,
    /// Message log (append, range, retention).
    pub log: L,
    /// Deduplication store.
    pub dedupe: D,
    /// Snapshot store.
    pub snapshots: S,
    /// Fanout engine.
    pub fanout: FanoutEngine,
    /// Topic router.
    pub router: Mutex<Box<dyn TopicRouter>>,
    /// Deduplication window duration.
    pub dedupe_window: Duration,
    /// Maximum payload bytes allowed.
    pub max_payload_bytes: usize,
}

/// All in-memory stores. Default for development.
pub type DefaultBroker =
    InMemoryBroker<MemoryOffsetStore, MemoryLogStore, MemoryDedupeStore, MemorySnapshotStore>;

/// All sled-backed stores. Available with `features = ["sled"]`.
#[cfg(feature = "sled")]
pub type SledBroker = InMemoryBroker<
    crate::storage::SledOffsetStore,
    crate::storage::SledLogStore,
    crate::storage::SledDedupeStore,
    crate::storage::SledSnapshotStore,
>;

impl<O, L, D, S> std::fmt::Debug for InMemoryBroker<O, L, D, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryBroker")
            .field("store", &self.store)
            .field("fanout", &self.fanout)
            .field("dedupe_window", &self.dedupe_window)
            .finish()
    }
}

impl InMemoryBroker<MemoryOffsetStore, MemoryLogStore, MemoryDedupeStore, MemorySnapshotStore> {
    /// Create a new default (all in-memory) broker.
    pub fn new(
        default_profile: crate::topic::TopicProfile,
        dedupe_window: Duration,
        max_payload_bytes: usize,
    ) -> Self {
        let store = TopicStore::new();
        let router: Box<dyn TopicRouter> = Box::new(LocalRouter::new(
            store.clone(),
            Arc::new(move || default_profile.clone()),
        ));
        Self {
            store,
            offsets: MemoryOffsetStore::new(),
            log: MemoryLogStore::new(),
            dedupe: MemoryDedupeStore::new(),
            snapshots: MemorySnapshotStore::new(),
            fanout: FanoutEngine::new(),
            router: Mutex::new(router),
            dedupe_window,
            max_payload_bytes,
        }
    }
}

impl<O: OffsetStore, L: LogStore, D: DedupeStore, S: SnapshotStore> InMemoryBroker<O, L, D, S> {
    /// Create a broker with the given storage backends.
    pub fn with_stores(
        default_profile: crate::topic::TopicProfile,
        dedupe_window: Duration,
        max_payload_bytes: usize,
        offsets: O,
        log: L,
        dedupe: D,
        snapshots: S,
    ) -> Self {
        let store = TopicStore::new();
        let router: Box<dyn TopicRouter> = Box::new(LocalRouter::new(
            store.clone(),
            Arc::new(move || default_profile.clone()),
        ));
        Self {
            store,
            offsets,
            log,
            dedupe,
            snapshots,
            fanout: FanoutEngine::new(),
            router: Mutex::new(router),
            dedupe_window,
            max_payload_bytes,
        }
    }

    fn validate_publish<'a>(&self, frame: &'a Frame) -> Result<(&'a str, &'a str)> {
        let topic = frame.topic.as_deref().ok_or_else(|| {
            RiftError::Frame(crate::error::FrameReject::RequiredFieldMissing("topic"))
        })?;
        let message_id = frame.message_id.as_deref().ok_or_else(|| {
            RiftError::Frame(crate::error::FrameReject::RequiredFieldMissing(
                "message_id",
            ))
        })?;
        let max = self.max_payload_bytes;
        if let Some(payload) = frame.payload.as_ref()
            && payload.len() > max
        {
            return Err(RiftError::Message(MessageReject::TooLarge {
                actual: payload.len(),
                max,
            }));
        }
        if let Some(ttl) = frame.ttl_ms
            && frame.timestamp > 0
            && now_ms() - frame.timestamp > ttl as i64
        {
            return Err(RiftError::Message(MessageReject::Expired));
        }
        Ok((topic, message_id))
    }
}

#[async_trait]
impl<
    O: OffsetStore + 'static,
    L: LogStore + 'static,
    D: DedupeStore + 'static,
    S: SnapshotStore + 'static,
> Broker for InMemoryBroker<O, L, D, S>
{
    async fn publish(&self, frame: &Frame) -> Result<PublishOutcome> {
        let (topic, message_id) = self.validate_publish(frame)?;
        crate::topic::store::validate_name(topic)?;

        // Route to get/create the topic entry (metadata + limits).
        let route: Route = {
            let router = self.router.lock();
            router
                .route(topic, None)
                .ok_or_else(|| RiftError::Topic(TopicReject::NotFound(topic.to_string())))?
        };

        if !route.entry.can_publish() {
            return Err(RiftError::Topic(TopicReject::PublisherLimit(
                topic.to_string(),
            )));
        }
        route.entry.inc_publisher();

        // Dedupe.
        let mut duplicate = false;
        if !self
            .dedupe
            .check_and_record(topic, message_id, self.dedupe_window)
        {
            duplicate = true;
        }

        // Allocate offset.
        let offset = self.offsets.alloc(topic);

        // Build log entry.
        let entry = LogEntry {
            offset,
            publisher_session: frame.session_id.clone(),
            message_id: message_id.to_string(),
            class: frame
                .event
                .clone()
                .unwrap_or_else(|| MessageClass::Event.as_str().to_string()),
            event: frame.event.clone(),
            payload: frame.payload.clone().unwrap_or_default(),
            timestamp: frame.timestamp,
        };

        // Persist to log store with retention from topic profile.
        let profile = route.entry.profile.read().clone();
        self.log.append(topic, entry.clone(), profile.retention);

        // Record snapshot if enabled.
        if profile.snapshot_enabled {
            // We capture a lightweight snapshot via log.latest().
            // Full snapshots are the responsibility of the caller.
            let _ = &profile;
        }

        // Fan out to subscribers (not duplicates).
        if !duplicate {
            let serialized = crate::broker::broker::serialize_frame_for_fanout(frame, offset);
            self.fanout.deliver(topic, serialized);
        }

        Ok(PublishOutcome { offset, duplicate })
    }

    async fn subscribe(
        &self,
        topic: &str,
        intent: SubscribeIntent,
        sink: ConnectionSink,
    ) -> Result<SubscriptionId> {
        crate::topic::store::validate_name(topic)?;
        {
            let router = self.router.lock();
            let route = router
                .route(topic, None)
                .ok_or_else(|| RiftError::Topic(TopicReject::NotFound(topic.to_string())))?;
            if !route.entry.can_subscribe() {
                return Err(RiftError::Topic(TopicReject::SubscriberLimit(
                    topic.to_string(),
                )));
            }
            route.entry.inc_subscriber();
        }
        Ok(self.fanout.subscribe(topic, intent, sink))
    }

    async fn unsubscribe(&self, id: SubscriptionId) -> Result<bool> {
        if let Some(topic) = self.fanout.unsubscribe(id) {
            if let Some(entry) = self.store.get(&topic) {
                entry.dec_subscriber();
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn drop_sink(&self, sink_id: u64) -> usize {
        let topics = self.fanout.drop_sink(sink_id);
        let count = topics.len();
        for topic in topics {
            if let Some(entry) = self.store.get(&topic) {
                entry.dec_subscriber();
            }
        }
        count
    }

    async fn replay(&self, topic: &str, from: i64, to: i64) -> Result<Vec<Bytes>> {
        // Use log store for replay.
        Ok(self
            .log
            .range(topic, from, to)
            .into_iter()
            .map(|e| e.payload)
            .collect())
    }

    async fn snapshot(&self, topic: &str) -> Result<Option<crate::storage::StoredSnapshot>> {
        // Capture from log store's latest state.
        if let Some(entry) = self.log.latest(topic) {
            let now = now_ms();
            Ok(Some(crate::storage::StoredSnapshot {
                snapshot_id: uuid::Uuid::new_v4().to_string(),
                topic: topic.to_string(),
                base_offset: entry.offset,
                payload: entry.payload.clone(),
                created_at: now,
                expires_at: None,
            }))
        } else {
            Ok(None)
        }
    }

    async fn subscriber_count(&self, topic: &str) -> usize {
        self.fanout.topic_subscriber_count(topic)
    }

    async fn head_offset(&self, topic: &str) -> i64 {
        self.offsets.head(topic)
    }
}

impl<
    O: OffsetStore + 'static,
    L: LogStore + 'static,
    D: DedupeStore + 'static,
    S: SnapshotStore + 'static,
> InMemoryBroker<O, L, D, S>
{
    /// Wrap as a trait object.
    pub fn into_arc(self) -> Arc<dyn Broker> {
        Arc::new(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::fanout::test_sink::CountingSink;
    use crate::frame::{Codec, FrameFlags, FrameType};
    use crate::topic::{RetentionPolicy, TopicProfile};

    fn make_frame(topic: &str, msg_id: &str, payload: &[u8]) -> Frame {
        Frame {
            version: 0x0100,
            frame_id: 1,
            frame_type: FrameType::Data,
            flags: FrameFlags::empty(),
            codec: Codec::Json,
            session_id: Some("s-1".into()),
            stream_id: None,
            topic: Some(topic.into()),
            event: Some("chat.message.created".into()),
            message_id: Some(msg_id.into()),
            correlation_id: None,
            trace_id: None,
            timestamp: 0,
            ttl_ms: None,
            priority: None,
            payload: Some(Bytes::copy_from_slice(payload)),
        }
    }

    const PAYLOAD_LIMIT: usize = 65_536;

    #[tokio::test]
    async fn publish_assigns_offset() {
        let b = DefaultBroker::new(
            TopicProfile::default(),
            Duration::from_secs(60),
            PAYLOAD_LIMIT,
        );
        let out = b.publish(&make_frame("t", "m1", b"hello")).await.unwrap();
        assert_eq!(out.offset, 1);
        let out2 = b.publish(&make_frame("t", "m2", b"world")).await.unwrap();
        assert_eq!(out2.offset, 2);
    }

    #[tokio::test]
    async fn publish_requires_topic_and_message_id() {
        let b = DefaultBroker::new(
            TopicProfile::default(),
            Duration::from_secs(60),
            PAYLOAD_LIMIT,
        );
        let mut f = make_frame("t", "m1", b"x");
        f.topic = None;
        assert!(b.publish(&f).await.is_err());
        f.topic = Some("t".into());
        f.message_id = None;
        assert!(b.publish(&f).await.is_err());
    }

    #[tokio::test]
    async fn publish_fans_out_to_subscribers() {
        let b = DefaultBroker::new(
            TopicProfile::default(),
            Duration::from_secs(60),
            PAYLOAD_LIMIT,
        );
        let sink = Arc::new(CountingSink::new(1));
        b.subscribe("t", SubscribeIntent::Live, sink.clone())
            .await
            .unwrap();
        b.publish(&make_frame("t", "m1", b"hi")).await.unwrap();
        assert_eq!(sink.count(), 1);
    }

    #[tokio::test]
    async fn publish_dedupes_within_window() {
        let b = DefaultBroker::new(
            TopicProfile::default(),
            Duration::from_secs(60),
            PAYLOAD_LIMIT,
        );
        let sink = Arc::new(CountingSink::new(1));
        b.subscribe("t", SubscribeIntent::Live, sink.clone())
            .await
            .unwrap();
        let out1 = b.publish(&make_frame("t", "dup", b"x")).await.unwrap();
        let out2 = b.publish(&make_frame("t", "dup", b"x")).await.unwrap();
        assert!(!out1.duplicate);
        assert!(out2.duplicate);
        assert_eq!(sink.count(), 1);
    }

    #[tokio::test]
    async fn replay_returns_in_range() {
        let profile = TopicProfile {
            retention: RetentionPolicy::Count(100),
            ..TopicProfile::default()
        };
        let b = DefaultBroker::new(profile, Duration::from_secs(60), PAYLOAD_LIMIT);
        for i in 1..=5 {
            b.publish(&make_frame("t", &format!("m{i}"), b"x"))
                .await
                .unwrap();
        }
        let r = b.replay("t", 2, 4).await.unwrap();
        assert_eq!(r.len(), 3);
    }

    #[tokio::test]
    async fn subscribe_and_unsubscribe() {
        let b = DefaultBroker::new(
            TopicProfile::default(),
            Duration::from_secs(60),
            PAYLOAD_LIMIT,
        );
        let s = Arc::new(CountingSink::new(1));
        let id = b
            .subscribe("t", SubscribeIntent::Live, s.clone())
            .await
            .unwrap();
        assert!(b.unsubscribe(id).await.unwrap());
        b.publish(&make_frame("t", "m1", b"x")).await.unwrap();
        assert_eq!(s.count(), 0);
    }

    #[tokio::test]
    async fn drop_sink_removes_all_subs() {
        let b = DefaultBroker::new(
            TopicProfile::default(),
            Duration::from_secs(60),
            PAYLOAD_LIMIT,
        );
        let s = Arc::new(CountingSink::new(7));
        b.subscribe("a", SubscribeIntent::Live, s.clone())
            .await
            .unwrap();
        b.subscribe("b", SubscribeIntent::Live, s.clone())
            .await
            .unwrap();
        assert_eq!(b.drop_sink(7).await, 2);
    }
}
