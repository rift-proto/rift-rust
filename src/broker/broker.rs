//! In-memory broker — orchestrates topic store, dedupe, offsets,
//! snapshots, and fanout (spec §22).

#![allow(unused_imports)]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;

use crate::broker::dedupe::DedupeStore;
use crate::broker::fanout::{ConnectionSink, FanoutEngine, SubscribeIntent, SubscriptionId};
use crate::broker::offset_store::OffsetStore;
use crate::broker::router::{LocalRouter, Route, TopicRouter};
use crate::broker::snapshot_store::{SharedSnapshotStore, SnapshotStore};
use crate::error::{MessageReject, Result, RiftError, TopicReject};
use crate::frame::Frame;
use crate::message::MessageClass;
use crate::now_ms;
use crate::topic::store::LogEntry;
use crate::topic::{RetentionPolicy, TopicProfile, TopicStore};

/// The outcome of publishing a message.
#[derive(Debug, Clone)]
pub struct PublishOutcome {
    /// The offset assigned by the broker.
    pub offset: i64,
    /// True if the dedupe key had been seen within the window.
    pub duplicate: bool,
}

/// Broker trait.
pub trait Broker: Send + Sync {
    /// Publish a message to a topic.
    fn publish(&self, frame: &Frame) -> Result<PublishOutcome>;

    /// Subscribe a connection sink to a topic.
    fn subscribe(
        &self,
        topic: &str,
        intent: SubscribeIntent,
        sink: ConnectionSink,
    ) -> Result<SubscriptionId>;

    /// Cancel a subscription.
    fn unsubscribe(&self, id: SubscriptionId) -> Result<bool>;

    /// Remove all subscriptions belonging to a sink.
    fn drop_sink(&self, sink_id: u64) -> usize;

    /// Replay messages in `[from, to]` on `topic`. The returned
    /// frames are already serialized and ready to send.
    fn replay(&self, topic: &str, from: i64, to: i64) -> Result<Vec<Bytes>>;

    /// Fetch a snapshot for `topic`, if one is available.
    fn snapshot(
        &self,
        topic: &str,
    ) -> Result<Option<crate::broker::snapshot_store::StoredSnapshot>>;

    /// Number of subscribers for a topic.
    fn subscriber_count(&self, topic: &str) -> usize;

    /// Current head offset for a topic.
    fn head_offset(&self, topic: &str) -> i64;
}

/// Single-process broker.
pub struct InMemoryBroker {
    /// In-memory topic store.
    pub store: TopicStore,
    /// Deduplication store.
    pub dedupe: DedupeStore,
    /// Per-topic offset allocator.
    pub offsets: OffsetStore,
    /// Snapshot store.
    pub snapshots: SharedSnapshotStore,
    /// Fanout engine.
    pub fanout: FanoutEngine,
    /// Topic router.
    pub router: Mutex<Box<dyn TopicRouter>>,
    /// Deduplication window duration.
    pub dedupe_window: Duration,
    /// Maximum payload bytes allowed.
    pub max_payload_bytes: usize,
}

impl std::fmt::Debug for InMemoryBroker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryBroker")
            .field("store", &self.store)
            .field("dedupe", &self.dedupe)
            .field("offsets", &self.offsets)
            .field("snapshots", &self.snapshots)
            .field("fanout", &self.fanout)
            .field("dedupe_window", &self.dedupe_window)
            .finish()
    }
}

impl InMemoryBroker {
    /// Create a new in-memory broker with the given default topic
    /// profile, dedupe window, and max payload size.
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
            dedupe: DedupeStore::new(),
            offsets: OffsetStore::new(),
            snapshots: Arc::new(SnapshotStore::new()),
            fanout: FanoutEngine::new(),
            router: Mutex::new(router),
            dedupe_window,
            max_payload_bytes,
        }
    }

    /// Validate that a frame carries the minimum fields for a publish.
    /// Returns the validated topic and message_id references on success.
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
        // Check message TTL.
        if let Some(ttl) = frame.ttl_ms {
            if frame.timestamp > 0 && now_ms() - frame.timestamp > ttl as i64 {
                return Err(RiftError::Message(MessageReject::Expired));
            }
        }
        Ok((topic, message_id))
    }
}

impl Broker for InMemoryBroker {
    fn publish(&self, frame: &Frame) -> Result<PublishOutcome> {
        let (topic, message_id) = self.validate_publish(frame)?;
        crate::topic::store::validate_name(topic)?;

        let route: Route = {
            let router = self.router.lock();
            router
                .route(topic, None)
                .ok_or_else(|| RiftError::Topic(TopicReject::NotFound(topic.to_string())))?
        };

        // Check publisher limit.
        if !route.entry.can_publish() {
            return Err(RiftError::Topic(TopicReject::PublisherLimit(
                topic.to_string(),
            )));
        }
        route.entry.inc_publisher();

        // Dedupe check: message_id is the primary dedupe key.
        let mut duplicate = false;
        if !self
            .dedupe
            .check_and_record(topic, message_id, self.dedupe_window)
        {
            duplicate = true;
        }

        let offset = self.offsets.alloc(topic);
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
        route.entry.append(entry.clone());

        // Fan out to subscribers if not a duplicate.
        if !duplicate {
            let serialized = serialize_frame_for_fanout(frame, offset);
            self.fanout.deliver(topic, serialized);
        }

        Ok(PublishOutcome { offset, duplicate })
    }

    fn subscribe(
        &self,
        topic: &str,
        intent: SubscribeIntent,
        sink: ConnectionSink,
    ) -> Result<SubscriptionId> {
        crate::topic::store::validate_name(topic)?;
        // Ensure the topic exists; profile is created via router.
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

    fn unsubscribe(&self, id: SubscriptionId) -> Result<bool> {
        if let Some(topic) = self.fanout.unsubscribe(id) {
            if let Some(entry) = self.store.get(&topic) {
                entry.dec_subscriber();
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn drop_sink(&self, sink_id: u64) -> usize {
        let topics = self.fanout.drop_sink(sink_id);
        let count = topics.len();
        for topic in topics {
            if let Some(entry) = self.store.get(&topic) {
                entry.dec_subscriber();
            }
        }
        count
    }

    fn replay(&self, topic: &str, from: i64, to: i64) -> Result<Vec<Bytes>> {
        let entry = self
            .store
            .get(topic)
            .ok_or_else(|| RiftError::Topic(TopicReject::NotFound(topic.to_string())))?;
        Ok(entry
            .range(from, to)
            .into_iter()
            .map(|e| e.payload)
            .collect())
    }

    fn snapshot(
        &self,
        topic: &str,
    ) -> Result<Option<crate::broker::snapshot_store::StoredSnapshot>> {
        Ok(self.snapshots.capture(topic, &self.store, None))
    }

    fn subscriber_count(&self, topic: &str) -> usize {
        self.fanout.topic_subscriber_count(topic)
    }

    fn head_offset(&self, topic: &str) -> i64 {
        self.offsets.head(topic)
    }
}

impl InMemoryBroker {
    /// Wrap as a trait object.
    pub fn into_arc(self) -> Arc<dyn Broker> {
        Arc::new(self)
    }
}

/// Helper: produce a serialized frame for fanout, stamping the
/// assigned offset.
pub fn serialize_frame_for_fanout(frame: &Frame, offset: i64) -> Bytes {
    let mut buf = Vec::with_capacity(16 + frame.payload.as_ref().map(|p| p.len()).unwrap_or(0));
    buf.extend_from_slice(b"OFF:");
    buf.extend_from_slice(&offset.to_be_bytes());
    if let Some(payload) = frame.payload.as_ref() {
        buf.extend_from_slice(payload);
    }
    Bytes::from(buf)
}

/// Subscription handle returned by the broker.
pub type BrokerSubscription = crate::broker::fanout::Subscription;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::fanout::test_sink::CountingSink;
    use crate::frame::{Codec, FrameFlags, FrameType};

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

    #[test]
    fn publish_assigns_offset() {
        let b = InMemoryBroker::new(
            TopicProfile::default(),
            Duration::from_secs(60),
            PAYLOAD_LIMIT,
        );
        let out = b.publish(&make_frame("t", "m1", b"hello")).unwrap();
        assert_eq!(out.offset, 1);
        let out2 = b.publish(&make_frame("t", "m2", b"world")).unwrap();
        assert_eq!(out2.offset, 2);
    }

    #[test]
    fn publish_requires_topic_and_message_id() {
        let b = InMemoryBroker::new(
            TopicProfile::default(),
            Duration::from_secs(60),
            PAYLOAD_LIMIT,
        );
        let mut f = make_frame("t", "m1", b"x");
        f.topic = None;
        assert!(b.publish(&f).is_err());
        f.topic = Some("t".into());
        f.message_id = None;
        assert!(b.publish(&f).is_err());
    }

    #[test]
    fn publish_fans_out_to_subscribers() {
        let b = InMemoryBroker::new(
            TopicProfile::default(),
            Duration::from_secs(60),
            PAYLOAD_LIMIT,
        );
        let sink = Arc::new(CountingSink::new(1));
        b.subscribe("t", SubscribeIntent::Live, sink.clone())
            .unwrap();
        b.publish(&make_frame("t", "m1", b"hi")).unwrap();
        assert_eq!(sink.count(), 1);
    }

    #[test]
    fn publish_dedupes_within_window() {
        let b = InMemoryBroker::new(
            TopicProfile::default(),
            Duration::from_secs(60),
            PAYLOAD_LIMIT,
        );
        let sink = Arc::new(CountingSink::new(1));
        b.subscribe("t", SubscribeIntent::Live, sink.clone())
            .unwrap();
        let out1 = b.publish(&make_frame("t", "dup", b"x")).unwrap();
        let out2 = b.publish(&make_frame("t", "dup", b"x")).unwrap();
        assert!(!out1.duplicate);
        assert!(out2.duplicate);
        assert_eq!(sink.count(), 1);
    }

    #[test]
    fn replay_returns_in_range() {
        let profile = TopicProfile {
            retention: RetentionPolicy::Count(100),
            ..TopicProfile::default()
        };
        let b = InMemoryBroker::new(profile, Duration::from_secs(60), PAYLOAD_LIMIT);
        for i in 1..=5 {
            b.publish(&make_frame("t", &format!("m{i}"), b"x")).unwrap();
        }
        let r = b.replay("t", 2, 4).unwrap();
        assert_eq!(r.len(), 3);
    }

    #[test]
    fn subscribe_and_unsubscribe() {
        let b = InMemoryBroker::new(
            TopicProfile::default(),
            Duration::from_secs(60),
            PAYLOAD_LIMIT,
        );
        let s = Arc::new(CountingSink::new(1));
        let id = b.subscribe("t", SubscribeIntent::Live, s.clone()).unwrap();
        assert!(b.unsubscribe(id).unwrap());
        b.publish(&make_frame("t", "m1", b"x")).unwrap();
        assert_eq!(s.count(), 0);
    }

    #[test]
    fn drop_sink_removes_all_subs() {
        let b = InMemoryBroker::new(
            TopicProfile::default(),
            Duration::from_secs(60),
            PAYLOAD_LIMIT,
        );
        let s = Arc::new(CountingSink::new(7));
        b.subscribe("a", SubscribeIntent::Live, s.clone()).unwrap();
        b.subscribe("b", SubscribeIntent::Live, s.clone()).unwrap();
        assert_eq!(b.drop_sink(7), 2);
    }
}
