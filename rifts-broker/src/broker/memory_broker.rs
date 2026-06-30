//! In-memory broker — spec section 22.
//!
//! This module implements a single-process broker that orchestrates all
//! the broker subsystem components: topic metadata store, per-topic
//! offset allocation, message log persistence, deduplication, snapshot
//! capture, topic routing, and live fanout delivery to subscribers.
//!
//! # Generic storage backends
//!
//! The [`InMemoryBroker`] struct is generic over four storage trait
//! parameters:
//!
//! - `O: OffsetStore` — per-topic monotonic offset allocation
//! - `L: LogStore` — append and range-query message log
//! - `D: DedupeStore` — time-window-based message deduplication
//! - `S: SnapshotStore` — topic snapshot capture and retrieval
//!
//! This design allows the same broker logic to be used with different
//! storage backends (e.g. in-memory for development, sled for
//! production). Type aliases are provided for common configurations:
//!
//! - [`DefaultBroker`] — all in-memory stores (development/testing)
//! - `SledBroker` — all sled-backed stores (production, feature `sled`)
//!
//! # Publish flow
//!
//! When a message is published, the broker:
//!
//! 1. Validates the frame (required fields, payload size, TTL)
//! 2. Routes the topic name to a [`TopicEntry`] via the router
//! 3. Checks and enforces the publisher limit
//! 4. Runs deduplication against the message ID
//! 5. Allocates a monotonic offset
//! 6. Builds a log entry and appends it to the log store
//! 7. Fans out to live subscribers (unless the message is a duplicate)
//!
//! # Thread safety
//!
//! The broker is safe to share across async tasks via `Arc<dyn Broker>`.
//! The [`TopicRouter`] is protected by a [`parking_lot::Mutex`] for
//! mutable access during topic creation, while all storage backends
//! use their own internal synchronization.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use tracing::instrument;

use crate::broker::broker::{Broker, PublishOutcome};
use crate::broker::fanout::{ConnectionSink, FanoutEngine, SubscribeIntent, SubscriptionId};
use crate::broker::router::{LocalRouter, Route, TopicRouter};
use rifts_core::error::{MessageReject, Result, RiftError, TopicReject};
use rifts_core::frame::Frame;
use rifts_core::message::MessageClass;
use rifts_core::now_ms;
use rifts_core::topic::TopicStore;
use rifts_core::topic::store::LogEntry;
use rifts_storage::{
    DedupeStore, LogStore, MemoryDedupeStore, MemoryLogStore, MemoryOffsetStore,
    MemorySnapshotStore, OffsetStore, SnapshotStore,
};

/// Single-process broker, generic over storage backends.
///
/// This struct wires together all the components needed for a
/// fully functional message broker: topic metadata, offset allocation,
/// message log, deduplication, snapshot storage, fanout delivery, and
/// topic routing.
///
/// Type parameters correspond to the four persistence traits:
/// - `O`: [`OffsetStore`] — per-topic offset allocation
/// - `L`: [`LogStore`] — append + range-query message log
/// - `D`: [`DedupeStore`] — deduplication
/// - `S`: [`SnapshotStore`] — snapshot capture and retrieval
///
/// Use the type aliases for common configurations:
/// - [`DefaultBroker`] — all memory-backed (development)
/// - `SledBroker` — all sled-backed (production, feature `sled`)
pub struct InMemoryBroker<O, L, D, S> {
    /// In-memory topic metadata store. Holds [`TopicEntry`] instances
    /// that track per-topic state such as publisher/subscriber counts,
    /// the topic profile, and the latest snapshot.
    pub store: TopicStore,
    /// Per-topic offset allocator. Generates monotonically increasing
    /// offsets for each message published to a topic.
    pub offsets: O,
    /// Message log store. Provides append and range-query operations
    /// for persisted messages, with configurable retention policies.
    pub log: L,
    /// Deduplication store. Tracks message IDs within a configurable
    /// time window to suppress duplicate deliveries to subscribers.
    pub dedupe: D,
    /// Snapshot store. Captures and retrieves per-topic snapshots for
    /// subscribers that want the latest state without full replay.
    pub snapshots: S,
    /// Fanout engine. Manages subscriptions and delivers serialized
    /// frames to all active subscribers of a topic.
    pub fanout: FanoutEngine,
    /// Topic router. Resolves topic names to [`TopicEntry`] handles,
    /// creating new topics on demand with the configured default
    /// profile. Protected by a mutex for mutable access during topic
    /// creation.
    pub router: Mutex<Box<dyn TopicRouter>>,
    /// Duration of the deduplication time window. Messages with the
    /// same deduplication key published within this window are marked
    /// as duplicates.
    pub dedupe_window: Duration,
    /// Maximum allowed payload size in bytes. Messages with payloads
    /// exceeding this limit are rejected with a
    /// [`MessageReject::TooLarge`] error.
    pub max_payload_bytes: usize,
}

/// All in-memory stores. Default configuration for development and
/// testing.
///
/// Uses [`MemoryOffsetStore`], [`MemoryLogStore`],
/// [`MemoryDedupeStore`], and [`MemorySnapshotStore`] — all backed
/// by in-process data structures with no disk persistence.
pub type DefaultBroker =
    InMemoryBroker<MemoryOffsetStore, MemoryLogStore, MemoryDedupeStore, MemorySnapshotStore>;

/// All sled-backed stores. Available with `features = ["sled"]`.
///
/// Uses sled persistent storage for all four store traits, providing
/// durability across process restarts.
#[cfg(feature = "sled")]
pub type SledBroker = InMemoryBroker<
    rifts_storage::SledOffsetStore,
    rifts_storage::SledLogStore,
    rifts_storage::SledDedupeStore,
    rifts_storage::SledSnapshotStore,
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
    ///
    /// Initializes all storage backends with their in-memory
    /// implementations and sets up a [`LocalRouter`] backed by the
    /// broker's own [`TopicStore`].
    ///
    /// # Arguments
    ///
    /// * `default_profile` — The [`TopicProfile`](rifts_core::topic::TopicProfile)
    ///   applied to topics when they are first created.
    /// * `dedupe_window` — The time window for message deduplication.
    ///   Messages with the same deduplication key within this window
    ///   are marked as duplicates.
    /// * `max_payload_bytes` — The maximum allowed payload size in
    ///   bytes. Messages exceeding this limit are rejected.
    pub fn new(
        default_profile: rifts_core::topic::TopicProfile,
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
    /// Create a broker with explicitly provided storage backends.
    ///
    /// Allows callers to inject custom implementations of the four
    /// storage traits, enabling different persistence strategies
    /// (e.g. sled-backed, file-backed, or mock stores for testing).
    ///
    /// # Arguments
    ///
    /// * `default_profile` — The [`TopicProfile`](rifts_core::topic::TopicProfile)
    ///   applied to topics when they are first created.
    /// * `dedupe_window` — The deduplication time window.
    /// * `max_payload_bytes` — Maximum allowed payload size in bytes.
    /// * `offsets` — The offset store implementation.
    /// * `log` — The message log store implementation.
    /// * `dedupe` — The deduplication store implementation.
    /// * `snapshots` — The snapshot store implementation.
    pub fn with_stores(
        default_profile: rifts_core::topic::TopicProfile,
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

    /// Validate a frame before publishing.
    ///
    /// Checks that the frame contains the required `topic` and
    /// `message_id` fields, that the payload does not exceed the
    /// configured maximum size, and that the message has not expired
    /// (TTL check). Returns the topic name and message ID as string
    /// slices on success.
    ///
    /// # Errors
    ///
    /// Returns [`RiftError::Frame`] if `topic` or `message_id` is
    /// missing, [`RiftError::Message`] with [`MessageReject::TooLarge`]
    /// if the payload exceeds the limit, or [`MessageReject::Expired`]
    /// if the TTL has been exceeded.
    fn validate_publish<'a>(&self, frame: &'a Frame) -> Result<(&'a str, &'a str)> {
        let topic = frame.topic.as_deref().ok_or_else(|| {
            RiftError::Frame(rifts_core::error::FrameReject::RequiredFieldMissing(
                "topic",
            ))
        })?;
        let message_id = frame.message_id.as_deref().ok_or_else(|| {
            RiftError::Frame(rifts_core::error::FrameReject::RequiredFieldMissing(
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
    #[instrument(skip(self, frame), fields(topic))]
    async fn publish(&self, frame: &Frame) -> Result<PublishOutcome> {
        let (topic, message_id) = self.validate_publish(frame)?;
        rifts_core::topic::store::validate_name(topic)?;

        // Route to get/create the topic entry (metadata + limits).
        let route: Route = {
            let router = self.router.lock();
            router
                .route(topic, None)
                .ok_or_else(|| RiftError::Topic(TopicReject::NotFound(topic.to_string())))?
        };

        if !route.entry.try_inc_publisher() {
            return Err(RiftError::Topic(TopicReject::PublisherLimit(
                topic.to_string(),
            )));
        }

        // Dedupe.
        let mut duplicate = false;
        if !self
            .dedupe
            .check_and_record(topic, message_id, self.dedupe_window)
            .await
        {
            duplicate = true;
        }

        // Allocate offset.
        let offset = self.offsets.alloc(topic).await;

        // Build log entry.
        let entry = LogEntry {
            offset,
            publisher_session: frame.session_id.clone(),
            message_id: message_id.to_string(),
            // `class` is the message class discriminator ("event" /
            // "command" / "state" / "system" / "reply"). It is NOT
            // the event name -- that goes in `event` below. The
            // current Frame shape does not carry an explicit class
            // field, so default to Event.
            class: MessageClass::Event.as_str().to_string(),
            event: frame.event.clone(),
            payload: frame.payload.clone().unwrap_or_default(),
            timestamp: frame.timestamp,
            // Stamped by `LogStore::append`; the in-memory path
            // additionally updates it via `TopicEntry::append`.
            appended_at: None,
        };

        // Persist to log store with retention from topic profile.
        let profile = route.entry.profile.read().clone();
        self.log
            .append(topic, entry.clone(), profile.retention)
            .await;

        // Record snapshot if enabled. Capture a fresh snapshot through the
        // configured `SnapshotStore` rather than the previous no-op, so
        // subscribers can later fetch the topic state via
        // `Broker::snapshot()`.
        if profile.snapshot_enabled {
            self.snapshots
                .capture(topic, &self.store, profile.snapshot_ttl)
                .await;
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
        rifts_core::topic::store::validate_name(topic)?;
        {
            let router = self.router.lock();
            let route = router
                .route(topic, None)
                .ok_or_else(|| RiftError::Topic(TopicReject::NotFound(topic.to_string())))?;
            if !route.entry.try_inc_subscriber() {
                return Err(RiftError::Topic(TopicReject::SubscriberLimit(
                    topic.to_string(),
                )));
            }
        }
        Ok(self.fanout.subscribe(topic, intent, sink))
    }

    async fn unsubscribe(&self, id: SubscriptionId) -> Result<bool> {
        if let Some(topic) = self.fanout.unsubscribe(id) {
            // If the topic entry is gone (explicitly removed) we
            // cannot decrement its subscriber count. Log a warning
            // so operators can spot the discrepancy; the
            // subscription itself is already removed.
            match self.store.get(&topic) {
                Some(entry) => entry.dec_subscriber(),
                None => tracing::warn!(
                    topic = %topic,
                    "topic entry missing during unsubscribe; subscriber counter not decremented",
                ),
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
        // Use log store for replay. Return just the payloads (as before)
        // to keep the public Broker contract stable; the per-entry
        // offset is preserved internally for ordering guarantees.
        Ok(self
            .log
            .range(topic, from, to)
            .await
            .into_iter()
            .map(|e| e.payload)
            .collect())
    }

    async fn snapshot(&self, topic: &str) -> Result<Option<rifts_storage::StoredSnapshot>> {
        Ok(self.snapshots.get(topic).await)
    }

    async fn subscriber_count(&self, topic: &str) -> usize {
        self.fanout.topic_subscriber_count(topic)
    }

    async fn head_offset(&self, topic: &str) -> i64 {
        self.offsets.head(topic).await
    }

    async fn dec_publisher(&self, topic: &str) {
        if let Some(entry) = self.store.get(topic) {
            entry.dec_publisher();
        }
    }

    async fn maintain(&self) -> usize {
        self.dedupe.sweep().await
    }
}

impl<
    O: OffsetStore + 'static,
    L: LogStore + 'static,
    D: DedupeStore + 'static,
    S: SnapshotStore + 'static,
> InMemoryBroker<O, L, D, S>
{
    /// Wrap this broker as a thread-safe trait object.
    ///
    /// Returns an `Arc<dyn Broker>` that can be shared across async
    /// tasks and passed to the server or gateway layer.
    pub fn into_arc(self) -> Arc<dyn Broker> {
        Arc::new(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::fanout::test_sink::CountingSink;
    use rifts_core::frame::{EncodingFormat, FrameFlags, FrameType};
    use rifts_core::topic::{RetentionPolicy, TopicProfile};

    fn make_frame(topic: &str, msg_id: &str, payload: &[u8]) -> Frame {
        Frame {
            version: 0x0100,
            frame_id: 1,
            frame_type: FrameType::Data,
            flags: FrameFlags::empty(),
            codec: EncodingFormat::Json,
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
