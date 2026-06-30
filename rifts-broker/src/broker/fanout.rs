//! Fanout engine — spec section 22.4.
//!
//! This module implements the message fanout mechanism that delivers a
//! published message to all active subscribers of a topic. The fanout
//! engine uses a "direct" strategy suitable for small-to-medium topics:
//! when a message is published, the engine iterates over every
//! subscriber registered for that topic and invokes their
//! [`FanoutSink::deliver`] method with a pre-serialized frame.
//!
//! # Subscriptions
//!
//! A [`Subscription`] ties a connection (represented by a
//! [`ConnectionSink`]) to a topic. Each subscription carries a
//! [`SubscribeIntent`] that indicates what kind of messages the
//! subscriber wants to receive (live-only, replay from an offset,
//! snapshot-then-live, etc.). Subscriptions are identified by a
//! monotonic [`SubscriptionId`].
//!
//! # Sink abstraction
//!
//! The fanout engine is transport-agnostic. It does not know whether
//! subscribers are local TCP connections, WebSocket clients, or
//! in-process channels. Instead, it operates on the [`FanoutSink`]
//! trait, which any transport layer can implement. The engine clones
//! [`Bytes`] for each delivery, so sinks receive an owned buffer
//! they can serialize or queue independently.
//!
//! # Backpressure and errors
//!
//! If a sink's delivery fails (e.g. the connection is closed or its
//! send queue is full), the error is reported but does not prevent
//! delivery to other subscribers. The caller (typically the broker
//! implementation) is responsible for cleaning up stale subscriptions
//! via [`FanoutEngine::unsubscribe`] or [`FanoutEngine::drop_sink`].
//!
//! # Concurrency
//!
//! The engine uses [`DashMap`] for both its topic-to-subscribers index
//! and its subscription-id-to-topic index, allowing concurrent reads
//! and writes without a global lock.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
pub use rifts_core::message::SubscribeIntent;
use uuid::Uuid;

mod conn_sink;
pub use conn_sink::ConnSink;

/// Identifies a single (connection, topic) subscription.
///
/// Subscription IDs are allocated monotonically by the
/// [`FanoutEngine`] and are unique within a single engine instance.
/// They are used to cancel subscriptions via
/// [`FanoutEngine::unsubscribe`] and are returned to the caller by
/// [`FanoutEngine::subscribe`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriptionId(pub u64);

/// A registered subscription record.
///
/// Contains all metadata about a subscription, including its unique
/// identifier, the topic it is subscribed to, the subscriber's intent,
/// and whether the subscription has been cancelled. Instances are
/// returned to callers and stored in the broker's subscription
/// tracking structures.
#[derive(Debug, Clone)]
pub struct Subscription {
    /// Unique identifier for this subscription, allocated by the
    /// [`FanoutEngine`].
    pub id: SubscriptionId,
    /// The name of the topic this subscription is listening to.
    pub topic: String,
    /// The subscriber's delivery intent (live, replay, snapshot, etc.).
    pub intent: SubscribeIntent,
    /// Whether the subscription has been told to stop. When `true`,
    /// no further messages will be delivered to the associated sink.
    pub cancelled: bool,
}

/// A shared, type-erased handle to a connection that can receive
/// fanned-out messages.
///
/// This is an `Arc<dyn FanoutSink>`, allowing the fanout engine to
/// deliver frames to any transport without knowing the concrete type.
/// The `Arc` enables sharing a single sink across multiple
/// subscriptions if needed.
pub type ConnectionSink = Arc<dyn FanoutSink>;

/// Trait for a connection that can receive fanned-out frames.
///
/// Implementors represent a single client connection (TCP, WebSocket,
/// in-process channel, etc.). The fanout engine calls
/// [`deliver`](FanoutSink::deliver) for each message that matches
/// the subscriber's topic and intent. The implementation is
/// responsible for queuing, serializing, or writing the frame to the
/// underlying transport.
///
/// # Thread safety
///
/// Implementations must be both [`Send`] and [`Sync`] because the
/// fanout engine may invoke `deliver` from any async task.
pub trait FanoutSink: Send + Sync {
    /// Deliver a serialized frame to this sink.
    ///
    /// The `frame` is a pre-serialized [`bytes::Bytes`] buffer
    /// (typically produced by
    /// [`serialize_frame_for_fanout`](crate::broker::broker::serialize_frame_for_fanout))
    /// that the sink can write directly to its transport.
    ///
    /// Returns `Ok(())` on success, or a [`FanoutError`] if delivery
    /// fails (e.g. the connection is closed or backpressured).
    fn deliver(&self, frame: bytes::Bytes) -> Result<(), FanoutError>;

    /// Return a unique identifier for this sink.
    ///
    /// Used by the fanout engine to group subscriptions by connection,
    /// enabling bulk cleanup via [`FanoutEngine::drop_sink`]. The ID
    /// must be unique across all active sinks; see
    /// [`new_sink_id`] for a UUID-derived allocation strategy.
    fn id(&self) -> u64;
}

/// Errors that can occur during fanout delivery to a sink.
///
/// The fanout engine treats these errors as non-fatal: a delivery
/// failure to one subscriber does not prevent delivery to others.
/// The caller is responsible for cleaning up subscriptions whose
/// sinks have been closed.
#[derive(Debug, thiserror::Error)]
pub enum FanoutError {
    /// The sink has been closed and should be removed from the fanout
    /// engine. This typically means the underlying TCP connection or
    /// channel has been dropped.
    #[error("sink closed")]
    Closed,
    /// The sink's internal send queue is full and cannot accept more
    /// messages at this time. The caller may choose to retry later,
    /// drop the message, or disconnect the slow subscriber.
    #[error("sink backpressured: queue={queue_bytes}, max={max_bytes}")]
    Backpressured {
        /// Current number of bytes queued in the sink's buffer.
        queue_bytes: usize,
        /// Maximum queue capacity in bytes configured for this sink.
        max_bytes: usize,
    },
}

/// In-process fanout engine that manages subscriptions and delivers
/// published messages to all active subscribers of a topic.
///
/// The engine maintains two indexes for efficient lookup:
///
/// - **by topic**: maps a topic name to a list of `(SubscriptionId,
///   ConnectionSink)` pairs, enabling fast fanout delivery.
/// - **by subscription ID**: maps a [`SubscriptionId`] to its topic
///   and sink, enabling fast unsubscription and sink cleanup.
///
/// Both indexes use [`DashMap`] for concurrent shard-level access
/// without a global lock.
///
/// # Usage
///
/// ```ignore
/// use std::sync::Arc;
/// use rifts::broker::fanout::{FanoutEngine, SubscribeIntent};
///
/// let engine = FanoutEngine::new();
/// let sink: Arc<dyn FanoutSink> = /* ... */;
/// let id = engine.subscribe("orders", SubscribeIntent::Live, sink);
/// let delivered = engine.deliver("orders", bytes::Bytes::from_static(b"hello"));
/// assert_eq!(delivered, 1);
/// ```
pub struct FanoutEngine {
    /// Maps topic name to a list of (subscription_id, sink) pairs.
    /// Used during fanout delivery to iterate over all subscribers
    /// of a given topic.
    by_topic: DashMap<String, Vec<(SubscriptionId, ConnectionSink)>>,
    /// Maps subscription_id to (topic, sink). Used for fast
    /// unsubscription and sink-level cleanup.
    by_id: DashMap<SubscriptionId, (String, ConnectionSink)>,
    /// Reverse index from sink_id to the set of subscription IDs
    /// belonging to that sink. Lets `drop_sink` clean up in O(N)
    /// where N is the number of subscriptions for *that* sink, not
    /// the total number of subscriptions across all sinks.
    by_sink: DashMap<u64, Vec<SubscriptionId>>,
    /// Monotonically increasing counter for allocating unique
    /// subscription IDs.
    seq: AtomicU64,
}

impl std::fmt::Debug for FanoutEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FanoutEngine")
            .field("subscription_count", &self.by_id.len())
            .finish()
    }
}

impl Default for FanoutEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl FanoutEngine {
    /// Create an empty fanout engine with no registered subscriptions.
    pub fn new() -> Self {
        Self {
            by_topic: DashMap::new(),
            by_id: DashMap::new(),
            by_sink: DashMap::new(),
            seq: AtomicU64::new(0),
        }
    }

    /// Register a new subscription for a topic.
    ///
    /// Adds the `sink` to the fanout list for the given `topic` and
    /// records the mapping from the allocated [`SubscriptionId`] back
    /// to the topic and sink. The `intent` parameter is stored for
    /// informational purposes but does not currently affect delivery
    /// behavior.
    ///
    /// Returns the allocated [`SubscriptionId`], which the caller can
    /// later pass to [`unsubscribe`](FanoutEngine::unsubscribe) to
    /// cancel the subscription.
    ///
    /// # Arguments
    ///
    /// * `topic` — The topic name to subscribe to.
    /// * `intent` — The subscriber's delivery preference.
    /// * `sink` — A shared handle to the connection that will receive
    ///   fanned-out frames.
    pub fn subscribe(
        &self,
        topic: &str,
        _intent: SubscribeIntent,
        sink: ConnectionSink,
    ) -> SubscriptionId {
        let id = SubscriptionId(self.seq.fetch_add(1, Ordering::Relaxed) + 1);
        let sink_id = sink.id();
        self.by_topic
            .entry(topic.to_string())
            .or_default()
            .push((id, sink.clone()));
        self.by_id.insert(id, (topic.to_string(), sink));
        self.by_sink.entry(sink_id).or_default().push(id);
        id
    }

    /// Remove a subscription by its ID.
    ///
    /// Removes the subscription from both the topic-to-subscribers
    /// index and the subscription-ID-to-topic index. Returns
    /// `Some(topic_name)` if the subscription existed, allowing the
    /// caller to decrement per-topic subscriber counters. Returns
    /// `None` if the subscription was not found (already cancelled
    /// or never registered).
    pub fn unsubscribe(&self, id: SubscriptionId) -> Option<String> {
        if let Some((_, (topic, sink))) = self.by_id.remove(&id) {
            if let Some(mut list) = self.by_topic.get_mut(&topic) {
                list.retain(|(sid, _)| *sid != id);
                // Clean up the topic entry when no subscribers remain,
                // mirroring the sink-level cleanup below.
                if list.is_empty() {
                    drop(list);
                    self.by_topic.remove(&topic);
                }
            }
            // Clean up reverse index.
            let sink_id = sink.id();
            if let Some(mut sids) = self.by_sink.get_mut(&sink_id) {
                sids.retain(|sid| *sid != id);
                if sids.is_empty() {
                    drop(sids);
                    self.by_sink.remove(&sink_id);
                }
            }
            Some(topic)
        } else {
            None
        }
    }

    /// Drop all subscriptions owned by a particular connection sink.
    ///
    /// Uses the reverse `by_sink` index to look up only this sink's
    /// subscription IDs in O(N) where N is the number of
    /// subscriptions for that specific sink, rather than scanning
    /// every subscription in the engine. Returns a list of topic
    /// names that had at least one subscription removed, so the
    /// caller can decrement per-topic subscriber counts.
    ///
    /// This is typically called when a client connection is closed,
    /// to clean up all of its subscriptions in a single operation.
    pub fn drop_sink(&self, sink_id: u64) -> Vec<String> {
        let mut topics = Vec::new();
        // O(N) over this sink's subscriptions only, not all
        // subscriptions. We collect the IDs first so we can release
        // the `by_sink` shard lock before mutating other shards.
        let ids: Vec<SubscriptionId> = self
            .by_sink
            .get(&sink_id)
            .map(|sids| sids.iter().copied().collect())
            .unwrap_or_default();
        for id in ids {
            if let Some(topic) = self.unsubscribe(id) {
                topics.push(topic);
            }
        }
        topics
    }

    /// Deliver a single serialized frame to all subscribers of a topic.
    ///
    /// Looks up all subscribers registered for the given `topic`,
    /// clones the (subscription_id, sink) list while holding the
    /// `by_topic` shard read lock, and then **releases the lock**
    /// before invoking `sink.deliver()`. This prevents a slow sink
    /// from blocking other operations on the same shard, and
    /// eliminates the deadlock risk that would arise if a sink's
    /// `deliver` callback re-entered the fanout engine (e.g. to
    /// subscribe or unsubscribe on a topic that happens to hash to
    /// the same shard).
    pub fn deliver(&self, topic: &str, frame: bytes::Bytes) -> usize {
        // Clone the subscriber list while holding the shard read lock
        // briefly, then drop the lock before calling out to each
        // sink.
        let subscribers: Vec<ConnectionSink> = match self.by_topic.get(topic) {
            Some(list) => list.iter().map(|(_id, sink)| sink.clone()).collect(),
            None => return 0,
        };
        let mut ok = 0;
        for sink in subscribers {
            if sink.deliver(frame.clone()).is_ok() {
                ok += 1;
            }
        }
        ok
    }

    /// Return the total number of active subscriptions across all
    /// topics.
    ///
    /// This is the number of entries in the subscription-ID-to-topic
    /// index. A single connection may have multiple subscriptions
    /// (one per topic), so this count may exceed the number of
    /// distinct connections.
    pub fn subscription_count(&self) -> usize {
        self.by_id.len()
    }

    /// Return the number of distinct subscriptions registered for a
    /// specific topic.
    ///
    /// Returns `0` if the topic has no subscribers or does not exist
    /// in the index.
    pub fn topic_subscriber_count(&self, topic: &str) -> usize {
        self.by_topic.get(topic).map(|l| l.len()).unwrap_or(0)
    }
}

/// Generate a fresh, unique connection sink identifier.
///
/// Produces a `u64` derived from the first 8 bytes of a new UUID v4,
/// interpreted as a little-endian unsigned integer. The probability
/// of collision is negligible for typical deployment sizes.
///
/// This ID is used to tag connection sinks so that the fanout engine
/// can group subscriptions by connection and clean them up in bulk
/// via [`FanoutEngine::drop_sink`].
pub fn new_sink_id() -> u64 {
    let u = Uuid::new_v4();
    let bytes = u.as_bytes();
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[..8]);
    u64::from_le_bytes(buf)
}

/// Test utilities for the fanout engine.
///
/// This module provides mock sink implementations that are useful
/// for unit testing broker and fanout logic without real network
/// connections.
pub mod test_sink {
    use std::sync::atomic::{AtomicU64, Ordering};

    use parking_lot::Mutex;

    use super::{FanoutError, FanoutSink};

    /// A test sink that counts deliveries and records message payloads.
    ///
    /// Useful in unit tests to verify that the correct number of
    /// messages were delivered and that the payload content matches
    /// expectations. The sink always accepts deliveries (never returns
    /// an error) and stores all received frames in an internal log.
    pub struct CountingSink {
        /// Unique identifier returned by [`FanoutSink::id`].
        id: u64,
        /// Atomic counter tracking the total number of deliveries.
        delivered: AtomicU64,
        /// Ordered log of all received message payloads.
        log: Mutex<Vec<Vec<u8>>>,
    }

    impl CountingSink {
        /// Create a new counting sink with the given unique `id`.
        ///
        /// The sink starts with zero deliveries and an empty message
        /// log.
        pub fn new(id: u64) -> Self {
            Self {
                id,
                delivered: AtomicU64::new(0),
                log: Mutex::new(Vec::new()),
            }
        }
        /// Return the total number of messages that have been
        /// delivered to this sink.
        pub fn count(&self) -> u64 {
            self.delivered.load(Ordering::SeqCst)
        }
        /// Return a snapshot of all message payloads that have been
        /// delivered to this sink, in delivery order.
        pub fn messages(&self) -> Vec<Vec<u8>> {
            self.log.lock().clone()
        }
    }

    impl FanoutSink for CountingSink {
        fn deliver(&self, frame: bytes::Bytes) -> Result<(), FanoutError> {
            self.delivered.fetch_add(1, Ordering::SeqCst);
            self.log.lock().push(frame.to_vec());
            Ok(())
        }
        fn id(&self) -> u64 {
            self.id
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_sink::CountingSink;
    use super::*;

    #[test]
    fn subscribe_and_fanout() {
        let fan = FanoutEngine::new();
        let s1 = Arc::new(CountingSink::new(1));
        let s2 = Arc::new(CountingSink::new(2));
        let s3 = Arc::new(CountingSink::new(3));
        fan.subscribe("t", SubscribeIntent::Live, s1.clone());
        fan.subscribe("t", SubscribeIntent::Live, s2.clone());
        fan.subscribe("other", SubscribeIntent::Live, s3.clone());

        let frame = bytes::Bytes::from_static(b"hi");
        let n = fan.deliver("t", frame);
        assert_eq!(n, 2);
        assert_eq!(s1.count(), 1);
        assert_eq!(s2.count(), 1);
        assert_eq!(s3.count(), 0);
    }

    #[test]
    fn unsubscribe_returns_topic() {
        let fan = FanoutEngine::new();
        let s = Arc::new(CountingSink::new(1));
        let id = fan.subscribe("t", SubscribeIntent::Live, s.clone());
        let topic = fan.unsubscribe(id);
        assert_eq!(topic, Some("t".to_string()));
        assert_eq!(fan.deliver("t", bytes::Bytes::from_static(b"x")), 0);
    }

    #[test]
    fn drop_sink_returns_topics() {
        let fan = FanoutEngine::new();
        let s1 = Arc::new(CountingSink::new(7));
        let s2 = Arc::new(CountingSink::new(7));
        fan.subscribe("t", SubscribeIntent::Live, s1.clone());
        fan.subscribe("u", SubscribeIntent::Live, s2.clone());
        let topics = fan.drop_sink(7);
        assert_eq!(topics.len(), 2);
        assert!(topics.contains(&"t".to_string()));
        assert!(topics.contains(&"u".to_string()));
        assert_eq!(fan.subscription_count(), 0);
    }

    #[test]
    fn topic_subscriber_count() {
        let fan = FanoutEngine::new();
        let s = Arc::new(CountingSink::new(1));
        fan.subscribe("t", SubscribeIntent::Live, s.clone());
        fan.subscribe("t", SubscribeIntent::Live, s.clone());
        assert_eq!(fan.topic_subscriber_count("t"), 2);
    }

    #[test]
    fn deliver_records_payload() {
        let fan = FanoutEngine::new();
        let s = Arc::new(CountingSink::new(1));
        fan.subscribe("t", SubscribeIntent::Live, s.clone());
        fan.deliver("t", bytes::Bytes::from_static(b"abc"));
        assert_eq!(s.messages(), vec![b"abc".to_vec()]);
    }

    #[test]
    fn sink_id_is_unique() {
        let a = new_sink_id();
        let b = new_sink_id();
        assert_ne!(a, b);
    }
}
