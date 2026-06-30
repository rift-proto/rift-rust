//! Broker trait and shared types — spec section 22.
//!
//! This module defines the [`Broker`] trait, which is the central async
//! interface for all topic-level operations in the system: publishing
//! messages, subscribing to topics, replaying historical messages,
//! retrieving snapshots, and managing subscriber/publisher lifecycles.
//!
//! Concrete implementations live in sibling modules:
//!
//! - [`InMemoryBroker`](crate::broker::InMemoryBroker) — single-process, all
//!   storage backed by in-memory data structures. Ideal for development,
//!   testing, and lightweight deployments that do not require persistence
//!   across restarts.
//!
//! # Frame serialization
//!
//! The helper function [`serialize_frame_for_fanout`] converts a [`Frame`]
//! and its broker-assigned offset into a [`Bytes`] buffer that can be
//! delivered to subscribers via the fanout engine. The wire layout is:
//!
//! ```text
//! [b"OFF:"][i64 BE offset][optional payload bytes...]
//! ```

use async_trait::async_trait;
use bytes::Bytes;

use crate::broker::fanout::{ConnectionSink, SubscribeIntent, SubscriptionId};
use rifts_core::error::Result;
use rifts_core::frame::Frame;

/// The outcome of publishing a message to a topic.
///
/// Returned by [`Broker::publish`] to inform the caller whether the
/// message was accepted and what offset was assigned. If the message's
/// deduplication key had already been seen within the configured
/// deduplication window, the `duplicate` flag is set to `true`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PublishOutcome {
    /// The monotonic offset assigned to this message within its topic.
    /// Offsets start at 1 and increase by 1 for each unique (non-duplicate)
    /// message published to the same topic.
    pub offset: i64,
    /// Whether the message's deduplication key had already been seen
    /// within the configured time window. When `true`, the message was
    /// still persisted and an offset was allocated, but the fanout
    /// engine did not deliver it to subscribers a second time.
    pub duplicate: bool,
}

/// The central async trait for all topic-level broker operations.
///
/// Implementations must be both [`Send`] and [`Sync`] so they can be
/// shared across async tasks (e.g. behind an `Arc<dyn Broker>`).
///
/// # Error handling
///
/// All fallible methods return [`rifts_core::error::Result`], which wraps
/// [`rifts_core::error::RiftError`]. Callers should inspect the error variant
/// to distinguish between client-side issues (e.g. missing required
/// fields, payload too large) and system-level failures (e.g. storage
/// errors, timeouts).
#[async_trait]
pub trait Broker: Send + Sync {
    /// Publish a message to the topic specified in `frame.topic`.
    ///
    /// The broker validates the frame (required fields present, payload
    /// size within limits, message not expired), routes the topic,
    /// checks for deduplication, allocates a monotonic offset, appends
    /// the message to the log store, and fans out to live subscribers.
    ///
    /// Returns a [`PublishOutcome`] with the assigned offset and whether
    /// the message was a duplicate.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `frame.topic` or `frame.message_id` is missing
    /// - The payload exceeds `max_payload_bytes`
    /// - The message has expired (TTL exceeded)
    /// - The topic does not exist or the publisher limit has been reached
    async fn publish(&self, frame: &Frame) -> Result<PublishOutcome>;

    /// Subscribe a connection sink to the given topic.
    ///
    /// The `intent` parameter controls what the subscriber wants to
    /// receive (e.g. live-only, replay from an offset, snapshot then
    /// live). The `sink` is a trait object that the fanout engine will
    /// deliver serialized frames to.
    ///
    /// Returns a [`SubscriptionId`] that can later be passed to
    /// [`unsubscribe`](Broker::unsubscribe) to cancel the subscription.
    ///
    /// # Errors
    ///
    /// Returns an error if the topic name is invalid, the topic does not
    /// exist, or the per-topic subscriber limit has been reached.
    async fn subscribe(
        &self,
        topic: &str,
        intent: SubscribeIntent,
        sink: ConnectionSink,
    ) -> Result<SubscriptionId>;

    /// Cancel a previously created subscription.
    ///
    /// Removes the subscription identified by `id` from the fanout
    /// engine and decrements the topic's subscriber count. Returns
    /// `true` if the subscription existed and was removed, `false` if
    /// it had already been cancelled or was never registered.
    async fn unsubscribe(&self, id: SubscriptionId) -> Result<bool>;

    /// Drop all subscriptions belonging to a particular connection sink.
    ///
    /// Called when a client connection is closed. The broker iterates
    /// over all subscriptions owned by `sink_id`, removes them from the
    /// fanout engine, and decrements subscriber counts for each affected
    /// topic.
    ///
    /// Returns the number of subscriptions that were removed.
    async fn drop_sink(&self, sink_id: u64) -> usize;

    /// Replay historical messages for a topic within the offset range
    /// `[from, to]` (inclusive on both ends).
    ///
    /// Returns a list of serialized payload [`Bytes`] for each message
    /// in the range. If no messages exist in the range, an empty `Vec`
    /// is returned.
    ///
    /// # Errors
    ///
    /// Returns an error if the topic does not exist or the log store
    /// fails to read the requested range.
    async fn replay(&self, topic: &str, from: i64, to: i64) -> Result<Vec<Bytes>>;

    /// Fetch the most recent snapshot for a topic, if one is available.
    ///
    /// A snapshot captures the latest state of a topic at a particular
    /// offset, allowing a subscriber to skip replaying the entire log.
    /// Returns `None` if no snapshot has been captured for the topic
    /// or if the snapshot has expired.
    async fn snapshot(&self, topic: &str) -> Result<Option<rifts_storage::StoredSnapshot>>;

    /// Return the number of active subscribers for a topic.
    ///
    /// This count includes all subscription intents (live, replay,
    /// snapshot-then-live, etc.). Returns `0` if the topic has no
    /// subscribers or does not exist.
    async fn subscriber_count(&self, topic: &str) -> usize;

    /// Return the current head (highest allocated) offset for a topic.
    ///
    /// Returns `0` if no messages have been published to the topic yet.
    async fn head_offset(&self, topic: &str) -> i64;

    /// Decrement the publisher count for `topic`.
    ///
    /// Called by the server when a connection that previously published
    /// to this topic is closing, so the publisher slot can be reused by
    /// a new connection. This prevents publisher slot exhaustion when
    /// clients disconnect without explicitly releasing their claim.
    ///
    /// Default: no-op. Brokers that do not enforce a per-topic publisher
    /// limit can leave this unimplemented.
    async fn dec_publisher(&self, _topic: &str) {}

    /// Perform periodic maintenance tasks.
    ///
    /// Called by the server on a regular interval (e.g. every 30s) to
    /// allow the broker to run garbage collection: sweeping expired
    /// deduplication entries, compacting logs, pruning stale state, etc.
    ///
    /// Returns the number of items cleaned up (e.g. dedupe entries swept,
    /// log entries evicted). Implementations that have no background
    /// maintenance to perform can leave the default (no-op) implementation.
    ///
    /// Default: no-op, returns 0.
    async fn maintain(&self) -> usize {
        0
    }
}

/// Serialize a frame for fanout delivery to subscribers.
///
/// Produces a [`Bytes`] buffer containing the broker-assigned offset
/// followed by the frame's payload. The format is:
///
/// ```text
/// [b"OFF:"][i64 big-endian offset][payload bytes (if present)]
/// ```
///
/// This encoding allows subscribers to extract the offset prefix with
/// a simple 12-byte header read (`"OFF:"` = 4 bytes + 8-byte `i64`)
/// and then process the remaining payload bytes.
///
/// # Arguments
///
/// * `frame` — The original [`Frame`] whose payload will be included.
/// * `offset` — The monotonic offset assigned by the broker for this
///   message within its topic.
pub fn serialize_frame_for_fanout(frame: &Frame, offset: i64) -> Bytes {
    let mut buf = Vec::with_capacity(16 + frame.payload.as_ref().map(|p| p.len()).unwrap_or(0));
    buf.extend_from_slice(b"OFF:");
    buf.extend_from_slice(&offset.to_be_bytes());
    if let Some(payload) = frame.payload.as_ref() {
        buf.extend_from_slice(payload);
    }
    Bytes::from(buf)
}

/// A type alias for the subscription handle returned by the broker.
///
/// This is a re-export of [`crate::broker::fanout::Subscription`],
/// which contains the subscription ID, topic name, intent, and
/// cancellation state.
pub type BrokerSubscription = crate::broker::fanout::Subscription;
