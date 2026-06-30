//! Message ordering policy (Rift spec section 9.4).
//!
//! This module defines the [`OrderingPolicy`] enum that describes how messages
//! within a topic are ordered. The ordering policy is part of a topic's
//! [`TopicProfile`](crate::topic::profile::TopicProfile) and determines which
//! guarantees the broker must uphold when delivering messages to subscribers.
//!
//! # Ordering levels
//!
//! From weakest to strongest:
//!
//! 1. **None** — no ordering guarantee at all.
//! 2. **Connection** — messages from a single connection arrive in the order
//!    they were sent on that connection.
//! 3. **Publisher** — messages from a single publisher (which may span multiple
//!    connections) arrive in the order they were published.
//! 4. **Topic** — all messages on the topic arrive in a single global order
//!    (the default).
//! 5. **Key** — messages sharing the same `ordering_key` arrive in order;
//!    messages with different keys may be interleaved.
//! 6. **Causal** — messages arrive in causal order, requiring vector-clock
//!    metadata attached to each frame.

use std::fmt;

/// How messages within a topic are ordered.
///
/// The ordering policy is chosen by the topic creator and stored in the
/// topic's [`TopicProfile`](crate::topic::profile::TopicProfile). Brokers
/// and routers use it to decide how to sequence deliveries.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum OrderingPolicy {
    /// No ordering guarantee — messages may arrive in any order.
    None,

    /// Ordered within a single connection.
    ///
    /// Messages originating from the same transport connection are
    /// delivered to subscribers in the order they were received by
    /// the broker on that connection.
    Connection,

    /// Ordered within a single publisher.
    ///
    /// Messages originating from the same publisher (identified by
    /// `session_id`) are delivered in order, even if the publisher
    /// reconnects or uses multiple connections.
    Publisher,

    /// Globally ordered within the topic (the default).
    ///
    /// All messages on the topic share a single monotonic offset
    /// sequence and are delivered to every subscriber in that order.
    #[default]
    Topic,

    /// Ordered by `ordering_key`.
    ///
    /// Messages that carry the same `ordering_key` header are
    /// delivered in the order they were published. Messages with
    /// different keys are independent and may be delivered
    /// concurrently or out of order relative to each other.
    Key,

    /// Causally ordered (requires vector-clock metadata).
    ///
    /// Messages are delivered in an order that respects causal
    /// dependencies. This requires each frame to carry a vector
    /// clock (or equivalent metadata) so the broker can compute
    /// the causal partial order.
    Causal,
}

impl fmt::Display for OrderingPolicy {
    /// Format the ordering policy as its lowercase wire representation.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            OrderingPolicy::None => "none",
            OrderingPolicy::Connection => "connection",
            OrderingPolicy::Publisher => "publisher",
            OrderingPolicy::Topic => "topic",
            OrderingPolicy::Key => "key",
            OrderingPolicy::Causal => "causal",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_topic() {
        assert_eq!(OrderingPolicy::default(), OrderingPolicy::Topic);
    }
}
