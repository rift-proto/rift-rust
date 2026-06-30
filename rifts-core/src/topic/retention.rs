//! Retention policy (Rift spec section 9.3).
//!
//! This module defines the [`RetentionPolicy`] enum that controls how long
//! messages are kept in a topic's replay log after they are published.
//! The retention policy is part of a topic's
//! [`TopicProfile`](crate::topic::profile::TopicProfile) and is enforced
//! by the [`TopicEntry::append`](crate::topic::store::TopicEntry::append)
//! method each time a new message is stored.

use std::time::Duration;

/// How long messages on a topic are kept before being evicted.
///
/// The retention policy is checked every time a new message is appended
/// to the topic's replay log. Older entries that no longer satisfy the
/// policy are removed immediately, so the log never exceeds the policy's
/// bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RetentionPolicy {
    /// No retention — messages are discarded immediately after fanout.
    ///
    /// Subscribers that are not connected at the time of publication
    /// will never see these messages.
    None,

    /// Retain messages for at most the given [`Duration`].
    ///
    /// Messages older than the TTL are evicted on the next append.
    Ttl(Duration),

    /// Retain at most `n` messages.
    ///
    /// When the log exceeds `n` entries the oldest messages are evicted
    /// in FIFO order.
    Count(usize),

    /// Retain at most `n` bytes of total payload across all log entries.
    ///
    /// When the total payload size exceeds the limit the oldest entries
    /// are evicted until the log fits within the budget.
    Size(usize),

    /// Retention is managed by an external durable storage backend.
    ///
    /// The in-memory log retains all entries; the external store is
    /// responsible for long-term persistence and eviction.
    Durable,

    /// Only the latest value per state key is kept (the default).
    ///
    /// After each append all older entries are evicted, leaving exactly
    /// the most recent message in the log. This is the most aggressive
    /// policy and is appropriate for stateful topics where only the
    /// current value matters.
    #[default]
    Latest,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_latest() {
        assert_eq!(RetentionPolicy::default(), RetentionPolicy::Latest);
    }
}
