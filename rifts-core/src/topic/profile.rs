//! Topic profile (Rift spec section 9.2).
//!
//! A [`TopicProfile`] bundles all the configurable policies for a single topic
//! into one value: retention, ordering, publisher/subscriber limits, rate
//! limits, and replay/snapshot capabilities. Profiles are stored alongside
//! each topic entry in the [`TopicStore`](crate::topic::store::TopicStore)
//! and can be updated at runtime (subject to the broker's policy).

use std::time::Duration;

use crate::topic::ordering::OrderingPolicy;
use crate::topic::retention::RetentionPolicy;

/// A topic profile defines every configurable policy for a single topic.
///
/// The default profile uses global topic ordering, latest-value retention,
/// a 10 000 subscriber/publisher limit, no rate limiting, and a 5-minute
/// replay window.
#[derive(Debug, Clone)]
pub struct TopicProfile {
    /// Human-readable profile name (e.g. `"default"`, `"chat"`, `"metrics"`).
    pub name: String,

    /// How long messages are retained in the replay log before eviction.
    pub retention: RetentionPolicy,

    /// The ordering guarantee applied to message delivery.
    pub ordering: OrderingPolicy,

    /// Maximum number of concurrent subscribers allowed on this topic.
    /// Attempts to subscribe beyond this limit are rejected with
    /// [`TopicReject::SubscriberLimit`](crate::error::TopicReject::SubscriberLimit).
    pub max_subscribers: usize,

    /// Maximum number of concurrent publishers allowed on this topic.
    /// Attempts to publish beyond this limit are rejected with
    /// [`TopicReject::PublisherLimit`](crate::error::TopicReject::PublisherLimit).
    pub max_publishers: usize,

    /// Per-publisher rate limit in messages per second. `None` means no
    /// limit is enforced per publisher.
    pub rate_limit_per_publisher: Option<u32>,

    /// Aggregate rate limit for the entire topic in messages per second.
    /// `None` means no topic-wide limit is enforced.
    pub rate_limit_total: Option<u32>,

    /// Whether late-joining subscribers can replay messages from the
    /// retention log. When `false`, only live messages are delivered.
    pub replay_enabled: bool,

    /// Whether the broker maintains a latest-value snapshot for this
    /// topic. Snapshots allow new subscribers to receive the current
    /// state immediately without waiting for the next live message.
    pub snapshot_enabled: bool,

    /// Optional TTL for snapshots. `None` means snapshots do not
    /// expire and are replaced on every new `snapshot_enabled`
    /// publish. Only relevant when `snapshot_enabled` is `true`.
    pub snapshot_ttl: Option<Duration>,

    /// Duration for which messages remain available for replay after
    /// they are published. Only relevant when `replay_enabled` is `true`.
    pub replay_window: Duration,
}

impl Default for TopicProfile {
    /// Returns the default topic profile.
    ///
    /// | Field                    | Default value           |
    /// |--------------------------|-------------------------|
    /// | `name`                   | `"default"`             |
    /// | `retention`              | `Latest`                |
    /// | `ordering`               | `Topic`                 |
    /// | `max_subscribers`        | 10 000                  |
    /// | `max_publishers`         | 10 000                  |
    /// | `rate_limit_per_publisher` | `None` (no limit)     |
    /// | `rate_limit_total`       | `None` (no limit)       |
    /// | `replay_enabled`         | `true`                  |
    /// | `snapshot_enabled`       | `true`                  |
    /// | `snapshot_ttl`           | `None` (no expiry)      |
    /// | `replay_window`          | 300 seconds (5 minutes) |
    fn default() -> Self {
        Self {
            name: "default".into(),
            retention: RetentionPolicy::Latest,
            ordering: OrderingPolicy::Topic,
            max_subscribers: 10_000,
            max_publishers: 10_000,
            rate_limit_per_publisher: None,
            rate_limit_total: None,
            replay_enabled: true,
            snapshot_enabled: true,
            snapshot_ttl: None,
            replay_window: Duration::from_secs(300),
        }
    }
}
