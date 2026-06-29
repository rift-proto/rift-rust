//! # Server Configuration (`ServerConfig`)
//!
//! This module defines all tunable parameters for the Rift server, including:
//! - Payload size limits
//! - Per-connection topic subscription limits
//! - Outbound queue byte limits
//! - Heartbeat policy
//! - Idle timeout
//! - Client reconnect interval hints
//! - Replay window and deduplication window
//! - Maximum authentication failure count
//! - Codec preference list
//! - Default profile for auto-created topics
//!
//! ## Defaults
//!
//! All default values follow the specification section 27.1
//! ("Typical Web Application" recommended values):
//!
//! | Parameter | Default |
//! |-----------|---------|
//! | `max_payload_bytes` | 65,536 (64 KiB) |
//! | `max_topics_per_connection` | 128 |
//! | `max_send_queue_bytes` | 1,048,576 (1 MiB) |
//! | `idle_timeout` | 300 s |
//! | `replay_window` | 300 s |
//! | `dedupe_window` | 60 s |
//! | `max_auth_failures` | 3 |
//!
//! ## Usage
//!
//! ```rust
//! use rifts::config::ServerConfig;
//! use std::time::Duration;
//!
//! let config = ServerConfig {
//!     max_payload_bytes: 128 * 1024,
//!     idle_timeout: Duration::from_secs(600),
//!     ..ServerConfig::default()
//! };
//! ```

use std::time::Duration;

use crate::protocol::heartbeat::HeartbeatPolicy;

/// Global server configuration.
///
/// Set via [`RiftServer::builder()`](crate::RiftServer::builder), this configuration
/// spans the entire server lifetime and cannot be changed at runtime.
///
/// Each field has its own semantics; see individual field documentation for details.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Maximum payload size in bytes for a single frame.
    ///
    /// Frames exceeding this limit are rejected at the decoding stage
    /// (returning [`ErrorCode::PayloadTooLarge`]).
    /// Specification section 27.1 recommends a default of 65,536 bytes (64 KiB).
    pub max_payload_bytes: usize,

    /// Maximum number of topics a single connection can subscribe to simultaneously.
    ///
    /// New subscribe requests are rejected by the server when this limit is exceeded.
    /// Specification section 27.1 recommends a default of 128.
    pub max_topics_per_connection: usize,

    /// Maximum byte size of the outbound send queue per connection.
    ///
    /// When the outbound queue accumulates beyond this threshold, the connection
    /// enters a "slow consumer" state, triggering backpressure (dropping volatile
    /// messages or pausing publishers).
    /// Specification section 27.1 recommends a default of 1,048,576 (1 MiB).
    pub max_send_queue_bytes: usize,

    /// Heartbeat policy controlling ping/pong intervals and timeout detection.
    ///
    /// See [`HeartbeatPolicy`] for details.
    pub heartbeat: HeartbeatPolicy,

    /// Connection idle timeout.
    ///
    /// If no frames (including heartbeats) are received within this time window,
    /// the server actively closes the connection.
    /// Default: 300 seconds.
    pub idle_timeout: Duration,

    /// Suggested initial reconnect wait time for the client (in milliseconds).
    ///
    /// The server communicates this value to the client via the Welcome frame;
    /// the client uses it as the base interval for exponential backoff.
    /// Default: 500 ms.
    pub reconnect_base_ms: u32,

    /// Suggested maximum reconnect wait time for the client (in milliseconds).
    ///
    /// This caps the exponential backoff to prevent the client from
    /// unboundedly increasing its wait time.
    /// Default: 15,000 ms (15 seconds).
    pub reconnect_max_ms: u32,

    /// Replay window duration.
    ///
    /// The server retains message offsets within this time range, allowing
    /// clients to request replay of missed messages after reconnecting.
    /// Offsets outside this window are cleaned up.
    /// Specification section 27.1 recommends a default of 300 seconds.
    pub replay_window: Duration,

    /// Deduplication window duration.
    ///
    /// Within this time range, messages with the same `dedupe_key` are
    /// detected as duplicates and discarded.
    /// After the window expires, the key is cleared, allowing the same key
    /// to be accepted again.
    /// Default: 60 seconds.
    pub dedupe_window: Duration,

    /// Maximum number of consecutive authentication failures per connection.
    ///
    /// Once this threshold is reached, the server closes the connection and
    /// disallows immediate reconnection.
    /// Default: 3 failures.
    pub max_auth_failures: u32,

    /// Codec negotiation whitelist.
    ///
    /// During the Hello phase, this list of available codecs is presented to
    /// the client. If empty (the default), all compiled-in codecs are offered.
    pub codec_offer: Vec<CodecOffer>,

    /// Default profile applied when a topic is auto-created on first subscribe.
    ///
    /// Individual topics can override these settings.
    pub default_topic_profile: DefaultTopicProfile,

    /// Redis connection configuration (only available with feature `redis`).
    ///
    /// When set, the server can use a [`RedisActorBroker`](crate::redis::RedisActorBroker)
    /// for multi-instance communication. If `None`, Redis-based brokers cannot be
    /// constructed from this configuration.
    #[cfg(feature = "redis")]
    pub redis: Option<RedisConfig>,
}

/// Redis connection configuration for multi-instance deployments.
///
/// Defines how the server connects to a Redis instance for shared state
/// (offsets, log, dedupe, snapshots) and cross-instance message fanout
/// via Redis Pub/Sub.
#[cfg(feature = "redis")]
#[derive(Debug, Clone)]
pub struct RedisConfig {
    /// Redis connection URL (e.g. `"redis://127.0.0.1:6379"`).
    pub url: String,
    /// Maximum number of connections in the Redis connection pool.
    /// Default: 8.
    pub pool_size: usize,
    /// Prefix prepended to all Redis keys used by this instance,
    /// enabling multiple logical deployments to share a single
    /// Redis server without key collisions.
    /// Default: `"rift"`.
    pub prefix: String,
}

#[cfg(feature = "redis")]
impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            url: "redis://127.0.0.1:6379".into(),
            pool_size: 8,
            prefix: "rift".into(),
        }
    }
}

/// Available codecs offered to the client during the Hello phase.
///
/// Each variant corresponds to a wire serialization format. The client selects
/// one before the Welcome phase, and it is used for all subsequent frame
/// encoding and decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodecOffer {
    /// [JSON](https://www.json.org/) text encoding — easy to debug, widest compatibility.
    Json,
    /// [CBOR](https://cbor.io/) binary encoding — smaller footprint, faster parsing.
    Cbor,
}

/// Default profile used when a topic is auto-created on first subscribe.
///
/// When a client first subscribes to a topic that does not yet exist, the
/// server automatically creates the topic and its associated storage using
/// this profile. All fields can be dynamically overridden afterward.
#[derive(Debug, Clone)]
pub struct DefaultTopicProfile {
    /// Message retention policy (e.g., keep latest N messages, time-based retention, keep forever).
    pub retention: crate::topic::retention::RetentionPolicy,
    /// Message ordering policy (topic-global ordering or per-key partitioned ordering).
    pub ordering: crate::topic::ordering::OrderingPolicy,
    /// Maximum number of subscribers for the topic; new subscribers are rejected once the limit is reached.
    pub max_subscribers: usize,
    /// Maximum number of publishers for the topic; new publishers are rejected once the limit is reached.
    pub max_publishers: usize,
    /// Whether historical message replay is enabled.
    ///
    /// When enabled, new subscribers can request consumption of historical
    /// messages by specifying an offset.
    pub replay_enabled: bool,
    /// Whether topic-level snapshots are enabled.
    ///
    /// When enabled, publishers can set snapshots, and new subscribers
    /// automatically receive the latest snapshot upon joining.
    pub snapshot_enabled: bool,
    /// Optional TTL for snapshots produced on this topic.
    /// `None` means snapshots never expire and are simply replaced.
    pub snapshot_ttl: Option<std::time::Duration>,
}

impl DefaultTopicProfile {
    /// Validate that the profile values are sensible. Returns
    /// `Err` with a human-readable message on a problem.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.max_subscribers == 0 {
            return Err("max_subscribers must be > 0");
        }
        if self.max_publishers == 0 {
            return Err("max_publishers must be > 0");
        }
        Ok(())
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_payload_bytes: 65_536,
            max_topics_per_connection: 128,
            max_send_queue_bytes: 1_048_576,
            heartbeat: HeartbeatPolicy::default(),
            idle_timeout: Duration::from_secs(300),
            reconnect_base_ms: 500,
            reconnect_max_ms: 15_000,
            replay_window: Duration::from_secs(300),
            dedupe_window: Duration::from_secs(60),
            max_auth_failures: 3,
            codec_offer: Vec::new(),
            default_topic_profile: DefaultTopicProfile::default(),
            #[cfg(feature = "redis")]
            redis: None,
        }
    }
}

impl ServerConfig {
    /// Set of capability flags advertised in the Welcome frame so
    /// clients can negotiate feature-aware behaviour on connect.
    /// Keep this list in sync with the implemented feature set.
    pub fn supported_features(&self) -> Vec<&'static str> {
        vec![
            "replay",
            "snapshot",
            "resume",
            "topic_profiles",
            "backpressure",
        ]
    }

    /// Validate the configuration and reject nonsensical values
    /// such as zero heartbeat intervals. Returns `Err` with a
    /// human-readable message; `Ok(())` on success.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_payload_bytes == 0 {
            return Err("max_payload_bytes must be > 0".into());
        }
        if self.max_send_queue_bytes == 0 {
            return Err("max_send_queue_bytes must be > 0".into());
        }
        self.heartbeat
            .validate()
            .map_err(|e| format!("heartbeat: {e}"))?;
        self.default_topic_profile
            .validate()
            .map_err(|e| format!("default_topic_profile: {e}"))?;
        Ok(())
    }
}

impl Default for DefaultTopicProfile {
    fn default() -> Self {
        Self {
            retention: crate::topic::retention::RetentionPolicy::Latest,
            ordering: crate::topic::ordering::OrderingPolicy::Topic,
            max_subscribers: 10_000,
            max_publishers: 10_000,
            replay_enabled: true,
            snapshot_enabled: true,
            snapshot_ttl: None,
        }
    }
}
