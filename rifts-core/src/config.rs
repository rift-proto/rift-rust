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
//! use rifts_core::config::ServerConfig;
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
use crate::topic::TopicProfile;

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

    /// Maximum number of concurrent connections the server will accept.
    ///
    /// When this limit is reached, new connections are rejected immediately
    /// with the close code [`CloseCode::ServerBusy`].
    /// `0` means unlimited (the default for backwards compatibility).
    /// A reasonable production default is 10,000.
    pub max_connections: usize,

    /// Write timeout for sending a frame to the transport.
    ///
    /// If a `write_frame` call exceeds this duration, the writer task
    /// releases the transport and the connection is torn down.
    /// Default: 30 seconds.
    pub write_timeout: Duration,

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
    pub codec_offer: Vec<crate::frame::EncodingFormat>,

    /// Default profile applied when a topic is auto-created on first subscribe.
    ///
    /// Individual topics can override these settings.
    pub default_topic_profile: TopicProfile,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_payload_bytes: 65_536,
            max_topics_per_connection: 128,
            max_send_queue_bytes: 1_048_576,
            max_connections: 0, // unlimited
            heartbeat: HeartbeatPolicy::default(),
            idle_timeout: Duration::from_secs(300),
            write_timeout: Duration::from_secs(30),
            reconnect_base_ms: 500,
            reconnect_max_ms: 15_000,
            replay_window: Duration::from_secs(300),
            dedupe_window: Duration::from_secs(60),
            max_auth_failures: 3,
            codec_offer: Vec::new(),
            default_topic_profile: TopicProfile::default(),
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
        Ok(())
    }
}
