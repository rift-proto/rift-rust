//! # Heartbeat Policy -- Spec §21
//!
//! This module defines the heartbeat configuration that the server sends
//! to the client during the **Ready** phase of the handshake (spec §5.5).
//! The heartbeat mechanism ensures both sides can detect unresponsive
//! peers promptly.
//!
//! ## How It Works
//!
//! 1. The client sends a **Ping** frame at least every `ping_interval`.
//! 2. The server must reply with a **Pong** frame within `pong_timeout`.
//! 3. If `max_missed_pongs` consecutive Pongs are not received, the
//!    connection is considered dead and will be closed.
//! 4. Any frame received from the remote peer also acts as a heartbeat
//!    (liveness signal), resetting the idle timer.
//!
//! ## Jitter
//!
//! To prevent a "thundering herd" of simultaneous heartbeats from many
//! clients, a random jitter of up to `jitter` milliseconds is added to
//! the effective ping interval on the client side (spec §27.1).
//!
//! ## Default Values
//!
//! The [`Default`] implementation provides sensible defaults for ordinary
//! web applications, as specified in §27.1:
//!
//! | Field             | Default   |
//! |-------------------|-----------|
//! | `ping_interval`   | 25 000 ms |
//! | `pong_timeout`    | 10 000 ms |
//! | `max_missed_pongs`| 2         |
//! | `idle_timeout`    | 300 s     |
//! | `jitter`          | 2 500 ms  |
//!
//! ## Relationship to Other Modules
//!
//! The `Ready` frame in [`hello`](super::hello) carries the effective
//! heartbeat parameters for a given connection.  The close code
//! [`CloseCode::IdleTimeout`](super::close::CloseCode::IdleTimeout) is
//! used when the idle timeout fires.

use std::time::Duration;

/// Heartbeat configuration sent to the client during the Ready phase.
///
/// The server includes this policy in its `Ready` frame so the client
/// knows how frequently to send Ping frames and how long to wait for
/// Pong replies before considering the connection degraded.
///
/// ## Effective Ping Interval
///
/// The actual interval between Ping frames on the client side is:
///
/// ```text
/// effective = ping_interval + random(0 ..= jitter)
/// ```
///
/// This prevents many clients from sending heartbeats at the exact same
/// instant, which could cause a "thundering herd" spike in frame traffic.
///
/// ## Dead Connection Detection
///
/// A connection is deemed dead when `max_missed_pongs` consecutive Ping
/// frames have been sent without receiving a Pong (or any other frame)
/// back.  At that point the connection is closed with an appropriate
/// close code.
#[derive(Debug, Clone, Copy)]
pub struct HeartbeatPolicy {
    /// Minimum interval between consecutive Ping frames.
    ///
    /// The client MUST send a Ping at least this often.  In practice the
    /// effective interval is `ping_interval + random(0 ..= jitter)` to
    /// distribute heartbeat traffic across many clients.
    pub ping_interval: Duration,

    /// Maximum time to wait for a Pong reply after sending a Ping.
    ///
    /// If no Pong (or other frame) is received within this window the
    /// current Ping is considered "missed".
    pub pong_timeout: Duration,

    /// Number of consecutive missed Pongs that triggers connection
    /// closure.
    ///
    /// Once this threshold is reached the connection is deemed
    /// unresponsive and will be torn down with an appropriate close code.
    pub max_missed_pongs: u32,

    /// Idle connection timeout.
    ///
    /// If no frames of any kind (data, Ping, Pong) are received within
    /// this duration the connection is closed with [`CloseCode::IdleTimeout`].
    ///
    /// [`CloseCode::IdleTimeout`]: crate::protocol::close::CloseCode::IdleTimeout
    pub idle_timeout: Duration,

    /// Maximum random jitter added to the client-side ping interval.
    ///
    /// A uniformly distributed random value in `[0, jitter]` is added to
    /// `ping_interval` on each cycle to prevent heartbeat synchronization
    /// (the "thundering herd" problem) across many concurrent connections.
    pub jitter: Duration,
}

impl Default for HeartbeatPolicy {
    /// Returns the default heartbeat policy for ordinary web applications
    /// as defined in spec §27.1.
    ///
    /// The defaults are tuned for typical browser-based clients on
    /// commodity networks.  High-throughput or low-latency deployments
    /// may wish to tighten these values.
    fn default() -> Self {
        Self {
            ping_interval: Duration::from_millis(25_000),
            pong_timeout: Duration::from_millis(10_000),
            max_missed_pongs: 2,
            idle_timeout: Duration::from_secs(300),
            jitter: Duration::from_millis(2_500),
        }
    }
}

impl HeartbeatPolicy {
    /// Reject nonsensical values that would break connection
    /// health-monitoring. Returns `Err` with a human-readable
    /// message identifying the offending field. Callers should
    /// invoke this from `ServerConfig::validate` (or equivalent)
    /// before the policy is handed to the connection layer.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.ping_interval.is_zero() {
            return Err("ping_interval must be > 0");
        }
        if self.pong_timeout.is_zero() {
            return Err("pong_timeout must be > 0");
        }
        if self.max_missed_pongs == 0 {
            return Err("max_missed_pongs must be > 0");
        }
        if self.idle_timeout.is_zero() {
            return Err("idle_timeout must be > 0");
        }
        if self.jitter > self.ping_interval {
            return Err("jitter must be <= ping_interval");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults() {
        let h = HeartbeatPolicy::default();
        assert_eq!(h.ping_interval, Duration::from_millis(25_000));
        assert_eq!(h.pong_timeout, Duration::from_millis(10_000));
    }
}
