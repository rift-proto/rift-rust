//! # In-Process Metrics Counters
//!
//! This module implements the metrics system defined in spec section 23.2,
//! using in-process `AtomicU64` counters that are zero-allocation, lock-free
//! (using `Relaxed` ordering semantics), and can be updated from any thread.
//!
//! ## Metric Categories
//!
//! | Category | Example Metrics |
//! |----------|----------------|
//! | **Connection** | Active connections, total opened/closed, reconnects, heartbeat timeouts |
//! | **Message** | Inbound/outbound totals, dropped, replayed, expired, ack timeouts, deduplicates |
//! | **Backpressure** | Send/receive queue depth, slow consumers, flow pause/resume counts, volatile drops |
//!
//! ## Usage
//!
//! ```rust
//! use rifts_core::metrics::Metrics;
//!
//! let metrics = Metrics::new();
//! metrics.inc(&metrics.connection_open_total);
//! metrics.add(&metrics.messages_in_total, 42);
//! ```
//!
//! ## Production Integration
//!
//! In production environments, these counters should be exported to Prometheus or OTLP.
//! The `Debug` impl prints the current value of all counters for convenient debugging.

use std::sync::atomic::{AtomicU64, Ordering};

/// Process-wide global metrics collection.
///
/// All fields are `AtomicU64` and support concurrent reads and writes.
/// Increment atomically via [`Metrics::inc`] and [`Metrics::add`].
#[derive(Debug, Default)]
pub struct Metrics {
    // ── Connection Metrics ─────────────────────────────────────────────────────
    /// Current number of active connections (incremented on open, decremented on close).
    pub active_connections: AtomicU64,
    /// Total number of connections opened (cumulative).
    pub connection_open_total: AtomicU64,
    /// Total number of connections closed (cumulative).
    pub connection_close_total: AtomicU64,
    /// Total number of client reconnect attempts.
    pub reconnect_total: AtomicU64,
    /// Total number of successful disconnect-resume recoveries.
    pub resume_success_total: AtomicU64,
    /// Total number of failed disconnect-resume recoveries.
    pub resume_failed_total: AtomicU64,
    /// Total number of disconnections caused by heartbeat timeouts.
    pub heartbeat_timeout_total: AtomicU64,

    // ── Message Metrics ───────────────────────────────────────────────────────
    /// Total number of inbound messages received (messages published by clients).
    pub messages_in_total: AtomicU64,
    /// Total number of outbound messages delivered (messages sent to clients).
    pub messages_out_total: AtomicU64,
    /// Total number of messages dropped (due to queue overflow, expiration, backpressure, etc.).
    pub messages_dropped_total: AtomicU64,
    /// Total number of messages replayed (historical messages sent after client disconnect-resume).
    pub messages_replayed_total: AtomicU64,
    /// Total number of messages expired (due to TTL or `expires_at` timeout).
    pub messages_expired_total: AtomicU64,
    /// Total number of acknowledgment timeouts (consumer did not ack within the required time).
    pub ack_timeout_total: AtomicU64,
    /// Total number of deduplication hits (duplicate `dedupe_key` detected).
    pub duplicate_total: AtomicU64,

    // ── Backpressure Metrics ──────────────────────────────────────────────────
    /// Current depth of the outbound send queue (in bytes).
    ///
    /// **Note**: This metric must be updated manually; it is not automatically synchronized.
    pub send_queue_depth: AtomicU64,
    /// Current depth of the inbound receive queue (in bytes).
    pub recv_queue_depth: AtomicU64,
    /// Total number of slow-consumer events triggered (outbound queue exceeded threshold).
    pub slow_consumer_total: AtomicU64,
    /// Total number of flow-control pause events (sender entered backpressure state).
    pub flow_pause_total: AtomicU64,
    /// Total number of flow-control resume events (backpressure relieved).
    pub flow_resume_total: AtomicU64,
    /// Total number of volatile message drops (low-priority messages discarded first during backpressure).
    pub volatile_drop_total: AtomicU64,
    /// Total number of state message coalescing events (State-type messages merged due to backpressure).
    pub state_coalesce_total: AtomicU64,
}

impl Metrics {
    /// Creates a new metrics collection with all counters at zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Atomically increments the specified counter by 1.
    ///
    /// Uses `Relaxed` ordering semantics — for metrics counters, strict global
    /// ordering is not important; only atomicity is required.
    pub fn inc(&self, counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Atomically increments the specified counter by `n`.
    ///
    /// Useful for batch counting (e.g., processing multiple messages at once).
    pub fn add(&self, counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inc_and_add() {
        let m = Metrics::new();
        m.inc(&m.connection_open_total);
        m.inc(&m.connection_open_total);
        m.add(&m.messages_in_total, 7);
        assert_eq!(m.connection_open_total.load(Ordering::Relaxed), 2);
        assert_eq!(m.messages_in_total.load(Ordering::Relaxed), 7);
    }
}
