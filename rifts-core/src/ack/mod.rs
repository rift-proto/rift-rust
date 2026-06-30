//! Acknowledgement system (Rift spec section 12).
//!
//! This module implements the server-side acknowledgement tracking used to
//! confirm to publishers that their messages have been received, persisted,
//! or delivered. The acknowledgement system has three components:
//!
//! 1. **[`AckStatus`]** — a status enum describing the outcome of a message
//!    (received, persisted, delivered, failed, etc.).
//!
//! 2. **[`AckPolicy`]** — an enum describing the level of acknowledgement
//!    guarantee requested by the publisher (none, server-only, quorum, etc.).
//!
//! 3. **[`AckManager`]** — a per-session tracker that records outstanding
//!    (sent-but-not-acked) message ids and their deadlines, allowing the
//!    server to reap timed-out messages.
//!
//! # Lifecycle
//!
//! When a publisher sends a data frame with the `REQUIRES_ACK` flag set:
//!
//! 1. The broker publishes the message and returns an outcome.
//! 2. The server creates an [`Ack`] and sends it back as an ack frame.
//! 3. The publisher responds with an ack control frame to confirm receipt.
//! 4. The server calls [`AckManager::complete`] to clear the tracking entry.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::now_ms;

use parking_lot::Mutex;
use uuid::Uuid;

/// Status of an acknowledgement (Rift spec section 12.1).
///
/// Each status represents a stage in the message processing pipeline,
/// from initial receipt through final delivery or failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckStatus {
    /// The server has received the message but has not yet processed it.
    Received,

    /// The message has been accepted by the server (semantic validation passed).
    Accepted,

    /// The message has been durably persisted (written to the replay log
    /// or external store).
    Persisted,

    /// The message has been delivered to all (or a quorum of) subscribers.
    Delivered,

    /// The message has been processed by a subscriber (end-to-end acknowledgement).
    Processed,

    /// The message was rejected (schema mismatch, policy violation, etc.).
    Rejected,

    /// The message expired (TTL exceeded) before it could be delivered.
    Expired,

    /// The message is a duplicate of one already seen (deduplication window).
    Duplicate,

    /// The message could not be delivered due to an internal error.
    Failed,
}

impl AckStatus {
    /// Return the lowercase wire representation of this status.
    pub fn as_str(self) -> &'static str {
        match self {
            AckStatus::Received => "received",
            AckStatus::Accepted => "accepted",
            AckStatus::Persisted => "persisted",
            AckStatus::Delivered => "delivered",
            AckStatus::Processed => "processed",
            AckStatus::Rejected => "rejected",
            AckStatus::Expired => "expired",
            AckStatus::Duplicate => "duplicate",
            AckStatus::Failed => "failed",
        }
    }
}

/// Acknowledgement policy (Rift spec section 12.3).
///
/// The policy determines the level of delivery guarantee the server
/// must achieve before sending an acknowledgement back to the publisher.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckPolicy {
    /// No acknowledgement is sent. The publisher fire-and-forgets.
    None,

    /// The server acknowledges receipt (before persistence or delivery).
    Server,

    /// The server acknowledges after the message is durably persisted.
    Persisted,

    /// The server acknowledges after a quorum of replicas has persisted
    /// the message (distributed deployments).
    Quorum,

    /// The server acknowledges after the message has been delivered to
    /// all subscribers.
    Subscriber,

    /// The server acknowledges after the application layer confirms
    /// processing (end-to-end acknowledgement).
    Application,
}

/// Acknowledgement payload sent to the peer.
///
/// Contains all the information the publisher needs to correlate the
/// acknowledgement with the original message and determine the outcome.
#[derive(Debug, Clone)]
pub struct Ack {
    /// Unique acknowledgement id, generated as a UUID v4.
    pub ack_id: String,

    /// The message id this acknowledgement corresponds to.
    pub message_id: String,

    /// The status of the acknowledgement.
    pub status: AckStatus,

    /// The broker-assigned offset of the persisted message, if applicable.
    pub offset: Option<i64>,

    /// Human-readable reason for the status (e.g. an error message for
    /// `Failed` or `Rejected`).
    pub reason: Option<String>,

    /// Machine-readable error code, if applicable.
    pub error_code: Option<String>,

    /// Suggested retry delay in milliseconds (used with rate-limiting
    /// or overload responses).
    pub retry_after_ms: Option<u32>,

    /// Server-side timestamp in milliseconds since the Unix epoch at
    /// which the acknowledgement was generated.
    pub server_time: i64,
}

impl Ack {
    /// Create a new acknowledgement for the given message id and status.
    ///
    /// A unique `ack_id` is generated automatically. The `server_time`
    /// is set to `now_ms()`.
    pub fn new(message_id: impl Into<String>, status: AckStatus) -> Self {
        Self {
            ack_id: Uuid::new_v4().to_string(),
            message_id: message_id.into(),
            status,
            offset: None,
            reason: None,
            error_code: None,
            retry_after_ms: None,
            server_time: now_ms(),
        }
    }

    /// Attach a broker-assigned offset to the acknowledgement.
    ///
    /// This is used when the message has been persisted and the publisher
    /// needs to know the exact offset for resume or audit purposes.
    pub fn with_offset(mut self, offset: i64) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Attach a human-readable reason string to the acknowledgement.
    ///
    /// Typically used for error statuses (`Failed`, `Rejected`) to convey
    /// the failure cause back to the publisher.
    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }
}

/// Default per-session cap on outstanding tracked messages. Tuned to
/// accommodate bursty workloads (e.g. 10 K outstanding / session)
/// while still bounding the worst-case memory consumption of a
/// single misbehaving session.
pub const DEFAULT_MAX_OUTSTANDING_PER_SESSION: usize = 10_000;

/// Maximum number of sessions the ack manager will track simultaneously.
/// Beyond this limit, new sessions are rejected (track returns false).
pub const DEFAULT_MAX_SESSIONS: usize = 100_000;

/// Tracks outstanding (sent-but-not-acked) frames per session.
///
/// The `AckManager` is shared across all connections via
/// [`SharedAckManager`] (an `Arc<AckManager>`). Each session's outstanding
/// messages are stored as a map from `message_id` to deadline timestamp.
///
/// # Thread safety
///
/// The inner map is protected by a [`parking_lot::Mutex`] which is held
/// only briefly for insertions, removals, and reaps.
pub struct AckManager {
    /// Map from session id to (message id → deadline in ms since epoch).
    outstanding: Mutex<HashMap<String, HashMap<String, i64>>>,
    /// Per-session cap on outstanding ack-tracked messages. New
    /// `track` calls beyond this limit return `false` so the caller
    /// can reject / disconnect a client that is not consuming
    /// acks fast enough.
    max_outstanding_per_session: usize,
}

impl Default for AckManager {
    fn default() -> Self {
        Self::new()
    }
}

impl AckManager {
    /// Create a new, empty acknowledgement manager.
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_MAX_OUTSTANDING_PER_SESSION)
    }

    /// Create a new manager with a custom per-session outstanding
    /// cap.
    pub fn with_capacity(max_outstanding_per_session: usize) -> Self {
        Self {
            outstanding: Mutex::new(HashMap::new()),
            max_outstanding_per_session,
        }
    }

    /// Mark a message as awaiting acknowledgement for a given session.
    ///
    /// The `timeout` is added to the current time to compute the deadline
    /// after which the message is considered timed out.
    ///
    /// Returns `false` (without recording) if the session already has
    /// `max_outstanding_per_session` tracked messages; callers
    /// should treat this as backpressure and either drop the
    /// message or close the connection.
    pub fn track(&self, session_id: &str, message_id: &str, timeout: Duration) -> bool {
        let deadline = now_ms().saturating_add(timeout.as_millis().try_into().unwrap_or(i64::MAX));
        let mut g = self.outstanding.lock();
        // Enforce per-process session count limit to prevent unbounded
        // growth from session floods.
        if !g.contains_key(session_id) && g.len() >= DEFAULT_MAX_SESSIONS {
            return false;
        }
        let entry = g.entry(session_id.to_string()).or_default();
        if entry.len() >= self.max_outstanding_per_session {
            return false;
        }
        entry.insert(message_id.to_string(), deadline);
        true
    }

    /// Mark a message as acknowledged.
    ///
    /// Returns `true` if the message was being tracked (and is now
    /// removed); `false` if it was not found (already acked or never
    /// tracked).
    pub fn complete(&self, session_id: &str, message_id: &str) -> bool {
        self.outstanding
            .lock()
            .get_mut(session_id)
            .and_then(|m| m.remove(message_id))
            .is_some()
    }

    /// Find and remove timed-out messages for a session.
    ///
    /// Returns the list of message ids whose deadline has passed. The
    /// returned ids are removed from the tracking set so they will not
    /// be reported again.
    pub fn reap_timeouts(&self, session_id: &str) -> Vec<String> {
        let now = now_ms();
        let mut g = self.outstanding.lock();
        let map = match g.get_mut(session_id) {
            Some(m) => m,
            None => return Vec::new(),
        };
        let expired: Vec<String> = map
            .iter()
            .filter(|(_, deadline)| **deadline <= now)
            .map(|(k, _)| k.clone())
            .collect();
        for k in &expired {
            map.remove(k);
        }
        expired
    }

    /// Drop all tracking state for a session.
    ///
    /// Called when a connection is torn down so that the session's
    /// outstanding entries do not leak.
    pub fn forget(&self, session_id: &str) {
        self.outstanding.lock().remove(session_id);
    }

    /// Reap timed-out messages across all sessions.
    ///
    /// Iterates over every tracked session, removes entries whose
    /// deadline has passed, and then drops any session buckets that
    /// became empty. Returns the total number of timed-out message
    /// entries removed.
    pub fn reap_all_timeouts(&self) -> usize {
        let now = now_ms();
        let mut g = self.outstanding.lock();
        let mut total = 0;
        // Collect expired entries per session, then clean up.
        let mut empty_sessions = Vec::new();
        for (sid, entries) in g.iter_mut() {
            let expired: Vec<String> = entries
                .iter()
                .filter(|(_, deadline)| **deadline <= now)
                .map(|(k, _)| k.clone())
                .collect();
            for k in &expired {
                entries.remove(k);
            }
            total += expired.len();
            if entries.is_empty() {
                empty_sessions.push(sid.clone());
            }
        }
        for sid in &empty_sessions {
            g.remove(sid);
        }
        total
    }
}

/// Type alias for a shared, reference-counted [`AckManager`].
///
/// Passed to each [`Connection`](crate::connection::Connection) at
/// construction time so that all connections share the same manager.
pub type SharedAckManager = Arc<AckManager>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn track_and_complete() {
        let m = AckManager::new();
        m.track("s1", "m1", Duration::from_secs(5));
        assert!(m.complete("s1", "m1"));
        assert!(!m.complete("s1", "m1"));
    }

    #[test]
    fn reap_timeouts_returns_expired() {
        let m = AckManager::new();
        m.track("s1", "m1", Duration::from_millis(0));
        m.track("s1", "m2", Duration::from_secs(60));
        // Force expiry of m1 by manipulating deadline.
        m.outstanding
            .lock()
            .get_mut("s1")
            .unwrap()
            .insert("m1".into(), 0);
        let expired = m.reap_timeouts("s1");
        assert_eq!(expired, vec!["m1".to_string()]);
    }

    #[test]
    fn forget_session() {
        let m = AckManager::new();
        m.track("s1", "m1", Duration::from_secs(5));
        m.forget("s1");
        assert!(!m.complete("s1", "m1"));
    }

    #[test]
    fn ack_constructors() {
        let a = Ack::new("m1", AckStatus::Persisted).with_offset(7);
        assert_eq!(a.offset, Some(7));
        assert_eq!(a.status, AckStatus::Persisted);
    }
}
