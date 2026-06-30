//! # Offset Tracker
//!
//! Tracks the last processed offset per topic for each session. This
//! information is the foundation of the resume mechanism defined in the
//! protocol specification section 13.2: when a client reconnects it
//! submits its `last_offsets`, and the server uses those offsets (along
//! with the current head offsets) to decide what data the client missed.
//!
//! ## Components
//!
//! * [`OffsetTracker`] -- A thread-safe, in-memory store mapping
//!   `(SessionId, topic)` pairs to the latest offset the session has
//!   acknowledged. Used by the server to record progress and by the
//!   resume system to retrieve the client's last-known position.
//!
//! * [`ResumeDecision`] -- An enum describing the outcome of comparing
//!   a client's last offsets against the server's current head offsets.
//!   Each variant maps to a distinct action the server should take.
//!
//! * [`decide`] -- The pure function that computes a [`ResumeDecision`]
//!   given the client's and server's offset maps.

use std::collections::HashMap;

use parking_lot::Mutex;

use crate::session::session::SessionId;

/// Per-session cap on tracked topics to prevent unbounded growth
/// from a single misbehaving session.
pub const MAX_TOPICS_PER_SESSION: usize = 1024;

/// Per-session offset tracker.
///
/// Maintains a mapping from [`SessionId`] to a set of `(topic, offset)`
/// pairs. The inner map is protected by a [`Mutex`] for concurrent access.
///
/// Each session is limited to `MAX_TOPICS_PER_SESSION` tracked topics.
#[derive(Default)]
pub struct OffsetTracker {
    /// Maps each session to its topic-to-offset map.
    inner: Mutex<HashMap<SessionId, HashMap<String, i64>>>,
}

impl OffsetTracker {
    /// Create a new, empty [`OffsetTracker`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the latest offset the session has processed for `topic`.
    ///
    /// If the session already has `MAX_TOPICS_PER_SESSION` tracked
    /// topics, the oldest topic is evicted.
    pub fn record(&self, session: &SessionId, topic: &str, offset: i64) {
        let mut g = self.inner.lock();
        let topics = g.entry(session.clone()).or_default();
        if topics.len() >= MAX_TOPICS_PER_SESSION && !topics.contains_key(topic) {
            // Evict the oldest topic (lowest offset) to make room.
            if let Some(oldest) = topics
                .iter()
                .min_by_key(|(_, v)| *v)
                .map(|(k, _)| k.clone())
            {
                topics.remove(&oldest);
            }
        }
        topics.insert(topic.to_string(), offset);
    }

    /// Read the last recorded offset for `(session, topic)`.
    ///
    /// Returns `None` if the session has never recorded an offset for
    /// this topic, or if the session itself is unknown to the tracker.
    pub fn get(&self, session: &SessionId, topic: &str) -> Option<i64> {
        self.inner
            .lock()
            .get(session)
            .and_then(|m| m.get(topic).copied())
    }

    /// Bulk read of all topics and their offsets for a session.
    ///
    /// Returns a cloned snapshot of the topic-to-offset map for the
    /// given session. If the session has never been recorded, returns
    /// an empty map.
    pub fn snapshot(&self, session: &SessionId) -> HashMap<String, i64> {
        self.inner.lock().get(session).cloned().unwrap_or_default()
    }

    /// Drop all offset data for a session.
    ///
    /// After this call, [`get`](Self::get) and [`snapshot`](Self::snapshot)
    /// will return `None` / empty for this session. This is typically
    /// called when a session is closed or expired.
    pub fn forget(&self, session: &SessionId) {
        self.inner.lock().remove(session);
    }
}

/// Resume decision result as defined in the protocol specification
/// section 13.3.
///
/// Each variant describes a different relationship between the client's
/// last-known offsets and the server's current head offsets, and maps
/// directly to a strategy the server should follow when deciding whether
/// to allow a resume.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeDecision {
    /// The client's offsets match the server's head exactly. The client
    /// has missed nothing and can proceed immediately.
    FullResume,

    /// The client is behind on at least one topic by more than one
    /// offset. A partial replay is needed for those topics.
    PartialResume,

    /// The client is behind on at least one topic but only by a single
    /// offset within each topic. A lightweight replay of the missing
    /// messages is sufficient.
    Replaying,

    /// At least one topic the client tracks is not present on the
    /// server (possibly due to compaction or expiry). The client must
    /// pull a fresh snapshot before continuing.
    SnapshotRequired,

    /// The client submitted an empty offset map, indicating it has
    /// never subscribed to any topics. A full re-subscription from
    /// scratch is required.
    ColdStart,

    /// The client's reported offset for at least one topic exceeds
    /// the server's head. This indicates data corruption or a
    /// protocol violation. The resume attempt must be rejected.
    Rejected,
}

/// Decide what to do with a resume attempt given the client's offsets
/// and the topic's current state.
///
/// This is a pure function with no side effects. It iterates over the
/// client's reported offsets and compares them to the server's head
/// offsets, returning the most appropriate [`ResumeDecision`].
///
/// # Arguments
///
/// * `last_offsets` -- The offsets the client claims to have last
///   processed, keyed by topic name. May be empty for a cold start.
///
/// * `topic_offsets` -- The server's current head offsets, keyed by
///   topic name.
///
/// # Decision Logic
///
/// 1. Empty `last_offsets` => [`ColdStart`](ResumeDecision::ColdStart).
/// 2. Any topic missing from the server => [`SnapshotRequired`](ResumeDecision::SnapshotRequired).
/// 3. Client ahead of server on any topic => [`Rejected`](ResumeDecision::Rejected).
/// 4. Client behind by exactly 1 on every lagging topic => [`Replaying`](ResumeDecision::Replaying).
/// 5. Client behind by more than 1 on any topic => [`PartialResume`](ResumeDecision::PartialResume).
/// 6. All offsets match => [`FullResume`](ResumeDecision::FullResume).
pub fn decide(
    last_offsets: &HashMap<String, i64>,
    topic_offsets: &HashMap<String, i64>,
) -> ResumeDecision {
    if last_offsets.is_empty() {
        return ResumeDecision::ColdStart;
    }
    let mut all_within = true;
    let mut any_behind = false;
    for (topic, last) in last_offsets {
        match topic_offsets.get(topic) {
            None => {
                // Topic not present — treat as snapshot.
                return ResumeDecision::SnapshotRequired;
            }
            Some(head) => {
                if *last > *head {
                    // Client is ahead of server — reject.
                    return ResumeDecision::Rejected;
                }
                if *last < *head {
                    any_behind = true;
                }
                if *last < head.saturating_sub(1) {
                    all_within = false;
                }
            }
        }
    }
    if any_behind && all_within {
        ResumeDecision::Replaying
    } else if any_behind {
        ResumeDecision::PartialResume
    } else {
        ResumeDecision::FullResume
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_and_read() {
        let t = OffsetTracker::new();
        let s = SessionId::new();
        t.record(&s, "room/1", 10);
        t.record(&s, "room/2", 5);
        assert_eq!(t.get(&s, "room/1"), Some(10));
        let snap = t.snapshot(&s);
        assert_eq!(snap.get("room/1"), Some(&10));
        assert_eq!(snap.get("room/2"), Some(&5));
        t.forget(&s);
        assert!(t.snapshot(&s).is_empty());
    }

    #[test]
    fn decide_cold_start_when_empty() {
        let d = decide(&HashMap::new(), &HashMap::new());
        assert_eq!(d, ResumeDecision::ColdStart);
    }

    #[test]
    fn decide_rejected_when_client_ahead() {
        let mut last = HashMap::new();
        last.insert("t".to_string(), 100);
        let mut head = HashMap::new();
        head.insert("t".to_string(), 50);
        assert_eq!(decide(&last, &head), ResumeDecision::Rejected);
    }

    #[test]
    fn decide_replaying_when_slightly_behind() {
        let mut last = HashMap::new();
        last.insert("t".to_string(), 9);
        let mut head = HashMap::new();
        head.insert("t".to_string(), 10);
        assert_eq!(decide(&last, &head), ResumeDecision::Replaying);
    }

    #[test]
    fn decide_partial_when_far_behind() {
        let mut last = HashMap::new();
        last.insert("t".to_string(), 1);
        let mut head = HashMap::new();
        head.insert("t".to_string(), 100);
        assert_eq!(decide(&last, &head), ResumeDecision::PartialResume);
    }

    #[test]
    fn decide_snapshot_when_topic_missing() {
        let mut last = HashMap::new();
        last.insert("t".to_string(), 1);
        let head = HashMap::new();
        assert_eq!(decide(&last, &head), ResumeDecision::SnapshotRequired);
    }
}
