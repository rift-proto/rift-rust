//! # Resume Manager
//!
//! Orchestrates session resumption as defined in the protocol specification
//! sections 5.4 and 13. When a client reconnects after a transport
//! interruption, it presents its `SessionId`, the epoch it last knew, and
//! its last-acknowledged offsets. The [`ResumeManager`] validates these
//! against the server's current state and returns a [`ResumeOutcome`]
//! telling the caller what action to take.
//!
//! ## Resume Flow
//!
//! 1. Client reconnects and sends its `SessionId`, epoch, and offsets.
//! 2. The server locates the existing [`Session`](crate::session::session::Session).
//! 3. [`ResumeManager::evaluate`] checks:
//!    - Is the session still alive (not closed/expired)?
//!    - Does the client's epoch match the server's current epoch?
//!    - What do the client's offsets tell us about missed data?
//! 4. A [`ResumeOutcome`] is returned and acted upon by the broker layer.
//!
//! ## Epoch Validation
//!
//! Each session has a monotonically increasing epoch counter. The epoch is
//! bumped every time the session goes through a resume cycle. If the client
//! presents an epoch that does not match the server's current value, the
//! resume is rejected with a [`SessionReject::Conflict`](crate::error::SessionReject::Conflict)
//! error, forcing the client to establish a new session.

use std::collections::HashMap;

use crate::error::{Result, RiftError, SessionReject};
use crate::session::offset_tracker::{OffsetTracker, ResumeDecision, decide};
use crate::session::session::Session;
use crate::topic::TopicStore;

/// High-level outcome of a resume attempt.
///
/// This enum is the public-facing counterpart of
/// [`ResumeDecision`](crate::session::offset_tracker::ResumeDecision),
/// translated into actionable guidance for the server's broker layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeOutcome {
    /// Session fully resumed. The client's offsets match the server's
    /// head and no replay is needed. The client may proceed normally.
    Resumed,

    /// Some topics required a replay. The client should process the
    /// replayed frames before continuing with new subscriptions.
    Partial,

    /// The server is still replaying data to the client. The client
    /// should wait until the replay completes before sending new
    /// messages.
    Replaying,

    /// The client must pull a fresh snapshot for at least one topic
    /// because the topic no longer exists on the server or its data
    /// has been compacted away.
    SnapshotRequired,

    /// The client submitted empty offsets, indicating a cold start.
    /// It must re-subscribe to all desired topics from scratch.
    ColdStart,

    /// The resume attempt was rejected (e.g., epoch mismatch, client
    /// ahead of server). The client must establish a new session.
    Rejected,
}

/// Convert a low-level [`ResumeDecision`] into the high-level
/// [`ResumeOutcome`] used by the broker layer.
impl From<ResumeDecision> for ResumeOutcome {
    fn from(d: ResumeDecision) -> Self {
        match d {
            ResumeDecision::FullResume => ResumeOutcome::Resumed,
            ResumeDecision::PartialResume => ResumeOutcome::Partial,
            ResumeDecision::Replaying => ResumeOutcome::Replaying,
            ResumeDecision::SnapshotRequired => ResumeOutcome::SnapshotRequired,
            ResumeDecision::ColdStart => ResumeOutcome::ColdStart,
            ResumeDecision::Rejected => ResumeOutcome::Rejected,
        }
    }
}

/// Resume manager that coordinates session resumption.
///
/// The offset tracker used during resume evaluation is owned by the
/// `SessionStore` (not duplicated here); this manager only orchestrates
/// the resume flow.
pub struct ResumeManager {}

impl Default for ResumeManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ResumeManager {
    /// Create a new resume manager.
    pub fn new() -> Self {
        Self {}
    }

    /// Evaluate a resume attempt and return the appropriate outcome.
    ///
    /// This method performs three checks in order:
    ///
    /// 1. **Liveness** -- Is the session still alive (not in `Closed`
    ///    state)? Returns [`SessionReject::Expired`](crate::error::SessionReject::Expired)
    ///    if not.
    /// 2. **Epoch match** -- Does `incoming_epoch` match the session's
    ///    current epoch? Returns [`SessionReject::Conflict`](crate::error::SessionReject::Conflict)
    ///    if not.
    /// 3. **Offset analysis** -- Delegates to
    ///    [`decide`](crate::session::offset_tracker::decide) to compare
    ///    the client's offsets against the server's head offsets.
    ///
    /// # Arguments
    ///
    /// * `session` -- The existing server-side session being resumed.
    /// * `incoming_epoch` -- The epoch the client claims in its resume
    ///   request.
    /// * `last_offsets` -- The last offsets the client processed, keyed
    ///   by topic name.
    /// * `topic_offsets` -- The server's current head offsets per topic.
    ///
    /// # Errors
    ///
    /// Returns an error if the session is expired.
    ///
    /// **Note**: epoch validation is performed by the caller
    /// ([`Connection::handshake`]) *before* the epoch is bumped for the
    /// new incarnation, so this method does not repeat the check.
    /// Passing the bumped epoch here would trivially match
    /// `session.current_epoch()` and make the check a no-op.
    pub fn evaluate(
        &self,
        session: &Session,
        _server_epoch: u32,
        last_offsets: &HashMap<String, i64>,
        topic_offsets: &HashMap<String, i64>,
    ) -> Result<ResumeOutcome> {
        if !session.is_alive() {
            return Err(RiftError::Session(SessionReject::Expired));
        }
        Ok(decide(last_offsets, topic_offsets).into())
    }

    /// Compute the head offset per topic currently in the store.
    ///
    /// Iterates over the given topic names and, for each one present in
    /// the [`TopicStore`], reads its head offset. Topics that do not
    /// exist in the store are silently omitted from the result.
    ///
    /// # Arguments
    ///
    /// * `store` -- The server's topic store containing topic entries.
    /// * `topics` -- The list of topic names to query.
    ///
    /// # Returns
    ///
    /// A map from topic name to its current head offset.
    pub fn topic_offsets(&self, store: &TopicStore, topics: &[String]) -> HashMap<String, i64> {
        let mut out = HashMap::new();
        for t in topics {
            if let Some(entry) = store.get(t) {
                out.insert(t.clone(), entry.head_offset());
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::session::ClientId;
    use crate::topic::profile::TopicProfile;

    #[test]
    fn evaluate_no_longer_checks_epoch() {
        // Epoch validation moved to Connection::handshake (performed
        // *before* bump_epoch), so evaluate accepts any _server_epoch
        // value without error.
        let m = ResumeManager::new();
        let s = Session::new(
            crate::session::session::SessionId::new(),
            ClientId::new("c"),
        );
        s.bump_epoch(); // server at epoch 2
        let mut last = HashMap::new();
        last.insert("t".into(), 1);
        let mut head = HashMap::new();
        head.insert("t".into(), 5);
        // Passing a mismatched epoch should now succeed — the caller is
        // responsible for epoch validation before bumping.
        let r = m.evaluate(&s, 1, &last, &head);
        assert!(r.is_ok());
    }

    #[test]
    fn happy_resume() {
        let m = ResumeManager::new();
        let s = Session::new(
            crate::session::session::SessionId::new(),
            ClientId::new("c"),
        );
        let mut last = HashMap::new();
        last.insert("t".into(), 4);
        let mut head = HashMap::new();
        head.insert("t".into(), 5);
        let r = m.evaluate(&s, s.current_epoch(), &last, &head).unwrap();
        assert_eq!(r, ResumeOutcome::Replaying);
    }

    #[test]
    fn topic_offsets_from_store() {
        use crate::broker::offset_store::OffsetStore;
        let m = ResumeManager::new();
        let store = TopicStore::new();
        let offsets = OffsetStore::new();
        let entry = store.get_or_create("t", TopicProfile::default()).unwrap();
        // Use OffsetStore for authoritative offsets.
        let o1 = offsets.alloc("t");
        let o2 = offsets.alloc("t");
        entry.append(crate::topic::store::LogEntry {
            offset: o1,
            publisher_session: None,
            message_id: "m1".into(),
            class: "event".into(),
            event: Some("e".into()),
            payload: bytes::Bytes::from_static(b"x"),
            timestamp: 0,
            appended_at: None,
        });
        entry.append(crate::topic::store::LogEntry {
            offset: o2,
            publisher_session: None,
            message_id: "m2".into(),
            class: "event".into(),
            event: Some("e".into()),
            payload: bytes::Bytes::from_static(b"x"),
            timestamp: 0,
            appended_at: None,
        });
        let heads = m.topic_offsets(&store, &["t".to_string()]);
        assert_eq!(heads.get("t").copied(), Some(2));
    }
}
