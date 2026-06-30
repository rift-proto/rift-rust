//! # Resume Manager
//!
//! Orchestrates session resumption as defined in the protocol specification
//! sections 5.4 and 13. When a client reconnects after a transport
//! interruption, it presents its `SessionId`, the epoch it last knew, and
//! its last-acknowledged offsets. The [`ResumeManager`] validates these
//! against the server's current state and returns a [`ResumeDecision`]
//! telling the caller what action to take.
//!
//! ## Resume Flow
//!
//! 1. Client reconnects and sends its `SessionId`, epoch, and offsets.
//! 2. The server locates the existing [`Session`].
//! 3. [`ResumeManager::evaluate`] checks:
//!    - Is the session still alive (not closed/expired)?
//!    - Does the client's epoch match the server's current epoch?
//!    - What do the client's offsets tell us about missed data?
//! 4. A [`ResumeDecision`] is returned and acted upon by the broker layer.
//!
//! ## Epoch Validation
//!
//! Each session has a monotonically increasing epoch counter. The epoch is
//! bumped every time the session goes through a resume cycle. If the client
//! presents an epoch that does not match the server's current value, the
//! resume is rejected with a [`SessionReject::Conflict`]
//! error, forcing the client to establish a new session.

use std::collections::HashMap;

use crate::session::offset_tracker::ResumeDecision;
use crate::session::offset_tracker::decide;
use crate::session::session::Session;
use rifts_core::error::{Result, RiftError, SessionReject};
use rifts_core::topic::TopicStore;

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

    /// Evaluate a resume attempt and return the appropriate decision.
    ///
    /// This method performs two checks in order:
    ///
    /// 1. **Liveness** -- Is the session still alive (not in `Closed`
    ///    state)? Returns [`SessionReject::Expired`]
    ///    if not.
    /// 2. **Offset analysis** -- Delegates to
    ///    [`decide`] to compare
    ///    the client's offsets against the server's head offsets.
    ///
    /// **Note**: epoch validation is performed by the caller
    /// *before* the epoch is bumped for the
    /// new incarnation, so this method does not repeat the check.
    pub fn evaluate(
        &self,
        session: &Session,
        last_offsets: &HashMap<String, i64>,
        topic_offsets: &HashMap<String, i64>,
    ) -> Result<ResumeDecision> {
        if !session.is_alive() {
            return Err(RiftError::Session(SessionReject::Expired));
        }
        Ok(decide(last_offsets, topic_offsets))
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
    use rifts_core::topic::profile::TopicProfile;

    #[test]
    fn evaluate_checks_liveness() {
        let m = ResumeManager::new();
        let s = Session::new(
            crate::session::session::SessionId::new(),
            ClientId::new("c"),
        );
        s.bump_epoch();
        let mut last = HashMap::new();
        last.insert("t".into(), 1);
        let mut head = HashMap::new();
        head.insert("t".into(), 5);
        let r = m.evaluate(&s, &last, &head);
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
        let r = m.evaluate(&s, &last, &head).unwrap();
        assert_eq!(r, ResumeDecision::Replaying);
    }

    #[tokio::test]
    async fn topic_offsets_from_store() {
        use rifts_storage::{MemoryOffsetStore, OffsetStore};
        let m = ResumeManager::new();
        let store = TopicStore::new();
        let offsets = MemoryOffsetStore::new();
        let entry = store.get_or_create("t", TopicProfile::default()).unwrap();
        let o1 = offsets.alloc("t").await;
        let o2 = offsets.alloc("t").await;
        entry.append(rifts_core::topic::store::LogEntry {
            offset: o1,
            publisher_session: None,
            message_id: "m1".into(),
            class: "event".into(),
            event: Some("e".into()),
            payload: bytes::Bytes::from_static(b"x"),
            timestamp: 0,
            appended_at: None,
        });
        entry.append(rifts_core::topic::store::LogEntry {
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
