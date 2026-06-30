//! # Session Store — Cross-Connection Session Persistence
//!
//! The [`SessionStore`] maintains a mapping from session IDs to live
//! [`Session`] objects and a shared [`OffsetTracker`] that records
//! per-session per-topic offset progress. This allows the server to
//! locate a previous session when a client reconnects and attempt
//! session resumption (spec 13).
//!
//! ## Thread Safety
//!
//! The inner map is protected by a [`parking_lot::RwLock`], allowing
//! concurrent readers (multiple handshakes looking up sessions) with
//! exclusive writers (session creation and removal).

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use super::offset_tracker::OffsetTracker;
use super::session::{Session, SessionId};

/// Thread-safe, shared store of active sessions and their offset progress.
///
/// `SessionStore` is designed to be cloned cheaply (`Arc`-backed) and
/// shared across all `Connection` instances in the server, enabling
/// cross-connection session resumption.
#[derive(Clone)]
pub struct SessionStore {
    /// Maps session ID string → session object. Protected by an
    /// `RwLock` for concurrent read access during handshakes.
    inner: Arc<RwLock<HashMap<String, Arc<Session>>>>,

    /// Shared offset tracker recording per-session per-topic offsets.
    /// This tracker persists across connections so that resume
    /// evaluation can use the recorded progress.
    offset_tracker: Arc<OffsetTracker>,
}

impl SessionStore {
    /// Create a new, empty session store.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            offset_tracker: Arc::new(OffsetTracker::new()),
        }
    }

    /// Look up a session by its ID.
    ///
    /// Returns `Some(session)` if the session exists in the store,
    /// `None` otherwise. The returned `Arc<Session>` is a shared
    /// reference that can be used concurrently.
    pub fn get(&self, session_id: &str) -> Option<Arc<Session>> {
        self.inner.read().get(session_id).cloned()
    }

    /// Insert or replace a session in the store.
    ///
    /// If a session with the same ID already exists, it is replaced
    /// and the old session is returned. The old session's offset
    /// history is also forgotten so it does not leak after a
    /// session-id reuse.
    pub fn insert(&self, session: Arc<Session>) -> Option<Arc<Session>> {
        let old = self.inner.write().insert(session.id.0.clone(), session);
        if let Some(ref prev) = old {
            self.offset_tracker.forget(&prev.id);
        }
        old
    }

    /// Remove a session from the store and clean up its offset data.
    ///
    /// Returns `true` if the session was present and removed.
    pub fn remove(&self, session_id: &str) -> bool {
        let sid = SessionId(session_id.to_string());
        self.offset_tracker.forget(&sid);
        self.inner.write().remove(session_id).is_some()
    }

    /// Returns a shared reference to the offset tracker.
    ///
    /// The offset tracker records per-session per-topic offset progress
    /// and is used by the resume system to evaluate what data a
    /// reconnecting client has missed.
    pub fn offset_tracker(&self) -> &Arc<OffsetTracker> {
        &self.offset_tracker
    }

    /// Returns the number of sessions currently in the store.
    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    /// Returns `true` if the store contains no sessions.
    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }

    /// Remove sessions whose idle time exceeds `idle_timeout`.
    ///
    /// Sessions in `Closed` or `Draining` state are always considered
    /// expired and are removed regardless of idle time. Active sessions
    /// are removed only when their idle duration exceeds the timeout.
    ///
    /// Offsets belonging to removed sessions are forgotten. Returns the
    /// number of sessions removed.
    pub fn expire_sessions(&self, idle_timeout: std::time::Duration) -> usize {
        let mut expired = Vec::new();
        {
            let guard = self.inner.read();
            for (id, session) in guard.iter() {
                let state = session.state();
                match state {
                    crate::session::session::SessionState::Closed
                    | crate::session::session::SessionState::Draining => {
                        expired.push(id.clone());
                    }
                    _ => {
                        if session.idle() > idle_timeout {
                            expired.push(id.clone());
                        }
                    }
                }
            }
        }
        for id in &expired {
            self.remove(id);
        }
        expired.len()
    }
}

impl Default for SessionStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::session::ClientId;

    #[test]
    fn insert_and_get() {
        let store = SessionStore::new();
        let s = Arc::new(Session::new(SessionId::new(), ClientId::new("c1")));
        let id = s.id.0.clone();
        store.insert(s.clone());
        assert!(store.get(&id).is_some());
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn remove_cleans_offsets() {
        let store = SessionStore::new();
        let s = Arc::new(Session::new(SessionId::new(), ClientId::new("c1")));
        let id = s.id.0.clone();
        let sid = s.id.clone();
        store.insert(s);
        store.offset_tracker().record(&sid, "t", 5);
        assert_eq!(store.offset_tracker().get(&sid, "t"), Some(5));
        assert!(store.remove(&id));
        assert!(store.get(&id).is_none());
        assert_eq!(store.offset_tracker().get(&sid, "t"), None);
    }
}
