//! # Session
//!
//! Defines the core session types that represent a logical connection
//! between a client and the server. A session is the unit of identity
//! and state in the protocol: it can survive transport-layer reconnects
//! through the resume mechanism (see specification sections 5.4 and 13).
//!
//! ## Types
//!
//! * [`SessionId`] -- A globally unique, ULID-based identifier for a
//!   single session instance.
//! * [`ClientId`] -- A long-lived client identity (e.g., a username or
//!   device ID) that persists across session expirations.
//! * [`SessionState`] -- The lifecycle state machine for a session.
//! * [`Session`] -- The session object itself, holding all mutable and
//!   immutable state for a logical connection.
//!
//! ## Thread Safety
//!
//! [`Session`] is designed to be shared across async tasks. The `state`
//! field is protected by a [`parking_lot::Mutex`], while the `epoch`
//! and `last_active` fields use atomic operations for lock-free access.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use rifts_core::now_ms;

use ulid::Ulid;

/// Unique session identifier based on ULID (Universally Unique
/// Lexicographically Sortable Identifier).
///
/// A [`SessionId`] is generated server-side when a client first
/// connects and is used to correlate reconnect attempts with the
/// original session during the resume process.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionId(pub String);

impl SessionId {
    /// Generate a new, unique session identifier.
    ///
    /// The underlying ULID guarantees lexicographic sort order by
    /// creation time and uniqueness across distributed systems.
    pub fn new() -> Self {
        Self(Ulid::new().to_string())
    }

    /// Borrow the identifier as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for SessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Long-lived client identity.
///
/// Unlike [`SessionId`], which is unique per session instance, a
/// [`ClientId`] represents the logical identity of a client (e.g., a
/// user account, device serial number, or service principal). Multiple
/// sessions over time may share the same `ClientId`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClientId(pub String);

impl ClientId {
    /// Create a new client identifier from any type that converts
    /// into a `String`.
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// Borrow the identifier as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Per-session lifecycle state.
///
/// A session transitions through these states in order during its
/// lifetime. Transitions are enforced by the server's frame handler
/// and are not reversible (except for `Resuming`, which may re-enter
/// `Ready` on a successful resume).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    /// Initial state immediately after the session object is created,
    /// before any protocol frames have been exchanged.
    Open,

    /// The Hello frame has been received and the server is processing
    /// codec/version negotiation.
    Hello,

    /// The client has been successfully authenticated via the
    /// [`AuthProvider`](crate::session::auth::AuthProvider) and an
    /// [`AuthContext`](crate::session::auth::AuthContext) has been
    /// attached to the session.
    Authenticated,

    /// The client has requested a session resume and the server is
    /// evaluating offsets and replaying missed data.
    Resuming,

    /// The session is fully established and ready to subscribe to
    /// topics or publish messages. All handshake steps are complete.
    Ready,

    /// The session is actively processing messages (publishing or
    /// receiving). This is the normal operational state.
    Active,

    /// The session is in the process of shutting down gracefully.
    /// In-flight messages are being drained but no new messages are
    /// accepted.
    Draining,

    /// The session is permanently closed. No further operations are
    /// permitted. The session object may be garbage-collected.
    Closed,
}

/// A logical session that can outlive a single transport connection.
///
/// One [`Session`] is created per authenticated client. It holds the
/// session identity, lifecycle state, an epoch counter for resume
/// validation, and timestamps for idle-timeout calculations.
///
/// # Thread Safety
///
/// All fields are either atomic or mutex-protected, allowing the
/// session to be shared freely across async tasks via `Arc<Session>`.
pub struct Session {
    /// Globally unique identifier for this session instance.
    pub id: SessionId,

    /// Long-lived client identity associated with this session.
    pub client_id: ClientId,

    /// Monotonically increasing epoch counter used to detect stale
    /// resume attempts. Starts at 1 and is bumped on each resume.
    pub epoch: AtomicU32,

    /// Current lifecycle state, protected by a mutex for safe
    /// concurrent reads and writes.
    pub state: parking_lot::Mutex<SessionState>,

    /// Timestamp (milliseconds since Unix epoch) when the session was
    /// first created. Immutable for the lifetime of the session.
    pub created_at: i64,

    /// Timestamp (milliseconds since Unix epoch) of the last activity
    /// on this session. Updated atomically by [`touch`](Self::touch).
    pub last_active: AtomicU64,
}

impl Session {
    /// Create a new session in the [`Open`](SessionState::Open) state
    /// with epoch 1 and timestamps set to the current time.
    ///
    /// # Arguments
    ///
    /// * `id` -- The unique session identifier (typically a fresh ULID).
    /// * `client_id` -- The authenticated client's long-lived identity.
    pub fn new(id: SessionId, client_id: ClientId) -> Self {
        let now = now_ms();
        Self {
            id,
            client_id,
            epoch: AtomicU32::new(1),
            state: parking_lot::Mutex::new(SessionState::Open),
            created_at: now,
            last_active: AtomicU64::new(now as u64),
        }
    }

    /// Read the current lifecycle state of the session.
    ///
    /// Acquires the state mutex briefly and returns a copy of the
    /// current [`SessionState`].
    pub fn state(&self) -> SessionState {
        *self.state.lock()
    }

    /// Transition the session to a new lifecycle state.
    ///
    /// After updating the state, this method also calls
    /// [`touch`](Self::touch) to record the state change as activity.
    pub fn set_state(&self, s: SessionState) {
        *self.state.lock() = s;
        self.touch();
    }

    /// Record activity on the session by updating the `last_active`
    /// timestamp to the current time.
    ///
    /// This is called automatically by [`set_state`](Self::set_state)
    /// and should also be called by the broker whenever a message is
    /// published or received on this session.
    pub fn touch(&self) {
        // `last_active` is an informational timestamp, not a
        // synchronization point. `Relaxed` is sufficient; the
        // caller observing `last_active` will also perform other
        // synchronized operations that establish the necessary
        // happens-before.
        self.last_active.store(now_ms() as u64, Ordering::Relaxed);
    }

    /// Read the current epoch value.
    ///
    /// The epoch starts at 1 and is incremented each time the session
    /// goes through a resume cycle.
    pub fn current_epoch(&self) -> u32 {
        self.epoch.load(Ordering::SeqCst)
    }

    /// Atomically increment the epoch and return the new value.
    ///
    /// This is called at the start of each resume attempt so that
    /// stale client connections can be detected by comparing epochs.
    pub fn bump_epoch(&self) -> u32 {
        self.epoch.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Check whether the session is still alive (i.e., not in the
    /// [`Closed`](SessionState::Closed) state).
    ///
    /// Returns `true` for all states except `Closed`.
    pub fn is_alive(&self) -> bool {
        !matches!(self.state(), SessionState::Closed)
    }

    /// Returns the duration since the session was last touched.
    ///
    /// Useful for implementing idle-timeout policies. If the clock
    /// has gone backwards (e.g., due to NTP adjustment), the duration
    /// is clamped to zero.
    pub fn idle(&self) -> Duration {
        let last = self.last_active.load(Ordering::SeqCst) as i64;
        Duration::from_millis((now_ms() - last).max(0) as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_lifecycle() {
        let s = Session::new(SessionId::new(), ClientId::new("c1"));
        assert_eq!(s.state(), SessionState::Open);
        s.set_state(SessionState::Ready);
        assert_eq!(s.state(), SessionState::Ready);
        assert_eq!(s.current_epoch(), 1);
        assert_eq!(s.bump_epoch(), 2);
        s.set_state(SessionState::Closed);
        assert!(!s.is_alive());
    }

    #[test]
    fn session_id_is_unique() {
        let a = SessionId::new();
        let b = SessionId::new();
        assert_ne!(a, b);
    }
}
