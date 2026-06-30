//! # Session Layer
//!
//! This module implements the session management subsystem as defined in
//! the protocol specification sections 5 and 13. A **session** represents
//! a logical connection between a client and the server. Unlike transport
//! connections (which may be short-lived and subject to reconnection), a
//! session persists across transport reconnects through the resume mechanism.
//!
//! ## Architecture Overview
//!
//! ```text
//! Session Layer
//! ├── auth          -- Pluggable authentication via the AuthProvider trait
//! ├── offset_tracker -- Per-topic offset tracking for resume decisions
//! ├── resume        -- Resume orchestration and epoch validation
//! └── session       -- Core Session, SessionId, ClientId, and lifecycle state
//! ```
//!
//! ## Key Concepts
//!
//! * **SessionId** -- A globally unique ULID that identifies a single session
//!   instance. Generated server-side when the client first connects.
//!
//! * **ClientId** -- A long-lived client identity (e.g., a username or device
//!   ID) that survives session expiration. Multiple sessions over time may
//!   share the same `ClientId`.
//!
//! * **Epoch** -- A monotonically increasing counter attached to a session.
//!   The epoch is bumped on each resume attempt so that stale client
//!   connections can be detected and rejected.
//!
//! * **Resume** -- When a transport connection drops and the client
//!   reconnects, it presents its `SessionId` and last-known offsets.
//!   The server decides whether to resume the session or reject the
//!   attempt.
//!
//! ## Session Lifecycle
//!
//! A session transitions through the following states:
//!
//! `Open` -> `Hello` -> `Authenticated` -> `Resuming` -> `Ready` -> `Active` -> `Draining` -> `Closed`
//!
//! See [`SessionState`] for details on each state.

pub mod auth;
pub mod offset_tracker;
pub mod resume;
#[allow(clippy::module_inception)]
pub mod session;
pub mod store;

pub use auth::{AllowAllAuth, AuthContext, AuthHints, AuthProvider, TokenAuth};
pub use offset_tracker::{OffsetTracker, ResumeDecision};
pub use resume::ResumeManager;
pub use session::{ClientId, Session, SessionId, SessionState};
pub use store::SessionStore;
