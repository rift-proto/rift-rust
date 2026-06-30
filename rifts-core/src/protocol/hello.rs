//! # Hello / Welcome / Ready Handshake -- Spec §5.2 – §5.5
//!
//! This module defines the data structures for the three-phase connection
//! handshake in the Rift/1 protocol:
//!
//! 1. **Hello** (§5.2) -- sent by the client to initiate the connection,
//!    advertising its protocol version, supported codecs, authentication
//!    modes, and optional resume information.
//! 2. **Welcome** (§5.3) -- sent by the server after successful
//!    authentication, confirming the negotiated codec, session ID, and
//!    resume window.
//! 3. **Ready** (§5.5) -- sent by the server after the session is fully
//!    established, carrying runtime parameters such as heartbeat policy,
//!    payload limits, and topic quotas.
//!
//! Between Welcome and Ready the server may optionally perform a
//! **Resume** exchange (§5.4), the result of which is reported as a
//! [`ResumeResult`].
//!
//! ## Connection Lifecycle
//!
//! ```text
//! Client                        Server
//!   │                               │
//!   │──── Hello ──────────────────>│  (auth + capabilities)
//!   │                               │
//!   │<─── Welcome ─────────────────│  (session + codec)
//!   │                               │
//!   │   [optional Resume exchange]  │
//!   │                               │
//!   │<─── Ready ───────────────────│  (limits + heartbeat)
//!   │                               │
//!   │   [data frames flow]          │
//! ```

use std::collections::BTreeMap;

use crate::frame::EncodingFormat;

/// Client Hello frame -- spec §5.2.
///
/// The `Hello` frame is the very first frame sent by the client after
/// opening the transport connection.  It advertises the client's
/// capabilities so the server can choose appropriate negotiation
/// parameters.
///
/// ## Construction
///
/// Use [`Hello::new`] to create a Hello with the mandatory fields
/// populated (protocol name, encoded version, and codec list).  All
/// other fields default to `None` or empty.
///
/// ## Fields
///
/// Most fields are optional.  The server will use its own defaults when
/// a field is absent.  See individual field documentation for details.
#[derive(Debug, Clone, Default)]
pub struct Hello {
    /// Protocol identifier string.  Must be `"rift"` for all Rift/1
    /// connections.  The server rejects connections with an unrecognized
    /// protocol name.
    pub protocol: String, // "rift"

    /// Encoded protocol version (`major << 8 | minor`).
    ///
    /// The server uses this value together with [`SUPPORTED_MAJOR`]
    /// (from the [`version`](super::version) module) to determine
    /// whether it can serve this client.
    pub version: u16, // major << 8 | minor

    /// Optional opaque client identifier.
    ///
    /// When present, the server may use it for logging, metrics, and
    /// session affinity.  The server does not validate the format.
    pub client_id: Option<String>,

    /// Optional existing session identifier for session resumption.
    ///
    /// If the client wishes to resume a previous session, it includes
    /// the session ID received in the prior `Welcome` frame.  The
    /// server will attempt to restore the session state and reply with
    /// a [`ResumeResult`].
    pub session_id: Option<String>,

    /// Optional epoch for session resumption.
    ///
    /// The epoch is a monotonically increasing counter that the server
    /// uses to distinguish successive incarnations of the same session.
    pub epoch: Option<u32>,

    /// List of content codecs the client supports, in order of
    /// preference.
    ///
    /// The server selects the first mutually supported codec and reports
    /// the choice in [`Welcome::negotiated_codec`].
    pub codecs: Vec<EncodingFormat>,

    /// List of compression algorithms the client supports, in order of
    /// preference.
    ///
    /// Compression negotiation is optional; when empty the connection
    /// uses no compression.
    pub compression: Vec<String>,

    /// List of authentication modes the client can use.
    ///
    /// The server selects a mode from this list (or rejects the
    /// connection if none are acceptable).  See [`AuthMode`] for the
    /// supported modes.
    pub auth_modes: Vec<AuthMode>,

    /// Per-topic last-seen offsets for session resumption.
    ///
    /// Each entry maps a topic name to the offset of the last message
    /// the client successfully processed.  The server uses these
    /// offsets to determine the replay starting point.
    pub last_offsets: BTreeMap<String, i64>,

    /// Optional client wall-clock time in milliseconds since the Unix
    /// epoch.
    ///
    /// The server can use this to estimate clock skew and include a
    /// correction in the `Welcome` response.
    pub client_clock: Option<i64>,

    /// Optional SDK identification metadata.
    ///
    /// When present, the server may use the SDK name and version for
    /// compatibility checks, deprecation warnings, and telemetry.
    pub sdk: Option<SdkInfo>,

    /// Optional list of protocol feature flags the client supports.
    ///
    /// Feature flags allow incremental rollout of optional protocol
    /// extensions without a major version bump.
    pub features: Vec<String>,
}

impl Hello {
    /// Creates a new [`Hello`] frame with the mandatory fields populated.
    ///
    /// The `protocol` field is set to `"rift"`, the `version` field is
    /// set to the current encoded protocol version, and `codecs` is set
    /// to the provided list.  All other fields default to `None` or
    /// empty.
    ///
    /// ```rust
    /// use rifts_core::protocol::hello::Hello;
    /// use rifts_core::frame::EncodingFormat;
    ///
    /// let hello = Hello::new(vec![EncodingFormat::Json, EncodingFormat::Cbor]);
    /// assert_eq!(hello.protocol, "rift");
    /// assert!(!hello.codecs.is_empty());
    /// ```
    pub fn new(codecs: Vec<EncodingFormat>) -> Self {
        Self {
            protocol: crate::protocol::version::PROTOCOL_NAME.to_string(),
            version: crate::protocol::version::encoded_version(),
            codecs,
            ..Default::default()
        }
    }
}

/// Authentication mode offered by the client or accepted by the server.
///
/// During the Hello exchange the client lists the authentication modes
/// it supports.  The server picks one (or rejects the connection) and
/// may include the chosen mode in the Welcome response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AuthMode {
    /// Bearer token authentication (e.g. JWT or opaque token).
    Bearer,

    /// Cookie-based authentication.
    Cookie,

    /// Mutual TLS (mTLS) authentication using client certificates.
    Mtls,

    /// Signed challenge-response authentication.
    ///
    /// The server sends a random challenge; the client signs it with a
    /// private key and returns the signature.
    SignedChallenge,

    /// Anonymous (unauthenticated) access.
    ///
    /// The server may restrict the topics and operations available to
    /// anonymous clients.
    Anonymous,
}

impl AuthMode {
    /// Returns the stable, lowercase, snake_case string name of this
    /// authentication mode.
    ///
    /// This is the string transmitted on the wire in the `auth_modes`
    /// list of the Hello frame.
    ///
    /// ```rust
    /// use rifts_core::protocol::hello::AuthMode;
    ///
    /// assert_eq!(AuthMode::SignedChallenge.name(), "signed_challenge");
    /// assert_eq!(AuthMode::Anonymous.name(), "anonymous");
    /// ```
    pub fn name(self) -> &'static str {
        match self {
            AuthMode::Bearer => "bearer",
            AuthMode::Cookie => "cookie",
            AuthMode::Mtls => "mtls",
            AuthMode::SignedChallenge => "signed_challenge",
            AuthMode::Anonymous => "anonymous",
        }
    }
}

/// SDK identification metadata -- spec §5.2 (`sdk` field).
///
/// The client may include this information in its Hello frame so the
/// server can track SDK versions across its fleet, issue deprecation
/// warnings, and collect usage telemetry.
#[derive(Debug, Clone, Default)]
pub struct SdkInfo {
    /// Name of the SDK (e.g. `"riftrust"`).
    pub name: String,

    /// Semantic version string of the SDK (e.g. `"0.3.1"`).
    pub version: String,
}

/// Server Welcome frame -- spec §5.3.
///
/// The `Welcome` frame is sent by the server immediately after
/// successful authentication.  It confirms the negotiated parameters
/// and assigns the session.
///
/// ## Resume Window
///
/// The `resume_window_ms` field tells the client how long the server
/// will retain session state after disconnection, enabling the client
/// to reconnect and resume without losing messages.
#[derive(Debug, Clone)]
pub struct Welcome {
    /// Unique session identifier assigned by the server.
    ///
    /// The client should include this value in subsequent Hello frames
    /// when attempting to resume the session.
    pub session_id: String,

    /// Monotonically increasing epoch counter for this session.
    ///
    /// Each new session incarnation increments the epoch.  The client
    /// must echo the epoch when resuming.
    pub epoch: u32,

    /// Content codec selected by the server from the client's codec
    /// list.
    ///
    /// All subsequent data frames on this connection MUST use this
    /// codec.
    pub negotiated_codec: EncodingFormat,

    /// Compression algorithm selected by the server, or `None` if no
    /// compression is to be used.
    pub negotiated_compression: Option<String>,

    /// Server wall-clock time in milliseconds since the Unix epoch at
    /// the moment the Welcome was generated.
    ///
    /// The client can use this together with `client_clock` to estimate
    /// round-trip time and clock skew.
    pub server_time: i64,

    /// Duration in milliseconds for which the server will retain
    /// session state after disconnection.
    ///
    /// A value of zero means the server does not support session
    /// resumption.
    pub resume_window_ms: u32,

    /// List of protocol feature flags supported by the server.
    pub features: Vec<String>,
}

/// Result of a session resume attempt -- spec §5.4.
///
/// When the client includes a `session_id` in its Hello, the server
/// attempts to restore the previous session.  The outcome is reported
/// as one of these variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeResult {
    /// The session was fully resumed; all state was restored.
    Resumed,

    /// The session was partially resumed; some messages may have been
    /// lost and the client should reconcile.
    Partial,

    /// The server rejected the resume request (e.g. due to internal
    /// constraints).
    Rejected,

    /// The session state has expired and is no longer available for
    /// resumption.
    Expired,

    /// Another connection is already bound to this session, preventing
    /// resumption.
    Conflict,
}

/// Server Ready frame -- spec §5.5.
///
/// The `Ready` frame is the final handshake frame.  It is sent by the
/// server after the session is fully established (and any resume
/// exchange is complete).  It carries the runtime parameters the client
/// needs to operate correctly on this connection.
///
/// After receiving Ready the client may begin publishing and
/// subscribing to topics.
#[derive(Debug, Clone)]
pub struct Ready {
    /// Unique session identifier assigned by the server.
    pub session_id: String,

    /// Monotonically increasing epoch counter for this session.
    pub epoch: u32,

    /// Minimum interval in milliseconds between consecutive Ping
    /// frames the client must send.
    pub ping_interval_ms: u32,

    /// Maximum time in milliseconds the client should wait for a Pong
    /// reply before considering the Ping missed.
    pub pong_timeout_ms: u32,

    /// Number of consecutive missed Pongs that triggers connection
    /// teardown on the client side.
    pub max_missed_pongs: u32,

    /// Idle timeout in milliseconds; the connection will be closed if
    /// no frames are received within this window.
    pub idle_timeout_ms: u32,

    /// Maximum random jitter in milliseconds to add to the effective
    /// ping interval, used to prevent heartbeat synchronization across
    /// many clients.
    pub jitter_ms: u32,

    /// Maximum payload size in bytes for a single frame on this
    /// connection.
    pub max_payload_bytes: u32,

    /// Maximum number of topics this connection may subscribe to
    /// simultaneously.
    pub max_topics_per_connection: u32,

    /// Maximum size in bytes of the client's outbound send queue before
    /// the server will apply back-pressure or close the connection.
    pub max_send_queue_bytes: u32,

    /// Server wall-clock time in milliseconds since the Unix epoch at
    /// the moment the Ready frame was generated.
    pub server_time: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hello_minimal() {
        let h = Hello::new(vec![EncodingFormat::Json, EncodingFormat::Cbor]);
        assert_eq!(h.protocol, "rift");
        assert!(!h.codecs.is_empty());
    }
}
