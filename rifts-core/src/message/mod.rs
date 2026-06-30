//! # Message Types -- Semantic Layer
//!
//! This module implements the message type system defined in spec section 8.
//!
//! ## Message Classes
//!
//! | Class | Purpose | Default Delivery Mode |
//! |-------|---------|----------------------|
//! | `Event` | Business events | AtLeastOnce |
//! | `Command` | Request-style commands (RPC semantics) | AtLeastOnce |
//! | `Reply` | Command responses | AtLeastOnce |
//! | `State` | State messages (only the latest value per key is valid) | LatestOnly |
//! | `Datagram` | High-frequency, loss-tolerant datagrams | BestEffort |
//! | `Stream` | Continuously ordered data streams (AI tokens, file chunks, etc.) | DurableOrdered |
//! | `Snapshot` | Topic state snapshots | AtLeastOnce |
//! | `System` | System control messages | AtLeastOnce |
//!
//! ## Submodules
//!
//! - [`command`]: Command (request) and Reply (response)
//! - [`datagram`]: High-frequency datagrams
//! - [`event`]: Business events
//! - [`snapshot`]: Topic state snapshots
//! - [`state`]: State messages and Presence
//! - [`stream`]: Continuously ordered stream segments

pub mod command;
pub mod datagram;
pub mod event;
pub mod snapshot;
pub mod state;
pub mod stream;

use serde::{Deserialize, Serialize};

/// Message class (spec section 8.1).
///
/// Every [`Message`] has exactly one `MessageClass`, which determines
/// the message's delivery semantics, retention policy, and client-side handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageClass {
    /// Business event -- a regular message in the publish/subscribe model.
    Event,
    /// Request-style command -- carries a `correlation_id` and expects a Reply from the peer.
    Command,
    /// Command response -- the Reply paired with a Command.
    Reply,
    /// State message -- only the latest value per `state_key` is retained.
    State,
    /// High-frequency datagram -- loss-tolerant, low-latency (e.g. mouse movement, input state).
    Datagram,
    /// Continuously ordered stream segment -- used for AI token streams, file transfers, audio/video frames, etc.
    Stream,
    /// Topic state snapshot -- used for fast initialization after reconnection.
    Snapshot,
    /// System control message -- used internally by the protocol.
    System,
}

impl MessageClass {
    /// Returns the string representation of the message class (used for serialization and logging).
    pub fn as_str(self) -> &'static str {
        match self {
            MessageClass::Event => "event",
            MessageClass::Command => "command",
            MessageClass::Reply => "reply",
            MessageClass::State => "state",
            MessageClass::Datagram => "datagram",
            MessageClass::Stream => "stream",
            MessageClass::Snapshot => "snapshot",
            MessageClass::System => "system",
        }
    }
}

/// Delivery semantics (spec section 8.2).
///
/// Determines how a message behaves on network anomalies: whether it is retried,
/// persisted, or only the latest value is retained.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryMode {
    /// At-most-once delivery -- no retries, no acknowledgment, may be lost.
    AtMostOnce,
    /// At-least-once delivery -- may be duplicated; the receiver must deduplicate.
    AtLeastOnce,
    /// Exactly-once effect -- guarantees equivalent-to-exactly-once semantics through idempotency.
    ExactlyOnceEffect,
    /// Latest-only -- old messages are overwritten by newer ones (e.g. state messages).
    LatestOnly,
    /// Best-effort delivery -- no delivery guarantee, suitable for high-frequency, low-value data.
    BestEffort,
    /// Durable ordered -- written to a persistent log and delivered strictly in offset order.
    DurableOrdered,
}

/// Specifies what a subscriber wants to receive from a topic.
///
/// Passed to broker subscribe methods to indicate the subscriber's
/// delivery preference. The broker implementation uses this to decide
/// replay and snapshot behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SubscribeIntent {
    /// Only receive new messages published after the subscription is
    /// established. Historical messages are not replayed.
    Live,
    /// Replay historical messages starting from the specified offset,
    /// then continue receiving live messages.
    Replay {
        /// The offset from which to begin replaying. Messages with
        /// offsets greater than or equal to this value will be
        /// delivered, followed by any new live messages.
        from: i64,
    },
    /// Capture a snapshot of the topic's current state, deliver it to
    /// the subscriber, then switch to live delivery.
    SnapshotThenLive,
    /// Receive only the most recent state of the topic (latest message
    /// or snapshot). Does not subscribe to ongoing live delivery.
    Latest,
    /// Receive only system-level notices (e.g. topic metadata changes,
    /// administrative messages). Regular data messages are not
    /// delivered.
    Passive,
    /// A temporary subscription that is automatically cleaned up when
    /// the connection disconnects. Useful for one-off queries or
    /// fire-and-forget operations.
    Ephemeral,
}

/// Subscribe acknowledgment result (spec section 10.2).
///
/// Status code returned by the server after processing a subscribe request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscribeResult {
    /// Subscription accepted.
    Accepted,
    /// Subscription denied (insufficient permissions or policy restrictions).
    Denied,
    /// Topic does not exist and auto-creation is not allowed.
    NotFound,
    /// Topic is closed.
    Gone,
    /// Historical message replay required (server returns a starting offset).
    ReplayRequired,
    /// Snapshot required (snapshot-mode subscription).
    SnapshotRequired,
    /// Subscription request was rate-limited.
    RateLimited,
    /// Server is overloaded, temporarily rejecting.
    Overloaded,
    /// Filter expression has a syntax error.
    InvalidFilter,
}

/// Typed messages exchanged with the Broker.
///
/// Each variant wraps the corresponding struct from its submodule; the `System` variant
/// is used for internal system communication.
///
/// # Pattern matching
///
/// ```rust,no_run
/// use rifts_core::message::{Message, MessageClass};
///
/// fn classify(msg: &Message) {
///     match msg {
///         Message::Event(e) => { /* handle business event */ }
///         Message::Command(c) => { /* handle command request */ }
///         Message::State(s) => { /* handle state update */ }
///         _ => {}
///     }
/// }
/// ```
#[derive(Debug, Clone)]
pub enum Message {
    /// Business event.
    Event(event::Event),
    /// Request-style command.
    Command(command::Command),
    /// Command response.
    Reply(command::Reply),
    /// State message.
    State(state::State),
    /// High-frequency datagram.
    Datagram(datagram::Datagram),
    /// Continuous stream segment.
    Stream(stream::StreamSegment),
    /// Topic state snapshot.
    Snapshot(snapshot::Snapshot),
    /// Generic system message, routed by event name.
    System {
        /// System event name (e.g. "heartbeat", "degradation").
        event: String,
        /// System message payload.
        payload: serde_json::Value,
    },
}

impl Message {
    /// Returns the class this message belongs to.
    ///
    /// Useful for generic dispatch when the specific variant is not important.
    pub fn class(&self) -> MessageClass {
        match self {
            Message::Event(_) => MessageClass::Event,
            Message::Command(_) => MessageClass::Command,
            Message::Reply(_) => MessageClass::Reply,
            Message::State(_) => MessageClass::State,
            Message::Datagram(_) => MessageClass::Datagram,
            Message::Stream(_) => MessageClass::Stream,
            Message::Snapshot(_) => MessageClass::Snapshot,
            Message::System { .. } => MessageClass::System,
        }
    }
}
