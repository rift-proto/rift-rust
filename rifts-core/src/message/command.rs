//! # Command / Reply -- Request-Style Commands and Responses (spec section 15)
//!
//! Commands implement RPC semantics: a client sends a Command (carrying a `correlation_id`),
//! the server processes it and returns a Reply (carrying the same `correlation_id`).
//!
//! ## Flow
//!
//! ```text
//! Client                         Server
//!   |                               |
//!   |-- Command { corr_id=42 } --->|
//!   |                               |  (process command)
//!   |<-- Reply { corr_id=42 } -----|
//!   |                               |
//! ```
//!
//! Supports idempotency (via `idempotency_key`) and timeout control (via `timeout_ms`).

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::{MessageReject, Result, RiftError};
use crate::now_ms;

/// A request-style command -- an RPC request sent to the server or a peer.
///
/// Each Command must carry a unique `correlation_id` used to match it with its Reply.
/// Supports an optional idempotency key (`idempotency_key`) for exactly-once-effect semantics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Command {
    /// Command name, identifies the operation to execute (e.g. `"room.create"`, `"user.update"`).
    ///
    /// The naming convention is typically `{domain}.{action}`.
    pub command: String,

    /// Correlation ID that pairs this command with its Reply.
    ///
    /// Must be unique within the same connection; the server echoes it back in the Reply as-is.
    pub correlation_id: String,

    /// Request timeout in milliseconds.
    ///
    /// If no Reply is received within this duration, the client should treat it as a timeout
    /// and may retry.
    pub timeout_ms: u32,

    /// Idempotency key (optional).
    ///
    /// When set, duplicate requests with the same `idempotency_key` return the cached response,
    /// preventing re-execution of side effects. Typically a UUID or ULID.
    pub idempotency_key: Option<String>,

    /// Request payload -- command arguments in JSON format.
    pub payload: serde_json::Value,

    /// Schema identifier, formatted as `{domain}.{name}@{major}.{minor}`.
    ///
    /// Used for version management and payload validation.
    pub schema: String,
}

impl Command {
    /// Creates a new Command instance.
    ///
    /// `idempotency_key` defaults to `None`; set it directly on the struct if needed.
    pub fn new(
        command: impl Into<String>,
        correlation_id: impl Into<String>,
        timeout_ms: u32,
        schema: impl Into<String>,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            command: command.into(),
            correlation_id: correlation_id.into(),
            timeout_ms,
            idempotency_key: None,
            payload,
            schema: schema.into(),
        }
    }
}

/// A command response -- the reply to a Command.
///
/// Carries the `correlation_id` of the original command so the client can match the request.
/// The response can be success (`Ok`), business error (`Error`), timeout (`Timeout`), or rejected (`Rejected`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reply {
    /// Correlation ID of the original command.
    pub correlation_id: String,

    /// Response status.
    pub status: ReplyStatus,

    /// Response payload (populated only on success).
    pub payload: Option<serde_json::Value>,

    /// Structured error information (populated only for `Error`/`Rejected` statuses).
    pub error: Option<ReplyError>,

    /// Server timestamp (millisecond Unix timestamp), used for client clock calibration.
    pub server_time: i64,
}

impl Reply {
    /// Constructs a successful response.
    pub fn ok(correlation_id: impl Into<String>, payload: serde_json::Value) -> Self {
        Self {
            correlation_id: correlation_id.into(),
            status: ReplyStatus::Ok,
            payload: Some(payload),
            error: None,
            server_time: now_ms(),
        }
    }

    /// Constructs an error response.
    ///
    /// `code` is a machine-readable error code (e.g. `"RIFT_AUTH_INVALID"`),
    /// `message` is a human-readable description.
    pub fn error(
        correlation_id: impl Into<String>,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            correlation_id: correlation_id.into(),
            status: ReplyStatus::Error,
            payload: None,
            error: Some(ReplyError {
                code: code.into(),
                message: message.into(),
            }),
            server_time: now_ms(),
        }
    }
}

/// Reply status code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplyStatus {
    /// Command executed successfully.
    Ok,
    /// Command execution failed (business error).
    Error,
    /// Command execution timed out.
    Timeout,
    /// Command was rejected (insufficient permissions or policy restrictions).
    Rejected,
}

/// Structured error information within a Reply.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplyError {
    /// Machine-readable error code (e.g. `"RIFT_AUTH_INVALID"`, `"RIFT_NOT_FOUND"`).
    pub code: String,
    /// Human-readable error description.
    pub message: String,
}

/// Serializes a Command to JSON bytes.
///
/// Validates that `correlation_id` is non-empty; returns `MessageReject::Rejected` otherwise.
pub fn encode_command(c: &Command) -> Result<Bytes> {
    if c.correlation_id.is_empty() {
        return Err(RiftError::Message(MessageReject::Rejected(
            "command requires correlation_id".into(),
        )));
    }
    Ok(Bytes::from(serde_json::to_vec(c)?))
}

/// Deserializes a Command from JSON bytes.
///
/// Validates that `correlation_id` is non-empty after deserialization.
pub fn decode_command(bytes: &[u8]) -> Result<Command> {
    let c: Command = serde_json::from_slice(bytes)?;
    if c.correlation_id.is_empty() {
        return Err(RiftError::Message(MessageReject::Rejected(
            "command requires correlation_id".into(),
        )));
    }
    Ok(c)
}

/// Serializes a Reply to JSON bytes.
pub fn encode_reply(r: &Reply) -> Result<Bytes> {
    Ok(Bytes::from(serde_json::to_vec(r)?))
}

/// Deserializes a Reply from JSON bytes.
pub fn decode_reply(bytes: &[u8]) -> Result<Reply> {
    Ok(serde_json::from_slice(bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_round_trip() {
        let c = Command::new(
            "room.create",
            "corr-1",
            5000,
            "room.create@1.0",
            serde_json::json!({"name": "general"}),
        );
        let bytes = encode_command(&c).unwrap();
        let back = decode_command(&bytes).unwrap();
        assert_eq!(back.correlation_id, "corr-1");
    }

    #[test]
    fn command_requires_correlation_id() {
        let c = Command {
            command: "x".into(),
            correlation_id: "".into(),
            timeout_ms: 100,
            idempotency_key: None,
            payload: serde_json::Value::Null,
            schema: "x@1.0".into(),
        };
        assert!(encode_command(&c).is_err());
    }

    #[test]
    fn reply_ok_and_error() {
        let ok = Reply::ok("c1", serde_json::json!({"id": 7}));
        assert_eq!(ok.status, ReplyStatus::Ok);
        let err = Reply::error("c1", "RIFT_AUTH_INVALID", "bad token");
        assert_eq!(err.status, ReplyStatus::Error);
    }
}
