//! Acknowledgement system — spec §12.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use uuid::Uuid;

/// Status of an acknowledgement (spec §12.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckStatus {
    Received,
    Accepted,
    Persisted,
    Delivered,
    Processed,
    Rejected,
    Expired,
    Duplicate,
    Failed,
}

impl AckStatus {
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

/// Acknowledgement strategy (spec §12.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckPolicy {
    None,
    Server,
    Persisted,
    Quorum,
    Subscriber,
    Application,
}

/// Acknowledgement payload sent to the peer.
#[derive(Debug, Clone)]
pub struct Ack {
    pub ack_id: String,
    pub message_id: String,
    pub status: AckStatus,
    pub offset: Option<i64>,
    pub reason: Option<String>,
    pub error_code: Option<String>,
    pub retry_after_ms: Option<u32>,
    pub server_time: i64,
}

impl Ack {
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

    pub fn with_offset(mut self, offset: i64) -> Self {
        self.offset = Some(offset);
        self
    }

    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }
}

/// Tracks outstanding (sent-but-not-acked) frames per session.
pub struct AckManager {
    /// session_id → message_id → deadline
    outstanding: Mutex<HashMap<String, HashMap<String, i64>>>,
}

impl Default for AckManager {
    fn default() -> Self {
        Self::new()
    }
}

impl AckManager {
    pub fn new() -> Self {
        Self {
            outstanding: Mutex::new(HashMap::new()),
        }
    }

    /// Mark a frame as awaiting acknowledgement.
    pub fn track(&self, session_id: &str, message_id: &str, timeout: Duration) {
        let deadline = now_ms() + timeout.as_millis() as i64;
        self.outstanding
            .lock()
            .entry(session_id.to_string())
            .or_default()
            .insert(message_id.to_string(), deadline);
    }

    /// Mark a message as acknowledged; returns `true` if the message
    /// was being tracked.
    pub fn complete(&self, session_id: &str, message_id: &str) -> bool {
        self.outstanding
            .lock()
            .get_mut(session_id)
            .and_then(|m| m.remove(message_id))
            .is_some()
    }

    /// Find timed-out message ids for a session; returns the expired
    /// ids and removes them from the tracking set.
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

    /// Drop everything for a session.
    pub fn forget(&self, session_id: &str) {
        self.outstanding.lock().remove(session_id);
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Shared ack manager.
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
