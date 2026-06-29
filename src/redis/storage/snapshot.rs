//! Redis-backed snapshot store. All methods are async.

use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;

use crate::now_ms;
use crate::redis::connection::RedisPool;
use crate::storage::{SnapshotStore, StoredSnapshot};
use crate::topic::TopicStore;

/// Redis-backed snapshot store.
///
/// Each topic's latest snapshot is stored as a Redis string value, serialized
/// as CBOR for compact storage and fast deserialization.
#[derive(Clone)]
pub struct RedisSnapshotStore {
    pool: RedisPool,
}

impl RedisSnapshotStore {
    /// Create a new [`RedisSnapshotStore`] backed by the given Redis connection pool.
    pub fn new(pool: RedisPool) -> Self {
        Self { pool }
    }

    fn snapshot_key(&self, topic: &str) -> String {
        self.pool.topic_key("snapshot", topic)
    }
}

#[async_trait]
impl SnapshotStore for RedisSnapshotStore {
    async fn capture(
        &self,
        topic: &str,
        store: &TopicStore,
        ttl: Option<Duration>,
    ) -> Option<StoredSnapshot> {
        let entry = store.get(topic)?;
        let latest = entry.log.read().last().cloned()?;

        let snapshot_id = format!("snap-{}", latest.offset);
        let expires_at = ttl.map(|d| now_ms() + d.as_millis() as i64);

        let snap = StoredSnapshot {
            snapshot_id,
            topic: topic.to_string(),
            base_offset: latest.offset,
            payload: latest.payload.clone(),
            created_at: now_ms(),
            expires_at,
        };

        let key = self.snapshot_key(topic);
        let payload_hex = hex_encode(&snap.payload);
        let expires_str = snap.expires_at.map(|e| e.to_string()).unwrap_or_default();

        let mut conn = self.pool.conn().clone();
        let _: Result<(), _> = redis::cmd("HSET")
            .arg(&key)
            .arg("snapshot_id")
            .arg(&snap.snapshot_id)
            .arg("topic")
            .arg(&snap.topic)
            .arg("base_offset")
            .arg(snap.base_offset)
            .arg("payload")
            .arg(&payload_hex)
            .arg("created_at")
            .arg(snap.created_at)
            .arg("expires_at")
            .arg(&expires_str)
            .query_async(&mut conn)
            .await;

        if let Some(ttl) = ttl {
            let _: Result<(), _> = redis::cmd("EXPIRE")
                .arg(&key)
                .arg(ttl.as_secs().max(1) as usize)
                .query_async(&mut conn)
                .await;
        }

        Some(snap)
    }

    async fn get(&self, topic: &str) -> Option<StoredSnapshot> {
        let key = self.snapshot_key(topic);
        let mut conn = self.pool.conn().clone();
        let fields: Vec<String> = redis::cmd("HMGET")
            .arg(&key)
            .arg("snapshot_id")
            .arg("topic")
            .arg("base_offset")
            .arg("payload")
            .arg("created_at")
            .arg("expires_at")
            .query_async(&mut conn)
            .await
            .unwrap_or_default();

        if fields.len() < 6 || fields[0].is_empty() {
            return None;
        }

        let base_offset: i64 = fields[2].parse().unwrap_or(0);
        let payload = hex_decode(&fields[3]);
        let created_at: i64 = fields[4].parse().unwrap_or(0);
        let expires_at: Option<i64> = if fields[5].is_empty() {
            None
        } else {
            fields[5].parse().ok()
        };

        if let Some(exp) = expires_at
            && now_ms() > exp
        {
            return None;
        }

        Some(StoredSnapshot {
            snapshot_id: fields[0].clone(),
            topic: fields[1].clone(),
            base_offset,
            payload,
            created_at,
            expires_at,
        })
    }

    async fn remove(&self, topic: &str) {
        let key = self.snapshot_key(topic);
        let mut conn = self.pool.conn().clone();
        let _: Result<(), _> = redis::cmd("DEL").arg(&key).query_async(&mut conn).await;
    }

    async fn list(&self) -> Vec<StoredSnapshot> {
        Vec::new()
    }
}

fn hex_encode(data: &Bytes) -> String {
    use std::fmt::Write;
    let mut s = String::new();
    for b in data.as_ref() {
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn hex_decode(hex: &str) -> Bytes {
    let mut bytes = Vec::new();
    for i in (0..hex.len()).step_by(2) {
        if i + 2 <= hex.len()
            && let Ok(b) = u8::from_str_radix(&hex[i..i + 2], 16)
        {
            bytes.push(b);
        }
    }
    Bytes::from(bytes)
}
