//! Redis-backed message deduplication using per-key TTL.
//! All methods are async.

use std::time::Duration;

use async_trait::async_trait;

use crate::connection::RedisPool;
use rifts_storage::DedupeStore;

/// Redis-backed deduplication store using per-key TTL.
///
/// Each message key is stored as a Redis set member with an expiry, providing
/// distributed deduplication across multiple server instances sharing the same
/// Redis cluster.
#[derive(Clone)]
pub struct RedisDedupeStore {
    pool: RedisPool,
}

impl RedisDedupeStore {
    /// Create a new [`RedisDedupeStore`] backed by the given Redis connection pool.
    pub fn new(pool: RedisPool) -> Self {
        Self { pool }
    }

    fn member_key(&self, topic: &str, message_id: &str) -> String {
        format!("{}:dedupe:{topic}:{message_id}", self.pool.prefix())
    }
}

#[async_trait]
impl DedupeStore for RedisDedupeStore {
    async fn check_and_record(&self, topic: &str, key: &str, window: Duration) -> bool {
        let member_key = self.member_key(topic, key);
        let window_secs = window.as_secs().max(1) as usize;
        let mut conn = self.pool.conn().clone();
        let result: Option<String> = redis::cmd("SET")
            .arg(&member_key)
            .arg("1")
            .arg("NX")
            .arg("EX")
            .arg(window_secs)
            .query_async(&mut conn)
            .await
            .unwrap_or(None);
        result.is_some()
    }

    async fn sweep(&self) -> usize {
        0
    }
}
