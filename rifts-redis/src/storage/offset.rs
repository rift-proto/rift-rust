//! Redis-backed monotonic offset allocation.
//!
//! Uses a Redis Hash with `HINCRBY` for atomic, distributed offset
//! allocation. Each topic maps to a field in a shared Redis hash.
//! All methods are async to avoid blocking the Tokio runtime.

use async_trait::async_trait;

use crate::connection::RedisPool;
use rifts_storage::OffsetStore;

/// Redis-backed monotonic offset store.
///
/// Uses `HINCRBY` on a shared Redis hash for atomic, distributed offset
/// allocation across multiple server instances.
#[derive(Clone)]
pub struct RedisOffsetStore {
    pool: RedisPool,
}

impl RedisOffsetStore {
    /// Create a new [`RedisOffsetStore`] backed by the given Redis connection pool.
    pub fn new(pool: RedisPool) -> Self {
        Self { pool }
    }

    fn hash_key(&self) -> String {
        self.pool.key("offsets")
    }
}

#[async_trait]
impl OffsetStore for RedisOffsetStore {
    async fn alloc(&self, topic: &str) -> i64 {
        let key = self.hash_key();
        let mut conn = self.pool.conn().clone();
        redis::cmd("HINCRBY")
            .arg(&key)
            .arg(topic)
            .arg(1)
            .query_async(&mut conn)
            .await
            .unwrap_or(1)
    }

    async fn head(&self, topic: &str) -> i64 {
        let key = self.hash_key();
        let mut conn = self.pool.conn().clone();
        redis::cmd("HGET")
            .arg(&key)
            .arg(topic)
            .query_async(&mut conn)
            .await
            .unwrap_or(None)
            .unwrap_or(0)
    }

    async fn remove(&self, topic: &str) {
        let key = self.hash_key();
        let mut conn = self.pool.conn().clone();
        let _: Result<(), _> = redis::cmd("HDEL")
            .arg(&key)
            .arg(topic)
            .query_async(&mut conn)
            .await;
    }
}
