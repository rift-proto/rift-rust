//! Redis-backed message log store using Sorted Sets.
//! All methods are async.

use async_trait::async_trait;

use crate::redis::connection::RedisPool;
use crate::storage::LogStore;
use crate::topic::retention::RetentionPolicy;
use crate::topic::store::LogEntry;

/// Redis-backed message log store using Sorted Sets.
///
/// Each topic's log is stored as a Redis Sorted Set keyed by offset, enabling
/// efficient range queries for replay scenarios.
#[derive(Clone)]
pub struct RedisLogStore {
    pool: RedisPool,
}

impl RedisLogStore {
    /// Create a new [`RedisLogStore`] backed by the given Redis connection pool.
    pub fn new(pool: RedisPool) -> Self {
        Self { pool }
    }

    fn log_key(&self, topic: &str) -> String {
        self.pool.topic_key("log", topic)
    }

    fn encode_entry(entry: &LogEntry) -> Vec<u8> {
        let mut buf = Vec::new();
        ciborium::into_writer(entry, &mut buf).unwrap_or_default();
        buf
    }

    fn decode_entry(data: &[u8]) -> Option<LogEntry> {
        ciborium::from_reader(data).ok()
    }
}

#[async_trait]
impl LogStore for RedisLogStore {
    async fn append(&self, topic: &str, entry: LogEntry, retention: RetentionPolicy) {
        let key = self.log_key(topic);
        let member = Self::encode_entry(&entry);
        let offset = entry.offset;
        let mut conn = self.pool.conn().clone();

        let _: Result<(), _> = redis::cmd("ZADD")
            .arg(&key)
            .arg(offset)
            .arg(&member)
            .query_async(&mut conn)
            .await;

        match retention {
            RetentionPolicy::None => {
                let _: Result<(), _> = redis::cmd("DEL").arg(&key).query_async(&mut conn).await;
            }
            RetentionPolicy::Count(n) if n > 0 => {
                let _: Result<(), _> = redis::cmd("ZREMRANGEBYRANK")
                    .arg(&key)
                    .arg(0)
                    .arg(-((n as i64) + 1))
                    .query_async(&mut conn)
                    .await;
            }
            RetentionPolicy::Latest => {
                let _: Result<(), _> = redis::cmd("ZREMRANGEBYRANK")
                    .arg(&key)
                    .arg(0)
                    .arg(-2)
                    .query_async(&mut conn)
                    .await;
            }
            _ => {}
        }
    }

    async fn range(&self, topic: &str, from: i64, to: i64) -> Vec<LogEntry> {
        let key = self.log_key(topic);
        let mut conn = self.pool.conn().clone();
        let data: Result<Vec<Vec<u8>>, _> = redis::cmd("ZRANGEBYSCORE")
            .arg(&key)
            .arg(from)
            .arg(to)
            .query_async(&mut conn)
            .await;
        data.unwrap_or_default()
            .iter()
            .filter_map(|d| Self::decode_entry(d))
            .collect()
    }

    async fn latest(&self, topic: &str) -> Option<LogEntry> {
        let key = self.log_key(topic);
        let mut conn = self.pool.conn().clone();
        let data: Result<Vec<Vec<u8>>, _> = redis::cmd("ZREVRANGE")
            .arg(&key)
            .arg(0)
            .arg(0)
            .query_async(&mut conn)
            .await;
        data.unwrap_or_default()
            .first()
            .and_then(|d| Self::decode_entry(d))
    }

    async fn remove(&self, topic: &str) {
        let key = self.log_key(topic);
        let mut conn = self.pool.conn().clone();
        let _: Result<(), _> = redis::cmd("DEL").arg(&key).query_async(&mut conn).await;
    }
}
