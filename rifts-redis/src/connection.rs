//! Redis connection pool and key-space helpers.
//!
//! `RedisPool` holds an async [`redis::aio::MultiplexedConnection`] for
//! Pub/Sub and fanout. Storage trait methods use the same async connection
//! so they never block the tokio runtime.

use redis::Client;
use redis::aio::MultiplexedConnection;

use rifts_core::error::{Result, RiftError, SystemReject};

/// A Redis connection pool holding an async multiplexed connection.
///
/// The async [`MultiplexedConnection`] is used for both Pub/Sub fanout
/// and storage operations so the tokio runtime is never blocked.
///
/// # Cloning
///
/// `RedisPool` derives `Clone`. Each clone shares the same underlying
/// async connection (multiplexed).
#[derive(Clone)]
pub struct RedisPool {
    /// Async multiplexed connection for Pub/Sub, fanout, and storage.
    conn: MultiplexedConnection,
    /// Redis connection URL for creating additional connections (e.g. Pub/Sub).
    url: String,
    /// Key prefix applied to every Redis key.
    prefix: String,
}

impl RedisPool {
    /// Create a new pool connected to the given Redis URL.
    pub async fn connect(url: &str, prefix: &str) -> Result<Self> {
        let client = Client::open(url)
            .map_err(|e| RiftError::System(SystemReject::Internal(format!("redis client: {e}"))))?;
        let conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| {
                RiftError::System(SystemReject::Internal(format!("redis connect: {e}")))
            })?;
        Ok(Self {
            conn,
            url: url.to_string(),
            prefix: prefix.to_string(),
        })
    }

    /// Return a reference to the async multiplexed connection.
    pub fn conn(&self) -> &MultiplexedConnection {
        &self.conn
    }

    /// The Redis connection URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Build a namespaced Redis key: `{prefix}:{suffix}`.
    pub fn key(&self, suffix: &str) -> String {
        format!("{}:{suffix}", self.prefix)
    }

    /// Build a topic-scoped namespaced Redis key: `{prefix}:{kind}:{topic}`.
    pub fn topic_key(&self, kind: &str, topic: &str) -> String {
        format!("{}:{kind}:{topic}", self.prefix)
    }

    /// The configured key prefix.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }
}

impl std::fmt::Debug for RedisPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisPool")
            .field("prefix", &self.prefix)
            .finish()
    }
}
