#![allow(dead_code, unused_imports, unused_variables, clippy::all)]
//! Multi-instance Redis integration test.
//!
//! This test requires a running Redis instance at
//! `redis://127.0.0.1:6379`. Skip in CI by default (`#[ignore]`).
//!
//! ```sh
//! REDIS_URL=redis://127.0.0.1:6379 cargo test --features redis -- --ignored --test-threads=1
//! ```

#[cfg(feature = "redis")]
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use rifts::broker::broker::Broker;
    use rifts::broker::fanout::{SubscribeIntent, test_sink::CountingSink};
    use rifts::frame::{EncodingFormat, Frame, FrameFlags, FrameType};
    use rifts::redis::{
        FanoutBridge, RedisActorBroker, RedisDedupeStore, RedisLogStore, RedisOffsetStore,
        RedisPool, RedisSnapshotStore,
    };

    fn make_frame(topic: &str, msg_id: &str, payload: &[u8]) -> Frame {
        Frame {
            version: 0x0100,
            frame_id: 1,
            frame_type: FrameType::Data,
            flags: FrameFlags::empty(),
            codec: EncodingFormat::Json,
            session_id: Some("s-1".into()),
            stream_id: None,
            topic: Some(topic.into()),
            event: Some("test.event".into()),
            message_id: Some(msg_id.into()),
            correlation_id: None,
            trace_id: None,
            timestamp: 0,
            ttl_ms: None,
            priority: None,
            payload: Some(bytes::Bytes::copy_from_slice(payload)),
        }
    }

    fn redis_url() -> String {
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string())
    }

    type TestBroker =
        RedisActorBroker<RedisOffsetStore, RedisLogStore, RedisDedupeStore, RedisSnapshotStore>;

    async fn make_broker(prefix: &str) -> TestBroker {
        let pool = RedisPool::connect(&redis_url(), prefix).await.unwrap();
        let offsets = Arc::new(RedisOffsetStore::new(pool.clone()));
        let log = Arc::new(RedisLogStore::new(pool.clone()));
        let dedupe = Arc::new(RedisDedupeStore::new(pool.clone()));
        let snapshots = Arc::new(RedisSnapshotStore::new(pool.clone()));
        let bridge = FanoutBridge::new(pool.clone());

        RedisActorBroker::new(pool, offsets, log, dedupe, snapshots, bridge, 65536)
    }

    #[tokio::test]
    #[ignore = "requires Redis at REDIS_URL"]
    async fn publish_assigns_offset() {
        let b = make_broker("test:pub").await;
        let out = b.publish(&make_frame("t", "m1", b"hello")).await.unwrap();
        assert_eq!(out.offset, 1);
    }

    #[tokio::test]
    #[ignore = "requires Redis at REDIS_URL"]
    async fn cross_instance_fanout() {
        // Instance A publishes to topic "chat".
        let broker_a = make_broker("test:a").await;
        // Instance B subscribes to topic "chat".
        let broker_b = make_broker("test:b").await;

        let sink_b = Arc::new(CountingSink::new(1));
        broker_b
            .subscribe("chat", SubscribeIntent::Live, sink_b.clone())
            .await
            .unwrap();

        // Publish from instance A.
        broker_a
            .publish(&make_frame("chat", "m-cross", b"hello from A"))
            .await
            .unwrap();

        // Wait a moment for Redis Pub/Sub to deliver.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Instance B's subscriber should have received the message.
        assert_eq!(sink_b.count(), 1);
    }
}
