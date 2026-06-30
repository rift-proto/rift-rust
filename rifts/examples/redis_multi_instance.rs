//! Redis multi-instance broker example.
//!
//! This example demonstrates two `rifts` server instances sharing
//! topics via Redis. Instance A publishes a message; Instance B
//! receives it through Redis Pub/Sub.
//!
//! ```sh
//! # Requires a running Redis at redis://127.0.0.1:6379
//! cargo run --example redis_multi_instance --features redis
//! ```

use std::sync::Arc;
use std::time::Duration;

use rifts::broker::broker::Broker;
use rifts::broker::fanout::SubscribeIntent;
use rifts::broker::fanout::test_sink::CountingSink;
use rifts::frame::{EncodingFormat, Frame, FrameFlags, FrameType};
use rifts::redis::{
    FanoutBridge, RedisActorBroker, RedisDedupeStore, RedisLogStore, RedisOffsetStore, RedisPool,
    RedisSnapshotStore,
};

fn make_frame(topic: &str, msg_id: &str, payload: &[u8]) -> Frame {
    Frame {
        version: 0x0100,
        frame_id: 1,
        frame_type: FrameType::Data,
        flags: FrameFlags::empty(),
        codec: EncodingFormat::Json,
        session_id: Some("example".into()),
        stream_id: None,
        topic: Some(topic.into()),
        event: Some("example.event".into()),
        message_id: Some(msg_id.into()),
        correlation_id: None,
        trace_id: None,
        timestamp: 0,
        ttl_ms: None,
        priority: None,
        payload: Some(bytes::Bytes::copy_from_slice(payload)),
    }
}

type TestBroker =
    RedisActorBroker<RedisOffsetStore, RedisLogStore, RedisDedupeStore, RedisSnapshotStore>;

async fn make_broker(prefix: &str) -> TestBroker {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let pool = RedisPool::connect(&url, prefix)
        .await
        .expect("redis connect");
    let offsets = Arc::new(RedisOffsetStore::new(pool.clone()));
    let log = Arc::new(RedisLogStore::new(pool.clone()));
    let dedupe = Arc::new(RedisDedupeStore::new(pool.clone()));
    let snapshots = Arc::new(RedisSnapshotStore::new(pool.clone()));
    let bridge = FanoutBridge::new(pool.clone());

    RedisActorBroker::new(pool, offsets, log, dedupe, snapshots, bridge, 65536)
}

#[tokio::main]
async fn main() -> rifts::Result<()> {
    println!("=== Redis Multi-Instance Demo ===\n");

    // Simulate two instances sharing Redis.
    let broker_a = make_broker("demo:a").await;
    let broker_b = make_broker("demo:b").await;

    let topic = "chat/room1";

    // Instance B subscribes to the topic.
    let sink_b = Arc::new(CountingSink::new(3));
    broker_b
        .subscribe(topic, SubscribeIntent::Live, sink_b.clone())
        .await?;
    println!("Instance B subscribed to '{topic}'");

    // Instance A publishes 3 messages.
    for i in 1..=3 {
        let msg_id = format!("msg-{i}");
        let out = broker_a
            .publish(&make_frame(
                topic,
                &msg_id,
                format!("Hello #{i}").as_bytes(),
            ))
            .await?;
        println!("Instance A published '{}' → offset {}", msg_id, out.offset);
    }

    // Wait for Redis Pub/Sub delivery.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Instance B should have received all 3 messages.
    println!("\nInstance B received {} messages", sink_b.count());
    assert_eq!(
        sink_b.count(),
        3,
        "cross-instance fanout should deliver all messages"
    );

    // Verify replay works (reads from Redis log).
    let replayed = broker_b.replay(topic, 1, 3).await?;
    println!("Replay from Redis log: {} entries", replayed.len());
    assert_eq!(replayed.len(), 3);

    println!("\n=== All checks passed ===");
    Ok(())
}
