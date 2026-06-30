//! Integration tests: full lifecycle, broker operations, session management,
//! and error handling.
//!
//! These tests exercise the Rift server end-to-end at the API level,
//! covering the critical paths that must work correctly in production.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rifts::broker::fanout::test_sink::CountingSink;
use rifts::broker::fanout::{FanoutEngine, SubscribeIntent};
use rifts::broker::{Broker, InMemoryBroker};
use rifts::codec::JsonCodec;
use rifts::codec::codec::PayloadCodecExt;
use rifts::frame::{EncodingFormat as FrameEncodingFormat, Frame, FrameFlags, FrameType};
use rifts::session::store::SessionStore;
use rifts::session::{ClientId, Session, SessionId, SessionState};
use rifts::storage::{DedupeStore, MemoryDedupeStore};
use rifts::topic::{RetentionPolicy, TopicProfile, TopicStore};

// ── Helper utilities ──────────────────────────────────────────────────────────

/// Build a minimal publish frame with required fields.
fn publish_frame(topic: &str, message_id: &str, payload: &[u8]) -> Frame {
    Frame {
        version: 0x0100,
        frame_id: 1,
        frame_type: FrameType::Data,
        flags: FrameFlags::empty(),
        codec: FrameEncodingFormat::Json,
        session_id: Some("test-session".into()),
        stream_id: None,
        topic: Some(topic.into()),
        event: Some("test.event".into()),
        message_id: Some(message_id.into()),
        correlation_id: None,
        trace_id: None,
        timestamp: 0,
        ttl_ms: None,
        priority: None,
        payload: Some(Bytes::copy_from_slice(payload)),
    }
}

fn test_broker() -> InMemoryBroker<
    rifts::storage::MemoryOffsetStore,
    rifts::storage::MemoryLogStore,
    rifts::storage::MemoryDedupeStore,
    rifts::storage::MemorySnapshotStore,
> {
    InMemoryBroker::new(TopicProfile::default(), Duration::from_secs(60), 65_536)
}

// ── Broker: publish / subscribe / fanout ─────────────────────────────────────

#[tokio::test]
async fn e2e_publish_subscribe_fanout() {
    let broker = test_broker();
    let sink = Arc::new(CountingSink::new(1));

    // Subscribe to topic "chat/general".
    let sub_id = broker
        .subscribe("chat/general", SubscribeIntent::Live, sink.clone())
        .await
        .expect("subscribe should succeed");

    // Publish two messages.
    let out1 = broker
        .publish(&publish_frame("chat/general", "msg-1", b"hello"))
        .await
        .expect("publish should succeed");
    let out2 = broker
        .publish(&publish_frame("chat/general", "msg-2", b"world"))
        .await
        .expect("publish should succeed");

    assert_eq!(out1.offset, 1);
    assert_eq!(out2.offset, 2);
    assert!(!out1.duplicate);
    assert!(!out2.duplicate);

    // Both messages delivered to the sink.
    assert_eq!(sink.count(), 2);

    // Unsubscribe and publish again — no more deliveries.
    let removed = broker
        .unsubscribe(sub_id)
        .await
        .expect("unsubscribe should succeed");
    assert!(removed);
    broker
        .publish(&publish_frame("chat/general", "msg-3", b"nope"))
        .await
        .expect("publish should succeed");
    assert_eq!(sink.count(), 2);
}

#[tokio::test]
async fn e2e_deduplication_within_window() {
    let broker = test_broker();
    let sink = Arc::new(CountingSink::new(1));

    broker
        .subscribe("t", SubscribeIntent::Live, sink.clone())
        .await
        .unwrap();

    // Same message_id twice within the dedupe window.
    let out1 = broker
        .publish(&publish_frame("t", "dup-key", b"x"))
        .await
        .unwrap();
    let out2 = broker
        .publish(&publish_frame("t", "dup-key", b"x"))
        .await
        .unwrap();

    assert!(!out1.duplicate);
    assert!(out2.duplicate);
    assert_eq!(sink.count(), 1, "duplicate should not be delivered");
}

#[tokio::test]
async fn e2e_replay_returns_historical_messages() {
    let profile = TopicProfile {
        retention: RetentionPolicy::Count(100),
        ..TopicProfile::default()
    };
    let broker = InMemoryBroker::new(profile, Duration::from_secs(60), 65_536);

    // Publish 10 messages.
    for i in 1..=10 {
        broker
            .publish(&publish_frame(
                "orders",
                &format!("m{i}"),
                format!("data-{i}").as_bytes(),
            ))
            .await
            .unwrap();
    }

    // Replay from offset 3 to 7 (inclusive).
    let replayed = broker.replay("orders", 3, 7).await.unwrap();
    assert_eq!(replayed.len(), 5);
}

#[tokio::test]
async fn e2e_multiple_subscribers() {
    let broker = test_broker();
    let s1 = Arc::new(CountingSink::new(1));
    let s2 = Arc::new(CountingSink::new(2));
    let s3 = Arc::new(CountingSink::new(3));

    broker
        .subscribe("t", SubscribeIntent::Live, s1.clone())
        .await
        .unwrap();
    broker
        .subscribe("t", SubscribeIntent::Live, s2.clone())
        .await
        .unwrap();
    // s3 subscribes to a different topic.
    broker
        .subscribe("other", SubscribeIntent::Live, s3.clone())
        .await
        .unwrap();

    broker
        .publish(&publish_frame("t", "m1", b"hi"))
        .await
        .unwrap();

    assert_eq!(s1.count(), 1);
    assert_eq!(s2.count(), 1);
    assert_eq!(s3.count(), 0, "wrong topic should not receive");
}

#[tokio::test]
async fn e2e_drop_sink_removes_all_subscriptions() {
    let broker = test_broker();
    let sink = Arc::new(CountingSink::new(7));

    broker
        .subscribe("a", SubscribeIntent::Live, sink.clone())
        .await
        .unwrap();
    broker
        .subscribe("b", SubscribeIntent::Live, sink.clone())
        .await
        .unwrap();
    broker
        .subscribe("c", SubscribeIntent::Live, sink.clone())
        .await
        .unwrap();

    let removed = broker.drop_sink(7).await;
    assert_eq!(removed, 3);

    // Publish to all three topics — no deliveries.
    for topic in &["a", "b", "c"] {
        broker
            .publish(&publish_frame(topic, "m1", b"x"))
            .await
            .unwrap();
    }
    assert_eq!(sink.count(), 0);
}

#[tokio::test]
async fn e2e_subscriber_count_tracking() {
    let broker = test_broker();
    let s1 = Arc::new(CountingSink::new(1));
    let s2 = Arc::new(CountingSink::new(2));

    assert_eq!(broker.subscriber_count("t").await, 0);

    let id1 = broker
        .subscribe("t", SubscribeIntent::Live, s1.clone())
        .await
        .unwrap();
    assert_eq!(broker.subscriber_count("t").await, 1);

    let id2 = broker
        .subscribe("t", SubscribeIntent::Live, s2.clone())
        .await
        .unwrap();
    assert_eq!(broker.subscriber_count("t").await, 2);

    broker.unsubscribe(id1).await.unwrap();
    assert_eq!(broker.subscriber_count("t").await, 1);

    broker.unsubscribe(id2).await.unwrap();
    assert_eq!(broker.subscriber_count("t").await, 0);
}

// ── Session lifecycle ─────────────────────────────────────────────────────────

#[test]
fn session_state_transitions() {
    let s = Session::new(SessionId::new(), ClientId::new("user-1"));
    assert_eq!(s.state(), SessionState::Open);
    assert!(s.is_alive());

    s.set_state(SessionState::Hello);
    assert_eq!(s.state(), SessionState::Hello);

    s.set_state(SessionState::Authenticated);
    s.set_state(SessionState::Ready);
    s.set_state(SessionState::Active);
    assert_eq!(s.state(), SessionState::Active);
    assert!(s.is_alive());

    s.set_state(SessionState::Draining);
    s.set_state(SessionState::Closed);
    assert!(!s.is_alive());
}

#[test]
fn session_epoch_tracking() {
    let s = Session::new(SessionId::new(), ClientId::new("user-1"));
    assert_eq!(s.current_epoch(), 1);

    // Bump on each resume.
    assert_eq!(s.bump_epoch(), 2);
    assert_eq!(s.bump_epoch(), 3);
    assert_eq!(s.current_epoch(), 3);
}

#[test]
fn session_idle_tracking() {
    let s = Session::new(SessionId::new(), ClientId::new("user-1"));
    // Immediately after creation the session should have near-zero idle time.
    assert!(s.idle() < Duration::from_secs(1));
}

// ── Dedupe store ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn dedupe_store_concurrent_single_fresh() {
    let store = Arc::new(MemoryDedupeStore::new());
    let w = Duration::from_secs(60);
    let mut handles = Vec::new();
    for _ in 0..16 {
        let s = store.clone();
        handles.push(tokio::spawn(async move {
            s.check_and_record("t", "racing-key", w).await
        }));
    }
    let mut fresh_count = 0;
    for h in handles {
        if h.await.unwrap() {
            fresh_count += 1;
        }
    }
    assert_eq!(fresh_count, 1, "exactly one thread must see fresh");
}

#[tokio::test]
async fn dedupe_store_sweep_cleans_expired() {
    let store = MemoryDedupeStore::new();
    store
        .check_and_record("t", "k", Duration::from_secs(0))
        .await;
    let swept = store.sweep().await;
    assert!(
        swept >= 1,
        "sweeping immediately-expired entries should remove at least 1"
    );
    assert!(
        store
            .check_and_record("t", "k", Duration::from_secs(60))
            .await
    );
}

// ── Topic limits ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn subscriber_limit_enforcement() {
    let profile = TopicProfile {
        max_subscribers: 1,
        ..TopicProfile::default()
    };
    let broker = InMemoryBroker::new(profile, Duration::from_secs(60), 65_536);
    let s1 = Arc::new(CountingSink::new(1));
    let s2 = Arc::new(CountingSink::new(2));

    // First subscriber succeeds.
    broker
        .subscribe("t", SubscribeIntent::Live, s1.clone())
        .await
        .expect("first subscriber should succeed");

    // Second subscriber should fail (limit = 1).
    let result = broker
        .subscribe("t", SubscribeIntent::Live, s2.clone())
        .await;
    assert!(result.is_err(), "second subscriber should be rejected");

    // After unsubscribing, a new subscriber can join.
    broker.drop_sink(1).await;
    broker
        .subscribe("t", SubscribeIntent::Live, s2.clone())
        .await
        .expect("subscriber after drain should succeed");
}

#[tokio::test]
async fn publisher_limit_enforcement() {
    let profile = TopicProfile {
        max_publishers: 1,
        ..TopicProfile::default()
    };
    let broker = InMemoryBroker::new(profile, Duration::from_secs(60), 65_536);

    // First publish creates the topic and claims the publisher slot.
    broker
        .publish(&publish_frame("t", "m1", b"x"))
        .await
        .expect("first publish should succeed");

    // Second publish from a different session should succeed (the frame
    // has "test-session" as session_id, but the broker's can_publish
    // check is per-topic, not per-session; the limit is 1 publisher
    // total, and the first one already used it). Actually— the
    // current implementation increments publisher count on every
    // publish, and the frame carries a session_id. Let's verify
    // with a second session.
    let mut frame2 = publish_frame("t", "m2", b"y");
    frame2.session_id = Some("other-session".into());
    let result = broker.publish(&frame2).await;
    // With max_publishers=1 and try_inc_publisher atomically checking,
    // the second publish should fail.
    assert!(result.is_err(), "second publisher should be rejected");
}

// ── Topic store ───────────────────────────────────────────────────────────────

#[test]
fn topic_store_get_or_create_and_stats() {
    let store = TopicStore::new();
    let entry = store
        .get_or_create("chat/room1", TopicProfile::default())
        .unwrap();
    assert_eq!(entry.name, "chat/room1");
    assert!(store.exists("chat/room1"));
    assert!(!store.exists("nonexistent"));

    let stats = store.stats();
    assert!(stats.contains_key("chat/room1"));
}

#[test]
fn topic_store_retention_policies() {
    let store = TopicStore::new();
    let entry = store
        .get_or_create(
            "t-count",
            TopicProfile {
                retention: RetentionPolicy::Count(3),
                ..TopicProfile::default()
            },
        )
        .unwrap();

    for i in 1..=5 {
        entry.append(rifts::topic::store::LogEntry {
            offset: i,
            publisher_session: None,
            message_id: format!("m{i}"),
            class: "event".into(),
            event: Some("e".into()),
            payload: Bytes::from(format!("data-{i}")),
            timestamp: 0,
            appended_at: None,
        });
    }
    let log = entry.log.read();
    assert_eq!(log.len(), 3, "count retention should keep only 3 entries");
    assert_eq!(log[0].offset, 3);
    assert_eq!(log[2].offset, 5);
}

#[test]
fn topic_store_size_retention() {
    let store = TopicStore::new();
    let entry = store
        .get_or_create(
            "t-size",
            TopicProfile {
                retention: RetentionPolicy::Size(10),
                ..TopicProfile::default()
            },
        )
        .unwrap();

    entry.append(rifts::topic::store::LogEntry {
        offset: 1,
        publisher_session: None,
        message_id: "m1".into(),
        class: "event".into(),
        event: None,
        payload: Bytes::from_static(b"1234567890"), // 10 bytes
        timestamp: 0,
        appended_at: None,
    });
    // This second entry pushes total size over 10.
    entry.append(rifts::topic::store::LogEntry {
        offset: 2,
        publisher_session: None,
        message_id: "m2".into(),
        class: "event".into(),
        event: None,
        payload: Bytes::from_static(b"x"),
        timestamp: 0,
        appended_at: None,
    });
    let log = entry.log.read();
    // After eviction, only the latest entry remains.
    assert_eq!(log.len(), 1);
    assert_eq!(log[0].offset, 2);
}

// ── Fanout engine ─────────────────────────────────────────────────────────────

#[test]
fn fanout_engine_independent_topics() {
    let fan = FanoutEngine::new();
    let s1 = Arc::new(CountingSink::new(1));
    let s2 = Arc::new(CountingSink::new(2));

    fan.subscribe("orders", SubscribeIntent::Live, s1.clone());
    fan.subscribe("alerts", SubscribeIntent::Live, s2.clone());

    fan.deliver("orders", Bytes::from_static(b"order-placed"));
    fan.deliver("alerts", Bytes::from_static(b"cpu-high"));

    assert_eq!(s1.count(), 1);
    assert_eq!(s2.count(), 1);
}

#[test]
fn fanout_engine_deliver_returns_delivery_count() {
    let fan = FanoutEngine::new();
    let s1 = Arc::new(CountingSink::new(1));
    let s2 = Arc::new(CountingSink::new(2));

    fan.subscribe("t", SubscribeIntent::Live, s1.clone());
    fan.subscribe("t", SubscribeIntent::Live, s2.clone());

    let delivered = fan.deliver("t", Bytes::from_static(b"hi"));
    assert_eq!(delivered, 2);
}

// ── Session store ─────────────────────────────────────────────────────────────

#[test]
fn session_store_insert_get_remove() {
    let store = SessionStore::new();
    let session = Arc::new(Session::new(SessionId::new(), ClientId::new("c1")));
    let sid = session.id.0.clone();

    store.insert(session.clone());
    assert!(store.get(&sid).is_some());
    assert_eq!(store.len(), 1);

    let removed = store.remove(&sid);
    assert!(removed);
    assert!(store.get(&sid).is_none());
    assert!(store.is_empty());
}

#[test]
fn session_store_expiry() {
    let store = SessionStore::new();
    let session = Arc::new(Session::new(SessionId::new(), ClientId::new("c1")));
    let sid = session.id.0.clone();
    store.insert(session.clone());

    // Active sessions with a long timeout should not expire.
    let expired = store.expire_sessions(Duration::from_secs(3600));
    assert_eq!(expired, 0);
    assert!(store.get(&sid).is_some());

    // Close the session — it should be expired regardless of timeout.
    session.set_state(SessionState::Closed);
    let expired = store.expire_sessions(Duration::from_secs(3600));
    assert_eq!(expired, 1);
    assert!(store.get(&sid).is_none());
}

// ── Codec round-trip ──────────────────────────────────────────────────────────

#[test]
fn json_codec_round_trip_frame_payload() {
    let c = JsonCodec;
    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    struct ChatMessage {
        room: String,
        text: String,
    }
    let msg = ChatMessage {
        room: "lobby".into(),
        text: "hello".into(),
    };
    let encoded = c.encode(&msg).unwrap();
    let decoded: ChatMessage = c.decode(&encoded).unwrap();
    assert_eq!(decoded, msg);
}

// ── Frame wire format ─────────────────────────────────────────────────────────

#[test]
fn frame_binary_round_trip_with_payload() {
    let frame = Frame {
        frame_type: FrameType::Data,
        codec: FrameEncodingFormat::Json,
        payload: Some(Bytes::from_static(b"test-payload")),
        flags: FrameFlags::empty(),
        frame_id: 1,
        timestamp: 1000,
        ..Frame::default()
    };

    let encoded = rifts::frame_codec::encode_frame(&frame).unwrap();
    let decoded =
        rifts::frame_codec::decode_binary_frame(&encoded, rifts::DEFAULT_MAX_BINARY_PAYLOAD)
            .unwrap();

    assert_eq!(decoded.frame_type, FrameType::Data);
    assert_eq!(decoded.frame_id, 1);
    assert_eq!(decoded.timestamp, 1000);
    assert_eq!(decoded.payload.as_deref(), Some(&b"test-payload"[..]));
}

#[test]
fn frame_decode_rejects_oversized_payload() {
    let frame = Frame {
        frame_type: FrameType::Data,
        codec: FrameEncodingFormat::Json,
        payload: Some(Bytes::from_static(b"x")),
        flags: FrameFlags::empty(),
        frame_id: 1,
        timestamp: 0,
        ..Frame::default()
    };
    let encoded = rifts::frame_codec::encode_frame(&frame).unwrap();
    // Try to decode with a limit smaller than the payload.
    let result = rifts::frame_codec::decode_binary_frame(&encoded, 0);
    assert!(result.is_err());
}

// ── Backpressure controller ───────────────────────────────────────────────────

#[test]
fn backpressure_accept_and_release() {
    let bp = rifts::flow::BackpressureController::new(100);
    assert_eq!(bp.try_enqueue(50), rifts::flow::BackpressureAction::Accept);
    assert_eq!(bp.current_bytes(), 50);
    bp.release(30);
    assert_eq!(bp.current_bytes(), 20);
}

#[test]
fn backpressure_rejects_when_full() {
    let bp = rifts::flow::BackpressureController::new(100);
    bp.set_strategy(rifts::flow::BackpressureStrategy::Pause);
    bp.try_enqueue(80);
    assert_eq!(bp.try_enqueue(50), rifts::flow::BackpressureAction::Pause);
}

#[test]
fn backpressure_overload_detection() {
    let bp = rifts::flow::BackpressureController::new(100);
    // 95 > 90% (high-water mark).
    bp.try_enqueue(95);
    assert!(bp.is_overloaded());
    bp.release(10);
    assert!(!bp.is_overloaded());
}

// ── Error handling ────────────────────────────────────────────────────────────

#[test]
fn error_conversion_from_std_io() {
    let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset");
    let rift_err: rifts::RiftError = io_err.into();
    assert!(matches!(rift_err, rifts::RiftError::Io(_)));
}

#[test]
fn topic_validation_rejects_bad_names() {
    assert!(rifts::topic::store::validate_name("").is_err());
    assert!(rifts::topic::store::validate_name("$system").is_err());
    assert!(rifts::topic::store::validate_name("a\x00b").is_err());
    assert!(rifts::topic::store::validate_name(&"x".repeat(257)).is_err());
}

#[test]
fn frame_requires_ack_flag() {
    let mut frame = Frame::default();
    assert!(!frame.requires_ack());

    frame.flags.set(FrameFlags::REQUIRES_ACK);
    assert!(frame.requires_ack());
}
