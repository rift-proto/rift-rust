#![allow(dead_code, unused_imports, unused_variables, clippy::all)]
//! Shared fixtures and helpers for the `rifts` benchmark suite.
//!
//! All benches compile as external crates, so they can only use the public
//! API of `rifts`. This module centralises payload generation, topic naming,
//! frame construction, a lightweight atomic-only `CountingSink`, and a
//! reusable tokio runtime so individual bench files stay small.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use rifts::broker::memory_broker::DefaultBroker;
use rifts::broker::{
    ConnectionSink, FanoutEngine, FanoutError, FanoutSink, SubscribeIntent, SubscriptionId,
};
use rifts::frame::{EncodingFormat, Frame, FrameFlags};
use rifts::message::MessageClass;
use rifts::storage::{MemoryDedupeStore, MemoryLogStore, MemoryOffsetStore, MemorySnapshotStore};
use rifts::topic::{LogEntry, RetentionPolicy, TopicProfile};
use rifts::{Broker, FrameType, encode_frame};
use std::hint::black_box;

pub const PAYLOAD_SIZES: &[usize] = &[0, 64, 1024, 16 * 1024, 256 * 1024, 1024 * 1024];
pub const PAYLOAD_SIZE_LABELS: &[&str] = &["0B", "64B", "1KiB", "16KiB", "256KiB", "1MiB"];
pub const SUBSCRIBER_COUNTS: &[usize] = &[1, 10, 100, 1000];
pub const TOPIC_COUNTS: &[usize] = &[1, 10, 100, 1000];
pub const CONCURRENCY_LEVELS: &[usize] = &[1, 2, 4, 8, 16, 64];

pub fn now_ms_public() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub fn payload_of(size: usize) -> Bytes {
    if size == 0 {
        return Bytes::new();
    }
    let mut v = Vec::with_capacity(size);
    let mut byte = 0u8;
    for _ in 0..size {
        v.push(byte);
        byte = byte.wrapping_add(7);
    }
    Bytes::from(v)
}

pub fn topic_name(idx: usize) -> String {
    format!("bench.topic.{idx}")
}

pub fn topic_names(count: usize) -> Vec<String> {
    (0..count).map(topic_name).collect()
}

pub fn build_data_frame(payload: Bytes, frame_id: u64) -> Frame {
    let mut f = Frame::data();
    f.frame_id = frame_id;
    f.timestamp = now_ms_public();
    f.topic = Some("bench.topic.0".to_string());
    f.message_id = Some(format!("msg-{frame_id}"));
    f.payload = Some(payload);
    f
}

pub fn build_control_frame(frame_id: u64) -> Frame {
    let mut f = Frame::control();
    f.frame_id = frame_id;
    f.timestamp = now_ms_public();
    f
}

pub fn build_ack_frame(frame_id: u64) -> Frame {
    let mut f = Frame::ack();
    f.frame_id = frame_id;
    f.timestamp = now_ms_public();
    f.flags = FrameFlags::empty().with(FrameFlags::REQUIRES_ACK);
    f
}

pub fn build_frame_with_type(ft: FrameType, frame_id: u64) -> Frame {
    let mut f = Frame {
        version: 1,
        frame_id,
        frame_type: ft,
        flags: FrameFlags::empty(),
        codec: EncodingFormat::Json,
        session_id: None,
        stream_id: None,
        topic: Some("bench.topic.0".to_string()),
        event: None,
        message_id: Some(format!("msg-{frame_id}")),
        correlation_id: None,
        trace_id: None,
        timestamp: now_ms_public(),
        ttl_ms: None,
        priority: None,
        payload: None,
    };
    black_box(&mut f);
    f
}

pub fn build_event_log_entry(offset: i64, payload: Bytes) -> LogEntry {
    LogEntry {
        offset,
        publisher_session: Some("bench-session".to_string()),
        message_id: format!("msg-{offset}"),
        class: MessageClass::Event.as_str().to_string(),
        event: Some("bench.event".to_string()),
        payload,
        timestamp: now_ms_public(),
        appended_at: None,
    }
}

pub fn default_profile() -> TopicProfile {
    TopicProfile::default()
}

pub fn profile_with_retention(retention: RetentionPolicy) -> TopicProfile {
    let mut p = TopicProfile::default();
    p.retention = retention;
    p
}

pub fn default_broker(dedupe_window_ms: u64, max_payload_bytes: usize) -> DefaultBroker {
    DefaultBroker::new(
        default_profile(),
        std::time::Duration::from_millis(dedupe_window_ms),
        max_payload_bytes,
    )
}

pub fn broker_with_stores(
    profile: TopicProfile,
    dedupe_window: std::time::Duration,
    max_payload_bytes: usize,
) -> DefaultBroker {
    DefaultBroker::with_stores(
        profile,
        dedupe_window,
        max_payload_bytes,
        MemoryOffsetStore::new(),
        MemoryLogStore::new(),
        MemoryDedupeStore::new(),
        MemorySnapshotStore::new(),
    )
}

pub fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime")
}

pub fn encode(frame: &Frame) -> bytes::Bytes {
    encode_frame(frame).expect("encode_frame")
}

/// Lightweight atomic-only sink that increments a counter without storing
/// payload bytes. Suitable for throughput benchmarks where the cost of
/// storing payloads in `Mutex<Vec<Vec<u8>>>` (as `rifts::broker::fanout::test_sink::CountingSink`
/// does) would dominate the measured time.
pub struct AtomicCountingSink {
    id: u64,
    count: AtomicU64,
    bytes: AtomicU64,
}

impl AtomicCountingSink {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            count: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        }
    }

    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    pub fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }
}

impl FanoutSink for AtomicCountingSink {
    fn deliver(&self, frame: Bytes) -> Result<(), FanoutError> {
        let len = frame.len() as u64;
        self.count.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(len, Ordering::Relaxed);
        black_box(frame);
        Ok(())
    }

    fn id(&self) -> u64 {
        self.id
    }
}

pub fn atomic_sink(id: u64) -> ConnectionSink {
    Arc::new(AtomicCountingSink::new(id))
}

pub fn subscribe_n_sinks(engine: &FanoutEngine, topic: &str, n: usize) -> Vec<SubscriptionId> {
    let mut ids = Vec::with_capacity(n);
    for i in 0..n {
        let id = engine.subscribe(topic, SubscribeIntent::Live, atomic_sink(i as u64));
        ids.push(id);
    }
    ids
}

pub fn bench_payload_sizes() -> Vec<(String, Bytes)> {
    PAYLOAD_SIZES
        .iter()
        .zip(PAYLOAD_SIZE_LABELS.iter())
        .map(|(&sz, &label)| (label.to_string(), payload_of(sz)))
        .collect()
}

pub fn black_box_broker_publish(broker: &Arc<dyn Broker>, frame: &Frame) {
    let _ = broker.publish(frame);
}
