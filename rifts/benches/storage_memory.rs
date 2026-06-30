//! Memory storage benchmarks — offset, log, dedupe, snapshot stores, key encoding.
//!
//! All bench iter closures use `tokio::runtime::Runtime::block_on` to drive
//! the async storage trait methods since criterion benches run outside
//! the tokio async runtime.

use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rifts::storage::{
    DedupeStore, LogStore, MemoryDedupeStore, MemoryLogStore, MemoryOffsetStore,
    MemorySnapshotStore, OffsetStore, SnapshotStore, dedupe_key, dedupe_prefix, log_key,
    log_prefix, offset_key, offset_prefix, snapshot_key, snapshot_prefix,
};
use rifts::topic::{RetentionPolicy, TopicProfile, TopicStore};
use std::hint::black_box;

use crate::common::{TOPIC_COUNTS, build_event_log_entry, payload_of, topic_names};

mod common;

/// Helper: drive an async future synchronously for criterion benches.
fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().expect("create tokio runtime for bench")
}

fn bench_offset_alloc(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("storage/offset_alloc");
    group.bench_function("same_topic", |b| {
        let store = MemoryOffsetStore::new();
        b.iter(|| rt.block_on(store.alloc(black_box("bench.topic"))));
    });
    for &n in TOPIC_COUNTS {
        group.bench_with_input(BenchmarkId::new("many_topics", n), &n, |b, &n| {
            let store = MemoryOffsetStore::new();
            let names = topic_names(n);
            let mut i = 0;
            b.iter(|| {
                let name = &names[i % n];
                rt.block_on(store.alloc(black_box(name)));
                i += 1;
            });
        });
    }
    group.finish();
}

fn bench_offset_head(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("storage/offset_head");
    group.bench_function("hit", |b| {
        let store = MemoryOffsetStore::new();
        rt.block_on(store.alloc("bench.topic"));
        b.iter(|| rt.block_on(store.head(black_box("bench.topic"))));
    });
    group.bench_function("miss", |b| {
        let store = MemoryOffsetStore::new();
        b.iter(|| rt.block_on(store.head(black_box("nonexistent.topic"))));
    });
    group.finish();
}

fn bench_offset_remove(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("storage/offset_remove");
    group.bench_function("remove", |b| {
        let store = MemoryOffsetStore::new();
        b.iter(|| {
            rt.block_on(store.alloc("bench.topic"));
            rt.block_on(store.remove(black_box("bench.topic")));
        });
    });
    group.finish();
}

fn bench_log_append(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("storage/log_append");
    for &sz in &[0usize, 64, 1024, 16384] {
        group.bench_with_input(BenchmarkId::new("size", sz), &sz, |b, &sz| {
            let store = MemoryLogStore::new();
            let p = payload_of(sz);
            let mut i = 0u64;
            b.iter(|| {
                let entry = build_event_log_entry(i as i64, p.clone());
                rt.block_on(store.append(
                    black_box("bench.topic"),
                    black_box(entry),
                    black_box(RetentionPolicy::None),
                ));
                i += 1;
            });
        });
    }
    group.finish();
}

fn bench_log_range(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("storage/log_range");
    for &n in &[10usize, 100] {
        group.bench_with_input(BenchmarkId::new("count", n), &n, |b, &n| {
            let store = MemoryLogStore::new();
            for _i in 0..n {
                rt.block_on(store.append(
                    "bench.topic",
                    build_event_log_entry(n as i64, payload_of(64)),
                    RetentionPolicy::None,
                ));
            }
            b.iter(|| {
                rt.block_on(store.range(
                    black_box("bench.topic"),
                    black_box(0),
                    black_box(n as i64 - 1),
                ))
            });
        });
    }
    group.finish();
}

fn bench_log_latest(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("storage/log_latest");
    group.bench_function("some", |b| {
        let store = MemoryLogStore::new();
        rt.block_on(store.append(
            "bench.topic",
            build_event_log_entry(1, payload_of(1024)),
            RetentionPolicy::None,
        ));
        b.iter(|| rt.block_on(store.latest(black_box("bench.topic"))));
    });
    group.bench_function("none", |b| {
        let store = MemoryLogStore::new();
        b.iter(|| rt.block_on(store.latest(black_box("nonexistent.topic"))));
    });
    group.finish();
}

fn bench_dedupe_check(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("storage/dedupe_check");
    let window = Duration::from_secs(30);
    group.bench_function("fresh", |b| {
        let store = MemoryDedupeStore::new();
        let mut i = 0u64;
        b.iter(|| {
            i += 1;
            rt.block_on(store.check_and_record(
                black_box("bench.topic"),
                black_box(&format!("key-{i}")),
                black_box(window),
            ));
        });
    });
    group.bench_function("duplicate", |b| {
        let store = MemoryDedupeStore::new();
        let _ = rt.block_on(store.check_and_record("bench.topic", "key-0", window));
        b.iter(|| {
            rt.block_on(store.check_and_record(
                black_box("bench.topic"),
                black_box("key-0"),
                black_box(window),
            ))
        });
    });
    group.finish();
}

fn bench_dedupe_sweep(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("storage/dedupe_sweep");
    for &n in &[100usize, 1000] {
        group.bench_with_input(BenchmarkId::new("expired", n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let store = MemoryDedupeStore::new();
                    let short_window = Duration::from_millis(0);
                    for i in 0..n {
                        rt.block_on(store.check_and_record(
                            "bench.topic",
                            &format!("key-{i}"),
                            short_window,
                        ));
                    }
                    std::thread::sleep(Duration::from_millis(5));
                    store
                },
                |store| rt.block_on(store.sweep()),
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_snapshot_capture_get(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("storage/snapshot_capture_get");
    let store = TopicStore::new();
    let entry = store
        .get_or_create(
            "bench.topic",
            TopicProfile {
                snapshot_enabled: true,
                ..TopicProfile::default()
            },
        )
        .unwrap();
    entry.append(build_event_log_entry(1, payload_of(1024)));
    group.bench_function("capture", |b| {
        let snap_store = MemorySnapshotStore::new();
        b.iter(|| rt.block_on(snap_store.capture(black_box("bench.topic"), &store, None)));
    });
    let snap_store = MemorySnapshotStore::new();
    let _ = rt.block_on(snap_store.capture("bench.topic", &store, None));
    group.bench_function("get", |b| {
        b.iter(|| rt.block_on(snap_store.get(black_box("bench.topic"))));
    });
    group.finish();
}

fn bench_encode_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage/encode");
    group.bench_function("offset_key", |b| {
        b.iter(|| black_box(offset_key(black_box("orders/incoming"))));
    });
    group.bench_function("log_key", |b| {
        b.iter(|| black_box(log_key(black_box("orders/incoming"), black_box(42))));
    });
    group.bench_function("log_prefix", |b| {
        b.iter(|| black_box(log_prefix(black_box("orders/incoming"))));
    });
    group.bench_function("snapshot_key", |b| {
        b.iter(|| {
            black_box(snapshot_key(
                black_box("orders/incoming"),
                black_box("snap-42"),
            ))
        });
    });
    group.bench_function("snapshot_prefix", |b| {
        b.iter(|| black_box(snapshot_prefix(black_box("orders/incoming"))));
    });
    group.bench_function("dedupe_key", |b| {
        b.iter(|| {
            black_box(dedupe_key(
                black_box("orders/incoming"),
                black_box("msg-00000001"),
            ))
        });
    });
    group.bench_function("dedupe_prefix", |b| {
        b.iter(|| black_box(dedupe_prefix(black_box("orders/incoming"))));
    });
    group.bench_function("offset_prefix", |b| {
        b.iter(|| black_box(offset_prefix(black_box("orders/incoming"))));
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_offset_alloc,
    bench_offset_head,
    bench_offset_remove,
    bench_log_append,
    bench_log_range,
    bench_log_latest,
    bench_dedupe_check,
    bench_dedupe_sweep,
    bench_snapshot_capture_get,
    bench_encode_decode,
);
criterion_main!(benches);
