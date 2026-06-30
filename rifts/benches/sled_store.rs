#![allow(dead_code, unused_imports, unused_variables, clippy::all)]
//! Sled-backed storage benchmarks (feature `sled`).
//!
//! Uses `tempfile` to isolate sled data per benchmark iteration.

use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rifts::storage::engine::SledEngine;
use rifts::storage::{
    DedupeStore, LogStore, OffsetStore, SledDedupeStore, SledLogStore, SledOffsetStore,
    SledSnapshotStore, SnapshotStore,
};
use rifts::topic::{RetentionPolicy, TopicProfile, TopicStore};
use std::hint::black_box;

use crate::common::{build_event_log_entry, payload_of, runtime};

mod common;

fn temp_db() -> sled::Db {
    let dir = tempfile::tempdir().expect("tempdir");
    sled::Config::default()
        .path(dir.path())
        .temporary(true)
        .open()
        .expect("sled open")
}

fn temp_engine(prefix: &str) -> (SledEngine, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = sled::Config::default()
        .path(dir.path())
        .temporary(true)
        .open()
        .expect("sled open");
    let tree = db.open_tree(prefix).expect("tree");
    (SledEngine::new(tree), dir)
}

fn bench_sled_offset_alloc(c: &mut Criterion) {
    let mut group = c.benchmark_group("sled/offset_alloc");
    group.bench_function("same_topic", |b| {
        let rt = runtime();
        let (engine, _dir) = temp_engine("offset");
        let store = SledOffsetStore::new(engine);
        b.iter(|| black_box(rt.block_on(store.alloc(black_box("bench.topic")))));
    });
    group.bench_function("many_topics", |b| {
        let rt = runtime();
        let (engine, _dir) = temp_engine("offset");
        let store = SledOffsetStore::new(engine);
        let topics: Vec<String> = (0..100).map(|i| format!("topic-{i}")).collect();
        let mut i = 0;
        b.iter(|| {
            let t = &topics[i % 100];
            black_box(rt.block_on(store.alloc(black_box(t))));
            i += 1;
        });
    });
    group.finish();
}

fn bench_sled_offset_head(c: &mut Criterion) {
    let mut group = c.benchmark_group("sled/offset_head");
    group.bench_function("hot", |b| {
        let rt = runtime();
        let (engine, _dir) = temp_engine("offset");
        let store = SledOffsetStore::new(engine);
        let _ = rt.block_on(store.alloc("bench.topic"));
        b.iter(|| black_box(rt.block_on(store.head(black_box("bench.topic")))));
    });
    group.bench_function("cold", |b| {
        let rt = runtime();
        let (engine, _dir) = temp_engine("offset");
        let store = SledOffsetStore::new(engine);
        b.iter(|| black_box(rt.block_on(store.head(black_box("nonexistent")))));
    });
    group.finish();
}

fn bench_sled_log_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("sled/log_append");
    let payloads: &[(usize, &str)] = &[(0, "0B"), (1024, "1KiB"), (16384, "16KiB")];
    let retentions = [
        ("none", RetentionPolicy::None),
        ("latest", RetentionPolicy::Latest),
        ("count100", RetentionPolicy::Count(100)),
    ];
    for &(sz, label) in payloads {
        let payload = payload_of(sz);
        for (rname, retention) in retentions {
            group.bench_with_input(
                BenchmarkId::new(format!("{rname}/{label}"), sz),
                &payload,
                |b, payload| {
                    let rt = runtime();
                    let (engine, _dir) = temp_engine("log");
                    let store = SledLogStore::new(engine);
                    let mut offset = 0i64;
                    b.iter(|| {
                        offset += 1;
                        let e = build_event_log_entry(offset, payload.clone());
                        black_box(rt.block_on(store.append(
                            black_box("bench.topic"),
                            black_box(e),
                            black_box(retention),
                        )));
                    });
                },
            );
        }
    }
    group.finish();
}

fn bench_sled_log_range(c: &mut Criterion) {
    let mut group = c.benchmark_group("sled/log_range");
    let sizes: &[(usize, &str)] = &[(10, "10"), (100, "100")];
    for &(n, label) in sizes {
        group.bench_with_input(BenchmarkId::new("full", label), &n, |b, &n| {
            let rt = runtime();
            let (engine, _dir) = temp_engine("log");
            let store = SledLogStore::new(engine);
            for i in 1..=n {
                let e = build_event_log_entry(i as i64, payload_of(64));
                rt.block_on(store.append("bench.topic", e, RetentionPolicy::None));
            }
            b.iter(|| {
                let r = rt.block_on(store.range(
                    black_box("bench.topic"),
                    black_box(1),
                    black_box(n as i64),
                ));
                black_box(r);
            });
        });
    }
    group.finish();
}

fn bench_sled_log_latest(c: &mut Criterion) {
    let mut group = c.benchmark_group("sled/log_latest");
    group.bench_function("some", |b| {
        let rt = runtime();
        let (engine, _dir) = temp_engine("log");
        let store = SledLogStore::new(engine);
        rt.block_on(store.append(
            "bench.topic",
            build_event_log_entry(1, payload_of(1024)),
            RetentionPolicy::None,
        ));
        b.iter(|| black_box(rt.block_on(store.latest(black_box("bench.topic")))));
    });
    group.bench_function("none", |b| {
        let rt = runtime();
        let (engine, _dir) = temp_engine("log");
        let store = SledLogStore::new(engine);
        b.iter(|| black_box(rt.block_on(store.latest(black_box("nonexistent")))));
    });
    group.finish();
}

fn bench_sled_dedupe_check(c: &mut Criterion) {
    let mut group = c.benchmark_group("sled/dedupe_check");
    let window = Duration::from_secs(30);
    group.bench_function("fresh", |b| {
        let rt = runtime();
        let (engine, _dir) = temp_engine("dedupe");
        let store = SledDedupeStore::new(engine);
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("key-{i}");
            i += 1;
            black_box(rt.block_on(store.check_and_record(
                black_box("bench.topic"),
                black_box(&key),
                black_box(window),
            )));
        });
    });
    group.bench_function("duplicate", |b| {
        let rt = runtime();
        let (engine, _dir) = temp_engine("dedupe");
        let store = SledDedupeStore::new(engine);
        let _ = rt.block_on(store.check_and_record("bench.topic", "key-0", window));
        b.iter(|| {
            black_box(rt.block_on(store.check_and_record(
                black_box("bench.topic"),
                black_box("key-0"),
                black_box(window),
            )));
        });
    });
    group.finish();
}

fn bench_sled_snapshot(c: &mut Criterion) {
    let mut group = c.benchmark_group("sled/snapshot");
    group.bench_function("capture", |b| {
        let rt = runtime();
        let (engine, _dir) = temp_engine("snapshot");
        let store = SledSnapshotStore::new(engine);
        let topic_store = TopicStore::new();
        let entry = topic_store
            .get_or_create("bench.topic", TopicProfile::default())
            .expect("create");
        entry.append(build_event_log_entry(1, payload_of(1024)));
        b.iter(|| {
            black_box(rt.block_on(store.capture(
                black_box("bench.topic"),
                black_box(&topic_store),
                black_box(None),
            )));
        });
    });
    group.bench_function("get_some", |b| {
        let rt = runtime();
        let (engine, _dir) = temp_engine("snapshot");
        let store = SledSnapshotStore::new(engine);
        let topic_store = TopicStore::new();
        let entry = topic_store
            .get_or_create("bench.topic", TopicProfile::default())
            .expect("create");
        entry.append(build_event_log_entry(1, payload_of(1024)));
        let _ = rt.block_on(store.capture("bench.topic", &topic_store, None));
        b.iter(|| black_box(rt.block_on(store.get(black_box("bench.topic")))));
    });
    group.bench_function("get_none", |b| {
        let rt = runtime();
        let (engine, _dir) = temp_engine("snapshot");
        let store = SledSnapshotStore::new(engine);
        b.iter(|| black_box(rt.block_on(store.get(black_box("nonexistent")))));
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_sled_offset_alloc,
    bench_sled_offset_head,
    bench_sled_log_append,
    bench_sled_log_range,
    bench_sled_log_latest,
    bench_sled_dedupe_check,
    bench_sled_snapshot,
);
criterion_main!(benches);
