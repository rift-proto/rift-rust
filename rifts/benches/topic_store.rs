#![allow(
    dead_code,
    unused_imports,
    unused_variables,
    clippy::all,
    unused_must_use
)]
//! Topic store benchmarks — get_or_create, append, range, retention, stats.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rifts::topic::{RetentionPolicy, TopicProfile, TopicStore, validate_name};
use std::hint::black_box;

use crate::common::{TOPIC_COUNTS, build_event_log_entry, payload_of, topic_name, topic_names};

mod common;

fn bench_validate_name(c: &mut Criterion) {
    let mut group = c.benchmark_group("topic/validate_name");
    let valid = "bench.topic.0";
    let invalid = "$reserved";
    group.bench_function("valid", |b| {
        b.iter(|| black_box(validate_name(black_box(valid)).is_ok()));
    });
    group.bench_function("invalid", |b| {
        b.iter(|| black_box(validate_name(black_box(invalid)).is_err()));
    });
    group.finish();
}

fn bench_get_or_create(c: &mut Criterion) {
    let mut group = c.benchmark_group("topic/get_or_create");
    for &n in TOPIC_COUNTS {
        group.bench_with_input(BenchmarkId::new("cold", n), &n, |b, &n| {
            let store = TopicStore::new();
            let names = topic_names(n);
            let mut i = 0;
            b.iter(|| {
                let name = &names[i % n];
                let entry = store.get_or_create(black_box(name), TopicProfile::default());
                black_box(entry);
                i += 1;
            });
        });
    }
    group.bench_function("hot", |b| {
        let store = TopicStore::new();
        let _ = store.get_or_create("bench.topic", TopicProfile::default());
        b.iter(|| {
            black_box(store.get_or_create(black_box("bench.topic"), TopicProfile::default()))
        });
    });
    group.finish();
}

fn bench_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("topic/append");
    let payloads: &[(usize, &str)] = &[(0, "0B"), (1024, "1KiB"), (16384, "16KiB")];
    let retentions = [
        ("none", RetentionPolicy::None),
        ("latest", RetentionPolicy::Latest),
        ("count100", RetentionPolicy::Count(100)),
        ("size1m", RetentionPolicy::Size(1024 * 1024)),
    ];
    for &(sz, label) in payloads {
        let payload = payload_of(sz);
        for (rname, retention) in retentions {
            let mut profile = TopicProfile::default();
            profile.retention = retention;
            group.bench_with_input(
                BenchmarkId::new(format!("{rname}/{label}"), sz),
                &(payload.clone(), profile),
                |b, (payload, profile)| {
                    let store = TopicStore::new();
                    let entry = store
                        .get_or_create("bench.topic", profile.clone())
                        .expect("create");
                    let mut offset = 0i64;
                    b.iter(|| {
                        offset += 1;
                        let e = build_event_log_entry(offset, payload.clone());
                        entry.append(black_box(e));
                    });
                },
            );
        }
    }
    group.finish();
}

fn bench_range(c: &mut Criterion) {
    let mut group = c.benchmark_group("topic/range");
    let sizes: &[(usize, &str)] = &[(10, "10"), (100, "100"), (1000, "1000")];
    for &(n, label) in sizes {
        group.bench_with_input(BenchmarkId::new("full", label), &n, |b, &n| {
            let store = TopicStore::new();
            let mut profile = TopicProfile::default();
            profile.retention = RetentionPolicy::None;
            let entry = store.get_or_create("bench.topic", profile).expect("create");
            for i in 1..=n {
                entry.append(build_event_log_entry(i as i64, payload_of(64)));
            }
            b.iter(|| {
                let r = entry.range(black_box(1), black_box(n as i64));
                black_box(r);
            });
        });
    }
    group.finish();
}

fn bench_snapshot(c: &mut Criterion) {
    let mut group = c.benchmark_group("topic/snapshot");
    group.bench_function("some", |b| {
        let store = TopicStore::new();
        let entry = store
            .get_or_create("bench.topic", TopicProfile::default())
            .expect("create");
        entry.append(build_event_log_entry(1, payload_of(1024)));
        b.iter(|| black_box(entry.snapshot()));
    });
    group.bench_function("none", |b| {
        let store = TopicStore::new();
        let entry = store
            .get_or_create("bench.topic.empty", TopicProfile::default())
            .expect("create");
        b.iter(|| black_box(entry.snapshot()));
    });
    group.finish();
}

fn bench_stats(c: &mut Criterion) {
    let mut group = c.benchmark_group("topic/stats");
    for &n in TOPIC_COUNTS {
        group.bench_with_input(BenchmarkId::new("topics", n), &n, |b, &n| {
            let store = TopicStore::new();
            for i in 0..n {
                let entry = store
                    .get_or_create(&topic_name(i), TopicProfile::default())
                    .expect("create");
                entry.append(build_event_log_entry(1, payload_of(64)));
            }
            b.iter(|| black_box(store.stats()));
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_validate_name,
    bench_get_or_create,
    bench_append,
    bench_range,
    bench_snapshot,
    bench_stats,
);
criterion_main!(benches);
