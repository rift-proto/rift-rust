#![allow(dead_code, unused_imports, unused_variables, clippy::all)]
//! Session resume benchmarks — SessionStore, OffsetTracker, ResumeManager, decide.

use std::collections::HashMap;
use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rifts::session::offset_tracker::decide;
use rifts::session::{ClientId, OffsetTracker, ResumeManager, Session, SessionId, SessionStore};
use rifts::topic::{TopicProfile, TopicStore};
use std::hint::black_box;

use crate::common::{TOPIC_COUNTS, topic_names};

mod common;

fn make_session() -> Arc<Session> {
    Arc::new(Session::new(
        SessionId::new(),
        ClientId::new("bench-client"),
    ))
}

fn bench_session_store_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("session/store_insert");
    for &n in TOPIC_COUNTS {
        group.bench_with_input(BenchmarkId::new("sessions", n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let store = SessionStore::new();
                    let sessions: Vec<Arc<Session>> = (0..n).map(|_| make_session()).collect();
                    (store, sessions)
                },
                |(store, sessions)| {
                    for s in sessions {
                        let id = s.id.as_str().to_string();
                        store.insert(s);
                        black_box(id);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_session_store_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("session/store_get");
    group.bench_function("hit", |b| {
        let store = SessionStore::new();
        let session = make_session();
        let id = session.id.as_str().to_string();
        store.insert(session);
        b.iter(|| black_box(store.get(black_box(&id))));
    });
    group.bench_function("miss", |b| {
        let store = SessionStore::new();
        b.iter(|| black_box(store.get(black_box("nonexistent"))));
    });
    group.finish();
}

fn bench_session_store_remove(c: &mut Criterion) {
    let mut group = c.benchmark_group("session/store_remove");
    for &n in TOPIC_COUNTS {
        group.bench_with_input(BenchmarkId::new("sessions", n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let store = SessionStore::new();
                    let ids: Vec<String> = (0..n)
                        .map(|_i| {
                            let s = make_session();
                            let id = s.id.as_str().to_string();
                            store.insert(s);
                            id
                        })
                        .collect();
                    (store, ids)
                },
                |(store, ids)| {
                    for id in &ids {
                        store.remove(black_box(id));
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_offset_tracker_record(c: &mut Criterion) {
    let mut group = c.benchmark_group("session/offset_record");
    for &n in TOPIC_COUNTS {
        group.bench_with_input(BenchmarkId::new("topics", n), &n, |b, &n| {
            let tracker = OffsetTracker::new();
            let session = SessionId::new();
            let topics = topic_names(n);
            let mut i = 0i64;
            b.iter(|| {
                let topic = &topics[(i as usize) % n];
                tracker.record(black_box(&session), black_box(topic), black_box(i));
                i += 1;
            });
        });
    }
    group.finish();
}

fn bench_offset_tracker_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("session/offset_get");
    group.bench_function("hit", |b| {
        let tracker = OffsetTracker::new();
        let session = SessionId::new();
        tracker.record(&session, "bench.topic", 42);
        b.iter(|| black_box(tracker.get(black_box(&session), black_box("bench.topic"))));
    });
    group.bench_function("miss", |b| {
        let tracker = OffsetTracker::new();
        let session = SessionId::new();
        b.iter(|| black_box(tracker.get(black_box(&session), black_box("nonexistent"))));
    });
    group.finish();
}

fn bench_offset_tracker_snapshot(c: &mut Criterion) {
    let mut group = c.benchmark_group("session/offset_snapshot");
    for &n in TOPIC_COUNTS {
        group.bench_with_input(BenchmarkId::new("topics", n), &n, |b, &n| {
            let tracker = OffsetTracker::new();
            let session = SessionId::new();
            let topics = topic_names(n);
            for (i, t) in topics.iter().enumerate() {
                tracker.record(&session, t, i as i64);
            }
            b.iter(|| black_box(tracker.snapshot(black_box(&session))));
        });
    }
    group.finish();
}

fn bench_decide(c: &mut Criterion) {
    let mut group = c.benchmark_group("session/decide");
    group.bench_function("cold_start", |b| {
        let last = HashMap::new();
        let topic = HashMap::new();
        b.iter(|| black_box(decide(black_box(&last), black_box(&topic))));
    });
    group.bench_function("full_resume", |b| {
        let mut last = HashMap::new();
        let mut topic = HashMap::new();
        last.insert("t1".to_string(), 10);
        topic.insert("t1".to_string(), 10);
        b.iter(|| black_box(decide(black_box(&last), black_box(&topic))));
    });
    group.bench_function("replaying", |b| {
        let mut last = HashMap::new();
        let mut topic = HashMap::new();
        last.insert("t1".to_string(), 9);
        topic.insert("t1".to_string(), 10);
        b.iter(|| black_box(decide(black_box(&last), black_box(&topic))));
    });
    group.bench_function("partial", |b| {
        let mut last = HashMap::new();
        let mut topic = HashMap::new();
        last.insert("t1".to_string(), 5);
        topic.insert("t1".to_string(), 10);
        b.iter(|| black_box(decide(black_box(&last), black_box(&topic))));
    });
    group.bench_function("snapshot_required", |b| {
        let mut last = HashMap::new();
        let topic = HashMap::new();
        last.insert("t1".to_string(), 10);
        b.iter(|| black_box(decide(black_box(&last), black_box(&topic))));
    });
    group.bench_function("rejected", |b| {
        let mut last = HashMap::new();
        let mut topic = HashMap::new();
        last.insert("t1".to_string(), 15);
        topic.insert("t1".to_string(), 10);
        b.iter(|| black_box(decide(black_box(&last), black_box(&topic))));
    });
    group.finish();
}

fn bench_resume_evaluate(c: &mut Criterion) {
    let mut group = c.benchmark_group("session/resume_evaluate");
    for &n in &[1, 10, 100] {
        group.bench_with_input(BenchmarkId::new("topics", n), &n, |b, &n| {
            let rm = ResumeManager::new();
            let session = make_session();
            let mut last = HashMap::new();
            let mut topic = HashMap::new();
            let topics = topic_names(n);
            for (i, t) in topics.iter().enumerate() {
                last.insert(t.clone(), i as i64);
                topic.insert(t.clone(), i as i64);
            }
            let epoch = session.current_epoch();
            b.iter(|| {
                black_box(rm.evaluate(black_box(&session), black_box(&last), black_box(&topic)))
            });
        });
    }
    group.finish();
}

fn bench_resume_topic_offsets(c: &mut Criterion) {
    let mut group = c.benchmark_group("session/resume_topic_offsets");
    for &n in TOPIC_COUNTS {
        group.bench_with_input(BenchmarkId::new("topics", n), &n, |b, &n| {
            let rm = ResumeManager::new();
            let store = TopicStore::new();
            let topics = topic_names(n);
            for t in &topics {
                let _ = store.get_or_create(t, TopicProfile::default());
            }
            b.iter(|| black_box(rm.topic_offsets(black_box(&store), black_box(&topics))));
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_session_store_insert,
    bench_session_store_get,
    bench_session_store_remove,
    bench_offset_tracker_record,
    bench_offset_tracker_get,
    bench_offset_tracker_snapshot,
    bench_decide,
    bench_resume_evaluate,
    bench_resume_topic_offsets,
);
criterion_main!(benches);
