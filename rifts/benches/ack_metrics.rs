//! Ack and metrics benchmarks — AckManager, Ack, AckStatus, Metrics.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rifts::Metrics;
use rifts::ack::{Ack, AckManager, AckStatus};
use std::hint::black_box;

mod common;

fn bench_ack_new(c: &mut Criterion) {
    let mut group = c.benchmark_group("ack/new");
    group.bench_function("received", |b| {
        b.iter(|| black_box(Ack::new(black_box("msg-1"), AckStatus::Received)));
    });
    group.bench_function("delivered", |b| {
        b.iter(|| black_box(Ack::new(black_box("msg-1"), AckStatus::Delivered)));
    });
    group.finish();
}

fn bench_ack_with_offset_reason(c: &mut Criterion) {
    c.bench_function("ack/with_offset_reason", |b| {
        b.iter(|| {
            black_box(
                Ack::new(black_box("msg-1"), AckStatus::Accepted)
                    .with_offset(black_box(42))
                    .with_reason(black_box("ok")),
            )
        });
    });
}

fn bench_ack_status_as_str(c: &mut Criterion) {
    let mut group = c.benchmark_group("ack/status_as_str");
    let statuses = [
        ("received", AckStatus::Received),
        ("accepted", AckStatus::Accepted),
        ("persisted", AckStatus::Persisted),
        ("delivered", AckStatus::Delivered),
        ("processed", AckStatus::Processed),
        ("rejected", AckStatus::Rejected),
        ("expired", AckStatus::Expired),
        ("duplicate", AckStatus::Duplicate),
        ("failed", AckStatus::Failed),
    ];
    for (name, status) in statuses {
        group.bench_function(name, |b| b.iter(|| black_box(status.as_str())));
    }
    group.finish();
}

fn bench_ack_track(c: &mut Criterion) {
    let mut group = c.benchmark_group("ack/track");
    group.bench_function("fresh", |b| {
        let mgr = AckManager::new();
        let mut i = 0u64;
        b.iter(|| {
            let msg = format!("msg-{i}");
            i += 1;
            mgr.track(
                black_box("session-1"),
                black_box(&msg),
                black_box(Duration::from_secs(30)),
            );
        });
    });
    group.bench_function("overwrite", |b| {
        let mgr = AckManager::new();
        mgr.track("session-1", "msg-1", Duration::from_secs(30));
        b.iter(|| {
            mgr.track(
                black_box("session-1"),
                black_box("msg-1"),
                black_box(Duration::from_secs(30)),
            );
        });
    });
    group.finish();
}

fn bench_ack_complete(c: &mut Criterion) {
    let mut group = c.benchmark_group("ack/complete");
    group.bench_function("hit", |b| {
        let mgr = AckManager::new();
        mgr.track("session-1", "msg-1", Duration::from_secs(30));
        b.iter(|| {
            black_box(mgr.complete(black_box("session-1"), black_box("msg-1")));
        });
    });
    group.bench_function("miss", |b| {
        let mgr = AckManager::new();
        b.iter(|| {
            black_box(mgr.complete(black_box("session-1"), black_box("nonexistent")));
        });
    });
    group.finish();
}

fn bench_ack_reap(c: &mut Criterion) {
    let mut group = c.benchmark_group("ack/reap");
    for &n in &[10usize, 100, 1000] {
        group.bench_with_input(BenchmarkId::new("outstanding", n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let mgr = AckManager::new();
                    let short = Duration::from_millis(1);
                    for i in 0..n {
                        mgr.track("session-1", &format!("msg-{i}"), short);
                    }
                    std::thread::sleep(Duration::from_millis(5));
                    mgr
                },
                |mgr| black_box(mgr.reap_timeouts(black_box("session-1"))),
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_ack_forget(c: &mut Criterion) {
    let mut group = c.benchmark_group("ack/forget");
    for &n in &[10usize, 100, 1000] {
        group.bench_with_input(BenchmarkId::new("session", n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let mgr = AckManager::new();
                    for i in 0..n {
                        mgr.track(&format!("session-{i}"), "msg-1", Duration::from_secs(30));
                    }
                    mgr
                },
                |mgr| {
                    for i in 0..n {
                        mgr.forget(black_box(&format!("session-{i}")));
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_metrics_inc(c: &mut Criterion) {
    c.bench_function("metrics/inc", |b| {
        let metrics = Metrics::new();
        let counter = &metrics.messages_in_total;
        b.iter(|| metrics.inc(black_box(counter)));
    });
}

fn bench_metrics_add(c: &mut Criterion) {
    let mut group = c.benchmark_group("metrics/add");
    for &n in &[1u64, 100, 10_000] {
        group.bench_with_input(BenchmarkId::new("n", n), &n, |b, &n| {
            let metrics = Metrics::new();
            let counter = &metrics.messages_out_total;
            b.iter(|| metrics.add(black_box(counter), black_box(n)));
        });
    }
    group.finish();
}

fn bench_metrics_multi_counter(c: &mut Criterion) {
    c.bench_function("metrics/multi_counter", |b| {
        let metrics = Arc::new(Metrics::new());
        let counters: [&AtomicU64; 5] = [
            &metrics.messages_in_total,
            &metrics.messages_out_total,
            &metrics.active_connections,
            &metrics.connection_open_total,
            &metrics.connection_close_total,
        ];
        b.iter(|| {
            for c in &counters {
                metrics.inc(black_box(c));
            }
        });
    });
}

criterion_group!(
    benches,
    bench_ack_new,
    bench_ack_with_offset_reason,
    bench_ack_status_as_str,
    bench_ack_track,
    bench_ack_complete,
    bench_ack_reap,
    bench_ack_forget,
    bench_metrics_inc,
    bench_metrics_add,
    bench_metrics_multi_counter,
);
criterion_main!(benches);
