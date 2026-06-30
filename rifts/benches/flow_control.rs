//! Flow control benchmarks — backpressure controller.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rifts::flow::{BackpressureAction, BackpressureController, BackpressureStrategy};
use std::hint::black_box;

mod common;

fn bench_bp_enqueue_accept(c: &mut Criterion) {
    let mut group = c.benchmark_group("flow/backpressure_enqueue");
    let capacities: &[(usize, &str)] = &[(1024, "1KiB"), (65536, "64KiB"), (1048576, "1MiB")];
    for &(cap, label) in capacities {
        group.bench_with_input(BenchmarkId::new("accept", label), &cap, |b, &cap| {
            let bp = BackpressureController::new(cap);
            let chunk = cap / 4;
            b.iter(|| {
                let act = bp.try_enqueue(black_box(chunk));
                if let BackpressureAction::Accept = act {
                    bp.release(black_box(chunk));
                }
                black_box(act);
            });
        });
    }
    group.finish();
}

fn bench_bp_overloaded(c: &mut Criterion) {
    let mut group = c.benchmark_group("flow/backpressure_overloaded");
    let strategies = [
        ("pause", BackpressureStrategy::Pause),
        ("drop_volatile", BackpressureStrategy::DropVolatile),
        ("coalesce", BackpressureStrategy::CoalesceState),
        ("downgrade", BackpressureStrategy::Downgrade),
        ("disconnect", BackpressureStrategy::Disconnect),
        ("snapshot", BackpressureStrategy::SnapshotLater),
    ];
    for (name, strat) in strategies {
        group.bench_with_input(BenchmarkId::new("full", name), &strat, |b, &strat| {
            let bp = BackpressureController::new(1024);
            bp.set_strategy(strat);
            // Fill the queue so the next enqueue hits the slow path.
            let _ = bp.try_enqueue(1024);
            b.iter(|| {
                let act = bp.try_enqueue(black_box(16));
                black_box(act);
            });
        });
    }
    group.finish();
}

fn bench_bp_release(c: &mut Criterion) {
    let mut group = c.benchmark_group("flow/backpressure_release");
    group.bench_function("release", |b| {
        let bp = BackpressureController::new(65536);
        for _ in 0..32 {
            let _ = bp.try_enqueue(1024);
        }
        b.iter(|| {
            bp.release(black_box(1024));
        });
    });
    group.finish();
}

fn bench_bp_counters(c: &mut Criterion) {
    let mut group = c.benchmark_group("flow/backpressure_counters");
    group.bench_function("current_bytes", |b| {
        let bp = BackpressureController::new(65536);
        let _ = bp.try_enqueue(1024);
        b.iter(|| black_box(bp.current_bytes()));
    });
    group.bench_function("is_overloaded", |b| {
        let bp = BackpressureController::new(65536);
        let _ = bp.try_enqueue(65536);
        b.iter(|| black_box(bp.is_overloaded()));
    });
    group.bench_function("available", |b| {
        let bp = BackpressureController::new(65536);
        b.iter(|| black_box(bp.available()));
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_bp_enqueue_accept,
    bench_bp_overloaded,
    bench_bp_release,
    bench_bp_counters,
);
criterion_main!(benches);
