#![allow(dead_code, unused_imports, unused_variables, clippy::all)]
//! Fanout engine benchmarks — subscribe, deliver, unsubscribe, drop_sink.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rifts::broker::{FanoutEngine, FanoutSink, SubscribeIntent, serialize_frame_for_fanout};
use std::hint::black_box;

use crate::common::{
    SUBSCRIBER_COUNTS, atomic_sink, build_data_frame, payload_of, subscribe_n_sinks,
};

mod common;

fn bench_subscribe(c: &mut Criterion) {
    let mut group = c.benchmark_group("fanout/subscribe");
    for &n in SUBSCRIBER_COUNTS {
        group.bench_with_input(BenchmarkId::new("live", n), &n, |b, &n| {
            b.iter_batched(
                FanoutEngine::new,
                |engine| {
                    for i in 0..n {
                        black_box(engine.subscribe(
                            black_box("bench.topic"),
                            SubscribeIntent::Live,
                            atomic_sink(i as u64),
                        ));
                    }
                    black_box(engine);
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_deliver(c: &mut Criterion) {
    let payload = payload_of(1024);
    let frame = build_data_frame(payload, 1);
    let bytes = serialize_frame_for_fanout(&frame, 1);
    let mut group = c.benchmark_group("fanout/deliver");
    for &n in SUBSCRIBER_COUNTS {
        group.bench_with_input(BenchmarkId::new("subscribers", n), &n, |b, &n| {
            let engine = FanoutEngine::new();
            subscribe_n_sinks(&engine, "bench.topic", n);
            b.iter(|| {
                let count = engine.deliver(black_box("bench.topic"), black_box(bytes.clone()));
                black_box(count);
            });
        });
    }
    group.finish();
}

fn bench_unsubscribe(c: &mut Criterion) {
    let mut group = c.benchmark_group("fanout/unsubscribe");
    for &n in SUBSCRIBER_COUNTS {
        group.bench_with_input(BenchmarkId::new("single", n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let engine = FanoutEngine::new();
                    let ids = subscribe_n_sinks(&engine, "bench.topic", n);
                    (engine, ids)
                },
                |(engine, ids)| {
                    if let Some(&id) = ids.first() {
                        black_box(engine.unsubscribe(black_box(id)));
                    }
                    black_box(engine);
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_drop_sink(c: &mut Criterion) {
    let mut group = c.benchmark_group("fanout/drop_sink");
    for &n in &[10usize, 100, 1000] {
        group.bench_with_input(BenchmarkId::new("sink", n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let engine = FanoutEngine::new();
                    let sink_id = 42;
                    for _ in 0..n {
                        let _ = engine.subscribe(
                            "bench.topic",
                            SubscribeIntent::Live,
                            atomic_sink(sink_id),
                        );
                    }
                    engine
                },
                |engine| {
                    black_box(engine.drop_sink(black_box(42)));
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_subscription_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("fanout/subscription_count");
    for &n in SUBSCRIBER_COUNTS {
        group.bench_with_input(BenchmarkId::new("count", n), &n, |b, &n| {
            let engine = FanoutEngine::new();
            subscribe_n_sinks(&engine, "bench.topic", n);
            b.iter(|| black_box(engine.subscription_count()));
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_subscribe,
    bench_deliver,
    bench_unsubscribe,
    bench_drop_sink,
    bench_subscription_count,
);
criterion_main!(benches);
