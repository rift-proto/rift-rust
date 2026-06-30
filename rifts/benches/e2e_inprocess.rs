#![allow(
    dead_code,
    unused_imports,
    unused_variables,
    clippy::all,
    unused_must_use
)]
//! End-to-end in-process benchmarks — full broker publish→fanout→subscriber path.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rifts::broker::{Broker, SubscribeIntent};
use rifts::frame::Frame;
use std::hint::black_box;

use crate::common::{
    PAYLOAD_SIZE_LABELS, PAYLOAD_SIZES, SUBSCRIBER_COUNTS, atomic_sink, build_data_frame,
    default_broker, payload_of,
};

mod common;

fn bench_e2e_publish_deliver(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e/publish_deliver");
    let payloads: Vec<(String, Frame)> = PAYLOAD_SIZES
        .iter()
        .zip(PAYLOAD_SIZE_LABELS.iter())
        .map(|(&sz, &label)| (label.to_string(), build_data_frame(payload_of(sz), 1)))
        .collect();
    for (label, frame) in &payloads {
        for &n in &[1, 10, 100] {
            group.bench_with_input(
                BenchmarkId::new(format!("{label}/subs_{n}"), n),
                &(frame, n),
                |b, &(frame, n)| {
                    let broker = default_broker(30_000, 2 * 1024 * 1024).into_arc();
                    for i in 0..n {
                        let _ = broker.subscribe(
                            "bench.topic.0",
                            SubscribeIntent::Live,
                            atomic_sink(i as u64),
                        );
                    }
                    let mut frame_id = 1u64;
                    b.iter(|| {
                        let mut f = frame.clone();
                        f.frame_id = frame_id;
                        frame_id += 1;
                        f.message_id = Some(format!("msg-{frame_id}"));
                        black_box(broker.publish(black_box(&f)));
                    });
                },
            );
        }
    }
    group.finish();
}

fn bench_e2e_subscribe_publish_cycle(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e/subscribe_publish_cycle");
    let payload = payload_of(256);
    for &n in SUBSCRIBER_COUNTS {
        group.bench_with_input(BenchmarkId::new("cycle", n), &n, |b, &n| {
            b.iter_batched(
                || default_broker(30_000, 2 * 1024 * 1024).into_arc(),
                |broker| {
                    for i in 0..n {
                        let _ = broker.subscribe(
                            "bench.topic.0",
                            SubscribeIntent::Live,
                            atomic_sink(i as u64),
                        );
                    }
                    let f = build_data_frame(payload.clone(), 1);
                    let _ = broker.publish(&f);
                    black_box(broker);
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_e2e_replay_then_live(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e/replay_then_live");
    let payload = payload_of(256);
    for &n in &[10, 100, 1000] {
        group.bench_with_input(BenchmarkId::new("history", n), &n, |b, &n| {
            let broker = default_broker(30_000, 2 * 1024 * 1024).into_arc();
            for i in 1..=n {
                let f = build_data_frame(payload.clone(), i as u64);
                let _ = broker.publish(&f);
            }
            let sink = atomic_sink(0);
            let _ = broker.subscribe(
                "bench.topic.0",
                SubscribeIntent::Replay { from: 1 },
                sink.clone(),
            );
            b.iter(|| {
                let f = build_data_frame(payload.clone(), n as u64 + 1);
                black_box(broker.publish(black_box(&f)));
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_e2e_publish_deliver,
    bench_e2e_subscribe_publish_cycle,
    bench_e2e_replay_then_live,
);
criterion_main!(benches);
