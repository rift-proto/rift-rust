#![allow(
    dead_code,
    unused_imports,
    unused_variables,
    clippy::all,
    unused_must_use
)]
//! In-memory broker benchmarks — publish, dedupe, replay, subscribe, snapshot.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rifts::broker::{Broker, SubscribeIntent};
use rifts::frame::Frame;
use std::hint::black_box;

use crate::common::{
    PAYLOAD_SIZE_LABELS, PAYLOAD_SIZES, SUBSCRIBER_COUNTS, atomic_sink, build_data_frame,
    default_broker, payload_of,
};

mod common;

fn bench_publish_no_subs(c: &mut Criterion) {
    let mut group = c.benchmark_group("broker_memory/publish_no_subs");
    let payloads: Vec<(String, Frame)> = PAYLOAD_SIZES
        .iter()
        .zip(PAYLOAD_SIZE_LABELS.iter())
        .map(|(&sz, &label)| {
            let frame = build_data_frame(payload_of(sz), 1);
            (label.to_string(), frame)
        })
        .collect();
    for (label, frame) in &payloads {
        group.bench_with_input(BenchmarkId::new("payload", label), frame, |b, frame| {
            let broker = default_broker(30_000, 2 * 1024 * 1024).into_arc();
            let _ = broker.publish(&build_data_frame(payload_of(64), 0));
            let mut frame_id = 1u64;
            b.iter(|| {
                let mut f = frame.clone();
                f.frame_id = frame_id;
                frame_id += 1;
                f.message_id = Some(format!("msg-{frame_id}"));
                black_box(broker.publish(black_box(&f)));
            });
        });
    }
    group.finish();
}

fn bench_publish_with_subs(c: &mut Criterion) {
    let mut group = c.benchmark_group("broker_memory/publish_with_subs");
    let payload = payload_of(1024);
    for &n in SUBSCRIBER_COUNTS {
        group.bench_with_input(BenchmarkId::new("subscribers", n), &n, |b, &n| {
            let broker = default_broker(30_000, 2 * 1024 * 1024);
            let arc_broker = broker.into_arc();
            let _ = arc_broker.subscribe("bench.topic.0", SubscribeIntent::Live, atomic_sink(0));
            for i in 1..n {
                let _ = arc_broker.subscribe(
                    "bench.topic.0",
                    SubscribeIntent::Live,
                    atomic_sink(i as u64),
                );
            }
            let mut frame_id = 1u64;
            b.iter(|| {
                let f = build_data_frame(payload.clone(), frame_id);
                frame_id += 1;
                black_box(arc_broker.publish(black_box(&f)));
            });
        });
    }
    group.finish();
}

fn bench_publish_dedupe(c: &mut Criterion) {
    let mut group = c.benchmark_group("broker_memory/publish_dedupe");
    group.bench_function("duplicate", |b| {
        let broker = default_broker(30_000, 2 * 1024 * 1024).into_arc();
        let frame = build_data_frame(payload_of(1024), 1);
        let _ = broker.publish(&frame);
        b.iter(|| black_box(broker.publish(black_box(&frame))));
    });
    group.bench_function("fresh", |b| {
        let broker = default_broker(30_000, 2 * 1024 * 1024).into_arc();
        let mut frame_id = 1u64;
        b.iter(|| {
            let f = build_data_frame(payload_of(1024), frame_id);
            frame_id += 1;
            black_box(broker.publish(black_box(&f)));
        });
    });
    group.finish();
}

fn bench_replay(c: &mut Criterion) {
    let mut group = c.benchmark_group("broker_memory/replay");
    let sizes: &[(usize, &str)] = &[(10, "10"), (100, "100"), (1000, "1000")];
    for &(n, label) in sizes {
        group.bench_with_input(BenchmarkId::new("range", label), &n, |b, &n| {
            let broker = default_broker(30_000, 2 * 1024 * 1024).into_arc();
            for i in 1..=n {
                let f = build_data_frame(payload_of(64), i as u64);
                let _ = broker.publish(&f);
            }
            b.iter(|| {
                let r = broker.replay(
                    black_box("bench.topic.0"),
                    black_box(1),
                    black_box(n as i64),
                );
                black_box(r);
            });
        });
    }
    group.finish();
}

fn bench_snapshot(c: &mut Criterion) {
    let mut group = c.benchmark_group("broker_memory/snapshot");
    group.bench_function("some", |b| {
        let broker = default_broker(30_000, 2 * 1024 * 1024).into_arc();
        let _ = broker.publish(&build_data_frame(payload_of(1024), 1));
        b.iter(|| black_box(broker.snapshot(black_box("bench.topic.0"))));
    });
    group.bench_function("none", |b| {
        let broker = default_broker(30_000, 2 * 1024 * 1024).into_arc();
        b.iter(|| black_box(broker.snapshot(black_box("nonexistent.topic"))));
    });
    group.finish();
}

fn bench_subscriber_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("broker_memory/subscriber_count");
    for &n in SUBSCRIBER_COUNTS {
        group.bench_with_input(BenchmarkId::new("subs", n), &n, |b, &n| {
            let broker = default_broker(30_000, 2 * 1024 * 1024).into_arc();
            for i in 0..n {
                let _ = broker.subscribe(
                    "bench.topic.0",
                    SubscribeIntent::Live,
                    atomic_sink(i as u64),
                );
            }
            b.iter(|| black_box(broker.subscriber_count(black_box("bench.topic.0"))));
        });
    }
    group.finish();
}

fn bench_head_offset(c: &mut Criterion) {
    let mut group = c.benchmark_group("broker_memory/head_offset");
    group.bench_function("some", |b| {
        let broker = default_broker(30_000, 2 * 1024 * 1024).into_arc();
        let _ = broker.publish(&build_data_frame(payload_of(64), 1));
        b.iter(|| black_box(broker.head_offset(black_box("bench.topic.0"))));
    });
    group.bench_function("none", |b| {
        let broker = default_broker(30_000, 2 * 1024 * 1024).into_arc();
        b.iter(|| black_box(broker.head_offset(black_box("nonexistent.topic"))));
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_publish_no_subs,
    bench_publish_with_subs,
    bench_publish_dedupe,
    bench_replay,
    bench_snapshot,
    bench_subscriber_count,
    bench_head_offset,
);
criterion_main!(benches);
