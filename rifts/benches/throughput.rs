#![allow(dead_code, unused_imports, unused_variables, clippy::all)]
//! Throughput benchmarks — operations per second.
//!
//! Covers four dimensions:
//! 1. Serial batch publish — payload × batch-size, no subscribers
//! 2. Concurrent publish   — 1/4/16/64 publishers hammering the broker
//! 3. Fanout delivery      — N subscribers, measure publish+deliver throughput
//! 4. Subscribe churn      — subscribe/unsubscribe pairs per second
//!
//! Uses criterion's `Throughput::Elements` to report op/s directly.
//!
//! ```text
//! cargo bench --bench throughput
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rifts::broker::{Broker, SubscribeIntent};
use std::hint::black_box;

use common::{atomic_sink, build_data_frame, payload_of, runtime, topic_name};

mod common;

// ── helpers ────────────────────────────────────────────────────────────

fn fresh_frame_id_counter() -> Arc<AtomicU64> {
    Arc::new(AtomicU64::new(0))
}

// ── 1. serial batch publish ────────────────────────────────────────────

fn bench_serial_batch_publish(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("throughput/serial_batch_publish");

    let payloads: &[(usize, &str)] = &[
        (1024, "1KiB"),
        (4096, "4KiB"),
        (16384, "16KiB"),
        (65536, "64KiB"),
        (1048576, "1MiB"),
    ];
    let batch_sizes: &[(usize, &str)] = &[(100, "x100"), (1000, "x1000"), (10000, "x10k")];

    for &(psz, plabel) in payloads {
        for &(batch, blabel) in batch_sizes {
            let payload = payload_of(psz);
            let total_bytes = (psz * batch) as u64;
            group.throughput(Throughput::Bytes(total_bytes));
            group.bench_with_input(
                BenchmarkId::new(format!("{plabel}/{blabel}"), blabel),
                &(payload, batch),
                |b, &(ref payload, batch)| {
                    b.iter_batched(
                        || {
                            let broker =
                                common::default_broker(30_000, 16 * 1024 * 1024).into_arc();
                            let frame_id = fresh_frame_id_counter();
                            (broker, build_data_frame(payload.clone(), 0), frame_id)
                        },
                        |(broker, mut frame, frame_id)| {
                            rt.block_on(async {
                                for i in 0..batch {
                                    frame.frame_id = frame_id.fetch_add(1, Ordering::Relaxed);
                                    frame.message_id = Some(format!("s-{i}"));
                                    let _ = broker.publish(black_box(&frame)).await;
                                }
                                black_box(());
                            });
                        },
                        criterion::BatchSize::SmallInput,
                    );
                },
            );
        }
    }
    group.finish();
}

// ── 2. concurrent publish ──────────────────────────────────────────────

fn bench_concurrent_publish(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("throughput/concurrent_publish");

    let levels: &[usize] = &[1, 4, 16, 64];
    let msgs_per_task: usize = 1000;
    let base_frame = build_data_frame(payload_of(1024), 0);

    for &n in levels {
        let total_bytes = (n * msgs_per_task * 1024) as u64;
        group.throughput(Throughput::Bytes(total_bytes));
        group.bench_with_input(BenchmarkId::new("publishers", n), &n, |b, &n| {
            b.iter_batched(
                || common::default_broker(30_000, 16 * 1024 * 1024).into_arc(),
                |broker| {
                    rt.block_on(async {
                        let mut handles = Vec::with_capacity(n);
                        for task_id in 0..n {
                            let broker = broker.clone();
                            let mut frame = base_frame.clone();
                            handles.push(tokio::spawn(async move {
                                for i in 0..msgs_per_task {
                                    frame.frame_id = ((task_id as u64) << 32) | (i as u64);
                                    frame.message_id = Some(format!("c{task_id}-{i}"));
                                    let _ = broker.publish(black_box(&frame)).await;
                                }
                            }));
                        }
                        for h in handles {
                            let _ = h.await;
                        }
                        black_box(());
                    });
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

// ── 3. fanout delivery throughput ──────────────────────────────────────

fn bench_fanout_throughput(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("throughput/fanout");

    let subscriber_counts: &[usize] = &[10, 100, 1000];
    let msgs: usize = 500;
    let payload = payload_of(1024);

    for &n_sub in subscriber_counts {
        let total_bytes = (msgs * 1024) as u64;
        group.throughput(Throughput::Bytes(total_bytes));
        group.bench_with_input(
            BenchmarkId::new("subscribers", n_sub),
            &n_sub,
            |b, &n_sub| {
                b.iter_batched(
                    || {
                        let broker = common::default_broker(30_000, 16 * 1024 * 1024).into_arc();
                        let topic = topic_name(0);
                        rt.block_on(async {
                            for i in 0..n_sub {
                                let _ = broker
                                    .subscribe(&topic, SubscribeIntent::Live, atomic_sink(i as u64))
                                    .await;
                            }
                        });
                        (broker, topic, build_data_frame(payload.clone(), 0))
                    },
                    |(broker, _topic, mut frame)| {
                        rt.block_on(async {
                            for i in 0..msgs {
                                frame.frame_id = i as u64;
                                frame.message_id = Some(format!("f-{i}"));
                                let _ = broker.publish(black_box(&frame)).await;
                            }
                            black_box(());
                        });
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

// ── 4. subscribe churn ─────────────────────────────────────────────────

fn bench_subscribe_churn(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("throughput/subscribe_churn");

    let churn_levels: &[usize] = &[100, 1000, 10000];

    for &n in churn_levels {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("pairs", n), &n, |b, &n| {
            b.iter_batched(
                || common::default_broker(30_000, 16 * 1024 * 1024).into_arc(),
                |broker| {
                    rt.block_on(async {
                        for i in 0..n {
                            let sink = atomic_sink(i as u64);
                            let sid = broker
                                .subscribe("bench.topic", SubscribeIntent::Live, sink)
                                .await
                                .unwrap();
                            let _ = broker.unsubscribe(sid).await;
                        }
                        black_box(());
                    });
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_serial_batch_publish,
    bench_concurrent_publish,
    bench_fanout_throughput,
    bench_subscribe_churn,
);
criterion_main!(benches);
