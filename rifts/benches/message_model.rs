//! Message semantic layer benchmarks — Event encode/decode, class, delivery mode.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rifts::message::event::{Event, decode_event_body, encode_event_body};
use rifts::{Message, MessageClass, SubscribeIntent};
use std::hint::black_box;

mod common;

fn make_event(payload_size: usize) -> Event {
    let payload = serde_json::json!({
        "body": "x".repeat(payload_size),
        "seq": 42u64,
    });
    Event::new(
        "bench.event.created",
        "msg-0001",
        "bench.event.created@1.0",
        payload,
    )
}

fn bench_event_encode(c: &mut Criterion) {
    let sizes: &[(usize, &str)] = &[(0, "0B"), (64, "64B"), (1024, "1KiB"), (16384, "16KiB")];
    let mut group = c.benchmark_group("message/event_encode");
    for &(sz, label) in sizes {
        let e = make_event(sz);
        group.bench_with_input(BenchmarkId::new("encode", label), &e, |b, e| {
            b.iter(|| encode_event_body(black_box(e)).expect("enc"));
        });
    }
    group.finish();
}

fn bench_event_decode(c: &mut Criterion) {
    let sizes: &[(usize, &str)] = &[(0, "0B"), (64, "64B"), (1024, "1KiB"), (16384, "16KiB")];
    let mut group = c.benchmark_group("message/event_decode");
    for &(sz, label) in sizes {
        let e = make_event(sz);
        let bytes = encode_event_body(&e).expect("enc");
        group.throughput(criterion::Throughput::Bytes(bytes.len() as u64));
        group.bench_with_input(BenchmarkId::new("decode", label), &bytes, |b, buf| {
            b.iter(|| decode_event_body(black_box(buf)).expect("dec"));
        });
    }
    group.finish();
}

fn bench_event_size_hint(c: &mut Criterion) {
    let sizes: &[(usize, &str)] = &[(0, "0B"), (1024, "1KiB"), (16384, "16KiB")];
    let mut group = c.benchmark_group("message/event_size_hint");
    for &(sz, label) in sizes {
        let e = make_event(sz);
        group.bench_with_input(BenchmarkId::new("hint", label), &e, |b, e| {
            b.iter(|| black_box(e.size_hint()));
        });
    }
    group.finish();
}

fn bench_class(c: &mut Criterion) {
    let classes = [
        MessageClass::Event,
        MessageClass::Command,
        MessageClass::Reply,
        MessageClass::State,
        MessageClass::Datagram,
        MessageClass::Stream,
        MessageClass::Snapshot,
        MessageClass::System,
    ];
    let mut group = c.benchmark_group("message/class");
    group.bench_function("as_str", |b| {
        b.iter(|| {
            for c in &classes {
                black_box(c.as_str());
            }
        });
    });
    group.bench_function("serde_round_trip", |b| {
        b.iter(|| {
            let s = serde_json::to_string(black_box(&MessageClass::Event)).expect("ser");
            let c: MessageClass = serde_json::from_str(black_box(&s)).expect("de");
            black_box(c);
        });
    });
    group.finish();
}

fn bench_subscribe_mode(c: &mut Criterion) {
    let modes = [
        SubscribeIntent::Live,
        SubscribeIntent::Replay { from: 0 },
        SubscribeIntent::SnapshotThenLive,
        SubscribeIntent::Latest,
        SubscribeIntent::Passive,
        SubscribeIntent::Ephemeral,
    ];
    let mut group = c.benchmark_group("message/subscribe_mode");
    group.bench_function("serde_round_trip", |b| {
        b.iter(|| {
            for m in &modes {
                let s = serde_json::to_string(black_box(m)).expect("ser");
                let back: SubscribeIntent = serde_json::from_str(black_box(&s)).expect("de");
                black_box(back);
            }
        });
    });
    group.finish();
}

fn bench_message_variant_class(c: &mut Criterion) {
    let event = Message::Event(Event::new("e", "m", "s", serde_json::json!({})));
    let mut group = c.benchmark_group("message/variant_class");
    group.bench_function("class", |b| {
        b.iter(|| black_box(black_box(&event).class()));
    });
    group.bench_function("clone", |b| {
        b.iter(|| black_box(black_box(&event).clone()));
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_event_encode,
    bench_event_decode,
    bench_event_size_hint,
    bench_class,
    bench_subscribe_mode,
    bench_message_variant_class,
);
criterion_main!(benches);
