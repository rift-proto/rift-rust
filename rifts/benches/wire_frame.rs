//! Wire-level frame codec benchmarks.
//!
//! Measures `encode_frame`, `decode_binary_frame`, and `decode_text_frame`
//! across payload sizes and frame types. These are the hot paths on every
//! connection read/write.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rifts::frame::{EncodingFormat, Frame};
use rifts::{DEFAULT_MAX_BINARY_PAYLOAD, decode_binary_frame, decode_text_frame, encode_frame};
use std::hint::black_box;

use crate::common::{
    bench_payload_sizes, build_ack_frame, build_control_frame, build_data_frame, payload_of,
};

mod common;

fn bench_encode_binary(c: &mut Criterion) {
    let payloads = bench_payload_sizes();
    let mut group = c.benchmark_group("wire/encode_binary");
    for (label, payload) in &payloads {
        let frame = build_data_frame(payload.clone(), 1);
        group.throughput(criterion::Throughput::Bytes(payload.len() as u64));
        group.bench_with_input(BenchmarkId::new("data", label), payload, |b, _| {
            b.iter(|| {
                let bytes = encode_frame(black_box(&frame)).expect("encode");
                black_box(bytes);
            });
        });
    }
    group.finish();
}

fn bench_decode_binary(c: &mut Criterion) {
    let payloads = bench_payload_sizes();
    let mut group = c.benchmark_group("wire/decode_binary");
    for (label, payload) in &payloads {
        let frame = build_data_frame(payload.clone(), 1);
        let encoded = encode_frame(&frame).expect("encode");
        group.throughput(criterion::Throughput::Bytes(encoded.len() as u64));
        group.bench_with_input(BenchmarkId::new("data", label), &encoded, |b, encoded| {
            b.iter(|| {
                let frame = decode_binary_frame(black_box(encoded), DEFAULT_MAX_BINARY_PAYLOAD)
                    .expect("decode");
                black_box(frame);
            });
        });
    }
    group.finish();
}

fn bench_decode_text(c: &mut Criterion) {
    let payloads = bench_payload_sizes();
    let mut group = c.benchmark_group("wire/decode_text");
    for (label, payload) in &payloads {
        let mut frame = build_data_frame(payload.clone(), 1);
        frame.codec = EncodingFormat::Json;
        let json = serde_json::to_vec(&frame).expect("json");
        group.throughput(criterion::Throughput::Bytes(json.len() as u64));
        group.bench_with_input(BenchmarkId::new("data", label), &json, |b, json| {
            b.iter(|| {
                let frame = decode_text_frame(black_box(json)).expect("decode");
                black_box(frame);
            });
        });
    }
    group.finish();
}

fn bench_encode_by_type(c: &mut Criterion) {
    let payload = payload_of(1024);
    let frames: Vec<(&str, Frame)> = vec![
        ("control", build_control_frame(1)),
        ("data", build_data_frame(payload.clone(), 1)),
        ("ack", build_ack_frame(1)),
    ];
    let mut group = c.benchmark_group("wire/encode_by_type");
    for (name, frame) in frames {
        group.bench_with_input(BenchmarkId::new("encode", name), &frame, |b, frame| {
            b.iter(|| {
                let bytes = encode_frame(black_box(frame)).expect("encode");
                black_box(bytes);
            });
        });
    }
    group.finish();
}

fn bench_round_trip(c: &mut Criterion) {
    let payload = payload_of(1024);
    let frame = build_data_frame(payload, 1);
    let mut group = c.benchmark_group("wire/round_trip");
    group.bench_function("encode+decode", |b| {
        b.iter(|| {
            let encoded = encode_frame(black_box(&frame)).expect("encode");
            let decoded = decode_binary_frame(black_box(&encoded), DEFAULT_MAX_BINARY_PAYLOAD)
                .expect("decode");
            black_box(decoded);
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_encode_binary,
    bench_decode_binary,
    bench_decode_text,
    bench_encode_by_type,
    bench_round_trip,
);
criterion_main!(benches);
