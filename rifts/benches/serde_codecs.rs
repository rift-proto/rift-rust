//! Serialization codec benchmarks — JSON vs CBOR encode/decode + negotiation.

use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rifts::codec::{CborCodec, JsonCodec, PayloadCodec, PayloadCodecExt, negotiate};
use rifts::frame::EncodingFormat;
use std::hint::black_box;

mod common;

#[derive(serde::Serialize, serde::Deserialize)]
struct BenchRecord {
    id: u64,
    name: String,
    topic: String,
    timestamp: i64,
    tags: Vec<String>,
    payload: Vec<u8>,
    meta: serde_json::Value,
}

fn make_record(id: u64, payload_size: usize) -> BenchRecord {
    BenchRecord {
        id,
        name: format!("event-{id}"),
        topic: "bench/topic/0".to_string(),
        timestamp: 1_700_000_000_000 + id as i64,
        tags: vec!["bench".to_string(), "test".to_string(), "rift".to_string()],
        payload: vec![0xAB; payload_size],
        meta: serde_json::json!({
            "source": "bench",
            "version": 1,
            "flags": ["compressed", "traced"],
        }),
    }
}

fn make_value(payload_size: usize) -> serde_json::Value {
    serde_json::json!({
        "id": 1u64,
        "name": "event-1",
        "topic": "bench/topic/0",
        "timestamp": 1_700_000_000_000i64,
        "tags": ["bench", "test", "rift"],
        "payload": serde_json::Value::Array((0..payload_size).map(|i| serde_json::Value::from((i & 0xff) as u8)).collect::<Vec<_>>()),
        "meta": {
            "source": "bench",
            "version": 1,
            "flags": ["compressed", "traced"],
        },
    })
}

fn bench_encode_value(c: &mut Criterion) {
    let sizes: &[(usize, &str)] = &[(0, "0B"), (64, "64B"), (1024, "1KiB"), (16384, "16KiB")];
    let mut group = c.benchmark_group("codec/encode_value");
    for &(sz, label) in sizes {
        let value = make_value(sz);
        let json_bytes = serde_json::to_vec(&value).expect("json");
        group.throughput(criterion::Throughput::Bytes(json_bytes.len() as u64));
        group.bench_with_input(BenchmarkId::new("json", label), &value, |b, v| {
            b.iter(|| JsonCodec.encode_value(black_box(v)).expect("enc"));
        });
        group.bench_with_input(BenchmarkId::new("cbor", label), &value, |b, v| {
            b.iter(|| CborCodec.encode_value(black_box(v)).expect("enc"));
        });
    }
    group.finish();
}

fn bench_decode_value(c: &mut Criterion) {
    let sizes: &[(usize, &str)] = &[(0, "0B"), (64, "64B"), (1024, "1KiB"), (16384, "16KiB")];
    let mut group = c.benchmark_group("codec/decode_value");
    for &(sz, label) in sizes {
        let value = make_value(sz);
        let json_bytes = JsonCodec.encode_value(&value).expect("enc");
        let cbor_bytes = CborCodec.encode_value(&value).expect("enc");
        group.throughput(criterion::Throughput::Bytes(
            json_bytes.len().max(cbor_bytes.len()) as u64,
        ));
        group.bench_with_input(BenchmarkId::new("json", label), &json_bytes, |b, buf| {
            b.iter(|| JsonCodec.decode_value(black_box(buf)).expect("dec"));
        });
        group.bench_with_input(BenchmarkId::new("cbor", label), &cbor_bytes, |b, buf| {
            b.iter(|| CborCodec.decode_value(black_box(buf)).expect("dec"));
        });
    }
    group.finish();
}

fn bench_encode_struct(c: &mut Criterion) {
    let sizes: &[(usize, &str)] = &[(0, "0B"), (64, "64B"), (1024, "1KiB")];
    let mut group = c.benchmark_group("codec/encode_struct");
    for &(sz, label) in sizes {
        let rec = make_record(1, sz);
        group.bench_with_input(BenchmarkId::new("json", label), &rec, |b, r| {
            b.iter(|| JsonCodec.encode(black_box(r)).expect("enc"));
        });
        group.bench_with_input(BenchmarkId::new("cbor", label), &rec, |b, r| {
            b.iter(|| CborCodec.encode(black_box(r)).expect("enc"));
        });
    }
    group.finish();
}

fn bench_decode_struct(c: &mut Criterion) {
    let sizes: &[(usize, &str)] = &[(0, "0B"), (64, "64B"), (1024, "1KiB")];
    let mut group = c.benchmark_group("codec/decode_struct");
    for &(sz, label) in sizes {
        let rec = make_record(1, sz);
        let json_bytes = JsonCodec.encode(&rec).expect("enc");
        let cbor_bytes = CborCodec.encode(&rec).expect("enc");
        group.bench_with_input(BenchmarkId::new("json", label), &json_bytes, |b, buf| {
            b.iter(|| {
                let _: BenchRecord = JsonCodec.decode(black_box(buf)).expect("dec");
            });
        });
        group.bench_with_input(BenchmarkId::new("cbor", label), &cbor_bytes, |b, buf| {
            b.iter(|| {
                let _: BenchRecord = CborCodec.decode(black_box(buf)).expect("dec");
            });
        });
    }
    group.finish();
}

fn bench_negotiate(c: &mut Criterion) {
    let server: Vec<Arc<dyn PayloadCodec>> = vec![Arc::new(CborCodec), Arc::new(JsonCodec)];
    let mut group = c.benchmark_group("codec/negotiate");
    group.bench_function("match_first", |b| {
        let client = vec![EncodingFormat::Cbor, EncodingFormat::Json];
        b.iter(|| negotiate(black_box(&server), black_box(&client)).expect("neg"));
    });
    group.bench_function("match_second", |b| {
        let client = vec![EncodingFormat::Json, EncodingFormat::Cbor];
        b.iter(|| negotiate(black_box(&server), black_box(&client)).expect("neg"));
    });
    group.bench_function("no_match", |b| {
        let client = vec![EncodingFormat::Json];
        let server_json: Vec<Arc<dyn PayloadCodec>> = vec![Arc::new(JsonCodec)];
        b.iter(|| negotiate(black_box(&server_json), black_box(&client)).expect("neg"));
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_encode_value,
    bench_decode_value,
    bench_encode_struct,
    bench_decode_struct,
    bench_negotiate,
);
criterion_main!(benches);
