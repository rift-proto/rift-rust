//! Frame model microbenchmarks — construction, flags, priority.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rifts::frame::{EncodingFormat, Frame, FrameFlags, FrameType, Priority};
use std::hint::black_box;

fn bench_construct(c: &mut Criterion) {
    let mut group = c.benchmark_group("frame/construct");
    let cases: Vec<(&str, FrameType)> = vec![
        ("control", FrameType::Control),
        ("data", FrameType::Data),
        ("ack", FrameType::Ack),
        ("flow", FrameType::Flow),
        ("error", FrameType::Error),
    ];
    for (name, ft) in cases {
        group.bench_with_input(BenchmarkId::new("ctor", name), &ft, |b, &ft| {
            b.iter(|| {
                let f = match ft {
                    FrameType::Control => Frame::control(),
                    FrameType::Data => Frame::data(),
                    FrameType::Ack => Frame::ack(),
                    FrameType::Flow => Frame::flow(),
                    FrameType::Error => Frame::error(),
                };
                black_box(f);
            });
        });
    }
    group.finish();
}

fn bench_flags(c: &mut Criterion) {
    let mut group = c.benchmark_group("frame/flags");
    group.bench_function("empty", |b| {
        b.iter(|| black_box(FrameFlags::empty()));
    });
    group.bench_function("from_bits", |b| {
        b.iter(|| black_box(FrameFlags::from_bits(0x0030)));
    });
    group.bench_function("with", |b| {
        b.iter(|| {
            black_box(
                FrameFlags::empty()
                    .with(FrameFlags::COMPRESSED)
                    .with(FrameFlags::REQUIRES_ACK)
                    .with(FrameFlags::TRACE),
            )
        });
    });
    group.bench_function("contains", |b| {
        let flags = FrameFlags::empty()
            .with(FrameFlags::COMPRESSED)
            .with(FrameFlags::REQUIRES_ACK);
        b.iter(|| black_box(flags.contains(black_box(FrameFlags::REQUIRES_ACK))));
    });
    group.bench_function("set_clear", |b| {
        let mut flags = FrameFlags::empty();
        b.iter(|| {
            flags.set(FrameFlags::SNAPSHOT);
            flags.clear(FrameFlags::SNAPSHOT);
            black_box(flags);
        });
    });
    group.finish();
}

fn bench_priority(c: &mut Criterion) {
    let mut group = c.benchmark_group("frame/priority");
    let levels = [
        Priority::Background,
        Priority::Volatile,
        Priority::Low,
        Priority::Normal,
        Priority::High,
        Priority::Critical,
    ];
    for p in levels {
        let name = p.to_string();
        group.bench_with_input(BenchmarkId::new("from_u8", name), &p, |b, &p| {
            b.iter(|| black_box(Priority::from_u8(black_box(p.as_u8()))));
        });
    }
    group.bench_function("compare", |b| {
        let a = Priority::High;
        let bb = Priority::Normal;
        b.iter(|| black_box(a > black_box(bb)));
    });
    group.finish();
}

fn bench_codec_tag(c: &mut Criterion) {
    let mut group = c.benchmark_group("frame/codec_tag");
    group.bench_function("json_tag", |b| {
        b.iter(|| black_box(EncodingFormat::Json.tag()));
    });
    group.bench_function("cbor_tag", |b| {
        b.iter(|| black_box(EncodingFormat::Cbor.tag()));
    });
    group.bench_function("from_tag", |b| {
        b.iter(|| black_box(EncodingFormat::from_tag(black_box(b'B'))));
    });
    group.bench_function("frame_type_tag", |b| {
        b.iter(|| black_box(FrameType::Data.tag()));
    });
    group.finish();
}

fn bench_full_struct(c: &mut Criterion) {
    let mut group = c.benchmark_group("frame/full_struct");
    group.bench_function("build_data", |b| {
        b.iter(|| {
            let mut f = Frame::data();
            f.frame_id = 1;
            f.timestamp = 1234567890;
            f.topic = Some("bench/topic".to_string());
            f.message_id = Some("msg-1".to_string());
            f.codec = EncodingFormat::Cbor;
            f.flags = FrameFlags::empty().with(FrameFlags::REQUIRES_ACK);
            f.priority = Some(Priority::High);
            f.payload = Some(bytes::Bytes::from_static(b"payload"));
            black_box(f);
        });
    });
    group.bench_function("clone", |b| {
        let mut f = Frame::data();
        f.frame_id = 1;
        f.topic = Some("bench/topic".to_string());
        f.payload = Some(bytes::Bytes::from_static(b"payload"));
        b.iter(|| black_box(black_box(&f).clone()));
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_construct,
    bench_flags,
    bench_priority,
    bench_codec_tag,
    bench_full_struct,
);
criterion_main!(benches);
