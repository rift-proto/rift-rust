//! Loopback WebSocket benchmarks (feature `client`).
//!
//! Spins up a local `RiftServer` on a random loopback port and a `RiftClient`,
//! then benchmarks connect/publish/roundtrip cycles. Uses ephemeral ports to
//! avoid fixed-port conflicts on CI dev machines.
//!
//! These are manual/heavy benchmarks — run with:
//! ```sh
//! cargo bench --features client --bench websocket_loop
//! ```

use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rifts::client::{RiftClient, RiftClientConfig};
use rifts::frame::EncodingFormat;
use rifts::server::RiftServer;

mod common;

fn alloc_loopback_addr() -> SocketAddr {
    for _retry in 0..10 {
        if let Ok(l) = TcpListener::bind("127.0.0.1:0")
            && let Ok(addr) = l.local_addr()
        {
            drop(l);
            return addr;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("failed to alloc loopback port after 10 retries");
}

async fn start_loopback_server() -> (String, Arc<tokio::sync::Notify>) {
    let addr = alloc_loopback_addr();
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_clone = shutdown.clone();
    tokio::spawn(async move {
        let server = RiftServer::builder()
            .websocket_transport()
            .build()
            .expect("build server");
        if let Err(e) = server.run(addr, shutdown_clone).await {
            eprintln!("loopback server error: {e}");
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    (format!("ws://{addr}"), shutdown)
}

fn make_config() -> RiftClientConfig {
    RiftClientConfig {
        client_id: "bench-client".to_string(),
        token: String::new(),
        session_id: None,
        epoch: 1,
        codecs: vec![EncodingFormat::Json],
        features: vec!["resume".into()],
        last_offsets: std::collections::BTreeMap::new(),
        auto_reconnect: false,
        reconnect_delay: Duration::from_millis(100),
        max_reconnect_attempts: 1,
    }
}

fn bench_connect_close(c: &mut Criterion) {
    let rt = common::runtime();
    let mut group = c.benchmark_group("loopback/connect_close");
    group.bench_function("connect_close", |b| {
        b.to_async(&rt).iter(|| async move {
            let (url, shutdown) = start_loopback_server().await;
            let client = RiftClient::new(url, make_config());
            let _ = client.connect().await;
            let _ = client.close().await;
            shutdown.notify_waiters();
            tokio::time::sleep(Duration::from_millis(50)).await;
        });
    });
    group.finish();
}

fn bench_publish(c: &mut Criterion) {
    let rt = common::runtime();
    let mut group = c.benchmark_group("loopback/publish");
    let payloads: &[(usize, &str)] = &[(0, "0B"), (64, "64B"), (1024, "1KiB"), (16384, "16KiB")];
    for &(sz, label) in payloads {
        group.bench_with_input(BenchmarkId::new("payload", label), &sz, |b, &sz| {
            b.to_async(&rt).iter(move || {
                let payload = serde_json::json!({
                    "data": "x".repeat(sz),
                });
                async move {
                    let (url, shutdown) = start_loopback_server().await;
                    let client = RiftClient::new(url, make_config());
                    let _ = client.connect().await;
                    let _ = client
                        .subscribe("bench.topic", rifts::message::SubscribeIntent::Live, None)
                        .await;
                    let _ = client
                        .publish("bench.topic", "bench.event", "json", payload, None)
                        .await;
                    let _ = client.close().await;
                    shutdown.notify_waiters();
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            });
        });
    }
    group.finish();
}

fn bench_roundtrip(c: &mut Criterion) {
    let rt = common::runtime();
    let mut group = c.benchmark_group("loopback/roundtrip");
    for &n in &[1usize, 10, 100] {
        group.bench_with_input(BenchmarkId::new("messages", n), &n, |b, &n| {
            b.to_async(&rt).iter(move || async move {
                let (url, shutdown) = start_loopback_server().await;
                let client = RiftClient::new(url, make_config());
                let _ = client.connect().await;

                let mut ev_rx = client.subscribe_events();
                let _ = client
                    .subscribe("bench.topic", rifts::message::SubscribeIntent::Live, None)
                    .await;

                for i in 0..n {
                    let payload = serde_json::json!({ "seq": i });
                    let _ = client
                        .publish("bench.topic", "bench.event", "json", payload, None)
                        .await;
                }

                for _ in 0..n {
                    let _ = tokio::time::timeout(Duration::from_secs(3), ev_rx.recv()).await;
                }

                let _ = client.close().await;
                shutdown.notify_waiters();
                tokio::time::sleep(Duration::from_millis(50)).await;
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_connect_close, bench_publish, bench_roundtrip,);
criterion_main!(benches);
