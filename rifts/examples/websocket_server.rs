//! Standalone WebSocket server example.
//!
//! Start the server, then connect with any WebSocket client (e.g.
//! `websocat ws://127.0.0.1:9000`).  Send a CBOR-encoded Rift/1 frame;
//! the server will respond with a Welcome frame on the first
//! connection.

use std::sync::Arc;

use tokio::signal;
use tokio::sync::Notify;

#[tokio::main]
async fn main() -> rifts::Result<()> {
    // Initialize a basic tracing subscriber to see server logs.
    tracing_subscriber::fmt()
        .with_env_filter("rifts=debug")
        .init();

    let shutdown = Arc::new(Notify::new());
    let server = rifts::RiftServer::builder().websocket_transport().build()?;

    let addr = "127.0.0.1:9000".parse().unwrap();

    // Spawn the server.
    let srv_hdl = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { server.run(addr, shutdown).await }
    });

    // Wait for Ctrl+C, then signal shutdown.
    signal::ctrl_c().await.ok();
    shutdown.notify_one();
    srv_hdl.await.unwrap()?;

    Ok(())
}
