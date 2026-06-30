//! Writer task — drains the outbound frame channel and writes each frame
//! to the transport.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tracing::warn;

use rifts_core::error::{RiftError, SystemReject};
use rifts_core::frame::Frame;
use rifts_transport::transport::TransportConnection;

/// Writer task — drains the outbound frame channel and writes each frame
/// to the transport.
///
/// The task exits when the channel is closed (all senders dropped) or
/// when a write error occurs. On write failure the transport slot is set
/// to `None` so that the reader task observes the shutdown and stops.
/// The `in_flight_bytes` counter is zeroed defensively on exit.
pub(crate) async fn writer_task(
    mut rx: mpsc::Receiver<Frame>,
    transport_slot: Arc<AsyncMutex<Option<Box<dyn TransportConnection>>>>,
    in_flight_bytes: Arc<AtomicUsize>,
    write_timeout: Duration,
) {
    while let Some(frame) = rx.recv().await {
        let bytes = frame.payload.as_ref().map(|p| p.len()).unwrap_or(0);
        let result = {
            let write_fut = async {
                let mut guard = transport_slot.lock().await;
                let Some(transport) = guard.as_mut() else {
                    return Err(RiftError::System(SystemReject::Internal(
                        "transport released".into(),
                    )));
                };
                transport.write_frame(&frame).await
            };

            match tokio::time::timeout(write_timeout, write_fut).await {
                Ok(r) => r,
                Err(_elapsed) => {
                    warn!(timeout_secs = write_timeout.as_secs(), "write timeout");
                    *transport_slot.lock().await = None;
                    break;
                }
            }
        };
        match result {
            Ok(()) => {
                in_flight_bytes.fetch_sub(bytes, Ordering::SeqCst);
            }
            Err(e) => {
                warn!("write error: {}", e);
                // Release the bytes for the frame that failed to write;
                // without this, in_flight_bytes grows monotonically and
                // eventually triggers spurious backpressure on a
                // reconnect using a new Connection instance.
                in_flight_bytes.fetch_sub(bytes, Ordering::SeqCst);
                // Release the transport so the reader stops too.
                *transport_slot.lock().await = None;
                break;
            }
        }
    }
    // Defensive: when the writer exits (channel closed or transport
    // dropped) zero the counter. At this point the Connection is
    // about to be dropped, so no other code can be mutating the
    // counter.
    in_flight_bytes.store(0, Ordering::SeqCst);
}
