use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::mpsc;

use rifts_core::frame::{EncodingFormat as FrameEncodingFormat, Frame, FrameFlags, FrameType};
use rifts_core::metrics::Metrics;

use super::{FanoutError, FanoutSink};

/// Lightweight fanout sink that writes inbound frames to the connection's
/// outbound mpsc channel.
///
/// Created by [`Connection::sink`](crate::connection::Connection::sink) and
/// handed to the broker so that subscription fanout can push frames directly
/// into this connection's write queue without going through the connection
/// object itself.
pub struct ConnSink {
    /// Sender half of the connection's outbound frame channel.
    pub tx: mpsc::Sender<Frame>,

    /// Atomic counter of bytes currently in the outbound queue.
    /// Shared with the writer task which decrements it on successful writes.
    pub in_flight_bytes: Arc<AtomicUsize>,

    /// Maximum allowed bytes in the outbound queue (from
    /// `ServerConfig::max_send_queue_bytes`).
    pub max_bytes: usize,

    /// Metrics counter for outgoing messages.
    pub metrics: Arc<Metrics>,

    /// Unique id for this sink, used by the broker to track subscriptions.
    pub sink_id: u64,

    /// Codec negotiated during the Hello handshake. All frames
    /// delivered via this sink are tagged with this codec so
    /// clients that negotiated CBOR receive correctly-encoded
    /// payloads.
    pub codec: FrameEncodingFormat,
}

impl FanoutSink for ConnSink {
    /// Attempt to deliver a serialized message frame to this connection's
    /// outbound queue.
    ///
    /// Returns `Err(FanoutError::Backpressured)` if the queue is at capacity
    /// or if the mpsc channel is full. Returns `Err(FanoutError::Closed)` if
    /// the channel has been closed (connection is shutting down).
    fn deliver(&self, frame: bytes::Bytes) -> std::result::Result<(), FanoutError> {
        let len = frame.len();
        // Atomic reserve: compare-and-swap loop so the byte-count
        // check and the increment are performed as a single atomic
        // operation.
        let mut prev = self.in_flight_bytes.load(Ordering::Acquire);
        loop {
            if prev + len > self.max_bytes {
                self.metrics.inc(&self.metrics.flow_pause_total);
                return Err(FanoutError::Backpressured {
                    queue_bytes: prev,
                    max_bytes: self.max_bytes,
                });
            }
            match self.in_flight_bytes.compare_exchange_weak(
                prev,
                prev + len,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(current) => prev = current,
            }
        }
        let f = Frame {
            frame_type: FrameType::Data,
            // Use the negotiated codec, not a hardcoded value, so
            // CBOR-negociated clients receive correctly-tagged frames.
            codec: self.codec,
            payload: Some(frame),
            flags: FrameFlags::empty(),
            ..Frame::default()
        };
        match self.tx.try_send(f) {
            Ok(()) => {
                self.metrics.inc(&self.metrics.messages_out_total);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                // Roll back the reservation we made above.
                self.in_flight_bytes.fetch_sub(len, Ordering::AcqRel);
                Err(FanoutError::Backpressured {
                    queue_bytes: self.in_flight_bytes.load(Ordering::SeqCst),
                    max_bytes: self.max_bytes,
                })
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.in_flight_bytes.fetch_sub(len, Ordering::AcqRel);
                Err(FanoutError::Closed)
            }
        }
    }
    fn id(&self) -> u64 {
        self.sink_id
    }
}
