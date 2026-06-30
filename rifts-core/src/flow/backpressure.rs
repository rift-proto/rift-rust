//! Per-connection backpressure controller (Rift spec section 18.1).
//!
//! This module implements the server-side flow-control mechanism that monitors
//! each connection's outbound message queue and selects a mitigation strategy
//! when the queue exceeds its configured capacity.
//!
//! # Design
//!
//! The controller uses lock-free atomic operations for the critical path
//! (enqueue and release) so that concurrent publisher tasks do not contend on
//! a mutex. Strategy selection is behind a `parking_lot::Mutex` since it is
//! only consulted when the queue is already full and contention is expected.
//!
//! A compare-and-swap loop in [`BackpressureController::try_enqueue`] eliminates
//! the time-of-check/time-of-use race between the capacity check and the
//! byte-count increment.
//!
//! # High-water mark
//!
//! The overload threshold is fixed at 90 % of `max_bytes`. When the queue
//! crosses this mark a flow-pause event is recorded; when it falls back below,
//! a flow-resume event is recorded. These counters are exposed for metrics and
//! diagnostics.

use std::sync::atomic::{AtomicUsize, Ordering};

/// Backpressure strategy applied when the send queue is full.
///
/// The server selects one of these strategies based on the topic profile,
/// connection priority, and operator configuration. Each variant maps to a
/// different trade-off between delivery guarantees and resource usage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BackpressureStrategy {
    /// Wait until the queue drains below the capacity threshold.
    ///
    /// This is the safest default — no messages are lost — but the
    /// producer is blocked until the consumer catches up.
    #[default]
    Pause,

    /// Drop the lowest-priority messages first.
    ///
    /// Messages marked with [`Priority::Volatile`] or [`Priority::Background`]
    /// are eligible for immediate discard. Higher-priority messages are
    /// preserved.
    DropVolatile,

    /// Collapse state messages by `state_key`, keeping only the latest value.
    ///
    /// This is useful for stateful topics where intermediate snapshots are
    /// redundant — only the most recent snapshot matters.
    CoalesceState,

    /// Lower delivery frequency by deferring or throttling writes.
    ///
    /// The caller should reduce the rate at which it pushes frames to the
    /// consumer, for example by introducing artificial delays or batching.
    Downgrade,

    /// Disconnect the slow consumer immediately.
    ///
    /// Use this strategy for topics where partial delivery is worse than
    /// no delivery, or when the consumer has been persistently slow.
    Disconnect,

    /// Switch the consumer to snapshot polling mode.
    ///
    /// Instead of streaming every message, the consumer is told to poll
    /// for the latest snapshot on demand. This dramatically reduces the
    /// server's outbound bandwidth for topics that support snapshots.
    SnapshotLater,
}

/// State of a single connection's outbound queue.
///
/// Each [`Connection`](crate::connection::Connection) owns one
/// `BackpressureController`. The controller tracks how many bytes are
/// currently queued, which strategy is active, and maintains counters for
/// operational metrics (drops, pauses, coalescings, etc.).
///
/// # Thread safety
///
/// All counters are [`AtomicUsize`] so they can be updated from any task
/// without locking. The strategy is behind a [`parking_lot::Mutex`] because
/// it is only read on the slow path (when the queue is full).
pub struct BackpressureController {
    /// Maximum number of bytes the outbound queue may hold before
    /// backpressure actions are triggered.
    max_bytes: usize,

    /// Current number of bytes in the queue, maintained via
    /// acquire-release semantics for accurate cross-thread accounting.
    current_bytes: AtomicUsize,

    /// The active backpressure strategy. Protected by a mutex because it
    /// is only consulted when the queue is already at capacity.
    strategy: parking_lot::Mutex<BackpressureStrategy>,

    /// Total number of times a backpressure decision has been applied
    /// (i.e. `try_enqueue` returned something other than `Accept`).
    applied: AtomicUsize,

    /// Number of messages dropped due to backpressure.
    dropped: AtomicUsize,

    /// Number of times the slow-consumer disconnect strategy fired.
    slow_consumer: AtomicUsize,

    /// Number of flow-pause events emitted (queue crossed high-water mark).
    flow_pause: AtomicUsize,

    /// Number of flow-resume events emitted (queue dropped below high-water mark).
    flow_resume: AtomicUsize,

    /// Number of volatile-priority messages dropped.
    volatile_drop: AtomicUsize,

    /// Number of state coalescing events.
    state_coalesce: AtomicUsize,
}

impl BackpressureController {
    /// Create a new controller with the given maximum queue capacity in bytes.
    ///
    /// The initial strategy is [`BackpressureStrategy::Pause`] (the default).
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            current_bytes: AtomicUsize::new(0),
            strategy: parking_lot::Mutex::new(BackpressureStrategy::default()),
            applied: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
            slow_consumer: AtomicUsize::new(0),
            flow_pause: AtomicUsize::new(0),
            flow_resume: AtomicUsize::new(0),
            volatile_drop: AtomicUsize::new(0),
            state_coalesce: AtomicUsize::new(0),
        }
    }

    /// Returns the maximum byte capacity of the outbound queue.
    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    /// Returns the current number of bytes in the outbound queue.
    ///
    /// Uses [`Ordering::Acquire`] to ensure the caller sees all preceding
    /// writes from the task that last modified the counter.
    pub fn current_bytes(&self) -> usize {
        self.current_bytes.load(Ordering::Acquire)
    }

    /// Returns the currently active backpressure strategy.
    pub fn strategy(&self) -> BackpressureStrategy {
        *self.strategy.lock()
    }

    /// Replace the active backpressure strategy.
    ///
    /// The new strategy takes effect on the next call to [`try_enqueue`](Self::try_enqueue)
    /// that encounters a full queue.
    pub fn set_strategy(&self, s: BackpressureStrategy) {
        *self.strategy.lock() = s;
    }

    /// Returns the number of bytes of remaining capacity in the queue.
    ///
    /// Uses saturating subtraction so the result is always non-negative,
    /// even if `current_bytes` briefly exceeds `max_bytes` due to a
    /// concurrent enqueue.
    pub fn available(&self) -> usize {
        self.max_bytes.saturating_sub(self.current_bytes())
    }

    /// Returns `true` if the connection is currently above the high-water
    /// mark (90 % of `max_bytes`).
    ///
    /// This can be used by metrics exporters to detect overloaded connections
    /// without inspecting the exact byte count.
    pub fn is_overloaded(&self) -> bool {
        if self.max_bytes == 0 {
            return false;
        }
        self.current_bytes() >= self.max_bytes - self.max_bytes / 10
    }

    /// Attempt to enqueue a payload of `payload_bytes` bytes.
    ///
    /// Returns the [`BackpressureAction`] the caller should take given the
    /// current strategy. If there is room in the queue the bytes are reserved
    /// atomically and [`BackpressureAction::Accept`] is returned.
    ///
    /// If the queue is at capacity the method consults the active strategy
    /// and increments the corresponding metric counter before returning the
    /// appropriate action.
    ///
    /// # Lock-free fast path
    ///
    /// Uses an atomic compare-and-swap loop so that the capacity check and
    /// the byte-count increment are performed as a single atomic operation,
    /// avoiding the time-of-check/time-of-use race that would arise with a
    /// separate load + store.
    pub fn try_enqueue(&self, payload_bytes: usize) -> BackpressureAction {
        let mut prev = self.current_bytes.load(Ordering::Acquire);
        loop {
            if prev + payload_bytes <= self.max_bytes {
                match self.current_bytes.compare_exchange_weak(
                    prev,
                    prev + payload_bytes,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return BackpressureAction::Accept,
                    Err(current) => prev = current,
                }
            } else {
                break;
            }
        }
        self.record_applied();
        match self.strategy() {
            BackpressureStrategy::Pause => {
                self.flow_pause.fetch_add(1, Ordering::Relaxed);
                BackpressureAction::Pause
            }
            BackpressureStrategy::DropVolatile => {
                self.record_dropped();
                self.volatile_drop.fetch_add(1, Ordering::Relaxed);
                BackpressureAction::DropVolatile
            }
            BackpressureStrategy::CoalesceState => {
                self.record_dropped();
                self.state_coalesce.fetch_add(1, Ordering::Relaxed);
                BackpressureAction::CoalesceState
            }
            BackpressureStrategy::Downgrade => BackpressureAction::Downgrade,
            BackpressureStrategy::Disconnect => {
                self.record_dropped();
                self.slow_consumer.fetch_add(1, Ordering::Relaxed);
                BackpressureAction::Disconnect
            }
            BackpressureStrategy::SnapshotLater => BackpressureAction::SnapshotLater,
        }
    }

    /// Release `bytes` from the queue after a message has been written to
    /// the transport.
    ///
    /// Uses a compare-and-swap loop with `saturating_sub` so the counter
    /// cannot underflow below zero even if the caller releases more
    /// bytes than were enqueued (a programming error).
    ///
    /// If the release causes the queue to drop back below the high-water
    /// mark (90 %) a flow-resume event is recorded automatically.
    pub fn release(&self, bytes: usize) {
        let hwm = self.high_water();
        let mut prev = self.current_bytes.load(Ordering::Acquire);
        loop {
            let next = prev.saturating_sub(bytes);
            match self.current_bytes.compare_exchange_weak(
                prev,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if prev >= hwm && next < hwm {
                        self.flow_resume.fetch_add(1, Ordering::Relaxed);
                    }
                    return;
                }
                Err(current) => prev = current,
            }
        }
    }

    /// High-water mark in bytes (90 % of `max_bytes`). Computed via
    /// subtraction to avoid overflow for small `max_bytes` values.
    fn high_water(&self) -> usize {
        self.max_bytes - self.max_bytes / 10
    }

    /// Increment the counter that records how many times a backpressure
    /// decision was applied.
    pub fn record_applied(&self) {
        self.applied.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the counter that records how many messages were dropped
    /// due to backpressure.
    pub fn record_dropped(&self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns the total number of times a backpressure action was applied.
    pub fn applied(&self) -> usize {
        self.applied.load(Ordering::Relaxed)
    }

    /// Returns the total number of messages dropped due to backpressure.
    pub fn dropped(&self) -> usize {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Returns the total number of slow-consumer disconnects.
    pub fn slow_consumer_count(&self) -> usize {
        self.slow_consumer.load(Ordering::Relaxed)
    }

    /// Returns the total number of flow-pause events (queue crossed the
    /// high-water mark).
    pub fn flow_pause_count(&self) -> usize {
        self.flow_pause.load(Ordering::Relaxed)
    }

    /// Returns the total number of flow-resume events (queue fell back
    /// below the high-water mark).
    pub fn flow_resume_count(&self) -> usize {
        self.flow_resume.load(Ordering::Relaxed)
    }

    /// Returns the total number of volatile-priority messages that were
    /// dropped by the `DropVolatile` strategy.
    pub fn volatile_drop_count(&self) -> usize {
        self.volatile_drop.load(Ordering::Relaxed)
    }

    /// Returns the total number of state coalescing events.
    pub fn state_coalesce_count(&self) -> usize {
        self.state_coalesce.load(Ordering::Relaxed)
    }
}

/// What the caller should do when the queue cannot accept a message.
///
/// Each variant corresponds to one branch of the active
/// [`BackpressureStrategy`]. The caller (typically the broker's fanout
/// path or the connection's write loop) is responsible for acting on the
/// returned value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureAction {
    /// The message was accepted into the queue — no further action needed.
    Accept,

    /// The caller should pause the producer until capacity becomes available.
    Pause,

    /// The caller should drop messages whose priority is [`Priority::Volatile`]
    /// or [`Priority::Background`].
    DropVolatile,

    /// The caller should coalesce state messages that share the same
    /// `state_key`, keeping only the latest value for each key.
    CoalesceState,

    /// The caller should downgrade the connection's delivery frequency.
    Downgrade,

    /// The caller should disconnect the slow consumer.
    Disconnect,

    /// The caller should switch the consumer to snapshot polling mode.
    SnapshotLater,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accept_under_limit() {
        let bp = BackpressureController::new(100);
        assert_eq!(bp.try_enqueue(50), BackpressureAction::Accept);
        assert_eq!(bp.try_enqueue(40), BackpressureAction::Accept);
        assert_eq!(bp.current_bytes(), 90);
    }

    #[test]
    fn pause_when_over_limit() {
        let bp = BackpressureController::new(100);
        bp.set_strategy(BackpressureStrategy::Pause);
        bp.try_enqueue(80);
        assert_eq!(bp.try_enqueue(50), BackpressureAction::Pause);
    }

    #[test]
    fn disconnect_strategy() {
        let bp = BackpressureController::new(100);
        bp.set_strategy(BackpressureStrategy::Disconnect);
        bp.try_enqueue(80);
        assert_eq!(bp.try_enqueue(50), BackpressureAction::Disconnect);
    }

    #[test]
    fn release_decrements() {
        let bp = BackpressureController::new(100);
        bp.try_enqueue(50);
        bp.release(30);
        assert_eq!(bp.current_bytes(), 20);
    }

    #[test]
    fn overloaded_detection() {
        let bp = BackpressureController::new(100);
        bp.try_enqueue(95);
        assert!(bp.is_overloaded());
        bp.release(10);
        assert!(!bp.is_overloaded());
    }
}
