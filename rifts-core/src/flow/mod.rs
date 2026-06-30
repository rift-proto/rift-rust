//! Flow control module — backpressure management (Rift spec section 18).
//!
//! The [`BackpressureController`] monitors per-connection outbound queue depth and
//! selects an appropriate mitigation action when the high-water mark is reached.

pub mod backpressure;

pub use backpressure::{BackpressureAction, BackpressureController, BackpressureStrategy};
