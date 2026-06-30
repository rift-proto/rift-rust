//! # Rifts Broker — Message routing core
//!
//! This crate implements the central message broker for the Rift Realtime
//! Protocol: publish, subscribe, fan-out, deduplication, flow control, and
//! per-connection state management.
//!
//! ## Modules
//!
//! | Module | Responsibility |
//! |--------|----------------|
//! | [`broker`] | The [`Broker`] trait, fanout engine, topic router, and in-memory broker |
//! | [`connection`] | Per-connection state machine — handshake, reader/writer tasks, teardown |
//!
//! [`Broker`]: broker::Broker

#![forbid(unsafe_code)]
#![deny(unreachable_pub)]
#![deny(missing_docs)]
#![warn(rust_2018_idioms)]

pub mod broker;
pub mod connection;
