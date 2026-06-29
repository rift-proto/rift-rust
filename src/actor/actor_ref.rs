//! Actor reference types -- type-safe request/response over local channels
//! or remote TCP connections.
//!
//! This module provides two reference types that abstract over the
//! transport layer used to communicate with an actor:
//!
//! - [`LocalActorRef<M>`] -- wraps a `tokio::sync::mpsc::Sender<M>` for
//!   in-process communication.  Messages are sent via a bounded channel
//!   with back-pressure (the channel capacity is set when the actor is
//!   spawned by [`TopicRegistry::get_or_spawn`]).
//!
//! - [`RemoteActorRef<M>`] -- a stub placeholder for cross-process
//!   actors that would communicate over TCP using the framed CBOR wire
//!   protocol.  Users who need distributed actors can implement this
//!   layer on top of [`WireTopicMsg`](crate::actor::WireTopicMsg).
//!
//! Both types are generic over the message enum `M`, enabling compile-time
//! verification that only the correct message type is sent to a given actor.

use std::marker::PhantomData;

use tokio::sync::mpsc;

use crate::error::{Result, RiftError};

/// A local actor reference backed by a `tokio::sync::mpsc::Sender`.
///
/// `LocalActorRef<M>` is a lightweight, clone-able handle to an actor
/// running on the same Tokio runtime.  It wraps an `mpsc::Sender<M>`
/// and exposes a synchronous, non-blocking [`send`](Self::send) method
/// that uses `try_send` under the hood.  If the actor's receive half
/// has been dropped (actor died or shut down), the send will fail with
/// [`RiftError::System`].
///
/// # Type parameter
///
/// * `M` -- the message enum that the target actor processes (typically
///   [`TopicMsg`](crate::actor::TopicMsg)).
///
/// # Example
///
/// ```ignore
/// let (tx, rx) = mpsc::channel::<TopicMsg>(256);
/// let actor_ref = LocalActorRef::new(tx);
///
/// // Send a message (non-blocking).
/// actor_ref.send(msg)?;
///
/// // Check if the actor is still alive.
/// if actor_ref.is_closed() {
///     eprintln!("actor is gone");
/// }
/// ```
#[derive(Debug)]
pub struct LocalActorRef<M> {
    /// The underlying channel sender used to deliver messages to the actor.
    tx: mpsc::Sender<M>,
    /// Marker to carry the message type `M` without storing a value.
    _phantom: PhantomData<M>,
}

impl<M> Clone for LocalActorRef<M> {
    /// Create a clone of this reference.
    ///
    /// Cloning is cheap -- it only increments the sender's reference
    /// count.  Multiple clones can send messages to the same actor
    /// concurrently.
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<M> LocalActorRef<M> {
    /// Create a new actor reference from an `mpsc::Sender`.
    ///
    /// # Arguments
    ///
    /// * `tx` -- the sender half of the channel connected to the actor.
    ///
    /// # Returns
    ///
    /// A new `LocalActorRef<M>` wrapping the provided sender.
    pub fn new(tx: mpsc::Sender<M>) -> Self {
        Self {
            tx,
            _phantom: PhantomData,
        }
    }

    /// Send a message to the actor without blocking.
    ///
    /// This method uses `try_send` internally, meaning it will return
    /// immediately rather than waiting for capacity.  If the channel
    /// is full (back-pressure) or closed (actor died), an error is
    /// returned.
    ///
    /// # Arguments
    ///
    /// * `msg` -- the message to deliver to the actor.
    ///
    /// # Returns
    ///
    /// * `Ok(())` -- the message was enqueued successfully.
    /// * `Err(RiftError::System(SystemReject::Overloaded))` -- the
    ///   channel is full (backpressure; the caller may retry).
    /// * `Err(RiftError::System(SystemReject::Internal))` -- the
    ///   channel is closed (the actor task has exited).
    pub fn send(&self, msg: M) -> Result<()> {
        use tokio::sync::mpsc::error::TrySendError;
        match self.tx.try_send(msg) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                Err(RiftError::System(crate::error::SystemReject::Overloaded))
            }
            Err(TrySendError::Closed(_)) => Err(RiftError::System(
                crate::error::SystemReject::Internal("actor died".into()),
            )),
        }
    }

    /// Returns `true` if the actor's receive half has been dropped.
    ///
    /// A closed channel indicates that the actor has either shut down
    /// gracefully (via a [`Shutdown`](crate::actor::TopicMsg::Shutdown)
    /// message) or panicked.  [`TopicRegistry::get_or_spawn`](crate::actor::TopicRegistry::get_or_spawn)
    /// uses this check to detect dead actors and spawn replacements.
    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    /// Clone the underlying `mpsc::Sender` for use in spawning.
    ///
    /// This is useful when you need to hand a raw sender to a Tokio
    /// task or pass it to a function that expects an `mpsc::Sender`
    /// rather than an `LocalActorRef`.
    pub fn sender(&self) -> mpsc::Sender<M> {
        self.tx.clone()
    }
}

/// A remote actor reference -- stub for cross-process actors communicating
/// over TCP using the framed CBOR wire protocol.
///
/// `RemoteActorRef<M>` is a placeholder type that documents the intended
/// interface for distributed actors.  A full implementation would:
///
/// 1. Accept a `TcpStream` (or connection pool) at construction time.
/// 2. Serialize `M` as a [`WireTopicMsg`](crate::actor::WireTopicMsg)
///    with a unique `request_id`.
/// 3. Send the serialized message over the framed connection.
/// 4. Await the response correlated by `request_id`.
///
/// Users who need cross-process actors can implement this pattern
/// themselves using the wire types already provided in
/// [`crate::actor::messages`].
///
/// # Type parameter
///
/// * `M` -- the message type (should map to a serializable wire variant).
#[doc(hidden)]
pub struct RemoteActorRef<M> {
    /// Marker to carry the message type `M` without storing a value.
    _phantom: PhantomData<M>,
}

impl<M> RemoteActorRef<M> {
    /// Create a stub remote reference.
    ///
    /// The full implementation would accept a `TcpStream` or connection
    /// handle and use the [`WireTopicMsg`](crate::actor::WireTopicMsg)
    /// types from [`crate::actor::messages`] for serialization.
    ///
    /// # Returns
    ///
    /// A new `RemoteActorRef<M>` stub.
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<M> Default for RemoteActorRef<M> {
    /// Return a default-constructed stub remote reference.
    ///
    /// Equivalent to calling [`RemoteActorRef::new()`].
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_ref_send_and_is_closed() {
        let (tx, rx) = mpsc::channel::<i32>(1);
        let r = LocalActorRef::new(tx);
        assert!(!r.is_closed());
        r.send(42).unwrap();
        drop(rx);
        // After dropping the receiver, the channel is closed.
        assert!(r.is_closed());
    }
}
