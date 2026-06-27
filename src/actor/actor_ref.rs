//! Actor reference types — type-safe request/response over
//! local channels or remote TCP connections.

use std::marker::PhantomData;

use tokio::sync::mpsc;

use crate::error::{Result, RiftError};

/// A local actor reference backed by an `mpsc::Sender`.
///
/// Type parameter `M` is the message enum.  `send(msg)` delivers the
/// message and returns a oneshot receiver for the response.
#[derive(Debug)]
pub struct LocalActorRef<M> {
    tx: mpsc::Sender<M>,
    _phantom: PhantomData<M>,
}

impl<M> Clone for LocalActorRef<M> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<M> LocalActorRef<M> {
    /// Create a new reference from a sender.
    pub fn new(tx: mpsc::Sender<M>) -> Self {
        Self {
            tx,
            _phantom: PhantomData,
        }
    }

    /// Send a message to the actor.  Returns an error if the actor has
    /// died (channel closed).
    pub fn send(&self, msg: M) -> Result<()> {
        self.tx.try_send(msg).map_err(|_| {
            RiftError::System(crate::error::SystemReject::Internal("actor died".into()))
        })
    }

    /// Returns `true` if the actor's channel is closed (actor has died).
    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    /// Create a new sender from this reference (for spawning).
    pub fn sender(&self) -> mpsc::Sender<M> {
        self.tx.clone()
    }
}

/// A remote actor reference — talks to an actor over TCP using the
/// framed CBOR wire protocol.
///
/// This is a stub: a full implementation would serialize `M` as
/// [`WireTopicMsg`](crate::actor::WireTopicMsg), send it over a
/// framed TCP connection, and await the reply.  Users who need
/// cross-process actors can implement this layer themselves using
/// the wire types already provided.
pub struct RemoteActorRef<M> {
    _phantom: PhantomData<M>,
}

impl<M> RemoteActorRef<M> {
    /// Create a stub remote reference.  The full implementation would
    /// accept a `TcpStream` and use the `actor/messages.rs` wire types.
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<M> Default for RemoteActorRef<M> {
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
