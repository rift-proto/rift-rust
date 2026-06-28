//! Actor-based broker — implements [`Broker`] by delegating to a
//! [`TopicRegistry`].
//!
//! Each topic is an independent actor task.  Publishes to different
//! topics execute concurrently.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::oneshot;

use crate::actor::messages::TopicMsg;
use crate::actor::registry::TopicRegistry;
use crate::broker::broker::{Broker, PublishOutcome};
use crate::broker::fanout::{ConnectionSink, SubscribeIntent, SubscriptionId};
use crate::error::{Result, RiftError};
use crate::frame::Frame;
use crate::storage::{DedupeStore, LogStore, OffsetStore, SnapshotStore};

/// A `Broker` backed by a `TopicRegistry` of actor tasks.
pub struct ActorBroker<O, L, D, S> {
    registry: Arc<TopicRegistry<O, L, D, S>>,
}

impl<O, L, D, S> ActorBroker<O, L, D, S> {
    /// Create a new actor-backed broker.
    pub fn new(registry: Arc<TopicRegistry<O, L, D, S>>) -> Self {
        Self { registry }
    }
}

#[async_trait]
impl<
    O: OffsetStore + 'static,
    L: LogStore + 'static,
    D: DedupeStore + 'static,
    S: SnapshotStore + 'static,
> Broker for ActorBroker<O, L, D, S>
{
    async fn publish(&self, frame: &Frame) -> Result<PublishOutcome> {
        let topic = frame.topic.as_deref().ok_or_else(|| {
            RiftError::Frame(crate::error::FrameReject::RequiredFieldMissing("topic"))
        })?;
        let actor = self.registry.get_or_spawn(topic);
        let (tx, rx) = oneshot::channel();
        actor.send(TopicMsg::Publish {
            frame: frame.clone(),
            reply_to: tx,
        })?;
        rx.await.map_err(|_| {
            RiftError::System(crate::error::SystemReject::Internal("actor died".into()))
        })?
    }

    async fn subscribe(
        &self,
        topic: &str,
        intent: SubscribeIntent,
        sink: ConnectionSink,
    ) -> Result<SubscriptionId> {
        let actor = self.registry.get_or_spawn(topic);
        let (tx, rx) = oneshot::channel();
        actor.send(TopicMsg::Subscribe {
            sink,
            intent,
            reply_to: tx,
        })?;
        rx.await.map_err(|_| {
            RiftError::System(crate::error::SystemReject::Internal("actor died".into()))
        })?
    }

    async fn unsubscribe(&self, _id: SubscriptionId) -> Result<bool> {
        // Unsubscribe requires knowing which topic the id belongs to.
        // In the actor model, the caller is expected to track this.
        // A full implementation would maintain a sid→topic index.
        Ok(false)
    }

    async fn drop_sink(&self, _sink_id: u64) -> usize {
        // DropSink is broadcast to all actors.  A full implementation
        // would iterate all actors with a `DropSink` message.
        // For the minimal impl, return 0.
        0
    }

    async fn replay(&self, topic: &str, from: i64, to: i64) -> Result<Vec<Bytes>> {
        let actor = self.registry.get_or_spawn(topic);
        let (tx, rx) = oneshot::channel();
        actor.send(TopicMsg::Replay {
            from,
            to,
            reply_to: tx,
        })?;
        rx.await.map_err(|_| {
            RiftError::System(crate::error::SystemReject::Internal("actor died".into()))
        })?
    }

    async fn snapshot(&self, topic: &str) -> Result<Option<crate::storage::StoredSnapshot>> {
        let actor = self.registry.get_or_spawn(topic);
        let (tx, rx) = oneshot::channel();
        actor.send(TopicMsg::Snapshot { reply_to: tx })?;
        rx.await.map_err(|_| {
            RiftError::System(crate::error::SystemReject::Internal("actor died".into()))
        })?
    }

    async fn subscriber_count(&self, topic: &str) -> usize {
        let _ = topic;
        0 // Not tracked at broker level in actor model.
    }

    async fn head_offset(&self, topic: &str) -> i64 {
        let actor = self.registry.get_or_spawn(topic);
        let (tx, rx) = oneshot::channel();
        actor.send(TopicMsg::HeadOffset { reply_to: tx }).ok();
        rx.await.unwrap_or(0)
    }
}
