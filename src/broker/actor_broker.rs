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
        let sink_id = sink.id();
        let actor = self.registry.get_or_spawn(topic);
        let (tx, rx) = oneshot::channel();
        actor.send(TopicMsg::Subscribe {
            sink,
            intent,
            reply_to: tx,
        })?;
        let id = rx.await.map_err(|_| {
            RiftError::System(crate::error::SystemReject::Internal("actor died".into()))
        })??;
        // Record the subscription in the registry's reverse indices so
        // `unsubscribe` / `drop_sink` / `subscriber_count` can locate
        // the right actor without a broadcast.
        self.registry.register_subscription(id, topic, sink_id);
        Ok(id)
    }

    async fn unsubscribe(&self, id: SubscriptionId) -> Result<bool> {
        let Some(topic) = self.registry.topic_for_subscription(&id) else {
            return Ok(false);
        };
        let actor = self.registry.get_or_spawn(&topic);
        let (tx, rx) = oneshot::channel();
        actor.send(TopicMsg::Unsubscribe { id, reply_to: tx })?;
        let removed = rx.await.map_err(|_| {
            RiftError::System(crate::error::SystemReject::Internal("actor died".into()))
        })??;
        if removed {
            self.registry.unregister_subscription(&id);
        }
        Ok(removed)
    }

    async fn drop_sink(&self, sink_id: u64) -> usize {
        let sids = self.registry.subs_for_sink(sink_id);
        let mut total = 0;
        for sid in sids {
            let Some(topic) = self.registry.topic_for_subscription(&sid) else {
                continue;
            };
            let actor = self.registry.get_or_spawn(&topic);
            let (tx, rx) = oneshot::channel();
            if actor
                .send(TopicMsg::DropSink {
                    sink_id,
                    reply_to: tx,
                })
                .is_ok()
                && let Ok(n) = rx.await
            {
                total += n;
            }
            self.registry.unregister_subscription(&sid);
        }
        total
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
        self.registry.count_subscriptions_for_topic(topic)
    }

    async fn head_offset(&self, topic: &str) -> i64 {
        let actor = self.registry.get_or_spawn(topic);
        let (tx, rx) = oneshot::channel();
        actor.send(TopicMsg::HeadOffset { reply_to: tx }).ok();
        rx.await.unwrap_or(0)
    }

    async fn dec_publisher(&self, _topic: &str) {
        // ActorBroker tracks publisher counts inside each TopicActor;
        // a full implementation would send a ConnectionClosed message
        // so the actor can decrement its own counter. For now this is
        // a no-op so the slot is eventually released only on actor
        // restart.
    }
}
