//! Topic actor — owns a single topic's state and processes requests
//! sequentially via an mpsc actor loop.
//!
//! One actor per topic.  No locks needed — the actor loop is
//! single-threaded.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{Semaphore, mpsc};
use tracing::warn;
use uuid::Uuid;

use crate::actor::messages::TopicMsg;
use crate::broker::broker::PublishOutcome;
use crate::broker::fanout::{ConnectionSink, SubscribeIntent, SubscriptionId};
use crate::error::{Result, RiftError};
use crate::frame::Frame;
use crate::message::MessageClass;
use crate::now_ms;
use crate::storage::{DedupeStore, LogStore, OffsetStore, SnapshotStore};
use crate::topic::profile::TopicProfile;
use crate::topic::store::LogEntry;
#[allow(dead_code)]
/// A local subscriber stored in the actor's subscriber map.
struct LocalSink {
    sink: ConnectionSink,
    intent: SubscribeIntent,
}

/// An actor that owns a single topic's state.
pub struct TopicActor<O: OffsetStore, L: LogStore, D: DedupeStore, S: SnapshotStore> {
    topic_name: String,
    profile: TopicProfile,
    offsets: Arc<O>,
    #[allow(dead_code)]
    log: Arc<L>,
    dedupe: Arc<D>,
    #[allow(dead_code)]
    snapshots: Arc<S>,
    subscribers: HashMap<SubscriptionId, LocalSink>,
    next_sub_id: u64,
    fanout_semaphore: Arc<Semaphore>,
    dedupe_window: Duration,
}

impl<O: OffsetStore, L: LogStore, D: DedupeStore, S: SnapshotStore> TopicActor<O, L, D, S> {
    /// Create a new topic actor.
    pub fn new(
        topic_name: String,
        profile: TopicProfile,
        offsets: Arc<O>,
        log: Arc<L>,
        dedupe: Arc<D>,
        snapshots: Arc<S>,
        dedupe_window: Duration,
    ) -> Self {
        Self {
            topic_name,
            profile,
            offsets,
            log,
            dedupe,
            snapshots,
            subscribers: HashMap::new(),
            next_sub_id: 1,
            fanout_semaphore: Arc::new(Semaphore::new(64)),
            dedupe_window,
        }
    }

    /// Run the actor loop.  Returns when the mpsc receiver is closed
    /// or a `Shutdown` message is received.
    pub async fn run(mut self, mut rx: mpsc::Receiver<TopicMsg>) {
        while let Some(msg) = rx.recv().await {
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.handle_message(msg)));
            match result {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {
                    // Error already sent back via oneshot.
                }
                Err(_panic) => {
                    warn!(
                        topic = %self.topic_name,
                        "topic actor panicked — continuing"
                    );
                }
            }
        }
    }

    fn handle_message(&mut self, msg: TopicMsg) -> Result<()> {
        match msg {
            TopicMsg::Publish { frame, reply_to } => {
                let outcome = self.handle_publish(&frame);
                let _ = reply_to.send(outcome);
            }
            TopicMsg::Subscribe {
                sink,
                intent,
                reply_to,
            } => {
                let id = SubscriptionId(self.next_sub_id);
                self.next_sub_id += 1;
                self.subscribers.insert(id, LocalSink { sink, intent });
                let _ = reply_to.send(Ok(id));
            }
            TopicMsg::Unsubscribe { id, reply_to } => {
                let ok = self.subscribers.remove(&id).is_some();
                let _ = reply_to.send(Ok(ok));
            }
            TopicMsg::Replay { from, to, reply_to } => {
                let entries = self.log.range(&self.topic_name, from, to);
                let payloads: Vec<Bytes> = entries.into_iter().map(|e| e.payload).collect();
                let _ = reply_to.send(Ok(payloads));
            }
            TopicMsg::Snapshot { reply_to } => {
                let latest = self.log.latest(&self.topic_name);
                let snap = latest.map(|e| crate::storage::StoredSnapshot {
                    snapshot_id: Uuid::new_v4().to_string(),
                    topic: self.topic_name.clone(),
                    base_offset: e.offset,
                    payload: e.payload.clone(),
                    created_at: now_ms(),
                    expires_at: None,
                });
                let _ = reply_to.send(Ok(snap));
            }
            TopicMsg::HeadOffset { reply_to } => {
                let _ = reply_to.send(self.offsets.head(&self.topic_name));
            }
            TopicMsg::DropSink { sink_id, reply_to } => {
                let count = self
                    .subscribers
                    .iter()
                    .filter(|(_, sub)| sub.sink.id() == sink_id)
                    .map(|(id, _)| *id)
                    .collect::<Vec<_>>()
                    .len();
                self.subscribers.retain(|_, sub| sub.sink.id() != sink_id);
                let _ = reply_to.send(count);
            }
            TopicMsg::Shutdown { reply_to } => {
                let _ = reply_to.send(());
                // Returning here will drop self, closing the channel,
                // ending the actor loop.
            }
        }
        Ok(())
    }

    fn handle_publish(&mut self, frame: &Frame) -> Result<PublishOutcome> {
        if frame.topic.is_none() {
            return Err(RiftError::Frame(
                crate::error::FrameReject::RequiredFieldMissing("topic"),
            ));
        }
        let message_id = frame.message_id.as_deref().ok_or_else(|| {
            RiftError::Frame(crate::error::FrameReject::RequiredFieldMissing(
                "message_id",
            ))
        })?;

        // Dedupe.
        let mut duplicate = false;
        if !self
            .dedupe
            .check_and_record(&self.topic_name, message_id, self.dedupe_window)
        {
            duplicate = true;
        }

        let offset = self.offsets.alloc(&self.topic_name);
        let entry = LogEntry {
            offset,
            publisher_session: frame.session_id.clone(),
            message_id: message_id.to_string(),
            class: frame
                .event
                .clone()
                .unwrap_or_else(|| MessageClass::Event.as_str().to_string()),
            event: frame.event.clone(),
            payload: frame.payload.clone().unwrap_or_default(),
            timestamp: frame.timestamp,
        };
        self.log
            .append(&self.topic_name, entry, self.profile.retention);

        // Fanout.
        if !duplicate {
            let payload = frame.payload.clone().unwrap_or_default();
            for sub in self.subscribers.values() {
                let payload = payload.clone();
                let sink = sub.sink.clone();
                let sem = self.fanout_semaphore.clone();
                tokio::spawn(async move {
                    let _permit = sem.acquire().await;
                    let _ = sink.deliver(payload);
                });
            }
        }

        Ok(PublishOutcome { offset, duplicate })
    }
}
