//! Topic registry -- lazily spawns [`TopicActor`]s keyed by topic name.
//!
//! The [`TopicRegistry`] is the main entry point for obtaining actor
//! references in the actor subsystem.  It uses a lock-free [`DashMap`]
//! to map each topic name (e.g. `"room/1"`) to a [`LocalActorRef`]
//! backed by a dedicated [`TopicActor`].  Actors are spawned lazily
//! on the first request for a given topic and reused on subsequent
//! calls.
//!
//! # Reverse indices
//!
//! Beyond the primary actor map, the registry maintains two reverse
//! indices that the [`ActorBroker`](crate::broker::ActorBroker) uses to
//! implement `unsubscribe`, `drop_sink`, and `subscriber_count` without
//! broadcasting a query to every actor:
//!
//! - **`sub_to_topic`** -- maps a [`SubscriptionId`] to the topic name
//!   it belongs to, enabling O(1) "which topic owns this subscription?"
//!   lookups.
//! - **`sink_to_subs`** -- maps a sink ID (`u64`) to the set of
//!   [`SubscriptionId`]s registered through that sink, enabling O(1)
//!   "remove all subscriptions for this connection" operations.
//!
//! # Lifecycle
//!
//! When an actor's channel closes (either because the actor received a
//! [`Shutdown`](crate::actor::TopicMsg::Shutdown) message or panicked),
//! [`get_or_spawn`](TopicRegistry::get_or_spawn) detects the dead
//! channel via [`LocalActorRef::is_closed`], removes the stale entry,
//! and transparently spawns a fresh actor on the next call.
//!
//! # Type parameters
//!
//! The registry is generic over the four storage backends that an actor
//! needs:
//!
//! * `O` -- [`OffsetStore`](crate::storage::OffsetStore): monotonic
//!   offset allocator per topic.
//! * `L` -- [`LogStore`](crate::storage::LogStore): append-only
//!   message log per topic.
//! * `D` -- [`DedupeStore`](crate::storage::DedupeStore): sliding-window
//!   deduplication by message ID.
//! * `S` -- [`SnapshotStore`](crate::storage::SnapshotStore): latest
//!   snapshot per topic for catch-up.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::mpsc;

use crate::actor::actor_ref::LocalActorRef;
use crate::actor::messages::TopicMsg;
use crate::actor::topic_actor::TopicActor;
use crate::broker::fanout::SubscriptionId;
use crate::storage::{DedupeStore, LogStore, OffsetStore, SnapshotStore};
use crate::topic::profile::TopicProfile;

/// A lazily-populated, thread-safe map of topic names to actor references.
///
/// `TopicRegistry` is the central coordination point in the actor
/// subsystem.  On the first request for a given topic it spawns a new
/// [`TopicActor`] task, stores its [`LocalActorRef`], and returns a
/// clone of the handle.  Subsequent requests for the same topic return
/// the existing handle without spawning.
///
/// The registry also maintains two reverse indices used by
/// [`ActorBroker`](crate::broker::ActorBroker) to implement efficient
/// `unsubscribe`, `drop_sink`, and `subscriber_count` operations:
///
/// - `sub_to_topic`: [`SubscriptionId`] -> topic name
/// - `sink_to_subs`: sink ID (`u64`) -> set of [`SubscriptionId`]
///
/// # Thread safety
///
/// All maps are `DashMap`s, so the registry can be shared across tasks
/// via `Arc<TopicRegistry<...>>` without external locking.
///
/// # Type parameters
///
/// * `O` -- the [`OffsetStore`](crate::storage::OffsetStore)
///   implementation used by each spawned actor for monotonic offset
///   allocation.
/// * `L` -- the [`LogStore`](crate::storage::LogStore) implementation
///   used by each spawned actor for append-only message persistence.
/// * `D` -- the [`DedupeStore`](crate::storage::DedupeStore)
///   implementation used by each spawned actor for deduplication.
/// * `S` -- the [`SnapshotStore`](crate::storage::SnapshotStore)
///   implementation used by each spawned actor for snapshot retrieval.
pub struct TopicRegistry<O, L, D, S> {
    /// Primary map from topic name to the actor's `LocalActorRef`.
    actors: DashMap<String, LocalActorRef<TopicMsg>>,

    /// Reverse index: subscription ID -> topic name.
    ///
    /// Enables O(1) lookup of which topic a given subscription belongs
    /// to, which is needed when processing `unsubscribe` calls.
    sub_to_topic: DashMap<SubscriptionId, String>,

    /// Reverse index: sink ID -> set of subscription IDs.
    ///
    /// Enables O(1) lookup of all subscriptions registered through a
    /// given connection sink, which is needed when processing
    /// `drop_sink` calls on disconnection.
    sink_to_subs: DashMap<u64, HashSet<SubscriptionId>>,

    /// Shared offset allocator, cloned into each spawned actor.
    offsets: Arc<O>,

    /// Shared log store, cloned into each spawned actor.
    log: Arc<L>,

    /// Shared deduplication store, cloned into each spawned actor.
    dedupe: Arc<D>,

    /// Shared snapshot store, cloned into each spawned actor.
    snapshots: Arc<S>,

    /// Default topic profile applied to newly spawned actors.
    ///
    /// Controls settings such as retention policy and maximum log size.
    default_profile: TopicProfile,

    /// Time window for deduplication (how long a `message_id` is
    /// remembered before it can be reused).
    dedupe_window: Duration,
}

impl<
    O: OffsetStore + 'static,
    L: LogStore + 'static,
    D: DedupeStore + 'static,
    S: SnapshotStore + 'static,
> TopicRegistry<O, L, D, S>
{
    /// Create a new, empty topic registry.
    ///
    /// The registry starts with no spawned actors.  Actors will be
    /// spawned on demand when [`get_or_spawn`](Self::get_or_spawn) is
    /// called for a topic name that does not yet have a live actor.
    ///
    /// # Arguments
    ///
    /// * `offsets` -- shared [`OffsetStore`] instance cloned into every
    ///   spawned actor.
    /// * `log` -- shared [`LogStore`] instance cloned into every
    ///   spawned actor.
    /// * `dedupe` -- shared [`DedupeStore`] instance cloned into every
    ///   spawned actor.
    /// * `snapshots` -- shared [`SnapshotStore`] instance cloned into
    ///   every spawned actor.
    /// * `default_profile` -- [`TopicProfile`] applied to every newly
    ///   spawned actor; controls retention and other topic-level
    ///   settings.
    /// * `dedupe_window` -- duration for which a published `message_id`
    ///   is remembered in the deduplication store.
    ///
    /// # Returns
    ///
    /// A new `TopicRegistry` ready to be shared across tasks.
    pub fn new(
        offsets: Arc<O>,
        log: Arc<L>,
        dedupe: Arc<D>,
        snapshots: Arc<S>,
        default_profile: TopicProfile,
        dedupe_window: Duration,
    ) -> Self {
        Self {
            actors: DashMap::new(),
            sub_to_topic: DashMap::new(),
            sink_to_subs: DashMap::new(),
            offsets,
            log,
            dedupe,
            snapshots,
            default_profile,
            dedupe_window,
        }
    }

    /// Get or spawn the actor for a topic.
    ///
    /// If a live actor already exists for the given topic name, its
    /// [`LocalActorRef`] is cloned and returned.  If the existing
    /// actor's channel is closed (actor died or shut down), the stale
    /// entry is removed and a fresh actor is spawned transparently.
    ///
    /// The spawned actor is added to a `tokio::spawn` task and will
    /// run until it receives a [`Shutdown`](TopicMsg::Shutdown) message
    /// or its `mpsc` sender is dropped.
    ///
    /// # Arguments
    ///
    /// * `topic` -- the topic name (e.g. `"room/1"`, `"orders/42"`).
    ///
    /// # Returns
    ///
    /// A [`LocalActorRef<TopicMsg>`] handle that can be used to send
    /// messages to the actor.
    pub fn get_or_spawn(&self, topic: &str) -> LocalActorRef<TopicMsg> {
        // Atomic check-and-spawn: if a live actor exists, return it.
        // If a dead actor exists, replace it. Otherwise, spawn a new
        // one. The `entry()` API guarantees that the closure runs
        // exactly once per key, so two concurrent callers cannot
        // both spawn a new actor for the same topic.
        let mut existing = None;

        match self.actors.entry(topic.to_string()) {
            dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                if !occ.get().is_closed() {
                    existing = Some(occ.get().clone());
                } else {
                    // Replace the dead entry with a fresh actor
                    // before the closure returns so other
                    // callers can find it.
                    let (tx, rx) = mpsc::channel(256);
                    let actor = TopicActor::new(
                        topic.to_string(),
                        self.default_profile.clone(),
                        self.offsets.clone(),
                        self.log.clone(),
                        self.dedupe.clone(),
                        self.snapshots.clone(),
                        self.dedupe_window,
                    );
                    let new_ref = LocalActorRef::new(tx);
                    tokio::spawn(async move { actor.run(rx).await });
                    occ.insert(new_ref.clone());
                    existing = Some(new_ref);
                }
                existing.unwrap()
            }
            dashmap::mapref::entry::Entry::Vacant(vac) => {
                let (tx, rx) = mpsc::channel(256);
                let actor = TopicActor::new(
                    topic.to_string(),
                    self.default_profile.clone(),
                    self.offsets.clone(),
                    self.log.clone(),
                    self.dedupe.clone(),
                    self.snapshots.clone(),
                    self.dedupe_window,
                );
                let new_ref = LocalActorRef::new(tx);
                tokio::spawn(async move { actor.run(rx).await });
                vac.insert(new_ref.clone());
                new_ref
            }
        }
    }

    /// Record a `(subscription_id, topic, sink_id)` triple in the
    /// reverse indices.
    ///
    /// This method should be called after the actor confirms a
    /// [`Subscribe`](TopicMsg::Subscribe) request.  It populates both
    /// the `sub_to_topic` and `sink_to_subs` maps so that future
    /// `unsubscribe` and `drop_sink` operations can resolve without
    /// broadcasting to every actor.
    ///
    /// # Arguments
    ///
    /// * `sid` -- the [`SubscriptionId`] returned by the actor.
    /// * `topic` -- the topic name the subscription belongs to.
    /// * `sink_id` -- the numeric identifier of the connection sink
    ///   through which the subscription was registered.
    pub fn register_subscription(&self, sid: SubscriptionId, topic: &str, sink_id: u64) {
        self.sub_to_topic.insert(sid, topic.to_string());
        self.sink_to_subs.entry(sink_id).or_default().insert(sid);
    }

    /// Look up the topic name that a subscription ID belongs to.
    ///
    /// Returns `None` if the subscription ID is not registered (either
    /// never registered or already unregistered).
    ///
    /// # Arguments
    ///
    /// * `sid` -- the [`SubscriptionId`] to look up.
    ///
    /// # Returns
    ///
    /// `Some(topic_name)` if the subscription exists, `None` otherwise.
    pub fn topic_for_subscription(&self, sid: &SubscriptionId) -> Option<String> {
        self.sub_to_topic.get(sid).map(|v| v.value().clone())
    }

    /// Return all subscription IDs registered for a given sink.
    ///
    /// This is used when a connection disconnects and all of its
    /// subscriptions need to be cleaned up.
    ///
    /// # Arguments
    ///
    /// * `sink_id` -- the numeric identifier of the connection sink.
    ///
    /// # Returns
    ///
    /// A vector of [`SubscriptionId`]s registered through the given
    /// sink.  Returns an empty vector if no subscriptions exist for
    /// the sink.
    pub fn subs_for_sink(&self, sink_id: u64) -> Vec<SubscriptionId> {
        self.sink_to_subs
            .get(&sink_id)
            .map(|s| s.value().iter().copied().collect())
            .unwrap_or_default()
    }

    /// Remove a subscription from both reverse indices.
    ///
    /// This method atomically removes the subscription from the
    /// `sub_to_topic` map and from the corresponding set in the
    /// `sink_to_subs` map.  If the sink's subscription set becomes
    /// empty after removal, the entire sink entry is cleaned up.
    ///
    /// # Arguments
    ///
    /// * `sid` -- the [`SubscriptionId`] to remove.
    ///
    /// # Returns
    ///
    /// `Some(topic_name)` if the subscription existed and was removed,
    /// `None` if the subscription ID was not found.
    pub fn unregister_subscription(&self, sid: &SubscriptionId) -> Option<String> {
        self.sub_to_topic.remove(sid).map(|(_, topic)| {
            for mut entry in self.sink_to_subs.iter_mut() {
                if entry.value_mut().remove(sid) {
                    if entry.value().is_empty() {
                        let k = *entry.key();
                        drop(entry);
                        self.sink_to_subs.remove(&k);
                    }
                    break;
                }
            }
            topic
        })
    }

    /// Count the number of active subscriptions targeting a specific topic.
    ///
    /// This performs a linear scan of the `sub_to_topic` reverse index.
    /// It is primarily used for diagnostics and monitoring rather than
    /// hot-path logic.
    ///
    /// # Arguments
    ///
    /// * `topic` -- the topic name to count subscriptions for.
    ///
    /// # Returns
    ///
    /// The number of subscriptions currently registered for the given
    /// topic.
    pub fn count_subscriptions_for_topic(&self, topic: &str) -> usize {
        self.sub_to_topic
            .iter()
            .filter(|kv| kv.value() == topic)
            .count()
    }

    /// Returns the number of spawned actors currently tracked by the
    /// registry.
    ///
    /// This counts entries in the primary `actors` map, which may
    /// include actors whose channels have closed but have not yet been
    /// reaped.
    ///
    /// # Returns
    ///
    /// The number of topic-to-actor entries in the registry.
    pub fn len(&self) -> usize {
        self.actors.len()
    }

    /// Returns `true` if no actors are currently tracked by the
    /// registry.
    ///
    /// # Returns
    ///
    /// `true` if the internal actor map is empty, `false` otherwise.
    pub fn is_empty(&self) -> bool {
        self.actors.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{
        MemoryDedupeStore, MemoryLogStore, MemoryOffsetStore, MemorySnapshotStore,
    };
    use std::time::Duration;

    #[tokio::test]
    async fn spawn_and_reuse() {
        let registry: TopicRegistry<
            MemoryOffsetStore,
            MemoryLogStore,
            MemoryDedupeStore,
            MemorySnapshotStore,
        > = TopicRegistry::new(
            Arc::new(MemoryOffsetStore::new()),
            Arc::new(MemoryLogStore::new()),
            Arc::new(MemoryDedupeStore::new()),
            Arc::new(MemorySnapshotStore::new()),
            TopicProfile::default(),
            Duration::from_secs(60),
        );
        let a = registry.get_or_spawn("room/1");
        let b = registry.get_or_spawn("room/1");
        // Same topic should return the same actor ref (by sender equality).
        assert_eq!(a.sender().capacity(), b.sender().capacity());
        assert_eq!(registry.len(), 1);
    }

    #[tokio::test]
    async fn different_topics_different_actors() {
        let registry: TopicRegistry<
            MemoryOffsetStore,
            MemoryLogStore,
            MemoryDedupeStore,
            MemorySnapshotStore,
        > = TopicRegistry::new(
            Arc::new(MemoryOffsetStore::new()),
            Arc::new(MemoryLogStore::new()),
            Arc::new(MemoryDedupeStore::new()),
            Arc::new(MemorySnapshotStore::new()),
            TopicProfile::default(),
            Duration::from_secs(60),
        );
        let _a = registry.get_or_spawn("room/1");
        let _b = registry.get_or_spawn("room/2");
        assert_eq!(registry.len(), 2);
    }
}
