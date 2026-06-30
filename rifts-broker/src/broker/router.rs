//! Topic router — spec section 22.3.
//!
//! This module defines the [`TopicRouter`] trait and its primary
//! implementation [`LocalRouter`]. A router is responsible for
//! resolving a topic name into a [`Route`] that contains the
//! [`TopicEntry`] handle for that topic.
//!
//! # Design motivation
//!
//! In a single-process deployment, the router is a thin lookup layer:
//! every topic lives in the local [`TopicStore`], and resolution is a
//! simple `get_or_create` call. The trait abstraction exists so that a
//! future distributed deployment can plug in a hash-based or
//! affinity-based router (e.g. consistent hashing across broker
//! shards) without changing any call-sites in the broker or server
//! layers.
//!
//! # Auto-creation
//!
//! The [`LocalRouter`] creates topics on demand when they are first
//! referenced. The new topic is initialized with the default profile
//! provided at router construction time via the
//! `default_profile_factory` closure. This lazy creation pattern
//! means producers and consumers do not need to pre-register topics.

use std::sync::Arc;

use rifts_core::topic::TopicEntry;
use rifts_core::topic::TopicStore;

/// A routing decision produced by a [`TopicRouter`].
///
/// Contains the [`TopicEntry`] handle that the broker should use for
/// all topic-level operations (publish permission checks, subscriber
/// counts, profile lookups, etc.).
#[derive(Debug, Clone)]
pub struct Route {
    /// The topic entry that owns the message. This handle provides
    /// access to the topic's profile, publisher/subscriber counters,
    /// and internal state.
    pub entry: Arc<TopicEntry>,
}

/// Trait for resolving topic names to routing decisions.
///
/// Implementations must be both [`Send`] and [`Sync`] so they can be
/// shared across async tasks. The `route` method is called for every
/// publish and subscribe operation, so implementations should aim for
/// low latency.
///
/// # Arguments for `route`
///
/// * `topic` — The topic name to resolve.
/// * `routing_key` — An optional routing key for future use (e.g.
///   partition key for hash-based routing). Currently unused by the
///   [`LocalRouter`] implementation.
pub trait TopicRouter: Send + Sync {
    /// Resolve a topic name to a [`Route`], or return `None` if the
    /// topic cannot be resolved (e.g. the name is invalid or the
    /// topic does not exist and cannot be created).
    fn route(&self, topic: &str, routing_key: Option<&str>) -> Option<Route>;
}

/// Single-process topic router that resolves topics via the local
/// [`TopicStore`].
///
/// When `route` is called, the router looks up the topic in the store
/// and creates it on demand if it does not yet exist. New topics are
/// initialized with the default profile produced by the
/// `default_profile_factory` closure provided at construction time.
///
/// # Examples
///
/// ```ignore
/// use std::sync::Arc;
/// use rifts::broker::router::LocalRouter;
/// use rifts::topic::{TopicStore, TopicProfile};
///
/// let store = TopicStore::new();
/// let router = LocalRouter::new(store, Arc::new(TopicProfile::default));
/// let route = router.route("orders", None).unwrap();
/// assert_eq!(route.entry.name, "orders");
/// ```
pub struct LocalRouter {
    /// The local topic store that holds all topic entries.
    pub store: TopicStore,
    /// Factory closure that produces the default [`TopicProfile`](rifts_core::topic::TopicProfile)
    /// for newly created topics. Called each time a new topic is
    /// encountered.
    pub default_profile_factory: Arc<dyn Fn() -> rifts_core::topic::TopicProfile + Send + Sync>,
}

impl std::fmt::Debug for LocalRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalRouter")
            .field("store", &self.store)
            .field("default_profile_factory", &"<fn>")
            .finish()
    }
}

impl LocalRouter {
    /// Create a new local router backed by the given topic store.
    ///
    /// # Arguments
    ///
    /// * `store` — The [`TopicStore`] to use for topic lookups and
    ///   creation.
    /// * `default_profile_factory` — A closure that returns the
    ///   default [`TopicProfile`](rifts_core::topic::TopicProfile) to apply
    ///   when a new topic is auto-created. The closure is called each
    ///   time a previously unseen topic name is encountered.
    pub fn new(
        store: TopicStore,
        default_profile_factory: Arc<dyn Fn() -> rifts_core::topic::TopicProfile + Send + Sync>,
    ) -> Self {
        Self {
            store,
            default_profile_factory,
        }
    }
}

impl TopicRouter for LocalRouter {
    fn route(&self, topic: &str, _routing_key: Option<&str>) -> Option<Route> {
        let entry = self
            .store
            .get_or_create(topic, (self.default_profile_factory)())
            .ok()?;
        Some(Route { entry })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_router_creates_topic() {
        let store = TopicStore::new();
        let router = LocalRouter::new(store, Arc::new(rifts_core::topic::TopicProfile::default));
        let route = router.route("room/1", None).unwrap();
        assert_eq!(route.entry.name, "room/1");
    }
}
