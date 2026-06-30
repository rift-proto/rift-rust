use std::collections::HashMap;

use rifts_core::message::SubscribeIntent;

/// Tracks which topics the client is subscribed to so they can be
/// re-sent after a reconnect.
#[derive(Debug)]
pub(super) struct SubscriptionTracker {
    topics: HashMap<String, SubscribeIntent>,
}

impl SubscriptionTracker {
    pub(super) fn new() -> Self {
        Self {
            topics: HashMap::new(),
        }
    }

    /// Record a new subscription (or update the mode of an existing one).
    pub(super) fn add(&mut self, topic: &str, mode: SubscribeIntent) {
        self.topics.insert(topic.to_string(), mode);
    }

    /// Remove a subscription. Returns `true` if it existed.
    pub(super) fn remove(&mut self, topic: &str) -> bool {
        self.topics.remove(topic).is_some()
    }

    /// Returns the mode for a topic, if subscribed.
    #[allow(dead_code)]
    pub(super) fn get(&self, topic: &str) -> Option<SubscribeIntent> {
        self.topics.get(topic).copied()
    }

    /// Iterate all tracked subscriptions.
    pub(super) fn iter(&self) -> impl Iterator<Item = (&String, &SubscribeIntent)> {
        self.topics.iter()
    }

    /// Returns the number of tracked subscriptions.
    #[allow(dead_code)]
    pub(super) fn len(&self) -> usize {
        self.topics.len()
    }
}

impl Default for SubscriptionTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_get() {
        let mut t = SubscriptionTracker::new();
        t.add("room/1", SubscribeIntent::Live);
        assert_eq!(t.get("room/1"), Some(SubscribeIntent::Live));
        assert_eq!(t.get("room/2"), None);
    }

    #[test]
    fn remove() {
        let mut t = SubscriptionTracker::new();
        t.add("a", SubscribeIntent::Replay { from: 0 });
        assert!(t.remove("a"));
        assert!(!t.remove("a"));
        assert_eq!(t.len(), 0);
    }

    #[test]
    fn update_mode() {
        let mut t = SubscriptionTracker::new();
        t.add("x", SubscribeIntent::Live);
        t.add("x", SubscribeIntent::Replay { from: 0 });
        assert_eq!(t.get("x"), Some(SubscribeIntent::Replay { from: 0 }));
    }

    #[test]
    fn iter_yields_all() {
        let mut t = SubscriptionTracker::new();
        t.add("a", SubscribeIntent::Live);
        t.add("b", SubscribeIntent::Ephemeral);
        let names: Vec<&str> = t.iter().map(|(k, _)| k.as_str()).collect();
        assert!(names.contains(&"a"));
        assert!(names.contains(&"b"));
    }
}
