//! # Key Encoding Helpers for Storage Backends
//!
//! This module provides functions that construct byte-level keys for every
//! higher-level store (offset, log, deduplication, snapshot). All keys use a
//! two-level namespace of the form `<topic_name>\x00<sub_key>`, where `\x00`
//! is the [`SEP`] separator byte.
//!
//! ## Why a Separator Byte?
//!
//! The null byte (`\x00`) acts as an unambiguous boundary between the topic
//! name and the sub-key. This prevents prefix collisions: scanning entries for
//! `"room/5"` will never accidentally match entries for `"room/50"` because
//! the prefix scanned is `room/5\x00`, not the raw string `room/5`.
//!
//! ## Key Formats
//!
//! | Store | Key format | Produced by |
//! |-------|-----------|-------------|
//! | Offset | `<topic>\x00head` | [`offset_key`] |
//! | Log | `<topic>\x00<offset:020>` | [`log_key`] |
//! | Dedupe | `<topic>\x00<message_id>\x00` | [`dedupe_key`] |
//! | Snapshot | `<topic>\x00<snapshot_id>` | [`snapshot_key`] |
//!
//! Each store also has a corresponding `*_prefix` function that returns the
//! topic portion of the key (everything up to and including the separator),
//! suitable for use with [`StorageEngine::scan_prefix`](crate::StorageEngine::scan_prefix).
//!
//! ## Lexicographic Ordering
//!
//! Log offsets are zero-padded to 20 digits so that lexicographic byte
//! ordering matches numeric ordering. This allows efficient range scans
//! over sorted key-value backends (e.g. sled) without a secondary index.

/// Separator byte placed between the topic name and the sub-key in every
/// encoded key. Using `\x00` (NUL) ensures that topic prefixes never collide
/// with each other regardless of their content.
pub const SEP: u8 = 0x00;

// ── Offset keys ──────────────────────────────────────────────

/// Build the key for a topic's current head offset.
///
/// The head offset is the highest offset that has been allocated for the
/// topic. The resulting key has the format `<topic>\x00head`.
///
/// # Parameters
///
/// - `topic` -- the topic name.
///
/// # Returns
///
/// A `Vec<u8>` containing the encoded key.
pub fn offset_key(topic: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(topic.len() + 6);
    k.extend_from_slice(topic.as_bytes());
    k.push(SEP);
    k.extend_from_slice(b"head");
    k
}

/// Build a prefix that matches all entries belonging to `topic` in the
/// offset namespace.
///
/// This prefix is passed to
/// [`StorageEngine::scan_prefix`](crate::StorageEngine::scan_prefix)
/// to retrieve every offset-related entry for the given topic.
///
/// # Parameters
///
/// - `topic` -- the topic name.
pub fn offset_prefix(topic: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(topic.len() + 1);
    k.extend_from_slice(topic.as_bytes());
    k.push(SEP);
    k
}

// ── Log keys ─────────────────────────────────────────────────

/// Build the key for a single log entry at the given `offset`.
///
/// The offset is zero-padded to 20 decimal digits so that lexicographic
/// byte ordering matches numeric ordering. The resulting key has the format
/// `<topic>\x00<offset:020>`.
///
/// # Parameters
///
/// - `topic` -- the topic name.
/// - `offset` -- the numeric offset of the log entry.
///
/// # Returns
///
/// A `Vec<u8>` containing the encoded key, ready for point lookups or
/// range scans.
pub fn log_key(topic: &str, offset: i64) -> Vec<u8> {
    let mut k = Vec::with_capacity(topic.len() + 22);
    k.extend_from_slice(topic.as_bytes());
    k.push(SEP);
    k.extend_from_slice(format!("{offset:020}").as_bytes());
    k
}

/// Build a prefix that matches all log entries belonging to `topic`.
///
/// Pass this to
/// [`StorageEngine::scan_prefix`](crate::StorageEngine::scan_prefix)
/// to retrieve every log entry for the given topic.
///
/// # Parameters
///
/// - `topic` -- the topic name.
pub fn log_prefix(topic: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(topic.len() + 1);
    k.extend_from_slice(topic.as_bytes());
    k.push(SEP);
    k
}

/// Build the **inclusive** start key for a log range scan beginning at
/// `from`.
///
/// This is equivalent to calling [`log_key`] with the `from` offset. It is
/// the lower bound of a `[from, to]` range query.
///
/// # Parameters
///
/// - `topic` -- the topic name.
/// - `from` -- the first offset to include in the range.
pub fn log_range_start(topic: &str, from: i64) -> Vec<u8> {
    log_key(topic, from)
}

/// Build the **exclusive** end key for a log range scan ending at `to`.
///
/// The returned key is one byte past the key for offset `to`, so that a
/// prefix scan up to (but not including) this key will include `to` but
/// exclude `to + 1`. This is achieved by appending a `0xFF` byte to the
/// standard log key.
///
/// # Parameters
///
/// - `topic` -- the topic name.
/// - `to` -- the last offset to include in the range.
pub fn log_range_end(topic: &str, to: i64) -> Vec<u8> {
    // Append a byte past the 20-digit zero-padded offset so that
    // scanning up to this key includes offset `to` but not `to+1`.
    let mut k = log_key(topic, to);
    k.push(0xFF);
    k
}

// ── Dedupe keys ──────────────────────────────────────────────

/// Build the key for a single deduplication entry.
///
/// The resulting key has the format `<topic>\x00<message_id>\x00`. The
/// trailing separator ensures that entries for different message IDs under
/// the same topic are cleanly separated.
///
/// # Parameters
///
/// - `topic` -- the topic name.
/// - `message_id` -- the deduplication key (typically a unique message ID).
pub fn dedupe_key(topic: &str, message_id: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(topic.len() + 1 + message_id.len() + 1);
    k.extend_from_slice(topic.as_bytes());
    k.push(SEP);
    k.extend_from_slice(message_id.as_bytes());
    k.push(SEP);
    k
}

/// Build a prefix that matches all deduplication entries belonging to
/// `topic`.
///
/// Pass this to
/// [`StorageEngine::scan_prefix`](crate::StorageEngine::scan_prefix)
/// to iterate over every deduplication entry for the given topic (e.g. for
/// sweeping expired entries).
///
/// # Parameters
///
/// - `topic` -- the topic name.
pub fn dedupe_prefix(topic: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(topic.len() + 1);
    k.extend_from_slice(topic.as_bytes());
    k.push(SEP);
    k
}

// ── Snapshot keys ────────────────────────────────────────────

/// Build the key for a single snapshot entry.
///
/// The resulting key has the format `<topic>\x00<snapshot_id>`. Each topic
/// stores at most one snapshot at a time; capturing a new snapshot replaces
/// the previous one.
///
/// # Parameters
///
/// - `topic` -- the topic name.
/// - `snapshot_id` -- the unique identifier for the snapshot.
pub fn snapshot_key(topic: &str, snapshot_id: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(topic.len() + 1 + snapshot_id.len());
    k.extend_from_slice(topic.as_bytes());
    k.push(SEP);
    k.extend_from_slice(snapshot_id.as_bytes());
    k
}

/// Build a prefix that matches all snapshot entries belonging to `topic`.
///
/// Pass this to
/// [`StorageEngine::scan_prefix`](crate::StorageEngine::scan_prefix)
/// to list or delete all snapshots for the given topic.
///
/// # Parameters
///
/// - `topic` -- the topic name.
pub fn snapshot_prefix(topic: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(topic.len() + 1);
    k.extend_from_slice(topic.as_bytes());
    k.push(SEP);
    k
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offset_key_has_separator() {
        let k = offset_key("room/5");
        assert!(k.windows(2).any(|w| w == [b'/', b'5']));
        assert!(k.contains(&SEP));
    }

    #[test]
    fn log_key_sort_order() {
        let k1 = log_key("t", 9);
        let k2 = log_key("t", 10);
        let k3 = log_key("t", 100);
        assert!(k1 < k2);
        assert!(k2 < k3);
    }

    #[test]
    fn log_key_topic_isolation() {
        let k_a = log_prefix("room/5");
        let k_b = log_prefix("room/50");
        // room/5\x00 should NOT match room/50\x00
        let room5_key = log_key("room/5", 1);
        assert!(room5_key.starts_with(&k_a));
        assert!(!room5_key.starts_with(&k_b));
    }

    #[test]
    fn dedupe_prefix_isolation() {
        let p = dedupe_prefix("t");
        let k = dedupe_key("t", "msg-1");
        assert!(k.starts_with(&p));
    }

    #[test]
    fn snapshot_key_round_trip() {
        let k = snapshot_key("room/1", "snap-abc");
        let p = snapshot_prefix("room/1");
        assert!(k.starts_with(&p));
    }
}
