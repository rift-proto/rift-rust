//! Redis-backed storage implementations of the four [`rifts_storage`]
//! traits: [`OffsetStore`], [`LogStore`], [`DedupeStore`], and
//! [`SnapshotStore`].
//!
//! Each store delegates to Redis commands via a shared [`RedisPool`].
//!
//! ## Submodules
//!
//! | Module | Trait implemented | Redis data type |
//! |--------|------------------|-----------------|
//! | [`offset`] | [`OffsetStore`] | Hash (`HINCRBY`) |
//! | [`log`] | [`LogStore`] | Sorted Set (`ZADD`, `ZRANGEBYSCORE`) |
//! | [`dedupe`] | [`DedupeStore`] | Set (`SADD`) + `EXPIRE` |
//! | [`snapshot`] | [`SnapshotStore`] | Hash (`HSET`, `HGETALL`) |

pub mod dedupe;
pub mod log;
pub mod offset;
pub mod snapshot;
