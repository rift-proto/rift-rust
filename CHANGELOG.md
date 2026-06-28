# Changelog

## [0.2.0] — 2026-06-28

### Added

- **Persistence layer** — pluggable storage traits (`StorageEngine`, `LogStore`,
  `OffsetStore`, `DedupeStore`, `SnapshotStore`) with full `sled` backend
  implementation.  Enables durable topic data across restarts.  ([3baf2dc])
- **RemoteBroker** — async `Broker` trait + TCP-based remote broker for
  distributing messages across process boundaries.  ([e7972ff])
- **Actor runtime** — `TopicActor`, `TopicRegistry`, `LocalActorRef`,
  `ActorBroker` providing actor-based broker topology for complex routing
  scenarios.  ([7ed2799])

### Fixed

- **CI** — clippy warnings resolved, `--D warnings` now passing with allow
  flags for `large_enum_variant` and `module_inception`.  ([038d50b],
  [f0b4261], [3011b0b], [7aad374])
- **Doc links** — all intra-crate doc links corrected, including sled-gated
  type references that were broken in default (non-sled) builds.  ([3011b0b])

### Documentation

- Full architecture guide, getting-started, protocol reference, and examples
  rewritten for the v0.2 module structure (72 files, four-layer model).
  ([653013d])

[3baf2dc]: https://github.com/lazhenyi/rifts/commit/3baf2dc
[e7972ff]: https://github.com/lazhenyi/rifts/commit/e7972ff
[7ed2799]: https://github.com/lazhenyi/rifts/commit/7ed2799
[038d50b]: https://github.com/lazhenyi/rifts/commit/038d50b
[f0b4261]: https://github.com/lazhenyi/rifts/commit/f0b4261
[3011b0b]: https://github.com/lazhenyi/rifts/commit/3011b0b
[7aad374]: https://github.com/lazhenyi/rifts/commit/7aad374
[653013d]: https://github.com/lazhenyi/rifts/commit/653013d

## [0.1.0] — 2025-06-28

Initial release — section 29 minimum compliant implementation of Rift
Realtime Protocol / 1.0.

### Features

- Frame envelope, types, flags, codec, priority
- CBOR + JSON codecs with protocol-level negotiation
- Eight message classes (event, command, reply, state, snapshot, datagram,
  stream, ack)
- Topic profiles, retention policies, ordering policies, in-memory topic store
- Session lifecycle, token auth, resume, offset tracking
- In-memory broker with router, fanout, dedupe, snapshots
- Nine acknowledgement types
- Backpressure controller + token-bucket rate limiter
- Transport abstraction + standalone WebSocket transport (tokio-tungstenite)
- Framework adapters: axum, actix-web, warp, ntex
- `RiftServer` builder and event loop
- Connection state machine (spec §5 lifecycle)
- Metrics collection
- Structured logging via `tracing`
