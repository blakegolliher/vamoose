# Control plane: as-built reference

This document describes the optional operator control plane implemented by
`migration-control-protocol`, `migration-coord`, `migration-worker`, and
`migration-tui`. It is distinct from the S3 shard-claim protocol owned by
`migration-core`; the coordinator observes and controls jobs but never
arbitrates data-plane claim ownership.

Current source and tests take precedence if this reference drifts. The
historical delivery sequence remains in [COORD_PLAN.md](COORD_PLAN.md), while
claim ownership is specified in [CLAIM_PROTOCOL.md](CLAIM_PROTOCOL.md).

## Dependency boundary

```text
 migration-control-protocol ---> migration-worker
             |                 --> migration-coord
             +-------------------> migration-tui

 migration-core ---> migration-mover ---> migration-worker
        +--------------------------------> migration-coord
```

Arrows point from a dependency to its consumer.

- `migration-control-protocol` is the canonical shared contract.
- `migration-worker` uses it for coordinator requests, responses, and events.
  Its normal data-plane dependencies remain `migration-core` and
  `migration-mover`; `migration-coord` is test-only.
- `migration-tui` uses the protocol types and reducer directly. Its normal
  dependency graph contains neither `migration-coord` nor `migration-core` or
  an AWS SDK; integration tests use an in-process coordinator.
- `migration-coord` depends on the protocol for wire/reducer behavior and on
  `migration-core` for the production S3 client and proven conditional-store
  primitives.

The manifests keep coordinator server/runtime dependencies out of the
protocol and TUI client boundary, and keep AWS dependencies out of the TUI.
The TUI's HTTP client may bring client-transport support such as `tower-http`
through `reqwest`; that is not a dependency on coordinator implementation.
See the relevant
[Cargo manifests](../crates/migration-control-protocol/Cargo.toml) and crate
roots for the executable dependency truth.

## Protocol ownership

[`migration-control-protocol`](../crates/migration-control-protocol/src/lib.rs)
owns:

- identifiers, jobs, workers, progress, errors, health, and snapshots;
- event envelopes and kinds, including sequence and attribution fields;
- control modes, command bodies, REST request/response bodies, and SSE query
  types;
- schema versioning and serialization behavior; and
- the deterministic [`Snapshot::apply` reducer](../crates/migration-control-protocol/src/reducer.rs).

The current control-plane schema version is 1. The reducer mutates only its
snapshot argument from an event envelope; it performs no HTTP, object-store,
filesystem, or runtime work. Coordinator live reduction, persisted replay,
and TUI client reduction therefore use the same transition implementation.

Some coordinator bookkeeping fields remain in the version-1 snapshot because
they are already persisted and client-visible. Separating them is a future
versioned change, not a layering cleanup.

## Coordinator ownership

[`migration-coord`](../crates/migration-coord/src/lib.rs) owns the stateful and
I/O-facing side of the contract:

- the `CoordStore` abstraction and production S3 adapter;
- coordinator key layout, event chunks, current/history snapshots, audit
  records, job configuration objects, and archived event chunks;
- lease acquisition, refresh, takeover, release, and loss handling;
- startup replay and sequence-counter restoration;
- runtime state, background cadence, worker eviction, and terminal-job
  archival;
- REST handlers, SSE catch-up/live framing, TLS listeners, authentication,
  authorization, and command audit attribution; and
- graceful shutdown.

The coordinator's S3 prefixes are disjoint from the worker's manifest,
claims, progress, batch, failure, and downgrade prefixes. The exact current
layout is defined in
[`migration-coord/src/layout.rs`](../crates/migration-coord/src/layout.rs).
Replay loads `state/snapshot.json`, discovers event chunks with sequences newer
than the snapshot, merges them by coordinator sequence, and folds them through
the protocol reducer.

Admin operations use bearer-token labels for audit attribution. Worker routes
use the configured cluster secret plus event-kind and worker-identity
validation. Dev mode has no authentication and refuses a non-loopback bind
unless the operator supplies the explicit unsafe override. The beta security
posture is recorded in [BETA_NOTES.md](BETA_NOTES.md).

## Runtime module map

The coordinator runtime is a façade assembled in
[`runtime/mod.rs`](../crates/migration-coord/src/runtime/mod.rs):

| Module | Responsibility |
|---|---|
| [`config.rs`](../crates/migration-coord/src/runtime/config.rs) | `Clock`, `SystemClock`, `RuntimeConfig`, production defaults |
| [`startup.rs`](../crates/migration-coord/src/runtime/startup.rs) | lease acquisition/backoff, persisted replay, writer and bus construction |
| [`ingest.rs`](../crates/migration-coord/src/runtime/ingest.rs) | event sequencing/reduction/buffering, live-bus caps, trailing progress, heartbeat mutation |
| [`query.rs`](../crates/migration-coord/src/runtime/query.rs) | state views, job pagination, worker control, subscriptions, buffered-tail reader, accessors |
| [`persistence.rs`](../crates/migration-coord/src/runtime/persistence.rs) | event flushing, snapshots, audit writes, worker eviction, archive eligibility and execution |
| [`lifecycle.rs`](../crates/migration-coord/src/runtime/lifecycle.rs) | lease-handle state, loss marking, graceful shutdown |
| [`test_clock.rs`](../crates/migration-coord/src/runtime/test_clock.rs) | deterministic integration-test clock |
| [`tests.rs`](../crates/migration-coord/src/runtime/tests.rs) | runtime invariant and compatibility tests |

The public `migration_coord::runtime::*` façade hides the internal module
seams. HTTP lives under `server/`, replay under `state.rs`, event chunking under
`events.rs`, storage under `store.rs`, and background cadence under `ticks.rs`.

## Runtime correctness invariants

### Ingest atomicity

One `tokio::sync::Mutex` guards runtime state. While holding it, ingestion:

1. rejects the operation if lease loss has been observed;
2. allocates the next monotonic coordinator sequence;
3. stamps the envelope;
4. applies the protocol reducer;
5. buffers the envelope in `EventLogWriter`; and
6. updates live-broadcast cap bookkeeping.

Those steps are one critical section. Threshold persistence and broadcast
sends happen after the guard is dropped. Every accepted event reaches live
state and the durable-log buffer before any live-bus rate decision is acted
on.

### Store I/O and flush serialization

Event-chunk PUTs, snapshot/history I/O, and archive I/O run outside the runtime
state mutex. A dedicated flush-token mutex spans event-chunk persistence, so
concurrent threshold, age, endpoint, snapshot, and shutdown flush requests
cannot write overlapping or out-of-order chunks.

Audit-key allocation is a deliberate, low-rate exception: it holds the state
mutex across conditional creates so concurrent admin commands cannot allocate
the same per-day sequence. Audit writes happen after the command event is
durable and use `put_if_absent`, so a restart cannot overwrite an earlier row.

### Flush-before-ack and retry

Event-emitting worker and command endpoints explicitly flush before returning
success. A successful response therefore covers durable event chunks, not only
RAM state. A flush snapshots pending envelopes, performs the PUT, then removes
only the successfully written prefix from the writer. Failed persistence
leaves the envelopes buffered for retry and returns an error to the caller.

The live bus is not the durability boundary. SSE clients may lag and receive a
`Resync` frame; state and the durable log remain complete.

### Lease fencing

The coordinator lease uses the S3-safe conditional create and conditional
delete primitives. Refresh rotates the lease through delete-then-create;
conflicts and storage failures fail closed as lease loss. Once runtime lease
loss is observed, the old coordinator refuses event ingestion, log flush,
snapshot writes, archives, audit writes, and lease release. This prevents a
deposed process from overwriting or deleting successor-owned state.

### Snapshot eviction and archive ordering

Disconnected worker rows become eviction candidates only at snapshot time. A
pruned row is removed from live state only after the pruned snapshot is
durable. The runtime rechecks the current row after I/O, so a worker that
rejoined while the snapshot was in flight survives; its newer event also
replays above the snapshot boundary.

A terminal job becomes archive-eligible only after a durable snapshot covers
its terminal phase. The runtime refuses to archive a job that still has
buffered events. Archival copies each hot event chunk to `archivelogs/` before
deleting the original and retries partial work on a later tick. Replay does not
read the archive; restoring archived history is not implemented.

### Live broadcast versus durable history

State and event-log buffering receive every ingested event. Rate limits apply
only to the broadcast channel:

- progress is capped per job/worker, with the latest suppressed frame retained
  as a trailing edge;
- error frames are capped per class/window; and
- other event kinds broadcast immediately.

Trailing frames keep their original sequence numbers and are emitted in
sequence order. Broadcast sends occur outside the state mutex. The result is a
bounded live display without weakening replay or aggregate accounting.

### Subscriber and buffered-tail lifecycle

SSE subscribes before catch-up, then merges the persisted log and the writer's
unflushed tail before following the live receiver. Sequence filtering removes
overlap between those sources. The tail reader shares access to the writer
state but does not retain the broadcast sender, so dropping all
`CoordRuntime` handles still closes the event bus even if an SSE body or tail
reader remains alive.

## Worker relationship

When `[coord]` is present, the worker's coordinator driver registers the
process, sends heartbeats, buffers allowed worker events, reports self-fencing,
and updates local run control from heartbeat responses. Current workers stamp
events with monotonically increasing client sequences. The coordinator
validates the whole batch before ingestion, binds worker-attributed kinds to
the URL identity, and skips already-applied client sequences using a
high-water mark reconstructed by replay.

The coordinator is not in the copy or claim commit path. A missing `[coord]`,
temporary coordinator outage, or TUI absence does not transfer claim authority
away from the worker's S3 protocol.

Job provisioning is manifest-driven: `vamoose coord` seeds one `JobCreated`
event from the bucket's `manifest.json` (id = run id, source/dest/totals from
the manifest) once the manifest exists, skipping the seed when replay already
holds the job. An explicit id (`--seed-job`, `[coord] job_id`) is accepted for
tests and unusual layouts. Worker registration for a job that is not seeded
yet returns 404, which the worker treats as "wait and retry", not as a fatal
misconfiguration. There is no REST route that creates a job.

## TUI relationship

The TUI performs a consistency-checked REST bootstrap: it reads the
coordinator cursor before and after walking job/worker/error pages and accepts
the snapshot only when the cursors match. It then resumes SSE from the maximum
known cursor. A torn bootstrap retries; an SSE `Resync` causes immediate REST
re-bootstrap. This remains a client-side workaround until a coordinator
`as_of_seq` snapshot boundary is designed.

Every decoded protocol event passes through the same protocol reducer used by
the coordinator. Unknown event kinds still advance the TUI resume cursor and
counter without attempting a protocol transition. TUI-only navigation,
connection, activity-history, and render state live outside the protocol
snapshot and survive snapshot replacement.

## Compatibility surfaces

The canonical import path is `migration_control_protocol::schema`. During the
transition:

- `migration_coord::schema::*` re-exports the exact protocol types;
- the coordinator's `server::worker`, `server::read`, `server::command`, and
  `server::stream` modules re-export their established DTO paths; and
- `migration_worker::coord_client::ControlMode` remains a compatibility
  re-export.

These are aliases, not duplicated definitions. Removal is a separately
announced breaking cleanup after downstream consumers migrate; it is tracked
in [NEXT.md](NEXT.md).

## Related references

- [DESIGN.md](../DESIGN.md) — whole-system architecture
- [CLAIM_PROTOCOL.md](CLAIM_PROTOCOL.md) — data-plane S3 ownership protocol
- [CORRECTNESS_RULES.md](CORRECTNESS_RULES.md) — cross-cutting correctness rules
- [BETA_NOTES.md](BETA_NOTES.md) — security posture and operator limitations
- [COORD_PLAN.md](COORD_PLAN.md) — historical delivery plan
- [`migration-control-protocol` crate docs](../crates/migration-control-protocol/src/lib.rs) — wire/reducer ownership
