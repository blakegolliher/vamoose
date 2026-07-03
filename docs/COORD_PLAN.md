# vamoose Coordinator + TUI — 6-Phase Build Plan

> **Status: delivered.** All six phases shipped and merged to `main`
> (PR #7, 2026-05-30). This document remains the design record for
> the coord daemon, the TUI, and the supply-chain CI wiring; the
> forward-looking phrasing below is historical.

This document is the working plan for `vamoose coord` (control plane) and
`vamoose tui` (operator UI). It is paired with the build prompt; this is the
internal-engineering view that records decisions, conflicts with existing
code, sequencing, and acceptance gates. Web dashboard is out of scope here.

Companion docs:
- `DESIGN.md` — overall vamoose architecture (the worker side)
- `docs/CLAIM_PROTOCOL.md` — v2 conditional-PUT primitives (lease reuses these)
- `docs/CORRECTNESS_RULES.md` — R-rules the coord must not break

Licensing: workspace already declares AGPL-3.0-only. New source files
follow the existing convention — module-level `//!` doc comment, license
inherited from workspace `Cargo.toml`. `cargo about`/`cargo deny` wiring
for a `THIRD_PARTY_LICENSES` artifact is a Phase 6 task.

---

## 0. Existing repo, briefly

```
crates/
  migration-core/   # shared types, S3 client (claim v2 — VAST-aware), parquet schema
  migration-mover/  # libnfs + io_uring file copy engine
  migration-worker/ # mig-worker binary: orchestrator, shard_processor, heartbeat
  migration-aggr/   # mig-aggr binary: TUI (stub), prometheus, S3-poll summary, verify
  mig-walker-rewrite
  vamoose-cli/      # single `vamoose` binary; subcommand dispatch into the above
```

Workspace dependencies already present and reusable: `aws-sdk-s3` v1,
`tokio` (multi-thread), `serde`, `serde_json`, `clap`, `chrono`, `uuid`,
`ratatui` 0.28, `crossterm` 0.28, `prometheus`, `hyper` v1, `async-trait`,
`anyhow`/`thiserror`, `tracing`, `tracing-subscriber`.

New dependencies the build needs:

| Need                          | Choice (recommended)                 | Notes |
|-------------------------------|--------------------------------------|-------|
| HTTP server (REST + SSE)      | `axum` 0.7 + `tower-http`            | Idiomatic, integrates with `hyper` v1 we already pull. |
| HTTP client (TUI → coord)     | `reqwest` 0.12 (rustls, stream)      | Picks up the same rustls family as our S3 wiring. |
| SSE on the client             | Hand-rolled over `reqwest` byte stream | `eventsource-client` is one option but drags its own old hyper; the SSE wire format is ~30 lines of parser. |
| TLS                           | `rustls` (already in workspace)      | Reuse `migration-core::s3` insecure-TLS knob shape for lab. |
| Token labels                  | `serde` + plain file I/O             | Admin-tokens file format: `<token> <label>` per line; label optional. |

Decisions intentionally pinned at the workspace `Cargo.toml` layer so every
crate uses the same versions.

---

## 1. Conflicts with the existing system (read this first)

These are points where the spec collides or interacts with code that
already exists. Each has a decision and a justification.

### 1.1 S3 layout: worker writes `progress/` flat; coord wants `jobs/{id}/`

**Today.** `migration-core::layout` keys all live at the bucket root:

```
manifest.json
index/<shard>.parquet
shards/<shard>.parquet.claim
progress/host-<id>.json
batches/host-<id>.jsonl
failures/host-<id>/<shard-stem>-e<epoch>.jsonl
downgrades/host-<id>/<shard-stem>-e<epoch>.jsonl
```

Single migration = single bucket.

**Build prompt asks.** `jobs/{job_id}/config.json`, `events/{job_id}/...`,
`archivelogs/{job_id}/...`, plus `coord/lease`, `state/snapshot.json`,
`audit/` — all under one `{bucket}/{prefix}/`.

**Conflict.** If the coord state bucket and the job (worker) bucket are the
same, the worker keys (`manifest.json`, `index/...`, `progress/...`) live
flat at the root while the coord's job tree wants `jobs/{job_id}/`.

**Decision (locked).** *Same bucket, coord under disjoint prefixes.* The
coord and the worker share one bucket. Coord owns:

```
coord/lease
state/snapshot.json
state/snapshot-<ts>.json
events/_cluster/<seq:020>.jsonl
events/<job_id>/<seq:020>.jsonl
jobs/<job_id>/config.json
jobs/<job_id>/files/failed.jsonl
audit/<YYYY-MM-DD>/<seq>.jsonl
archivelogs/<job_id>/
```

Worker continues to write at the bucket root exactly as today:
`manifest.json`, `index/<shard>.parquet`, `shards/<shard>.parquet.claim`,
`progress/host-<id>.json`, `batches/host-<id>.jsonl`,
`failures/host-<id>/<shard-stem>-e<epoch>.jsonl`.

The prefixes are disjoint — no key from `migration-core::layout` can
collide with any key under `coord/`, `state/`, `events/`, `jobs/`,
`audit/`, or `archivelogs/`. We add a unit test in
`migration-coord::layout` asserting this for every prefix constant.

**Multi-job, single coord bucket.** v1 takes the simple line: one bucket
holds *one* job's worker data AND coord state for *that* job. Many
concurrent jobs = many buckets, one coord process can manage all of
them. Coord config carries a list of buckets to manage (or "scan-and-
discover" from a parent prefix in a future revision). The
`jobs/<job_id>/config.json` for each bucket records the job's identity;
`<job_id>` does not need to be globally unique across buckets in v1.

> Tradeoff acknowledged: multi-job-in-one-bucket would need worker writes
> to move under `jobs/<job_id>/...` and re-plumb claim/aggr — we are
> *not* doing that in v1. Revisit if operators ask for it.

### 1.2 Worker `host_id` vs. coord `WorkerId`

**Today.** Worker has `host_id` derived from hostname + pid in
`migration-worker`. It's used as the S3 key suffix (`progress/host-X.json`).

**Build prompt asks.** `Worker.id: WorkerId`, assigned by coord at
register time (`POST /workers/register` returns it).

**Decision.** Coord-assigned `WorkerId` is a UUID v4 minted at register
time. Worker also reports its `host_id` to coord (free-form string,
stored on `Worker.host`); coord prefers `WorkerId` for routing, falls
back to `host_id` for human display. Re-register on restart gets a *new*
`WorkerId` — coord deduplicates by `host_id + pid + start_time` to mark
the prior `WorkerId` as `Disconnected`.

### 1.3 Commanding workers — Phase 1 is observation-only

**Tension.** `POST /jobs/{id}/pause` implies coord commands flow to
workers. Today workers run autonomously off shard claims.

**Decision.** Phase 1 = read-only state plane (no commanding). Phase 3
adds worker→coord registration/heartbeat/events. Phase 2 implements the
REST surface but pause/resume/cancel return `202 Accepted` and write an
event; the actual worker-side enforcement lands as part of Phase 3's
worker-to-coord integration. This staging keeps the early acceptance
tests pure-coord and avoids cross-cutting worker changes too early.

### 1.4 Existing `mig-aggr tui` stub

**Today.** `crates/migration-aggr/src/tui.rs` is `todo!()`. The aggr
binary polls S3 directly for `progress/` and `failures/` summaries.

**Decision (locked).** New crate `migration-tui` for the coord-driven
TUI. The old aggr S3-polling TUI stub stays a stub (it was never built);
when we want one-shot S3 summaries those subcommands (`summary`,
`verify`, `inspect`, `clean-partials`) continue to live in
`migration-aggr` unchanged. `vamoose tui` in the unified CLI dispatches to
`migration-tui`. `vamoose aggr` keeps its current shape.

### 1.5 Claim-protocol R-rules

The coord does not touch shard claims (R4/R6/R7/R8 stay the worker's
problem). The *only* claim-like primitive the coord uses is `coord/lease`
— and it uses the same delete-then-create takeover pattern documented in
the v2 claim protocol. Lease itself does not interact with R-rules
because there is no source/dest filesystem state behind it.

### 1.6 Existing `migration-mover` is libnfs-only

Coord and TUI have no FFI dependencies. They link to nothing beyond
`migration-core` for the S3 client. The `links = "nfs"` constraint in
`migration-mover` means we **must not** add coord/TUI as a dependent of
`migration-mover`. Composition stays through `vamoose-cli`.

### 1.7 VAST S3 quirks — already absorbed

`migration-core::s3` already encodes the "no `PUT If-Match`" reality and
implements `put_if_absent` + `delete_if_match`. The coord lease reuses
both verbatim; no new VAST-specific primitives.

### 1.8 Single-tenant; multi-admin tokens

Today there is no auth on `mig-aggr` (it's a sidecar). The coord
introduces:
- `Authorization: Bearer <admin-token>` — multi-token, all full
  authority. Token file format: `<token>\t<label>` per line, label
  optional; labels show up in `audit/`.
- `X-Cluster-Secret: <shared-secret>` — workers only. One per
  deployment, env-loaded.
- TLS terminates at coord. We don't add a reverse-proxy escape hatch
  for v1 — operator can front coord with one if they want.

No conflict, just additive.

---

## 2. Phase plan with acceptance gates

Each phase has a single PR target (or a small stack) and an explicit
acceptance bar before the next phase starts.

### Phase 1 — schema + S3 persistence  *(this PR)*

Goal: in-process coord daemon that can persist its own state, replay it,
and survive a restart. No worker interaction yet, no HTTP yet.

Deliverables:
1. New crate `migration-coord` (lib only — no binary yet).
2. Schema types (`Job`, `Worker`, `Event`, `ErrorBucket`, `Phase`,
   `WorkerState`, `ErrorClass`, `Progress`, `Health`, `EtaEstimate`,
   `ThroughputWindows`) with serde + round-trip tests.
3. S3 layout module + writers under `coord/`, `state/`, `events/`,
   `jobs/`, `audit/`, `archivelogs/`.
4. Lease (`coord/lease`) with acquire/refresh/takeover semantics.
5. Snapshot writer with cadence policy (1000 events OR 5min, whichever
   first; mirror cadence for event-log chunking).
6. Event-log writer with `{seq:020}.jsonl` chunking.
7. In-memory state struct + reducer (event → state mutation).
8. Replay routine: snapshot load → events since `last_seq` → state.
9. Archive-on-completion: move `events/<job_id>/` → `archivelogs/<job_id>/`.
10. Trait-based fake `S3Like` for unit tests; integration tests against
    VAST S3 (var204).

Acceptance:
- Start fake coord (in-process), create some jobs, write events, snapshot,
  drop the process, restart — state reconstitutes byte-identical (modulo
  `last_seen` timestamps from re-replay).
- Snapshot fires at both thresholds.
- `JobCompleted` rolls the event log to `archivelogs/`. *(Delivered by
  ledger F23 — see `work-items/COORD_ARCHIVE_WIRING.md`: archive runs
  on the snapshot tick after the terminal phase is durable in the
  snapshot, covering `JobCompleted`, `JobFailed`, and `JobCancelled`.)*
- Lease acquire/refresh/takeover all succeed against var204 specifically.
- Two concurrent coord starts — exactly one wins; loser backs off.

Estimated size: ~2.5 kLOC across `migration-coord/src/` (schema ~600,
persistence ~700, lease ~250, replay ~300, tests ~500, glue ~150).

### Phase 2 — coordinator subcommand

Goal: `vamoose coord` is a real network process.

Deliverables:
1. `migration-coord` gains an `axum`-based HTTP layer (REST + SSE).
2. `vamoose coord` subcommand in `vamoose-cli`.
3. REST endpoints (read + command — commands write events but workers
   don't enforce yet; that lands in Phase 3).
4. SSE `/stream` with `Last-Event-ID` resume and 15s keepalive pings.
5. Admin-token + cluster-secret auth middleware; audit log writes.
6. Sequence assignment is coord-side (monotonic `seq: u64`).
7. Per-job filter on `/stream?job_id=...`.

Acceptance:
- All endpoints return correctly shaped JSON.
- Wrong/missing tokens → 401.
- SSE resume via `Last-Event-ID` produces no gaps, no duplicates.
- An admin command lands a line in `audit/`.
- Sustained event ingest does not block SSE delivery (load test:
  10k events/sec ingest, 1 SSE consumer keeps up at ≤ 50ms tail).

### Phase 3 — worker → coord integration

Goal: workers report to coord. Commands still don't reach workers (that's
Phase 3.5 — but bundled in this phase for review).

Deliverables:
1. Worker-side coord client (new module in `migration-worker`):
   register, heartbeat (5s), event batches (1s).
2. Self-fence path posts to coord.
3. Reconnect logic with bounded local buffer (configurable, default 64
   MiB serialized events) and exp backoff.
4. Coord-side endpoints for `/workers/register`, `/workers/<id>/heartbeat`,
   `/workers/<id>/events`, `/workers/<id>/fence`.
5. Phase 3.5 (same PR or a follow-on): worker honors coord pause/resume
   commands by polling a per-job pause flag on its next heartbeat
   response. Cancel/drain follow the same shape.

**Anticipated conflicts:**
- Existing `migration-worker::heartbeat` writes JSON to S3 today. We
  *keep* that path (so aggr's S3-polling summary still works) and add the
  coord HTTP heartbeat alongside it. Drop the S3 heartbeat only after
  Phase 6 when coord is the source of truth across all tooling.
- Existing `migration-worker::orchestrator` decides shard claims
  autonomously. Pause has to gate the claim loop without breaking the
  R-rules. Likely shape: a `RunControl` shared bool the orchestrator
  checks before each new shard claim; in-flight shards finish before pause
  takes effect (matches the "drain" semantic).

Acceptance:
- Workers register, heartbeat, emit events under normal load.
- Self-fence event propagates to `/jobs/<id>` and over SSE.
- Worker crash → restart re-registers cleanly. Old `WorkerId` flipped to
  `Disconnected` by coord dedup.
- Coord restart → workers reconnect and resume event submission without
  data loss (bounded buffer covers the gap).
- Pause command observed by all workers within one heartbeat interval.

### Phase 4 — TUI foundation

Goal: live operator dashboard.

Deliverables:
1. New crate `migration-tui` (recommended — see §1.4).
2. `vamoose tui` subcommand in `vamoose-cli`.
3. REST snapshot fetch + SSE subscribe with `Last-Event-ID` resume.
4. In-memory derived state (HashMaps).
5. Jobs list screen with sort/filter, footer aggregate.
6. Rolling windows (1s/1m/5m) maintained client-side from `ProgressDelta`.
7. Render throttled ≤ 20 fps.

Acceptance:
- Jobs list shows live updates.
- Reconnect after coord drop is transparent (yellow → green).
- Filter + sort work; no visible stutter under load.

### Phase 5 — TUI job detail tabs

Deliverables:
1. Five tabs: Overview, Workers, Errors, Plan, Verify.
2. Tab labels carry live counters.
3. Workers tab default sort by MB/s desc; row drill-down modal.
4. Errors tab error-class grouping + live tail.
5. Plan tab renders `jobs/<job_id>/config.json` verbatim with config hash.
6. Verify tab progress + retry queue.

Acceptance: all five tabs render, labels update, drill-down works,
grouping correct.

### Phase 6 — TUI polish

Deliverables:
1. Command palette (`:`) with tab completion.
2. Confirm modals for destructive commands.
3. Help overlay (`?`), search (`/`) with n/N.
4. Theme: dark + light, VAST teal accent, `NO_COLOR` honored.
5. `cargo about` / `cargo deny` wired up; CI emits
   `THIRD_PARTY_LICENSES` artifact.

Acceptance: palette round-trips through audit; theme passes both
terminals; `NO_COLOR=1` layout intact.

---

## 3. Cross-cutting concerns

### 3.1 Event versioning

Events are serde-tagged enums (`#[serde(tag = "kind")]`). Add
`schema_version: u8` to the envelope (`{ seq, at, schema_version, kind,
... }`). Coord refuses to load events with a `schema_version` newer than
it knows. Snapshot files carry the same field.

### 3.2 Time

All timestamps are UTC. Coord stamps `at` on every event ingest (worker's
local `at` is preserved separately as `worker_at` when present, for
diagnostic purposes only). Clock-drift handling is coord-side only —
workers never compare clocks across nodes.

### 3.3 Bounded cardinality on the wire (SSE)

- Per-file events (`ErrorEmitted`) stream, but coord caps to N/sec
  globally per error class to prevent flooding. Excess is folded into
  `ErrorBucket.count`.
  *Delivered (ledger F24):* `ERROR_STREAM_MAX_PER_SEC` per class per
  second, decided inline at ingest (`runtime.rs` `StreamCaps`).
  Excess still reaches state — and therefore `ErrorBucket.count` —
  as always; only the bus is capped. The per-job bucket *table* is
  additionally capped at `ERROR_BUCKET_CAP` distinct classes plus
  one catch-all overflow bucket.
- `WorkerHeartbeat` never streams (updates state but not the event log).
  *Delivered by construction:* no heartbeat event kind exists; pinned
  by the `worker_heartbeat_never_streams` test.
- `ProgressDelta` streams at 1Hz max per (job, worker) — coord coalesces.
  *Delivered (ledger F24) as a bus-only cap*
  (`PROGRESS_STREAM_MIN_INTERVAL_MS`): state folds every delta and
  the event log carries every delta; coalescing the log itself would
  change replay semantics, so a log-side cap is recorded as a
  follow-up in the F24 ledger row.

### 3.4 SSE backpressure

Each SSE consumer has a bounded per-consumer channel (1024 events). On
overflow coord sends a synthetic `Resync` event telling the client to
re-fetch `/jobs` snapshot — losing live tail is better than blocking
ingest.

### 3.5 Audit trail

Single `audit/<YYYY-MM-DD>/<seq>.jsonl` per command. Includes the
admin-token *label* (never the token itself), action, target, args,
generated `command_id`, and the result (accepted/rejected with reason).

### 3.6 Testing strategy

- Unit tests: a `S3Like` trait already implicit in `migration-core`
  (via `ClaimStore`). Define a coord-side `CoordStore` trait the lease,
  snapshot, and event-log writers all use; implement against an
  in-memory fake. Replay/lease/archive logic is unit-tested at this
  layer.
- Integration tests: hit var204 VAST S3 via the standard fixture
  (matches `migration-core` and `migration-mover` patterns). One
  end-to-end test per acceptance bullet.
- Load test in Phase 2 uses a synthetic worker firing events at coord;
  not a unit test (kept under `scripts/coord-load-test.sh`).

### 3.7 Logging

Reuse `vamoose-cli::logging`. Coord-specific events get target
`coord::...`. The TUI suppresses tracing-fmt output entirely (logs to a
file via `tracing-appender` if `--log-file` is passed).

---

## 4. What we are deliberately *not* doing in v1

- Job creation from TUI (CLI only).
- Multi-select bulk operations in TUI.
- Per-job bells/notifications.
- Web dashboard (reuses the same REST+SSE later).
- Sharded coord (one coord process per deployment).
- Hot rotation of admin tokens (restart coord to pick up token file changes).
- Worker reshuffling across jobs (a worker stays bound to one job for v1).

---

## 5. Decisions locked before Phase 1 code

| # | Decision                              | Choice                                                                          |
|---|---------------------------------------|---------------------------------------------------------------------------------|
| Q1| Coord state bucket vs. job bucket     | **Same bucket, disjoint prefixes** (v1 = one job per bucket; coord scans many). |
| Q2| HTTP framework for coord              | **axum 0.7 + tower-http**.                                                      |
| Q3| TUI placement                         | **New crate `migration-tui`**.                                                  |

Everything else is local to a phase and can be revisited as we go.
