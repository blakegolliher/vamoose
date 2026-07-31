# Coord runtime batch: flush outside the lock (F45b), HWM attribution (F20 residue), trailing-edge flush + row eviction (F24 residue)

Status: in progress on branch `coord-runtime-batch`.
Ledger: F45, F20, F24 rows in `docs/REVIEW_LEDGER.md` (do NOT edit
the ledger or `docs/NEXT.md` from this branch).
Decisions (owner, 2026-07-31): F45b fix-before-beta (single-flusher,
PUT outside the runtime lock); F20 residue closed via envelope
caller attribution; F24 log stays COMPLETE (coalescing is won't-do)
and the two small live-side bits land.
Scope: `crates/migration-coord` only. Work items in order 1 → 2 → 3,
one commit each, tests before fix within each.

## Item 1 — F45b: event-chunk flush must not hold the runtime lock across S3 PUTs

### Problem

`CoordRuntime` serializes everything through one
`tokio::sync::Mutex<RuntimeInner>` (`runtime.rs:10`, `:248`). The
flush paths (`flush_log`, `flush_aged`) hold that lock across the
store PUT — so during every chunk flush, ingestion AND every API/
TUI read (`state()`, `job()`, etc. all `.lock()`) stall for the full
S3 round-trip. Error storms make flushes big exactly when operators
are watching.

### Fix (decided)

Restructure chunk flush as: under the lock, take the full buffered
chunk (and compute its key/seq range) and mark it in-flight; RELEASE
the lock; PUT outside it; reacquire to record the flush result.
Constraints that must provably survive:

- **Flush-before-ack (F03) is inviolable**: any path that acks
  events to a worker (`events_batch` → `flush_log`) must still
  await durable persistence of those events before responding.
  Moving the PUT off the lock must not move it off the request path.
- **Single flusher**: at most one chunk PUT in flight; concurrent
  flush requests for already-covered seqs await the in-flight result
  rather than double-PUTting (chunk keys are seq-ranged — two racing
  flushes must never produce overlapping or out-of-order chunks).
  A dedicated flush mutex/token (held across the PUT, distinct from
  the state mutex) is the expected shape.
- **Failure atomicity**: a failed PUT must leave the buffer intact
  (events not lost, retried on the next flush) and must still fail
  the ack path — exactly today's semantics.
- Scope: the event-chunk flush paths are the mandatory target.
  Snapshot/archive writes (tick-driven) may adopt the same pattern
  ONLY if it falls out naturally; otherwise leave them and note it.

### Acceptance tests — FIRST, observed red

The existing worker_endpoints/runtime tests pin flush-before-ack and
chunk contents — they must stay green unmodified. New (red today):

1. `reads_do_not_block_on_inflight_flush` — a store double whose
   `put` on `events/` blocks on a signal (gate pattern like
   `FailEventPuts` in archive_wiring.rs but gated, not failing):
   start a flush (do not release the gate), then issue a state read
   (`rt.state()`) with a short timeout — it must complete while the
   PUT is still pending. RED today (read waits on the runtime lock
   held across the PUT).
2. `ingest_does_not_block_on_inflight_flush` — same gate; a new
   ingest on another task completes (applies to state, buffers)
   while the PUT is pending. RED today.
3. `concurrent_flushes_never_overlap_chunks` — force two flush
   attempts around one gated PUT; assert the durable chunks cover
   contiguous, non-overlapping seq ranges and every event lands
   exactly once.
4. Existing F03 ack-durability tests: green before and after,
   unmodified — cite them by name in the report.

## Item 2 — F20 residue: caller attribution so every stamped entry can advance the HWM

### Problem (PR #40 note)

The reducer keys the client_seq HWM on
`EventKind::attributed_worker()`; `ClaimConflictDetected/Resolved`
and `VerifyFileMismatch` return None, so a stamped entry of those
kinds at a batch tail cannot advance the mark and re-applies on
resend. Unreachable with today's worker; a silent double-count the
day someone adds that emission.

### Fix (decided)

Additive `from_worker: Option<WorkerId>` on `EventEnvelope`
(`#[serde(default)]`, `skip_serializing_if`, no SCHEMA_VERSION bump —
same precedent as `client_seq`, PR #40). `ingest_worker_event`
stamps it from the URL id (already validated by the phase-1 trust
boundary). The reducer's HWM maintenance keys on `from_worker`
when present, falling back to `attributed_worker()` for older
envelopes (whose HWM behavior must stay byte-identical). Admin/
internal ingest paths leave it None.

### Acceptance tests — FIRST, observed red

1. `stamped_non_attributed_tail_dedups_on_resend` — batch ending in
   a stamped `ClaimConflictResolved`; resend the identical batch;
   nothing re-applies (state + log identical). RED today.
2. `replay_reconstructs_hwm_from_from_worker` — restart/replay then
   resend: still deduped. RED today.
3. Legacy envelopes without `from_worker` (fixtures from existing
   tests): HWM behavior unchanged — extend an existing test's
   assertions rather than duplicating it.

## Item 3 — F24 residue: trailing-edge flush + worker-row eviction (log stays complete)

The log-side coalescing question is CLOSED as won't-do: the durable
log carries every event (replay fidelity + audit). Do not touch the
log path. Two live-side items:

### 3a. Trailing-edge flush

`StreamCaps` is leading-edge-only: a burst's LAST ProgressDelta is
suppressed from the bus until the next event arrives — the live TUI
shows a stale final count indefinitely on a quieting job. Fix: when
a delta is suppressed, retain it (per (job, worker) key, latest
wins); on the runtime's existing tick (`ticks.rs` already drives
periodic work into the runtime) broadcast any retained delta older
than the cap interval and clear it. Bound: a burst's final value
reaches live subscribers within ~2× `PROGRESS_STREAM_MIN_INTERVAL_MS`.
Log/replay are untouched (the event was already logged); the flushed
frame is a re-broadcast of an already-ingested envelope, not a new
event — no new seq, no log write.

Tests: suppressed-final-delta reaches a subscriber after the tick
(RED today — the existing `progress_delta_coalesced_per_job_worker`
pins bus=1-of-7; extend, don't weaken: the 7th delta's VALUES must
arrive by the deadline without a new event); no duplicate broadcast
when the trailing frame was already the last broadcast one.

### 3b. Worker-row eviction

`state.workers` grows forever. HARD CONSTRAINT: state is rebuilt by
replay, so eviction MUST be replay-deterministic — no wall-clock
pruning of live state. Decided mechanism: prune at SNAPSHOT-WRITE
time only — the snapshot writer omits rows whose state is
`Disconnected` (not Fenced — fenced rows are operator-relevant) and
whose last activity is older than a const window (suggest 24h,
named const with doc comment); replay = snapshot + events, so both
sides converge and live memory is bounded by snapshot cadence. If
you find this unsound against how snapshots/replay actually compose,
STOP on this sub-item and report — do not invent an alternative
mechanism unilaterally.

Tests: snapshot omits stale Disconnected rows and keeps fresh/
Fenced ones; replay from pruned snapshot + subsequent events
converges with live state (extend the existing replay-equality
tests); a WorkerJoined for an evicted id resurrects it cleanly.

## Constraints (hard fences)

- `crates/migration-coord` only; claim.rs untouchable; no changes to
  `lease.rs`, `auth.rs`, the admin command path, or `EventKind`
  variants (envelope field is additive only).
- No new top-level `tests/*.rs`; extend `worker_endpoints.rs` /
  in-crate test mods / existing store doubles.
- A PARALLEL session owns vamoose-cli, migration-worker's exit path,
  and migration-mover — do not touch those crates.

## Gate & handoff

Full gate before every commit:
`cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace && cargo deny --all-features check`

One commit per item (tests-first within each, PR #30/#34 `#[ignore]`
red convention, observed-red quoted). This doc rides commit 1.
Commit; do NOT push. No AI/Claude attribution anywhere.
