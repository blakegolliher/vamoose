# Gate all coord store writes on the lease

Status: in review — fix + tests on branch coord-lease-fence-writes.
Ledger: F02 in `docs/REVIEW_LEDGER.md`.
Priority: critical — split-brain corruption of the event log.
Scope: `migration-coord` (`runtime.rs`, `ticks.rs`),
`vamoose-cli/src/cmd/coord.rs`.

## Problem

The coord runtime checks `lease_lost` in `ingest`, `record_audit`,
and `record_heartbeat` (guards at `runtime.rs` ~207/243/444/492) —
but **not** in `flush_log` (~562), `flush_aged` (~569),
`write_snapshot` (~577), or `shutdown` (~593). When the lease loop
detects loss it cancels the shutdown token, and `cmd/coord.rs` then
unconditionally calls `runtime.shutdown()`, which flushes buffered
chunks and overwrites `state/snapshot.json` with plain PUTs — after a
successor coord may have taken over and replayed.

Chunk keys carry no lease epoch and both coords resume seq counters
from the same flushed history, so the loser's keys can equal the
winner's: the winner's chunks and snapshot get clobbered. Permanent
event-log corruption, unrecoverable from S3 alone.

## Required reading

- `crates/migration-coord/src/runtime.rs` (the `lease_lost` flag and
  which methods honor it)
- `crates/migration-coord/src/ticks.rs` (lease loop → shutdown-token
  cancel path)
- `crates/vamoose-cli/src/cmd/coord.rs` (the unconditional
  `shutdown()` call)
- `crates/migration-coord/src/store.rs` `MemStore` test util
- Existing runtime unit tests for the guarded methods — copy their
  arrange style.

## Acceptance tests — write these FIRST

All against `MemStore`, in `runtime.rs` tests or the crate's
integration tests. The mock must count writes (`put`/`put_if_absent`
calls) so "no writes happened" is assertable.

1. `flush_log_after_lease_lost_writes_nothing` (red before fix) —
   ingest a few events (buffered), `mark_lease_lost()`, call
   `flush_log()`. Assert: store write-count unchanged AND the call
   returns the lease-lost error (same error shape `ingest` uses).
2. `flush_aged_after_lease_lost_writes_nothing` (red before fix).
3. `write_snapshot_after_lease_lost_writes_nothing` (red before fix).
4. `shutdown_after_lease_lost_skips_flush_and_snapshot` (red before
   fix) — buffered events + lease lost + `shutdown()`. Assert zero
   new store writes and a non-panicking, distinguishable return so
   the CLI can log "lease lost — buffered events NOT flushed
   (successor owns the log)".
5. `shutdown_with_lease_held_still_flushes` — regression guard: the
   normal path must keep flushing on clean shutdown.
6. `cmd_coord_lease_lost_exit_is_nonzero` — if the coord exits
   because the lease was lost, the process result the CLI maps to
   must be distinguishable from clean shutdown (unit-test whatever
   seam `cmd/coord.rs` exposes; if none exists, extract one).

## Fix shape

- Add the same `lease_lost` guard the ingest path uses to
  `flush_log`, `flush_aged`, `write_snapshot`; `shutdown` composes
  them and must short-circuit before any write.
- The lease-lost error must be loud at the CLI layer: log exactly
  what was dropped (buffered event count) and why that is correct
  (the successor replayed from the last durable state; our buffer
  was never acked as durable — note: F03 changes ack semantics; keep
  the log message honest relative to whatever lands first).
- Do NOT try to fix the refresh HEAD-then-PUT race (F19) here — that
  is a fencing-token design item; gating writes is the containment
  that makes F19 survivable.

## Out of scope / do NOT

- No chunk-key schema changes (lease epoch in keys is a possible F19
  follow-up; changing key shape breaks replay compatibility and
  needs its own design).
- No changes to lease acquire/takeover — that state machine is
  correct and tested.

## Definition of done

- [ ] Tests 1–4 written first and observed red.
- [ ] All acceptance tests green; full gate green (fmt, clippy,
      workspace tests, deny).
- [ ] Ledger F02 updated; this doc's Status flipped.
