# Fresh-claim grace window for the progress cross-check

Status: open — not started.
Ledger: F01, F30 in `docs/REVIEW_LEDGER.md`.
Priority: critical — blocks the M6 multi-worker milestone.
Scope: `migration-worker` (orchestrator, heartbeat), spec docs. New
integration test. No protocol-atom changes in `migration-core`.

## Problem

`check_progress_liveness` (`crates/migration-worker/src/orchestrator.rs`,
~line 1141) implements the fast-reclaim cross-check from
`docs/work-items/PROGRESS_LIVENESS_CROSS_CHECK.md` §5. Its
etag-mismatch and progress-absent branches return "eligible for
reclaim" **immediately**, with no reference to how old the claim is.

But a worker's progress object is only written on heartbeat ticks
(`heartbeat.rs::HeartbeatTask::run`, default `heartbeat_sec = 30`).
Between `try_acquire` and the owner's next tick, the on-S3
`progress/host-<id>.json` still carries the *previous* tick's
`held_etag` — `None` at startup, or the previous shard's etag. Any
peer scanning in that window sees mismatch → eligible → fast-reclaims
a live, seconds-old claim. The rightful owner detects the loss on its
next HEAD, self-fences, and **exits the whole process**.

Consequences: at fleet startup, `scan_shards` iterates the manifest
in order with no shuffle, so every worker evaluates shard 1's fresh
claim first — a near-deterministic theft cascade that fences healthy
workers one by one. The same hole fires at end-of-run when idle
workers scan aggressively. No data corruption (rename idempotency +
fence hold), but eventual progress dies at fleet scale.

The spec itself is wrong: PROGRESS_LIVENESS_CROSS_CHECK.md §4 claims
the one-tick staleness "is accounted for by the 2 × heartbeat_sec
threshold" — that threshold only guards the `heartbeat_utc` branch,
not the etag-mismatch/absent branches. The implementation follows the
spec faithfully. Fix both.

Related: with multiple self-reclaimed orphans at startup
(`ClaimTarget::AlreadyReclaimed`), `held_etag` can only name one
shard, so queued orphans are stealable the same way — the grace
window must be measured from the claim's own `claimed_utc`, which
covers this case too (self-reclaim mints fresh claims).

## Required reading

- `docs/work-items/PROGRESS_LIVENESS_CROSS_CHECK.md` (esp. §4, §5)
- `docs/CLAIM_PROTOCOL.md` "Worker lifecycle"
- `crates/migration-worker/src/orchestrator.rs`:
  `check_progress_liveness`, its call sites in `scan_shards`, and the
  existing liveness unit tests (one per spec-table row)
- `crates/migration-core/src/records.rs`: `ClaimRecord.claimed_utc`
- `crates/migration-core/src/claim.rs` unit tests — the mock
  `ClaimStore` + `tokio::time::pause()` patterns to reuse

## Acceptance tests — write these FIRST

Unit tests live next to the existing `check_progress_liveness` tests
in `orchestrator.rs`. "Red before fix" = must fail against current
code; if one doesn't, the test is wrong — stop and re-read.

1. `fresh_claim_mismatched_progress_not_eligible` (red before fix)
   — claim with `claimed_utc = now - 5s`; progress object present,
   `held_etag = Some("different-etag")`, fresh `heartbeat_utc`.
   Assert NOT eligible.
2. `fresh_claim_absent_progress_not_eligible` (red before fix)
   — claim with `claimed_utc = now - 5s`; no progress object for the
   host. Assert NOT eligible.
3. `fresh_claim_none_held_etag_not_eligible` (red before fix)
   — claim age 5s; progress present with `held_etag = None` (owner
   between shards / just started). Assert NOT eligible.
4. `aged_claim_mismatched_progress_eligible`
   — claim with `claimed_utc = now - (2 × heartbeat_sec + 1s)`,
   mismatched `held_etag`. Assert eligible (orphan reclaim must
   keep working).
5. `aged_claim_absent_progress_eligible`
   — same age, no progress object (owner crashed before first tick).
   Assert eligible.
6. `boundary_exactly_grace_not_eligible`
   — claim age exactly `2 × heartbeat_sec`. Assert NOT eligible
   (strictly-greater comparison; matches the heartbeat-staleness
   convention in the existing rows).
7. `matching_etag_never_eligible_regardless_of_age`
   — regression guard on the happy path.
8. **Two-live-workers integration** (new file,
   `crates/migration-worker/tests/two_live_workers.rs`) (red before
   fix). In-process, no hardware: in-memory `ClaimStore` (extract or
   re-export the mock used by `migration-core/src/claim.rs` tests —
   e.g. behind a `test-util` feature — do not fork a divergent copy),
   stub shard processor that sleeps a simulated few seconds per
   shard, `tokio::time::pause()`.
   - `two_workers_start_simultaneously_no_theft`: 2 orchestrators,
     4 shards, started concurrently. Drive to completion. Assert:
     every shard `Completed` exactly once; zero fence trips; zero
     reclaims of claims younger than the grace window (instrument via
     the store mock's op log).
   - `dead_worker_still_reclaimed_after_grace`: worker A claims a
     shard then goes silent (drop its heartbeat + processor). Worker
     B must reclaim it after the grace/lease threshold and complete
     it. Guards against over-correcting into "never reclaim".

## Fix shape

In `check_progress_liveness`, the etag-mismatch and progress-absent
branches must additionally require
`now - claim.claimed_utc > 2 × heartbeat_sec` before returning
eligible. Use the same conservative time handling as the existing
staleness math (future timestamps → not stale). `claimed_utc` is
already on the claim record; v2 owners never rewrite claims, so it is
stable for the ownership window.

Optional hardening (do after the guard, not instead of it): publish
the progress object synchronously inside the acquire path so the
window narrows operationally. The grace guard is the correctness fix;
the synchronous publish is an optimization and must not replace it.

Update both spec docs in the same PR:
- PROGRESS_LIVENESS_CROSS_CHECK.md §4/§5: correct the staleness
  claim, add the claim-age guard to the reference logic and the
  decision table.
- CLAIM_PROTOCOL.md "Worker lifecycle": same.

Trade-off to state in the docs: crash recovery via the progress
cross-check now takes up to `2 × heartbeat_sec` longer for a worker
that dies immediately after acquiring. The lease-based path is
unchanged. This is the intended trade.

## Out of scope / do NOT

- Do not touch the four protocol atoms in `migration-core/claim.rs`.
- Do not add shard-scan shuffling or backoff changes here (separate
  concern; the guard alone closes the hole).
- Do not weaken test 5 — dead-worker reclaim is the feature's reason
  to exist.

## Definition of done

- [ ] Tests 1–3, 6, 8a written first and observed red.
- [ ] All acceptance tests green.
- [ ] Both spec docs updated (the spec bug is part of the finding).
- [ ] Full gate green: fmt, clippy `-D warnings`, `cargo test
      --workspace`, deny.
- [ ] `scripts/fast-reclaim-drill.sh` re-run on hardware before
      marking `verified` in the ledger (kill -9 → reclaim latency
      should grow by ≤ one grace window, no more).
- [ ] Ledger F01/F30 rows updated; this doc's Status flipped.
