# Protocol test pack: heartbeat.rs + s3.rs (+ the R4 TOCTOU fix)

Status: landed — merged in PR #20.
Ledger: F28, F29, F16, F34 in `docs/REVIEW_LEDGER.md`.
Priority: high — these are the only unautomated legs of the
anti-dual-writer chain.
Scope: `migration-worker/src/heartbeat.rs`,
`migration-core/src/s3.rs`. This item IS mostly tests; it also fixes
one real bug (F16) and deletes one landmine (F34).

## Problem

- **F28:** `heartbeat.rs` (~340 lines) has zero tests. It is the
  S3-side trigger of the fence: claim.rs transitions are tested, the
  mover's fence gate is tested, the coord-side trip is tested — the
  thing that *pulls the trigger* is verified only by the manual M5
  hardware run.
- **F29:** `s3.rs` (~460 lines) has zero tests. The
  200/404/412(+"PreconditionFailed" code-string) → outcome-enum
  mapping and the etag-unquoting are what make v2 at-most-once.
  Project history (M5_NOTES) shows precondition-semantics assumptions
  are exactly where this system broke before.
- **F16 (real bug, fix here):** the heartbeat tick snapshots the held
  claim, PUTs progress (tens–hundreds of ms), then HEADs using the
  *stale snapshot*. `complete()` clears the cell then does its
  delete-then-create. If the tick snapshots just before the clear,
  the HEAD lands after complete and sees 404/new-etag → `Lost` →
  fence → the worker **exits after a perfectly clean completion**. At
  fleet scale this fires. The fix: after observing `Lost`, re-check
  the held-claim cell under the lock — if the snapshot no longer
  matches the cell (cell cleared or changed), suppress the trip.
- **F34 (landmine, delete here):** the "StillHeld but etag changed →
  adopt observed etag" branch in the heartbeat is unreachable
  (`claim::refresh` only returns `StillHeld` on exact match) — and if
  refresh semantics ever changed, silently adopting a foreign etag
  would be ownership forgery. Delete it; if refresh's contract is
  ever widened this should become an invariant violation, not an
  adoption.

## Required reading

- `crates/migration-worker/src/heartbeat.rs` (all of it)
- `crates/migration-core/src/claim.rs` — the mock-ClaimStore +
  `tokio::time::pause()` test patterns to reuse; `refresh` contract
- `crates/migration-core/src/s3.rs` — error mapping, etag handling
- `docs/CLAIM_PROTOCOL.md` R4, R6, R7
- `aws-smithy` test-util / `aws_smithy_runtime::client::http::test_util`
  (mockable HTTP for the SDK) — or extract pure classifiers if
  mocking fights back; either is acceptable, pure extraction
  preferred for the 412/404 mapping.

## Acceptance tests — write these FIRST

### heartbeat.rs (mock ClaimStore + paused time)

1. `etag_mismatch_trips_fence` — HEAD observes a different etag →
   fence tripped, no further progress PUTs, task exits its loop.
2. `refresh_transient_errors_within_budget_no_trip` — R6: fewer than
   `floor(lease/heartbeat)` consecutive transient errors, then
   success → no trip, budget resets.
3. `refresh_budget_exhausted_trips` — R6: budget+1 consecutive
   failures → trip.
4. `clock_jump_trips` — R7: wall vs mono divergence > lease/2 → trip
   (inject the clock; if the clock isn't injectable yet, making it
   injectable is in scope).
5. `clean_completion_race_does_not_fence` (red before F16 fix) —
   interleave: tick snapshots held claim → orchestrator clears cell +
   completes (mock store now returns 404/new etag) → tick's HEAD
   observes Lost. Assert NO fence trip. Then
   `real_loss_still_fences`: same interleaving but the cell still
   holds the snapshot etag → trip. This pair is the R4 regression
   test.
6. `shutdown_stops_ticks_and_writes_final_progress` — pin the
   existing documented shutdown behavior while you're here.
7. (F34) delete the adopt branch; `refresh` contract change would
   now fail compilation or hit an explicit
   `unreachable-invariant` error — assert via a test on the match
   arms if practicable, otherwise the deletion + comment suffices.

### s3.rs (smithy test-util or extracted classifiers)

8. `put_if_absent_412_maps_to_precondition_failed` — including the
   VAST-style body with `PreconditionFailed` code string.
9. `delete_if_match_412_maps_to_lost_race_outcome` and
   `delete_if_match_404_maps_to_not_found`.
10. `etag_unquoted_on_put_get_list` — quoted `"abc"` from the wire
    compares equal to stored unquoted `abc` across all three read
    paths (the apples-to-apples invariant).
11. `if_match_header_requotes_stored_etag` — the outbound header
    carries the quoted form.
12. `unexpected_5xx_maps_to_transient_not_success` — no path may
    treat an ambiguous error as success (pin the taxonomy).

## Fix shape

Tests drive it. For s3.rs prefer extracting pure
`classify_put_response` / `classify_delete_response` helpers over
heavy HTTP mocking — the SDK plumbing stays thin and untested wiring,
the semantics get table tests. For the F16 fix, the suppression check
must take the same lock the orchestrator's clear takes (no new
lock-ordering edges — check the existing order: permit → pool; cell
lock is leaf).

## Out of scope / do NOT

- No changes to claim.rs atoms.
- No live-endpoint tests (that's `vamoose doctor`'s job).

## Definition of done

- [ ] Test 5's first half observed red before the F16 fix; the whole
      pack green after.
- [ ] F34 branch deleted.
- [ ] Full gate green; ledger F16/F28/F29/F34 updated; Status flipped.
