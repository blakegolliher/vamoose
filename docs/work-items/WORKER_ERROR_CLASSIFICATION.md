# Classify shard errors: worker-local vs shard-fatal

Status: landed — merged in PR #18.
Ledger: F13 in `docs/REVIEW_LEDGER.md` (F42 is the follow-up).
Priority: high — one stale/sick worker can terminal-`Failed` shards
the rest of the fleet could process.
Scope: `migration-worker/src/orchestrator.rs`,
`migration-core/src/errors.rs` (taxonomy only).

## Problem

The orchestrator treats **every** error from `processor.process()`
as shard-fatal and calls `claim::fail()` — a terminal state skipped
by all scanners and counted toward `all_terminal`
(`orchestrator.rs` ~595–657). The spec (`docs/CLAIM_PROTOCOL.md`
"fail") reserves `fail()` for errors "that would re-occur for any
worker reclaiming the shard."

Misclassified today: local scratch I/O errors (EIO/ENOMEM reading
the downloaded parquet) and `Error::SchemaVersionMismatch` from
`ShardReader::open` — the latter is a property of the *worker's*
binary version, so one stale binary in a mixed fleet permanently
fails shards every other worker could do. Those rows are then never
copied unless an operator notices.

## Required reading

- `docs/CLAIM_PROTOCOL.md` — `fail()` contract, and what leaving a
  claim Active means (lease/cross-check reclaim)
- `crates/migration-worker/src/orchestrator.rs` — the process-error
  arm, `reclaim` atom usage
- `crates/migration-core/src/{errors.rs, shard.rs, claim.rs}`
- `crates/migration-worker/src/shard_processor.rs` — what errors
  actually flow out of `process()`

## Acceptance tests — write these FIRST

1. `classifier_table` (red before fix — classifier won't exist) —
   pure fn `classify_shard_error(&Error) -> ShardErrorClass` with a
   table test covering at least: corrupt-parquet/decode error →
   `Fatal`; `SchemaVersionMismatch` → `WorkerLocal`; scratch-file
   I/O error → `WorkerLocal`; S3 download error → `WorkerLocal`;
   row-level failures (already handled via the failure sink) are NOT
   routed here (assert unchanged behavior via existing tests).
2. `worker_local_error_releases_claim_not_failed` (red before fix)
   — orchestrator-level test with a stub processor returning a
   `WorkerLocal` error against the mock ClaimStore: assert the claim
   ends **absent or reclaimable** (released via the worker's own
   delete-if-match), NOT `Failed`, and the shard is added to a local
   in-memory skip set so this worker doesn't thrash re-claiming it.
3. `fatal_error_still_marks_failed` — corrupt-parquet stub error →
   claim `Failed` with reason, as today.
4. `skip_set_does_not_block_all_terminal_exit` — a worker with a
   skipped shard must still exit when every shard reaches a terminal
   state via other workers (drive the mock store to Completed by a
   simulated peer).
5. `repeated_worker_local_errors_backoff` — the release path must
   jitter/backoff (reuse the existing contention backoff) so a
   fleet-wide transient (e.g. S3 blip) doesn't turn into a
   claim/release storm. Assert via mock-store op counts over paused
   time.

## Fix shape

- `ShardErrorClass { Fatal, WorkerLocal }` + pure classifier in the
  worker (taxonomy additions in `migration-core/errors.rs` only if an
  error kind is currently stringly-typed and can't be matched).
- `WorkerLocal` path: release the claim with the existing
  delete-if-match atom (the worker owns the etag — this is safe and
  spec-clean), record the shard in a per-run skip set, log loudly
  with the classification, and continue the scan loop after backoff.
- `Fatal` path: unchanged (`fail()` with reason).
- Update `docs/CLAIM_PROTOCOL.md` "fail" section with one paragraph:
  worker-local errors release-and-skip instead of failing, and why.

## Out of scope / do NOT

- F42 (retry/backoff for transient S3 on scan/acquire) — note it,
  don't build it here.
- No new terminal states in the claim protocol.
- Do not retry `WorkerLocal` errors in-place on the same worker —
  release-and-skip keeps the failure domain small and lets a healthy
  peer take it.

## Definition of done

- [ ] Tests 1–2 written first and observed red.
- [ ] All acceptance tests green; full gate green.
- [ ] CLAIM_PROTOCOL.md updated.
- [ ] Ledger F13 updated; this doc's Status flipped.
