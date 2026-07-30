# F20 phase 1: worker-event allow-list + caller/payload identity binding

Status: in progress on branch `f20-event-trust`.
Ledger: F20 in `docs/REVIEW_LEDGER.md` (do NOT edit the ledger or
`docs/NEXT.md` from this branch).
Design: `docs/design/COORD_TRUST_AND_FENCING.md` Part 2, DECIDED
2026-07-30 — this item executes D2 (EventKind allow-list) and D3
(worker-id binding). D4/D5 (client_seq idempotency + storage-failure
batch semantics) are a SEPARATE stacked work item — do not start
them here. If implementation reality contradicts the design doc,
STOP and report.
Scope: `crates/migration-coord/src/server/worker.rs` (the
`POST /workers/{id}/events` handler and its module tests), plus
`server.rs`/`errors.rs` only if a new ApiError shape is needed.

## The bug (current code)

`POST /workers/{id}/events` (`server/worker.rs:247-278`) accepts
ALL 19 `EventKind` variants from any `X-Cluster-Secret` holder —
including operator/lifecycle kinds (`JobCreated`, `JobPaused`,
`JobResumed`, `JobCancelled`, `JobCompleted`, `JobFailed`,
`JobPhaseChanged`, `VerifyStarted`, `VerifyCompleted`) — bypassing
bearer auth AND the admin command path's audit rows
(`command.rs:107-116`). The URL `{id}` is parsed and DISCARDED
(`worker.rs:252-255`): no registration check, no payload binding,
so one worker can fence another (`WorkerFenced` →
`state.rs:238-243`) or inflate any job's counters.

Production reality that makes this zero-behavior-change: the worker
today emits ONLY `ProgressDelta` (`coord_driver.rs:452`, `:835`;
the `JobCreated` in coord_client.rs is a test fixture).

## The fix (decided)

### D2 — allow-list

The worker route accepts exactly these kinds:
`ProgressDelta`, `ErrorEmitted`, `WorkerStateChanged`,
`WorkerRecovered`, `ClaimConflictDetected`, `ClaimConflictResolved`,
`VerifyFileMismatch`, and `WorkerFenced` ONLY when its payload
`worker_id` equals the URL id (self-fence report).

Everything else → HTTP 403 with the rejected kind named in the
error body, plus a `tracing::warn!` naming kind + caller id.
`WorkerJoined`/`WorkerLeft` stay synthesized by register/heartbeat
paths and are NOT accepted raw. Lifecycle kinds remain
operator/coord-internal; widening the list later is a one-line
reviewed change.

### D3 — identity binding

Stop discarding the URL id:
1. The worker must be REGISTERED (whatever `/workers/register`
   records in state — mirror how `heartbeat` resolves a known
   worker; reject unknown ids with the same status that endpoint
   uses for unknown workers, or 403 if it has no precedent).
2. Every payload `worker_id` field (on the kinds that carry one)
   must equal the URL id → else 403 naming both ids.

### Batch semantics (the D2/D3 slice only)

Validate the ENTIRE batch against the allow-list + binding checks
BEFORE ingesting any entry; reject the whole batch (403) if any
entry fails, so one malformed entry cannot smuggle siblings in and
these rejections can never cause partial application. (Storage
failures mid-batch keep today's documented partial semantics — that
residue is D4/D5's job, not yours.)

## Constraints (hard fences)

- MUST NOT change `EventKind`/`schema.rs`, the reducer
  (`state.rs`), `runtime.rs`, or `lease.rs` (a parallel session
  owns lease.rs). Rejection lives at the HANDLER layer.
- MUST NOT touch `crates/migration-core/src/claim.rs`.
- MUST NOT change the admin command path or auth middleware
  (`auth.rs`) — scoped tokens are explicitly out of scope (D6).
- No new top-level `tests/*.rs` files; tests go in the existing
  in-crate test modules (`server/worker.rs` / server test mods —
  follow the existing in-process patterns there).
- Do not break the worker: `ProgressDelta` batches with the
  caller's own id must flow exactly as before (the
  migration-worker integration tests must stay green untouched).

## Acceptance tests — write these FIRST, observe them red

True reds against current behavior (no stubs needed — assert the
NEW behavior, watch it fail today):

1. `worker_route_rejects_operator_kinds` — for each of
   `JobCreated`, `JobPaused`, `JobResumed`, `JobCancelled`,
   `JobCompleted`, `JobFailed`, `JobPhaseChanged`, `VerifyStarted`,
   `VerifyCompleted`: POST from a registered worker → 403, error
   names the kind, AND coord state is unchanged (job phase intact,
   no new event in the log — assert via seq/last_seq not advancing).
   RED today (200 + state transition).
2. `worker_route_rejects_foreign_worker_id` — registered worker A
   sends `ProgressDelta` carrying worker B's id → 403 naming both;
   state unchanged. RED today.
3. `worker_route_rejects_unregistered_caller` — events POST for an
   id that never registered → rejected; state unchanged. RED today.
4. `worker_fenced_self_only` — `WorkerFenced` with own id → applied;
   with another id → 403. Half red today.
5. `one_bad_entry_rejects_whole_batch` — batch of [valid
   ProgressDelta, JobCancelled]: 403, NEITHER applied (seq
   unchanged). RED today (partial application).
6. `allowed_kinds_still_flow` — each allow-listed kind with the
   caller's own id ingests, gets a seq, reaches state exactly as
   before. GREEN today; must stay green.

## Gate & handoff

Full gate before every commit:
`cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace && cargo deny --all-features check`

Two commits: tests-red (PR #30/#34 `#[ignore]`-marker convention,
observed-red quoted), then the fix. This doc rides the first
commit. Commit; do NOT push. No AI/Claude attribution anywhere.
