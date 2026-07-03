# Coord guards: durable audit keys, phase legality, cardinality caps

Status: done — in review. F22, F25, F24 implemented test-first on
branch `coord-guards` (one commit each); full gate green.
Ledger: F22, F25, F24 in `docs/REVIEW_LEDGER.md`.
Priority: high (F22 silent audit loss, F25 state corruption via
legal-looking commands), medium (F24 resource bounds).
Scope: `migration-coord` (`runtime.rs` audit path, `state.rs` reducer,
`server/command.rs`, `server.rs` ApiError, `layout.rs` docs,
`schema.rs` caps only).

Work the items in order F22 → F25 → F24; each is independently
committable. Write each item's tests before its fix.

## Item 1 — F22: audit rows must survive a crash-window counter reset

### Problem

`record_audit` (`runtime.rs:445-487`) numbers audit rows with an
in-memory `audit_seq_today` and writes
`audit/<YYYY-MM-DD>/<seq:020>.jsonl` via **unconditional
`store.put`** (:485). The counter is persisted only via snapshots
(`Snapshot.audit_seq_today/date`; replay restores it). Between
snapshots, rows get seqs N+1..N+k; a crash before the next snapshot
reverts the counter to N — and because the write is a plain `put` with
**no `command_id` de-dup anywhere in code**, the restarted coord's
next audits silently overwrite the earlier rows. On a fresh day with
no snapshot the counter resets to 0 entirely.

The docs contradict each other about this: `layout.rs:90-93` claims
"we de-dup on `command_id`" (no such de-dup exists);
`runtime.rs:440-444` / `schema.rs:689-692` claim the snapshot
persistence makes numbering continuous (only up to the last snapshot).

Secondary ordering wart: `record_and_ingest` (`server/command.rs:68-92`)
writes the `Accepted` audit row **before** `ingest` + `flush_log` — if
either fails, the audit trail asserts a command that never took
effect.

### Acceptance tests — write these FIRST

Harness: `tests/command_endpoints.rs` — reuse `fresh_app_with_clock`
(:56-69), `post_json` (:76-94), the crash-restart pattern from
`command_ack_implies_durable` (:315-340: `clock.advance` + a second
`CoordRuntime::start` on the same MemStore), and the existing audit
tests (`multiple_commands_assign_increasing_audit_seqs_within_a_day`
:253, `audit_rolls_over_on_new_utc_day` :277). Note `rt_cfg()` sets
`max_events_per_chunk: 1000`, so snapshots only happen when a test
calls `write_snapshot` explicitly — perfect for opening the crash
window.

1. `audit_rows_survive_crash_window_counter_reset` (red before fix) —
   command A, command B (seqs 1, 2), **no snapshot**, crash-restart
   from the same store, command C. Assert three distinct audit
   objects exist under `audit/<day>/` and A's and B's bodies are
   intact (today C overwrites seq 1).
2. `audit_write_never_overwrites` (red before fix) — the write must be
   `put_if_absent`; on collision, pick the next free seq (or a
   restart-suffix — see Fix shape) rather than clobbering. Assert via
   MemStore `ops()` (`store.rs:226`) that no plain `PUT audit/...`
   occurs and via content that both rows survive.
3. `audit_seq_still_monotonic_within_process` — regression: the happy
   path (existing :253 test) keeps its increasing-seq property.
4. `failed_ingest_audits_rejected_not_accepted` — reorder pin: when
   `ingest`/`flush_log` fails after audit (drive with the lease-lost
   fence — `mark_lease_lost` then a command), the audit trail must
   not contain a lone `Accepted` row for the command. Either the
   audit is written after the effect, or a compensating
   `Rejected`/`Failed` row lands. Pick per Fix shape.

### Fix shape

- Write audit rows with `put_if_absent`; on `PutOutcome::Collision`,
  advance the seq and retry (bounded loop — the collision means a
  prior generation used the number; the next free slot is at most k
  ahead). This makes the key allocation self-healing without new
  persisted state. Update the contradictory doc comments
  (`layout.rs:90-93` — delete the phantom command_id-dedup claim;
  `runtime.rs:440-444`) to the actual contract.
- Ordering: move `record_audit` **after** the successful
  `ingest`+`flush_log` in `record_and_ingest` (the audit describes an
  applied command). `retry_failed`'s audit-only path (:193-217) is
  unaffected. If moving it breaks an existing pinned behavior, the
  alternative is an explicit outcome field written after the fact —
  but prefer the reorder; it is smaller.
- Audit remains lease-fenced (F02) — don't disturb that.

## Item 2 — F25: reject nonsense phase transitions

### Problem

Nothing enforces phase legality. `transition_phase`
(`state.rs:344-366`) only drops exact no-ops. Concretely:

- `JobPaused` on a `Cancelled` job → `Cancelled → Paused`
  (`state.rs:131-135` — no pausable check).
- `JobResumed` **hardcodes `from: Paused`** (`state.rs:150`) and
  computes the resume target as the `.from` of the last non-Paused
  history entry (`:141-148`) — so resuming a never-paused `Copying`
  job drives `Copying → Planned` (the rewind bug), or `→ Scanning` on
  empty history.
- Terminal arms (`JobCancelled`/`JobCompleted`/`JobFailed`,
  `:154-170`) transition from anything, including other terminal
  states.

The command handlers (`server/command.rs:103-187`) never read
`job.phase` — `require_job` (:54-62) checks existence only — so the
API happily accepts pause-a-cancelled-job and emits the event.

### Acceptance tests — write these FIRST

Reducer tests live in `state.rs` `mod tests` (:505-998; mirror
`pause_then_resume_returns_to_prior_phase` :577). Endpoint tests in
`command_endpoints.rs` (mirror `pause_unknown_job_404s_and_writes_no_audit`
:100 for the reject-with-no-side-effects shape).

5. `pause_on_terminal_job_is_rejected` (red before fix) — endpoint
   returns 409 with a code like `invalid_phase`, message naming
   current phase and the command; no event ingested, no audit
   `Accepted` row, no phase change.
6. `resume_on_non_paused_job_is_rejected` (red before fix) — the
   rewind bug's endpoint-level pin: resume on a `Copying` job → 409;
   phase stays `Copying`; `phase_history` unchanged.
7. `resume_restores_prior_phase` — regression: pause `Copying` →
   resume → `Copying` (the existing :577 test keeps passing).
8. `reducer_ignores_illegal_transition_events` (red before fix) —
   defense in depth for replay: a `JobPaused` envelope against a
   `Cancelled` job in the log (crafted directly, as a
   pre-guard-history or buggy-writer artifact) must be a warn+no-op
   in `Snapshot::apply`, not a state change. Same for `JobResumed`
   on non-Paused. Terminal states are absorbing: no event moves a job
   out of `Completed`/`Failed`/`Cancelled`.
9. `legal_matrix_table` — pure table test over a new
   `phase_transition_allowed(from, to) -> bool` (or
   `Phase::can_pause()/can_resume()`-style predicates): Paused
   re-entry from active phases only, linear progression per the
   `Phase` doc comment (`schema.rs`), terminal trio absorbing.

### Fix shape

- Pure legality predicate(s) next to `Phase` (schema.rs) — the doc
  comment at `schema.rs:126-128` already states the intended rule;
  encode it.
- Command layer: after `require_job`, consult the job's current phase
  and reject illegal commands with a new `ApiError::conflict` (**409**
  — `server.rs` has no conflict constructor today; add it beside
  `bad_request`/`not_found` :163-173). Reject **before** audit/ingest
  so no side effects leak.
- Reducer: `transition_phase` gains the same guard as a warn+no-op
  (replay must tolerate historical illegal events — never panic, never
  apply). `JobResumed` derives `from` from the job's actual phase, not
  a hardcoded `Paused`.
- Do not change the resume-target derivation for the legal case
  (last non-Paused `.from` is correct once resume is only legal from
  `Paused`).

## Item 3 — F24: bound what one worker can bloat

### Problem

COORD_PLAN §3.3 (`COORD_PLAN.md:362-368`) promises: per-class rate
caps on `ErrorEmitted` streaming (excess folds into
`ErrorBucket.count`), `WorkerHeartbeat` never streams, `ProgressDelta`
coalesced to 1 Hz per (job, worker). None of it exists — `ingest`
(`runtime.rs:219-250`) applies and broadcasts everything.

The state-bloat half: `ErrorEmitted` buckets per job are an unbounded
`Vec<ErrorBucket>` keyed by `ErrorClass` equality (`state.rs:252-281`)
— `ErrorClass::Other(String)` (`schema.rs`) is free-form, so one
worker emitting distinct strings grows live state, every snapshot, and
the event log without bound (only `sample_paths` is capped,
`ERROR_SAMPLE_CAP = 10`). Worker rows are also unbounded (re-registration
mints fresh UUIDs), and `Job.phase_history` grows per transition —
note both, cap what's cheap.

JobId length/charset is already bounded (`JobId::MAX_BYTES = 128`,
hardening-batch-1) — do not re-do that.

### Acceptance tests — write these FIRST

10. `error_bucket_count_capped_per_job` (red before fix) — emit more
    distinct `Other(String)` classes than the cap (constant next to
    `ERROR_SAMPLE_CAP`, suggest 64/job); assert the bucket Vec stops
    growing and the overflow folds into a catch-all bucket (e.g.
    `Other("(overflow)")` with an accurate count) — records are
    counted, identities dropped. Extend
    `error_emitted_aggregates_per_class` (`state.rs:644`).
11. `progress_delta_coalesced_per_job_worker` (red before fix) —
    ingest N `ProgressDelta` events for one (job, worker) inside one
    second of injected clock; assert state reflects the latest values
    but the broadcast/event log carries at most the 1 Hz cap
    (op-log/subscriber-count assertion via the stream test harness in
    `stream.rs` tests).
12. `worker_heartbeat_never_streams` — pin the §3.3 rule if heartbeats
    currently reach the bus; if they already don't hit the event log,
    pin that with a test and note it.
13. `caps_do_not_break_totals` — regression: files/bytes totals and
    bucket counts remain exact under capping (identity is lossy, the
    counts are not).

### Fix shape

- Bucket cap in the reducer arm (`state.rs:252-281`): cheapest, bounds
  state+snapshot+replay cost simultaneously. Constant + doc comment.
- Coalescing/rate caps at the **ingest boundary** (`runtime.rs::ingest`
  or a thin shim in front of the broadcast): state applies every
  delta; the event log/bus carry the coalesced stream. Keep the
  mechanism simple (per-(job,worker) last-emitted timestamp via the
  injected clock; per-class token counter for ErrorEmitted). Do NOT
  add a background task — decide inline at ingest.
- If coalescing the *event log* (not just the bus) changes replay
  semantics beyond what a session can verify, it is acceptable to cap
  the bus only and record the log-side cap as a follow-up in the
  ledger row — say so explicitly in the report.

## Out of scope / do NOT

- No changes to flush-before-ack (F03), the lease fence (F02), archive
  (F23), or the tail-reader catch-up seams.
- No auth/trust-boundary work (F20) — caps here are resource bounds,
  not security.
- No TUI changes.
- Do not re-tune `ERROR_SAMPLE_CAP`.
- No new persisted state for the audit counter (the put_if_absent
  retry makes it unnecessary).

## Definition of done

- [ ] Tests 1–2, 5–6, 8, 10–11 written first and observed red.
- [ ] All acceptance tests green; full gate green (fmt, clippy,
      workspace tests, deny).
- [ ] `layout.rs`/`runtime.rs` audit doc comments corrected;
      `ApiError::conflict` added; COORD_PLAN §3.3 checkboxes/notes
      updated with delivered-by references.
- [ ] Ledger F22/F24/F25 updated; this doc's Status flipped.
