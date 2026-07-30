# F20 phase 2: per-worker client_seq idempotency + convergent batch retry

Status: landed — merged in PR #40.
Ledger: F20 in `docs/REVIEW_LEDGER.md` (do NOT edit the ledger or
`docs/NEXT.md` from this branch).
Design: `docs/design/COORD_TRUST_AND_FENCING.md` Part 2, DECIDED
2026-07-30 — this item executes D4 (per-worker `client_seq`
high-water mark) and D5's storage-failure residue. Phase 1 (D2+D3
allow-list + binding + validate-before-ingest) merged in PR #38 and
is on this branch's base — build on it, don't rework it. If
implementation reality contradicts the design doc, STOP and report.
Scope: `crates/migration-coord` (`server/worker.rs`, `schema.rs`,
`state.rs`, snapshot surface, `tests/worker_endpoints.rs`) and
`crates/migration-worker` (`coord_client.rs` — the EventBuffer and
batch sender ONLY).

## The bug (current, documented residue of phase 1)

1. **Resend double-apply**: the worker's resend buffer drops entries
   only on a 200 (`coord_client.rs` batch sender); a lost response →
   resend → every entry applied twice, permanently baked into
   state, log, and snapshots.
2. **Storage failure mid-batch**: the ingest loop `?`-propagates
   (`server/worker.rs`), so entries 0..k stay applied with no
   record; the worker's retry then re-applies them (double-apply
   again).

Both collapse into one fix: dedup that survives replay.

## The fix (decided)

### Worker side (migration-worker/coord_client.rs)

Stamp each buffered event with a per-worker, monotonically
increasing `client_seq: u64` at `EventBuffer::push` time (a plain
counter on the buffer; starts at 1; survives resends because the
buffer holds the stamped entries). The batch sender transmits it
per entry. FORWARD GAPS ARE LEGITIMATE: the buffer drops entries
under budget pressure, so the coord must never reject a gap —
only order matters.

### Coord side

- `WorkerEventEntry` gains `client_seq: Option<u64>`
  (`#[serde(default)]`) — absent means a pre-upgrade worker and
  keeps today's at-least-once semantics unchanged (no flag day).
- The HWM must survive crash + replay, so it must ride the durable
  event stream: add `client_seq: Option<u64>` to `EventEnvelope`
  (next to the existing optional diagnostic `worker_at`,
  `schema.rs:620-632`, `#[serde(default)]` — additive and
  backward-compatible; old log chunks deserialize with None).
  Confirm nothing pins envelope field-set exhaustively (serde
  deny_unknown_fields, contract tests) — if something does, STOP
  and report.
- `State` gains `last_client_seq: HashMap<WorkerId, u64>`,
  maintained by the reducer when it applies an envelope carrying
  (worker attribution + client_seq) — so replay reconstructs it,
  and it must be carried by the snapshot the same way other reducer
  state is (follow how existing State maps are snapshotted; if the
  snapshot is just serialized State, this is free — verify).
- Ingest path (`server/worker.rs`, after phase 1's validation
  pass): for entries with `client_seq` ≤ the caller's HWM → SKIP
  (already applied; do not ingest, do not assign a seq). Entries
  above the HWM apply normally. Within a batch, stamped entries
  must be strictly increasing; a violation is a 400-class rejection
  of the whole batch (it means a buggy client, not a replay).
- **Response shape**: extend `EventsBatchResponse` backward-
  compatibly so the worker can still drop everything on success —
  e.g. keep `seqs` for applied entries and add a
  `deduped: u64` count, or per-entry `Option<u64>`; your call, with
  two constraints: (a) an old worker reading only a 200 status
  remains correct, (b) the new worker drops resend-buffer entries
  on 200 exactly as today. Document the choice in the module doc.

### Net effect (pin this as the headline test)

A storage failure mid-batch followed by the worker's retry of the
SAME batch converges to exactly-once effective application: state,
log contents, and counters identical to a single clean send.

## Constraints (hard fences)

- Do NOT touch: `lease.rs`, `auth.rs`, the admin command path,
  `migration-core/src/claim.rs`, `migration-worker`'s `config.rs` /
  `orchestrator.rs` / anything under `migration-mover` (a PARALLEL
  session owns those files).
- Do NOT rework phase 1's validation; it runs first, unchanged.
- No new top-level `tests/*.rs` files — coord tests extend the
  existing `worker_endpoints.rs` harness; worker-side buffer tests
  extend `coord_client.rs`'s existing test mod.
- Schema changes are ADDITIVE ONLY (`Option` + serde default); if
  `SCHEMA_VERSION` semantics require a bump for additive envelope
  fields, follow whatever precedent the codebase set (check how
  `worker_at` landed) and say what you found.

## Acceptance tests — write these FIRST, observe them red

True reds against current behavior:

1. `resend_after_lost_response_is_idempotent` — send a stamped
   batch, apply it, then send the IDENTICAL batch again (simulating
   a lost 200): second response succeeds, nothing new applied —
   job counters, error buckets, coord `last_seq`, and the durable
   log contents identical to the single send. RED today
   (double-apply).
2. `replay_reconstructs_hwm` — ingest stamped events, flush,
   rebuild the runtime from stores (the harness's existing
   restart/replay pattern — `worker_endpoints.rs` durability tests
   have one), then resend the old batch: still deduped. RED today.
3. `storage_failure_then_retry_converges` — if the harness can
   inject a store failure mid-batch (fault-injection double at the
   runtime/store seam): fail entry k, retry the whole batch, assert
   exactly-once effect. If the existing harness genuinely cannot
   inject this, cover the same convergence with test 1 (retry after
   partial simulated by ingesting a prefix manually) and SAY SO in
   the report.
4. `unstamped_entries_keep_legacy_semantics` — entries without
   client_seq behave exactly as before (apply every time), and
   mixed stamped/unstamped batches don't corrupt the HWM. GREEN
   half / RED half as applicable — state which.
5. `non_monotonic_batch_rejected` — stamped entries out of order
   within one batch → whole batch rejected, nothing applied. RED
   today (no such check exists).
6. Worker side: `event_buffer_stamps_monotonic_client_seq` — pushes
   get 1,2,3…; drain+resend preserves stamps; drops under budget
   create forward gaps and the counter never reuses a stamp. RED
   via the honest-stub convention if needed.

## Gate & handoff

Full gate before every commit:
`cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace && cargo deny --all-features check`

Suggested commits: (1) tests red + doc (PR #30/#34 `#[ignore]`
convention, observed-red quoted), (2) schema/state/snapshot
plumbing, (3) ingest dedup + worker stamping. Commit; do NOT push.
No AI/Claude attribution anywhere (no Co-Authored-By trailers).
