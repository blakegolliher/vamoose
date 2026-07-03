# Review ledger

Journal of review findings and their disposition. Every finding from
a structured review gets a row here; rows are updated in place as
work lands (history lives in git). If a finding is scheduled, its
work item column links a doc in `docs/work-items/` that a fresh
implementation session can execute standalone.

Statuses: `open` (tracked, not scheduled) → `scheduled` (work-item
doc exists) → `in-progress` → `landed` (PR merged, tests green) →
`verified` (hardware-verified where the work item requires it).
`wontfix` needs a written reason in the Notes column.

## How to run a work item

Fire a fresh session with a prompt of this shape:

> Read `docs/work-items/<DOC>.md` and `docs/REVIEW_LEDGER.md` row
> <ID>. Write the acceptance tests in the doc FIRST and run them —
> the ones marked "red before fix" must fail. Then implement until
> every acceptance test passes. Iterate; do not weaken a test to make
> it pass — if a test is wrong, say so and stop. Finish with the full
> gate: `cargo fmt --all -- --check && cargo clippy --workspace
> --all-targets -- -D warnings && cargo test --workspace && cargo
> deny --all-features check`. Then update the ledger row and the
> doc's Status line, and note anything you learned that the doc got
> wrong.

Rules that bind every work item (from `docs/CORRECTNESS_RULES.md`):
no `PUT If-Match` anywhere in the claim path; do not touch FFI
signatures in `crates/migration-mover/src/libnfs/`; protocol-behavior
changes need a hardware verification note before `verified`.

## Findings — 2026-07-01 four-track review

Tracks: claim protocol vs spec; worker+mover; coord+TUI+CLI;
docs+tests+CI. Full narrative context is in `docs/HANDOFF.md`
"Known issues".

### Critical

| ID  | Area     | Finding | Work item | Status | Notes |
|-----|----------|---------|-----------|--------|-------|
| F01 | protocol | Fresh claims stealable: `check_progress_liveness` etag-mismatch/absent branches have no claim-age grace; owner then self-fences and exits. Theft cascade at fleet startup. | [CLAIM_FRESH_GRACE.md](work-items/CLAIM_FRESH_GRACE.md) | in-progress | fix + tests on branch claim-fresh-grace; hardware fast-reclaim-drill pending before verified. Spec bug (PROGRESS_LIVENESS_CROSS_CHECK §4) carried into code; found independently by two tracks. |
| F02 | coord    | Lease-lost coord still flushes event log + snapshot on shutdown; deposed coord can clobber its successor's chunks. | [COORD_LEASE_FENCE_WRITES.md](work-items/COORD_LEASE_FENCE_WRITES.md) | scheduled | |
| F03 | coord    | Events acked to workers before durable (RAM buffer, 1000-event/5-min flush); crash drops acked events incl. pause/cancel and regresses `next_seq`, freezing TUIs. | [COORD_EVENT_ACK_DURABILITY.md](work-items/COORD_EVENT_ACK_DURABILITY.md) | scheduled | |

### Major

| ID  | Area     | Finding | Work item | Status | Notes |
|-----|----------|---------|-----------|--------|-------|
| F04 | worker   | Per-host failures/downgrades JSONL overwritten on every shard flush — the at-least-once reconciliation trail silently loses records. | [WORKER_FAILURE_SINK_APPEND.md](work-items/WORKER_FAILURE_SINK_APPEND.md) | in-progress | fix + tests on branch worker-failure-sink-append |
| F05 | mover    | `FileCopyResult.torn` computed then discarded by `copy_regular`; modified-during-copy files commit silently, no downgrade record. | [MOVER_TORN_COPY_SURFACE.md](work-items/MOVER_TORN_COPY_SURFACE.md) | scheduled | |
| F06 | mover    | EOF-clamp livelock in `pipelined_copy`: source shrinks mid-copy → stale reorder-buf entry never drained → busy-spin holding the inflight permit. | [MOVER_EOF_REORDER_BOUNDS.md](work-items/MOVER_EOF_REORDER_BOUNDS.md) | scheduled | Same doc as F07. |
| F07 | mover    | `reorder_buf` unbounded: one slow read RPC can buffer the rest of a file in RAM. | [MOVER_EOF_REORDER_BOUNDS.md](work-items/MOVER_EOF_REORDER_BOUNDS.md) | scheduled | |
| F08 | mover    | `chmod` before `chown` strips setuid/setgid on NFSv3 (kill-priv semantics); rsync order is chown→chmod→utimes. Both sync + async paths. | [MOVER_ATTR_ORDER.md](work-items/MOVER_ATTR_ORDER.md) | scheduled | |
| F09 | mover    | Sync copy path never issues NFS COMMIT (unstable writes + local close + rename): durability gap on arbitrary NFSv3 destinations. | — | open | Mitigated on VAST (NVRAM ack). Needs an `nfs_fsync` binding — protected-FFI design work. |
| F10 | mover    | Symlink/hardlink commits not idempotent under at-least-once retry: reclaimed half-done shard yields EEXIST failure storms. | — | open | Design choice: EEXIST-with-matching-target = success, vs unlink-then-create. |
| F11 | mover    | Error path sends `Close` while pread/pwrite RPCs are in flight on the same fh — potential use-after-free inside libnfs. | — | open | Verify against linked .so per FFI rules; or drain before close. |
| F12 | mover    | No I/O deadline anywhere on the data plane (no `nfs_set_timeout`); hung server wedges shards forever while heartbeats look healthy. | — | open | Needs new FFI binding + config plumbing. |
| F13 | worker   | Every `process()` error marks the claim `Failed` (terminal), incl. worker-local errors; a stale binary can poison shards fleet-wide. | [WORKER_ERROR_CLASSIFICATION.md](work-items/WORKER_ERROR_CLASSIFICATION.md) | scheduled | |
| F14 | worker   | Backpressure `degraded` is a one-way trap: inputs only update after a shard completes, but degraded blocks claiming shards. | [WORKER_BACKPRESSURE_RECOVERY.md](work-items/WORKER_BACKPRESSURE_RECOVERY.md) | scheduled | |
| F15 | worker   | Hardlink groups + dir-attr ordering are batch-scoped, not shard-scoped: groups straddling a batch boundary lose nlink fidelity silently. | — | open | Interacts with multi-pass design (MULTI_PASS_MOVER phases 3–8). |
| F16 | worker   | R4 TOCTOU: heartbeat snapshots held claim, `complete()` races it, stale HEAD → spurious self-fence on a clean completion. | [PROTOCOL_TEST_PACK.md](work-items/PROTOCOL_TEST_PACK.md) | scheduled | Fixed alongside the heartbeat test pack. |
| F17 | worker   | Shutdown watchdog forces `_exit(0)` on wedged shutdown — supervisors see success for fenced/failed runs. | [HARDENING_BATCH_1.md](work-items/HARDENING_BATCH_1.md) | scheduled | |
| F18 | coord    | SSE catch-up reads only flushed chunks; events in the writer buffer at subscribe time are silently missed (doc claims "gaps impossible"). | [COORD_EVENT_ACK_DURABILITY.md](work-items/COORD_EVENT_ACK_DURABILITY.md) | scheduled | Same root as F03. |
| F19 | coord    | Lease refresh is HEAD-then-unconditional-PUT: a completed takeover can be overwritten by the deposed holder's stale refresh. | — | open | Fencing-token design; adjacent to F02. |
| F20 | coord    | `POST /workers/{id}/events` accepts any `EventKind` from any cluster-secret holder (admin bypass), no idempotency key, non-atomic batches. | — | open | Trust-boundary + dedup design. |
| F21 | coord    | Auth defaults fail open: no tokens configured = dev mode, on default bind `0.0.0.0:8443`. | [HARDENING_BATCH_1.md](work-items/HARDENING_BATCH_1.md) | scheduled | |
| F22 | coord    | Audit keys unconditionally PUT with an in-memory day-seq: post-crash counter reset silently overwrites audit rows; audit written before effect. | — | open | |
| F23 | coord    | `archive_job` has zero production callers: event log grows unbounded; coord start / TUI start / SSE reconnect are O(entire history) S3 reads. | [COORD_ARCHIVE_WIRING.md](work-items/COORD_ARCHIVE_WIRING.md) | scheduled | |
| F24 | coord    | COORD_PLAN §3.3 rate caps / cardinality bounds unimplemented: `ErrorClass::Other(String)` + unvalidated job ids let one worker bloat state, snapshot, and log. | — | open | |
| F25 | coord    | Phase machine allows nonsense transitions (pause a cancelled job; resume rewinds a copying job to Planned); commands don't validate current phase. | — | open | |
| F26 | tui      | No REST bootstrap (streams entire log from seq 0) and `Resync` recovery unimplemented — overflow permanently desyncs counters. | — | open | Plan §3.4/P4 deliverable gap; pairs with F23. |
| F27 | tui      | Terminal restore is Drop-based; under release `panic=abort` a panic leaves the operator terminal raw. | [HARDENING_BATCH_1.md](work-items/HARDENING_BATCH_1.md) | scheduled | |

### Test-coverage gaps

| ID  | Area     | Finding | Work item | Status | Notes |
|-----|----------|---------|-----------|--------|-------|
| F28 | tests    | `heartbeat.rs` has zero tests — the S3-side fence trigger is the only unautomated leg of the anti-dual-writer chain. | [PROTOCOL_TEST_PACK.md](work-items/PROTOCOL_TEST_PACK.md) | scheduled | |
| F29 | tests    | `s3.rs` has zero tests — the 200/404/412 → outcome mapping IS the at-most-once guarantee. | [PROTOCOL_TEST_PACK.md](work-items/PROTOCOL_TEST_PACK.md) | scheduled | |
| F30 | tests    | No harness anywhere runs two LIVE workers concurrently (every M5 harness serializes them) — exactly the configuration F01 breaks. | [CLAIM_FRESH_GRACE.md](work-items/CLAIM_FRESH_GRACE.md) | in-progress | fix + tests on branch claim-fresh-grace; hardware fast-reclaim-drill pending before verified. |
| F31 | tests    | `vamoose-cli` `config.rs` (255 lines, every subcommand funnels through it) has zero tests; no clap `debug_assert` test. | [HARDENING_BATCH_1.md](work-items/HARDENING_BATCH_1.md) | scheduled | |
| F32 | tests    | `mig-walker-rewrite` schema-drift rejection untested (the M2 incident-1 class); only the happy path is covered. | — | open | |

### Minor / hygiene

| ID  | Area     | Finding | Work item | Status | Notes |
|-----|----------|---------|-----------|--------|-------|
| F33 | worker   | ~12 `eprintln!("[shutdown] ...")` debug lines in the production shutdown path. | [HARDENING_BATCH_1.md](work-items/HARDENING_BATCH_1.md) | scheduled | |
| F34 | worker   | Dead "adopt observed etag" branch in heartbeat — unreachable today, ownership-forgery if refresh semantics ever change. | [PROTOCOL_TEST_PACK.md](work-items/PROTOCOL_TEST_PACK.md) | scheduled | Delete or turn into invariant violation. |
| F35 | mover    | `uring.rs` `FixedBufferPool::acquire` is `todo!()` — panic=abort landmine. | [HARDENING_BATCH_1.md](work-items/HARDENING_BATCH_1.md) | scheduled | |
| F36 | aggr     | `migration-aggr` is 100% `todo!()` stubs behind a documented CLI; any subcommand aborts. Nothing cleans orphaned `.partial` files today. | — | open | Product decision: implement `clean-partials`/`verify`, or gut to `bail!` like `vamoose aggr`. |
| F37 | coord    | `JobId` permits `_cluster` (collides with the cluster log prefix), `.`/`..`, control chars, unbounded length. | [HARDENING_BATCH_1.md](work-items/HARDENING_BATCH_1.md) | scheduled | |
| F38 | tui      | Unknown future `EventKind` fails serde → disconnect → replay of the same frame → permanent reconnect loop against a newer coord. | — | open | |
| F39 | tui      | stderr tracing layer always installed — any log line corrupts the ratatui display (plan §3.7 unimplemented); `vamoose tui` silently starts the S3 log uploader. | — | open | |
| F40 | worker   | `verify_shard_etag` silently skips the integrity check when either etag is empty — reachable bypass on production manifests. | — | open | |
| F41 | mover    | Sync path reports `row.size` as bytes_moved on EarlyEof/Skip — inflates throughput counters (async path reports actual). | — | open | |
| F42 | worker   | Transient S3 errors on scan/acquire/download `?`-propagate to process exit; a sustained S3 hiccup takes the fleet down. | — | open | Fold into F13's classification follow-up. |
| F43 | ci       | No MSRV job despite `rust-version = 1.75`; SCHEMA_CONTRACT.md's promised cross-repo drift check unimplemented. | — | open | |
| F44 | docs     | DESIGN.md claim-protocol sections still describe v1 (freshness banner added 2026-07-01; full rewrite pending). | — | open | |
| F45 | coord    | Coord config requires `[nfs]` section it never uses; runtime mutex held across S3 PUT during chunk flush (documented, hurts at 10k events/s). | — | open | |

## Suggested execution order

1. F01+F30 (CLAIM_FRESH_GRACE) — blocks M6.
2. F04, F05 (the two silent data-loss accounting holes) — small diffs.
3. F02, F03+F18 (coord durability pair).
4. F06+F07, F08 (mover robustness).
5. F13, F14 (worker policy).
6. F16+F28+F29+F34 (PROTOCOL_TEST_PACK), F17+F21+F27+F31+F33+F35+F37 (HARDENING_BATCH_1), F23 (archive) — parallelizable batches.
