# Vamoose project state — handoff for next session

Last update: 2026-07-01, after the CI-green + docs-refresh pass on
branch `ci-green`.

---

## TL;DR

The system is delivered through the COORD_PLAN milestone: v2 claim
protocol (delete-then-create) verified by the M5 self-fence run on
real VAST hardware, `vamoose coord` (REST + SSE daemon), `vamoose
tui` (operator dashboard), worker → coord integration, and
supply-chain CI (fmt, clippy, cargo-deny, cargo-about).

GitHub CI had been red since the workflow landed (fmt drift, a
cargo-deny advisory/license pile-up, and a cargo-about config bug).
Branch `ci-green` fixes all of it and adds the previously-missing
clippy gate. 558 tests pass.

A four-track deep review (claim protocol, worker/mover, coord/TUI,
docs/tests) was completed 2026-07-01. The protocol core is faithful
to spec, but the review found real issues — see "Known issues" below
before starting protocol or fleet work.

---

## Repo state

Location: `~/projects/vamoose/migration/`

- `main` == `coord` (PR #7 merged 2026-05-30) — the COORD_PLAN
  delivery.
- `ci-green` — CI fixes + lint cleanup + this docs refresh.
- Tags: `m2-m3-verified` (M2/M3 baseline), `m5-pass-v2-claim-protocol`
  (M5 verification under v2).

Verification record: `docs/work-items/M5_NOTES.md` ("Pass record").
Protocol spec: `docs/CLAIM_PROTOCOL.md` +
`docs/work-items/CLAIM_PROTOCOL_V2_DELETE_THEN_CREATE.md`.

Outside git (session state, parent dir): `m2.parquet/`,
`m2.canonical/`, `aggr-fixture/`, `migration.tar.gz` — lab artifacts.
The parent-dir `worker.toml` is **dangerous** (self-overlap dest URL
from an old session); use `examples/worker.toml` instead. A stale
`m2.rocks/` directory from the RocksDB era can be deleted.

---

## Known issues (2026-07-01 review)

Full write-ups live in the session review; headline items, in
priority order:

1. **Fresh claims are stealable (critical, protocol).** The
   progress-liveness cross-check's etag-mismatch/absent branches have
   no claim-age grace: between `try_acquire` and the owner's first
   heartbeat progress PUT (≤ 30 s), any scanning peer sees
   `held_etag != claim_etag` and fast-reclaims a live claim. The
   owner then self-fences and exits. Near-deterministic theft cascade
   at fleet startup; also fires at end-of-run when idle workers scan
   aggressively. No data corruption (rename idempotency + fence
   hold), but it breaks eventual progress at fleet scale. Fix shape:
   require `now - claimed_utc > 2 × heartbeat_sec` on the
   mismatch/absent branches of
   `orchestrator.rs::check_progress_liveness`, and/or publish
   progress synchronously at acquire. Needs a harness that runs two
   *live* workers concurrently — every existing M5 harness serializes
   them.
2. **Coord: lease-lost shutdown still flushes (critical, coord).**
   `Runtime::shutdown` flushes the event log and snapshot without
   checking `lease_lost`, so a deposed coord can clobber its
   successor's chunks. Gate all store writes on the lease.
3. **Coord: events acked before durable.** Workers get seqs for
   events that live only in RAM (flush is 1000-events/5-min); a coord
   crash silently drops acked events (including pause/cancel) and
   regresses `next_seq`, which freezes reconnecting TUIs.
4. **Mover: EOF-clamp livelock + unbounded reorder buffer**
   (`pipelined_copy.rs`) — a source truncated mid-copy can hot-spin
   the task forever holding its permit; one slow read RPC can buffer
   the rest of the file in RAM.
5. **Mover: torn-copy result discarded** — `FileCopyResult.torn` is
   computed and ignored by `copy_regular`; files modified during copy
   commit silently with no downgrade record.
6. **Mover: `chmod` before `chown` strips setuid/setgid** on NFSv3
   destinations; apply owner before mode (rsync order).
7. **Worker: failure/downgrade JSONL overwritten per shard** — the
   per-host sink key is unconditionally PUT after each shard, so only
   the last shard's records survive. That's the at-least-once
   reconciliation trail — losing it converts recorded failures into
   silent data loss.
8. **Worker: every `process()` error marks the shard `Failed`
   (terminal)** — including worker-local errors (scratch I/O, schema
   version of a stale binary). One bad worker can terminal-fail
   shards the rest of the fleet could do.
9. **Worker: backpressure "degraded" is a one-way trap** — inputs
   only update after a shard completes, but degraded blocks claiming
   shards; once tripped the worker sleeps forever.
10. **No I/O deadlines on the data plane** — libnfs has no timeout
    configured (sync or async); a black-holed connection wedges a
    shard forever while heartbeats keep it looking alive.
11. **TUI: terminal restore is Drop-based** — under release
    `panic = "abort"` a panic leaves the operator's terminal raw.
    Install a panic hook.
12. **Coord auth defaults fail open** — omitting both token flags is
    dev mode (no auth) while the default bind is `0.0.0.0:8443`.
    Refuse dev mode off-loopback.

Additional majors are catalogued per-crate in the review (SSE
catch-up gap, audit-key overwrite, unbounded event log / archive
never called, phase-machine holes, batch-scoped hardlink groups,
symlink/hardlink EEXIST on at-least-once retry, sync-path missing
NFS COMMIT, R4 completion/heartbeat TOCTOU).

---

## Test-coverage gaps worth closing first

1. `migration-worker/src/heartbeat.rs` — 0 tests; it's the S3-side
   trigger of the anti-dual-writer chain (everything around it is
   tested).
2. `migration-core/src/s3.rs` — 0 tests; the 200/404/412 →
   outcome-enum mapping is what makes v2 at-most-once.
3. An automated mini-M5: two orchestrators against the in-memory
   ClaimStore with paused time (claim / die / reclaim / late-refresh
   fence / late-complete refused).
4. `vamoose-cli/src/config.rs` — 0 tests; every subcommand funnels
   through it.
5. `mig-walker-rewrite` schema-drift rejection (the M2 incident-1
   class) — only the happy path is covered.

---

## Open items (carried + new)

- **Fix the fresh-claim grace window** (known issue 1) — top of the
  protocol queue; small code change, needs the two-live-workers
  harness.
- **Coord durability pass** (known issues 2, 3, plus archive wiring).
- **Walker canonical-schema PR** (walker repo `~/projects/nfs-walker/`)
  — when walker emits canonical natively, `mig-walker-rewrite`
  becomes a pass-through and gets deleted.
- **`run_prefix` for multi-run-per-bucket** — layout still
  bucket-root-only.
- **Multi-pass mover Phases 3–8** — pass driver, `vamoose pass`;
  Phases 1–2 (bucketed pool, pipelined copy) are shipped.
- **migration-aggr** — still 100 % `todo!()` stubs behind a
  documented CLI (running any subcommand aborts). Decide: implement
  `clean-partials`/`verify` (both referenced by other docs), or gut
  the binary to `bail!` like `vamoose aggr` does. Nothing cleans
  orphaned `.partial` files today.
- **xattr support** — mover has dead code awaiting walker capture.
- **M3.5 (real io_uring)** — deferred; `uring.rs` still has a
  `todo!()` landmine in `FixedBufferPool::acquire`.
- **libnfs vendoring / bindgen** — long-term FFI determinism.

---

## Critical environment details

See memory `reference_verification_env.md` for cluster URL, AWS
profile, libnfs binary path, NFS exports, and worker invocation
(`sudo HOME=/home/vastdata RUST_LOG=...`). Also covered in
`crates/migration-mover/MANUAL_VERIFY.md`.

---

## What the next agent should NOT do

- Touch any FFI signatures. The order in
  `crates/migration-mover/src/libnfs/mod.rs` is correct, verified
  against the linked library. Header comments warn about it.
- Re-implement the overlap guards. They exist and are tested.
- Change the schema contract. v1 is locked.
- Run the worker against any manifest with `dst_url` matching
  `src_url`.
- Use the `worker.toml` in `~/projects/vamoose/` (parent dir).
- "Fix" `deny.toml`'s advisory ignores without reading their
  justification comments — the rustls-webpki trio is only reachable
  on the deliberate `verify_tls = false` bypass path.
