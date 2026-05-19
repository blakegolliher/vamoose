# Vamoose project state — handoff for next session

Last update: 2026-05-04, after PR2 hardening committed (`18f0833`).

---

## TL;DR

M2 and M3 are verified end-to-end against real VAST hardware
(baseline `327fd66`, tag `m2-m3-verified`). PR2 hardening
(`18f0833`) added the surfaces and guardrails that would have
caught the FFI silent-zero bug without needing end-to-end
verification.

The system correctly migrates the test tree (10 files including
100 MiB, 2 symlinks, 3-way hardlink, full directory tree) from one
NFS export to another with full metadata preservation and atomic
.partial rename. Source remains intact. SHA-256 verification of
content passes.

PR2 is committed but **not yet hardware-verified**. That's the
top remaining task.

---

## Repo state

Location: `~/projects/vamoose/migration/`

Recent commits:
- `18f0833` — PR2: libnfs FFI hardening + verification narrative
- `3deef13` — docs: handoff document for next session (this file)
- `228ed21` — docs: add PR2 hardening work item
- `327fd66` — Baseline: M2/M3 verified end-to-end against real VAST
  (tag: `m2-m3-verified`)

Branch: `master`. Tests: 100 passing, 1 ignored (the FFI smoke).

Outside git (session state, parent dir):
- `~/projects/vamoose/m2.parquet` — walker parquet output
  (`scans/<scan_id>/part-*.parquet`; legacy-schema, pre-canonical)
- `~/projects/vamoose/m2.canonical` — shim output (canonical schema)
- `~/projects/vamoose/migration.tar.gz` — pre-git snapshot
- `~/projects/vamoose/worker.toml` — **DANGEROUS:** still has
  self-overlap dest URL from a previous session; do not use as-is.
  Use `examples/worker.toml` from inside the repo instead.

(Historical: an `m2.rocks` directory used to live here from the
RocksDB-walker era. Walker is parquet-only now; if you still see one
of those, it's a stale artifact and can be removed.)

---

## Three bugs found during M2/M3 verification, all fixed

Documented in detail in `M2_NOTES.md` "M2/M3 verification incidents".
Summary:

1. **Walker schema mismatch.** Resolved by `mig-walker-rewrite` shim.
2. **Dest path overlap data-loss.** Resolved by 5-fix bundle:
   `join_root` helper, per-file self-target check, startup overlap
   guard, NFSv3 baseline, symlink mode degradation.
3. **libnfs FFI signature mismatch.** FFI fix landed in baseline.
   PR2 added the regression test, EARLY_EOF surface, and
   correctness rule.

Each was undetectable by unit tests; each was caught only by
end-to-end verification on real hardware. The "verify against real
hardware" rule in `docs/CORRECTNESS_RULES.md` is load-bearing.

---

## What's left from PR2

PR2 code/docs are done. Two validation steps remain, both
hardware-gated:

1. **Run the FFI smoke test against real VAST.** 30 seconds.
   Confirms the new `tests/libnfs_ffi_smoke.rs` actually exercises
   real libnfs and the parameter order matches the linked binary.

   ```bash
   VAMOOSE_TEST_NFS_URL=nfs://main.selab-var204.selab.vastdata.com/bgolliher/vamoose-source \
   VAMOOSE_TEST_NFS_PATH=/src-test/m2-verify/large.bin \
   VAMOOSE_TEST_NFS_EXPECTED_SIZE=104857600 \
       cargo test -p migration-mover --test libnfs_ffi_smoke -- \
       --ignored --nocapture
   ```

2. **Re-run the full M2/M3 verification cookbook.** Confirms PR2
   didn't regress anything. Expected output: "CONTENT MATCHES" with
   no `EARLY_EOF` records in the downgrades sink (because the FFI
   is correct).

Cookbook is in the previous version of this doc and in
`crates/migration-mover/MANUAL_VERIFY.md`.

---

## Open items not yet scheduled

- **Walker canonical-schema PR.** When walker emits canonical
  schema natively, `mig-walker-rewrite` becomes a no-op pass-through
  and gets deleted. Owner: walker repo at `~/projects/nfs-walker/`.
- **`run_prefix` for multi-run-per-bucket.** Real layout gap. The
  current bucket layout is bucket-root only. To run multiple
  migrations through one bucket, a `run_prefix` field in the
  worker config would prefix all keys.
- **xattr support in walker.** Mover already has dead code for
  applying xattrs; walker doesn't capture them. Deferred until
  walker support lands.
- **M3.5 (real io_uring).** Currently M3 uses tokio JoinSet +
  spawn_blocking; the original M3 plan called for io_uring. See
  `M3_NOTES.md` for the rationale on deferral.
- **M4 (NFSv4.2 server-side COPY).** Cancelled. NFSv3 is the
  baseline; `Strategy::ServerSideCopy` variant retained but never
  selected by `pick`.
- **M5 — multi-host self-fence test.** Next milestone per
  `DESIGN.md`. 3 workers, kill -9 mid-batch, verify reclaim +
  self-fence + no dual-writer corruption.
- **Aggregator (`mig-aggr`).** Partial implementation in
  `crates/migration-aggr/`. Not yet exercised against real data.
- **libnfs vendoring / `bindgen`.** Open questions for long-term
  FFI determinism (post-PR2).

---

## Critical environment details

See memory `reference_verification_env.md` for the durable
operational details (cluster URL, AWS profile, libnfs binary path,
NFS exports, worker invocation with `sudo HOME=/home/vastdata
RUST_LOG=...`). Also covered in `crates/migration-mover/MANUAL_VERIFY.md`.

The dangerous parent-dir `worker.toml` is captured in memory
`project_dangerous_worker_toml.md` so future sessions don't
accidentally pick it up.

---

## What the next agent should NOT do

- Touch any FFI signatures. The current order in
  `crates/migration-mover/src/libnfs/mod.rs` is correct, verified
  against the linked library. Header comment in that file warns
  about it.
- Re-implement the overlap guards. They exist and are tested.
- Change the schema contract. v1 is locked.
- Run the worker against any manifest with `dst_url` matching
  `src_url` (the overlap guard refuses, but don't tempt it).
- Use the `worker.toml` in `~/projects/vamoose/` (parent dir).

---

## Open questions for Blake

- Whether to vendor libnfs into the workspace for FFI determinism
  (long-term, post-PR2).
- Whether to set up `bindgen` against `/usr/local/include/nfsc/`
  to auto-generate FFI bindings (post-PR2).
- Whether `run_prefix` work belongs in the next milestone or is
  parked.
- Whether to start exercising the aggregator, or hold until M5+.
