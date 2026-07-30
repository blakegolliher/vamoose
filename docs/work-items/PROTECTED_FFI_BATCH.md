# Protected-FFI batch: I/O deadlines (F12), drain-before-close (F11), sync COMMIT (F09)

Status: in progress on branch `protected-ffi-batch`.
Ledger: F09, F11, F12 in `docs/REVIEW_LEDGER.md` (do NOT edit the
ledger or `docs/NEXT.md` from this branch).
Design: `docs/design/PROTECTED_FFI_DATA_PLANE.md` (DECIDED
2026-07-30 — this work item executes those decisions verbatim; if
implementation reality contradicts the design doc, STOP and report).
Scope: `crates/migration-mover` (FFI layer + copy paths),
`crates/migration-worker` (`config.rs` `[mover]` block, orchestrator
config plumbing, error-classification test). Work the items in
order F12 → F11 → F09; one commit per item, tests before fix within
each.

## FFI ground rules (this is the protected layer)

Exactly TWO new externs are authorized, already verified by the
coordinator (2026-07-30) as byte-identical in the pinned source tree
(`~/projects/libnfs`, tag `libnfs-6.0.2-148-gdc7e6f8`) and the
installed header, and exported by the linked
`/usr/local/lib/libnfs.so.16.0.2`:

```c
int  nfs_fsync(struct nfs_context *nfs, struct nfsfh *nfsfh);
void nfs_set_timeout(struct nfs_context *nfs, int milliseconds);
```

Re-run the verification yourself before declaring them and paste the
output into the commit message:
`grep -n "nfs_fsync(\|nfs_set_timeout(" /usr/local/include/nfsc/libnfs.h ~/projects/libnfs/include/nfsc/libnfs.h | grep -v async`
`nm -D /usr/local/lib/libnfs.so.16.0.2 | grep -wE "nfs_fsync|nfs_set_timeout"`

- MUST NOT modify or remove any EXISTING extern declaration or
  struct layout in `src/libnfs/mod.rs` or `src/libnfs/asyncio/ffi.rs`.
- MUST NOT touch `crates/migration-core/src/claim.rs`.
- MUST NOT add any new top-level `tests/*.rs` file (CI disk cliff);
  hardware cases go into EXISTING binaries
  (`libnfs_async_integration.rs`, `libnfs_ffi_smoke.rs`,
  `file_mover_smoke.rs`).
- Rust-side declarations mirror the C signatures exactly
  (`c_int` return for fsync, no return for set_timeout).

## Item 1 — F12: bind `nfs_set_timeout`, plumb `[mover] rpc_timeout_ms`

Design decision: every libnfs context gets an explicit per-RPC
timeout at creation; default 60_000 ms (matches libnfs's implicit
behavior, now explicit and configurable); `0` means "leave the
library default untouched" (do not call set_timeout). Timeout errors
must classify as retryable/transient, never shard corruption.

1. Extern + safe wrapper. Call it immediately after each context is
   created (so the mount itself is also bounded), at ALL creation
   points: sync `NfsContext::mount_url` (and thus `SimplePool` /
   `MultiPool`), async `AsyncNfsContext::mount`
   (`asyncio/mod.rs:247-335`), bucketed `mount_pair`
   (`bucketed_pool.rs:192-208`). Add the value to `MountOpts` and
   the sync mount path's config so no creation point can forget it
   (prefer making it non-optional in the signature over relying on
   call-site discipline).
2. Config: new `rpc_timeout_ms` in the worker TOML `[mover]` block
   (`migration-worker/src/config.rs:88-109`, next to
   `nfs_connections`), `#[serde(default)]` to 60_000; document `0`.
   Thread through `MoverConfig::from_options` and
   `orchestrator.rs:193-211` into both pools. Update
   `examples/worker.toml`.
3. Classification: add a test in migration-worker's existing
   error-classification test module driving a timeout-shaped
   `MoveError`/libnfs error (libnfs surfaces
   `"Command timed out"` / `RPC_STATUS_TIMEOUT`) through
   `classify_shard_error`, asserting it lands in the
   retryable/WorkerLocal bucket. If it currently classifies wrong,
   that test is your honest red; if it already passes, say so in the
   commit message and rely on the config-plumbing reds below.
4. CI acceptance tests (red-first where meaningful): config parse →
   `MoverConfig` → `MountOpts` carry the value; default is 60_000;
   `0` disables the call (pin via whatever seam the wrapper exposes,
   e.g. an `Option`/skip path unit test). Hardware: one `#[ignore]`
   case in `libnfs_async_integration.rs` against an unreachable
   address asserting failure within ~2× the configured timeout
   rather than a hang (the fd-swap test at `:235` shows the current
   60s-hang shape).

## Item 2 — F11: drain-before-close on the pipelined error path

Design decision: on any early error in `pipelined_copy`
(`pipelined_copy.rs` — write error at `:230`, read error at `:237`,
`:247`, fsync at `:258`), stop issuing new ops and AWAIT everything
still in `reads_inflight`/`writes_inflight`, discarding results,
BEFORE returning the error — so `copy_regular`'s unconditional
closes (`file_mover.rs:245-252`) act on quiescent fhs. Dropping the
futures does not cancel the RPCs (`asyncio/mod.rs:19-26`); with
Item 1 landed, the drain wait is bounded by the RPC timeout.

1. Structure the drain as a named helper so no error return can
   bypass it (e.g. run the copy body in an inner fn/block and drain
   in one place on its `Err`). Every early-return path in
   `pipelined_copy` must go through the drain — this must be
   reviewable by construction, not by convention.
2. One-time .so audit (design D2): read the pinned tree's
   `nfs_close_async`/fh-free path and record in the commit message
   whether close-with-outstanding-same-fh-ops is a real UAF in
   `libnfs-6.0.2-148-gdc7e6f8` (it informs backport urgency; the
   drain ships regardless of the answer).
3. CI acceptance: unit-test whatever pure seam the refactor exposes
   (at minimum: a test pinning that the drain helper awaits all
   futures it is handed, via plain tokio futures — no FFI needed).
   Hardware: one `#[ignore]` case in `libnfs_async_integration.rs`
   that gets reads genuinely in flight, forces an error return,
   and asserts the context remains usable afterwards (subsequent
   open/read succeeds; process doesn't crash).

## Item 3 — F09: bind sync `nfs_fsync`, COMMIT before the rename

Design decision: the sync copy path issues one whole-file COMMIT
after the write loop, before closing the write fh — mirroring the
async path's `dst.fsync` (`pipelined_copy.rs:245-258`).

1. Extern (verified above) + safe wrapper `ops::fsync(ctx, fh)`
   next to the other fh ops in `ops.rs`. Failure phase: your call
   between a new `FailurePhase::Commit` variant and reusing an
   existing phase — BUT check first whether `FailurePhase` is
   serialized into failure-sink records or matched exhaustively
   downstream; if adding a variant ripples into schema/contract
   surfaces, reuse `Write` with a distinguishable message and note
   the trade-off in the commit message.
2. Call site: `do_libnfs_copy` (`mover.rs:546-604`) after
   `stream_copy`, before `close_fh` on the write fh. A COMMIT
   failure fails the row through the normal `MoveError` path.
   Empty-file (`do_empty`), symlink, hardlink, dir rows carry no
   unstable data — no COMMIT there. If the sync path has a
   server-side-copy branch, confirm whether its data is
   server-acked and note what you find.
3. Documentation (part of the design decision): add the durability
   model to `DESIGN.md` — UNSTABLE writes + whole-file COMMIT
   before rename, on BOTH paths; note that NFSv3 metadata ops are
   protocol-synchronous; note the VAST NVRAM mitigation that
   existed before this change. Add a crash-durability note to
   `MANUAL_VERIFY.md` (a kill-the-server drill is rig work, not a
   test claim).
4. CI acceptance: none possible for the COMMIT itself (FFI +
   server); say so. Hardware: extend an existing sync smoke
   (`libnfs_ffi_smoke.rs` or `file_mover_smoke.rs`) with a
   write→fsync→read-back case exercising the new wrapper.

## Gate & handoff

Full gate before every commit:
`cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace && cargo deny --all-features check`

Three commits (one per item, tests-first within each; use the
`#[ignore = "red against ..."]`-in-test-commit convention from
PRs #30/#34 where a true red exists), plus this doc in the first
commit. Commit to this branch; do NOT push — the coordinator
reviews, re-gates, and owns push/PR. No AI/Claude attribution
anywhere (no Co-Authored-By trailers).
