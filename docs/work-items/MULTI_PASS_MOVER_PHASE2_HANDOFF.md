# Phase 2 hand-off brief — per-file async copy pipeline

Hand-off to a fresh Claude Code session. Paste the body below into the
new session as the opening message. The italic note at the very top
is for you, not the agent.

> *Branch is `phase-1-bucketed-pool` (2 commits ahead of main). Working
> tree carries unrelated dirty files (DESIGN.md, walker rewrites, M5
> scripts, libnfs/ops.rs, libnfs/pool.rs, etc.) from a prior effort —
> leave them alone. **Async libnfs mount against var204 is currently
> broken** (regression from the LIBNFS_ASYNC_FORK closing-note state on
> 2026-05-18). Phase 1 smoke is therefore blocked, and Phase 2's M2/M3
> cookbook re-run is too — see Task 0 below. Phase 1's 12 unit tests
> still pass; the bucketed pool itself is sound.*

---

## Hand-off (paste from here)

You are picking up Phase 2 of the multi-pass converging mover work
for vamoose, an NFS-to-NFS migration tool. The full design lives in
`docs/work-items/MULTI_PASS_MOVER.md`; Phase 2 specifics are in
`docs/work-items/MULTI_PASS_MOVER_PHASE2_NOTES.md`. Read both in full
before designing. They are the source of truth; this brief just
orients and locks in decisions already made.

### What's already landed

- **Phase 0** — async libnfs FFI surface
  (`crates/migration-mover/src/libnfs/asyncio/`). Closed 2026-05-18;
  closing note is the bottom of `LIBNFS_ASYNC_FORK.md`. Key
  substitutions: `fsync` is whole-file (no per-range COMMIT);
  `nconnect > 1` is rejected at mount; FILE_SYNC stability is
  selected at *open* time via `Flags::wronly_sync()`, not per-write.
  `AsyncNfsContext` is `Clone` (Arc-internal), one service task per
  context. Methods: `mount / open / create / close / pread / pwrite
  / fsync / stat / fstat / unlink / rename / utimes / chmod / chown
  / symlink / link / mkdir / readlink / queue_length`.
- **Phase 1** — bucketed async pool
  (`crates/migration-mover/src/bucketed_pool.rs`). `BucketedAsyncPool`
  owns three `AsyncNfsContextPair`s (src+dst per bucket) with
  per-bucket `rsize`/`wsize`/pipeline-depth. `pair_for_size(size)`
  selects by file size. 12 unit tests pass; env-gated smoke
  (`tests/bucketed_pool_smoke.rs`) ran against var204.
- **Existing fleet machinery** stays: claim protocol v2
  (`crates/migration-core/src/claim.rs`), R6/R7/R8 self-fence
  (`crates/migration-core/src/fence.rs`,
  `crates/migration-worker/src/heartbeat.rs`), atomic `.partial →
  final` rename commit point. Don't reimplement; don't weaken.

### Decisions baked in (do not relitigate)

These were settled in the session that produced this hand-off.

1. **No `AsyncNfsOps` trait extraction.** `pipelined_copy` stays
   concrete against `AsyncNfsContext`. No unit tests with mocks for
   backpressure / saturation / torn-read / fence-fails-rename
   scenarios — those are covered by the real-VAST integration test
   only. (This is §5 Option B in `MULTI_PASS_MOVER_PHASE2_NOTES.md`.)
   Do NOT modify `asyncio/`; if you find yourself wanting to,
   stop and report.
2. **Worker integration via a unified `FileMover` trait.** Add
   `trait FileMover { async fn copy_file(...) -> Result<FileCopyResult,
   MoveError>; }` with two impls — the existing sync libnfs path and
   the new bucketed-async path. Worker holds `Arc<dyn FileMover>`;
   the `--use-bucketed-pool` CLI flag picks which impl is constructed.
   (This is §6 Option B in `MULTI_PASS_MOVER_PHASE2_NOTES.md`.)
3. **Async pipe handles regular files only.** Symlinks, hardlinks,
   directories, and special files continue to flow through the
   existing sync strategy paths. The `FileMover` trait can either
   delegate per-row by file type, or the async impl can defer
   to the sync impl for non-regular rows — implementer's choice.
4. **Cutover stability is open-time.** Dest fhs are opened with
   `Flags::wronly_sync()` for cutover passes and plain
   `Flags::wronly().with_create()` for bulk. `pipelined_copy` is
   identical between the two; it takes already-opened fhs.
5. **Per-bucket pipeline depths from BUCKETS table:** large=32,
   medium=8, small=2. Do not tune in this phase; leave the existing
   CLI override flags for operator use. Read and write depths stay
   equal in v1.
6. **Hash inline, in-pipeline.** `xxh3_128` runs against each chunk
   before it moves into `submit()`. No separate hashing task.

### Load-bearing invariants

Read `docs/CORRECTNESS_RULES.md` in full. These are the ones Phase 2
must preserve and the ones easiest to break:

- **R8 — pre-rename fence check.** The fence check sits between
  `pipelined_copy().await?` (which contains the whole-file fsync)
  and `dst_ctx.rename(partial, final).await`. **No work between
  them** — no closing, no logging, no metric updates. The exact
  call structure is in `MULTI_PASS_MOVER_PHASE2_NOTES.md` §3; follow
  it. fh closes happen *before* the fence check.
- **Atomic rename is the only commit point.** Not the fsync, not the
  pwrite-acks. The rename. fsync durabilizes; rename publishes.
- **NFSv3 only.** If libnfs surfaces v4-flavored anything, the fix
  is "force v3", not "investigate v4". `MountOpts.version` is
  pinned to 3.
- **Path bytes (`Vec<u8>`), not strings.** Every path through
  vamoose is bytes. UTF-8 conversion is forbidden for filesystem
  paths.
- **`endpoint.root + row.path` via `migration_mover::join_root`**,
  never `endpoint.url + row.path`. This is the M2 data-loss
  cautionary tale.
- **Source/dest overlap.** Startup guard + per-file self-target
  check both stay. Don't weaken; don't bypass.
- **R4/R6/R7.** Unchanged at the claim/heartbeat layer. Phase 2
  does not touch those paths. If you find yourself editing
  `claim.rs`, `fence.rs`, `heartbeat.rs`, `orchestrator.rs`, or
  `shard_processor.rs`, stop and report.

### Your job, in order

0. **Task 0 — unblock async libnfs mount against var204.** As of the
   session that produced this hand-off (2026-05-18), the async FFI
   integration tests fail at `AsyncNfsContext::mount` with
   `Errno { errno: 4, detail: "Command timed out" }` (or "Command was
   cancelled" on parallel-mount sibling cancellation). Sync libnfs FFI
   smoke (`tests/libnfs_ffi_smoke.rs`) passes against the same
   cluster, kernel NFS mounts work, and the libnfs.so.16 binary
   predates Phase 0. The regression is in the asyncio surface itself,
   not the cluster. Specifically:
   - Failing tests:
     `concurrent_preads_no_crosstalk` (60+ s timeout) and
     `dropping_futures_does_not_break_neighbors` (fast EINTR
     cascade).
   - Passing tests (validation-only, no network):
     `rejects_non_v3`, `rejects_nconnect_gt_one`.
   - Commit `34b8434 Async libnfs FFI review follow-ups` is the only
     asyncio-touching commit since `71ea279`. Inspection shows it
     **does not touch the mount path** — all changes are on
     `AsyncNfsFh` (Drop guard via `Weak<Inner>`) and the
     `pwrite_stable` removal. So either the bug was latent in
     `71ea279` and only surfaced under some condition the closing-
     note's one-time gate run didn't hit, or the runbook (added in
     `34b8434` but not enforced against `34b8434` itself) would have
     caught something else.
   - Likely investigation paths: the service-task driver's
     mio/AsyncFd readiness loop (`asyncio/driver.rs`), the 1 ms tick
     decision flagged in the closing note, the `Request::Mount`
     handler in `asyncio/request.rs`, and whether `nfs_mount_async`
     is being driven correctly through `nfs_service` after the
     initial submit.
   - Pass criteria: `libnfs_async_integration` all four tests pass
     against var204; `libnfs_async_ffi_smoke` all five tests pass;
     `libnfs_async_perf_smoke` reproduces the 352 MB/s ASYNC vs
     270 MB/s SYNC baseline (or comparable — don't chase a 10 % delta).
   - Document the root cause and the fix in a closing note at the
     bottom of `docs/work-items/LIBNFS_ASYNC_FORK.md`. Update the
     `docs/CORRECTNESS_RULES.md` runbook if the gate spec needs
     tightening (e.g., asserting the runbook was actually executed,
     not just present).
   - Only after Task 0 is green should you start Task 1.

1. **Implement `crates/migration-mover/src/pipelined_copy.rs`** per
   §1-§4 of `MULTI_PASS_MOVER_PHASE2_NOTES.md`. Concrete contract is
   in §4. ReadPipeline / WritePipeline shapes are in §1-§2 — the
   short-read-safe two-cursor model is required.
2. **Extract `FileMover` trait.** Place is `migration-mover/src/`
   (new file `file_mover.rs`, or extend `strategy.rs` — your call).
   Two impls: keep the existing sync mover's signature behind one
   impl (`LibnfsFileMover` or similar); the bucketed-async impl
   (`AsyncBucketedFileMover`) calls into `pipelined_copy` for
   regular files and delegates to the sync impl for symlinks /
   hardlinks / dirs / special files.
3. **Worker wiring.** `crates/migration-worker/src/orchestrator.rs`
   (or wherever the pool is constructed). Add the
   `--use-bucketed-pool` CLI flag; default off. When on, construct
   `Arc<BucketedAsyncPool>` and wrap in `AsyncBucketedFileMover`;
   when off, keep today's behavior. The flag flows through to
   `MoverConfig` or equivalent.
4. **M2/M3 cookbook re-run against var204** with
   `--use-bucketed-pool`. See `memory/reference_verification_env.md`
   for invocation. Use `scripts/manual-verify.sh` (or its current
   equivalent in the repo) to assert CONTENT MATCHES on every file.
   Document the run — bytes/sec, file count, any anomalies — in a
   short closing note appended to
   `docs/work-items/MULTI_PASS_MOVER_PHASE2_NOTES.md`.
5. **Stop.** The M5 self-fence harness re-run is **not your job** —
   it's the user's R8 verification gate. Hand back with a clear
   summary of what shipped and what's left.

### Verification gates

- After Phase 2 code lands: `cargo build -p migration-mover -p
  migration-worker` clean, no new warnings. `cargo test -p
  migration-mover --lib` shows 70+ passing (no regressions vs the
  current 70-test baseline; new tests welcome but not load-bearing).
- After var204 cookbook re-run: byte-for-byte parity (sha256 + mode
  + uid/gid + mtime + symlink target + hardlink grouping) on every
  file in the test tree.
- The M5 self-fence harness (`scripts/m5-self-fence-test.sh`) is
  re-run by the user, not you.

### Constraints — what NOT to touch

- `crates/migration-mover/src/libnfs/asyncio/` — leave alone for
  Tasks 1-4. §5 Option B means no FFI surface changes for the
  pipelined-copy work itself. **Task 0 is the explicit exception**:
  if diagnosing the mount regression requires changes inside
  `asyncio/`, that's fine — but every such change requires the full
  pre-merge runbook (`CORRECTNESS_RULES.md` "Pre-merge runbook: async
  libnfs FFI changes") to be re-executed before Task 1 starts.
- `crates/migration-mover/src/libnfs/{mod.rs, ops.rs, pool.rs}` —
  sync FFI surface; stays as fallback path.
- `crates/migration-core/src/{claim.rs, fence.rs}` — R-rule
  machinery. Phase 2 consumes; does not modify.
- `crates/migration-worker/src/{heartbeat.rs, orchestrator.rs,
  shard_processor.rs}` — claim/fence consumers. If you must thread
  a config field through, that's fine, but no logic changes.
- `~/projects/vamoose/worker.toml` — has the self-overlap that
  caused the M2 data-loss incident. Never reuse. Use
  `examples/worker.toml` for any local testing.

### Verification env (var204)

Loaded automatically into your context via
`memory/reference_verification_env.md`. Highlights:

- Cluster: `https://main.selab-var204.selab.vastdata.com`
- AWS profile: `var204`
- libnfs: `/usr/local/lib/libnfs.so.16` (NOT the system v14)
- Source export:
  `nfs://main.selab-var204.../bgolliher/vamoose-source`
- Dest export:
  `nfs://main.selab-var204.../bgolliher/vamoose-dest`
- libnfs requires UID 0 — worker invocation is
  `sudo HOME=/home/vastdata RUST_LOG=info,aws_smithy_runtime=warn
  target/debug/vamoose worker run --config examples/worker.toml`
  (or current equivalent).
- M5 fast-iter mode is `--files 100 --threshold 5` (~60-90 s/iter).
  Reserve `--files 10000` for canonical pass records.

### Commit hygiene

- No "Co-Authored-By: Claude" or any LLM attribution in commit
  messages or PR bodies.
- Current branch is `phase-1-bucketed-pool`. Either continue
  committing on top of it or branch off (`phase-2-pipelined-copy`).
  Don't push without confirmation.
- Many unrelated files in the working tree are dirty from a prior
  effort. `git status` will show
  `DESIGN.md`, `M3_NOTES.md`, walker rewrites, M5 scripts,
  `libnfs/ops.rs`, `libnfs/pool.rs`, etc. — leave them unstaged.
  Stage only files your Phase 2 work genuinely touches.
- Multiple small commits along Phase 2's natural seams are
  preferred over one giant commit.

### When you're done

- Phase 2 commits on the branch.
- A short closing note appended to
  `docs/work-items/MULTI_PASS_MOVER_PHASE2_NOTES.md` capturing what
  shipped, var204 cookbook results, and any deferred follow-ups.
- A summary in your final reply that names: (a) the commits, (b)
  the green test counts, (c) the cookbook result, (d) the M5
  invocation the user should run next.

Read the two work-item docs end-to-end first. Then ask the user
which slice to start on (the natural first slice is `pipelined_copy.rs`
since `FileMover` and worker wiring both depend on it) — do not
assume.
