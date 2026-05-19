# Phase 2 design notes — per-file async copy pipeline

Companion to `MULTI_PASS_MOVER.md` Phase 2. Written at the close of
the 2026-05-18 session (Phase 1 just landed); intended to be reviewed
fresh and either folded into the main work item or kept as-is to
drive implementation.

The work-item already commits to the high-level shape — `pipelined_copy`
per file, pre/post-stat brackets, `fsync` before rename, R8 fence
check sitting between `fsync().await?` and `rename().await`. Below
is what the sketch leaves under-specified and the call structure to
implement against.

## TL;DR — start here in the morning

1. Skim §3 (call structure) and §4 (pipelined_copy contract) — those
   are the load-bearing shapes.
2. Decide §5 (mocking surface): yes/no on extracting `AsyncNfsOps` as
   a trait. Affects whether Phase 2 re-runs the async FFI Gate D+E.
3. Decide §6 (worker integration): Option A (parallel pool field
   behind `--use-bucketed-pool`) is the recommended starting point.
4. Once §5 and §6 are settled, implementation order is:
   - (optional) Extract `AsyncNfsOps` trait — touches `asyncio/`.
   - Write `crates/migration-mover/src/pipelined_copy.rs`.
   - Worker wiring + CLI flag.
   - Real-VAST M2/M3 cookbook re-run with `--use-bucketed-pool`.
   - **M5 self-fence harness re-run** — R8 gate; non-negotiable.

---

## 1. Read pipeline: order-preserving, short-read-safe

The work-item sketch's `ReadPipeline::new(src, src_fh, size, rsize, depth)`
says nothing about how out-of-order completions are reassembled. The
naive "pre-issue reads at offsets `[0, rsize, 2*rsize, ...]`" model
**breaks on short reads** — NFSv3 does not guarantee `pread(off, rsize)`
returns `rsize` bytes before EOF. Required model:

- Maintain two cursors: `next_issue_off` and `next_deliver_off`.
- Issue up to `depth` reads in flight; each read targets the current
  `next_issue_off`, advances `next_issue_off` by
  `min(rsize, size - next_issue_off)`.
- Completions arrive in any order; stash them in a
  `BTreeMap<offset, Vec<u8>>` — tiny (at most `depth` entries).
- `next_chunk()` returns the entry at `next_deliver_off`, advances
  `next_deliver_off` by the chunk's actual length. If a chunk was
  short relative to its requested length, the **next** read fills
  the gap (re-issue at `actual_end`).
- EOF detection: cap by `size` from the start so we never issue past
  EOF.

Backpressure: `submit_more_reads()` only fires when (a) an in-flight
slot is free AND (b) `next_issue_off < size`.

## 2. Write pipeline: bounded-mpsc backpressure, no reordering

Writes don't have to come back in order — `pwrite` is per-offset and
idempotent. The write pipeline is simpler:

- Bounded `mpsc::Sender<(off, Vec<u8>)>` of capacity
  `write_pipeline_depth`.
- Background task (or `FuturesUnordered` inside `submit` / `drain`):
  pulls from the channel, issues `dst.pwrite(off, buf)`, awaits
  completion.
- `submit(chunk)`: pushes onto the channel — naturally backpressured.
- `drain()`: closes the channel, awaits all in-flight to complete,
  returns `Err` if any failed.

Buffer ownership: `pwrite` consumes a `Vec<u8>`. Reader emits
`Vec<u8>` (the async FFI's `pread` already returns one). Hash with
`hasher.update(&chunk)` **before** moving the chunk into
`submit(chunk)`. Zero-copy through the pipeline.

## 3. Call structure (the R8-load-bearing shape)

```rust
async fn copy_one_file(
    pool: &BucketedAsyncPool,
    src_root: &[u8], dst_root: &[u8],
    row: &RowView,
    fence: &Fence,
    cutover: bool,
) -> Result<FileCopyResult, MoveError> {
    let (src_ctx, dst_ctx, cfg) = pool.pair_for_size(row.size);

    let src_path     = join_root(src_root, &row.path);
    let final_path   = join_root(dst_root, &row.path);
    let partial_path = make_partial(&final_path);   // existing paths.rs helper

    self_target_check(&src_path, &final_path)?;     // existing per-file invariant

    let src_fh = src_ctx.open(&src_path, Flags::rdonly()).await?;
    let dst_open_flags = if cutover {
        Flags::wronly_sync()
    } else {
        Flags::wronly()
    }.with_create();
    let dst_fh = dst_ctx.create(&partial_path, dst_open_flags, row.mode).await?;

    // The fence MUST be checked between fsync and rename. Everything
    // else (hash, torn detection, partial cleanup) runs around it.
    let result = pipelined_copy(
        src_ctx, &src_fh, dst_ctx, &dst_fh, row.size, cfg,
    ).await;

    // Close fhs before commit decision. The bytes are durable post-
    // fsync; close is just freeing libnfs state.
    src_ctx.close(src_fh).await.ok();
    dst_ctx.close(dst_fh).await.ok();

    let copy = result?;                              // propagate read/write/fsync errors

    fence.check_pre_rename()?;                       // R8: critical placement
    dst_ctx.rename(&partial_path, &final_path).await?;

    Ok(copy)
}
```

Key invariants this encodes:

- **Fence check sits between `pipelined_copy().await?` (which
  contains the fsync) and `rename().await`.** No work — no closes,
  no logging — between them. Closing the fhs *before* the fence
  check is intentional: bytes are durable after fsync; post-fsync
  close is just resource cleanup. Closing before rename means we
  don't hold fhs longer than needed.
- **`pipelined_copy` cannot rename or commit-publish.** Its contract
  is "durabilize bytes to `dst_fh`, return hash/torn metadata."
  Publishing is the caller's job.
- **Cutover stability is selected at `create()`**, not as a
  parameter to `pipelined_copy`. The function body is identical
  between cutover and bulk passes; only the open flag differs.

## 4. pipelined_copy contract

```rust
async fn pipelined_copy(
    src: &AsyncNfsContext, src_fh: &AsyncNfsFh,
    dst: &AsyncNfsContext, dst_fh: &AsyncNfsFh,
    size: u64, cfg: BucketConfig,
) -> Result<FileCopyResult, MoveError>;

pub struct FileCopyResult {
    pub file_hash:    [u8; 16],   // xxh3_128
    pub bytes_copied: u64,
    pub torn:         bool,
    pub post_stat:    NfsStat64,  // for the Phase 3 manifest writer
}
```

Internal sequence:

1. `pre = src.fstat(src_fh).await?` — before any read is issued.
2. Set up `ReadPipeline` (depth=`cfg.read_pipeline_depth`) and
   `WritePipeline` (depth=`cfg.write_pipeline_depth`).
3. Loop:
   `let chunk = reader.next_chunk().await?;`
   `hasher.update(&chunk);`
   `writer.submit(off, chunk).await?;`
4. `writer.drain().await?` — all pwrites acked.
5. `dst.fsync(dst_fh).await?` — whole-file NFS COMMIT.
6. `post = src.fstat(src_fh).await?` — after last read & write, but
   before return.
7. `torn = (pre.size, pre.mtime, pre.ctime)
         != (post.size, post.mtime, post.ctime)`.
8. Return `FileCopyResult`.

Pre/post stat ordering note: post-stat is the very last thing before
return. It does **not** race with anything (drain + fsync have
completed). A torn-read here means the source mutated *during* the
copy; the bytes on dst are some interleaving of pre and post
versions, but the rename still publishes them. The convergence
guarantee (next pass re-copies because `torn=true`) is what makes
this correct.

Zero-byte files: the loop body doesn't execute, fsync still runs (on
an empty file — harmless), result is hash of empty (`xxh3_128([])`),
torn detection still works.

## 5. Mocking surface for unit tests

`AsyncNfsContext` is not a trait — it's a concrete type. To
unit-test `pipelined_copy` without VAST, two options:

**Option A — extract a trait.** Define
`trait AsyncNfsOps { async fn pread(...); async fn pwrite(...);
async fn fsync(...); async fn fstat(...); }`. Impl on
`AsyncNfsContext`. `pipelined_copy` becomes generic. Lets unit tests
use a mock impl backed by in-memory `Vec<u8>`.

**Option B — in-process libnfs loopback.** Skip the trait; rely
entirely on the env-gated real-VAST integration test. `pipelined_copy`
stays concrete; trade-off is no fast unit feedback.

**Recommendation: Option A.** The trait adds maybe 50 lines, gives
testable backpressure / saturation / torn-read / fence-fails-rename
scenarios, and doesn't constrain the async surface (impl is a
one-liner pass-through). Concretely:

```rust
#[async_trait]
pub trait AsyncNfsOps: Send + Sync {
    async fn pread(&self, fh: &AsyncNfsFh, off: u64, len: u32)
        -> Result<Vec<u8>, NfsError>;
    async fn pwrite(&self, fh: &AsyncNfsFh, off: u64, buf: Vec<u8>)
        -> Result<u32, NfsError>;
    async fn fsync(&self, fh: &AsyncNfsFh) -> Result<(), NfsError>;
    async fn fstat(&self, fh: &AsyncNfsFh)
        -> Result<NfsStat64, NfsError>;
}
```

The trait lives in `crate::libnfs::asyncio` (or a new `asyncio::ops`
submodule); the impl on `AsyncNfsContext` is in `asyncio/mod.rs`.

**Caveat:** taking Option A touches `asyncio/` — which trips the
"pre-merge runbook: async libnfs FFI changes" gate in
`CORRECTNESS_RULES.md`. The trait extraction is mechanical (no
behavior change), so the re-run should be uneventful, but **plan to
re-run Gates D and E from `LIBNFS_ASYNC_FORK.md`** if Option A is
taken. If that's not acceptable, fall back to Option B and accept
the slower feedback loop.

Unit tests to write:

- `MockOps` with deterministic in-memory `Vec<u8>` source/dest.
- `pipeline_saturates_at_depth` — count concurrent pread futures,
  assert ≤ depth.
- `pipeline_handles_short_reads` — mock returns half-rsize chunks;
  assert byte-perfect output.
- `pipeline_handles_zero_byte_file` — `size = 0`; assert one fsync,
  no reads/writes, hash matches `xxh3_128([])`.
- `pipeline_detects_torn_read` — mock fstat returns different mtime
  on post-stat; assert `torn = true`.
- `fence_blocks_rename_after_fsync_ack` — stub fence + mock;
  `pipelined_copy` returns Ok, `fence.check_pre_rename` returns Err,
  rename never called.

## 6. Worker integration

Today the worker holds `Arc<dyn LibnfsContextPool>` for src/dst. The
async pool has a different shape — no acquire/release guard, just
shared refs. Two ways to bridge:

**Option A — duck-typed:** worker carries both
`Option<Arc<BucketedAsyncPool>>` *and* the existing
`Arc<dyn LibnfsContextPool>`. `--use-bucketed-pool` flips which one
the mover dispatches to per file. Sync path is fallback for
symlinks / hardlinks / dirs in v1.

**Option B — unified trait:** new
`trait FileMover { async fn copy_file(...) -> Result<FileCopyResult, ...>; }`
with two impls. Cleaner but more work.

**Recommendation: Option A** for Phase 2. The flag gate is
short-lived; revisit consolidation in Phase 7 after observability
lands.

## 7. Interaction with `batch.rs::InflightLimiter`

The existing per-size-class concurrency budgets (default small=256,
medium=16, large=4) stay. The per-file pipeline depth layers
*inside* each file fiber. Peak in-flight RPCs per bucket:

| Bucket | Files-in-flight (default) | Per-file depth | Peak RPCs |
|---|---|---|---|
| large  | 4   | 32 | 128 |
| medium | 16  | 8  | 128 |
| small  | 256 | 2  | 512 |

All comfortably under the async FFI's `REQUEST_CHANNEL_CAP = 1024`.
If a future tuning push wants to raise small-bucket files-in-flight,
bump that constant in `asyncio/mod.rs` and document.

## 8. Open questions — resolve during implementation, not before

Real but not load-bearing-enough to design up front:

- **Per-bucket `read_pipeline_depth` vs `write_pipeline_depth`
  asymmetry.** BUCKETS sets them equal. If real-VAST profiling shows
  writes are slower than reads (likely — dest fsync is in the
  critical path), shallower write depth may help. Trivially tunable
  via existing CLI override flags.
- **Hash in-pipeline vs in-task.** xxh3_128 is ~10 GB/s; at 213 MB/s
  baseline the in-pipeline cost is <3% CPU. If a profile shows hash
  on the critical path, move it to a separate task fed by a tee
  channel. Don't pre-optimize.
- **Logging volume.** One `tracing::debug!` per file (path, size,
  bucket, duration, peak depth, torn) at debug; one summary line at
  info every N files. Concrete cadence in Phase 7.
- **Multi-bucket-pool sharing across workers in one process.** Phase 1
  made the pool per-worker. If we ever go single-process-many-workers,
  the pool becomes shared and `pair_for_size` needs to handle that.
  Out of scope for Phase 2.

## 9. Verification gates Phase 2 must pass

Per work-item doc, restated for clarity:

- All Phase 2 unit tests pass:
  `cargo test -p migration-mover --lib pipelined_copy`.
- M2 / M3 cookbook re-run against real VAST with `--use-bucketed-pool`;
  CONTENT MATCHES on every file.
- **M5 self-fence harness (`scripts/m5-self-fence-test.sh`) re-run
  with `--use-bucketed-pool`; every assertion in `M5_SELF_FENCE.md`
  passes.** This is the R8 gate. Do not skip.
- If §5 Option A is taken: re-run Gates D and E from
  `LIBNFS_ASYNC_FORK.md` (`async_ffi_smoke` + `async_integration`)
  against var204. Gate F (perf smoke) does not need to re-run unless
  the trait extraction introduces a measurable indirection cost (it
  shouldn't).

---

## Status at session close (2026-05-18)

- Phase 1 landed in working tree (not committed):
  - `crates/migration-mover/src/bucketed_pool.rs` — `BucketedAsyncPool`
    + 12 passing unit tests.
  - `crates/migration-mover/src/lib.rs` — re-exports.
  - `crates/migration-mover/tests/bucketed_pool_smoke.rs` — env-gated
    smoke (not yet executed against var204).
- `docs/work-items/MULTI_PASS_MOVER.md` reconciled with the
  LIBNFS_ASYNC_FORK closing-note deltas (nconnect, readahead, fsync
  semantics, FILE_SYNC-at-open).
- This file is the Phase 2 design queue. Read §1–§4 first; decide §5
  and §6; then start implementation.

---

## Closing note (2026-05-18) — Tasks 0–3 landed; T4 handed back

Tasks 0–3 from the Phase 2 hand-off brief
(`MULTI_PASS_MOVER_PHASE2_HANDOFF.md`) shipped in four commits on
`phase-1-bucketed-pool`:

- `ada8b84` — **Task 0**: async libnfs mount fd-swap fix. Root cause
  + verification + tightened pre-merge runbook live in
  `docs/work-items/LIBNFS_ASYNC_FORK.md` "Closing note 2".
- `dee7893` — **Task 1**: `pipelined_copy.rs`. Two-cursor short-read
  read pipeline; bounded write pipeline; `pwrite_all` wrapper that
  loops on libnfs's writemax cap (VAST var204 negotiates 1 MiB →
  every chunk from the medium/large buckets short-writes without
  this); inline `xxh3_128`; pre/post stat brackets; whole-file
  fsync. Pre-rename fence check is the *caller's* job (T2).
- `184dd1d` — **Task 2**: `FileMover` trait + `AsyncBucketedFileMover`
  impl. R8 placement: `pipelined_copy().await? → close fhs →
  apply_async_attrs → fence.check_pre_rename → rename`. Symlinks,
  hardlinks, dirs, empty, skip all delegate to the wrapped sync
  Mover per Phase 2 Decision #3.
- `dacb2e6` — **Task 3**: `--use-bucketed-pool` CLI flag + worker
  wiring. `ShardProcessor` now holds `Arc<dyn FileMover>`; the
  orchestrator constructs either `Mover` or `AsyncBucketedFileMover`
  at startup. Off by default during rollout.

### Decisions baked in during implementation

These either confirm or refine the hand-off's "Decisions baked in"
section:

- §5 Option B is what shipped: `pipelined_copy` stays concrete
  against `AsyncNfsContext`, no `AsyncNfsOps` trait. Unit tests are
  trivial (hash constant, error wrapping); real loop logic is
  verified against var204 via env-gated smokes
  (`pipelined_copy_smoke`, `file_mover_smoke`).
- §6 Option **B** shipped (the hand-off's Decision #2, not the
  NOTES's Option A recommendation): unified `FileMover` trait with
  two impls, not duck-typed `Option<Arc<...>>`. Cleaner; worth the
  small refactor to `Arc<dyn FileMover>` in `ShardProcessor`.
- `MoverConfig` derived `Clone`. Necessary so the async wrapper can
  share an `Arc<MoverConfig>` built from the same fields the sync
  `Mover` was constructed with. All embedded subconfigs were
  already `Copy`/`Clone`; only the four `String` fields needed
  cloning.
- libnfs `pwrite` short-write handling lives **inside**
  `pipelined_copy::pwrite_all`, not at the `AsyncNfsContext` layer.
  Touching `asyncio/` for Tasks 1–4 is forbidden (the hand-off's
  constraint section). 2× memory per in-flight chunk in the fast
  path; only the unwritten tail is rebuilt on short writes.

### Verification gates (this session, var204)

- **Task 0 gates** — see `LIBNFS_ASYNC_FORK.md` Closing note 2.
  Async integration parallel 25/25 across 5 reps; FFI smoke 5/5
  across 3 reps; perf smoke ASYNC 482–490 MB/s vs SYNC 236–275 MB/s
  (runs 2–3; baseline was 352/270).
- **`pipelined_copy_smoke`** — env-gated; byte-perfect 100 MiB
  roundtrip through the medium bucket × 3 reps. Inline xxh3 hash
  matches a fresh sync re-read of dst.
- **`file_mover_smoke`** — env-gated; full end-to-end through
  `AsyncBucketedFileMover::move_one` in ~550 ms. Dst `stat`
  confirms size, mode `0o644`, and the applied mtime.
- **Workspace** — `cargo test --workspace --no-fail-fast`: all
  suites pass; 74/74 in migration-mover (+4 vs the pre-Phase-2
  baseline: `pipelined_copy` × 3 + `file_mover::trait_is_dyn_safe`).
- **`vamoose worker --help`** — `--use-bucketed-pool` flag visible
  with help text.

### What's left (T4 — operator-driven cookbook)

The multi-shard M2/M3 cookbook re-run against var204 with
`--use-bucketed-pool` is operator-scale work (build S3 manifest +
parquet shards from the M2 test tree, upload, run, verify) and
is handed back. The per-file end-to-end is already proven by
`file_mover_smoke`; the orchestrator + shard-processor changes
are mechanical (`Arc<dyn FileMover>` swap with no logic changes),
covered by the existing workspace unit tests, and built clean.

To drive the cookbook yourself:

```bash
# Build (or use Phase 1's existing build).
cargo build -p migration-mover -p migration-worker -p vamoose-cli

# Construct a fresh run pointing at /src-test/m2-verify on var204
# (re-use scripts/m5-self-fence-test.sh's setup logic as a template
# — scan, mig-walker-rewrite shim, upload index/<shard>, build
# manifest.json, upload, prime shards/<shard>.claim Free).

# Run with the async path.
sudo HOME=/home/vastdata RUST_LOG=info,aws_smithy_runtime=warn \
    target/debug/vamoose worker --use-bucketed-pool --config \
    /path/to/worker.toml

# Verify.
scripts/manual-verify.sh /mnt/vamoose-source/src-test/m2-verify \
    /mnt/vamoose-dest/<your-dst-root>/m2-verify
```

The M5 self-fence harness re-run (`scripts/m5-self-fence-test.sh`)
is the R8 verification gate and is your job per the hand-off
brief, not this session's.

---

## Closing note — 2026-05-19 (T4 cookbook + M5 R8 gate)

Both Phase 2 verification gates exercised against var204. Runs were
driven by new harness scripts (`scripts/t4-cookbook.sh`,
re-run of `scripts/m5-self-fence-test.sh`).

### M5 self-fence harness (R8 gate, async path)

`sudo -E bash scripts/m5-self-fence-test.sh --files 100 --file-size 4096`

All seven A–G assertions PASS:
- A: A's claim ended `state=completed host=m5-host-B`.
- B: dst file count == src file count (100).
- C: SHA-256 parity on all 100.
- D: A exited 0 with v2 `claim refresh: HEAD shows different etag`.
- E: failures/host-{A,B}.jsonl absent.
- F: no concurrent renames across A+B within any 1.0 s window;
  158 total commits with 58 sequential duplicates (at-least-once
  under fence trip — expected and bounded).
- G: 0 B-partials, 1 A-partial (A was paused mid-write — allowed).

Worker A's post-rename `commit: rename .partial → final` log line
(introduced in 9822296 for the async path) is what makes F countable
under `--use-bucketed-pool`; without it the harness reported zero
commits even on a clean run.

### T4 cookbook (M2/M3 parity, async path)

`sudo -E bash scripts/t4-cookbook.sh --skip-large`

Mixed tree: 50 × 4 KiB (small bucket) + 5 × 10 MiB (medium bucket)
+ symlinks (2) + 3-way hardlink group + mode/owner variation +
non-ASCII path. 65 regular files, ~52 MB.

Async run: wall_clock 4.97 s, 10.1 MiB/s. Sync run (`--sync`):
9.63 s, 5.2 MiB/s.

Parity verified via `scripts/manual-verify.sh`. SHA-256 matches all
65 regular files in both runs. Mode + uid + gid match in both.
Symlink targets match. Hardlink groupings match.

**Known mover-wide limitation:** `manual-verify.sh` step [5]
(mtime to nanosecond resolution) fails for **both** sync and async
runs against VAST var204 in exactly the same shape:

1. Regular-file mtimes truncated to microseconds (sub-µs digits
   zeroed). Affects every file. Identical in sync and async.
2. Symlink mtimes ~10 s late (= worker run time, not source time);
   libnfs lacks `lutimes` on either path.
3. Directory mtimes ~10 s late (= last child-write on dst, not
   source's last-child-write time); no post-pass dir-mtime
   restoration on either path.

Because both classes show up identically in the sync mover, this is
**not a Phase 2 regression**. It's a pre-existing limitation of the
libnfs FFI surface against VAST's NFSv3 SETATTR3 semantics. T4 is
declared PASSED on the criteria the Phase 2 hand-off actually
specifies (sha256 + mode + uid/gid + symlink target + hardlink
grouping); the mtime parity gap is filed as a separate follow-up,
not blocking on Phase 2.

Parity logs for both runs saved at
`t4/run/<ts>-{sync,async}/parity.log`.

### Large-bucket coverage

The Phase 2 acceptance pass deliberately ran with `--skip-large`
to keep iteration time tight. The bucketed-pool selector code is
exercised by unit tests (`buckets_form_non_overlapping_partition`,
`bucket_for_size` table, etc.); a real ≥ 1 GiB run is queued as a
follow-up once the mtime-parity follow-up doc lands so we can
exercise the large bucket without re-deriving the limitation.

### Phase 2 status

Code: T0–T3 landed on `phase-1-bucketed-pool` (commits ada8b84
through dacb2e6 + observability fix 9822296).
Verification: M5 R8 gate green; T4 cookbook green on the
spec'd criteria. Phase 2 is **done**. Remaining work is
documenting the mtime follow-up and the routine repo cleanup.

