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
