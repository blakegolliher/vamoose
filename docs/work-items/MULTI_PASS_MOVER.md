# Multi-pass converging mover

Status: Phase 0 prerequisite (`docs/work-items/LIBNFS_ASYNC_FORK.md`)
closed 2026-05-18 — `AsyncNfsContext` is shipped at
`crates/migration-mover/src/libnfs/asyncio/`. Phases 1–2 (bucketed
pool, pipelined single-pass copy) are delivered
(`bucketed_pool.rs`, `pipelined_copy.rs`, `file_mover.rs`); Phases
3–8 (pass driver, `vamoose pass`, PASS_DRIVER_SEMANTICS) are not
started. The async-surface deltas from the closing note have
been threaded into the `BUCKETS` table, the `pipelined_copy` sketch,
and the cutover-pass language below; in summary: `nconnect>1` is
rejected at mount, `libnfs_readahead` is gone, `fsync` is whole-file
(per-range COMMIT deferred until cutover actually needs it — see
`LIBNFS_ASYNC_FORK_AUDIT.md`), and FILE_SYNC stability is selected at
open time via `Flags::wronly_sync()`, not per-write.

---

## Goal

Add a **multi-pass converging copy mode** to vamoose so the mover
can be run against a live, actively-written source tree, with each
pass re-syncing only the files that changed since the last pass,
culminating in a **cutover pass** (writes stopped) that brings
source and destination to byte-for-byte parity.

Today vamoose is strictly single-shot: it claims shards, copies
files, exits. There is no record of what the destination tree looks
like after a run, no concept of a second pass, no way to detect or
resync drift. This work adds:

1. A **per-pass parquet manifest** of destination state, written
   atomically at end-of-pass and carried forward across passes.
2. A **streaming merge-join classifier** that diffs the latest
   walker scan against the previous pass's manifest, partitioning
   files into NEW / DIRTY / UNCHANGED / DELETED in O(n) memory.
3. A **pass driver** that wraps the existing claim/fleet machinery:
   each pass is a normal multi-worker fleet run against a synthetic
   shard index of NEW+DIRTY files, with UNCHANGED rows carried
   forward into the new manifest.
4. **Bucketed async libnfs contexts** (small / medium / large) with
   per-file pipeline depths, replacing the current
   one-context-per-pair-of-workers sync model on the inner copy
   loop. (Prerequisite: `LIBNFS_ASYNC_FORK.md`.)
5. **Torn-read detection** via pre/post stat bracketing, with the
   convergence guarantee that any torn-read is re-copied next pass
   and the cutover pass (writes stopped) is the final settle.
6. A **cutover pass mode** with FILE_SYNC writes and optional
   full-tree re-verify.

---

## Decisions baked in (do not relitigate)

These were settled in the conversation that produced this work
item. Each is here to keep the implementation from drifting back
toward the original external-prompt design that didn't account for
existing vamoose work.

- **Whole-file recopy on dirty, no block-level diff for v1.** A
  dirty file is identified by (size, mtime, ctime) tuple mismatch
  *or* a TornRead flag from the previous pass, and the entire file
  is re-copied. No 1 MiB block hashing, no per-block `block_hashes`
  column in the manifest, no rsync-style diff. Justification: the
  hashing infrastructure is non-trivial to land safely, the workload
  profile that *requires* sub-file diffs is unknown at the time of
  writing, and the original prompt's block-diff was the single
  biggest source of new complexity. If, post-launch, the resync
  passes are shown to be moving substantially more bytes than the
  bytes-actually-changed, revisit; until then, ship without it.
  The manifest schema does include `file_hash` so the cutover
  re-verify path has something to compare against — see schema below.

- **Each pass uses the existing claim protocol.** A pass is not a
  single-host operation. Pass 0 runs the existing fleet against the
  walker output exactly as today. Pass N (N≥1) generates a *synthetic
  shard index* containing only the NEW+DIRTY rows, uploads it
  alongside a pass-N manifest entry, and runs the existing fleet
  against that shard index. The S3 claim/heartbeat/self-fence
  machinery is reused unchanged — workers don't know which pass
  they're processing, they just see a shard index and process it.

- **Bucketed async libnfs is M3.5 / prerequisite, not part of this
  work.** See `LIBNFS_ASYNC_FORK.md`. This work assembles the three
  contexts into a bucketed pool, but the underlying async surface
  must already exist and be verified.

- **All five R-rules (R4 / R6 / R7 / R8 / overlap) must continue to
  hold across every pass.** The per-pass parquet manifest does *not*
  become a new commit point; the atomic `.partial → final` rename is
  still the only commit point, still gated on `check_fence()`. See
  "R-rule preservation" below.

- **No deletion from destination in v1.** DELETED rows are recorded
  in the manifest (so observability shows them) but the dest file is
  not unlinked. Flag-gated `--prune-deleted` is a follow-up.

- **NFSv3 only.** Per `CORRECTNESS_RULES.md`. No NFSv4 features.

- **Multi-worker partial manifest merging is in scope.** Each worker
  emits a per-host partial manifest as it processes its shards; the
  pass-completion step merges per-host partials + carried-forward
  UNCHANGED rows + DELETED rows into the canonical `pass_NNNN.parquet`.
  Atomic rename of `.tmp → final` is the pass commit point.

---

## What the existing codebase already provides

For implementers coming from the original external prompt: vamoose
already has substantial machinery that the prompt was unaware of.
Do not duplicate this:

- **S3-coordinated multi-worker claim protocol v2**
  (`migration-core/src/claim.rs`, `s3.rs`) — conditional PUT +
  DELETE-then-create for ownership transfer. Each pass uses this.
- **R6/R7/R8 self-fence and heartbeat**
  (`migration-worker/src/heartbeat.rs`,
  `migration-core/src/fence.rs`). Verified end-to-end against
  real VAST hardware in M5.
- **Atomic `.partial → final` commit point**
  (`crates/migration-mover/src/paths.rs:79-109`,
  `mover.rs:530-538`). The rename is the commit; it is gated on
  `fence.is_valid()` immediately before issuance.
- **Hardlink groups, symlinks, attribute order pinning**
  (`shard_processor.rs:54-67, 309-343`, `mover.rs:386-424`,
  `mover.rs:550-594`). Don't reimplement.
- **xattr-reserved schema** (`schema.rs:56`, `attrs.rs:73-99`).
  Dead code today; the manifest schema below preserves the
  reservation.
- **FFI smoke test gate** against the linked `.so`. Mandatory
  for every new FFI symbol. See `CORRECTNESS_RULES.md` "Cross-check
  C library FFI".
- **Source/dest overlap guard** (startup + per-file). Stays.

---

## Architecture

```
   ┌──────────────────────────────────────────────────────────────┐
   │                    Pass driver (new)                          │
   │                                                                │
   │   ┌──────────────┐    ┌──────────────────────────────┐         │
   │   │ Walker index │    │ Previous pass manifest        │        │
   │   │  (sorted     │    │  pass_NNNN-1.parquet          │        │
   │   │   by path)   │    │  (sorted by path)             │        │
   │   └──────┬───────┘    └───────────┬───────────────────┘        │
   │          │                        │                            │
   │          ▼                        ▼                            │
   │      ┌──────────────────────────────────┐                      │
   │      │  Streaming merge-join classifier │                      │
   │      │  → (NEW, DIRTY, UNCHANGED, DEL)  │                      │
   │      └─────┬────────────────────────────┘                      │
   │            │                                                    │
   │      ┌─────┴────────┐                                          │
   │      ▼              ▼                                          │
   │ Synthetic shard   Carried-forward                              │
   │ index (NEW+DIRTY) (UNCHANGED rows)                             │
   │      │              │                                          │
   │      │              ├────────────────────────────────┐         │
   │      ▼              │                                ▼         │
   │ S3 upload           │                          Merge with      │
   │      │              │                          per-host        │
   │      ▼              │                          partial         │
   │ ┌───────────────────┴──────────────┐           manifests       │
   │ │  Existing fleet (unchanged):     │           ▼                │
   │ │  - claim protocol v2             │     ┌─────────────┐        │
   │ │  - shard processor               │     │ pass_NNNN.  │        │
   │ │  - mover (with new bucketed pool)│     │ parquet     │        │
   │ │  - per-host partial manifests    │     │ (atomic     │        │
   │ │    (new — per-host sink)         │     │  rename)    │        │
   │ └──────────────┬───────────────────┘     └─────────────┘        │
   │                ▼                                                │
   │     per-host partial manifests                                  │
   │     manifests/host-<id>/pass-NNNN.parquet                       │
   │                                                                  │
   └──────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
              Inside each worker: bucketed async libnfs pool
              (small / medium / large) drives per-file pipelined
              reads and writes per LIBNFS_ASYNC_FORK.md.
```

---

## Pass lifecycle

### Pass 0 (initial bulk copy)

1. nfs-walker scan produces sharded parquet index at
   `s3://run/<id>/index/part-NNNN.parquet` (existing).
2. Manifest `manifest.json` + claim objects uploaded (existing).
3. Pass driver records `passes/pass-0/started_utc` + walker scan id
   in S3.
4. Workers claim shards and process — identical to today's
   single-shot flow, with two additions:
   - Mover routes per-file copies through the **bucketed async
     libnfs pool** based on row.size (small / medium / large
     bucket). The sync pool stays available behind
     `--legacy-single-context` for A/B benching.
   - Each worker emits a **per-host partial manifest** to
     `manifests/pass-0000/host-<id>.parquet` as it completes files.
     One row per completed file (success, skipped, or errored).
5. When all shards have `state: "Completed"` claims (no claimable
   shards remain — same exit condition as today), the pass driver
   triggers the **pass-completion merge**:
   - List all `manifests/pass-0000/host-*.parquet` objects.
   - Stream-merge them by path into a single sorted output.
   - Write to `manifests/pass-0000.parquet.tmp`.
   - On success, atomic S3 PUT-rename to `manifests/pass-0000.parquet`.
     This is the pass-0 commit point.
6. Pass driver records `passes/pass-0/completed_utc` and either
   exits (single-pass run) or starts pass 1.

### Pass N (N ≥ 1) — delta resync

1. Pass driver reads `manifests/pass-NNNN-1.parquet` (sorted by
   path) and the latest walker scan (also sorted by path).
   Predicate-pushdown projection to (path, size, mtime_ns, ctime_ns,
   mode, file_type, file_hash, torn_read) — the full
   `block_hashes` column does *not* exist in this schema; nothing
   else is bulky.
2. Streaming merge-join (single linear pass through both inputs,
   O(1) memory per row) emits per-path classification:
   | Source | Manifest | Tuple match | torn_read prev | Classification |
   |---|---|---|---|---|
   | yes | no | n/a | n/a | NEW |
   | yes | yes | yes | false | UNCHANGED |
   | yes | yes | yes | true | DIRTY (retry torn) |
   | yes | yes | no | n/a | DIRTY |
   | no | yes | n/a | n/a | DELETED |
3. Pass driver writes a **synthetic shard index** containing only
   NEW + DIRTY rows, using the existing walker-output schema
   (`migration-core/src/schema.rs`). One synthetic shard per ~1M
   rows, byte-budgeted as in nfs-walker. Uploaded to
   `s3://run/<id>/index-pass-NNNN/part-*.parquet`.
4. Pass driver writes claim objects for the synthetic shards at
   `s3://run/<id>/shards-pass-NNNN/part-*.parquet.claim`.
5. **The fleet runs unchanged** — workers claim, process, emit
   per-host partial manifests, exit. They don't know it's a
   delta pass. The only differences from pass 0 are:
   - The shard-set under contention is smaller (synthetic, only
     NEW+DIRTY).
   - Workers also receive **UNCHANGED rows** as an "advisory" input
     they need to carry forward; this is *not* delivered via the
     shard index (which only contains rows to act on) but via a
     separate read-only input the pass driver hands to the merge
     step (see #6).
6. On pass completion, the merge step combines:
   - Per-host partial manifests for NEW+DIRTY (processed rows).
   - Carried-forward UNCHANGED rows from `pass-NNNN-1.parquet`
     (path + tuple + mode + file_hash unchanged).
   - DELETED row markers (status=Deleted, no file_hash) from the
     classifier output.
   Streaming merge by path, output sorted, atomic rename to
   `manifests/pass-NNNN.parquet`. Pass commit point.

### Cutover pass

Cutover pass = pass N (N ≥ 1) run with `--cutover`. Differences:

- Writers open destination files with **FILE_SYNC** stability via
  `Flags::wronly_sync()` at `open` / `create` time (the linked
  libnfs sets stability per-fh, not per-write — see
  `LIBNFS_ASYNC_FORK_AUDIT.md`). Every `pwrite` against a cutover-
  opened fh issues a stable write; no UNSTABLE + per-range COMMIT
  batching. Throughput tanks; correctness wins. The multi-pass
  model is the throughput optimization; cutover is the paranoid
  pass.
- After fleet completes but *before* the merge step, the pass
  driver runs **invariant: classifier must have produced zero DIRTY
  rows**. Writes are supposed to be stopped; any DIRTY is drift.
  Default behavior: fail loudly, exit non-zero. Override with
  `--cutover-allow-drift`.
- Optional `--cutover-verify` (default ON): a coordinator-side
  full-tree re-read pass. For every row in the manifest, compute
  `file_hash` from the destination and compare to the manifest's
  `file_hash`. Mismatches fail the cutover. `--cutover-skip-verify`
  disables.
- Writes still use the bucketed async pool; only the per-file open
  flag changes (cutover opens dest fhs with `Flags::wronly_sync()`,
  bulk passes open without it). The bucket configs themselves are
  identical between bulk and cutover.

### Resume on restart

1. On pass-driver startup, list `manifests/` in S3. The highest
   `pass_NNNN.parquet` (final name, not `.tmp`) is the ground
   truth. The pass driver resumes at pass N+1.
2. If `pass_NNNN.parquet.tmp` exists (interrupted merge step),
   delete it. The next pass rebuilds from `pass_NNNN-1.parquet`.
3. If any per-host partial manifest exists under
   `manifests/pass-NNNN/host-*.parquet` and the corresponding
   `pass_NNNN.parquet` is **not** present (final name missing),
   the partials are stale — delete them. The current pass restarts.
4. Mid-pass resume (partial completion of a single pass) is **not**
   supported in v1. A pass either completed or it didn't. The
   existing per-shard claim/heartbeat handles intra-pass worker
   crashes; pass-level restart means re-running the whole pass.

---

## Bucketed async libnfs pool

Per the original external prompt's design, tuned per file-size
bucket. Built on top of `AsyncNfsContext` from `LIBNFS_ASYNC_FORK.md`.

```rust
pub struct BucketConfig {
    pub name: &'static str,
    pub min_size: u64,        // inclusive
    pub max_size: u64,        // inclusive
    pub rsize: u32,
    pub wsize: u32,
    pub read_pipeline_depth: u32,
    pub write_pipeline_depth: u32,
}

pub const BUCKETS: [BucketConfig; 3] = [
    BucketConfig {
        name: "large",     min_size: 1 << 30, max_size: u64::MAX,
        rsize: 4 * MiB, wsize: 4 * MiB,
        read_pipeline_depth: 32, write_pipeline_depth: 32,
    },
    BucketConfig {
        name: "medium",    min_size: 1 << 20, max_size: (1 << 30) - 1,
        rsize: 2 * MiB, wsize: 2 * MiB,
        read_pipeline_depth: 8,  write_pipeline_depth: 8,
    },
    BucketConfig {
        name: "small",     min_size: 0,       max_size: (1 << 20) - 1,
        rsize: 128 * KiB, wsize: 128 * KiB,
        read_pipeline_depth: 2,  write_pipeline_depth: 2,
    },
];
```

Removed from this sketch vs the original external prompt: `nconnect`
and `libnfs_readahead`. The linked libnfs (`/usr/local/lib/libnfs.so.16`)
rejects `nconnect > 1` at mount and does not surface a readahead
tunable on the async API — see `LIBNFS_ASYNC_FORK_AUDIT.md`. Every
bucket therefore runs one TCP connection per context. If a future
libnfs adds `nconnect` support, this field comes back; until then,
parallelism per bucket is "one connection × per-file pipeline depth ×
inflight-file count from `batch.rs::InflightLimiter`".

Rationale (copy this comment block above `BUCKETS` in code):

- **Large** (≥1 GiB): throughput-bound. Hide round-trip latency
  with a deep per-file pipeline (32 concurrent RPCs × 4 MiB =
  128 MiB streaming window over the single TCP connection).
- **Medium** (1 MiB – 1 GiB): balanced. 16 MiB window covers small
  files in one batch and pipelines well on larger ones.
- **Small** (<1 MiB): latency-bound. Per-file pipeline depth of 2
  is defensive (256 KiB file = 2 RPCs total). Parallelism comes
  from many concurrent *files*, not pipelining within one file —
  scale by raising the per-class `InflightLimiter` budget, not by
  fanning out connections (libnfs gives us one per context).

### Implementation surface

```rust
pub struct BucketedAsyncPool {
    small:  AsyncNfsContextPair,
    medium: AsyncNfsContextPair,
    large:  AsyncNfsContextPair,
}

impl BucketedAsyncPool {
    pub fn new(src_url: &str, dst_url: &str) -> Result<Self> { ... }
    pub fn pair_for_size(&self, size: u64) -> (&AsyncNfsContext /*src*/,
                                                &AsyncNfsContext /*dst*/,
                                                &BucketConfig);
}
```

A worker holds **one** `BucketedAsyncPool` (six contexts total:
src+dst × 3 buckets). Workers do not share pools; each worker
process has its own. This is consistent with today's
`migration-worker` model where the worker owns its `LibnfsContextPool`.

### Concurrency-across-files

Today, `batch.rs::InflightLimiter` enforces per-size-class
concurrency budgets (small=256, medium=16, large=4 default). Those
defaults stay; they govern *how many files* are in flight per
size class. The per-file pipeline depth (32/8/2) is layered
**inside** the per-file work. So at peak: large bucket runs
(4 files × 32 in-flight RPCs) = 128 concurrent RPCs over the
large-bucket context's single TCP connection.

The two layers stack cleanly: the existing `JoinSet` of size-class
file fibers is unchanged; what changes is the body of each fiber
goes from "sync `while remaining > 0` loop" to "async pipeline
over `FuturesUnordered`".

### Defaults are overridable via CLI

`--read-pipeline-depth-override <bucket>=<N>`,
`--write-pipeline-depth-override <bucket>=<N>`,
`--bucket-stats` to log observed peak depth at end of run.
`--legacy-single-context` retains the existing sync pool for A/B
benching during the rollout.

---

## Per-file async copy pipeline

Replaces `mover.rs::do_libnfs_copy` (the current sync `while
remaining > 0` loop) for files routed through the bucketed pool.
Sketch:

```rust
async fn pipelined_copy(
    src: &AsyncNfsContext, src_fh: &Fh,
    dst: &AsyncNfsContext, dst_fh: &Fh,
    size: u64, bucket: &BucketConfig,
    fence: &Fence,
) -> Result<FileCopyResult> {
    // Pre-stat for torn-read detection.
    let pre_attrs = src.fstat(src_fh).await?;

    let mut reader = ReadPipeline::new(src, src_fh, size, bucket.rsize, bucket.read_pipeline_depth);
    let mut writer = WritePipeline::new(dst, dst_fh, bucket.write_pipeline_depth);
    let mut hasher = Xxh3Hasher::new();

    // Note: stability (UNSTABLE vs FILE_SYNC) is selected at *open* time
    // via `Flags::wronly_sync()` on the dst fh by the caller — the shipped
    // libnfs surface does not take a per-pwrite stability flag. See
    // LIBNFS_ASYNC_FORK.md closing note.

    while let Some(chunk) = reader.next_chunk().await? {
        hasher.update(&chunk);
        writer.submit(chunk).await?;        // backpressured by depth
    }
    writer.drain().await?;                  // wait for all writes acked
    dst.fsync(dst_fh).await?;               // whole-file NFS COMMIT (per-range
                                            // COMMIT deferred — see
                                            // LIBNFS_ASYNC_FORK_AUDIT.md)

    let file_hash = hasher.finalize();

    // Post-stat for torn-read detection.
    let post_attrs = src.fstat(src_fh).await?;
    let torn = (pre_attrs.size, pre_attrs.mtime, pre_attrs.ctime)
            != (post_attrs.size, post_attrs.mtime, post_attrs.ctime);

    Ok(FileCopyResult { file_hash, torn, size: post_attrs.size, ... })
}
```

Then, **outside** `pipelined_copy`, the existing
`mover.rs::do_libnfs_copy_with_fence` shape applies:

```rust
// R8: check fence immediately before commit point.
fence.check_pre_rename()?;
dst.rename(partial_path, final_path).await?;
```

The rename **stays the commit point**. The whole-file `fsync`
inside `pipelined_copy` issues the NFS COMMIT that durabilizes the
bytes; the rename publishes them. The fence check between
`fsync().await?` and `rename().await` is the R8 invariant —
unchanged from today's model in *placement* but more important in
*timing* because the COMMIT can complete tens of milliseconds
before the rename. See R-rule preservation below for the proof
obligation.

---

## Manifest schema

Per-pass parquet, sorted by path, zstd-3 compressed,
dictionary-encoded path column. Schema (final wording goes in
`migration-core/src/schema.rs`'s new `manifest` module):

| Column | Arrow type | Notes |
|---|---|---|
| `path` | BYTE_ARRAY (no UTF-8 logical type) | Path bytes per `CORRECTNESS_RULES.md` "Path bytes, not strings". |
| `size` | INT64 | |
| `mtime_ns` | INT64 | nanoseconds since epoch; canonical attr key |
| `ctime_ns` | INT64 | nanoseconds since epoch |
| `mode` | UINT32 | POSIX mode bits including file type |
| `uid` | UINT32 | |
| `gid` | UINT32 | |
| `file_type` | UINT8 | regular / dir / symlink / fifo / socket / block / char |
| `symlink_target` | BYTE_ARRAY (nullable) | populated iff S_ISLNK |
| `inode` | UINT64 | hardlink grouping key |
| `nlink` | UINT32 | |
| `xattr_blob` | BYTE_ARRAY (nullable) | reserved; NULL until walker emits xattrs |
| `file_hash` | BINARY(16) (nullable) | xxh3_128 of file contents; nullable because dirs/symlinks/special files have none, and carried-forward UNCHANGED rows preserve the previous pass's hash |
| `status` | UINT8 | `Copied=0`, `Skipped=1`, `Errored=2`, `TornRead=3`, `CarriedForward=4`, `Deleted=5` |
| `torn_read` | BOOL | true iff pre/post attrs diverged this pass |
| `pass_n` | INT32 | the pass that last *acted on* this row; carried-forward rows preserve the pass that originally copied them |
| `error_message` | BYTE_ARRAY (nullable) | populated iff status == Errored |

Notes:

- **No `block_hashes` column.** Whole-file recopy; not needed.
- **`file_hash` is per-file xxh3_128 over file contents**, computed
  inline during the copy via streaming hash of the bytes that pass
  through the read pipeline. xxh3_128 cost is negligible (~10 GB/s
  on modern x86; less than 3% CPU at 213 MB/s baseline).
- **Sort key: path bytes**. Stable lexicographic order of byte
  sequences (not codepoint order). Single-writer task pulls
  completions from a priority queue keyed by path.
- **Row group target: 128 MiB** with `WRITE_BATCH_SIZE` of 2048
  rows. Same conventions nfs-walker uses for its index.

### Per-host partial manifest schema

Same as the canonical schema. Per-host partial manifests are just
the subset of rows that this host processed in this pass. The
merge step at pass completion unions all per-host partials with
the carried-forward UNCHANGED rows from the previous pass.

---

## Streaming merge-join classifier

Single linear pass, O(1) memory per row, no random lookups, no
joins-in-RAM.

```rust
pub fn classify_pass(
    walker_index: impl Stream<Item = WalkerRow>,   // sorted by path
    prev_manifest: impl Stream<Item = ManifestRow>, // sorted by path
) -> impl Stream<Item = Classification> { ... }

pub enum Classification {
    New     { walker_row: WalkerRow },
    Dirty   { walker_row: WalkerRow, reason: DirtyReason },
    Unchanged { manifest_row: ManifestRow },
    Deleted { manifest_row: ManifestRow },
}

pub enum DirtyReason {
    TupleMismatch { from: Attrs, to: Attrs },
    PreviousTornRead,
}
```

Inputs:

- `walker_index` is the *current* walker scan output, sorted by
  path (the walker emits sorted within shards; cross-shard merge
  is needed if there are multiple shards — borrow nfs-walker's
  merge-iter or write a fresh `kway_merge_by_path`).
- `prev_manifest` is `pass_NNNN-1.parquet` streamed via
  `ParquetRecordBatchStream` with **projection**: read only
  (path, size, mtime_ns, ctime_ns, mode, file_hash, torn_read,
  status). Do not read `xattr_blob` (largest column) unless the
  output classification requires it (it doesn't, in v1).

Output:

- A stream of `Classification` enums, in path order.
- Tuple match = (`size`, `mtime_ns`, `ctime_ns`) all equal. Mode
  changes alone do not trigger DIRTY in v1 (mode-only re-stamps
  are handled by the attribute applier at the end of pass-N copy,
  not by re-copying data). Revisit if real workloads have
  chmod-without-mtime-change patterns.

Test cases (must exist as unit tests before the classifier ships):

- New, dirty, unchanged, deleted, all combinations.
- Empty source.
- Empty manifest (= equivalent to "everything is new" — first pass).
- Identical source + manifest (= everything UNCHANGED).
- Massive corpus simulated (1M rows synthetic stream) to assert
  no random access and bounded memory.
- Boundary: path bytes that sort identically up to a prefix
  (`"foo"` vs `"foo\x00"`).
- TornRead carried from previous → DIRTY even if tuple matches.

---

## Torn-read detection

Per-file pre/post stat bracket, inside `pipelined_copy` above.

- `pre_attrs = nfs_fstat64_async(src_fh)` *before* the first
  `pread_async`.
- `post_attrs = nfs_fstat64_async(src_fh)` *after* the last
  `pread_async` completes (and *before* the COMMIT on dest).
- If `(pre.size, pre.mtime, pre.ctime) != (post.size, post.mtime,
  post.ctime)`, mark `torn_read = true` in the per-host partial
  manifest row.

Semantics:

- A torn-read does **not** abort the copy. The destination receives
  whatever bytes were read; the COMMIT and rename happen; the file
  is on dest. But the manifest carries `torn_read = true` so the
  next pass forces a re-copy.
- The next pass's classifier sees `torn_read = true` and emits
  `DirtyReason::PreviousTornRead` regardless of the tuple. The
  cutover pass (writes stopped, no possible new torn reads)
  guarantees convergence.
- The theoretical hole — file modified twice within the mtime
  resolution, same final size, same ctime change pattern — is
  convergence-out by the cutover pass. Document this; it's not a
  defect, it's a converging-system invariant.

---

## R-rule preservation (correctness audit)

This work *must* preserve every load-bearing invariant. For each
rule, the proof obligation:

### R4 (claim transfer)

Unchanged. The claim protocol v2 (`migration-core/src/claim.rs`)
operates on parquet shard objects. The pass driver uploads
synthetic shards for pass N≥1, but the protocol semantics on each
shard are identical to today. No code changes in claim.rs.

### R6 (consecutive HEAD-fail / retry budget)

Unchanged. Heartbeat (`migration-worker/src/heartbeat.rs`) is at
the worker level and does not know which pass is running. The
heartbeat refresh fails on `412` exactly as before; budget exhaustion
trips the fence exactly as before.

### R7 (clock-drift guard)

Unchanged. Wall-vs-monotonic check is at the worker level. The
multi-pass model has no new clock dependencies.

### R8 (pre-commit fence check)

**Most subtle.** The rule today: `mover.rs:530-538` calls
`fence.check_pre_rename()` immediately before `nfs_rename(partial,
final)`. Both calls are synchronous and back-to-back; the time
window between fence check and rename issue is bounded by syscall
overhead (microseconds).

In the new async model, the rename happens after the COMMIT
completes. The COMMIT (issued via `AsyncNfsContext::fsync`, which
is whole-file in the linked libnfs — see `LIBNFS_ASYNC_FORK_AUDIT.md`)
can take milliseconds to tens of milliseconds. The fence check
still happens **immediately before** the rename syscall is issued
(i.e., between `fsync().await?` and `rename().await`). The window
between fence check and the rename RPC hitting the wire remains
bounded by the time to send one RPC.

Proof obligation:

- The fence check is placed exactly between `fsync().await?` and
  `rename(...).await`. Not before fsync — *after*. This is the
  critical placement.
- The M5 self-fence harness (`scripts/m5-self-fence-test.sh`,
  `docs/work-items/M5_SELF_FENCE.md`) is re-run against the new
  mover *unchanged*. If any assertion fails, R8 is violated and
  the work does not ship.
- A new unit test in the mover crate stubs the fence, simulates
  fsync returning ack, then trips the fence between fsync-ack
  and rename-issue, and asserts the rename is not issued.

### Source/dest overlap

Unchanged. Startup guard (`migration-core/src/overlap.rs`) +
per-file self-target check (`mover.rs:325-338`) both stay; they do
not depend on the pass model.

### Atomic rename commit point

**The rename remains the only commit point.** The per-pass parquet
manifest is *not* a commit point — it's an observation artifact.
A file is durably copied (and visible to a reader on dest) the
moment `nfs_rename(partial, final)` succeeds; the manifest row is
written after that. If the worker crashes between rename and
manifest, the next pass's classifier sees the file on dest and the
walker sees the source unchanged → UNCHANGED → no re-copy. (This
requires that the dest tree be re-walked by nfs-walker, *not* that
we trust the previous manifest — see "Open question: does each
pass need a fresh source walk?" below.)

The pass-NNNN.parquet *file* in S3 is committed by its atomic
rename from `.tmp` to final, mirroring the per-file commit pattern.

---

## File / module locations

Expected layout:

```
crates/migration-mover/src/
├── (existing files unchanged at top level)
├── libnfs/asyncio/            # from LIBNFS_ASYNC_FORK.md
├── bucketed_pool.rs           # NEW — BucketedAsyncPool + BUCKETS
├── pipelined_copy.rs          # NEW — per-file async pipeline
└── manifest/
    ├── mod.rs                 # NEW — schema + reader + writer
    ├── classifier.rs          # NEW — streaming merge-join
    └── partial.rs             # NEW — per-host partial manifest sink

crates/migration-core/src/
├── (existing files unchanged)
└── pass.rs                    # NEW — pass-level S3 layout helpers

crates/migration-worker/src/
├── (existing files unchanged)
└── manifest_sink.rs           # NEW — per-host partial manifest writer
                               # hook in shard_processor

crates/vamoose-cli/src/cmd/
├── (existing subcommands)
└── pass.rs                    # NEW — vamoose pass [plan | run | merge | cutover]

docs/
├── (existing)
└── PASS_DRIVER_SEMANTICS.md   # NEW — operator-facing doc:
                               # pass 0, pass N, cutover, resume
```

---

## CLI surface

New top-level subcommand `vamoose pass`:

```
vamoose pass plan      \                # offline: read prev manifest +
   --prev-pass <N>     \                # walker, classify, upload
   --walker-index <s3://...>            # synthetic shards
   # outputs:
   #   - count of NEW/DIRTY/UNCHANGED/DELETED
   #   - synthetic shard index at s3://run/<id>/index-pass-<N>/
   #   - claim objects at s3://run/<id>/shards-pass-<N>/

vamoose pass run      \                 # the coordinator role:
   --pass <N>                           # waits for fleet to drain
                                        # the synthetic shards, then
                                        # runs the merge step

vamoose pass cutover  \                 # same as 'pass run' but with:
   --pass <N> [--allow-drift]           # - FILE_SYNC writes
              [--skip-verify]           # - dirty-must-be-zero invariant
                                        # - full-tree re-verify

vamoose pass merge    \                 # standalone: run only the merge
   --pass <N>                           # step (recover from interrupted
                                        # merge); reads per-host partials
                                        # + UNCHANGED carry-forward,
                                        # emits pass_<N>.parquet
```

Tuning flags (apply to fleet workers when present):

```
--read-pipeline-depth-override <bucket>=<N>
--write-pipeline-depth-override <bucket>=<N>
--reader-multiplier <N>        # multiplier on the bucket's
                               # configured read_pipeline_depth for the
                               # per-bucket reader worker count
--writer-multiplier <N>        # likewise for write_pipeline_depth
--channel-capacity <N>
--legacy-single-context        # A/B benching gate; keeps existing sync pool
```

Observability:

```
--bucket-stats     # per-bucket configured + observed peak pipeline depth,
                   # bytes read/written, files processed
--pass-stats       # per-pass wall time, total bytes, file classifications,
                   # commit count, error count, effective throughput
```

Existing flags on `vamoose worker run` (the actual data-plane invocation)
stay; the pass driver invokes workers exactly as today, just with
synthetic shard indices.

---

## Phases (implementation plan)

Sized for sane, individually-verifiable PRs. Each is its own
verification gate per `CORRECTNESS_RULES.md` "Verification gates".

### Phase 0 — prereq

`LIBNFS_ASYNC_FORK.md` lands and is verified (FFI smoke + service-task
integration test pass).

### Phase 1 — Bucketed async pool

- `bucketed_pool.rs`: `BucketedAsyncPool` + `BUCKETS` config.
  `BucketConfig` carries only the fields the shipped FFI actually
  honors: `rsize`, `wsize`, `read_pipeline_depth`,
  `write_pipeline_depth` (plus `name` / `min_size` / `max_size` for
  selection). No `nconnect`, no `libnfs_readahead`. The pool
  constructor builds three `AsyncNfsContextPair`s (src+dst per
  bucket) via `AsyncNfsContext::mount` with
  `MountOpts { nconnect: 1, version: 3, rsize, wsize, .. }`.
- Unit tests: bucket selection boundary cases at 1 MiB and 1 GiB
  exact and ±1 byte; size 0 boundary; `u64::MAX` boundary.
- No mover integration yet — just the pool.
- Verification: pool builds, mounts against real VAST, all three
  contexts come up with their tuned mount opts. Smoke against
  small/medium/large files. The async FFI smoke test gates from
  `LIBNFS_ASYNC_FORK.md` Gate D are *not* re-run here — Phase 1 is
  composition over an already-verified surface; we don't touch
  `asyncio/` (see `CORRECTNESS_RULES.md` "Pre-merge runbook: async
  libnfs FFI changes" — that gate fires only on changes inside
  `asyncio/`).

### Phase 2 — Per-file async copy pipeline

- `pipelined_copy.rs`: implements `pipelined_copy`. UNSTABLE writes
  (dest fh opened *without* `Flags::wronly_sync()`) + one whole-
  file `fsync` per file. Inline xxh3_128. Pre/post stat brackets.
  Per-range COMMIT is **not** wired here — when the cutover pass
  needs it, it lives behind a separate code path (see Phase 6 and
  `LIBNFS_ASYNC_FORK_AUDIT.md` follow-up #2).
- Integration in `mover.rs::do_libnfs_copy`: behind
  `--use-bucketed-pool` flag (default off), route to
  `pipelined_copy`. Sync path stays as fallback.
- R8 fence check placement explicit in the new code path
  (`fsync().await?` → `fence.check_pre_rename()?` →
  `rename().await`).
- Unit tests with mocked async context: pipeline depth saturation,
  drain semantics, torn-read pre/post mismatch flagged.
- Integration test against real VAST: M2/M3 cookbook re-run with
  `--use-bucketed-pool`. CONTENT MATCHES must hold.
- M5 self-fence harness re-run with `--use-bucketed-pool`. All
  assertions in `M5_SELF_FENCE.md` must pass.

### Phase 3 — Per-host partial manifest writer

- `manifest/mod.rs`: manifest schema (Arrow). Writer that takes
  the existing `shard_processor::FileResult` records and emits
  parquet rows.
- `manifest_sink.rs` in worker: hook into shard_processor to feed
  the writer with every completed-file event.
- Per-host partial manifests written under
  `manifests/pass-NNNN/host-<id>.parquet.tmp` and atomically
  renamed on per-shard or per-batch boundaries (operational
  trade-off: per-batch is more S3 traffic but lower restart cost).
- Unit tests: schema round-trip, sort order preserved despite
  parallel JoinSet completion order, atomic-rename behavior under
  injected error.

### Phase 4 — Classifier + synthetic shard index generator

- `manifest/classifier.rs`: streaming merge-join.
- Synthetic shard writer: takes the NEW + DIRTY stream and
  produces walker-shape parquet shards in `index-pass-N/`.
- `cmd/pass.rs`: `pass plan` subcommand wires classifier + shard
  writer + claim object creation.
- Unit tests: classification matrix (every combination), sorted
  output, predicate-pushdown projection on the manifest reader.

### Phase 5 — Pass driver / coordinator

- `cmd/pass.rs`: `pass run`, `pass merge` subcommands.
- `pass.rs` in migration-core: S3 layout helpers for
  `passes/pass-N/started_utc`, completion markers, manifest path.
- Pass driver polls the fleet via existing claim/progress
  surfaces (no new heartbeat protocol) until all synthetic shards
  are Completed, then triggers merge.
- Merge step: stream-merge per-host partials + UNCHANGED carry-
  forward + DELETED markers → `pass_NNNN.parquet.tmp` → atomic
  rename.
- Resume: highest pass_NNNN.parquet is ground truth; .tmp gets
  cleaned; partial-pass restart re-runs the whole pass.
- Integration test in lab: write to source between passes, observe
  DIRTY classification, observe re-copy.

### Phase 6 — Cutover mode

- `cmd/pass.rs cutover` subcommand: invokes `pass run` with
  `--cutover` flag propagated to workers (which then route to
  the FILE_SYNC write path in `pipelined_copy`).
- Pre-merge invariant check: zero DIRTY allowed (override
  `--allow-drift`).
- Full-tree re-verify pass: coordinator reads every manifest row,
  re-reads the dest file, recomputes xxh3_128, asserts match.
  Parallel across the bucketed pool. Override `--skip-verify`.
- Integration test: multi-pass scenario ending with cutover.

### Phase 7 — Observability

- `--bucket-stats` per the table in the original prompt: configured
  vs observed peak pipeline depth, bytes, files.
- `--pass-stats` per-pass.
- `tracing` structured fields throughout.

### Phase 8 — Documentation

- `docs/PASS_DRIVER_SEMANTICS.md`: operator-facing doc — pass 0,
  pass N lifecycle, cutover procedure, resume behavior, manifest
  query examples (using DataFusion against `pass_NNNN.parquet`).
- README section on multi-pass + cutover.
- Update `DESIGN.md` if needed (or replace with v3 referencing this
  work).
- Update `CORRECTNESS_RULES.md` with any new invariants surfaced
  during implementation (likely: "pass-NNNN.parquet rename is the
  pass commit point", "fence check between COMMIT and rename in
  async mover").

---

## Open questions

1. **Does each pass need a fresh source walk?** If yes, the user
   needs to re-run nfs-walker before each pass-N invocation, and
   the pass driver uses the *latest* walker output as the "current
   state" input to the classifier. If no, the walker output from
   pass 0 is reused — but then the classifier can't detect NEW
   files that appeared on source after pass 0. **Tentative answer:
   yes, each pass requires a fresh walker scan.** The "active
   writes" workload by definition creates new files; the classifier
   must see them. This means pass N actually has two prereqs: the
   previous manifest AND a fresh walker scan. Document this in the
   operator-facing doc. The walker is already incremental-friendly
   (the M5 work touched walker config).

2. **xxh3_128 cost at high bucket-pool throughput.** Inline hashing
   should be fine at single-worker 213 MB/s baseline. At a fleet of
   100 workers each at 10 GbE, aggregate hash CPU is 100 workers ×
   1.2 GB/s = 120 GB/s; xxh3 is ~10 GB/s per core, so each worker
   spends ~12% of one core hashing. Acceptable. But if a worker is
   CPU-saturated, the hash adds tail latency to the per-file
   pipeline. Mitigation: hash in a separate task fed by the read
   stream (one extra channel). Probably unnecessary at v1 speeds;
   measure first.

3. **Dirty classification on mode-only changes.** Today: tuple
   match = unchanged, so a `chmod` without a `touch` is missed.
   The attribute applier in the existing mover re-stamps mode
   regardless (because pass-0 attr application runs unconditionally
   for processed files); but UNCHANGED rows in pass N never go
   through the worker, so a mode-only drift never gets restamped.
   Decision needed: do we widen the dirty heuristic to (size, mtime,
   ctime, mode, uid, gid), or accept that mode/owner drift requires
   an explicit `--full-attr-pass`? **Tentative: accept the
   limitation in v1, document it, add `--full-attr-pass` later.**

4. **DELETED handling at the destination.** v1 records DELETED in
   the manifest and leaves the dest file in place. Operationally
   this means a deleted source file persists on dest until manual
   cleanup. Flag-gated `--prune-deleted` for the dest unlink is
   straightforward but introduces another commit-point class
   (the unlink) that needs its own fence check. Out of scope for
   v1; revisit.

5. **Manifest carry-forward bloat over many passes.** Each pass
   rewrites every row. For 5 B files at ~100 bytes/row (compressed),
   that's 500 GB of manifest per pass; 10 passes = 5 TB. The
   dictionary-encoded `path` column + zstd compression should keep
   the on-the-wire size dramatically lower (paths share prefixes,
   the bulk of `file_hash`/`mode`/`uid`/`gid` are stable), but
   it's a real cost. Mitigation: pass N's manifest *could*
   reference pass M (M<N) for unchanged rows rather than carrying
   them, but that breaks the "manifest is a complete snapshot"
   property and the resume semantics. **Tentative: pay the
   carry-forward cost in v1; revisit if it becomes operational
   friction.**

6. **Failure recovery during the merge step.** If the pass-completion
   merge crashes after writing 50% of `pass_NNNN.parquet.tmp`, on
   restart the resume logic deletes the .tmp and re-runs the merge.
   But the per-host partial manifests are already complete — the
   merge re-reads them from S3, which is fine. Cost: O(time-to-merge)
   redo on every coordinator restart. Acceptable.

7. **Backpressure when source is faster than dest.** Existing
   `migration-worker/src/backpressure.rs` handles fail-rate-based
   backoff. The async pipeline doesn't change that surface; the
   write pipeline's `submit().await` naturally backpressures the
   read pipeline (full pipeline = await). Stress test under
   real-VAST-asymmetric-speed to confirm.

---

## Cross-references

- `LIBNFS_ASYNC_FORK.md` — required prerequisite, closed 2026-05-18.
  Its closing note is the source of truth for the async-FFI shape;
  the deltas (no `nconnect>1`, no readahead, `fsync` is whole-file,
  FILE_SYNC stability is open-time) are already threaded into the
  sections above.
- `LIBNFS_ASYNC_FORK_AUDIT.md` — symbol-by-symbol audit. Item in the
  "follow-up" section covers per-range NFS COMMIT support, which
  this work item will revisit only if the cutover pass actually
  needs it.
- `CORRECTNESS_RULES.md` — every invariant that must continue to
  hold.
- `M5_SELF_FENCE.md` — the verification harness that gates Phase 2.
- `M3_NOTES.md` — original concurrency model; this work extends
  rather than replaces.
- `DESIGN.md` — the v2 architecture document; needs a v3 follow-up
  once this work lands, or this doc + `LIBNFS_ASYNC_FORK.md`
  collectively supersede the relevant sections.
- `docs/HANDOFF.md` — current project state at the moment this work
  item was authored; useful baseline for "what changed".
