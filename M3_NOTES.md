# M3 — Notes from the milestone

What M3 actually delivered, what it intentionally deferred, and why.

> Historical note (post-2026-05-15): the nfs-walker has since been
> rewritten to drop RocksDB and SQLite entirely; the only output
> backend is sharded Parquet written direct. References below to a
> walker RocksDB intermediate or to `nfs-walker convert` describe the
> data flow as it existed during M3 sign-off and are preserved for
> historical accuracy. The "Stale binary caveat" sub-section is
> obsolete: walker's parquet writer is now the canonical path, not
> an experimental [[bin]] orphan.

## What M3 ships

- **Multi-context libnfs pool** (`MultiPool`). Pre-mounts
  `cfg.mover.nfs_connections` (src, dst) pairs at startup. Concurrent
  acquires get distinct contexts. `SimplePool` becomes a thin N=1
  specialization on top of `MultiPool` for tests.
- **Concurrent shard dispatch** in `shard_processor::run_batch`. Per
  batch, rows are partitioned into hardlink groups (sequential within
  group) and singletons (parallel). A `tokio::JoinSet` runs them
  concurrently, throttled per-row by an `InflightLimiter` with three
  `tokio::Semaphore`s (small / medium / large) sized from
  `[batch].inflight_*`.
- **Blocking-pool offload.** The mover's libnfs work happens inside
  `tokio::task::spawn_blocking`. `pool.acquire().await` runs on the
  async runtime; the libnfs sync calls run on the blocking pool so
  the runtime's worker threads stay free for other tasks.
- **Hardlink fidelity under concurrency.** Per-group sequential
  dispatch keeps hardlink groups consistent: the leader copies, then
  members `nfs_link` against the leader's final path. No shared
  mutable hardlink map.
- **Per-host failures sink** mirroring downgrades. `FailureSink`
  buffers `FailureRecord`s; the orchestrator drains + S3-PUTs to
  `failures/host-<id>.jsonl` after every shard.
- **Throughput counter + heartbeat publishing.** `ThroughputCounter`
  is a single atomic byte counter sampled by the heartbeat task.
  Rolling 60s MB/s lands in `progress/host-<id>.json` on every tick.
- **Backpressure gate.** After each shard, `Backpressure` evaluates
  the shard's failure rate vs `[backpressure].failure_pct_threshold`
  and the latest throughput vs `throughput_floor_mb_s`. If degraded,
  the orchestrator sleeps before the next claim and publishes
  `degraded:<reason>` to progress.
- **Config plumbing.** `[mover].nfs_connections` drives pool size.
  `[batch].inflight_*` drives the limiter. `[batch].bytes_budget` is
  parsed from a TOML size string ("8 GiB", "4 MiB", …) into a real
  byte count. `[backpressure].*` plumbed end-to-end.

## Directory attribute application

Walker emits dir rows in the canonical parquet with mode/owner/mtime
populated, but pre-fix the strategy selector returned `Strategy::Skip`
for them and the mover only mkdir'd dirs on demand with default
mode/owner. Now:

- `Strategy::DirAttrs` covers `FileTypeTag::Dir` rows.
- `Mover::do_dir_attrs` ensures the dir exists (`mkdir_p` on the dir
  path itself, for the empty-source-dir case) and then runs the same
  `apply_attrs` chain as files (chmod → chown → utimes).
- `shard_processor::run_batch` runs dir rows in **Phase 2**, after
  all non-dir rows in the same batch have committed, **deepest-first**
  and **sequential**. File commits inside a dir bump the dir's mtime,
  so dirs must come last; `mkdir` of an empty child bumps the
  parent's mtime, so deepest-first ensures the parent's setattr is
  the last write to its mtime.

**Cross-shard caveat (known v1 limitation).** If a dir row lands in
shard A and a child file row lands in a later shard B, B's commit
will restamp A's dir mtime *after* A's setattr ran. A real fix
needs a post-run dir-attr pass at the orchestrator level. In
practice walker partitioning keeps dir + children co-located so the
case is rare; document and move on.

## What M3 explicitly does NOT ship

These were in the original M3 wishlist but didn't land in v1, with
honest reasons:

- **True io_uring integration with libnfs.** libnfs is its own
  userspace TCP/UDP transport. The kernel-side wins from io_uring
  (registered fixed buffers, polled I/O on the NFS socket, zero-copy
  splice) only matter if you can register the libnfs socket with the
  ring and route reads/writes through it. That requires either:
  - a libnfs rewrite (out of scope), or
  - moving to a kernel NFS mount + uring (already documented as the
    `kernel_cfr` escape hatch).

  The cheaper "use io_uring for the local tmpfs ops" angle nets
  almost nothing — the parquet shard reads are page-cache hits and
  the attribute syscalls are negligible against the network round-trip.

  **Decision:** deliver concurrency via N libnfs contexts driven by
  tokio's blocking pool. Document and move on.

- **Pre-registered fixed buffer pool** (`FixedBufferPool::acquire`).
  Same reason — without io_uring integration there's nothing to
  register the buffers *with*. The per-task `vec![0u8; 1 MiB]` is a
  ~µs allocation cost that disappears under the libnfs network round
  trip. Keep as `_buffers: Arc<FixedBufferPool>` placeholder so M3.5
  can wire it in without changing the mover surface.

  **Follow-up (2026-08-02):** after confirming that no executable path ever
  consumed the placeholder, the mover cleanup removed `_buffers`, the fake
  fixed-buffer API, and the unused `io-uring` dependency. The paragraph above
  records the original M3 decision; a future io_uring effort must begin with a
  new measured design rather than rely on dormant scaffolding.

- **Striped reads for large files.** Within a single libnfs context,
  one file copy is one synchronous loop. To pipeline N reads against
  one file you need libnfs's *async* API (`nfs_pread_async` +
  `nfs_get_fd` + `nfs_service`) and a custom event loop driving it.
  Worthwhile when bandwidth-bound on multi-GB files, but at this
  point in the design we'd rather measure first. M3.5 candidate.

- **`SEEK_HOLE` / `SEEK_DATA` sparse-aware copy.** Per
  `SCHEMA_CONTRACT.md` "Sparse files", M3 was to introduce
  `nfs_lseek`-driven sparse copy. The libnfs build at `/usr/lib/...`
  didn't have a usable sparse-seek surface in the version we link
  against (see `M2_NOTES.md` for the exact version). Operators
  migrating sparse-heavy trees still see destination capacity
  inflation; the gap is documented in the schema contract and is
  M3.5+.

## Concurrency notes for future readers

- **Per-context ordering.** libnfs contexts are not thread-safe; the
  `&mut NfsContext` in every op enforces single-caller use at the
  type level. The pool hands out exclusive ownership of a pair until
  the guard drops.
- **Blocking offload.** Every `move_one`/`move_hardlink` call wraps
  its sync body in `spawn_blocking`. The default tokio blocking pool
  caps at 512 threads; with `nfs_connections=16` and `inflight_*`
  defaults adding up to ~276, we stay well under.
- **Hardlink ordering.** The per-group sequential runner is the
  invariant — never parallelize within a hardlink group, because the
  follow-on rows depend on the leader's `RENAME` having finished.
  See `shard_processor::run_group`.
- **Backpressure cadence.** The gate evaluates *after* each shard,
  not per-row. Per-row evaluation (sliding window over millions of
  rows/sec) is overkill for shard-completion-time decisions and the
  M3 implementation is intentionally per-shard.

## What M3.5 should pick up

- libnfs async API exploration → per-file pipelining → fixed buffer
  registration. Bench on large-file workloads to decide whether the
  complexity pays.
- Sparse-aware copy if the libnfs version we ship with grows the
  surface (or we vendor a patched build).
- True progress aggregator UI (`mig-aggr watch`) — backend already
  emits everything it needs; the TUI is just plumbing.

## Verification

M3 has no automated end-to-end perf bench (same reasoning as M2
manual verification). The expected artifact from a real run:

- Wall-clock improvement vs M2 single-row dispatch on the same tree,
  scaling roughly linearly with `nfs_connections` until source NFS
  saturates.
- `progress/host-<id>.json` shows non-zero `throughput_mb_s_1m`.
- `degraded:throughput_low` or `degraded:failure_rate_high` appears
  in `progress.status` if the dest can't keep up — confirms the gate
  trips.

Record results below, same shape as `M2_NOTES.md`:

- Date:
- Operator:
- Tree shape:
- Pool size used:
- Wall-clock vs M2 (same tree):
- Peak throughput_mb_s_1m observed:
- Whether the gate ever tripped:
## Verification: end-to-end metadata fidelity (2026-05-04)

First end-to-end run that exercises the full chain — walker → shim →
S3 manifest → worker → destination NFS write — with a probe file
whose mtime and atime have known sub-second precision, and confirms
that the destination preserves the values as far as the schema
allows.

### Setup

Probe file: `/mnt/vamoose-source/src-test/m2-verify/tiny.txt`,
touched to:

- `mtime = 2024-01-15 12:34:56.789123456 UTC`
- `atime = 2024-06-20 18:30:45.123456789 UTC`

Source filesystem (Linux NFS client → VAST NFSv3 export) preserves
full nanosecond precision under `stat`, confirmed before the run.

### Chain of preservation

| Stage                                 | mtime value                  | Subsec precision     |
| ------------------------------------- | ---------------------------- | -------------------- |
| Source kernel `stat`                  | `12:34:56.789123456`         | ns                   |
| libnfs `nfs_stat64` / READDIRPLUS     | `nfs_mtime_nsec=789123456`   | ns                   |
| Walker RocksDB (`mtime_us`)           | `1705322096789123`           | µs (loses last 3 ns) |
| Walker parquet (`mtime_us`)           | `1705322096789123`           | µs                   |
| Vamoose canonical (`sec`, `nsec`)     | `(1705322096, 789123000)`    | µs (×1000 → ns)      |
| Mover `nfs_utimes` to dest            | applied as ns to libnfs      | ns sent on wire      |
| Destination kernel `stat`             | `12:34:56.789123000`         | ns (trailing zeros)  |

The 3-digit precision loss is permanent and lives in walker's
`mtime_us` column. Anything finer than microseconds cannot survive
the round-trip until walker's parquet schema gains explicit
`mtime_sec` + `mtime_nsec` columns. Documented as a future schema
migration; not blocking M3 sign-off.

### Result

Destination `stat` after worker run, kernel cache dropped:
Access: 2024-06-20 18:30:45.123456000 +0000
Modify: 2024-01-15 12:34:56.789123000 +0000

Both timestamps match the canonical-schema values exactly. This is
the first time vamoose's "metadata fidelity is preserved end-to-end"
claim has been demonstrated rather than asserted.

### Stale binary caveat

The walker fix that made this verification possible was a build-system
bug, not a source bug. Walker's `nfs-walker-parquet-parallel` binary
on disk had been stale since April 27 and was applying a legacy
seconds-to-microseconds multiplier to already-microsecond data,
saturating to `i64::MAX`. SQLite output via `nfs-walker convert` was
correct; parquet output via the orphan binary was wrong. Resolution:
walker repo commit 48a6453 re-introduces the binary as a `[[bin]]`
target so cargo rebuilds it on every change.

See walker repo `docs/WALKER_SUBSEC_INVESTIGATION.md` for the full
bisection. This is the same class of bug as the libnfs FFI signature
mismatch in M2 — a binary on disk diverged from the source it should
have been built from. Both were caught only by end-to-end verification.

### Known issue: clean-shutdown hang

RESOLVED in 8f768f8 (2026-05-04) via fence cancellation token. Section preserved for history.

After "all shards terminal; worker exiting" the worker logs
"fence tripped; self-fencing worker" but does not actually exit;
SIGINT is required to return to the shell. Has been reproduced on
every successful run including the FFI-fixed verification on the
baseline commit.

Tracked: `docs/work-items/WORKER_SHUTDOWN_HANG.md`. Not blocking
verification correctness — the work *is* done by the time the hang
starts; it's a teardown defect.
