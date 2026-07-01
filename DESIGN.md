# Distributed NFS File Migration System — Design (v2)

> **Freshness note (2026-07-01).** This doc predates several shipped
> changes; where it disagrees with the following, the following win:
> - Claim protocol: the claim/heartbeat/reclaim mechanics described
>   below are the *v1* design (`PUT If-Match` heartbeats). The shipped
>   protocol is v2 delete-then-create — see `docs/CLAIM_PROTOCOL.md`.
> - Workspace: the crate list below predates `migration-coord`,
>   `migration-tui`, `mig-walker-rewrite`, and `vamoose-cli` (the
>   operator plane) — see `README.md`.
> - M4 (NFSv4.2 server-side COPY) is cancelled; `strategy::pick`
>   never selects it. M2/M3/M5 are verified complete.
> - io_uring is still deferred (M3.5); the data plane is async libnfs.
> - `mig-aggr` subcommands (incl. `clean-partials`) are stubs.

## Goal

Migrate large numbers of files between POSIX/NFS shares at **wire rate**,
using a fleet of worker hosts that coordinate through a parquet-based file
index served from VAST S3. Workers join and leave dynamically; no central
coordinator.

Wire rate means: the bottleneck is the source NFS server's request rate or
the destination NFS server's ingest rate, **not** anything the mover does.
Every design choice below is in service of that.

---

## Changes from v1

This doc replaces v1. Major changes, all in service of throughput and
correctness:

- **Shard-level claiming via S3 conditional PUT** (was: row-range claims
  with optimistic conflict resolution). Eliminates the race-resolution code
  path entirely. Drops claim traffic by ~1000×.
- **Workers download their assigned parquet shard once** to local tmpfs and
  mmap it; no per-batch S3 parquet reads on the data path.
- **Byte-budgeted micro-batches inside a shard** (was: 1000-row fixed
  batches). Wire-rate-correct for both 1KB and 1GB files.
- **Self-fencing on heartbeat failure** to prevent dual-writer corruption
  when a worker is partitioned but still copying.
- **Tighter heartbeat/lease ratio**: 30s heartbeat / 3min lease (was 30s /
  15min). Faster recovery from dead workers at wire rate.
- **Custom io_uring data mover** built on the **same libnfs user-space
  driver used by `nfs-walker`** for both read and write paths. Bypasses the
  kernel NFS client. Preserves all POSIX attributes recorded in the index.
- **NFSv4.2 server-side COPY** as a fast path when source and dest share a
  server.
- **Path/attribute encoding fixed**: paths are byte arrays, materialized
  `row_id` column in parquet, JSON records use base64 for any non-UTF-8.
- **Per-file failure log** separate from claim records.
- **xattr support deferred**: schema reserves `xattr_blob`, mover apply
  path is wired up but inactive (NULL column) until `nfs-walker` adds
  xattr capture. See Future work.

---

## High-level architecture

```
   ┌─────────────────────────────────┐
   │  nfs-walker scan                │   (pre-existing, libnfs-based)
   │   → sharded Parquet, direct     │   scans/<scan_id>/part-rNN-SSSSS.parquet
   └─────────────┬───────────────────┘
                 │  upload (one-shot, immutable)
                 ▼
   ┌──────────────────────────────────────────────┐
   │  VAST S3 bucket: migration-run-<id>/         │
   │  ├── manifest.json                           │
   │  ├── index/                                  │
   │  │   ├── part-0000.parquet                   │
   │  │   ├── part-0001.parquet                   │
   │  │   └── ...                                 │
   │  ├── shards/                                 │
   │  │   ├── part-0000.parquet.claim   (atomic)  │
   │  │   └── ...                                 │
   │  ├── progress/                               │
   │  │   └── host-<id>.json                      │
   │  ├── batches/                                │
   │  │   └── host-<id>.jsonl       (audit trail) │
   │  └── failures/                               │
   │      └── host-<id>.jsonl       (per-file)    │
   └──────────────────────────────────────────────┘
             ▲                    ▲
             │                    │
   ┌─────────┴────────┐  ┌────────┴─────────┐
   │ Worker host A    │  │ Worker host B …N │   1–100 hosts
   │ (Rust)           │  │ (Rust)           │   add/remove anytime
   │  - claim shard   │  │                  │
   │  - mmap parquet  │  │                  │
   │  - io_uring +    │  │                  │
   │    libnfs mover  │  │                  │
   └────────┬─────────┘  └────────┬─────────┘
            │                     │
            ▼                     ▼
       ┌─────────────────────────────────┐
       │ Source NFS  (libnfs READ)       │
       │ Dest NFS    (libnfs WRITE)      │
       │ — or NFSv4.2 server-side COPY — │
       └─────────────────────────────────┘
```

---

## Components

### 1. Index (input, immutable)

Pre-built by `nfs-walker` (existing, libnfs-based), writing sharded
Parquet directly. Output layout is
`scans/<scan_id>/part-rNN-SSSSS.parquet` + a `metadata.json`. Sharded
into ~hundreds of parquet files, ~GB each, ~5–6 B rows total.

**Required schema** (columns the mover relies on):

| Column | Type | Notes |
|---|---|---|
| `row_id` | UINT64 | **Materialized at write time**. `(shard_idx << 40) \| row_in_shard`. Never derived at read time — predicate pushdown can reorder rows. |
| `path` | BYTE_ARRAY | Raw bytes, no UTF-8 logical type. POSIX paths can contain arbitrary non-UTF-8 bytes; storing as bytes preserves them losslessly. |
| `size` | UINT64 | Used for byte-budgeted batching. |
| `mtime_sec` / `mtime_nsec` | INT64 / INT32 | For `utimensat` after copy. |
| `atime_sec` / `atime_nsec` | INT64 / INT32 | Optional preservation. |
| `mode` | UINT32 | POSIX mode bits including type. |
| `uid` / `gid` | UINT32 | For `chown` (worker must run as root or have CAP_CHOWN). |
| `nlink` | UINT32 | Hardlink detection (see Hardlinks below). |
| `inode` | UINT64 | Hardlink grouping key. |
| `xattr_blob` | BYTE_ARRAY (nullable) | Serialized name→value xattrs, format documented in `migration-core`. **Walker support deferred** (see Future work); column is reserved in the schema and emitted as NULL until the walker side lands. |
| `symlink_target` | BYTE_ARRAY (nullable) | Set iff `S_ISLNK(mode)`; raw bytes. |
| `file_type` | UINT8 | Regular / dir / symlink / fifo / socket / block / char. Filters out non-data entries quickly. |

The walker may not currently emit all of these; v1 of the mover can ignore
columns the walker doesn't produce, but the design assumes they're added
over time.

### 2. Manifest

Single immutable `manifest.json`, written once at upload:

```json
{
  "run_id": "2026-05-02T10:00:00Z-jobname",
  "created_utc": "2026-05-02T10:00:00Z",
  "shards": [
    { "key": "index/part-0000.parquet", "rows": 12345678, "bytes": 2147483648, "etag": "..." },
    { "key": "index/part-0001.parquet", "rows": 12345678, "bytes": 2147483648, "etag": "..." }
  ],
  "total_rows": 5500000000,
  "source": { "kind": "nfs", "url": "nfs://src-server/export", "root": "/" },
  "dest":   { "kind": "nfs", "url": "nfs://dst-server/export", "root": "/" },
  "options": {
    "preserve_owner": true,
    "preserve_mode":  true,
    "preserve_times": true,
    "preserve_xattr": true,
    "server_side_copy": "auto"
  }
}
```

ETags let workers detect a swapped manifest. Source/dest URLs are explicit;
the mover uses libnfs against these directly.

### 3. Claims (shard-level, atomic)

One claim object per shard:

```
shards/part-0042.parquet.claim
```

Contents:

```json
{ "host": "worker-07", "claimed_utc": "2026-05-02T10:14:22Z", "epoch": 3 }
```

**Claiming uses S3 conditional PUT (`If-None-Match: *`).** VAST S3 supports
both `If-None-Match: *` and `If-Match: <etag>` per RFC 9110 (confirmed). The
request either succeeds (worker owns the shard) or returns `412 Precondition
Failed` (someone else owns it). No race-resolution code in the worker. The
S3 server is the arbiter.

**Heartbeat refresh**: every 30s the owner overwrites its claim with
`If-Match: <previous-etag>`, bumping `epoch` and updating `claimed_utc`.

**Reclaim after death**: any worker reading the claim sees a stale
`claimed_utc`. After `LEASE_TIMEOUT` (3 min) it attempts an `If-Match`
PUT to overwrite the claim with its own ownership. The original owner can
no longer refresh because the etag has changed — it will see its `If-Match`
fail and self-fence (see below).

### 4. Progress (per-host, observability only)

```
progress/host-<id>.json
```

```json
{
  "host": "worker-07",
  "started_utc":   "2026-05-02T10:00:00Z",
  "heartbeat_utc": "2026-05-02T10:42:11Z",
  "current_shard": "part-0042.parquet",
  "shard_rows_total": 12345678,
  "shard_rows_done":  4521000,
  "shard_bytes_done": 4398046511104,
  "files_ok":     4520837,
  "files_failed":      163,
  "throughput_mb_s_1m": 22150.4
}
```

Updated every 30s. Aggregator reads these. **Workers do not depend on
progress files for correctness** — they're observability only.

### 5. Batch audit trail (per-host, append-only)

```
batches/host-<id>.jsonl
```

One record per micro-batch completed (see Mover below). For post-run
forensics — which batches took how long, which contained failures.
Optional; can be turned off at scale.

### 6. Per-file failures

```
failures/host-<id>.jsonl
```

One line per failed file with full context for retry:

```json
{
  "row_id": 17592186049321,
  "shard":  "part-0042.parquet",
  "path_b64": "L2RhdGEvc3R1ZmYvxIDigKzigJzigKjigJk=",
  "error": "ENOSPC",
  "phase": "write",
  "ts": "2026-05-02T10:55:33Z"
}
```

`path_b64` because POSIX paths aren't guaranteed UTF-8. A separate retry
run reads `failures/*.jsonl`, builds a synthetic single-shard parquet, and
re-runs against it.

### 7. Aggregator (Rust, read-only sidecar)

Same as v1: separate Rust binary, reads `progress/`, `shards/`, and
optionally `batches/` and `failures/`. Three modes: TUI, JSON summary,
Prometheus exporter. Workers don't depend on it.

---

## Mover (the wire-rate part)

This is where throughput lives or dies. The mover is a per-worker subsystem
that, given a parquet shard and the file rows in it, copies files from
source to dest as fast as the underlying servers allow.

### Architecture

```
   ┌──────────────────────────────────────────────────────────┐
   │                       Worker process                     │
   │                                                          │
   │  parquet shard (mmap'd local tmpfs)                      │
   │           │                                              │
   │           ▼                                              │
   │   batch builder ── byte-budgeted, file-type-aware ──┐    │
   │                                                     ▼    │
   │   ┌───────────────────────────────────────────────────┐  │
   │   │   work pool: tokio tasks over file rows           │  │
   │   │                                                   │  │
   │   │   for each row:                                   │  │
   │   │     classify(file_type, size) → strategy          │  │
   │   │       ↓                                           │  │
   │   │     ┌──────────────────────────────────────────┐  │  │
   │   │     │ strategy A: NFSv4.2 server-side COPY     │  │  │
   │   │     │   (when same server src+dst)             │  │  │
   │   │     │                                          │  │  │
   │   │     │ strategy B: libnfs READ → libnfs WRITE   │  │  │
   │   │     │   driven by io_uring fixed buffers       │  │  │
   │   │     │   (default)                              │  │  │
   │   │     │                                          │  │  │
   │   │     │ strategy C: kernel copy_file_range       │  │  │
   │   │     │   (only for ad-hoc kernel-mounted dirs)  │  │  │
   │   │     └──────────────────────────────────────────┘  │  │
   │   └───────────────────────────────────────────────────┘  │
   │                                                          │
   │  attribute applier: chown / chmod / utimensat / xattr    │
   │  (post-data, batched per-file)                           │
   │                                                          │
   └──────────────────────────────────────────────────────────┘
```

### Why libnfs (user-space) instead of a kernel NFS mount

`nfs-walker` already speaks libnfs directly and gets ~340K entries/sec
against a 34-cnode VAST cluster. The same machinery is the right answer for
the mover for the same reasons:

- **No kernel mount tuning required.** No fighting with `nconnect`,
  `actimeo`, `nocto`, kernel page cache pressure, dirty page writeback
  storms, or `vm.dirty_*` parameters.
- **Predictable concurrency.** Per-worker, per-connection pipeline depth is
  a config parameter, not a kernel side-effect.
- **Same code path as the scanner.** Reusing the libnfs wrapper from
  `nfs-walker` means the mover sees the filesystem the same way the
  walker did when it built the index — same inode resolution, same
  symlink semantics, same xattr handling.
- **Direct RDMA path possible later** without rebuilding the kernel
  module story.

### Why io_uring on top of libnfs

libnfs is single-threaded per connection. To saturate a 200GbE link with
1KB files (millions of IOPS) you need:

- Many libnfs connections (the walker uses up to 1024 walker threads).
- Each connection issuing many in-flight RPCs.

io_uring provides:

- A zero-syscall submission/completion path for the per-RPC work the
  worker does outside libnfs (file-local buffer alloc, attribute syscalls
  on local tmpfs, batch flushes).
- Fixed-buffer registration: pre-allocate N MB of buffers, register them
  once, hand them to libnfs READ/WRITE without per-op alloc/free.
- Batched submission of attribute ops (`fchownat`, `fchmodat`,
  `utimensat`, `setxattr`) at end-of-file.

The combined model: libnfs handles the NFS protocol; io_uring handles
everything else. Walker hits ~340K op/s with this pattern; the mover
should land in the same neighborhood for small-file workloads.

### Strategy selection per file

Decided per-file based on `(file_type, size, src_url, dst_url)`:

| Condition | Strategy |
|---|---|
| `src` and `dst` are the same NFSv4.2 server, file is regular, size > 64KB | **NFSv4.2 server-side COPY** (`COPY` op). Wire rate = server's internal rate. Mover does almost no data-plane work. |
| Symlink | `READLINK` (or use cached `symlink_target` from index) → `SYMLINK` on dest. No data path. |
| Hardlink (nlink > 1, inode seen before in this shard) | `LINK` on dest, no data copy. |
| Empty file (size == 0) | `CREATE` only. |
| Otherwise | **libnfs READ → libnfs WRITE via io_uring fixed buffers**. |

Strategy C (`copy_file_range` over kernel mounts) is **not** the default
but is supported as an escape hatch for environments where libnfs can't be
used (e.g. NFSv3-only with no Kerberos requirement and the operator has
already mounted both sides).

### Byte-budgeted micro-batches

Within a shard, the worker walks rows in `row_id` order and accumulates a
micro-batch up to:

- `BATCH_BYTES = 8 GiB` of source data, OR
- `BATCH_FILES = 100_000` rows, OR
- end of shard.

Whichever hits first. This matters:

- **1KB files**: a 100K-row batch is 100MB of source data — fits comfortably
  in fixed buffers, gives io_uring enough work to amortize submission cost.
- **1GB files**: a 8-file batch is 8GiB; mover streams them with bounded
  in-flight count.

Per-batch in-flight concurrency is also adaptive:

- For files < 1 MB: up to **256 concurrent files** in flight (IOPS bound).
- For files 1 MB – 1 GB: up to **16 concurrent files** (bandwidth bound).
- For files > 1 GB: up to **4 concurrent files**, each split into striped
  reads of `STRIPE_SIZE = 4 MiB` × `STRIPE_DEPTH = 32` (bandwidth bound,
  large per-file readahead).

These numbers are starting points; the worker should expose them as config
and we'll tune in M6.

### Attribute preservation

After data is written, attributes are applied **once per file** to avoid
multiple round-trips:

1. `WRITE` last data block.
2. `SETATTR` with mode + uid/gid + atime/mtime in a single call (NFSv4
   batches these).
3. If `xattr_blob` non-null, issue `SETXATTR` per name/value pair. **In v1
   this branch is dead code** — the walker doesn't emit xattrs yet, so
   `xattr_blob` is always NULL. The mover code path is wired up so xattr
   support lights up automatically once the walker side lands.

For NFSv3 servers, `SETATTR` doesn't carry uid/gid/mode in one call as
cleanly; mover falls back to a sequence but submits them via io_uring
chained ops to keep the syscall overhead low.

`utimensat` happens last so that a successful copy is always reflected in
the dest file's mtime matching the source.

### Atomic destination writes

Files are written to a temp name in the dest directory:

```
.<basename>.<host>.<pid>.partial
```

On success, atomic `RENAME` to the final name. On failure, the partial
file remains (cleaned up by a separate `mig-aggr clean-partials` mode).
This guarantees: a reader on the dest never sees a half-written file.

### Hardlinks

Hardlink groups are detected by `inode` collisions within the index. The
walker reports `inode` and `nlink`; the mover:

1. On first occurrence of an inode, copies the file normally and remembers
   `(inode → dest_path)` in a per-shard map.
2. On subsequent occurrences, issues NFS `LINK` from the remembered
   `dest_path` to the new path.

Cross-shard hardlinks are out of scope for v1 — they fall back to a full
copy and lose hardlink identity. (Real fix: pre-process the index to put
all rows of a hardlink group into the same shard.)

---

## Worker lifecycle

1. **Startup**
   - Read `manifest.json`, verify ETags of shards.
   - Generate stable `host-id` (configured, or hostname+random suffix).
   - Reconcile own state: read `shards/*.claim` looking for any owned by
     this `host-id`. If found, the worker resumes them (epoch bump).
   - Open libnfs handles to source and dest URLs.

2. **Claim a shard**
   - List `shards/`, identify a parquet shard with no live claim.
     "Live" = `(now - claimed_utc) < LEASE_TIMEOUT`.
   - Issue conditional PUT to `shards/<shard>.parquet.claim`:
     - First-time claim: `If-None-Match: *`.
     - Reclaim of stale: `If-Match: <stale-etag>`.
   - On 412, pick another shard.
   - On success, download `index/<shard>.parquet` to local tmpfs, mmap.

3. **Process shard**
   - Walk rows in `row_id` order, building byte-budgeted micro-batches.
   - For each micro-batch, run the mover (strategy selection per row).
   - On every 30s tick: refresh claim (heartbeat), update `progress/`,
     append batch audit if enabled.
   - On any heartbeat refresh failure (412 — someone reclaimed): **stop
     all in-flight copies, drop libnfs handles for in-flight files, exit
     this shard.** This is the self-fence.

4. **Complete shard**
   - When all rows in the shard are processed, write a final claim record
     with `state: "completed"` (overwrite, `If-Match`).
   - Delete local mmap'd parquet.
   - Loop to step 2.

5. **Exit**
   - When no claimable shards remain (every claim is `completed` or live
     and recent), the worker exits cleanly.

---

## Self-fencing (correctness)

The dual-writer scenario is the one bug that can corrupt user data, so it
gets explicit treatment.

**Invariant**: at any instant, the worker writing to dest paths derived
from rows in shard X must hold a valid claim on X.

**Mechanism**:
- The mover's batch loop checks an atomic "claim_valid" flag before issuing
  each new file copy.
- The heartbeat task sets `claim_valid = false` immediately on any of:
  - HTTP 412 on heartbeat (lost claim).
  - HTTP 5xx repeated past retry budget.
  - Local clock jump > `LEASE_TIMEOUT/2`.
- When `claim_valid = false`, the mover:
  - Cancels in-flight tasks (`tokio::select!` on a cancellation token).
  - **Does not** issue further `RENAME` ops. Partial files left on dest
    are cleaned up by `mig-aggr clean-partials`.
  - Exits the shard. The worker may try to claim a new shard.

This is why writes go to `.partial` and only `RENAME` makes them visible:
the rename is the commit point, and we don't commit without a valid claim.

---

## Conflict resolution

There isn't any. S3 conditional PUT serializes claims at the storage layer.
Two workers issuing `If-None-Match: *` against the same key: one gets 200,
the other gets 412. No timestamps, no host-id tiebreakers, no clock
assumptions.

---

## Dynamic membership

- **Add a host**: it starts, follows the lifecycle. Claims whatever's free.
- **Remove gracefully**: worker drains current shard, marks claim
  `completed`, exits.
- **Crash**: claim goes stale (no heartbeat refresh). After
  `LEASE_TIMEOUT` (3 min), another worker reclaims. Crashed worker, if it
  comes back, sees its claim has been overwritten and self-fences.

---

## Failure modes

| Failure | Detection | Recovery |
|---|---|---|
| Worker crashes mid-shard | Heartbeat stale > 3 min | Another worker reclaims via `If-Match` PUT. |
| Worker partitioned from S3 | Heartbeat refresh fails | Worker self-fences before any other host reclaims. |
| Source NFS slow / cnode pinning | Throughput metric drops | Surfaced in `progress/`; operator can rebalance shards or add hosts. |
| Source NFS file gone at copy time | Per-file ENOENT | Logged to `failures/`; batch continues. |
| Dest NFS full | Per-file ENOSPC | Logged; worker pauses claims (backpressure gate). |
| Dest NFS partial write | io_uring error | `.partial` file remains; not renamed; logged. |
| Two workers race for shard | S3 conditional PUT | One gets 412 and picks another shard. |
| Parquet shard corrupt | Parquet decode error | Worker writes `failed` claim record, picks another shard. |
| NFSv4.2 COPY unsupported on path | Server returns NOTSUPP | Mover falls back to READ/WRITE for that file. |
| Aggregator down | N/A | Workers don't care; observability degraded only. |
| Manifest swapped underneath us | ETag mismatch on shard re-fetch | Worker logs and exits this run. |

---

## Backpressure

If the worker's recent failure rate exceeds a threshold (e.g. >5% in the
last 60s) **or** sustained throughput drops below a floor, the worker:

- Stops claiming new shards (does not abandon current).
- Continues current shard at reduced concurrency.
- Logs a `degraded` event to `progress/`.

Avoids the failure mode where all 100 hosts pile onto a degraded dest and
generate 100× ENOSPC events.

---

## Read patterns on VAST S3 (revised)

| Object | Frequency per host | Size | Notes |
|---|---|---|---|
| `manifest.json` | Once at startup | KB | Cached. |
| `index/part-NNNN.parquet` | Once per shard | GB | **Downloaded fully to local tmpfs**; not range-read on the data path. |
| `shards/*.claim` (LIST) | Every claim attempt + every 30s | KB | LIST returns small set. |
| `shards/<own>.claim` (PUT) | Every 30s heartbeat + on completion | <1 KB | Conditional PUT. |
| `progress/host-<self>.json` (PUT) | Every 30s | KB | Overwrite. |
| `batches/host-<self>.jsonl` (PUT) | Per micro-batch | KB | Append-then-PUT. |
| `failures/host-<self>.jsonl` (PUT) | Per failed file | KB | Append-then-PUT. |

At 100 hosts, claim/heartbeat traffic is ~3.3 PUT/s and ~3.3 LIST/s
**globally**. Shard parquet downloads are the only bulk traffic and happen
~once per worker per ~10 min (assuming wire-rate processing of a few-GB
shard). VAST S3 sees almost no load.

---

## Configuration

```toml
[run]
bucket   = "migration-run-2026-05-02"
endpoint = "https://vast-s3.example.com"
region   = "us-east-1"

[worker]
host_id           = "auto"
heartbeat_sec     = 30
lease_timeout_sec = 180          # 3 min, 6× heartbeat

[shard]
local_scratch  = "/var/lib/mig/scratch"  # tmpfs preferred
max_in_flight  = 1                       # # of shards concurrently in this worker

[mover]
strategy_default     = "libnfs_io_uring"  # or "nfs42_copy", "kernel_cfr"
src_url              = "nfs://src/export"
dst_url              = "nfs://dst/export"
nfs_connections      = 16
pipeline_depth       = 8
io_uring_queue_depth = 256
fixed_buffer_count   = 256
fixed_buffer_size    = "1 MiB"

[batch]
bytes_budget       = "8 GiB"
files_budget       = 100_000
inflight_small     = 256          # files < 1 MiB
inflight_medium    = 16           # 1 MiB – 1 GiB
inflight_large     = 4            # > 1 GiB
large_stripe_size  = "4 MiB"
large_stripe_depth = 32

[copy]
preserve_owner = true
preserve_mode  = true
preserve_times = true
preserve_xattr = true              # honored when walker emits xattr_blob; no-op until then
server_side_copy = "auto"          # auto | force | off

[backpressure]
failure_pct_window_sec = 60
failure_pct_threshold  = 5.0
throughput_floor_mb_s  = 100
```

---

## Open questions (smaller, real)

1. **Hardlink groups across shards**: do we accept the v1 limitation, or
   add an "index post-processor" step that re-shards by inode-group?
2. **NFSv4.2 COPY testing**: validate against VAST's NFSv4.2 implementation
   for both the same-server and cross-server cases. May not be supported
   on all surfaces.
3. **Worker count vs source cluster size**: from the walker measurements,
   the source NFS cluster has a per-client throughput ceiling. With 100
   workers the aggregate exceeds any single client, but if all 100 hammer
   one cnode, we'll re-create the pinning issue. Worth thinking about
   shard-to-cnode affinity in the index build (out of scope for v1, worth
   noting).

---

## Resolved

- **VAST S3 conditional PUT**: confirmed. `If-None-Match: *` and
  `If-Match: <etag>` both work per RFC 9110. The claim protocol can rely
  on them.

---

## Future work (deferred but designed-for)

- **xattr capture in `nfs-walker`**. The mover schema reserves
  `xattr_blob` and the apply path is wired up (currently dead code
  because the column is always NULL). When walker xattr support lands:
  - Walker reads xattrs during scan (`GETXATTR` / `LISTXATTR` over libnfs).
  - Serialization format for `xattr_blob` to be defined in
    `migration-core` — proposed: length-prefixed `(name_len, name,
    value_len, value)` tuples, repeated. Spec it before walker
    implementation so both sides agree.
  - Decide on filtering rules — e.g. skip `system.*` namespaces, preserve
    `user.*` and `security.*` (the latter requires CAP_SYS_ADMIN on dest).
  - No mover changes required when this lights up; existing branch
    activates as soon as non-NULL `xattr_blob` values appear.
- **Cross-shard hardlink consolidation** (see open question 1).
- **Shard-to-cnode affinity hints** in the manifest, so workers can be
  pinned to source cnodes that own their shards' data.
- **Resume-after-restart** of partially-completed shards. v1 restarts a
  shard from the beginning if a worker died mid-shard. Mid-shard checkpoints
  (every N micro-batches) would let reclaim resume from the last committed
  row, at the cost of more S3 writes.

---

## Tech stack

- **Language**: Rust.
- **Workspace**:
  ```
  migration/
  ├── crates/
  │   ├── migration-core/   # types, S3 client, parquet reader, claim protocol
  │   ├── migration-mover/  # libnfs + io_uring data path
  │   ├── migration-worker/ # binary: orchestration + mover
  │   └── migration-aggr/   # binary: read-only sidecar
  ```
- **Crates**:
  - `aws-sdk-s3` (Apache-2.0).
  - `arrow` + `parquet` v54 (Apache-2.0).
  - `tokio` (MIT).
  - `io-uring` crate or `tokio-uring` (MIT).
  - `libnfs` via FFI — same approach as `nfs-walker` (LGPL-2.1-or-later;
    see Attribution below).
  - `serde`, `serde_json`, `clap`, `tracing`, `ratatui` (MIT or dual MIT/Apache-2.0).
  - `crossbeam-channel`, `crossbeam-deque` (MIT/Apache-2.0) — same
    primitives as `nfs-walker` for the work-stealing pool.

---

## Milestones

1. **M1 — Skeleton.** Single worker, claims a shard via S3 conditional
   PUT, mmaps parquet, walks rows, logs (no-op mover). Aggregator reads
   `progress/`.
2. **M2 — Mover v1: libnfs READ/WRITE.** No io_uring yet. Single-file
   copies with attribute preservation. Validate end-to-end correctness
   against a small dataset and verify SHA-256 match source/dest.
3. **M3 — io_uring + fixed buffers + adaptive concurrency.** Performance
   pass: byte-budgeted batches, in-flight concurrency by size class.
   Bench against `nfs-walker`'s measured ceiling.
4. **M4 — NFSv4.2 server-side COPY fast path.** Detect server support,
   route eligible files. Bench delta vs M3.
5. **M5 — Multi-host + self-fencing test.** 3 workers; kill -9 mid-batch;
   verify reclaim + self-fence + no dual-writer corruption (audit dest
   tree against source).
6. **M6 — Scale test.** 100 workers on the production-class target. Tune
   batch budgets, concurrency, connection counts. Goal: aggregate
   throughput within 90% of source-cluster ceiling.
7. **M7 — Hardlink + symlink end-to-end (xattr deferred).** Validate
   POSIX attribute fidelity (mode, uid/gid, atime/mtime, hardlinks,
   symlinks) via a `mig-aggr verify` mode that diffs src and dst tree
   metadata. xattr verification gated on walker support landing.

---

## Attribution / licensing notes

This design is original. When implementing:

- **`libnfs` is LGPL-2.1-or-later.** This is the most important attribution
  consideration in the project. LGPL allows use from non-LGPL code via
  dynamic linking. The `nfs-walker` precedent (which links libnfs via FFI
  through a build script + `pkg-config`) is what we'll follow. Required:
  - Distribute the libnfs source or a written offer for it with any
    binary distribution.
  - Preserve libnfs's copyright and license notices.
  - If the mover ever statically links libnfs, that triggers stronger
    obligations; avoid static linking.
- **`nfs-walker` is MIT.** Reusing its libnfs FFI wrappers requires
  preserving its copyright and license notice in any derived files.
  Easiest path: vendor the wrapper code into `migration-core` with the
  original MIT header intact.
- **`aws-sdk-s3`, `arrow`, `parquet`** are Apache-2.0 — preserve `LICENSE`
  and `NOTICE`, mark any modified files.
- **`tokio`, `serde`, `clap`, `tracing`, `crossbeam-*`, `ratatui`,
  `io-uring`** are MIT (or dual MIT/Apache-2.0) — preserve copyright
  notices.
- **`gxhash`** (used in `nfs-walker` for checksums; if reused for
  end-to-end verify) — verify license before pulling in.

A `THIRD_PARTY_LICENSES.md` file at the workspace root, regenerated by
`cargo about` on every build, is the right pattern for keeping this
honest.
