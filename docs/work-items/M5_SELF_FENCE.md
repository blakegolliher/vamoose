# M5 — Multi-host self-fence verification

Status: harness landed; manual run on real VAST hardware pending.

## Goal

Prove, against real VAST S3 + NFS hardware, that two workers
contending for the same parquet shard cannot produce dual-writer
corruption on the destination tree. Specifically:

- The lease/reclaim handshake serializes ownership across workers.
- A worker that loses its claim mid-shard self-fences before
  committing any further `.partial → final` rename.
- The destination tree exactly equals the source tree by content,
  with no double-commit and no orphan partials from the worker
  that completed cleanly.

## Scope: two workers, not three

`DESIGN.md` Milestones lists "3 workers" for M5. This harness exercises
**two workers**. The substitution is called out explicitly here:

- **Originally in scope:** three workers, kill -9 mid-batch.
- **Substituted with:** two workers, SIGSTOP mid-shard followed by
  SIGCONT after the second worker has reclaimed and finished.
- **Reason:** Two workers on a single shard already exercises the
  full reclaim + self-fence path. A third worker on the same shard
  serializes at the S3 conditional-PUT layer the same way — it adds
  no protocol surface that two doesn't already cover. Three-worker
  testing is more useful at multi-shard scale (M6, where the
  question is throughput and rebalancing rather than correctness).
- **Why SIGSTOP rather than SIGKILL:** SIGKILL terminates worker A
  cleanly and leaves no way to test the self-fence path — A has
  already exited by the time the heartbeat would have failed.
  SIGSTOP keeps A's process alive but unable to run, simulating a
  network partition / GC pause / cgroup throttle. After SIGCONT, A
  tries to refresh its claim, gets 412 because B has overwritten it,
  and self-fences. That is the path under test.
- **Extension:** The harness is trivially extensible to N workers
  in a loop if the user later wants three- or more-worker coverage.

## What "done" looks like

The harness asserts the following, in order, after both workers have
exited. Each assertion writes pass/fail to `assertions.log`; any
failure causes the script to exit non-zero.

- **A. Final claim record.** The shard's final claim object has
  `state == "Completed"` and `host == B`. Worker B was the last
  legitimate owner.
- **B. Dest file count == source file count.** Counted with `find -type f`,
  excluding any `.partial` survivors.
- **C. Per-file SHA-256 match.** `sha256sum` over every regular file,
  sorted, must diff clean between source and destination roots.
  Content-only — `stat` output is not compared because dest mtimes
  are µs-truncated by `nfs_utimes` (see
  `docs/work-items/MOVER_UTIMENSAT.md`); that is expected and not a
  correctness defect.
- **D. Worker A clean self-fence.** A's exit code is `0`, A's stderr
  contains at least one `"fence tripped"` line, and that line's
  reason mentions either `"412"` or `"heartbeat refresh"` — i.e. A
  fenced because the heartbeat refresh saw 412 after B reclaimed,
  not for any other reason.
- **E. Failures sinks.** `failures/host-A.jsonl` and
  `failures/host-B.jsonl` are either absent in S3 or empty. The
  source tree is static; any per-file failure is a real failure.
- **F. Single commit per dest path.** A single `commit: rename
  .partial → final` debug log line exists per final dest path
  across **A.err and B.err combined**. No path commits twice.
- **G. No orphan partials from B.** `.partial` files on the dest
  matching A's `<host>.<pid>` stamp are allowed (A was paused
  mid-write). `.partial` files matching B's `<host>.<pid>` stamp
  are forbidden (B exited cleanly). A final dest file with the same
  basename as a B partial is forbidden — it would mean B somehow
  committed an inconsistent rename.

## Why these assertions are sufficient

The **"no double-commit"** guarantee is proven by the conjunction of
F (single rename event per dest path) with A, B, and C (final tree
state matches source). It is **not** proven by the SIGSTOP timing
alone — SIGSTOP only buys the *opportunity* for the race to occur;
the assertions prove the race did not corrupt state regardless.

The protocol-level reasoning underneath:

1. **Lease ownership serializes at S3.** `DESIGN.md` "Conflict
   resolution" and `docs/CORRECTNESS_RULES.md` "Critical correctness
   rules" pin this:
   `If-None-Match: *` and `If-Match: <etag>` are honored by VAST S3
   per RFC 9110. Two workers cannot simultaneously hold a valid etag
   on the same claim object. When B reclaims, A's etag becomes
   stale; A's next refresh PUT returns 412.

2. **The fence trips before the next commit.**
   `docs/CORRECTNESS_RULES.md` "Critical correctness rules" —
   *"Self-fence before commit. The mover must
   check `fence.is_valid()` before issuing RENAME (the commit
   point). If the fence is tripped, leave the `.partial` file and
   exit the shard."* The heartbeat task converts a 412 into
   `Fence::trip(...)`, which sets the atomic invalid flag and
   cancels the cancellation token. The shard processor checks
   `fence.is_valid()` between rows and at task entry; the mover
   running rows already in flight at fence-trip time still attempts
   to rename, but B's overwrite of the claim object is a strict
   happens-after of B's own renames — see (3).

3. **Partial-file naming is collision-free across workers.**
   `.<basename>.<host>.<pid>.partial` (`docs/CORRECTNESS_RULES.md`
   "Atomic rename") means workers never write to each other's partials. Worker A's
   in-flight `.partial` for some path P cannot collide with worker
   B's `.partial` for the same path P. Both workers' renames target
   the same final dest, but they target *distinct* sources for that
   rename — the source side of `nfs_rename` is per-worker. The only
   way the dest can end up wrong is if both workers committed a
   final rename for the same path, which assertion F catches
   directly.

4. **Static source tree.** The harness generates files once and
   never touches them again. There is no source mutation under copy
   that could legitimately produce divergent file content between
   the two workers' copies.

What this **doesn't** prove:

- It does not prove anything about kernel-buffer zombie writes —
  see Known limitations.
- It does not prove correctness under source mutation, hardlink
  fanout, or symlink targets that change mid-run.

These are M3.5+ / M7 concerns and are explicitly out of scope here.

## Test parameters and timing budget

Defaults are tuned to widen the mid-shard window so worker A can be
caught with `kill -STOP` between the time it claims the shard and
the time it would otherwise finish:

| Parameter | Default | Why |
|---|---|---|
| `--files` | `1000` | Enough rows that mid-shard pause is reliably catchable; few enough to keep the run short. |
| `--file-size` | `4096` bytes | Files are deterministic content. Bytes encode the path so a SHA-256 mismatch is diagnosable. |
| `[worker] heartbeat_sec` | `10` | Faster reclaim-after-stale than the 30s production default. |
| `[worker] lease_timeout_sec` | `60` | 6× heartbeat, mirrors the production ratio. |
| `[mover] nfs_connections` | `1` | One context = one libnfs RPC at a time = mid-shard window stays open long enough to SIGSTOP. |
| `[batch] inflight_small/medium/large` | `1/1/1` | Same reason — keep the dispatch single-row at a time. |
| Reclaim wait budget | `2 × lease_timeout_sec` (120s) | Enough for B to observe staleness and reclaim. |
| B-completes wait budget | `4 × lease_timeout_sec` (240s) | Headroom for B to finish 1000 × 4 KiB at low concurrency. |
| A-self-fences wait budget | `3 × heartbeat_sec` (30s) | After SIGCONT, A's first heartbeat must trip and exit. If A hangs past this budget, treat as a regression of `WORKER_SHUTDOWN_HANG` (fixed in 8f768f8). |

## Known limitations

- **Two workers, not three** (see Scope above).
- **Does not exercise kernel-buffer zombie writes.** libnfs is a
  userspace transport: writes go out via `nfs_pwrite` on the calling
  thread. The mover wraps each per-file copy in
  `tokio::task::spawn_blocking`, so a SIGSTOP'd worker process
  emits no further bytes to the network until SIGCONT. A
  kernel-mounted destination would have a different risk profile —
  pages already dirtied in the page cache can flush after the
  process is paused — but the worker doesn't use a kernel mount on
  the data path. M3.5+ if a kernel-mount escape hatch ever ships
  to production.
- **Static source tree.** No source mutation under copy. Walker
  re-runs against a changing tree are out of scope.
- **Single shard.** This harness produces exactly one parquet shard
  and refuses to start otherwise. Multi-shard contention and
  shard-stealing semantics are M6 territory.
- **No NFSv4.2 server-side COPY.** Strategy selection on this stack
  always picks libnfs READ→WRITE under NFSv3; the rename commit
  path is the same regardless.

## Recipe

Real-hardware run, from the workspace root:

```bash
# Preflight: build the workspace so the harness's freshness check
# (binary mtime > newest src mtime) passes.
cd ~/projects/vamoose/migration
cargo build --release --workspace

# Run the harness.
scripts/m5-self-fence-test.sh \
    --files 1000 \
    --file-size 4096 \
    --bucket-prefix "m5-$(date -u +%Y%m%dT%H%M%SZ)"

# On success, tail the assertions log:
tail -n +1 m5/run/<ts>/assertions.log
```

Per-flag overrides:

| Flag | Default | Purpose |
|---|---|---|
| `--files N` | 1000 | Number of source files to generate. |
| `--file-size BYTES` | 4096 | Per-file size. |
| `--bucket-prefix S` | `m5-<ts>` | S3 key prefix under the test bucket. |
| `--keep-artifacts` | off | Skip cleanup of the test bucket prefix. Useful for forensics. |

Required environment:

- `AWS_PROFILE` — VAST S3 profile (e.g. `var204`).
- `VAMOOSE_BUCKET` — S3 bucket for the run.
- `VAMOOSE_ENDPOINT` — VAST S3 endpoint URL.
- `VAMOOSE_SRC_NFS_URL` and `VAMOOSE_DST_NFS_URL` — distinct
  source/dest NFS exports, no overlap (the worker's overlap guard
  refuses overlapping endpoints).
- `VAMOOSE_SRC_MOUNT` and `VAMOOSE_DST_MOUNT` — kernel-mounted
  paths the harness uses to (a) populate the source tree and
  (b) read back the dest tree for SHA-256 verification. The worker
  itself uses libnfs on `*_NFS_URL`; the kernel mounts exist only
  for the harness's own driving and reading.
- `NFS_WALKER` — path to a post-RocksDB-removal walker binary
  (single-step direct parquet output: `nfs-walker <url> -o <out>.parquet`).
  Default detection prefers
  `~/projects/nfs-walker/target/release/nfs-walker`; the older
  `~/projects/nfs-walker/build/nfs-walker` symlink is stale and
  ships the obsolete `export-parquet` subcommand, so the harness only
  falls back to it if `target/release` is missing entirely.

## Layout

Each run writes to `m5/run/<ts>/` under the workspace root:

```
m5/run/<ts>/
├── assertions.log     # one line per assertion: PASS/FAIL <name> <detail>
├── workerA.toml
├── workerB.toml
├── A.out, A.err, A.pid
├── B.out, B.err, B.pid
└── source/            # generated source tree (deterministic content)
```

S3 artifacts under `s3://$VAMOOSE_BUCKET/<bucket-prefix>/` are
removed on clean exit unless `--keep-artifacts` is passed.
