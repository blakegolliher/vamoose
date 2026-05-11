# M5 partition test — R8 stress harness

Companion to `docs/work-items/M5_SELF_FENCE.md`. Same protocol, same
assertions A–G, plus one new assertion (H) that actually exercises
R8's commit-point gate.

## Why this exists

The original M5 (SIGSTOP) harness halts worker A's entire process,
including its heartbeat task. After SIGCONT, the heartbeat takes at
least one tick interval to fire its first HEAD and detect ownership
loss; until then `fence.is_valid() == true` and R8's commit-point
checks all wave rows through. The SIGSTOP harness therefore PASSes
with R8 in but never actually invokes the R8 code path
(`files_fenced == 0` in the observed run).

R8 only fires when the fence trips **while rows are in flight**.
That is the production failure mode where:

- The worker is running normally (heartbeat task scheduled, copies
  dispatching).
- S3 becomes unreachable *for this worker only*.
- The heartbeat HEAD calls fail consistently.
- After `ceil(lease_timeout / heartbeat_sec)` consecutive failures
  (R6 retry budget), the heartbeat task trips the fence preemptively.
- Rows currently in `spawn_blocking` hit the R8 check and bail with
  `FailurePhase::Fenced` instead of committing.

This harness reproduces that scenario with `iptables`.

## Mechanism

```
T+0      Phase 1+2: source tree, walker, configs.
T+a      Phase 3: launch A. A claims shard, starts copying rows.
T+a+ε    Wait for A to have committed N pre-partition rows (default 30).
T+b      Install iptables REJECT --tcp-reset on outbound port 443 to
         the resolved VAST endpoint IP. Existing pooled connections get
         RST; new SYNs get RST. NFS (port 2049) is untouched, so A keeps
         copying files at libnfs level.
T+b+1s   First failed heartbeat HEAD. consec_failures = 1.
…
T+b+R*hb After R = ceil(lease_timeout/heartbeat_sec) ticks of failure,
         R6 trips the fence with reason
         "heartbeat HEAD failing for N consecutive ticks ...".
T+b+R*hb In-flight rows (up to inflight_*) hit Mover::check_fence()
         before their commit op (ops::rename / ops::link / ops::symlink)
         → MoveError(phase=Fenced, error="FENCE_TRIPPED").
         Shard processor's record_outcome arm:
           - increments outcome.files_fenced (NOT files_failed)
           - emits tracing::warn with row_id
           - does NOT write to FailureSink (M5 assertion E preserved)
T+b+R*hb +ε  Shard processor's between-row check sees invalid fence,
         exits the shard cleanly. Worker exits.
T+c      Phase 3 cont'd: remove iptables rule. Launch B.
T+c+ε    B's scan sees A's stale claim (claimed_utc is the original
         acquire time; v2 heartbeat doesn't bump it). B reclaims and
         processes the remainder of the shard.
T+d      B completes shard.
```

## Assertions

A–G are identical to M5_SELF_FENCE.md. Repeated here for completeness;
**H** is the new one this harness exists for.

- **A.** Final claim record `state == "Completed"` and `host == B`.
- **B.** Dest file count equals source file count, `.partial` excluded.
- **C.** Per-file SHA-256 match between source and destination trees.
- **D.** Worker A self-fenced cleanly. Exit code 0 AND at least one
  `"fence tripped"` line in A.err AND the reason mentions either
  `"412"`, `"heartbeat refresh"`, `"HEAD shows different etag"`, or
  `"heartbeat HEAD failing for"` (R6 path — the path this test
  actually exercises).
- **E.** `failures/host-A.jsonl` and `failures/host-B.jsonl` absent or
  empty. R8 sends Fenced rows to a separate counter, not to the
  failures sink.
- **F.** No concurrent in-flight renames across A.out + B.out within
  1.0s. Sequential duplicates allowed (same population mix as M5).
- **G.** No B-stamped `.partial` files survive on dest. A-stamped
  `.partial` files are allowed.
- **H.** *R8 actually fired.* At least one of:
  - `files_fenced > 0` in `progress/host-m5-host-A.json` (read while
    the run is still live, OR via `--keep-artifacts` to skip the
    S3 cleanup), and/or
  - `grep -c "FENCE_TRIPPED\|Fenced" m5/run/<ts>/A.err > 0`
  These are not redundant — if the harness wipes the progress object
  during cleanup before the assertion runs, the log-count fallback
  catches it.

## Why H is the load-bearing assertion

Without H, the partition test would just be a "different mechanism for
triggering R3" test. H is what proves the R8 mechanism — the
commit-point fence check inside the mover — actually executed.

The expected value of `files_fenced`:

- With `inflight_small/medium/large = 1` (default for predictability),
  exactly one row is inside `spawn_blocking` at fence-trip time. R8
  catches it with very high probability (the libnfs read-write loop
  is much longer than the post-R8 pre-rename window). Expected
  `files_fenced ≈ 1`.
- With `inflight > 1`, multiple rows are in flight; expected
  `files_fenced ≈ inflight × P(row past R8 at trip time) ≤ inflight`.
  Higher concurrency makes R8 more visible but also speeds up A's
  copy throughput, which means you need a bigger dataset to keep A
  copying during the entire R6 retry-budget window.

## Test parameters

| Parameter | Default | Why |
|---|---|---|
| `--files` | 5000 | Big enough that A is still copying when R6 exhausts at ~10s into the partition. M5 used 1000; partition needs more. |
| `--file-size` | 4096 | Same as M5. Deterministic content. |
| `--pre-partition-commits` | 30 | How many A-commits to wait for before installing the iptables block. Gives A a chance to settle into steady-state copy. |
| `--bucket-prefix` | `m5p-<ts>` | Distinct prefix from M5 to avoid stomping on M5 artifacts. |
| `--keep-artifacts` | off | Preserves S3 state + progress objects for forensics. **Strongly recommended** for the first few runs so you can inspect `files_fenced` on the progress file. |
| `[worker].heartbeat_sec` | 1 | Same aggressive value as the in-repo M5 harness. R6 budget = 10 ticks. |
| `[worker].lease_timeout_sec` | 10 | R6 trips fence at ~10s into partition. |
| Partition window | `lease + 30s` (≈ 40s) | 10s for R6 to fire + 30s buffer to confirm A has fully exited. |

## Known limitations

- **Host-wide port 443 block.** The iptables rule blocks port 443 to
  the VAST endpoint for the *entire host*, not just for A's process.
  Selective per-process blocking would need network namespaces or
  cgroup-v2 + nftables hooks, which is out of scope. Consequences:
  - The harness can't read S3 during the partition window. It just
    sleeps for `partition_window` seconds, then resumes.
  - If anything else on the host is talking to the VAST S3 endpoint,
    it'll get RSTed too. Run this on a quiet host.

- **B's claim only after partition lifts.** B can't be launched until
  iptables is removed; otherwise B can't reach S3 either. This means
  the partition test is *sequential* (A fences, then B runs) rather
  than concurrent like M5 (A and B overlap). The R3 fence-window
  population is therefore smaller: B doesn't dispatch any rows while
  A is still in flight. F's "sequential duplicates" count will be
  much lower than M5's — bounded by `pre_partition_commits` (~30) +
  any rows A committed after the partition started but before R8
  caught them (~`(R-1) × inflight × per-row-rate`).

- **Requires sudo iptables.** The Phase 0 preflight verifies
  `sudo -n iptables -L OUTPUT` works; if it doesn't, the test fails
  before doing any S3 work.

- **iptables rule must be removed unconditionally.** The cleanup
  trap removes it before any other cleanup work. If the trap doesn't
  fire (`kill -9` of the harness), operator must run
  `sudo iptables -D OUTPUT -d <vast-ip> -p tcp --dport 443 -j REJECT --reject-with tcp-reset`
  manually. The harness logs the rule it installs so this is
  recoverable.

## Recipe

```bash
cd ~/projects/vamoose/migration
cargo build --release --workspace

# Same env as M5:
export AWS_PROFILE=var204
export VAMOOSE_BUCKET=vamoose
export VAMOOSE_ENDPOINT=https://main.selab-var204.selab.vastdata.com
export VAMOOSE_SRC_NFS_URL=nfs://main.selab-var204.selab.vastdata.com/bgolliher/vamoose-source
export VAMOOSE_DST_NFS_URL=nfs://main.selab-var204.selab.vastdata.com/bgolliher/vamoose-dest
export VAMOOSE_SRC_MOUNT=/mnt/vamoose-source
export VAMOOSE_DST_MOUNT=/mnt/vamoose-dest
export VAMOOSE_SRC_ROOT=/m5p/$(date -u +%Y%m%dT%H%M%SZ)
export VAMOOSE_DST_ROOT=/m5p-dst/$(date -u +%Y%m%dT%H%M%SZ)
export AWS_S3_FLAGS=--no-verify-ssl

scripts/m5-partition-test.sh --files 5000 --file-size 4096 --keep-artifacts
```

After the run, with `--keep-artifacts`:

```bash
RUN_TS=$(ls -t m5/run/ | head -1)

# Inspect R8's worker-side count
aws --endpoint-url "$VAMOOSE_ENDPOINT" $AWS_S3_FLAGS s3 cp \
    s3://$VAMOOSE_BUCKET/progress/host-m5-host-A.json - | jq .

# Confirm Fenced log lines
grep -c "FENCE_TRIPPED\|Fenced" m5/run/$RUN_TS/A.err

# Confirm R6 trip reason
grep "heartbeat HEAD failing\|claim refresh" m5/run/$RUN_TS/A.err
```

A successful run looks like:

- All eight assertions PASS.
- `files_fenced` on host-A progress is ≥ 1.
- `A.err` has at least one `"heartbeat HEAD failing for N consecutive
  ticks"` line (R6 trip path).
- `A.err` has `files_fenced` Fenced-row warnings, one per R8 hit.
