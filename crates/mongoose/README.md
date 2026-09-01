# mongoose — single-host NFS-to-NFS data mover

`mongoose` is the vamoose migration engine with the distribution
removed, shipped as **one binary**. The nfs-walker scanner and the
`mig-walker-rewrite` canonical converter are compiled in as libraries
(the scanner pinned to the same commit
`packaging/nfs-walker.lock.json` pins for vamoose), and the copy path
is the same libnfs mover and shard processor the vamoose worker uses
— but everything lives on local disk under one work directory.

**No S3. No claims. No coordinator. No worker fleet. No TUI. No
external tools.**

## Usage

```bash
# 1. Build the local index (scan + canonical shards + manifest).
mongoose prepare \
  --src nfs://source.example.com/export \
  --dst nfs://dest.example.com/export \
  --source-root / \
  --dest-root / \
  --work-dir /var/lib/mongoose/run-001 \
  --walker-workers 32 \
  --shard-size-mb 512 \
  --exclude .snapshot

# 2. Copy using the index.
mongoose copy \
  --work-dir /var/lib/mongoose/run-001 \
  --nfs-connections 32 \
  --inflight-small 256 \
  --inflight-medium 16 \
  --inflight-large 4 \
  --use-raw-fh

# Or both in one shot.
mongoose run \
  --src nfs://source.example.com/export \
  --dst nfs://dest.example.com/export \
  --work-dir /var/lib/mongoose/run-001 \
  --nfs-connections 32 \
  --use-raw-fh

# 3. Resync while the source stays live: rescan, classify what
#    changed since the last pass, copy only that. Repeat as needed.
mongoose sync --work-dir /var/lib/mongoose/run-001 --use-raw-fh

# 4. Cutover: stop source writers, absorb the last drift, verify.
mongoose sync --work-dir /var/lib/mongoose/run-001 --use-raw-fh   # absorbs final drift
mongoose sync --work-dir /var/lib/mongoose/run-001 --cutover      # must find zero drift
```

Run as root: libnfs binds reserved ports for AUTH_SYS. The full
reserved-port range is enabled by default (`LIBNFS_USE_ALL_RESERVED`
is set for you; without it, `/etc/services` name registrations
throttle a host to ~55 context pairs — export
`LIBNFS_USE_ALL_RESERVED=0` to opt out). Nothing else to install or
stage — the scanner and converter are inside the binary. An existing nfs-walker scan can still be adopted with
`--scan-dir` instead of rescanning.

Logging is compact by default (warnings plus mongoose's own stage
lines and progress ticks). `-v` restores full INFO from the embedded
walker/rewrite/mover engines, `-vv` enables debug, and `RUST_LOG`
overrides the flag entirely.

`--purge-intermediates` (on `prepare`, `run`, and `sync`) deletes the
raw walker scan output once the canonical shards are committed — the
scan is a pure intermediate that doubles the index footprint — and,
for `sync`, also this pass's delta shards once the baseline has
advanced. Canonical shards are always kept: they are the baseline the
next sync classifies against. A `--scan-dir` outside the work dir is
never touched.

## Work-dir layout

```
<work-dir>/
  run.json                      run identity (sticky; a re-run with
                                different src/dst is refused)
  scan/attempt-NNNN/            nfs-walker output + progress log
  scan.json                     scan checkpoint
  canonical/part-NNNN.parquet   canonical shards
  rewrite.json                  mig-walker-rewrite checkpoint
  manifest.json                 local run plan (shard paths are
                                work-dir-relative, never S3 keys)
  progress.json                 copy progress + completed-shard list
  failures/part-NNNN.jsonl      per-file failures, per shard
  downgrades/part-NNNN.jsonl    per-file metadata downgrades, per shard
```

## Resume

- `prepare` checkpoints each stage; re-running skips a completed scan
  and resumes the rewrite (`mig-walker-rewrite --resume`).
- `copy` resumes at shard granularity: shards listed in
  `progress.json` are skipped, and an interrupted shard is
  reprocessed from its first row on the next run (copies are
  idempotent: `.partial` + rename, or truncate-and-heal with
  `--direct-commit`).
- SIGINT/SIGTERM stops at the next batch boundary; no row is ever
  interrupted mid-copy.

## Correctness posture (inherited from vamoose)

- Atomic `.partial` then rename publish by default; NFS COMMIT before
  publish. `--direct-commit` (raw-FH path only) trades that atomicity
  for RPCs — only safe while nothing consumes the destination.
- Source/destination overlap is refused at prepare and copy.
- Owner/mode/times preserved per the copy options recorded in the
  manifest; missing source attributes are recorded as downgrades, not
  failures.
- Hardlink groups copy sequentially within a shard micro-batch; the
  first row is fully published before the rest `nfs_link` to it.
- Failures and downgrades are separate JSONL streams, drained to
  local files after every shard.

## v1 limitations (deliberate)

- Resync is scan-diff based (`mongoose sync`, design:
  `docs/work-items/MONGOOSE_RESYNC.md`): each pass is a full metadata
  rescan diffed against the previous pass on `(file_type, size,
  mtime, ctime)`, with whole-file recopy on any mismatch — no
  block-level/rsync-style sub-file diff. Rows that failed or tore in
  a pass are forced into the next pass's delta until they succeed.
- Deletions are detected and **recorded**
  (`passes/pass-NNNN/classify/deleted.jsonl`) but never propagated to
  the destination. Renames therefore copy as delete+create.
- A change invisible to the tuple (e.g. content rewritten within the
  server's ctime granularity with identical size) is missed by
  scan-diff; the cutover pass's zero-drift check is the convergence
  gate, not a byte-level verify.
- Single host; no distributed execution, no S3 claims.
- Hardlink fidelity is micro-batch/shard scoped — links that span
  shards (or batches) copy as separate files.
- Directory metadata convergence is shard-scoped, same as the vamoose
  worker: a directory whose children land in a different shard can
  end with a bumped mtime (the migration root itself is re-stamped at
  the end of a complete run).
- NFSv3 via libnfs only; Linux, root, and reserved-port expectations
  are unchanged from vamoose.

## Exit codes

| code | meaning |
|------|---------|
| 0    | success — including a deliberate SIGINT/SIGTERM stop (resume with `mongoose copy`) |
| 1    | error (bad flags, missing index, unreachable export, corrupt shard) |
| 2    | copy completed but recorded per-file failures (see `failures/`) |
