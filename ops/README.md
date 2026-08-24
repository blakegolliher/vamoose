# Operations harness

This directory is the tracked operator entry point. It replaces the
site-specific `bigrun/` scripts with one validated run specification and a
provenance-checked release deployment. Real endpoints, hostnames, generated
configs, and credentials remain outside Git.

## Configure one run

```bash
cp ops/run.env.example ops/run.env
$EDITOR ops/run.env
ops/validate-run.sh --require-artifacts
```

`ops/run.env` is a Bash data file and should be treated like local
configuration. It contains references to an AWS profile but never access
keys. Provision the named profile independently on every host.

The external `nfs-walker` binary is pinned by SHA256 in the specification.
The vamoose executables and libnfs come from the separately verified release
bundle. Site addresses, hostnames, profiles, and hook paths remain only in the
ignored `run.env`.

Before any state-changing lifecycle operation, the harness verifies the
archive sidecar and build metadata, rejects dirty bundles, and requires this
checkout to be clean at the exact Git SHA recorded in the release. This binds
the operator scripts and deployed executables to one reviewed source tree.

Each `WORKER_INSTANCES` record describes one process. Multiple records may
target the same host. Connections, concurrency, scratch path, instance name,
and worker identity are rendered directly from that record; there is no
second-worker `sed` mutation.

## Initialize and prepare

```bash
ops/init-run.sh
ops/prepare-run.sh
```

The current claim protocol supports one run per bucket, so `init-run.sh` uses a
fresh bucket as the isolation boundary. It writes `.vamoose-run.json` with the
run ID, run-spec digest, release digest, and endpoints. Every lifecycle command
requires that marker to match. A non-empty unmarked bucket is rejected.

`prepare-run.sh` is a resumable, checkpointed scan → rewrite → upload pipeline:

- A successful walker scan gets a row/size/mtime/SHA256 Parquet-inventory checkpoint. An interrupted
  scan attempt is preserved, and the next invocation starts a new numbered
  attempt; partial output is never mistaken for a complete scan.
- Canonical rewrite writes each shard through a `.partial` file, atomically
  renames it, and records rows, source fingerprint, size, and SHA256 in JSON.
  Resume validates both the checkpoint and canonical Parquet metadata.
- Upload reads the rewrite report instead of parsing logs. It validates remote
  size, run metadata, SHA256, and ETag with HEAD, checkpoints every shard, and
  creates `manifest.json` with a conditional immutable PUT.

Canonical shards remain local through manifest activation so an interrupted
upload can resume without re-running rewrite.

## Render and deploy

```bash
ops/render-worker-configs.sh
ops/deploy-release.sh
```

Rendered configs live under ignored `ops/generated/RUN_ID-HASH/`. The hash
covers the run specification and renderer, making each rendered set immutable.

Deployment accepts only a clean, checksum-valid release bundle. It copies the
archive and verification tools to each unique host, invokes the atomic release
installer, and installs every instance config at
`/etc/vamoose/workers/INSTANCE.toml`. It also installs the rendered
`vamoose-worker@.service` template and reloads systemd. It does not install
into `/usr/local`, overwrite a live binary, or copy credentials.

## Start, observe, and stop workers

```bash
ops/start-workers.sh
ops/worker-status.sh
vamoose status --config /path/to/local.toml --watch
ops/stop-workers.sh
```

`start-workers.sh` first checks for `manifest.json`, starts every explicit
systemd instance, and waits for both an active service and a fresh S3 progress
heartbeat. A stale progress object from an earlier process does not count.
Logs go to journald under `vamoose-worker-INSTANCE`; no unbounded `nohup` log
is created. `stop-workers.sh --hard` is available for reclaim drills and sends
SIGKILL only to the configured units.

Exit code 3 (worker fenced) is in `RestartPreventExitStatus`, leaving the unit
failed and visible for investigation instead of entering a restart loop.

`vamoose status` reports failed claims separately from unclaimed shards,
heartbeat age/staleness, active and terminal rows, aggregate throughput,
current-process failed/fenced counters, watched row rate, and ETA. `--json`
provides the same snapshot to lifecycle automation. Watch mode exits when all
manifest shards reach a terminal claim state.

## Begin, reset, and finalize safely

```bash
ops/reset-run.sh --confirm-run-id "$RUN_ID"
ops/begin-run.sh
ops/finalize-run.sh
```

Reset requires the exact run ID, stops only configured systemd instances,
verifies the destination path remains below the expected kernel mount, and
deletes only children of the configured destination root. `/`, an export root,
path traversal, and overlapping source/destination roots are rejected during
spec validation. Only mutable coordination prefixes are cleared; the identity
marker, manifest, and index remain.

`begin-run.sh` records the timing boundary and supports optional idempotent
pause/resume hooks. If startup fails after pausing external work, an EXIT trap
immediately invokes the resume hook. A successful run leaves the paused marker
for the finalizer, which owns resumption.

`finalize-run.sh` first requires every manifest shard to be terminal. Once
terminal state is proven, its cleanup trap always resumes paused resources,
including when health or data verification fails. It rejects failed claims,
progress-file failures, and any object under `failures/`; closes migration and
verification timings; stops only configured service instances; and invokes the
deterministic sampled verifier. Verification uses raw-byte paths and `lstat`,
checks content for bounded-size regular files, compares mode/owner/time/size,
and compares source and destination symlink targets. Status, provenance,
preparation checkpoints, timings, the sample, and results are retained under
ignored `ops/state/RUN_ID/evidence/`.

This harness deliberately does not reproduce the old `nohup`, `pkill`, log
scraping, tab-separated paths, or unguarded `rm -rf` behavior.
