# Quickstart: migrate one filesystem with three hosts

This is the whole story for the common case: a full source export, an empty
destination export, and a few Linux hosts that can reach both. No
configuration management is needed; every host gets the same package and
the same two files under `/etc/vamoose`.

## What you need

- **Hosts**: x86_64 Linux with glibc 2.34 or newer, root via `sudo`, and
  network access to the source NFS server, the destination NFS server, and
  the S3 endpoint. Workers talk NFSv3 directly (libnfs); nothing has to be
  mounted. Workers run as root (reserved ports and ownership preservation).
- **An empty S3 bucket** for coordination, with object **versioning OFF**,
  plus an access key that can list, get, put, and delete objects in it.
  One migration per bucket.
- **The vamoose package** (`vamoose-*.rpm` or `vamoose_*.deb`).
- Port `8443/tcp` open from every host to the one host that will run the
  coordinator.

## 1. Install the package on every host

```bash
sudo dnf install ./vamoose-0.1.0-1.x86_64.rpm      # RHEL / Rocky / Alma
sudo apt install ./vamoose_0.1.0-1_amd64.deb        # Debian / Ubuntu
```

The package installs:

| Path | Purpose |
|---|---|
| `/usr/bin/vamoose` | the command (`worker`, `coord`, `tui`, `status`, `doctor`) |
| `/etc/vamoose/vamoose.toml.example` | the configuration to copy and edit |
| `/etc/vamoose/vamoose.env.example` | the secrets file to copy and edit |
| `/usr/lib/systemd/system/vamoose-worker@.service` | worker service (one or more per host) |
| `/usr/lib/systemd/system/vamoose-coord.service` | coordinator service (one host only) |
| `/usr/share/doc/vamoose/` | this guide, the full configuration reference, licenses |

## 2. Write the configuration once, copy it everywhere

On the first host:

```bash
sudo cp /etc/vamoose/vamoose.toml.example /etc/vamoose/vamoose.toml
sudo vi /etc/vamoose/vamoose.toml
```

Set these lines and leave the rest:

```toml
[run]
bucket   = "vamoose-migration-1"                     # the empty bucket
endpoint = "https://s3.example.com"

[mover]
src_url = "nfs://source.example.com/source-export"
dst_url = "nfs://destination.example.com/destination-export"

[coord]
url = "http://node1.example.com:8443"                # the coordinator host
```

Then the secrets file (mode 0600):

```bash
sudo install -m 0600 /etc/vamoose/vamoose.env.example /etc/vamoose/vamoose.env
sudo vi /etc/vamoose/vamoose.env        # AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
                                        # VAMOOSE_CLUSTER_SECRET=$(openssl rand -hex 32)
                                        # VAMOOSE_ADMIN_TOKEN=$(openssl rand -hex 32)
```

`VAMOOSE_CLUSTER_SECRET` authenticates the workers to the coordinator;
`VAMOOSE_ADMIN_TOKEN` authenticates `vamoose tui` (and anyone else
driving the control API) to it. Both are just random strings that must be
the same on every host.

Copy the two files, unchanged, to the other hosts:

```bash
for h in node2 node3; do
  sudo scp -p /etc/vamoose/{vamoose.toml,vamoose.env} root@$h:/etc/vamoose/
done
```

Check each host before starting anything:

```bash
sudo vamoose doctor
```

`doctor` parses the configuration, reaches the bucket, exercises the
conditional-write primitives the claim protocol depends on, mounts the
source and destination exports over libnfs the way the workers will (hence
`sudo`), and confirms `[prepare] source_root` exists and `dest_root` takes
a file. Every line is PASS, WARN, FAIL, or SKIP; the exit code is non-zero
only on FAIL.

## 3. Start the services

On the coordinator host (the one named in `[coord] url`):

```bash
sudo systemctl enable --now vamoose-coord
```

On every host, including that one:

```bash
sudo systemctl enable --now vamoose-worker@main
```

Nothing copies yet. Workers log `no manifest.json in bucket yet; waiting`
and poll; the coordinator logs that it will seed the job when a manifest
appears. This is the intended idle state — the services can be enabled at
install time and left alone.

## 4. Build the index — this starts the migration

On any one host:

```bash
sudo vamoose prepare
```

It scans the source with the bundled `nfs-walker`, converts the scan to
the canonical index (sharded Parquet under `index/` in the bucket), and
publishes `manifest.json` with a conditional create. The moment the
manifest lands, every enabled worker starts claiming shards and the
coordinator seeds the job under the manifest's run id — there is no
separate "start" command.

While it runs it reports its stage and counters to the bucket
(`prepare/progress.json`), so `sudo vamoose tui` and `vamoose status` on
any host show the scan (files, rate, elapsed), the index (shards
rewritten and uploaded), and the publish step before the job exists.
A large tree spends an hour here; you do not have to watch the terminal
`prepare` runs in.

Things worth knowing:

- **It resumes.** Interrupt it and run it again: a finished scan is not
  redone, uploaded shards are verified (size plus a SHA256 stamped on
  the object) rather than re-sent; shards the conversion had not
  finished are converted again. Use `--fresh`
  to start a new run instead, or `--run-id NAME` to pick the run's name
  (it becomes the coordinator job id). Scan output and checkpoints live
  under `/var/lib/vamoose/prepare/<run_id>/`.
- **Subtrees.** `[prepare] source_root = "/projects/alpha"` migrates that
  path within the source export; `dest_root` is where it lands in the
  destination export. Both default to `/`.
- **Existing scan.** `--scan-dir /path/to/walk.parquet` skips the scan and
  uses an nfs-walker output you already have.
- **One run per bucket.** A bucket that already holds a different
  manifest is refused; use a fresh bucket for a second migration.
- **Scratch space.** `[prepare] work_dir` (default
  `/var/lib/vamoose/prepare`) holds the scan plus the shard or two being
  converted: each converted shard is uploaded as soon as it is written
  and deleted locally once the bucket holds it. Budget about 70 bytes
  per file for the scan (a 600-million-file tree needs ~45 GB) plus the
  shard size. `--keep-index` keeps the converted shards on disk.
- Runs as root because `nfs-walker` binds reserved NFS ports; it reads
  S3 credentials from `/etc/vamoose/vamoose.env` automatically.

## 5. Watch it

From any host:

```bash
sudo vamoose tui
```

The TUI reads `[coord] url` from `vamoose.toml` and the admin token from
`vamoose.env` and shows files/s, throughput, bytes, errors, and every
worker's state. `?` opens the key help; `:` opens the command palette.
Until `prepare` has published the manifest it shows `prepare`'s own
progress instead (scan → index → publish), or how to start one.

Without the coordinator (or from a host that only has S3 access):

```bash
set -a; . /etc/vamoose/vamoose.env; set +a
vamoose status --watch          # shard counts, MB/s, ETA, per-worker heartbeat age
                                # (before the manifest exists: prepare's stage and counters)
```

Logs are in the journal: `journalctl -u vamoose-worker@main -f` and
`journalctl -u vamoose-coord -f`.

## 6. Pause, stop, abort

In the TUI palette (`:`):

| Command | Effect |
|---|---|
| `:stop` (alias of `:pause`) | Workers finish the batch in hand and hold. Claims stay theirs; nothing is lost. |
| `:resume` | Workers continue where they held. |
| `:abort` (alias of `:cancel`) | **Final.** Workers finish the shard in hand, then exit cleanly; the job cannot be resumed through the coordinator. |

Stopping one host is always available and safe: `sudo systemctl stop
vamoose-worker@main`. The worker finishes the batch it is copying, hands
its shard back, and exits 0; another worker picks the shard up at once,
and already copied files are recognized on replay rather than copied
twice. (A worker killed outright — SIGKILL, power loss — is covered too:
its shard is reclaimed once its heartbeat goes stale, about a minute.)

To resume a run after a stop: `sudo systemctl start vamoose-worker@main`.

## 7. Finish

Workers exit 0 when every shard in the manifest is terminal, and
`vamoose status` prints `Terminal: all manifest shards completed`. Per-file
failures, if any, are JSONL objects under the bucket's `failures/` prefix
and are counted in both the TUI and `status`.

Stop application writers (or use immutable source and destination snapshot
roots), then produce independent metadata evidence:

```bash
sudo vamoose verify --writers-stopped
```

The command rescans both exports instead of trusting the migration index. Exit
`0` is a metadata pass, `1` is an incomplete operational failure, `2` means
mismatches, and `3` means mutation made the result inconclusive. The default
artifacts are under `/var/lib/vamoose/verify/<verification-id>/`. V1 does not
verify file content, xattrs, ACLs, or sparse extents; retain the content spot
checks below until sampled/full content modes land.

Exit code 3 from a worker means it fenced itself because a peer took its
claim or its clock jumped. systemd deliberately does not restart it; read
its journal, then `systemctl start` it again. Exit code 4 means the worker
could not reach S3 for a full lease window (a network or DNS outage) and
gave its shard up defensively; systemd restarts it after `RestartSec`, and
it rejoins on its own once the store is back. A peer reclaims the
surrendered shard either way.

## A second migration: another directory, a new bucket (or a new prefix)

One bucket holds one migration per key prefix. Instead of a new bucket you
can set `[run] prefix = "run-2"` on every host: `manifest.json`, `shards/`,
`index/`, the coord's `state/` and everything else then live under
`run-2/`, and the finished run's objects stay where they are. The prefix is
part of the run's identity — `prepare` refuses to resume a run whose prefix
changed — and `doctor` reports `LIST s3://bucket/run-2/`.


Nothing is scripted; this is the full sequence for a subtree. Substitute
your hosts, exports, and paths. The package is already installed from
step 1, so this is config + bucket + services + `prepare`.

**Config** (on the coordinator host, then copy it to every host):

```toml
[run]
bucket   = "vamoose-migration-2"                     # a NEW empty bucket; one migration per bucket
endpoint = "https://s3.example.com"
region   = "us-east-1"

[mover]
src_url = "nfs://source.example.com/source-export"
dst_url = "nfs://destination.example.com/destination-export"

[coord]
url = "http://node1.example.com:8443"
cluster_secret_env = "VAMOOSE_CLUSTER_SECRET"

[shard]
local_scratch = "/dev/shm/vamoose"

[prepare]
source_root = "/projects/beta"                       # path inside the source export
dest_root   = "/beta"                                # path inside the destination export; workers create it
work_dir    = "/var/lib/vamoose/prepare-beta"        # scan + in-flight shards only
```

**Bucket**, once:

```bash
aws --endpoint-url https://s3.example.com s3 mb s3://vamoose-migration-2
aws --endpoint-url https://s3.example.com s3api get-bucket-versioning --bucket vamoose-migration-2   # prints nothing: versioning off
```

**On every host** — services read the config at start, so stop them first:

```bash
sudo systemctl stop vamoose-worker@main vamoose-coord
sudo install -m 0644 /path/to/vamoose.toml /etc/vamoose/vamoose.toml
cd / && sudo vamoose doctor          # PASS on every line; "dest_root absent (workers create it)" is fine
```

**Start** (coordinator on one host, a worker on each):

```bash
sudo systemctl start vamoose-coord vamoose-worker@main     # node1
sudo systemctl start vamoose-worker@main                   # every other host
```

Workers log `no manifest.json in bucket yet; waiting for vamoose prepare`.

**Prepare** — this starts the migration:

```bash
cd / && sudo vamoose prepare
```

It ends with `prepared: s3://vamoose-migration-2/manifest.json (N shards, R rows)`;
workers pick it up within 15 seconds.

**Watch; stop and start a worker if you want to see the hand-off:**

```bash
sudo vamoose tui                                   # or: sudo vamoose status --watch
sudo systemctl stop vamoose-worker@main            # finishes the batch in hand, releases its shard, exits 0
sudo journalctl -u vamoose-worker@main --since -5min -o cat | grep -E 'stop requested|claim released'
sudo systemctl start vamoose-worker@main           # rejoins; a peer has already reclaimed the shard
```

**Confirm it ended:**

```bash
cd / && sudo vamoose status                        # Shards: N total | N completed … Terminal: all manifest shards completed
sudo journalctl -u vamoose-coord --since -1h -o cat | grep 'job ended'
systemctl show -p Result -p ExecMainStatus vamoose-worker@main    # Result=success, ExecMainStatus=0, on every host
```

**Verify metadata with Vamoose** after stopping writers:

```bash
cd / && sudo vamoose verify --writers-stopped
```

**Add V1 content spot checks** from a host with both exports mounted at
`/mnt/source` and `/mnt/destination`:

```bash
sudo diff <(cd /mnt/source/projects/beta && find . | sort) <(cd /mnt/destination/beta && find . | sort) && echo same-tree
sudo du -sb /mnt/source/projects/beta /mnt/destination/beta
cd /mnt/source/projects/beta && sudo find . -type f | shuf -n 500 | while read f; do sudo cmp -s "$f" "/mnt/destination/beta/$f" || echo DIFF "$f"; done; echo cmp-done
aws --endpoint-url https://s3.example.com s3 ls s3://vamoose-migration-2/failures/ | wc -l    # 0
```

Making a small test run last long enough to stop a worker mid-shard: a
lone worker copies a few hundred small files per second, so a 10K-file
tree is over in seconds. Two `[batch]` knobs slow it down — do not use
them for a real migration:

```toml
[mover]
nfs_connections = 2       # NFS connection pairs per worker (default 16)

[batch]
files_budget   = 100      # rows per batch; a stop finishes the batch in hand, so this bounds how far
                          # past SIGTERM the worker copies (default 100000, or 8 GiB, whichever first)
inflight_small = 4        # files under 1 MiB copied concurrently by one worker (default 256)
```

## More than one worker per host

Add a per-instance file and start a second instance; anything not set in
it comes from the shared file:

```bash
sudo tee /etc/vamoose/workers/fast.toml >/dev/null <<'EOT'
[run]
bucket   = "vamoose-migration-1"
endpoint = "https://s3.example.com"
region   = "us-east-1"
[mover]
src_url = "nfs://source.example.com/source-export"
dst_url = "nfs://destination.example.com/destination-export"
nfs_connections = 64
[coord]
url = "http://node1.example.com:8443"
cluster_secret_env = "VAMOOSE_CLUSTER_SECRET"
[shard]
local_scratch = "/var/lib/vamoose/scratch-fast"
EOT
sudo systemctl enable --now vamoose-worker@fast
```

## Backing off

A worker that sees more than `failure_pct_threshold` (5%) failed files in
a shard degrades itself: it stops claiming, waits five minutes, copies one
probe shard, and doubles the wait (to at most 30 minutes) each time the
probe fails too. The journal says `degraded reason=failure_rate_high`.
A MB/s floor (`[backpressure] throughput_floor_mb_s`) can trip the same
state as `throughput_low`; it is off by default because small-file trees
run at a few MB/s on a healthy host. Set it only for workloads whose
per-file size makes a bytes-per-second floor meaningful.

## Where things are

- Configuration search order for every `vamoose` command:
  `--config` / `VAMOOSE_CONFIG`, then `/etc/vamoose/workers/<instance>.toml`
  (services only), then `./vamoose.toml`, then `/etc/vamoose/vamoose.toml`.
- Scan output, canonical shards, and checkpoints:
  `/var/lib/vamoose/prepare/<run_id>/` (`[prepare] work_dir`).
- Full configuration reference with every tunable:
  `/usr/share/doc/vamoose/vamoose.toml.full` (a copy of
  [examples/worker.toml](../examples/worker.toml)).
- Known limitations and the security posture for the beta:
  [BETA_NOTES.md](BETA_NOTES.md).
