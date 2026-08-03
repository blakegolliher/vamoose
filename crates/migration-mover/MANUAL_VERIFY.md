# M2 Manual Verification

The M2 mover is single-threaded, single-file, libnfs-only. We do not
ship an automated end-to-end test against libnfs because the
scaffolding cost is high and the only signal that matters is "does it
behave correctly against real VAST." This document is the manual
recipe.

A passing run of this recipe is a hard requirement to declare M2 done.
Record results in `M2_NOTES.md`.

## Test tree

Build a small tree on the source NFS export. ~1000 files total, mixed
to exercise each strategy at least a few times.

Recommended layout:

```
test-tree/
├── empty.bin                       # 0 bytes
├── tiny.bin                        # 1 byte
├── small/                          # ~100 files of ~1 KiB each
├── medium/                         # ~50 files of ~1 MiB each
├── large/                          # ~5 files of ~100 MiB each
├── deep/a/b/c/d/e/buried.bin       # tests mkdir-on-demand path
├── modes/                          # files with 0644, 0755, 0600, 0640, ...
│                                   # plus a 4755 root-owned file (see [5])
├── owners/                         # mix of uid/gid (run as root for this)
├── links/
│   ├── target.bin                  # nlink-2 source
│   ├── hardlink-to-target.bin      # nlink-2, same inode as target.bin
│   ├── symlink-rel -> ../target.bin
│   └── symlink-abs -> /test-tree/links/target.bin
└── unicode/
    └── 日本語ファイル.bin          # non-ASCII path bytes
```

Build with whatever scripting is convenient. The shape matters; the
exact file count doesn't.

## Recipe

### 1. Walk the source

```bash
nfs-walker --src nfs://<src>/<export>/test-tree \
           --out s3://<bucket>/test-run/index/
# Plus whatever nfs-walker needs to produce manifest.json
```

Confirm the produced parquet has `row_id`, `path`, `size`, `mode`,
`file_type` at minimum.

### 2. Configure the worker

`/etc/mig/worker.toml` — copy `examples/worker.toml`, set:

```toml
[run]
bucket   = "<bucket>"
endpoint = "https://<vast-s3-endpoint>"
region   = "us-east-1"

[shard]
local_scratch = "/var/lib/mig/scratch"   # tmpfs preferred

[mover]
src_url = "nfs://<src>/<export>"         # only used as a fallback;
dst_url = "nfs://<dst>/<export>"         # manifest URLs win

[copy]
preserve_owner            = true
preserve_mode             = true
preserve_times            = true
require_chown_capability  = true   # set false if not running as root
```

### 3. Run a single worker

```bash
RUST_LOG=info,mig_worker=debug,migration_mover=debug \
    cargo run --release --bin mig-worker -- --config /etc/mig/worker.toml
```

The worker should:

- Mount source and dest exports once at startup.
- Fail fast if `preserve_owner=true` and CAP_CHOWN missing
  (unless `require_chown_capability=false`).
- Claim the single shard.
- Walk every row, copying files one at a time.
- Print a per-file warning for any failure with phase + errno.
- Mark the shard `Completed` and exit when done.

Watch for these log signals:

- `shard claimed`
- `shard opened total_rows=...`
- One `file failed` line per failure (none expected on a clean tree)
- Final `mig-worker shutting down` on clean exit

### 4. Verify content + metadata + links

```bash
scripts/manual-verify.sh /mnt/src/test-tree /mnt/dst/test-tree
```

Exits zero on success. Inspect the diff hunks for any failures.

The script covers:

- **[4]** sha256 of every regular file matches.
- **[5]** mode + uid + gid + mtime match (atime intentionally not
  checked — preserve_atime is best-effort). Include a `4755`
  root-owned file in the test tree (`modes/`); post-migration
  `stat -c '%a %u:%g' <dst>` must show `4755` — setuid must survive
  the chown-before-chmod attr order (F08; NFSv3 kill-priv strips
  S_ISUID/S_ISGID if ownership changes after chmod).
- **[6]** symlink targets match byte-for-byte.
- **[7]** hardlink groupings match — inode numbers will differ between
  filesystems, but the partitioning of paths into groups must be
  identical.

### 5. Spot-check residual `.partial` files

A clean run leaves no `.partial` files on the dest. Confirm:

```bash
find /mnt/dst/test-tree -name '.*.partial' -print
# expected: nothing
```

Any `.partial` survivors point to a worker that crashed or fenced
mid-copy. M2 does not implement automatic cleanup; that's a separate
follow-up task before M3 (`mig-aggr clean-partials`).

## Crash durability (F09) — rig drill, not a test claim

Both copy paths issue UNSTABLE WRITEs followed by one whole-file NFS
COMMIT before the commit-point rename (DESIGN.md "Mover behavior").
No automated test can verify what the *server* does with a COMMIT —
proving it requires a kill-the-server drill: power-fail (or force a
failover of) the destination filer mid-run, bring it back, and re-run
the full step-4 content verification over everything that had already
been renamed to its final name. Every published (post-rename) file
must verify byte-for-byte; `.partial` survivors are expected and fine.
If you run the drill, record the result in `M2_NOTES.md` like any
other verification incident.

## Recording results

Append to `M2_NOTES.md` at the workspace root:

- Date of run.
- libnfs version (`pkg-config --modversion libnfs`).
- Source + dest VAST versions if known.
- Tree size (file count, total bytes).
- Wall-clock duration of the worker.
- Any per-file failures observed (phase + errno + a sample path).
- Any libnfs quirks worth M3 knowing about.
