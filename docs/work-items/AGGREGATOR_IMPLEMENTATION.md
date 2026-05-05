
# Aggregator (`mig-aggr`) — implement the six stubbed subcommands

## State today

The crate is fully skeletoned: `main.rs` wires clap parsing and
dispatch correctly, six module files exist with the right function
signatures, and the binary builds. Every module is `todo!()`. None
of the subcommands do real work.

The aggregator has never been exercised against real data even
though we now have downgrade and progress JSONL files in S3 from
verified runs. This work-item is the spec for filling in the modules.

## Today's S3 layout (the data the aggregator consumes)

After a verified worker run:
s3://<bucket>/manifest.json                                 (1 file)
s3://<bucket>/index/part-r*.parquet                         (N shards)
s3://<bucket>/shards/part-r*.parquet.claim                  (N claims, ETag-locked)
s3://<bucket>/progress/host-<id>.json                       (1 per host, latest)
s3://<bucket>/downgrades/host-<id>.jsonl                    (1 per host, append-only)
s3://<bucket>/failures/host-<id>.jsonl                      (1 per host, append-only)

Sample downgrade line:

```json
{"row_id":1099511627780,"shard":"part-r01-00000.parquet",
 "path_b64":"L3NyYy10ZXN0L20yLXZlcmlmeS9obC1saW5rLTEudHh0",
 "downgrade":"FSID_UNGROUPED",
 "ts":"2026-05-04T14:28:47.352345909Z"}
```

Downgrade kinds in flight: `FSID_UNGROUPED`, `SYMLINK_MODE_NFSV3`,
`SYMLINK_TIME_NFSV3`, `EARLY_EOF`. Failure kinds defined in
`migration_core::records`.

Progress JSON shape: see worker's `heartbeat.rs` — host_id, status,
shards_terminal, throughput_mb_s_1m, last_heartbeat_utc, etc.

## Top-level CLI gaps to fix first

- **`--profile <NAME>`** — pick up named AWS profile from
  `~/.aws/credentials`. Worker has it via `[run].profile` in TOML;
  aggregator should match.
- **`--verify-tls`** (default `true`, set `--no-verify-tls` to skip)
  — VAST lab clusters have self-signed certs, worker does this in
  `verify_tls = false`, aggregator can't talk to them today.
- **`MIG_S3_PROFILE` and `MIG_S3_VERIFY_TLS` env vars** alongside
  the existing `MIG_S3_*` ones.

These changes are in `main.rs` clap and the S3 client construction
helper (likely shared with worker via `migration-core::s3`).

## Per-subcommand specs

### `summary` — rollup of per-host JSONL into one summary

Output (default JSON):

```json
{
  "run_id": "<from manifest>",
  "manifest": {
    "shards": 2,
    "total_rows": 17
  },
  "shards_terminal": 2,
  "shards_in_flight": 0,
  "shards_pending": 0,
  "downgrades": {
    "FSID_UNGROUPED": 1,
    "SYMLINK_MODE_NFSV3": 0,
    "SYMLINK_TIME_NFSV3": 2,
    "EARLY_EOF": 0,
    "_total": 3
  },
  "failures": {
    "by_phase": { "open": 0, "read": 0, "write": 0, "rename": 0, "setattr": 0 },
    "by_kind": { "perm_denied": 0, "size_changed": 0, ... },
    "_total": 0
  },
  "hosts": [
    {
      "host_id": "vastdataubuntu2404-05bfb1cb",
      "throughput_mb_s_1m": 0,
      "shards_processed": 2,
      "downgrades": 3,
      "failures": 0,
      "last_heartbeat_utc": "2026-05-04T14:28:47Z",
      "status": "ok"
    }
  ]
}
```

Implementation outline (≈120 lines):
1. Fetch manifest to learn run_id, total shards, total rows.
2. List `progress/` prefix; fetch every JSON; aggregate into hosts[].
3. List `downgrades/` prefix; stream each JSONL, count by kind.
4. List `failures/` prefix; stream each JSONL, count by phase + kind.
5. List `shards/` prefix; count `.claim` keys to derive
   shards-terminal vs in-flight.
6. Format JSON or human-readable text per `--format`.

`--format` options: `json` (default), `text` (operator-readable
with totals and warnings), `prometheus` (delegate to `metrics`?).

### `inspect <shard>` — drill into one shard

Per-shard detail. Useful when `summary` shows downgrades and the
operator wants to see which paths.

Output:

```json
{
  "shard": "part-r01-00000.parquet",
  "claim": {
    "host_id": "...",
    "claimed_at_utc": "...",
    "etag": "...",
    "terminal": true,
    "terminal_at_utc": "..."
  },
  "downgrades": [
    {"row_id": 1099511627780, "path": "/src-test/m2-verify/hl-link-1.txt",
     "downgrade": "FSID_UNGROUPED", "ts": "..."},
    ...
  ],
  "failures": []
}
```

Fetch `shards/<shard>.claim`, scan all `downgrades/` and `failures/`
JSONL, filter by `shard` field, decode `path_b64` to UTF-8 (replace
invalid bytes with U+FFFD for display only — keep the b64 in the
record).

### `verify` — manifest vs claims/progress consistency

Different from `manual-verify.sh` (which is per-file source-vs-dest
content/metadata diff). This subcommand checks the orchestration
layer:

- Every shard in the manifest has a claim or is unclaimed.
- Every claim's `host_id` appears in `progress/`.
- No claim is older than `lease_timeout_sec * 2` without being
  terminal (suggests a fenced worker that never released).
- Sum of per-host shards_processed == manifest.shards (when run
  is complete).
- No row_id appears in both downgrades and failures (would
  indicate double-counting).

Outputs warnings/errors with severity. Exit non-zero if any error.

### `clean-partials` — remove `.partial` files left by fenced workers

`.partial` filenames are `.<basename>.<host>.<pid>.partial`. After
a fenced worker exits, these can remain if the fence tripped after
write but before rename.

Logic:
1. Scan all destination directories (need NFS access — same libnfs
   pool the worker uses, or recursively walk via sftp/ssh, TBD).
2. For each `.partial` file, parse host and pid from the name.
3. Cross-reference with `progress/host-<host>.json`:
   - If host doesn't exist → orphaned, candidate for deletion.
   - If host exists and pid in current `progress.pids[]` → live, skip.
   - If host exists but pid not in `pids[]` → orphaned, candidate.
4. With `--dry-run`, print the list. Without, delete.

This is the most complex subcommand because it requires NFS access
in addition to S3. May warrant deferring until last.

### `metrics` — Prometheus exporter on a port

HTTP endpoint exposing the same data as `summary` in Prometheus
exposition format. Useful for dashboards monitoring an in-progress
run.

Recommended approach: implement as `summary` underneath, format
differently. Same data fetch, different output.

Endpoint defaults to `0.0.0.0:9090` per the existing CLI.

### `watch` — TUI dashboard (live updating)

ratatui-based TUI showing progress as a run executes. Lower
priority than the others since `summary` + watch loop in shell
covers most operator needs short term.

Likely defer until the other five are working.

## Order of implementation

1. **CLI gaps** (`--profile`, `--verify-tls`, env vars). 30 min.
2. **`summary`**. The keystone. Most other subcommands reuse its
   data-fetch helpers.
3. **`inspect`**. Trivial after summary's helpers exist.
4. **`verify`**. Builds on summary + inspect.
5. **`metrics`**. Reformat summary's output.
6. **`clean-partials`**. Needs NFS access; design it carefully.
7. **`watch`**. Defer to last.

Total scope: 2-4 days of focused work.

## Tests

Each subcommand needs:
- Unit tests against synthetic JSONL fixtures (no S3 round-trip).
- An integration test that mocks the S3 layer (likely via
  `aws-sdk-s3-mock` or a httpmock-backed endpoint).
- One smoke test that runs against a real bucket from a recent
  worker run, gated `#[ignore]` like the libnfs FFI smoke test.

## What you can use as a test fixture today

A real run from 2026-05-04 produced this in `s3://vamoose/`:
manifest.json                                  17 rows, 2 shards
index/part-r00-00000.parquet                   0 entries
index/part-r01-00000.parquet                   17 entries
shards/part-r00-00000.parquet.claim
shards/part-r01-00000.parquet.claim
downgrades/host-vastdataubuntu2404-05bfb1cb.jsonl

1× FSID_UNGROUPED on hl-link-1.txt
2× SYMLINK_TIME_NFSV3 on link-rel.bin and link-abs.bin
progress/host-vastdataubuntu2404-05bfb1cb.json
failures/   (empty)


Worth saving a snapshot for the `#[ignore]` smoke test before the
bucket gets reset.

## Out of scope

- A web UI. Anything beyond `metrics` Prometheus exposition is too
  ambitious for this work-item.
- Aggregator-side modifications to manifest, claims, downgrades,
  failures, or progress. Aggregator is read-only against S3 except
  for `clean-partials` which deletes `.partial` files on NFS.
- Cross-run aggregation. One run at a time per `--bucket`.

