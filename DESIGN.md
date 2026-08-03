# Vamoose as-built architecture

This document describes the system implemented in this repository. Current
source and tests take precedence if this overview drifts; detailed contracts
are linked rather than repeated here. Milestone notes, work-item prompts, and
the delivered coordinator plan are historical records, not current
specifications.

Vamoose migrates an immutable, sharded file index from one NFS export to
another. Workers use S3 for shard ownership and durable progress. A separate,
optional control plane supplies live operator state and commands without
becoming part of the data-plane ownership protocol.

## Responsibilities

The implementation separates four concerns:

- **Data plane:** `migration-core`, `migration-mover`, and
  `migration-worker` read immutable Parquet shards, acquire S3 claims, and copy
  filesystem objects through libnfs.
- **Claim coordination:** the S3 objects owned by `migration-core` arbitrate
  shard ownership. This path has no required coordinator service or database.
- **Operator control plane:** `migration-control-protocol`,
  `migration-coord`, and `migration-tui` provide an optional REST/SSE view,
  commands, durable event history, and a terminal UI. They do not replace S3
  claims.
- **Observability and operations:** worker progress/failure/downgrade objects,
  coordinator snapshots/events/audit records, unified-CLI logging, and the
  limited `migration-aggr` utility expose different views of the same run.

At a high level:

```text
 nfs-walker             S3 run bucket                     worker fleet
     |          manifest + immutable parquet shards            |
     +--------> index/ and manifest.json ---------------------->|
                        shard claims, progress, records <------>|
                                                                 |
 source NFS <---------------- libnfs copy --------------------> dest NFS

                         optional operator plane
 worker HTTP reporting ---> vamoose coord <--- REST/SSE ---> vamoose tui
                                  |
                          S3 event/snapshot/audit state
```

## Crate ownership and dependencies

| Crate | Owns |
|---|---|
| `migration-core` | Run manifest and record formats, Parquet schema/reader, S3 run layout and client, claim protocol, fencing primitives |
| `migration-mover` | NFSv3/libnfs copy execution, file-kind selection, POSIX attribute application, partial-file commit, sync and bucketed-async paths |
| `migration-worker` | Configuration for the worker runtime, claim/reclaim lifecycle, heartbeat and self-fencing, shard processing, backpressure, optional coordinator client |
| `migration-control-protocol` | Versioned control-plane wire types, REST/SSE bodies, snapshots, events, commands, validation, and the pure `Snapshot::apply` reducer |
| `migration-coord` | Coordinator store, S3 layout, event chunks, snapshots, replay, audit, lease, archival, HTTP/SSE, authentication, and runtime lifecycle |
| `migration-tui` | REST bootstrap, SSE reconnect/resume, client-side state and histories, input handling, terminal lifecycle, and deterministic ratatui views |
| `migration-aggr` | The standalone `mig-aggr` binary; only `clean-partials` is implemented |
| `mig-walker-rewrite` | Temporary conversion from the walker repository's legacy Parquet output to the canonical schema |
| `vamoose-cli` | The `vamoose` process boundary: CLI parsing/dispatch, configuration composition, logging startup/shutdown, and final exit status |

The important control-plane boundary is:

```text
 migration-control-protocol ---> migration-worker
             |                 --> migration-coord
             +-------------------> migration-tui

 migration-core ---> migration-mover ---> migration-worker
        +--------------------------------> migration-coord
```

Arrows point from a dependency to its consumer.

`migration-worker` and `migration-tui` depend directly on the protocol crate;
neither has a normal dependency on `migration-coord`. The TUI also has no
normal dependency on `migration-core` or the AWS SDK. In-process integration
tests may use `migration-coord` as a dev-dependency. See
[the control-plane reference](docs/CONTROL_PLANE.md) for the runtime boundary
and durability invariants.

## Immutable migration input

An external `nfs-walker` scan produces sharded Parquet. Until the walker emits
the canonical schema directly, `mig-walker-rewrite` converts its output. The
canonical column and metadata contract is mirrored with the walker repository
in [SCHEMA_CONTRACT.md](SCHEMA_CONTRACT.md).

Each run bucket contains an immutable `manifest.json` and immutable objects
under `index/`. The manifest identifies the format version, run, source and
destination endpoints (including logical roots), copy options, shard keys,
row/byte counts, and each uploaded shard's ETag. A worker:

1. loads the manifest and rejects an unsupported format version;
2. rejects overlapping source and destination endpoints before any write;
3. downloads a claimed shard to local scratch;
4. verifies the downloaded object's ETag against the manifest; and
5. mmaps the local Parquet shard and consumes canonical rows in materialized
   `row_id` order.

The manifest and index are input truth, not mutable work queues. Claims,
progress, failures, downgrades, and coordinator state live under disjoint S3
prefixes.

## S3 claim protocol and worker lifecycle

The detailed as-built ownership protocol is
[docs/CLAIM_PROTOCOL.md](docs/CLAIM_PROTOCOL.md). Its essential properties are:

- acquisition is `PUT If-None-Match: *`;
- a held claim's ETag is the ownership token and remains stable;
- heartbeat refresh is a read/compare, not a rewrite;
- reclaim and terminalization use conditional delete followed by conditional
  create; and
- a worker that cannot prove ownership trips its shared fence before another
  commit.

The worker scans the manifest's shards, skips immutable terminal claims,
acquires available work, and reclaims only after the lease and progress-
liveness policy permit it. It writes per-host heartbeat/progress records while
processing. Shard-fatal data errors can produce an immutable failed marker;
worker-local failures release work so a peer can retry it. Successful
processing terminates the claim as completed.

Claim execution is deliberately at-least-once around crash windows. File
publication is designed to make a replay safe: regular files publish with an
atomic rename, while hardlink and symlink replay accepts an existing target
only after verifying it matches the already-committed result. The fence is
checked at commit points. See [docs/CORRECTNESS_RULES.md](docs/CORRECTNESS_RULES.md)
for the cross-cutting rules.

When `[coord]` is configured, a background worker client registers, sends
heartbeats and buffered events, and consumes the coordinator's control mode.
Client sequence high-water marks make current-worker event retries converge.
Without `[coord]`, the same worker continues in S3-only mode; claim correctness
does not depend on the optional service.

## Mover behavior

Vamoose's protocol baseline is NFSv3 over dynamically linked libnfs. There is
no custom io_uring mover, NFSv4.2 server-side COPY, or kernel
`copy_file_range` execution path.

### Regular files

Two regular-file implementations exist:

1. **Synchronous MultiPool path (default).** A long-lived `MultiPool` owns
   source/destination libnfs context pairs. Each file runs on Tokio's blocking
   pool and streams libnfs READ to WRITE through a 1 MiB buffer. Concurrency is
   across files.
2. **Bucketed asynchronous path (opt-in).** `[mover]
   use_bucketed_pool = true` or `vamoose worker --use-bucketed-pool` selects
   source/destination async context pairs for small, medium, and large files.
   Regular-file data uses the bounded pipelined copy implementation. Special
   rows still delegate to the synchronous mover.

The public outcome label `Strategy::LibnfsIoUring` is a retained compatibility
name for regular-file libnfs work; it does not describe an io_uring
implementation.

For both regular-file paths, the destination is a hidden sibling named
`.<base>.<host>.<pid>.partial`. Data is made stable with a whole-file NFS
COMMIT (`fsync` in the libnfs API) before publication. Owner, mode, and times
are applied in the order `chown -> chmod -> utimes`; owner precedes mode so
NFSv3 kill-priv behavior cannot strip freshly applied setuid/setgid bits. The
fence is checked immediately before the atomic rename publishes the final
path. A commit failure or fence trip leaves the partial unrenamed.

The bucketed path brackets a copy with source metadata and records a torn-copy
downgrade if the source changed while the file was copied. The default sync
path does not currently perform that bracket.

### Other rows and metadata

- Empty regular files use create, attributes, fence check, and rename without
  a data loop.
- Symlinks preserve raw target bytes. NFSv3 limitations in link metadata are
  recorded as downgrades. Replay compares an existing destination target.
- Hardlink candidates are grouped by `(fsid, inode)` when possible and copied
  sequentially within a micro-batch: the first member is copied, then later
  members link to its final path. Missing `fsid` falls back to inode-only
  grouping with a downgrade. Fidelity across batch or shard boundaries is a
  known limitation.
- Directory attributes are applied deepest-first after non-directory rows in
  the same batch. Later work in another batch or shard can restamp a parent
  directory's mtime.
- FIFOs, sockets, and device rows are skipped by the mover.

Failures and fidelity downgrades are separate immutable JSONL objects keyed by
host, shard, and claim epoch. A failed row does not inflate bytes-moved
accounting; a committed early-EOF or torn copy remains visible as a downgrade.

### Batching and backpressure

Rows are accumulated in materialized order until either the configured byte
budget, file budget, or shard end closes a micro-batch. Per-size inflight
limits bound concurrent file work inside it. After each completed shard, the
worker evaluates its failure percentage and measured throughput. An unhealthy
result stops ordinary new claims; after a cooldown, one probe claim is allowed.
A healthy probe reopens the gate, while another unhealthy result extends the
cooldown up to its cap.

## Optional control plane

`migration-control-protocol` is the canonical control-plane contract. Version
1 snapshot and event shapes, serde behavior, identifiers, commands, request/
response bodies, and the deterministic reducer live there. It has no
dependency on `migration-core`, the coordinator, Tokio, Axum, or an AWS SDK.

`migration-coord` owns everything stateful around that contract: single-writer
lease acquisition and refresh, persisted event chunks, snapshots, replay,
audit allocation, terminal-job archival, HTTP/SSE, authentication, and runtime
ticks. It re-exports the protocol schema at `migration_coord::schema` for
source compatibility. It never owns or mutates worker shard claims.

`migration-tui` bootstraps from REST, then follows SSE with resume and resync
handling. It uses the protocol reducer directly so server replay and client
state share transition semantics. Live broadcast caps reduce display traffic;
the coordinator's reducer and durable event log still see every accepted
event. See [docs/CONTROL_PLANE.md](docs/CONTROL_PLANE.md) for the module map and
concurrency/durability details.

No production CLI or REST route currently creates/imports a control-plane job.
A fresh coordinator starts with an empty job registry, and worker registration
requires the configured job to exist. The runtime, replay, command, worker, and
TUI paths are implemented and tested once `JobCreated` state is present; an
operator provisioning workflow is deferred.

## Configuration

The canonical operator input is the existing worker-shaped TOML:

```toml
[run]          # bucket, endpoint, region, profile, verify_tls
[worker]
[shard]
[mover]
[batch]
[copy]
[backpressure]
[coord]        # optional worker-to-coordinator connection
```

The full shape and defaults are demonstrated by
[examples/worker.toml](examples/worker.toml). The same file is accepted by
standalone `mig-worker` and by every configuration-consuming `vamoose`
subcommand. Control-only commands need only `[run]`; `vamoose worker` also
requires the worker-specific sections. Unified-CLI-only `[nfs]`, `[walker]`,
`[aggr]`, and `[logging]` sections are optional additions, and standalone
`mig-worker` ignores them through Serde's normal unknown-field behavior.

The older `[global]` plus `[s3]` vamoose shape remains a compatibility input.
The CLI normalizes either format to bucket, endpoint, region, profile, and
`verify_tls`, rejects a file that mixes the two format roots, and does not
fall through to another parser after a malformed input. Compatibility worker
projection retains its established defaults and requires `[nfs]`.

Historical mover fields such as `strategy_default`, `pipeline_depth`,
`io_uring_queue_depth`, `fixed_buffer_count`, `fixed_buffer_size`, and
`server_side_copy` remain accepted so operator files do not break. They do not
select or tune an unimplemented strategy. `use_bucketed_pool` and
`rpc_timeout_ms` are active mover settings.

Logging policy also preserves the source format's behavior: canonical input
without `[logging]` uses the minimal fallback subscriber; compatibility input
without it uses the established standard defaults; explicit `[logging]` works
with either. The TUI remains quiet on stderr and does not start the S3 log
uploader. The `vamoose` process boundary owns command dispatch, orderly
logging shutdown, and final exit status.

## Command and aggregation status

The unified CLI currently implements `worker`, `status`, `doctor`, `init`,
`coord`, and `tui`. Its `walker`, `rewrite`, `aggr`, and end-to-end `run`
subcommands retain their command-line shapes but fail safely with actionable
messages. Operators invoke `nfs-walker` and `mig-walker-rewrite` directly for
index preparation.

The standalone `mig-aggr` binary is not a complete observability sidecar.
`clean-partials` is implemented: it scans a locally mounted destination for
mover-shaped partial files, defaults to dry-run, requires `--delete` to
remove them, and gates deletion on claim liveness unless `--force` is given.
`watch`, `summary`, `metrics`, `inspect`, and `verify` return explicit
unimplemented errors rather than panicking. Current live observability comes
from `vamoose status` or the optional coordinator and TUI.

The control surface also has two deliberate version-1 limits: `drain` is
encoded as a paused job with reason `drain`, so the worker currently observes
pause rather than a distinct drain mode; `retry-failed` records an accepted
audit command but has no retry event or worker queue behind it.

## Current limitations and deferred work

The authoritative current list is [docs/NEXT.md](docs/NEXT.md), with operator
impact summarized in [docs/BETA_NOTES.md](docs/BETA_NOTES.md). Important
boundaries include:

- several libnfs and reclaim guarantees still require the recorded VAST
  hardware verification pass;
- hardlink fidelity and directory-attribute ordering are batch/shard scoped;
- multi-pass migration and cross-boundary hardlink reconciliation are not
  implemented;
- walker/rewrite/run composition and most aggregation commands remain stubs;
- walker-side xattr capture is not yet available;
- the S3 data-plane layout represents one run at the bucket root;
- archive restore, scoped control-plane credentials, and an atomic coordinator
  snapshot boundary for TUI bootstrap are deferred;
- fresh-deployment control-plane job provisioning is not implemented;
- distinct drain execution and retry-failed queueing are not implemented; and
- compatibility configuration and control-plane re-exports remain until a
  separately approved breaking cleanup.

No dormant executable scaffolding is retained for io_uring, NFSv4.2 COPY, or
kernel `copy_file_range`. A future strategy effort must begin with a measured,
reviewed implementation rather than treating those names as existing support.

## Documentation map

- [docs/CLAIM_PROTOCOL.md](docs/CLAIM_PROTOCOL.md): detailed S3 claim protocol
- [docs/CONTROL_PLANE.md](docs/CONTROL_PLANE.md): control-plane architecture and invariants
- [docs/CORRECTNESS_RULES.md](docs/CORRECTNESS_RULES.md): cross-cutting correctness rules
- [SCHEMA_CONTRACT.md](SCHEMA_CONTRACT.md): mirrored Parquet schema contract
- [docs/BETA_NOTES.md](docs/BETA_NOTES.md): operator limitations and security posture
- [docs/NEXT.md](docs/NEXT.md): remaining work
- [docs/COORD_PLAN.md](docs/COORD_PLAN.md): historical coordinator delivery plan

## Toolchain and licensing

The workspace MSRV is Rust 1.91.1 and the edition is 2021. The mover
dynamically links libnfs; static linking is not supported by the repository's
licensing policy. Workspace code is AGPL-3.0-only. See
[THIRD_PARTY_LICENSES.md](THIRD_PARTY_LICENSES.md) for the generated dependency
inventory.
