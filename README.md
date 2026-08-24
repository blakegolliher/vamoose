# vamoose

Distributed NFS-to-NFS migration with S3-based shard coordination and an
optional REST/SSE operator control plane.

Vamoose workers copy from immutable Parquet indexes without a required central
service. S3 conditional operations arbitrate shard ownership. Operators can add
`vamoose coord` and `vamoose tui` for live state and controls without moving
claim authority out of the data plane.

## Components

- `migration-core` — canonical Parquet and S3 record formats, run layout,
  claim protocol, shard reader, and S3 client.
- `migration-mover` — NFSv3/libnfs copy engine. The default path uses a
  synchronous `MultiPool`; the opt-in bucketed path pipelines regular-file I/O
  through async libnfs. Special file types keep their dedicated sync paths.
- `migration-worker` — claim/reclaim lifecycle, heartbeat and self-fencing,
  shard processing, progress, and optional coordinator reporting.
- `migration-control-protocol` — versioned control-plane wire schema and pure
  snapshot reducer. It is independent of the coordinator runtime and the data
  plane.
- `migration-coord` — optional S3-backed coordinator: lease, replay, event log,
  snapshots, audit, archival, authentication, REST, and SSE.
- `migration-tui` — optional ratatui dashboard over coordinator REST and SSE.
- `migration-aggr` — standalone `mig-aggr`; `clean-partials` is implemented,
  while its observability commands currently fail safely as unimplemented.
- `mig-walker-rewrite` — resumable converter from legacy walker Parquet to the
  canonical schema, with atomic shard activation and JSON checkpoints.
- `vamoose-cli` — the unified `vamoose` entry point and lifecycle boundary.

There is no implemented custom io_uring mover, NFSv4.2 server-side COPY, or
kernel `copy_file_range` strategy. `Strategy::LibnfsIoUring` remains only as a
compatibility label for regular-file libnfs outcomes.

## Build

Requires Rust 1.91.1 or newer and the libnfs system library.

```bash
# Debian/Ubuntu
sudo apt install libnfs-dev pkg-config

# RHEL/Rocky
sudo dnf install libnfs-devel pkgconf-pkg-config

cargo build --release
```

Binaries land in `target/release/`. The unified entry point is
`target/release/vamoose`; use `vamoose --help` and
`vamoose <command> --help` for the authoritative CLI syntax.

## Command status

| Command | Status |
|---|---|
| `vamoose worker` | Implemented migration worker |
| `vamoose status` | Implemented text/JSON S3 status, one-shot or watched |
| `vamoose doctor` | Implemented configuration, S3, NFS, and permission checks |
| `vamoose init` | Implemented S3 layout marker initialization |
| `vamoose prepare` | Implemented scan (bundled `nfs-walker`) → canonical index → verified upload → `manifest.json` |
| `vamoose coord` | Implemented optional REST/SSE coordinator |
| `vamoose tui` | Implemented terminal dashboard and controls |
| `vamoose walker` | Stub; `vamoose prepare` runs the scan |
| `vamoose rewrite` | Stub; `vamoose prepare` runs the rewrite |
| `vamoose aggr` | Stub; use `vamoose status`, the TUI, or standalone `mig-aggr` |
| `vamoose run` | Stub; script the pipeline explicitly |

Standalone `mig-aggr clean-partials` is real and dry-run by default. Its
`watch`, `summary`, `metrics`, `inspect`, and `verify` commands return clear
unimplemented errors.

The coordinator seeds its control-plane job from the bucket's `manifest.json`
(job id = the manifest's run id), so a fresh deployment needs no job-create
step; `vamoose coord --seed-job` remains for an explicit id. Workers register
against the same id by default and wait, rather than fail, while the job is
not seeded yet.

## Quick start

The operator story is [docs/QUICKSTART.md](docs/QUICKSTART.md): install the
package on each host, copy one configuration file and one secrets file to
`/etc/vamoose`, enable `vamoose-coord` on one host and `vamoose-worker@main`
on all of them, build the index, and drive the run from `vamoose tui`.

```bash
sudo dnf install ./vamoose-*.rpm                      # every host
sudo cp /etc/vamoose/vamoose.toml.example /etc/vamoose/vamoose.toml   # edit, copy to all hosts
sudo install -m 0600 /etc/vamoose/vamoose.env.example /etc/vamoose/vamoose.env
sudo systemctl enable --now vamoose-coord             # one host
sudo systemctl enable --now vamoose-worker@main       # every host; idles until the index exists
sudo vamoose prepare                                  # one host: scan -> index -> manifest; the run starts
sudo vamoose tui                                      # any host: watch, :stop, :resume, :abort
```

`vamoose prepare` runs the bundled `nfs-walker` (packages built with
`NFS_WALKER_BIN=`), `mig-walker-rewrite`, and a verified upload with a
conditional-create `manifest.json`, checkpointing every stage so it can be
re-run. The tracked [`ops/`](ops/README.md) harness remains the advanced,
fully scripted lifecycle (validated run specification, provenance-checked
bundle deployment over SSH, timing, reset, and sampled verification) for
sites that want that level of control.

The TUI exposes pause (`:stop`), resume, cancel (`:abort`), drain, and
retry-failed through its command palette. Pause takes effect at the next batch
boundary and keeps every claim; cancel is final and lets each worker finish
the shard in hand. Drain maps to paused state and retry-failed is audit-only;
neither is a separate worker execution mode yet. `NO_COLOR=1` and
`VAMOOSE_THEME=light` are supported. Workers without `[coord]` continue to
operate through S3 claims alone.

## Configuration

One file, `/etc/vamoose/vamoose.toml`, configures every command and service
on a host. Commands look for it in this order: `--config` / `VAMOOSE_CONFIG`,
`/etc/vamoose/workers/<instance>.toml` when `VAMOOSE_INSTANCE` is set (the
systemd template exports it), `./vamoose.toml`, then
`/etc/vamoose/vamoose.toml`.

`examples/vamoose.toml` is the slim file the packages install as
`vamoose.toml.example`; `examples/worker.toml` is the full reference with
every tunable. The canonical format is:

```toml
[run]           # bucket, endpoint, region; optional profile, verify_tls
[mover]         # src_url, dst_url, and tuning
[coord]         # optional: url + job wiring for workers, listen/TLS/tokens for the daemon
[worker] [shard] [batch] [copy] [backpressure]   # optional; production defaults
```

Only `[run]` and `[mover]` are required to run a worker. Control-only commands
need only `[run]`. Optional unified-CLI sections are `[nfs]`, `[walker]`,
`[aggr]`, and `[logging]`. The older `[global]`/`[s3]` vamoose format remains
accepted as a compatibility input; mixed canonical and compatibility roots are
rejected.

Historical mover fields remain parseable, but do not enable removed or
unimplemented strategies. See [DESIGN.md](DESIGN.md#configuration) and the
comments in [examples/worker.toml](examples/worker.toml) for current semantics.

## Documentation

- [docs/QUICKSTART.md](docs/QUICKSTART.md) — install, configure, run, and stop a migration
- [docs/RELEASES.md](docs/RELEASES.md) — building packages and release bundles
- [DESIGN.md](DESIGN.md) — concise as-built system architecture
- [docs/CONTROL_PLANE.md](docs/CONTROL_PLANE.md) — current control-plane ownership and invariants
- [docs/CLAIM_PROTOCOL.md](docs/CLAIM_PROTOCOL.md) — authoritative S3 claim protocol
- [docs/CORRECTNESS_RULES.md](docs/CORRECTNESS_RULES.md) — cross-cutting correctness rules
- [SCHEMA_CONTRACT.md](SCHEMA_CONTRACT.md) — mirrored Parquet schema contract
- [docs/BETA_NOTES.md](docs/BETA_NOTES.md) — operator limitations and security posture
- [docs/NEXT.md](docs/NEXT.md) — current follow-up work
- [docs/HANDOFF.md](docs/HANDOFF.md) — stable handoff and document map
- [docs/COORD_PLAN.md](docs/COORD_PLAN.md) — historical coordinator delivery plan
- [docs/work-items/](docs/work-items/) and [docs/baselines/](docs/baselines/) — historical implementation and verification records

## License

Licensed under the GNU Affero General Public License v3.0 (AGPL-3.0). See
[LICENSE](LICENSE).

The mover dynamically links libnfs (LGPL-2.1-or-later); do not statically link
it. Third-party licenses are tracked in
[THIRD_PARTY_LICENSES.md](THIRD_PARTY_LICENSES.md).

Copyright (C) 2026 Blake Golliher
