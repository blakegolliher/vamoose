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
- `mig-walker-rewrite` — temporary converter from legacy walker Parquet to the
  canonical schema.
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
| `vamoose status` | Implemented one-shot or watched S3 status |
| `vamoose doctor` | Implemented configuration, S3, NFS, and permission checks |
| `vamoose init` | Implemented S3 layout marker initialization |
| `vamoose coord` | Implemented optional REST/SSE coordinator |
| `vamoose tui` | Implemented terminal dashboard and controls |
| `vamoose walker` | Stub; invoke `nfs-walker` directly |
| `vamoose rewrite` | Stub; invoke `mig-walker-rewrite` directly |
| `vamoose aggr` | Stub; use `vamoose status`, the TUI, or standalone `mig-aggr` |
| `vamoose run` | Stub; script the pipeline explicitly |

Standalone `mig-aggr clean-partials` is real and dry-run by default. Its
`watch`, `summary`, `metrics`, `inspect`, and `verify` commands return clear
unimplemented errors.

The coordinator daemon, worker client, REST/SSE surface, and TUI are
implemented, but a fresh coordinator has no supported job-create/import
workflow yet. It starts with an empty job registry and rejects registration for
an unknown job. This provisioning gap is tracked in
[docs/NEXT.md](docs/NEXT.md).

## Quick start

See `scripts/manual-verify.sh` and the per-crate notes for the end-to-end
cookbook. At a high level:

1. Configure S3 access (endpoint, region/profile, and run bucket).
2. Create a canonical configuration from `examples/worker.toml`. The same
   `[run]`-rooted file configures standalone `mig-worker` and the
   configuration-consuming `vamoose` commands. Source and destination must not
   overlap.
3. Run `vamoose init --config <path>` to materialize the bucket-prefix markers.
4. Run `nfs-walker`, convert legacy output with `mig-walker-rewrite` if needed,
   and upload the canonical immutable Parquet shards plus `manifest.json`.
   `scripts/manual-verify.sh` shows the current explicit pipeline.
5. Start `vamoose worker --config <path>` on each migration host.
6. Monitor the data plane with `vamoose status --config <path>`. The optional
   coordinator and TUI can operate once control-plane job state is provisioned,
   but the repository does not yet provide that provisioning command or route:
   - add `[coord]` to the worker configuration;
   - start `vamoose coord --config <path>` with the desired TLS/auth options;
   - connect with `vamoose tui --coord-url https://coord.host:8443`.

The TUI exposes pause, resume, cancel, drain, and retry-failed endpoints through
its command palette. In the current control contract, drain maps to paused
state and retry-failed is audit-only; neither is a separate worker execution
mode yet. `NO_COLOR=1` and `VAMOOSE_THEME=light` are supported. Workers without
`[coord]` continue to operate through S3 claims alone.

## Configuration

`examples/worker.toml` is the canonical operator format:

```toml
[run]
[worker]
[shard]
[mover]
[batch]
[copy]
[backpressure]
[coord] # optional
```

Control-only commands can use a file containing just `[run]`. Optional
unified-CLI sections are `[nfs]`, `[walker]`, `[aggr]`, and `[logging]`. The
older `[global]`/`[s3]` vamoose format remains accepted as a compatibility
input; mixed canonical and compatibility roots are rejected.

Historical mover fields remain parseable, but do not enable removed or
unimplemented strategies. See [DESIGN.md](DESIGN.md#configuration) and the
comments in [examples/worker.toml](examples/worker.toml) for current semantics.

## Documentation

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
