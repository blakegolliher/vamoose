# vamoose

Wire-rate distributed NFS-to-NFS migration with S3-based coordination.

## Status

The control-plane work in [docs/COORD_PLAN.md](docs/COORD_PLAN.md) is
delivered end-to-end: `vamoose coord` (REST + SSE HTTP daemon),
`vamoose tui` (terminal dashboard with pause / resume / cancel /
drain controls, command palette, light & dark themes), worker → coord
integration with backoff and dedup, and CI gated by `cargo deny` and
`cargo about`.

M5 self-fence verification passes against the v2 claim protocol.
See [docs/work-items/M5_NOTES.md](docs/work-items/M5_NOTES.md) for
the verification record.

## Architecture

vamoose has four planes:

- **Data plane** — userspace libnfs reads from source NFS and writes
  to destination NFS. No kernel mount required.
- **Control plane** — Apache Parquet shards in S3 describe the files
  to migrate. The walker scans the source filesystem and produces
  these shards.
- **Coordination plane** — S3 objects implement a claim protocol
  that gives workers at-most-once shard completion via
  delete-then-create with `PUT If-None-Match` and `DELETE If-Match`.
- **Operator plane** *(optional)* — `vamoose coord` exposes a REST +
  SSE HTTP surface that workers report into; `vamoose tui` connects
  to it and renders a live dashboard with pause / resume / cancel /
  drain controls. Workers run standalone without a coord; the
  operator plane is additive.

Workers are stateless. Coordination is object-store-native: no etcd,
no Postgres, no message broker. Adding a worker means starting a new
`vamoose worker` process pointed at the same S3 bucket.

## Components

- `crates/migration-core` — claim protocol, S3 client, parquet
  schema, bucket layout
- `crates/migration-mover` — the file copy engine, libnfs FFI
- `crates/migration-worker` — the orchestrator (driven by
  `vamoose worker`)
- `crates/migration-aggr` — placeholder for the operator
  observability sidecar (all subcommands are stubs today; live
  observability comes from `vamoose status` / `vamoose coord` +
  `vamoose tui`)
- `crates/migration-control-protocol` — control-plane REST/SSE wire
  schema and pure snapshot reducer; distinct from the S3 claim
  protocol in `migration-core`
- `crates/migration-coord` — HTTP/SSE control-plane daemon
  (`vamoose coord`); S3-backed event log, snapshot, single-writer
  lease
- `crates/migration-tui` — terminal dashboard (`vamoose tui`);
  ratatui front-end over the coord's REST + SSE
- `crates/vamoose-cli` — the unified `vamoose` binary; thin wrapper
  that composes the crates above into subcommands
- `crates/mig-walker-rewrite` — schema shim from the legacy walker
  output to canonical

## Build

Requires Rust 1.75+ and the libnfs system library.

```bash
# Debian/Ubuntu
sudo apt install libnfs-dev pkg-config

# RHEL/Rocky
sudo dnf install libnfs-devel pkgconf-pkg-config

cargo build --release
```

Binaries land in `target/release/`. The unified entry point is
`target/release/vamoose`; run `vamoose --help` for the subcommand
list.

## Quick start

See `scripts/manual-verify.sh` and the per-crate notes for the
end-to-end cookbook. At a high level:

1. Configure S3 access (AWS CLI profile, bucket, endpoint).
2. `vamoose init` — lay out the bucket prefixes.
3. Scan the source filesystem and upload canonical parquet shards
   to S3. (`vamoose walker` is still a stub: run `nfs-walker` and,
   if its output is legacy-schema, `mig-walker-rewrite`, then upload
   the canonical shards — `scripts/manual-verify.sh` shows the exact
   steps.)
4. Copy `examples/worker.toml` per migration host and edit the
   source / destination / S3 stanzas. This is the canonical config
   for `vamoose worker`, `status`, `init`, `coord`, and `doctor`, as
   well as the standalone `mig-worker`. **Do not** reuse a config
   whose source path overlaps its destination path.
5. Launch `vamoose worker --config <path>` on each migration host.
6. Monitor progress. Pick the surface that fits:
   - **One-shot or watch loop** — `vamoose status` (reads the S3
     aggregate directly; no daemon required).
   - **Live operator dashboard** — start `vamoose coord` on a
     coordinator host, point each worker at it via the `[coord]`
     stanza in its TOML, then run `vamoose tui --coord-url
     https://coord.host:8443`. The TUI streams events over SSE and
     exposes pause / resume / cancel / drain / retry-failed via a
     `:`-prefixed command palette. `NO_COLOR=1` and
     `VAMOOSE_THEME=light` are honored.

## Documentation

- [DESIGN.md](DESIGN.md) — architectural overview
- [SCHEMA_CONTRACT.md](SCHEMA_CONTRACT.md) — parquet index schema
- [docs/CORRECTNESS_RULES.md](docs/CORRECTNESS_RULES.md) —
  engineering invariants that must hold across the codebase
- [docs/CLAIM_PROTOCOL.md](docs/CLAIM_PROTOCOL.md) — the v2 claim
  protocol and self-fence rules
- [docs/COORD_PLAN.md](docs/COORD_PLAN.md) — control-plane design
  (coord daemon, TUI, supply-chain CI)
- [docs/work-items/](docs/work-items/) — protocol specifications and
  verification records

## License

Licensed under the GNU Affero General Public License v3.0
(AGPL-3.0). See [LICENSE](LICENSE) for the full text.

The mover dynamically links libnfs (LGPL-2.1-or-later). Do not
statically link libnfs. Third-party licenses tracked in
[THIRD_PARTY_LICENSES.md](THIRD_PARTY_LICENSES.md).

Copyright (C) 2026 Blake Golliher
