# vamoose

Wire-rate distributed NFS-to-NFS migration with S3-based coordination.

## Status

M5 self-fence verification passes against the v2 claim protocol.
See [docs/work-items/M5_NOTES.md](docs/work-items/M5_NOTES.md) for
the verification record.

## Architecture

vamoose has three planes:

- **Data plane** — userspace libnfs reads from source NFS and writes
  to destination NFS. No kernel mount required.
- **Control plane** — Apache Parquet shards in S3 describe the files
  to migrate. The walker scans the source filesystem and produces
  these shards.
- **Coordination plane** — S3 objects implement a claim protocol
  that gives workers at-most-once shard completion via
  delete-then-create with `PUT If-None-Match` and `DELETE If-Match`.

Workers are stateless. Coordination is object-store-native: no etcd,
no Postgres, no message broker. Adding a worker means starting a new
`mig-worker` process pointed at the same S3 bucket.

## Components

- `crates/migration-core` — claim protocol, S3 client, parquet
  schema, bucket layout
- `crates/migration-mover` — the file copy engine, libnfs FFI
- `crates/migration-worker` — the orchestrator binary (`mig-worker`)
- `crates/migration-aggr` — sidecar for operator observability
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

Binaries land in `target/release/`.

## Quick start

See `scripts/manual-verify.sh` and the per-crate notes for the
end-to-end cookbook. At a high level:

1. Configure S3 access (AWS CLI profile, bucket, endpoint).
2. Walk the source filesystem and upload the canonical shards to S3.
3. Generate worker config TOMLs (one per host).
4. Launch `mig-worker --config <path>` on each migration host.
5. Monitor progress via the aggregator or directly via S3.

## Documentation

- [DESIGN.md](DESIGN.md) — architectural overview
- [SCHEMA_CONTRACT.md](SCHEMA_CONTRACT.md) — parquet index schema
- [docs/CORRECTNESS_RULES.md](docs/CORRECTNESS_RULES.md) —
  engineering invariants that must hold across the codebase
- [docs/work-items/](docs/work-items/) — protocol specifications and
  verification records

## License

Licensed under the GNU Affero General Public License v3.0
(AGPL-3.0). See [LICENSE](LICENSE) for the full text.

The mover dynamically links libnfs (LGPL-2.1-or-later). Do not
statically link libnfs. Third-party licenses tracked in
[THIRD_PARTY_LICENSES.md](THIRD_PARTY_LICENSES.md).

Copyright (C) 2026 Blake Golliher
