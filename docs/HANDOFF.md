# Current project handoff

Vamoose's planned layering and maintainability sequence is complete. Current
work should start from [NEXT.md](NEXT.md), not from the old cleanup sequence or
the historical coordinator plan.

This handoff deliberately avoids branch names, PR state, and exact test totals.
Those change faster than the architectural contracts. Verify the working tree,
current source, and CI before starting new work.

## Authoritative map

- [../README.md](../README.md) — entry point, build, quick start, component and
  command status
- [../DESIGN.md](../DESIGN.md) — concise as-built system architecture
- [CONTROL_PLANE.md](CONTROL_PLANE.md) — current control-plane ownership,
  runtime modules, and durability/concurrency invariants
- [CLAIM_PROTOCOL.md](CLAIM_PROTOCOL.md) — as-built S3 claim protocol
- [CORRECTNESS_RULES.md](CORRECTNESS_RULES.md) — rules that changes must not
  weaken
- [BETA_NOTES.md](BETA_NOTES.md) — operator limitations, security posture, and
  operational contracts
- [NEXT.md](NEXT.md) — current remaining work
- [REVIEW_LEDGER.md](REVIEW_LEDGER.md) — finding-by-finding disposition
- [COORD_PLAN.md](COORD_PLAN.md), `work-items/`, milestone notes, and
  `baselines/` — historical delivery and review records

Source and tests win when a current document drifts. Historical documents
remain useful for rationale, but their present-tense statements are not
current specifications.

## Current boundaries

- `migration-core` owns the immutable run formats, Parquet schema/reader, S3
  layout/client, claim protocol, and fence primitives.
- `migration-mover` owns the NFSv3/libnfs data plane. The normal synchronous
  `MultiPool` and opt-in bucketed async regular-file path are the only copy
  implementations. No io_uring, NFSv4.2 COPY, or kernel
  `copy_file_range` execution scaffold remains.
- `migration-worker` owns claim/reclaim, heartbeat/self-fence, shard
  processing, progress, and optional coordinator reporting.
- `migration-control-protocol` owns the control-plane wire contract and pure
  snapshot reducer.
- `migration-coord` owns control-plane persistence, replay, lease, audit,
  archival, authentication, HTTP/SSE, and runtime lifecycle.
- `migration-tui` depends directly on the protocol, bootstraps over REST, and
  follows SSE. It does not pull coordinator, data-plane, or AWS dependencies
  into its normal graph.
- `vamoose-cli` owns unified configuration composition, command dispatch,
  logging teardown, and final process status. The `[run]` worker shape is the
  canonical configuration; `[global]`/`[s3]` remains compatible.

The optional coordinator does not own worker claims. Workers without `[coord]`
remain valid S3-only participants.

## Operational reality

- Use `examples/worker.toml` as the canonical configuration for `mig-worker`
  and configuration-consuming `vamoose` commands.
- `vamoose worker`, `status`, `doctor`, `init`, `coord`, and `tui` are
  implemented. `vamoose walker`, `rewrite`, `aggr`, and `run` fail safely as
  stubs.
- Standalone `mig-aggr clean-partials` is implemented and dry-run by default;
  the other `mig-aggr` observability commands return unimplemented errors.
- `Strategy::LibnfsIoUring` and several mover TOML fields are retained
  compatibility names, not evidence of an io_uring or server-side-copy path.
- The control-plane schema remains version 1. Compatibility re-exports remain
  in place and require a separately announced breaking cleanup.

## Known limitations and validation

The beta limits in [BETA_NOTES.md](BETA_NOTES.md) are part of the operating
contract. In particular, hardlink grouping and directory-attribute ordering
are bounded by the current micro-batch/shard execution model, most aggregation
commands are not implemented, and the TUI's atomic bootstrap boundary is still
a client-side retry rather than a coordinator `as_of_seq` API.
The coordinator also lacks a supported production job-create/import workflow;
a fresh runtime has no job row for worker registration.

Several data-plane fixes are CI-covered but still await the recorded VAST
hardware exercises: reclaim timing, pipelined copy, replay idempotency,
setuid/setgid preservation, bounded RPC timeout, drain-before-close, NFS
COMMIT/readback, and crash behavior. Do not replace those tests with simulated
unit claims or mark ledger rows verified without the rig evidence.

The mirrored [../SCHEMA_CONTRACT.md](../SCHEMA_CONTRACT.md) must remain
byte-identical with the `nfs-walker` repository. Its known prose tensions are
tracked in [NEXT.md](NEXT.md) and require a paired change.

## Starting new work

1. Read [NEXT.md](NEXT.md) and the linked correctness/beta material.
2. Run `git status`, update from the current default branch, and preserve local
   work.
3. Confirm the proposed change against source and tests; do not infer behavior
   from a historical prompt.
4. For libnfs/FFI work, follow the hardware gate in
   [CORRECTNESS_RULES.md](CORRECTNESS_RULES.md) and
   [`../crates/migration-mover/MANUAL_VERIFY.md`](../crates/migration-mover/MANUAL_VERIFY.md).
5. Do not alter the claim or wire protocol as incidental cleanup.
