# What's next

This is the living list of remaining work. The architecture-maintainability
sequence is complete: control-protocol extraction, coordinator runtime
decomposition, TUI feature/view decomposition, unified CLI lifecycle,
canonical configuration composition, obsolete mover-scaffolding removal, and
the as-built documentation reconciliation have landed.

Future work should be selected from this document and current evidence rather
than from another predetermined cleanup sequence. The review ledger remains the
finding-by-finding disposition record; beta/operator impact lives in
[BETA_NOTES.md](BETA_NOTES.md).

## 1. Open review-ledger work

| Finding | State |
|---|---|
| F15 | Open, with the limitation accepted for beta: hardlink grouping and directory-attribute ordering are micro-batch scoped. The durable fix belongs in a reviewed multi-pass design; it is not a small one-pass patch. |

All other ledger findings are landed or otherwise decided. A `landed` row that
requires hardware evidence is not `verified` until the relevant exercise below
passes.

## 2. Hardware-backed validation

These checks require the VAST/libnfs rig and remain enabled or documented for
that environment:

- [ ] F01/F30 — run `scripts/fast-reclaim-drill.sh`; reclaim latency must stay
      within the documented progress-liveness grace rather than falling back to
      the full lease, with no live-claim theft cascade.
- [ ] F06/F07 — rerun the ignored `pipelined_copy_smoke` tests.
- [ ] F10 — run ignored hardlink and symlink replay-idempotency cases in
      `file_mover_smoke.rs`; verify link identity/target bytes on the server.
- [ ] F08 — migrate a root-owned `4755` file and confirm the destination mode
      remains `4755`.
- [ ] F12 — run the ignored bounded-timeout case in
      `libnfs_async_integration.rs` against a black-holed address.
- [ ] F11 — run `error_path_leaves_context_usable_after_drain` against the
      linked libnfs implementation.
- [ ] F09 — run `sync_write_fsync_commit_readback` and the documented
      server-crash drill in `crates/migration-mover/MANUAL_VERIFY.md`.

Do not mark these verified from unit tests alone. The detailed FFI invocation
and pass criteria are in [CORRECTNESS_RULES.md](CORRECTNESS_RULES.md).

## 3. Functional work not yet implemented

- **Multi-pass migration:** design and implement later passes, including F15
  cross-batch/cross-shard hardlink fidelity and directory/root metadata
  convergence. Do not revive removed mover strategies as placeholders.
- **Aggregation and observability:** standalone `mig-aggr` implements only
  `clean-partials`. `watch`, `summary`, `metrics`, `inspect`, and `verify`
  return safe unimplemented errors; `vamoose aggr` is also a stub.
- **Pipeline stubs:** `vamoose walker`, `rewrite`, and `run` retain CLI
  shapes but are not implemented; `vamoose prepare` is the supported path
  (bundled `nfs-walker` → `mig-walker-rewrite` → verified upload →
  conditional-create `manifest.json`). Remaining prepare follow-ups: a
  `packaging/nfs-walker.lock.json` digest pin checked at bundle time (the
  bundle records the SHA256 today), and overlapping scan → index → copy so
  workers start on the first uploaded shard (HANDOFF lever 3).
- **Walker schema completion:** coordinate the walker repository's canonical
  output and xattr capture before deleting `mig-walker-rewrite` or claiming
  xattr fidelity.
- **Multiple runs per bucket:** the worker layout still places one run at the
  bucket root. A `run_prefix` or equivalent requires a deliberate layout and
  compatibility design.
- **Archived control history restore:** the coordinator writes
  `archivelogs/`, but replay does not restore from it and no restore command is
  implemented.
- **`vamoose verify --sample N`:** a Rust, libnfs-based sampled verifier so
  the quickstart does not need the kernel-mount/SSH `ops/finalize-run.sh`
  path to confirm a run.

## 4. Focused follow-ups

- Add a `clean-partials --lease-timeout-sec` override. Its liveness gate uses
  the protocol default (180 seconds), so operators with longer configured
  worker leases must currently wait out that lease or use `--force`
  deliberately.
- Add a coordinator snapshot/read boundary such as `as_of_seq` so the TUI can
  replace its bounded torn-walk bootstrap retry with a server-defined atomic
  boundary.
- Decide whether to age `Snapshot.last_client_seq` and
  `Job.assigned_workers` entries alongside disconnected worker eviction if
  their residual growth becomes operationally relevant.
- Close the `JobId` deserialization validation gap: transparent Serde input
  currently bypasses `JobId::new` validation.
- Add a classifier bridge test that drives reader-produced schema drift errors
  through `migration-worker::classify_shard_error`.
- Revisit `migration-core::s3` typing for `get`/`list`/`head_object` errors;
  they remain generic `Other` errors beneath retry policy.
- Remove the control-plane compatibility surfaces only through a separately
  announced breaking change after downstream users migrate to
  `migration-control-protocol`. This includes `migration_coord::schema` and
  legacy server DTO re-exports.
- Separate coordinator persistence bookkeeping from the version-1 control
  snapshot only through an explicit versioned design. Do not trim
  `audit_seq_today`, `audit_seq_date`, or `last_client_seq` as incidental
  cleanup.
- Remove the unused `Unicode-DFS-2016` allowance from `deny.toml` in a focused
  supply-chain cleanup after rechecking the dependency tree.

## 5. Cross-repository schema contract

`SCHEMA_CONTRACT.md` is mirrored with `nfs-walker` and must not be edited in
only this repository. Two wording corrections remain queued for a coordinated,
byte-identical update:

- file type `Unknown = 0` currently produces `CorruptRow`, while the contract
  calls that outcome `ShardCorrupt`; and
- the contract's required-column table lists the full canonical shape, while
  the current reader enforces the five non-nullable columns and handles other
  canonical columns according to their null/default semantics.

Set the `NFS_WALKER_REPO` repository variable (and `NFS_WALKER_TOKEN` when the
target is private) to enable the cross-repository drift check.

## Completed architecture sequence

- Shared control-plane types and reducer live in
  `migration-control-protocol`; worker and TUI no longer compile against the
  coordinator runtime in their production graphs.
- Coordinator runtime and TUI application/state/rendering are decomposed into
  responsibility modules without changing their public façades.
- The unified CLI owns dispatch, logging teardown, and final process status;
  doctor and worker shutdown lifecycle defects are fixed.
- The worker `[run]` format is canonical across the unified CLI, with
  `[global]`/`[s3]` retained as a compatibility input.
- Only the implemented synchronous and bucketed-async libnfs paths remain
  executable; unused io_uring, server-side COPY, and kernel-CFR scaffolding is
  gone while operator TOML compatibility remains.
- Current architecture, control-plane, handoff, README, and next-work docs now
  describe the as-built system; historical plans and review records remain
  identifiable as history.

Process lessons from the completed review campaign remain in
[LESSONS.md](LESSONS.md).
