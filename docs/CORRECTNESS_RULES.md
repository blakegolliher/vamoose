# Correctness rules

Engineering invariants that must hold across the codebase. Each rule
exists because violating it has caused — or could cause — silent data
corruption or wedged worker state. Don't weaken any of these without
an explicit design change.

## Critical correctness rules

- **Conditional PUT is the only ownership mechanism.** Never fall back
  to "GET-then-PUT" for claims. The v2 protocol uses
  `PUT If-None-Match: *` for first-time claim and
  `DELETE If-Match: <etag>` for ownership transfer; both are honored
  by VAST S3 and most S3-compatible stores. See
  [CLAIM_PROTOCOL.md](CLAIM_PROTOCOL.md).

- **Self-fence before commit.** The mover must check
  `fence.is_valid()` before issuing `RENAME` (the commit point). If
  the fence is tripped, leave the `.partial` file and exit the shard.
  The shard processor adds post-acquire-permit fence rechecks at the
  three dispatch sites (singleton, run_group, dirs phase 2) to catch
  fence trips that happen while a task waits for an inflight permit.

- **Atomic rename.** Writes go to
  `.<basename>.<host>.<pid>.partial`; only `RENAME` makes them
  visible. Two workers cannot collide on each other's partial files.

- **Path bytes, not strings.** POSIX paths are byte sequences. Use
  `Vec<u8>` everywhere; serialize to JSON via base64.

- **Materialized `row_id`.** Read it from the parquet column. Do not
  derive from `(shard_idx, row_offset_at_read_time)` — pushdown can
  reorder rows.

- **`endpoint.root` is part of the path.** Mover operations on
  source use `source.root + row.path`. Mover operations on dest use
  `dest.root + row.path`. The libnfs mount target is just
  `endpoint.url`, never `endpoint.url + row.path`. Use
  `migration_mover::join_root` for the byte-aware concatenation —
  do not write your own.

- **Source and dest paths must be provably distinct before any
  write.** A startup check (`migration_core::overlap::check`)
  verifies endpoints don't overlap; a per-file check inside the
  mover verifies the specific paths don't collide. Both checks must
  exist; either alone is insufficient.

- **`.partial` is in the same directory as the final destination.**
  This is a correctness requirement for atomic rename, but combined
  with overlapping source/dest it becomes a data-loss vector. The
  per-file self-target check protects against this.

- **NFSv3 is the protocol baseline.** Code paths that require NFSv4
  features (server-side COPY, batched SETATTR, SETATTR-on-symlink)
  must not be on the default path. NFSv4 optimizations may exist
  but must be optional. Where libnfs surfaces v4-flavored callbacks,
  the correct response is to pin v3, not to start exploring v4.

- **Cross-check C library FFI against the linked binary, not just
  the system header.** When wrapping a C library, parameter order
  and types must match the actually-linked `.so`, not whatever
  header is installed at `/usr/include`. Multiple library versions
  may coexist on the same host. Verify with `ldd <binary>` to find
  the linked `.so`, then `objdump -T <so> | grep <symbol>` to
  identify it. The header that ships with that `.so` is the
  authoritative declaration.

  Specifically: on at least one verification host, libnfs 4.x at
  `/usr/include` and libnfs 5.x at `/usr/local/include` declare
  `nfs_pread` with reversed parameter order. The FFI was originally
  written against the wrong header and produced silent data loss
  in M2 verification (see `M2_NOTES.md`).

  Future libnfs FFI changes must be tested against
  `crates/migration-mover/tests/libnfs_ffi_smoke.rs` before merging.

- **Verify binaries are fresh against current source when output is
  wrong.** When end-to-end output is wrong and source review finds
  nothing, check the modification timestamp of every binary in the
  pipeline against the source files those binaries should have been
  built from. A binary substantially older than its source is suspect.
  Cargo will not rebuild what it doesn't know about: a `[[bin]]` entry
  silently removed from `Cargo.toml`, a feature flag that gates the
  build, or a Makefile target that's no longer invoked can all leave
  an orphan binary on disk that diverges from source as fixes land
  elsewhere.

  Treat this as the same class of bug as the libnfs FFI mismatch:
  source review proves nothing because the bug is below the source —
  the binary on disk is the diverged artifact.

- **Async libnfs: one service task owns the context.** When using
  `AsyncNfsContext` (`crates/migration-mover/src/libnfs/asyncio/`),
  the service task is the only entity that may issue libnfs calls
  against its context. libnfs contexts are not thread-safe; the
  service task design relies on serialized issuance. Public API
  methods always go through the request mpsc — never call any
  libnfs `*_async` symbol directly from a caller task. Callbacks
  fire on the service-task thread inside `nfs_service`; their only
  legal work is `oneshot_tx.send(...)`. Any future change that wants
  to chain libnfs calls from inside a callback must redesign the
  service task.

- **Async libnfs: dropping a future does NOT cancel the RPC.** libnfs
  has no NFSv3 cancel surface. Dropping the returned future drops
  the oneshot receiver; the RPC continues to completion and the
  result is silently discarded. Callers that require cancellation
  semantics (e.g. the multi-pass mover's cutover pass abandoning a
  slow read) must layer a higher-level cancellation token themselves
  and decide what to do with the in-flight bytes — they cannot rely
  on Drop to make the RPC stop.

## Verification gates

Manual verification against real hardware is a milestone gate. A
milestone is not done — even with all unit tests passing — until it
has been verified end-to-end on real hardware or verification has
been explicitly waived for that milestone.

Building a subsequent milestone on top of an unverified milestone
compounds risk: bugs in the lower layer become harder to attribute
once the upper layer is in place.

### Pre-merge runbook: async libnfs FFI changes

Any change that touches `crates/migration-mover/src/libnfs/asyncio/`
— in particular `ffi.rs`, `callbacks.rs`, `driver.rs`, or the
`AsyncNfsContext` public surface in `mod.rs` — must re-run all three
async test binaries against var204 before merge. Like the sync FFI
smoke gate (above), this exists because parameter-order or callback-
shape mismatches against the linked `.so` produce silent data loss
that no Rust-level test catches.

The three binaries:

1. `libnfs_async_ffi_smoke` — per-symbol round-trip (pread, write+read,
   stat/fstat, attr+namespace ops, symlink/readlink).
2. `libnfs_async_integration` — 64-way concurrent pread no-crosstalk,
   drop-during-flight survives, `nconnect>1` rejection, NFSv3-only
   gate.
3. `libnfs_async_perf_smoke` — ASYNC vs SYNC throughput at single
   context, 32 × 1 MiB reads. Async must meet or beat sync (see
   `docs/work-items/LIBNFS_ASYNC_FORK.md` closing note for the
   2026-05-18 baseline: 352 MB/s ASYNC vs 270 MB/s SYNC).

Invocation (env vars per `reference_verification_env`):

```
cargo build -p migration-mover --tests --release
sudo -E target/release/deps/libnfs_async_ffi_smoke-*    --ignored --nocapture
sudo -E target/release/deps/libnfs_async_integration-*  --ignored --nocapture
sudo -E target/release/deps/libnfs_async_perf_smoke-*   --ignored --nocapture
```

**Run the integration binary with the default `--test-threads`
(parallel) — do not pass `--test-threads=1`.** Parallel execution
spins up multiple `AsyncNfsContext::mount` calls in the same
process, which is the only configuration that exercises the libnfs
mount-time fd-swap path (NFSv3 `nfs_mount_async` walks
portmap → mountd → portmap → nfsd, disconnecting and reconnecting
at each step — each transition changes `rpc->fd`). The 2026-05-18
post-mortem ("async libnfs mount regression" in
`docs/work-items/LIBNFS_ASYNC_FORK.md`) lost ~half a day because
the original gate run used `--test-threads=1` and the latent bug
sat unobserved.

Pass criteria: 5/5 smoke, 4/4 integration **at default
parallelism**, async ≥ sync on perf. Any regression — including
the perf binary dropping below the recorded baseline by more than
~10 % — is a blocker, not a soft signal.

## Style conventions

- **Errors: prefer matching on `Error` variants over string contents.**
  `Error::PreconditionFailed` is **not** a real error — it's a
  protocol signal.

- **Use `tracing` not `log`.** Structured fields
  (`tracing::info!(host = %h, "…")`).

- **No `unwrap()` outside tests** except where panicking is genuinely
  desired (e.g. invariant checks).

- **Follow `nfs-walker`'s patterns where the problems overlap**
  (work-stealing pool, sharded parquet writer threads, libnfs FFI).

## Authoritative source-of-truth files

Current Rust source and tests are the executable truth; manifests define the
dependency graph. Bring current documentation forward when it drifts rather
than implementing an older plan implicitly.

- `SCHEMA_CONTRACT.md` — mirrored column names, types, null semantics, path
  encoding, fsid grouping, Parquet KV footer keys, downgrade semantics, and
  version policy. Coordinate edits with the `nfs-walker` repository.
- `DESIGN.md` — concise current system architecture.
- `docs/CONTROL_PLANE.md` — current control-plane ownership and runtime
  invariants.
- `docs/CLAIM_PROTOCOL.md` — current S3 claim protocol.
- `THIRD_PARTY_LICENSES.md` — generated dependency inventory. libnfs is
  **LGPL-2.1-or-later, dynamic-linked only**.
