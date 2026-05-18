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
  `docs/work-items/CLAIM_PROTOCOL_V2_DELETE_THEN_CREATE.md`.

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

- `SCHEMA_CONTRACT.md` — column names, types, null semantics, path
  encoding, fsid grouping, parquet KV footer keys, downgrade
  semantics, version policy.
- `DESIGN.md` — architecture (claim protocol, fence, mover
  strategies, milestone scope). If code disagrees with it, the
  design wins unless the design has been explicitly updated.
- `THIRD_PARTY_LICENSES.md` — keep accurate when adding deps.
  libnfs is **LGPL-2.1-or-later, dynamic-linked only**.
