# Surface torn-copy detection instead of discarding it

Status: open — not started.
Ledger: F05 in `docs/REVIEW_LEDGER.md`.
Priority: high — modified-during-copy files currently commit silently.
Scope: `migration-mover` (`file_mover.rs`, `downgrade.rs`,
`pipelined_copy.rs` docs).

## Problem

`pipelined_copy` carefully detects torn reads: it brackets the copy
with pre/post `fstat` and sets `FileCopyResult::torn`
(`pipelined_copy.rs` ~274) when size/mtime/ctime changed mid-copy.
`copy_regular` in `file_mover.rs` then reads **only**
`bytes_copied` — `torn` is dropped on the floor. A file modified
while being copied is committed via rename with potentially
interleaved old/new content, counted as a success, and leaves no
record anywhere.

The module docs justify committing torn files because "the next pass
re-copies torn rows" — but no multi-pass driver exists (see
MULTI_PASS_MOVER.md: phases 3–8 unbuilt). The shard is marked
`Completed` after one pass. Until multi-pass exists, a torn copy must
at minimum leave an operator-visible record.

## Required reading

- `crates/migration-mover/src/pipelined_copy.rs` (module docs +
  `FileCopyResult`)
- `crates/migration-mover/src/file_mover.rs` (`copy_regular`,
  `apply_async_attrs`, how downgrades are recorded today)
- `crates/migration-mover/src/downgrade.rs` (`DowngradeKind`,
  existing unit-test patterns)

## Acceptance tests — write these FIRST

The FFI copy path is hardware-gated, so test at the seam: extract the
result-handling decision into a pure function and test that.

1. `torn_result_produces_downgrade_record` (red before fix — the
   function won't exist yet; write the test against the intended
   signature) — pure fn takes a `FileCopyResult` with `torn = true`
   plus row identity, returns a downgrade record of a new
   `DowngradeKind::TornCopy` variant carrying the pre/post
   (size, mtime, ctime) pairs.
2. `clean_result_produces_no_record` — `torn = false` → `None`.
3. `torn_file_still_commits` — the torn outcome must NOT become a
   failure: the row still counts as copied (at-least-once semantics;
   source remains intact). Assert the classification result marks
   commit-and-record, not fail.
4. `downgrade_sink_roundtrips_torn_record` — JSONL serialization
   round-trip in `downgrade.rs`, following the existing tests there.
5. Wire-up test: `copy_regular`'s caller-visible summary (whatever
   struct carries per-shard counters) gains a `files_torn` count;
   assert it increments when the classifier returns a torn record
   (drive the extracted function; no NFS needed).

## Fix shape

- New `DowngradeKind::TornCopy { pre: (u64, i64, i64), post: (u64,
  i64, i64) }` (match existing variant styles/serde).
- Pure classifier fn in `file_mover.rs` (or a sibling module):
  `classify_copy(result, row) -> CopyDisposition` where the
  disposition says "commit" and optionally carries a downgrade
  record. `copy_regular` consumes it: emit the record to the
  downgrade sink, bump `files_torn`, `tracing::warn!` with the path.
- Update the `pipelined_copy.rs` module-doc paragraph that promises a
  multi-pass re-copy: state the actual behavior (commit + TornCopy
  downgrade record) and reference MULTI_PASS_MOVER for the future.
- The sync mover path (`mover.rs::do_libnfs_copy`) has no pre/post
  bracket; add a note in its docs that torn detection is
  async-path-only for now (do not implement it there in this item).

## Out of scope / do NOT

- No multi-pass driver work.
- No retry-on-torn logic (re-reading a live file can tear again;
  that's the multi-pass design's job).
- Do not turn torn into a failure — source-intact + operator record
  is the contract here.

## Definition of done

- [ ] Tests written first; 1 red (missing fn/variant) before fix.
- [ ] All acceptance tests green; full gate green.
- [ ] Module docs updated (no more phantom multi-pass claim).
- [ ] Ledger F05 updated; this doc's Status flipped.
