# Hardlink replay idempotency: EEXIST + fileid match = success

Status: in progress on branch `f10-link-idempotency`.
Ledger: F10 in `docs/REVIEW_LEDGER.md` (do NOT edit the ledger or
`docs/NEXT.md` from this branch — the coordinator sweeps those).
Priority: medium (at-least-once replay of a committed hardlink lands
in the failure sink today).
Scope: `migration-mover` only — `src/mover.rs`, `src/libnfs/ops.rs`
(safe-wrapper layer), `tests/file_mover_smoke.rs` (existing binary).

## Decision (made by the project owner, 2026-07-13)

When `nfs_link` fails with `EEXIST` during a hardlink row, stat both
the linkpath and the target on the destination context and compare
`nfs_fileid`:

- **Same fileid** → the linkpath is already a link to the target —
  this is our own committed work being replayed after a
  died-post-link-pre-ack worker; return `Ok(())`.
- **Different fileid** → a real conflict (some other file occupies the
  linkpath); fail the row as today, with a message that names both
  fileids so the operator can tell conflict from replay.
- **Stat fails during recovery** → fail the row with the ORIGINAL
  `EEXIST` error (recovery must never mask the primary failure).

Explicitly rejected: unlink-then-create (destroys pre-existing data
and opens a crash window with the link missing) and keep-as-failure
(status quo).

## Constraints (hard fences)

- MUST NOT modify any `extern "C"` declaration or struct layout in
  `src/libnfs/mod.rs`. The existing `nfs_stat64` binding
  (`mod.rs:194`) is sufficient — add a new SAFE wrapper in
  `src/libnfs/ops.rs` next to `stat_times` (whose doc comment at
  `ops.rs:224` already invites promotion to the full `nfs_stat_64`).
- MUST NOT touch `crates/migration-core/src/claim.rs`.
- MUST NOT add any new top-level `tests/*.rs` file anywhere in the
  workspace (CI disk cliff — every top-level test file links a
  full-workspace debug binary). Hardware coverage goes into the
  EXISTING `tests/file_mover_smoke.rs`.
- `do_hardlink`'s R8 fence-check-before-link (`mover.rs:499`) must
  remain immediately before the `ops::link` call, and the recovery
  path must NOT bypass it — recovery only runs after `ops::link`
  returned, so no extra fence check is needed, but do not reorder.

## Acceptance tests — write these FIRST, observe them red

The EEXIST-recovery decision must be a pure function so it is
CI-testable without an NFS server. Suggested seam in `mover.rs`:

```rust
/// Decide the outcome of a hardlink EEXIST: Ok(()) iff the existing
/// linkpath already IS the target (same fileid). `stats` are the
/// results of statting (target, linkpath) during recovery.
fn resolve_hardlink_eexist(
    link_err: MoveError,
    target_stat: Result<u64, MoveError>,   // fileid or stat failure
    linkpath_stat: Result<u64, MoveError>,
) -> Result<(), MoveError>
```

Unit tests in the existing `#[cfg(test)] mod tests` in `mover.rs`
(around line 810):

1. `eexist_same_fileid_is_success` — both stats Ok with equal fileids
   → `Ok(())`.
2. `eexist_different_fileid_stays_failure` — stats Ok, fileids differ
   → `Err`, phase `Hardlink`, message contains both fileids.
3. `eexist_stat_failure_preserves_original_error` — either stat
   `Err` → returned error is the ORIGINAL `EEXIST` `MoveError`
   (assert on its message/phase), not the stat error.
4. `non_eexist_errors_pass_through` — wiring-level guarantee: the
   recovery arm must only trigger on `EEXIST`. Test whatever shape
   the guard takes (e.g. a helper that classifies the link error, or
   assert `resolve_hardlink_eexist` is only reachable behind an
   `EEXIST` match — document the choice).

Red-before-fix: commit the tests first calling the not-yet-written
helper is a compile failure, which does not demonstrate red. Instead:
write the helper as an honest stub returning the pass-through failure
(status-quo behavior: every EEXIST stays a failure), observe tests 1
fail / 2–3 pass or fail meaningfully, then implement. The commit
message of the test commit must state what was observed red.

Hardware smoke (rides the existing `#[ignore]` pattern): add a case to
`tests/file_mover_smoke.rs` that copies a file, calls `move_hardlink`
for a second path, then calls `move_hardlink` AGAIN for the same row
(simulating replay) and asserts the second call succeeds and both
paths share an inode. This runs on the VAST rig pass, not CI.

## Implementation sketch

1. `ops.rs`: add `pub fn stat_fileid(ctx, path) -> Result<u64, MoveError>`
   (or promote to returning full `nfs_stat_64` per the existing TODO —
   your call; keep `stat_times` behavior identical either way).
2. `mover.rs` `do_hardlink`: match `ops::link` error; on `EEXIST`
   (use the same errno-name convention as `ops.rs` — errors carry
   `error == "EEXIST"` per `mkdir`'s handling at `ops.rs:309`), stat
   `target_abs` and `linkpath_abs` and delegate to
   `resolve_hardlink_eexist`.
3. Doc comment on `do_hardlink` updated: link-is-commit-point now has
   an idempotent replay story; say why unlink-then-create was
   rejected.

## Gate & handoff

Full gate before every commit:
`cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace && cargo deny --all-features check`

One commit for tests-red (stub), one for the fix, plus this doc.
Commit to this branch; do NOT push — the coordinator reviews the
diff, re-runs the gate, and pushes. No AI/Claude attribution
anywhere (no Co-Authored-By trailers).
