# Symlink replay idempotency: EEXIST + readlink match = success

Status: landed — merged in PR #34.
Ledger: F10 in `docs/REVIEW_LEDGER.md` (do NOT edit the ledger or
`docs/NEXT.md` from this branch — the coordinator sweeps those).
This is the second half of F10: PR #30 landed the hardlink half
(`docs/work-items/HARDLINK_REPLAY_IDEMPOTENCY.md`); this item is its
symlink mirror and completes the row.
Priority: medium (at-least-once replay of a committed symlink row
lands in the failure sink today).
Scope: `migration-mover` only — `src/mover.rs`,
`tests/file_mover_smoke.rs` (existing binary). NO changes under
`src/libnfs/` are needed at all: `ops::readlink` already exists and
is already used by `do_symlink` to read the source target.

## Decision (owner's F10 call, 2026-07-13, extended per NEXT.md §3)

`do_symlink` (`mover.rs:413-430`): the symlink IS the commit point
(R8 — NFSv3 has no atomic symlink-replace). When `ops::symlink`
fails with `EEXIST`, `readlink` the DESTINATION path on the dst
context and byte-compare with the intended target:

- **Match** → the dst symlink already points at the intended target:
  our own committed work replayed after a died-post-symlink-pre-ack
  worker; treat as success and CONTINUE with the normal post-commit
  steps (mode-downgrade record, best-effort lutimes), which are
  identical to the first run and idempotent under at-least-once.
- **Mismatch** → real conflict; fail the row as today, with a
  message naming both targets (lossy-display the byte strings) so
  the operator can tell conflict from replay.
- **`readlink` fails during recovery** (including EINVAL because the
  existing entry is not a symlink at all) → fail with the ORIGINAL
  `EEXIST` error; recovery must never mask the primary failure.

Unlink-then-create stays rejected (destroys pre-existing data,
crash window with the link missing).

## Constraints (hard fences)

- MUST NOT touch anything under `src/libnfs/` (no new wrappers are
  needed; if you believe one is, stop and report instead).
- MUST NOT touch `crates/migration-core/src/claim.rs`.
- MUST NOT add any new top-level `tests/*.rs` file anywhere (CI disk
  cliff). Hardware coverage goes into the EXISTING
  `tests/file_mover_smoke.rs`.
- The R8 fence-check must remain immediately before `ops::symlink`;
  recovery runs only after `ops::symlink` returned (mirror the
  hardlink arm's comment at `mover.rs:511-515`).
- Reuse the existing `is_eexist` guard (`mover.rs:837`) — do not
  duplicate it.

## Acceptance tests — write these FIRST, observe them red

Pure decision function, mirroring `resolve_hardlink_eexist`:

```rust
/// Decide the outcome of a symlink EEXIST: Ok(()) iff the existing
/// dst entry is a symlink whose target byte-equals `intended`.
fn resolve_symlink_eexist(
    link_err: MoveError,
    intended: &[u8],
    dst_readlink: Result<Vec<u8>, MoveError>,
) -> Result<(), MoveError>
```

Unit tests in `mover.rs`'s existing `#[cfg(test)] mod tests`, next
to the hardlink EEXIST block (same red-via-honest-stub approach as
PR #30 — stub returns the pass-through failure, `#[ignore]` markers
carrying the observed-red note in the test commit only):

1. `symlink_eexist_matching_target_is_success` — readlink Ok with
   byte-identical target → `Ok(())`. Include a non-UTF-8 target case
   (e.g. `b"/t/\xff\xfe"`) to pin byte-compare, not string-compare.
2. `symlink_eexist_different_target_stays_failure` — readlink Ok,
   different bytes → `Err`, phase `Symlink`, message contains both
   targets (lossy-rendered).
3. `symlink_eexist_readlink_failure_preserves_original_error` —
   readlink `Err` (use an EINVAL-shaped MoveError: the
   entry-is-not-a-symlink case) → returned error is the ORIGINAL
   EEXIST MoveError.
4. Wiring guard: non-EEXIST symlink errors pass through — covered by
   the existing `is_eexist` tests; add only what pins the NEW arm
   (e.g. that the recovery match arm is entered solely behind
   `is_eexist`, documented the same way the hardlink arm's test 4
   does).

Hardware smoke: add `symlink_replay_is_idempotent` to the EXISTING
`tests/file_mover_smoke.rs`, following `hardlink_replay_is_idempotent`
(added in PR #30) as the template: create/copy a symlink row, replay
the same row, assert the second call succeeds and the destination
readlink still equals the intended target. `#[ignore]`, VAST rig
env contract, same helpers.

## Implementation sketch

1. `do_symlink`: wrap `ops::symlink(pair.dst(), &target, &dst)` in
   the same `if let Err(e)` + `is_eexist` shape as `do_hardlink`;
   on EEXIST, `ops::readlink(pair.dst(), &dst)` and delegate to
   `resolve_symlink_eexist(e, &target, readlink_result)`. On Ok,
   fall through to the existing post-commit mode/mtime handling
   (do NOT early-return past it).
2. Doc comment on `do_symlink` updated with the replay story,
   mirroring `do_hardlink`'s.

## Gate & handoff

Full gate before every commit:
`cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace && cargo deny --all-features check`

One commit for tests-red (stub), one for the fix, plus this doc in
the first commit. Commit to this branch; do NOT push — the
coordinator reviews the diff, re-runs the gate, and pushes. No
AI/Claude attribution anywhere (no Co-Authored-By trailers).
