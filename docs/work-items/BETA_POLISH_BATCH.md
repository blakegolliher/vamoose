# Beta polish batch: optional [nfs] for coord (F45a), fenced exit code, EINTR errno name

Status: in progress on branch `beta-polish-batch`.
Ledger: F45 row in `docs/REVIEW_LEDGER.md` (do NOT edit the ledger
or `docs/NEXT.md` from this branch).
Decisions (owner, 2026-07-31): F45a make `[nfs]` optional now;
fenced clean shutdown exits non-zero (dedicated code); add EINTR to
`errno_name` before beta freezes the failure-sink wire strings.
Scope: `crates/vamoose-cli` (config + doctor + worker cmd),
`crates/migration-worker` (exit-path plumbing only),
`crates/migration-mover` (ONLY the `errno_name` helper + tests).
Three items in order, one commit each, tests before fix.

## Item 1 — F45a: `vamoose coord` must not require `[nfs]`

### Problem

The unified `Config` (`vamoose-cli/src/config.rs:18-21`) has a
non-optional `nfs: Nfs`, so every subcommand — including `coord`,
which never reads it — refuses configs without an `[nfs]` section.
PR #22 pinned this with
`missing_nfs_section_is_currently_an_error_even_for_coord`
(`config.rs:293-310`), noting the ripple: ~seven `cfg.nfs.*` reads
in `cmd::doctor`.

### Fix (decided)

`nfs: Option<Nfs>`. Then:
- `coord` runs with no `[nfs]` present.
- Subcommands that NEED it (worker run, and whichever others read
  `cfg.nfs` — enumerate them by grep, list them in the report) fail
  fast with a clear, actionable error naming the missing section —
  not an unwrap.
- `doctor`: NFS checks run when the section is present; when absent,
  each NFS check reports as explicitly SKIPPED ("no [nfs] section")
  rather than failing or silently disappearing.
- REPLACE the PR #22 pin test with tests asserting the new contract
  (coord-without-[nfs] parses and passes validation; worker-without-
  [nfs] errors with the clear message). Update the example config
  comments if they claim [nfs] is always required.

### Tests — FIRST, observed red

1. `coord_config_without_nfs_section_is_valid` — RED today (parse
   error).
2. `worker_config_without_nfs_errors_clearly` — asserts the error
   text names `[nfs]` and the subcommand; red or adjusted from the
   old pin test (state which).
3. Doctor skip behavior — unit-level if doctor has test seams;
   otherwise the finest-grained test the structure allows (report
   what that was).

## Item 2 — fenced run exits non-zero

### Problem

A worker that self-fences (claim lost, clock jump, 412 storm) shuts
down cleanly and exits 0 — supervisors cannot distinguish "fenced,
should alert/restart" from "migration complete". PR #22 item A
scoped the watchdog only.

### Fix (decided)

Dedicated exit code **3** for "run ended because the worker fenced",
documented where the CLI documents exit behavior (and in the
`worker run` help text). Clean completion stays 0; existing error
exits keep their current codes (enumerate what they are today in
the report; do not renumber anything existing). Plumb from the
orchestrator's end-of-run state (the `Fence` is already consulted —
find where the run concludes and whether it ended fenced) up to the
process exit in `vamoose-cli`. Prefer a pure
`exit_code_for_outcome(...)` mapping function as the testable seam.

### Tests — FIRST, observed red

1. `fenced_outcome_maps_to_exit_3` — pure-seam unit test. RED via
   the honest-stub convention (stub returns 0) or as a compile-fresh
   seam with the mapping asserted — state which.
2. `clean_outcome_maps_to_exit_0` + existing-error-code pins so a
   future renumbering trips a test.

## Item 3 — EINTR in `errno_name`

### Problem (PR #39 note)

`errno_name` (`migration-mover/src/libnfs/mod.rs`, the match table
near `:320`) has no EINTR arm, so F12 timeout failures record
`errno=4` in the published failures JSONL. Beta freezes that wire
format.

### Fix (decided)

Add `libc::EINTR => "EINTR"` to the table. Then check the F12
timeout-classification carve-out from PR #39
(`classify_shard_error`'s timeout-shape detection in
migration-worker): it must recognize timeout failures regardless of
whether the message carries `"Command timed out"` or the errno name
`EINTR` — if it keys only on the libnfs message strings, confirm
those strings still flow (they come from libnfs, not errno_name)
and add an `EINTR`-named case to its tests either way so the two
representations stay covered.

Fence note: `errno_name` is a safe pure helper — this is the ONLY
thing under `src/libnfs/` you may touch. No extern declarations, no
struct layouts, nothing else in that directory.

### Tests — FIRST, observed red

1. `errno_4_names_eintr` — RED today (falls through to the numeric
   fallback).
2. Classifier: EINTR-shaped error stays retryable/WorkerLocal
   (red or green today — observe and state).

## Constraints (hard fences)

- A PARALLEL session owns all of `crates/migration-coord` — do not
  touch it.
- `migration-mover`: only the `errno_name` function + its tests.
- No new top-level `tests/*.rs` anywhere; no ledger/NEXT.md edits;
  `migration-core/src/claim.rs` untouchable.

## Gate & handoff

Full gate before every commit:
`cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace && cargo deny --all-features check`

One commit per item (tests-first, PR #30/#34 `#[ignore]` red
convention, observed-red quoted). This doc rides commit 1. Commit;
do NOT push. No AI/Claude attribution anywhere.
