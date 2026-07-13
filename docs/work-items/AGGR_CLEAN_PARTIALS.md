# mig-aggr: implement `clean-partials`, bail the other stubs

Status: landed — merged in PR #31.
Ledger: F36 in `docs/REVIEW_LEDGER.md` (do NOT edit the ledger or
`docs/NEXT.md` from this branch — the coordinator sweeps those).
Priority: medium (every `mig-aggr` subcommand today is a `todo!()`
that PANICS on invocation; nothing in the system cleans orphaned
`.partial` files left by fenced/killed workers).
Scope: `crates/migration-aggr` only, plus its `Cargo.toml` deps.

## Decision (made by the project owner, 2026-07-13)

- Implement `clean-partials` for real: a lease-aware sweep of
  orphaned `.partial` files, dry-run by default.
- The five observability stubs (`watch`, `summary`, `metrics`,
  `inspect`, `verify`) become clear `anyhow::bail!` errors instead of
  `todo!()` panics — e.g. ``bail!("`mig-aggr watch` is not
  implemented; use the migration-tui dashboard")``. Delete the
  now-dead stub modules if that leaves the code simpler.
- Explicitly rejected for now: implementing `verify` (needs
  src+dst access, bigger scope) and gutting everything (leaves
  `.partial` cleanup unowned).

## Design

`clean-partials` gains a required `--dest-root <path>` argument: the
LOCALLY MOUNTED destination export. The sweep walks the tree with
`std::fs` / `walkdir`-style recursion — NO libnfs, NO FFI in this
crate, ever. The existing S3 args (`--endpoint/--region/--bucket`)
are used read-only to check run liveness.

What counts as a partial: a file whose NAME matches the mover's
pattern from `migration-mover/src/paths.rs::partial_path` —
`.<base>.<host>.<pid>.partial` (hidden dot-prefixed sibling, four
dot-separated fields ending in `partial`, where `<pid>` is numeric).
Do NOT depend on migration-mover for the pattern (that would drag FFI
linkage into aggr); reimplement the matcher here and add a test that
pins the exact format against a literal example produced by
`partial_path`'s documented shape, with a comment cross-referencing
`paths.rs` so drift is caught in review.

Safety gate (the important part): the sweep must refuse to delete
while the run may still have live workers. Add `migration-core` as a
dependency and use the `ClaimStore` trait (`claim.rs:186`) READ-ONLY:

- List claims in the bucket; if ANY claim's lease is still live
  (unexpired heartbeat per the protocol's existing freshness rules —
  reuse whatever migration-core already exposes for "is this lease
  expired", do not invent a new rule), refuse to delete: print the
  live claims and exit non-zero.
- `--force` overrides the liveness gate (operator says the run is
  dead); still prints what the gate would have said.
- `--dry-run` lists every matching file with size and mtime, deletes
  nothing, exits 0. THIS IS THE DEFAULT unless `--delete` is given —
  change the CLI so destructive behavior is opt-in: keep `--dry-run`
  as an accepted no-op alias, add `--delete` as the flag that arms
  deletion. A bare `mig-aggr clean-partials` must never delete.

Deletion pass: delete only regular files whose name matches the
pattern; never follow symlinks; count and report deleted files and
bytes; a per-file deletion error is reported and skipped, not fatal
(exit non-zero at the end if any deletion failed).

## Constraints (hard fences)

- MUST NOT modify `crates/migration-core/src/claim.rs` or anything
  outside `crates/migration-aggr/` (except the workspace `Cargo.lock`
  as a natural consequence of the dep change).
- MUST NOT add libnfs/FFI or `migration-mover` as a dependency.
- MUST NOT add any new top-level `tests/*.rs` file (CI disk cliff) —
  all tests live in `#[cfg(test)]` modules inside `src/`.
- ClaimStore usage is READ-ONLY (list/get). No writes to the bucket.

## Acceptance tests — write these FIRST, observe them red

Structure the sweep as pure-ish functions over injected inputs so
tests need no S3: e.g. `is_partial_name(&OsStr) -> bool`,
`plan_sweep(root) -> Vec<Candidate>`, and a liveness check taking
`&dyn ClaimStore` (use migration-core's `FakeStore`, `claim.rs:470`,
as the double).

In `#[cfg(test)]` modules (tempdir for fs tests):

1. `partial_name_matcher_pins_mover_format` — accepts
   `.data.bin.host-a.12345.partial`; rejects the non-hidden
   `data.bin.host-a.12345.partial`, a name with non-numeric pid,
   a plain `.hidden` file, and a final name `data.bin`.
2. `dry_run_deletes_nothing` — tempdir with matching + non-matching
   files; default invocation reports the matches and every file
   still exists afterwards.
3. `delete_removes_only_matches` — with `--delete` armed and a dead
   store, matching files are gone, non-matching (including a decoy
   `.partial`-suffixed DIRECTORY and a symlink whose name matches)
   survive.
4. `live_lease_blocks_delete` — FakeStore holding one live claim →
   sweep refuses, non-zero, nothing deleted.
5. `force_overrides_liveness` — same store + `--force --delete` →
   deletion proceeds.
6. `stub_subcommands_bail_not_panic` — calling the five stub run
   functions returns `Err` (assert message names the subcommand);
   none panic.

Red-before-fix: tests 6 can run against the current `todo!()` stubs
and MUST be observed red (panic ≠ Err) before the bail! change. For
the sweep, commit tests against honest stubs (matcher returns false,
plan returns empty) and observe failures, then implement. State what
was observed red in the test commit message.

## Gate & handoff

Full gate before every commit:
`cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace && cargo deny --all-features check`

Note `cargo deny` runs with the new migration-core dep in the graph —
if it flags anything, stop and report rather than editing deny.toml.

Suggested commits: (1) tests red + honest stubs, (2) matcher + sweep
implementation, (3) bail! conversion + main.rs doc header update,
plus this doc. Commit to this branch; do NOT push — the coordinator
reviews the diff, re-runs the gate, and pushes. No AI/Claude
attribution anywhere (no Co-Authored-By trailers).
