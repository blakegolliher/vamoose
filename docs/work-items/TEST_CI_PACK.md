# Test/CI pack: schema-drift rejection tests + MSRV and contract-drift CI

Status: landed — merged in PR #24. F32 tests 1-5 green against the existing rejection
machinery (no silent-accepts found); F43 msrv + contract-drift jobs
added, and `rust-version` corrected 1.75 → 1.91.1 (1.75 was untrue:
Cargo.lock is v4, which needs cargo ≥ 1.78, and the locked aws-sdk
crates require 1.91.1; verified by building the workspace on 1.91.1).
Ledger: F32, F43 in `docs/REVIEW_LEDGER.md`.
Priority: medium — F32 is the M2 incident-1 class with zero regression
coverage; F43 is two promised-but-missing CI guards.
Scope: `migration-core/src/shard.rs` tests,
`mig-walker-rewrite/src/main.rs` tests, `.github/workflows/ci.yml`.
(Repo-root files: `DESIGN.md` is NOT in docs/; `SCHEMA_CONTRACT.md` is
at repo root too.)

Work F32 then F43; independently committable.

## Item 1 — F32: drive a schema-drifted parquet through the reader

### Problem

The M2 incident-1 class — a producer whose parquet schema doesn't
match reader expectations — has no regression test. The rejection
machinery exists and its *classification* is tested
(`worker_error_classification.rs:143-160` asserts hand-constructed
`Error::MissingColumn` / type-mismatch errors classify `Fatal`), but
no test makes `ShardReader` actually **produce** those errors from a
drifted file:

- `validate_schema` (`shard.rs:246-253`) → `Error::MissingColumn` is
  never exercised in the rejecting direction.
- Column **type** mismatch surfaces as
  `Error::Other("column X: unexpected arrow type")` via `type_err`
  (`shard.rs:322-324`) — and only at decode time (`into_rows`), not at
  `open`. Untested, and the open-vs-decode distinction is exactly what
  bit in M2.
- The walker shim's input plucking (`req_string`/`req_u16`/... ,
  `mig-walker-rewrite/src/main.rs:579-639`) rejects missing/mistyped
  walker input columns — also only happy-path tested
  (`round_trip_through_shard_reader`, main.rs:924-1007).

### Acceptance tests — write these FIRST

All red-before-fix in the sense that the *helpers* need extending; the
production code should already reject — if any of these tests reveal a
silent acceptance instead of the expected error, that is a real bug:
fix it in this item and call it out in the report.

1. `open_rejects_missing_required_column` — for each of
   `REQUIRED_COLUMNS` (`schema.rs:63`): write a parquet without that
   column, assert `ShardReader::open` → `Err(Error::MissingColumn(c))`.
   The existing `ShardWriter` test helper (`shard.rs:451-504`)
   hardcodes `canonical_schema()` — extend it to accept a custom
   schema/batch (e.g. `with_schema(...)` or a raw-batch write path).
2. `decode_rejects_mistyped_required_column` — write `size` as Utf8
   (or `path` as u64); assert `open` **succeeds** (name-only check —
   pin that this is by design, with a comment) and the first
   `into_rows()` item is `Err(Error::Other(...unexpected arrow type...))`.
   Assert where it surfaces, not just that it errors.
3. `walker_shim_rejects_missing_input_column` — feed `rewrite_shard` a
   walker parquet missing a required input column (reuse
   `synthetic_walker_batch`, main.rs:822, minus a column); assert the
   "missing required column" error.
4. `walker_shim_rejects_mistyped_input_column` — same with a wrong
   Arrow type; assert the "is not Utf8/UInt16/..." error.
5. `drifted_errors_classify_fatal` — bridge test: take the errors
   actually produced by tests 1–2 (not hand-built ones) and assert
   `classify_shard_error` maps them `Fatal`. This closes the gap the
   existing classification tests leave.

## Item 2 — F43: MSRV job + SCHEMA_CONTRACT drift check

### Problem

`Cargo.toml:21` declares `rust-version = "1.75"` but every CI
toolchain step uses `dtolnay/rust-toolchain@stable`
(`ci.yml:31/:58/:101`) — nothing verifies the promise.
`SCHEMA_CONTRACT.md:12-13` promises "CI in either repo should fail if
its copy diverges from the other" (vendored byte-identical in
`nfs-walker`) — no job does this.

### Work

6. **MSRV job**: add a `msrv` job to `ci.yml` mirroring the `test` job
   but with `dtolnay/rust-toolchain@1.75` and `cargo check --workspace
   --all-targets --locked` (check, not test — MSRV verifies
   compilability; keep it fast). Same apt deps (`libnfs-dev
   pkg-config`). If 1.75 genuinely cannot build the workspace (a dep
   or feature crept past it), do NOT silently bump: report the true
   MSRV, set `rust-version` to it in the same commit, and say so —
   the declared value must be the tested value.
7. **Contract drift check**: add a job that fetches
   `nfs-walker/SCHEMA_CONTRACT.md` from the upstream repo and diffs it
   byte-wise against ours, failing on divergence. The nfs-walker repo
   location/token availability is environment-specific: if the repo is
   not reachable from CI (private, no token configured), implement the
   job behind an `if: vars.NFS_WALKER_REPO != ''` guard (documented in
   the workflow) and additionally add the always-on cheap half: a
   repo-local test asserting `schema.rs`'s canonical column
   names/types match the table in our `SCHEMA_CONTRACT.md` (parse the
   contract's column table in a unit test — drift between code and
   contract file is the failure mode we can always catch locally).
8. CI hygiene: whatever lands must keep the four existing checks
   (fmt+clippy, build+test, deny, licenses) untouched — additive jobs
   only.

## Out of scope / do NOT

- No new walker features; the shim stays a shim.
- Do not weaken any existing reader rejection to make a test pass —
  a discovered silent-accept is a bug to fix, not to pin.
- No workflow-wide toolchain pinning changes; MSRV is an additional
  job.
- DESIGN.md/CLAIM_PROTOCOL.md freshness (F44) is handled separately —
  don't touch them here.

## Definition of done

- [ ] Tests 1–5 green (any discovered silent-accept fixed and
      reported); MSRV job green in CI; drift check present (full or
      guarded+local-half per above).
- [ ] Full gate green (fmt, clippy, workspace tests, deny).
- [ ] Ledger F32/F43 updated; this doc's Status flipped.
