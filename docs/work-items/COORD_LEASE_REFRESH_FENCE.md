# F19: coord lease refresh via delete-then-create (no more unconditional PUT)

Status: in progress on branch `f19-lease-refresh-fence`.
Ledger: F19 in `docs/REVIEW_LEDGER.md` (do NOT edit the ledger or
`docs/NEXT.md` from this branch).
Design: `docs/design/COORD_TRUST_AND_FENCING.md` Part 1, DECIDED
2026-07-30, Option A. If implementation reality contradicts the
design doc, STOP and report.
Scope: `crates/migration-coord/src/lease.rs` (refresh + its tests
+ doc comments) and, only if signatures force it, the refresh call
site in `ticks.rs`. NOTHING else.

## The bug (current code)

`refresh` (`lease.rs:263-290`) is HEAD-then-UNCONDITIONAL-PUT: it
compares the held etag via `head`, then rewrites the lease body
with `store.put` (last-write-wins). Interleave: deposed coord A
HEADs its still-current etag → candidate B completes a takeover
(conditional delete+create, new etag) → A's unconditional PUT
overwrites B's fresh lease and returns Ok. A keeps operating as a
believed owner; the F02 write gate never trips because its only
input is refresh returning `LeaseLost` (`ticks.rs:113-117`).

## The fix (decided)

Refresh reuses the takeover atoms, in order:

1. `delete_if_match(LEASE_KEY, handle.etag)` — on 412 / object
   gone: return `Err(Error::LeaseLost)`.
2. `put_if_absent(LEASE_KEY, body with new expires_at)` — on
   `AlreadyExists` (a candidate slipped into the gap): return
   `Err(Error::LeaseLost)`. On `Created(etag)`: return the new
   `LeaseHandle` (same `lease_id` and identity fields — only
   `expires_at` changes; `lease_id` still rotates ONLY on takeover).

Every failure mode of every step resolves to the owner concluding
`LeaseLost`, which trips the F02 gate. The crash window between
delete and create (lease object briefly absent) is an ordinary
cold-acquire opportunity for candidates — an availability blip,
never split-brain. A transient store error mid-refresh forfeits the
lease; that is the accepted fail-safe direction.

Rewrite the doc comment at `lease.rs:263-278`: it currently
justifies the unconditional PUT ("the alternative — a conditional
PUT — is the primitive VAST S3 doesn't honor"); the new text must
explain that delete-then-create achieves a conditional refresh from
the two VAST-safe atoms, and why every failure maps to LeaseLost.
**No `PUT If-Match` anywhere** — that rule is absolute.

## Constraints (hard fences)

- Acquire, takeover, release, and the `LeaseBody`/`LeaseHandle`
  shapes are correct and tested — do NOT touch them beyond what the
  refresh change strictly requires.
- MUST NOT touch `crates/migration-core/src/claim.rs`.
- MUST NOT touch `server/`, `runtime.rs`, `state.rs` (a parallel
  session owns the worker-endpoint surface).
- No new top-level `tests/*.rs` files; tests live in `lease.rs`'s
  existing `#[cfg(test)]` mod (FakeStore/CoordStore test doubles
  already used there — `lease.rs:338-517`).

## Acceptance tests — write these FIRST, observe them red

The honest red for the headline bug: the current refresh uses an
unconditional `put`; the new one must never. If the store double
used by lease tests can count calls per method (or can be extended
within `lease.rs`'s test mod / the existing double's home), the
sharpest test is:

1. `refresh_never_writes_unconditionally` — run a successful
   refresh; assert zero `put` (unconditional) calls and exactly one
   `delete_if_match` + one `put_if_absent`. RED today (current code
   makes exactly one unconditional `put`).

Behavioral tests (red or green as noted):

2. `refresh_rotates_etag_and_extends_expiry` — successful refresh
   returns a handle with a NEW etag and later `expires_at`, same
   `lease_id`; a second refresh with the new handle also succeeds.
   (Green today by accident — must stay green.)
3. `refresh_after_takeover_returns_lease_lost` — the existing test
   (`lease.rs:414-432`) must stay green unmodified (or minimally
   adapted if the double changed).
4. `refresh_loses_create_race_returns_lease_lost` — delete succeeds,
   then a candidate's `put_if_absent` wins before ours (seed the
   store between the two calls if the double allows interleaving,
   or simulate by pre-creating after delete): refresh returns
   `LeaseLost`, does not panic, and the returned error is the same
   variant `ticks.rs` already maps to `mark_lease_lost`.
5. `refresh_delete_conflict_returns_lease_lost` — the held etag no
   longer matches at delete time: `LeaseLost`.

If the existing double cannot express test 4's interleave, say so
in the report and cover it by construction (the code path is a
straight-line two-call sequence; test 1 + 5 then pin both halves).

## Gate & handoff

Full gate before every commit:
`cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace && cargo deny --all-features check`

Two commits: tests-red (with the PR #30/#34 `#[ignore]`-marker
convention and observed-red output quoted), then the fix. This doc
rides the first commit. Commit; do NOT push. No AI/Claude
attribution anywhere.
