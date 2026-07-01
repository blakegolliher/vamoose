# Stop overwriting the per-host failure/downgrade sinks

Status: open — not started.
Ledger: F04 in `docs/REVIEW_LEDGER.md`.
Priority: high — silent loss of the at-least-once reconciliation trail.
Scope: `migration-core/src/layout.rs`, `migration-worker/src/orchestrator.rs`.

## Problem

After each shard, the orchestrator drains the failure and downgrade
sinks and PUTs them to fixed per-host keys
(`orchestrator.rs` ~676–691: `layout::downgrades_key(&host_id)`,
`layout::failures_key(&host_id)`). `drain_jsonl()` clears the buffer,
and S3 PUT replaces the object — so each shard's flush **overwrites
the previous shard's records**. Only the last non-empty shard per
host survives a run.

These JSONL records are the operator's reconciliation trail: per-file
failures are supposed to be retried/re-driven from them. Overwriting
them converts recorded failures into silently unaccounted data loss.

## Required reading

- `crates/migration-core/src/layout.rs` (key scheme + its unit tests)
- `crates/migration-worker/src/orchestrator.rs` flush sites
- `crates/migration-mover/src/failure.rs`, `downgrade.rs` (drain
  semantics)
- `docs/CORRECTNESS_RULES.md` (unconditional PUT is allowed only for
  keys a single writer owns — per-flush unique keys must keep that
  property)

## Acceptance tests — write these FIRST

1. `failures_key_unique_per_flush` (unit, `layout.rs`) — new key fn
   takes host + shard stem + claim epoch (or another
   collision-proof discriminator available at flush time) and
   produces distinct keys for distinct shards AND for the same shard
   re-processed at a higher epoch (post-reclaim re-run must not
   clobber the fenced run's records). Same for downgrades.
2. `flush_two_shards_preserves_both` (red before fix) — drive the
   extracted flush helper (see Fix shape) twice against a mock store
   with two different shards' records; list the failures prefix;
   assert both objects exist and their contents round-trip.
3. `flush_uses_put_if_absent` — the mock store records the
   precondition; assert the flush refuses to clobber an existing key
   (and surfaces a distinguishable outcome if it ever collides,
   rather than silently overwriting).
4. `empty_drain_writes_nothing` — preserve the current "skip empty"
   behavior.
5. Prefix-listing compatibility: whatever consumes these keys today
   (grep for the failures/downgrades prefixes in scripts/ and docs/)
   must still find records by listing the per-host prefix. Add a test
   asserting the new keys still live under the old per-host prefix.

## Fix shape

- Extract the two flush blocks in the orchestrator into a helper
  (e.g. `flush_sinks(store, host_id, shard, epoch, failures,
  downgrades)`) so it is unit-testable against the mock store.
- Key scheme: keep the per-host prefix, add a per-flush unique
  suffix — e.g. `failures/host-<id>/<shard-stem>-e<epoch>.jsonl`.
  Shard stem + epoch is deterministic (no timestamps — replay-safe)
  and collision-proof across reclaims.
- Write with `put_if_absent`; treat an unexpected 412 as a loud
  error, not success.
- Update the key-scheme doc comments in `layout.rs` and any operator
  docs that name the old flat key (grep for `host-<id>.jsonl` /
  `failures_key` in docs/ and scripts/).

## Out of scope / do NOT

- No S3 read-modify-append (S3 has no append; do not fake one).
- Do not change record schemas.
- `aggr-fixture/` fixtures in the parent dir may reference old-style
  keys; ignore them (session artifacts, not repo).

## Definition of done

- [ ] Test 2 written first and observed red.
- [ ] All acceptance tests green; full gate green (fmt, clippy,
      workspace tests, deny).
- [ ] Doc comments + operator docs updated to the new key scheme.
- [ ] Ledger F04 updated; this doc's Status flipped.
