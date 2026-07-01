# Backpressure must be able to recover

Status: open — not started.
Ledger: F14 in `docs/REVIEW_LEDGER.md`.
Priority: high — a tripped worker idles forever.
Scope: `migration-worker/src/backpressure.rs`, its orchestrator
call sites.

## Problem

`Backpressure::update` (`backpressure.rs` ~56) is only fed after a
shard completes; `degraded()` blocks claiming new shards. Once
degraded, no new shard ever completes, so the inputs never update —
the worker sleeps in `degraded:` status forever. Both triggers are
easy to hit legitimately: one shard with >5% failures (e.g. an
EEXIST storm from a reclaimed half-done shard — see F10), or a 60s
throughput sample under 100 MB/s, which small-file shards produce
routinely.

## Required reading

- `crates/migration-worker/src/backpressure.rs` (+ existing tests)
- Orchestrator scan-loop call sites of `degraded()`
- `docs/DESIGN.md` backpressure rationale (what it protects: not
  piling every host onto a struggling destination)

## Acceptance tests — write these FIRST

Extend the existing unit tests; use paused time.

1. `degraded_expires_into_probe_after_cooldown` (red before fix) —
   trip degradation; advance past the cooldown (default suggestion:
   `5 × 60s`, constant next to the existing thresholds); assert the
   gate now permits exactly ONE probe claim (a `probe()` /
   `try_claim_token()` style accessor), not a full reopening.
2. `probe_success_clears_degraded` (red before fix) — feed a healthy
   `update()` after the probe; assert `degraded() == None` and
   normal claiming resumes.
3. `probe_failure_redegrades_with_longer_cooldown` — unhealthy
   update after the probe → degraded again; assert the next probe
   window is later (exponential, capped — suggest ×2 up to 30 min).
4. `healthy_worker_unaffected` — regression: no degradation → no
   probe bookkeeping, `degraded()` stays `None`.
5. `orchestrator_respects_probe_single_flight` — orchestrator-level:
   while degraded-with-probe-available, at most one claim attempt
   passes the gate until its shard completes and `update()` runs
   (mock store op counts over paused time).

## Fix shape

- Keep the trip conditions unchanged — the fix is an exit path, not
  a sensitivity change.
- Add cooldown + single-probe state to `Backpressure` (timestamps
  via the injected clock pattern already used in the worker; no
  `SystemTime::now()` inline).
- Orchestrator: when degraded, sleep as today until probe eligibility;
  then claim one shard, process it, feed `update()`; the state
  machine decides from there.
- Status string: `degraded:<reason>` gains `probe-pending` /
  `probing` variants so operators can see recovery attempts in
  `vamoose status` / the TUI.

## Out of scope / do NOT

- Don't re-tune the thresholds themselves (5% / 100 MB/s) — separate
  conversation with data.
- Don't remove the feature; destination-protection is the point.

## Definition of done

- [ ] Tests 1–2 written first and observed red.
- [ ] All acceptance tests green; full gate green.
- [ ] Ledger F14 updated; this doc's Status flipped.
