# Lessons learned — review-ledger campaign (2026-07, PRs #11–#29)

Hard-won rules from landing 36 ledger findings in parallel sessions.
Read this before running the next campaign; every item below cost at
least one broken build, conflict, or wasted CI cycle to learn.

## Orchestration

- **One ledger-touching branch at a time.** `REVIEW_LEDGER.md` is a
  markdown table; edits to adjacent rows do not auto-merge (PR #12 vs
  #11 conflicted exactly this way). Sessions never edit the ledger;
  the coordinator batches status sweeps into a single branch per
  round.
- **Fence parallel sessions by interface, not just by file.** Two
  collisions happened despite file fences:
  - PR #18 changed `scan_shards`'s signature while PR #19's new test
    called it — each branch green alone, main broken after both.
    Caught pre-merge only because the coordinator looked. Stack
    dependent branches instead (as F03-on-F02 and F14-on-F13 were).
  - PR #25's stream caps (1 Hz ProgressDelta coalescing) broke PR
    #26's live-tail test — a *semantic* interface. If a sibling PR
    changes observable behavior your branch relies on, rebase and
    re-gate against merged main before pushing; CI builds the merge
    commit, but a local gate on the fork point tests stale reality.
- **Rebase onto current main before the final gate whenever a sibling
  merged mid-flight.** Same reason as above, cheaper than diagnosing
  it in CI.
- **Agents commit locally and never push; the coordinator reviews the
  diff, re-runs the full gate independently, and owns push/PR.** This
  caught fence violations, let PR bodies flag judgment calls (e.g.
  F42's manifest-swap behavior change), and made the SSH-killed F04
  session recoverable without losing work.
- **Work-item docs are contracts.** Fresh-context sessions succeed in
  proportion to doc precision: write them from a code scout (exact
  file:line, existing test patterns to mirror, known traps), mark
  which tests must be red first, and require the agent to report doc
  inaccuracies and deviations rather than silently adapting.
- **One commit per sub-item** in batched work items — reviewable, and
  cherry-pickable if one item stalls.

## CI / infrastructure

- **`ld terminated with signal 7 [Bus error]` = the runner's disk is
  full**, not a flaky linker. The linker mmaps its output; ENOSPC on
  a mapped page is delivered as SIGBUS. Every top-level `tests/*.rs`
  file links its own full-workspace debug executable (arrow + aws ≈
  multi-GB each); two PRs in a row tipped the cliff by adding one
  file each. Fixes: fold new tests into existing binaries
  (subdirectory-module pattern, `#[path] mod`), and PR #28's durable
  headroom (`CARGO_PROFILE_DEV_DEBUG=line-tables-only` + preinstalled
  bloat removal in the build+test job).
- **A declared MSRV that CI doesn't build is fiction.**
  `rust-version = "1.75"` couldn't even parse the lockfile (v4 needs
  ≥1.78) and the aws-sdk needed 1.91.1. The `msrv` job reads the
  value from Cargo.toml, so declared ≡ tested forever.
- **Pipelines eat exit codes.** `cargo test … | tail` returns tail's
  status; a failing gate sailed through a `&&` chain and pushed.
  `set -o pipefail` or keep the deciding command unpiped.
- **GitHub mergeability shows UNKNOWN for minutes after pushes.**
  Don't poll — `git merge-tree --write-tree origin/main branch`
  answers locally and instantly.
- **Key CI watchers to the commit SHA, not the PR.** A
  `gh pr checks N` until-loop exits on the PREVIOUS run's completed
  results when you re-push (the new run hasn't registered as pending
  yet) — it did so twice in a row on PR #34. Poll
  `gh run list --commit <sha>` for the pushed SHA until a run exists
  AND completes, then read the checks.
- **After a merge, verify the branch TIP is what merged.** PR #34 was
  merged while its final commit (a license-bundle regen) raced the
  merge click and silently missed main, leaving CI broken for every
  later branch. One line catches it:
  `git merge-base --is-ancestor <branch-tip> origin/main`.
- **Re-running a failed CI job reproduces deterministic failures.**
  Rerun-once is a fine flakiness probe, but read the log signature
  first: of this campaign's three "flaky" failures, one was infra
  (disk), one was a cross-PR semantic interaction, one was a real
  correctness bug. None were actual flakes.

## Testing

- **Make failure messages carry the diagnosis.** The unreproducible
  "resync never converged" CI failure became a 1-in-6 local repro the
  moment the assert printed connection status, driver liveness,
  per-map equality, seq positions, and the recent reducer-input tail.
  Instrument first, theorize second.
- **Bimodal failures (instant pass vs full-deadline burn) are wedges,
  not slowness.** Raising the deadline is only correct when the
  passing-time distribution actually has a tail; 0.09s-or-forever
  means a liveness bug.
- **Adversarial harnesses earn their keep.** The bus-overflow test
  (tiny channel, stalled consumer, 128 KiB events) found a genuine
  production bug — the torn-bootstrap double-count — that no clean
  harness would have hit.
- **Outbound caps starve overflow-forcing harnesses.** After the F24
  bus caps, the resync test's 80-event same-class ErrorEmitted flood
  put only ~10 frames on the live bus — whether that still overflowed
  the 4-slot channel depended on socket-buffer timing (passed solo in
  0.1s, failed under full-workspace load). A harness that must
  overflow a rate-limited path needs an event kind the limiter
  ignores (here: VerifyFileMismatch, uncapped and a reducer no-op).
  Audit flood-based tests whenever a new cap lands.
- **Envelope-seq dedup cannot see into a snapshot.** Replaying events
  whose effects are already baked into REST-fetched state
  double-counts aggregates. A torn multi-request walk has NO sound
  resume cursor (pre-walk double-counts, post-walk under-counts);
  the only sound outcomes are a clean pass (same cursor before and
  after) or a retryable error. True generally, not just for the TUI.
- **Test at seams, not through hardware.** Every FFI- or
  clock-coupled fix in the campaign was made testable by extracting a
  pure function or a tiny trait (ReorderState, AttrOp/plan_attr_ops,
  classify_put_response, DriftClock, AttrExec, the download seam) —
  and the extraction commit goes in first, pinned green, before any
  behavior change.
- **`tokio::time::pause()` cannot cover wall-clock math** (F01's
  grace windows) — those tests run in real time; budget for it.
- **Paused-time + FakeStore op-log assertions** ("exactly one claim
  attempt in this window", "zero writes after the fence") are the
  campaign's workhorse pattern; extend the op log/failure rigging
  rather than inventing new mocks.

## Protocol / design patterns that recurred

- **Fence every store write on ownership, not just the obvious ones**
  (F02: flush/snapshot/shutdown; F23: archive DELETEs). Anything a
  deposed node could write, a successor can lose.
- **Deterministic, replay-safe unique keys beat timestamps**
  (failures/host-x/stem-eEPOCH.jsonl); `put_if_absent` + advance
  beats in-memory counters (audit keys after crash).
- **Ack ⇒ durable, and archive ⇒ covered-by-snapshot.** Any
  operation that makes data invisible to one path must be gated on
  the artifact that makes it recoverable through another.
- **Conservative classification defaults:** unknown errors → Fatal
  (loud, terminal) rather than retry loops; unknown event kinds →
  skip-with-cursor-advance (tolerate the future) rather than
  disconnect.
- **Docs are code.** Spec/code drift appeared three separate ways
  (R6 ceil-vs-floor, layout.rs's phantom command_id de-dup, DESIGN.md
  still describing v1). When behavior lands, sweep the prose in the
  same PR or a same-day docs PR — a wrong spec is worse than none.
