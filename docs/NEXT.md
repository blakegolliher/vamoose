# What's next

Living tally. Update this whenever an item lands or a decision is
made; `docs/REVIEW_LEDGER.md` rows stay the per-finding source of
truth. State as of 2026-07-30 (post PR #40): **43 of 45 ledger
findings landed; 2 open** (F15, F45). What remains is those two
design items, hardware verification (§2), and small follow-ups (§3).

## 1. Design decisions needed (blocks the remaining ledger findings)

| Finding | Decision to make |
|---|---|
| F15 | Hardlink groups straddling batch boundaries: fold into the multi-pass design (MULTI_PASS_MOVER phases 3–8) or pre-shard by group. |
| F45 | Coord's unused `[nfs]` config requirement (pinned in PR #22 — not a one-liner) + runtime mutex held across S3 PUT during chunk flush (perf at 10k events/s). |

The 2026-07-30 design-decision batch is DONE: both design docs
(`docs/design/PROTECTED_FFI_DATA_PLANE.md`,
`docs/design/COORD_TRUST_AND_FENCING.md`, merged in PR #36 with the
owner's calls recorded) are fully executed — F12/F11/F09 in PR #39,
F19 in PR #37, F20 in PRs #38+#40. F15 still rides the multi-pass
design; F45 still needs its own call.

## 2. Hardware verification (needs the VAST rig; flips rows to `verified`)

- [ ] F01/F30 — `scripts/fast-reclaim-drill.sh`: kill -9 → reclaim
      latency grows by ≤ one grace window; no theft cascade.
- [ ] F06/F07 — `pipelined_copy_smoke` (`#[ignore]`d) re-run.
- [ ] F10 — `hardlink_replay_is_idempotent` (PR #30) and
      `symlink_replay_is_idempotent` (PR #34) in
      `file_mover_smoke.rs` (`#[ignore]`d): replayed rows succeed;
      hardlinks share an inode (`nlink == 2`), symlink readlink
      byte-equals the intended target.
- [ ] F08 — MANUAL_VERIFY.md check [5]: migrate a `4755` root-owned
      file; destination `stat` must show `4755`.
- [ ] F12 — `libnfs_async_integration.rs` bounded-timeout case
      (`#[ignore]`d, PR #39): mount vs blackholed address fails
      within ~2× `rpc_timeout_ms` instead of hanging.
- [ ] F11 — `error_path_leaves_context_usable_after_drain`
      (`#[ignore]`d, PR #39): forced write failure with reads in
      flight; context stays usable.
- [ ] F09 — `sync_write_fsync_commit_readback` in
      `file_mover_smoke.rs` (`#[ignore]`d, PR #39); plus the
      MANUAL_VERIFY.md crash-drill note (kill the server mid-run) —
      rig exercise, not a test.

## 3. Small follow-ups (codeable now, none urgent)

- [ ] `clean-partials` `--lease-timeout-sec` override: the liveness
      gate uses the protocol default (180s), so deployments with a
      longer configured `worker.lease_timeout_sec` could pass the
      gate while a worker still holds a claim (documented in
      `clean_partials.rs`; PR #31 note).
- [ ] `records.rs` `ClaimRecord.epoch` doc comment says "increments
      on each heartbeat" — stale v1 wording; v2 owners never rewrite
      a held claim (claim.rs header). Docs-only sweep in
      migration-core; flagged independently by two sessions.
- [ ] deny.toml: `Unicode-DFS-2016` license allowance no longer
      matches anything in the tree (pre-existing warning).
- [ ] F20 residue (PR #40 note): non-attributed kinds
      (`ClaimConflict*`, `VerifyFileMismatch`) can't advance the
      client_seq HWM (no caller attribution on the envelope), so a
      stamped tail entry of those kinds re-applies on resend unless
      a later attributed entry covers it. Unreachable with today's
      worker; closing it = caller attribution on the envelope
      (small additive design call).
- [ ] `errno_name` has no EINTR entry (PR #39 note): row-level
      timeout failures log as `errno=4` in the failures JSONL;
      adding it changes published failure strings — owner call.
- [ ] DESIGN.md's "Configuration" example TOML doesn't list
      `rpc_timeout_ms` (PR #39 note); `examples/worker.toml` does.
- [ ] F24 residue (noted in its ledger row): log-side coalescing
      decision, worker-row eviction, trailing-edge flush (a burst's
      last ProgressDelta is dropped from the live bus until the next
      event; log/replay unaffected).
- [ ] Coord: `/jobs` (or healthz-scoped snapshot) carrying an
      `as_of_seq` would make TUI bootstrap atomic and retire the
      torn-walk retry loop (see LESSONS.md).
- [ ] Worker exit codes: a *fenced* run that shuts down cleanly still
      exits 0 (PR #22 item A scoped the watchdog only) — decide
      whether supervisors should see fenced ≠ clean.
- [ ] `JobId` serde(transparent) deserialization bypasses `new()`
      validation (noted in PR #22 item D).
- [ ] SCHEMA_CONTRACT.md wording vs code (noted in PR #24): Unknown=0
      file type raises `CorruptRow` (contract says `ShardCorrupt`);
      contract's "required" table lists 14 columns, code enforces the
      5 non-nullable ones. Align the prose.
- [ ] Classifier bridge test in migration-worker driving
      reader-produced drift errors through `classify_shard_error`
      directly (PR #24 deviation note — fence has lifted).
- [ ] s3.rs: `get`/`list`/`head_object` errors still typed
      `Error::Other`; typed as `S3` they'd classify WorkerLocal
      without the retry wrapper's retry-everything blanket (PR #27
      scope note).

## 4. Operational switches

- [ ] Set the `NFS_WALKER_REPO` repository variable (e.g.
      `blakegolliher/nfs-walker`, plus `NFS_WALKER_TOKEN` secret if
      private) to activate the cross-repo SCHEMA_CONTRACT drift job.

## Done (for orientation)

Rounds 1–2 of the review ledger: F01–F08, F13/F14, F16–F18,
F21–F35, F37–F44 — landed via PRs #11–#28; ledger sweep in PR #29.
Quick-calls round (2026-07-13): F36 landed in PR #31
(`clean-partials` real + stubs bail); F10 landed across PR #30
(hardlink half) and PR #34 (symlink half).
Design-decision batch (2026-07-30): design docs decided + merged
(PR #36); F19 delete-then-create lease refresh (PR #37); F20
worker-event trust in two phases — allow-list + binding (PR #38),
client_seq idempotency (PR #40); protected-FFI batch F12+F11+F09
(PR #39, two new externs total; the F11 .so audit confirmed the
close-while-inflight UAF is real in the linked libnfs).
Process and pitfalls: `docs/LESSONS.md`.
