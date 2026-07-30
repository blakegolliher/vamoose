# What's next

Living tally. Update this whenever an item lands or a decision is
made; `docs/REVIEW_LEDGER.md` rows stay the per-finding source of
truth. State as of 2026-07-13 (post PR #34): **38 of 45 ledger
findings landed; 7 open.** Everything open is design-decision work
(§1), hardware verification (§2), or small follow-ups (§3).

## 1. Design decisions needed (blocks the remaining ledger findings)

Each needs a call from the project owner before it can become a
test-first work item:

| Finding | Decision to make |
|---|---|
| F09 | NFS COMMIT on the sync path: add an `nfs_fsync` binding (protected-FFI design work) vs document VAST-only durability (NVRAM ack). |
| F11 | Error path sends `Close` with pread/pwrite in flight (potential UAF inside libnfs): drain-before-close vs verify against the linked .so. |
| F12 | Data-plane I/O deadlines: needs a new `nfs_set_timeout` FFI binding + config plumbing — design together with F09/F11 as one protected-FFI batch. |
| F15 | Hardlink groups straddling batch boundaries: fold into the multi-pass design (MULTI_PASS_MOVER phases 3–8) or pre-shard by group. |
| F19 | Lease fencing tokens for coord (refresh is HEAD-then-unconditional-PUT; F02's write gate is the containment, not the fix). |
| F20 | Worker-endpoint trust boundary: EventKind allow-list per role, idempotency keys, atomic batches. |
| F45 | Coord's unused `[nfs]` config requirement (pinned in PR #22 — not a one-liner) + runtime mutex held across S3 PUT during chunk flush (perf at 10k events/s). |

Design docs are DRAFTED for the two batches and await the owner's
calls (drafted 2026-07-13, on the `design-docs` branch until then):
`docs/design/PROTECTED_FFI_DATA_PLANE.md` (F09+F11+F12, decisions
D1–D5) and `docs/design/COORD_TRUST_AND_FENCING.md` (F19+F20,
decisions D1–D6). F15 rides the multi-pass design. The former F10
and F36 quick calls were made 2026-07-13 and landed (§Done).

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
Process and pitfalls: `docs/LESSONS.md`.
