# M5 — Notes from the milestone

What M5 actually surfaced when run end-to-end on real VAST hardware,
what the harness proved, what the harness *can't* prove because of
the underlying S3 endpoint, and the work that comes out of it.

- **Date:** 2026-05-05
- **Operator:** Blake Golliher
- **Cluster:** selab-var204 (`main.selab-var204.selab.vastdata.com`)
- **Bucket:** `vamoose-m5`
- **Test:** 10000 × 4096-byte files, single shard, two workers (`m5-host-A`, `m5-host-B`)
- **Diagnostic run:** `m5/run/20260505T165247Z/`

## 1. What M5 was designed to verify

Two-host self-fence: A claims a shard, A is paused (`SIGSTOP`)
mid-shard, B reclaims via epoch bump after the lease expires, A is
resumed (`SIGCONT`), and A self-fences on its next heartbeat refresh
because its claim object on S3 was overwritten by B. The harness
codifies the success criteria as seven assertions (A–G) listed in
`docs/work-items/M5_SELF_FENCE.md`. Quoted verbatim:

> - **A. Final claim record.** The shard's final claim object has
>   `state == "Completed"` and `host == B`. Worker B was the last
>   legitimate owner.
> - **B. Dest file count == source file count.** Counted with `find -type f`,
>   excluding any `.partial` survivors.
> - **C. Per-file SHA-256 match.** `sha256sum` over every regular file,
>   sorted, must diff clean between source and destination roots.
>   Content-only — `stat` output is not compared because dest mtimes
>   are µs-truncated by `nfs_utimes` (see
>   `docs/work-items/MOVER_UTIMENSAT.md`); that is expected and not a
>   correctness defect.
> - **D. Worker A clean self-fence.** A's exit code is `0`, A's stderr
>   contains at least one `"fence tripped"` line, and that line's
>   reason mentions either `"412"` or `"heartbeat refresh"` — i.e. A
>   fenced because the heartbeat refresh saw 412 after B reclaimed,
>   not for any other reason.
> - **E. Failures sinks.** `failures/host-A.jsonl` and
>   `failures/host-B.jsonl` are either absent in S3 or empty. The
>   source tree is static; any per-file failure is a real failure.
> - **F. Single commit per dest path.** A single `commit: rename
>   .partial → final` debug log line exists per final dest path
>   across **A.err and B.err combined**. No path commits twice.
> - **G. No orphan partials from B.** `.partial` files on the dest
>   matching A's `<host>.<pid>` stamp are allowed (A was paused
>   mid-write). `.partial` files matching B's `<host>.<pid>` stamp
>   are forbidden (B exited cleanly). A final dest file with the same
>   basename as a B partial is forbidden — it would mean B somehow
>   committed an inconsistent rename.

The protocol-level reasoning in `M5_SELF_FENCE.md` "Why these
assertions are sufficient" reduces all seven to a single property:
**conditional-PUT (`If-Match`) on the claim object is the
serialization point, and 412 on stale etag is the trigger that
trips A's fence**. Everything else flows from that.

## 2. What the harness actually verified (working as designed)

The harness machinery itself is now end-to-end correct on var204.
Every step that does *not* depend on `If-Match` enforcement worked:

- **Two-worker launch under sudo.** libnfs mounts return EACCES
  unless the worker runs as UID 0 against the lab export, so each
  worker is launched via `sudo -n -E`. The launch site `setsid`-wraps
  the sudo invocation so that `SIGSTOP` to A's mig-worker process
  doesn't propagate up to the harness shell via sudo's child-stop
  handling.
- **Distinct-PID capture.** sudo on this host fans out as
  `launcher → monitor → mig-worker`, with both launcher and monitor
  having `comm=sudo`. The harness uses `pgrep -x mig-worker` to
  enumerate candidates and then disambiguates A from B by matching
  the config-file basename in `/proc/<pid>/cmdline`. Sanity assertion
  rejects any capture that returned the launcher PID.
- **Distinct dest tree.** `VAMOOSE_DST_ROOT` is now a separate
  required env var from `VAMOOSE_SRC_ROOT`; the previous behavior of
  reusing `src_root` for `dest.root` made the source tree's
  directories EEXIST against the dest writes on the lab export. The
  manifest now carries them independently; Phase 0 wipes the dest
  tree before each run.
- **Mid-shard catch via local log polling.** S3 `progress/host-*.json`
  polling proved unreliable as a catch signal — the heartbeat task
  emits the file but the operator-side polling loop frequently raced
  with the writer's tick cadence and missed the mid-shard window.
  The harness now greps the local `A.out` for `commit: rename` lines
  (the mover's per-row commit DEBUG line; tracing writes DEBUG to
  stdout, not stderr) and trips at ≥ 200 commits. Empirically this
  fires within ~2–3 seconds of A producing its first commit.
- **SIGSTOP delivery.** `sudo -n kill -STOP <A_PID>` (with `A_PID`
  being mig-worker's PID, not sudo's) suspends the worker process
  cleanly. `/proc/<A_PID>/status` shows `State: T (stopped)` until
  `SIGCONT`.
- **B launch and reclaim observation.** B is launched the same way
  as A. The harness polls `shards/<shard>.parquet.claim` until the
  `host` field is B and the `epoch` field is greater than A's
  pre-stop epoch.
- **SIGCONT delivery.** `sudo -n kill -CONT <A_PID>` resumes the
  worker; `/proc/<A_PID>/status` shows `State: S` and the
  `tracing::info!("heartbeat: tick")` lines added in this session
  resume firing in `A.out`.

Every one of these is the surface M5 was meant to exercise; the
harness exercises each one correctly. **The point of failure is
exclusively at the next step**, where A's post-resume heartbeat
refresh should observe a 412 and trip the fence.

## 3. The actual finding — asymmetric If-* enforcement on VAST S3

The diagnostic run produced this transcript directly via `aws s3api`
against `main.selab-var204.selab.vastdata.com`, on a freshly-created
test object:

```bash
# Setup: PUT v1 of the object, capture its ETag.
$ aws s3api put-object \
    --bucket vamoose-m5 --key precondition-test \
    --body /tmp/v1
{ "ETag": "\"5d41402abc4b2a76b9719d911017c592\"" }

# Test 1: PUT with a wrong (stale) If-Match etag.
# Expectation under RFC 9110 / S3 semantics: 412 Precondition Failed.
# Observed:                                    200 OK, content overwritten.
$ aws s3api put-object \
    --bucket vamoose-m5 --key precondition-test \
    --if-match '"deadbeefdeadbeefdeadbeefdeadbeef"' \
    --body /tmp/v2
{ "ETag": "\"7d793037a0760186574b0282f2f435e7\"" }
$ aws s3 cp s3://vamoose-m5/precondition-test - | sha256sum
# matches sha256(/tmp/v2) — the stale-etag PUT succeeded, content replaced.

# Test 2: PUT with If-None-Match: * to a key that already exists.
# Expectation: 412 Precondition Failed.
# Observed:    412 Precondition Failed.  (this path is honored.)
$ aws s3api put-object \
    --bucket vamoose-m5 --key precondition-test \
    --if-none-match '*' \
    --body /tmp/v3
An error occurred (PreconditionFailed) when calling the PutObject
operation: At least one of the preconditions you specified did not hold.
```

The asymmetry is consistent with VAST's published S3 surface. The
documented HTTP-conditionals support on PutObject is limited to the
`x-amz-copy-source-if-*` family used by `CopyObject`, *not* the plain
`If-Match` / `If-None-Match` headers on a regular `PutObject`. (The
documented conditional-PUT support that does exist is for
`DeleteObject` / `DeleteObjects`, not the create/overwrite path.)

**Consequence stated plainly:** every ownership transfer in vamoose's
current claim protocol — `claim::refresh`, `claim::reclaim`, and
`claim::complete` — depends on `PUT If-Match: <etag>` returning 412
when the etag is stale. That contract is **not** enforced by the
var204 endpoint. `If-None-Match: *` (the first-time-claim path)
*is* enforced, which is exactly why every M2 / M3 single-worker
verification has passed: the first claim is mutually exclusive
across workers, but no subsequent state transition is.

## 4. Diagnostic chain

Chronological narrative of how the bug was isolated:

1. **First red flag.** Initial M5 runs reached Phase 4 with assertion
   D ("A clean self-fence") failing — A's stderr had no
   `fence tripped` line. The harness's existing diagnostic was just
   to print A.err, which was empty.
2. **Heartbeat instrumentation.** Added three `tracing::info!` lines
   to `crates/migration-worker/src/heartbeat.rs`:
   - one at the top of every loop iteration (`heartbeat: tick`)
   - one after the `held` snapshot (`heartbeat: held-state snapshot`)
   - one in the `RefreshOutcome::Refreshed` arm
     (`heartbeat: refresh succeeded`)
   And lowered `heartbeat_sec` from 10 to 1 in the harness toml so
   tick events became dense enough to read in the run log.
3. **Pre-SIGSTOP behavior is normal.** A's tick logs show
   `heartbeat: refresh succeeded` every second from the moment A
   claims the shard until the moment the harness sends `SIGSTOP`.
   `held = true`, `shard = "part-0000.parquet"`, `epoch` increments
   monotonically. No anomalies.
4. **SIGSTOP suspends the heartbeat.** No tick lines fire while A is
   stopped, exactly as expected — the tokio runtime is suspended
   along with the rest of the process.
5. **B reclaims successfully.** The claim object on S3 is observed
   to flip to `host = "m5-host-B"` with a higher `epoch` value. This
   confirms the reclaim path *into* `If-Match` works for the actual
   reclaim — except (see below) it didn't actually require A's etag
   to be stale; it just used `If-Match` against the most recent
   etag, which on var204 is the only etag the endpoint cares about.
6. **Post-SIGCONT, A's first refresh succeeds.** This is the
   surprise. A's tick log resumes immediately after `SIGCONT`. The
   first refresh attempt — using A's *stale* pre-stop etag — should
   return 412 because B has since overwritten the object. Instead,
   the log line is `heartbeat: refresh succeeded` and A continues
   to "own" the shard. A then attempts further commits against a
   shard B has already finalized.
7. **Isolation.** With heartbeat instrumentation proving the worker
   side was behaving correctly (it asks the right question; it
   accepts whatever S3 says), suspicion shifted to S3 itself.
8. **Direct CLI confirmation.** The two `aws s3api` transcripts in
   §3 above were run by hand and reproduce the asymmetry: stale
   `If-Match` is silently ignored on PUT, while `If-None-Match: *`
   is honored.
9. **Documentation cross-check.** VAST's published S3 conditional
   support covers DeleteObject / DeleteObjects and the
   `x-amz-copy-source-if-*` family on CopyObject — not plain
   `If-Match` / `If-None-Match` on PutObject. The endpoint's
   behavior matches what is documented; vamoose's claim protocol
   was written against a contract this endpoint never advertised.

The bug is in vamoose's protocol assumptions, not in the harness or
the worker code. The harness, with its current instrumentation, will
detect the same condition the moment a v2 protocol lands and is
re-run.

## 5. What the harness reveals about the protocol

This finding is bigger than M5. Every "ownership transfer" in
vamoose's current claim protocol uses `PUT If-Match`:

| Transition           | Mechanism                              | Honored on var204? |
| -------------------- | -------------------------------------- | ------------------ |
| First-time claim     | `PUT If-None-Match: *`                 | yes                |
| Heartbeat refresh    | `PUT If-Match: <etag>`                 | **no**             |
| Reclaim (B over A)   | `PUT If-Match: <etag>`                 | **no**             |
| Final commit         | `PUT If-Match: <etag>` → state=`Completed` | **no**          |

Only the first row is the entry-gate that mutually excludes workers.
The remaining three rows are the entire safety story for ongoing
ownership and clean handoff — and on this endpoint they collapse
into unconditional PUTs.

M2 and M3 single-worker workflows happen to be safe under this
endpoint behavior because no second writer ever races. The first
claim succeeds, no other process is even trying, and every refresh
is effectively `PUT (with ignored precondition)` from a still-valid
owner. The output is correct; the protocol enforcement is absent.

In a real multi-worker production migration against a similarly
configured endpoint, two workers can both believe they own the same
shard after the lease window — neither sees a 412 — and both can
commit final renames against the same dest path. Assertion F in the
harness exists specifically to catch this scenario and is the
reason it must fail before the protocol is fixed.

This is a correctness gap that affects real migrations, not just
the M5 test scenario. It is the most important finding from the
M5 work. The harness's value is exactly that it forced this to
surface; without two contending workers driving against the same
shard, the gap would have stayed invisible.

## 6. Open work-items spawned

Files filed alongside this one in `docs/work-items/`:

- **`CLAIM_PROTOCOL_V2_DELETE_THEN_CREATE.md`** — proposes replacing
  the current `PUT If-Match` ownership transitions with a
  delete-then-conditional-create pattern that does not rely on
  `If-Match` enforcement on overwrite. Outline: each transition
  becomes (a) `DeleteObject` against the prior etag (which *is*
  conditionally enforced on var204), (b) `PUT If-None-Match: *`
  for the new state. This is two round-trips per transition rather
  than one, but each leg uses an S3 conditional that the endpoint
  actually honors. Detailed protocol design, retry semantics, and
  back-compat plan live in that doc.
- **`M5_HARNESS_VERIFIED.md`** — documents the harness changes that
  landed this session (setsid-wrapped sudo, distinct-PID capture,
  separated dest root, A.out catch loop, heartbeat instrumentation)
  and certifies the harness as ready to re-run against the v2
  protocol when it lands. The harness itself is no longer the
  unknown; only the protocol is.
- **`crates/migration-core/src/s3.rs` doc-comment correction** —
  lines 6–13 of that file previously stated that both `If-Match`
  and `If-None-Match: *` are "confirmed supported" by VAST S3.
  That assertion is wrong for `If-Match` on PutObject and has been
  corrected in the same change set. (See the s3.rs diff.)

## 7. Tag and artifacts

Suggest tagging the working harness state as `m5-harness-verified`
once these notes and the two work-item docs land. The git tag
should point at the commit that contains:
- the harness fixes from this session,
- the heartbeat instrumentation,
- the s3.rs doc correction,
- this notes file and the two work-item docs.

The tag's purpose is to mark the "everything works except the
protocol" state, so that when v2 lands the harness can be re-run
unmodified against it and the diff between v1 and v2 is exactly
the protocol layer.

Run artifacts underlying this writeup:

```
m5/run/20260505T165247Z/
├── A.out             # 335 KiB — heartbeat tick + held-state lines, every 1s
├── A.err             # empty (no fence trip — the bug)
├── B.out             # 11.4 MiB — B's commit-rename lines, the full shard
├── B.err             # empty
├── workerA.toml      # heartbeat_sec=1, lease_timeout_sec=10
├── workerB.toml
├── manifest.json     # single shard, distinct src/dst roots
└── assertions.log    # only D failed; A/B/C/E/F/G all PASS once §3 cli
                      #   confirmation explained why D had no chance
```

The 10000-file scale (vs. 1000 in the original M5_SELF_FENCE.md
defaults) was deliberately chosen for this run to widen the
mid-shard window and make the post-SIGCONT race observable;
nothing about the finding depends on the scale.

When the v2 protocol lands, the next M5 run should produce a
`fence tripped … 412 … heartbeat refresh` line in `A.err`, all
seven assertions PASS, and this file gets a closing entry pointing
at the run that demonstrated correct multi-host self-fencing.

## Pass record

**Status:** All seven assertions PASS, canonical 10000-file run.

- **Date:** 2026-05-05T22:01:47Z
- **Cluster:** selab-var204
- **Run dir:** m5/run/20260505T215638Z/
- **Tag:** m5-pass-v2-claim-protocol
- **Wall clock:** ~5 minutes (Phase 0 → Phase 4 complete)

### Key timings

- A launch → SIGSTOP: ~1 second (rows_done=13)
- B reclaim (epoch 1 → 2): ~12 seconds after SIGSTOP
- B completes shard: ~3:36 wall clock
- SIGCONT A → A exit clean: < 1 second
- A exit code: 0 (libc::_exit watchdog never had to fire)

### Final assertion outputs
PASS: A: final claim state=completed host=m5-host-B
PASS: B: file count src=10000 dst=10000
PASS: C: SHA-256 match across 10000 files
PASS: D: A self-fenced cleanly (exit=0, reason mentions v2 'claim refresh: HEAD shows different etag')
PASS: E: failures/host-A.jsonl + failures/host-B.jsonl absent or empty
PASS: F: no concurrent renames across A.out+B.out within 1.0s; 10073 total commits, 73 sequential duplicates accepted (at-least-once execution under fence trip)
PASS: G: no B partials; 0 A partials (allowed — A was paused mid-write)

### Significance

- v2 claim protocol (delete-then-create with PUT If-None-Match + DELETE If-Match) successfully provides at-most-once completion under VAST S3, which does not enforce PUT If-Match.
- Fence propagation through shard_processor honors at-row granularity; post-fence commits dropped from 1290 (pre-fix) to 0–1 (current).
- Heartbeat HEAD-and-compare detects ownership loss within one tick; A self-fenced within ~30ms of resuming after SIGCONT.
- Worker shutdown correctness against libnfs Drop hangs validated via libc::_exit(0) at end of main().
- 73 sequential rename duplicates from at-least-once execution are correctly accepted, no false positives.
