# Claim Protocol

This is the **as-built** reference for how workers coordinate ownership
of parquet shards through VAST S3. If code disagrees with this doc, the
code wins — bring the doc forward.

For the v1-to-v2 design rationale (why HEAD-and-compare, why
delete-then-create), see
`docs/work-items/CLAIM_PROTOCOL_V2_DELETE_THEN_CREATE.md`.

For the architectural picture this protocol fits inside, see
`DESIGN.md` "Claims" and "Self-fencing".

---

## Goals

Two correctness goals and one liveness goal, in priority order:

1. **No data corruption.** A reader on the destination must never see a
   half-written file, a file with bytes from two different source
   versions interleaved, or metadata from two different copy attempts
   interleaved.
2. **No data loss.** Every row in every shard must be copied at least
   once and committed atomically.
3. **Eventual progress.** When a worker dies, its shard becomes
   claimable by another worker within a bounded time (`lease_timeout`).

The protocol explicitly does **not** guarantee exactly-once execution
of a row's commit (see "What's not enforced" below). It guarantees
exactly-once content because the source is static and renames are
atomic.

---

## The four atoms

All claim-side state changes go through these four operations in
`migration-core/src/claim.rs`.

### `try_acquire(shard, host)`

**S3:** `PUT shards/<shard>.parquet.claim` with `If-None-Match: *`.

| S3 response | Outcome | Caller action |
|---|---|---|
| 200 | `Acquired { etag, record }` | Start processing |
| 412 | `Contended { existing_etag, existing }` | Pick another shard |

The shard's claim object is created with `epoch=1, state=Active`. The
returned etag is the proof of ownership; it does not change for the
duration of this ownership window.

### `refresh(shard, held_etag)` — the heartbeat

**S3:** `HEAD shards/<shard>.parquet.claim`. **No write.**

| Observed S3 etag | Outcome |
|---|---|
| Exactly equals `held_etag` | `StillHeld { etag }` |
| Different etag, or 404 | `Lost` |

This is the load-bearing difference from the v1 protocol: heartbeats
do **not** write to S3. The held etag is stable for the entire
ownership window. The owner's only writes to its own claim object are
`try_acquire`/`reclaim` (create it) and `complete` (finalize it).

Why HEAD-and-compare rather than PUT If-Match: it eliminates an entire
class of "heartbeat-vs-completion" and "heartbeat etag drift" races,
and it cuts claim PUT traffic to ~0 in steady state.

### `reclaim(shard, observed_etag, new_host, new_epoch)`

Two-step: `DELETE shards/<shard>.parquet.claim` with
`If-Match: <observed_etag>`, then `PUT` with `If-None-Match: *`.

| DELETE | PUT | Outcome |
|---|---|---|
| 200 Deleted | 200 | `Won { etag, record }` |
| 412 EtagMismatch | — | `LostRace` |
| 404 NotFound | — | `LostRace` |
| 200 Deleted | 412 | `LostRace` |

The caller is responsible for confirming the claim is genuinely stale
(`now - claimed_utc > lease_timeout`) before calling this. The
protocol layer does not enforce lease semantics — that policy is
applied in the orchestrator's `scan_shards` step.

Between the DELETE and the PUT the claim object is briefly absent. A
fresh `try_acquire` from a third worker can win the PUT race; we
report this as `LostRace`. Either way, exactly one owner emerges.

### `complete(shard, held_etag, host, epoch)`

Same two-step shape as `reclaim`. Writes a terminal `state=Completed`
record on success; returns `Lost` if either step doesn't see our held
etag.

```
DELETE If-Match: held_etag      →  Deleted | EtagMismatch | NotFound
PUT If-None-Match: *            →  Completed { etag } | Lost (412)
```

Terminal-state writes use delete-then-create rather than overwrite so
they share the same S3 semantics as reclaim, which simplifies
reasoning. The completed claim has a stable etag forever after.

### `fail(shard, held_etag, host, epoch)`

Identical S3 shape to `complete`, but writes `state=Failed`. Called
from the orchestrator when `processor.process()` returns an error
that would re-occur for any worker reclaiming the shard — corrupt
parquet, malformed row schema, anything shard-fatal as opposed to
worker-fatal. Distinct from `complete` so scanners can tell
"finished cleanly" from "couldn't be processed."

```
DELETE If-Match: held_etag      →  Deleted | EtagMismatch | NotFound
PUT If-None-Match: *            →  Failed { etag } | Lost (412)
```

Without `fail`, an unrecoverable shard would loop forever through
the fleet: worker A bails on corrupt parquet → lease expires →
worker B reclaims → also bails → repeat. `Failed` is terminal for
the scanner, so the loop terminates and an operator can see the
record and intervene.

#### Terminal-state PUT retry

Both `complete` and `fail` retry the new-state `PUT If-None-Match: *`
up to **3 attempts with 1s / 2s / 4s exponential backoff** on
transient errors (anything that isn't `PreconditionFailed`).
Reclaim's PUT is **not** retried — a transient there is safe to
surface (peer eventually picks up the still-stale claim), but a
transient terminal PUT after a successful DELETE leaves the claim
absent on S3, and the next worker would re-acquire and silently
redo the shard. The retry costs at most ~7s in the rare exhaustion
case and pays for itself the first time a 503/429 lands on the
final PUT of a real shard.

`PreconditionFailed` is returned immediately without retry — it
means a fresh `try_acquire` won the absent-window race, which is a
genuine `Lost`, not a transient.

---

## State machine

```
                      ┌─── try_acquire ───┐
                      │                   │
                      ▼                   ▼
   (absent)  ──> Active(host=A, epoch=1, etag=E1)
                      │           ▲
                      │           │ refresh (HEAD): StillHeld
                      │           │
                      │           ├─ refresh (HEAD): Lost → fence trip
                      │           │
                      │           ├─ heartbeat retry budget exhausted → fence trip
                      │           │
                      │           ├─ clock jump > lease/2 → fence trip
                      │           │
                      │           │
                      │           │ (lease expires from a peer's POV)
                      │           ▼
                      │   reclaim by B → Active(host=B, epoch=2, etag=E2)
                      │
                      └─── complete ───>
                            Completed(host=A or B, epoch=N, etag=Eterminal)
```

Terminal states (`Completed`, `Failed`) are immutable. A worker that
discovers a terminal claim in `scan_shards` skips the shard.

---

## Worker lifecycle

The orchestrator loop in `migration-worker/src/orchestrator.rs`:

1. **Scan**: list `shards/`, classify each as `Free` / `Stale(etag)` /
   `Active(live)` / `Terminal`. An `Active` claim is considered
   `Stale` when *either* `now - claimed_utc > lease_timeout`
   (the lease path) *or* the owner's `progress/host-<id>.json` is
   absent, has a mismatched `held_etag`, or has `heartbeat_utc`
   older than `2 × heartbeat_sec` (the progress-cross-check path —
   see `docs/work-items/PROGRESS_LIVENESS_CROSS_CHECK.md`). The
   cross-check shortens typical recovery from `lease_timeout` to
   `2 × heartbeat_sec` without changing the v2 protocol's
   correctness story; both signals are OR'd and either can fire.
2. **Acquire**: `try_acquire` on a Free shard, or `reclaim` on a Stale
   shard. On `Contended`/`LostRace`, pick another.
3. **Set held-claim cell**: `*current = Some(HeldClaim{shard, etag, epoch})`.
   The heartbeat task reads this cell to know whether to HEAD.
4. **Process the shard**: shard processor walks rows, runs the mover,
   collects per-batch outcomes. Between rows it polls
   `fence.is_valid()` and bails out if tripped.
5. **Clear held-claim cell**: `*current = None` *before* `complete()`.
   (This is the R4 fix — see "Race catalog" below.)
6. **Complete**: `claim::complete(held_etag)` writes the terminal
   record.
7. **Loop**: re-scan for the next shard.

Exits when `fence.is_valid() == false` or every shard is terminal.

---

## Self-fencing

The fence is the single most important correctness primitive in the
worker (`migration-worker/src/fence.rs`). It is an atomic bool plus a
`CancellationToken`. When tripped:

- The shard processor's row-level `fence.is_valid()` check stops
  dispatching new rows.
- The heartbeat task's cancellation arm of its `tokio::select!` fires,
  drops out of the loop, writes a final `fenced` progress record.
- The orchestrator's main-loop top-of-iteration check breaks the loop.

The fence is tripped from four sites:

| Trigger | Where | Reason text contains |
|---|---|---|
| HEAD shows different etag (claim reclaimed) | heartbeat task | `"claim refresh: HEAD shows different etag"` |
| Retry budget exhausted on persistent transient errors | heartbeat task | `"heartbeat HEAD failing for ... consecutive ticks"` |
| Local clock jump > `lease_timeout/2` | heartbeat task | `"clock jump detected"` |
| Orchestrator shutdown | orchestrator | `"worker shutting down"` |

The fence is **never** tripped from inside the mover. The mover relies
on the shard processor's between-row check, plus the bit-identical
content / atomic rename argument (see below).

---

## What the protocol enforces

- **At most one writer per shard at any instant where both writers can
  observe each other.** S3 conditional ops are the serialization
  point; the protocol never asks the workers to compare clocks against
  each other or vote.
- **Bounded recovery from worker death.** Within
  `min(2 × heartbeat_sec, lease_timeout) + heartbeat_sec` of the
  last live progress write, the claim is reclaimable. The first
  term is the progress-cross-check window (per
  `PROGRESS_LIVENESS_CROSS_CHECK.md`); the lease term remains as
  the fallback for any reclaimer whose progress fetch fails or
  whose schema cannot read the cross-check fields.
- **Terminal-state immutability.** Completed/Failed claims cannot be
  silently overwritten by a new owner.

## What's NOT enforced (and why it's still safe)

- **Exactly-once row commit.** During the gap between "peer reclaims"
  and "our heartbeat HEAD sees the new etag," the original worker
  continues to commit renames. Both workers commit the *same* row's
  rename to the *same* dest path. The M5 test observed 44 such
  duplicate commits in one run.

  This is **safe** because:

  1. `.partial` names are `<host>.<pid>`-stamped — workers never
     collide on each other's partials.
  2. Each rename is atomic at the NFS level — readers never see a
     half-written file.
  3. The source tree is static (v1 assumption) and both workers read
     the same source bytes; the destination ends up bit-identical
     regardless of which worker's rename lands last.
  4. Attribute application reads from the same indexed row; metadata
     converges.

  See M5 assertion F: it allows sequential duplicates and forbids
  *concurrent in-flight* renames within a 1.0s window, which is the
  actual harm model.

- **Fence-check immediately before the rename syscall (R8).** The
  shard processor checks `fence.is_valid()` between row dispatches but
  the mover's `do_libnfs_copy` / `do_empty` do not re-check just
  before `ops::rename`. A row already inside `spawn_blocking` at
  fence-trip time will complete its rename. Bounded by per-file copy
  duration. Tracked as a follow-up; the at-least-once safety argument
  above keeps this from being a correctness defect.

- **Source mutation under copy.** The protocol assumes the source
  tree is frozen for the duration of the run. If a file changes
  bytes between A's read and B's read of the same row, the two
  workers' renames will commit different content. Out of scope for
  v1; flagged in DESIGN.md "Future work".

- **Cross-shard hardlink consolidation.** Hardlink groups are
  preserved within a shard but not across shards. See SCHEMA_CONTRACT.md
  and DESIGN.md "Hardlinks".

---

## Race catalog

Ordered by frequency × consequence in a healthy fleet. The numbering
matches the analysis in conversation; "R4/R6/R7" are the ones this
revision fixed.

| # | Race | Outcome | Frequency |
|---|---|---|---|
| R1 | Two workers race for the same Free shard via `try_acquire` | S3 picks one via `If-None-Match: *`; loser sees `Contended`, re-scans | Common at multi-worker startup; transparent |
| R2 | Two workers race to `reclaim` the same Stale claim | One DELETE succeeds, others see EtagMismatch/NotFound and report `LostRace` | Once per worker-death event |
| R3 | **Fence-window dupes.** Owner is partitioned; peer reclaims; owner keeps committing renames until either (a) its next HEAD sees the new etag, (b) R6 retry-budget exhausts. Since the progress-cross-check landed, peer reclaims at `2 × heartbeat_sec` (faster), but owner's R6 trip is still gated on `lease_timeout` — so the dupe window under sustained partition can widen to ~`lease_timeout - 2 × heartbeat_sec` of overlapping writes. | At-least-once row commits, all bit-identical, atomic. Wider window = more duplicate work, no corruption. M5 assertion F (1.0s concurrent-rename bound) still holds. | Per partition / GC pause / lease overrun event |
| R4 | ~~Spurious fence after clean `complete()`~~ — **fixed** | Held-claim cell is now cleared *before* `complete()`; heartbeat skips HEAD during complete's window | Fixed |
| R5 | Reclaim PUT race: fresh `try_acquire` lands between reclaimer's DELETE and PUT | Reclaimer sees `LostRace`; the fresh acquirer owns | Sub-ms window; effectively zero |
| R6 | ~~Persistent transient HEAD failure never trips fence~~ — **fixed** | Heartbeat now counts consecutive failures; trips after a full lease window's worth (`ceil(lease_timeout / heartbeat_sec)` ticks) | Fixed |
| R7 | ~~Local clock jump not detected~~ — **fixed** | Wall-vs-monotonic drift is compared each tick; fence trips on `> lease_timeout / 2` | Fixed |
| R8 | Mover commits one more rename per row that was already inside `spawn_blocking` at fence-trip time | Bounded by per-file copy time; safe due to bit-identical content | One-per-row at fence-trip; **not fixed**, deferred |
| R9 | Manifest swapped under us (operator fat-fingers re-upload) | `verify_shard_etag` catches this at download; otherwise undetected | Operator fault |
| R10 | Shard processor's runtime stalls past `2 × heartbeat_sec` (was `lease_timeout` pre-cross-check) | Owner's heartbeat task can't publish progress while the runtime is stalled; peer's next scan sees `heartbeat_utc` stale and fast-reclaims. On resume, owner's next HEAD sees the new etag → fence. Recovery is `O(heartbeat_sec)` rather than `O(lease_timeout)`. | Triggered by any runtime stall longer than `2 × heartbeat_sec` (used to require multi-minute stalls; now ~tens of seconds at defaults) |

---

## Diagnostics

When a fence trip happens, the `tracing` event chain looks like:

```
heartbeat:  refresh: claim lost (claim object replaced under us)
             OR
            heartbeat HEAD failing for N consecutive ticks ...
             OR
            clock jump detected: wall-mono drift Xs > lease/2 ...
fence:      fence tripped; self-fencing worker
shard_proc: (between-row check returns false; outcome.fenced=true)
orch:       shard processing fenced
heartbeat:  (cancel_token cancelled) → writes "fenced" progress, exits
orch:       fence.is_valid()==false at loop top → break
```

Useful greps in worker logs:

- `fence tripped` — single line per fence trip with the reason.
- `commit: rename .partial → final` — every dest-path commit (DEBUG
  level on `migration_mover`). Use to catch concurrent duplicates.
- `refresh: claim lost` — the canonical "I lost the claim" signal.
- `heartbeat HEAD failing` — R6 budget tracking; increments on every
  failed tick, resets on success.

Useful S3-side checks (from the operator host):

```bash
# Current claim state for a shard
aws s3 cp s3://$BUCKET/shards/part-0042.parquet.claim - | jq .

# Current ownership across all shards
aws s3 ls s3://$BUCKET/shards/ | awk '{print $4}' | while read k; do
  aws s3 cp "s3://$BUCKET/$k" - 2>/dev/null \
    | jq -r --arg k "$k" '"\($k)\t\(.host)\t\(.state)\t\(.epoch)\t\(.claimed_utc)"'
done

# Per-host liveness
aws s3 cp s3://$BUCKET/progress/host-$HOST.json - | jq .
```

---

## Bucket prerequisites

**Bucket versioning must be OFF.** The v2 protocol depends on
`DELETE If-Match` actually removing the claim object. Under bucket
versioning the DELETE creates a delete marker instead; subsequent
`PUT If-None-Match: *` calls can race the marker and produce
surprising 412s on what should be a free claim.

The orchestrator probes `GetBucketVersioning` at startup and:

- exits with a clear error if the bucket reports `Enabled` or
  `Suspended` (suspended is unsafe too — pre-existing delete
  markers and non-current versions still persist);
- emits a `WARN` and continues if the probe itself errors (e.g.
  the IAM permission is restricted); the operator is responsible
  for confirming the bucket state in that case.

If you're migrating onto a bucket that *was* versioned, disable
versioning and clear any non-current versions / delete markers
under the `shards/` prefix before starting workers.

## LostRace / Contended backoff

When multiple workers race for the same Free or Stale shard, all
losers see `Contended` (try_acquire) or `LostRace` (reclaim) on
the same tick. The orchestrator sleeps `heartbeat_sec / 4 + jitter`
(uniform in `[0, heartbeat_sec / 4)`) before its next scan,
spreading the inevitable LIST+GET retry storm across roughly half a
heartbeat. Without the backoff, M-1 of M workers all re-issue
`scan_shards` immediately, amplifying S3 ops linearly in M.

## Operational tuning

| Knob | Worker config | Default | Notes |
|---|---|---|---|
| `[worker].heartbeat_sec` | u64 | 30 | HEAD interval. Lower → faster fence detection, more S3 ops. |
| `[worker].lease_timeout_sec` | u64 | 180 | Should be ≥ `6 * heartbeat_sec`. Doubles as R6 retry budget window and R7 drift threshold (halved). |

Implied derived values:

- R6 retry budget = `ceil(lease_timeout_sec / heartbeat_sec)` ticks
  before preemptive fence. With defaults: 6 ticks ≈ 3 minutes.
- R7 clock-drift threshold = `lease_timeout_sec / 2` seconds. With
  defaults: 90 seconds.

At M6 scale (100 workers, multi-minute shards), the default 30/180 is
the right ratio. The M5 harness uses 10/60 to make the test reach
SIGSTOP fast.

---

## References

- `crates/migration-core/src/claim.rs` — the four atoms, fakes, unit tests
- `crates/migration-worker/src/heartbeat.rs` — heartbeat task, R6/R7 guards
- `crates/migration-worker/src/orchestrator.rs` — scan, acquire, complete, R4 fix
- `crates/migration-worker/src/fence.rs` — fence primitive
- `docs/work-items/CLAIM_PROTOCOL_V2_DELETE_THEN_CREATE.md` — v2 design rationale
- `docs/work-items/M5_SELF_FENCE.md` — M5 verification harness assertions
- `DESIGN.md` "Claims" and "Self-fencing" — architectural overview
