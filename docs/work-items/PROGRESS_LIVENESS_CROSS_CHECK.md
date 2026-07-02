# Progress-file cross-check for fast reclaim

Design spec for accelerating stale-claim detection by cross-checking
the per-host `progress/host-<id>.json` object against the per-shard
claim's `claimed_utc`. Reduces typical recovery time from
`lease_timeout` (180s default) to ~`2 × heartbeat_sec` (60s default)
without changing the v2 claim protocol's correctness story.

Companion docs: `CLAIM_PROTOCOL.md` "Race catalog" rows R3 / R10,
`CLAIM_PROTOCOL_V2_DELETE_THEN_CREATE.md` §3 (the underlying
protocol that this change builds on, not modifies).

Status: implemented and hardware-verified (closed 2026-05-20 — see
§11). The reference logic in §5 ships in
`migration-worker/src/orchestrator.rs::check_progress_liveness`.

## 1. Problem statement

Today, `migration-worker/src/orchestrator.rs::scan_shards` classifies
an `Active` claim as `Stale` purely on the basis of
`now - record.claimed_utc > lease_timeout`. `claimed_utc` is written
once at acquire/reclaim time and *never refreshed* during the
ownership window — that's an intentional property of the v2
protocol (heartbeats are read-only). The consequence is that a
worker which dies *the instant after acquiring* is not reclaimable
for a full `lease_timeout` (default 180 s), even though every other
worker can already see — via the per-host progress file — that it
has stopped heartbeating.

The protocol already maintains two liveness signals on S3:

| Signal | Where | Refresh cadence | Today's role |
|---|---|---|---|
| `claimed_utc` in claim body | `shards/<X>.parquet.claim` | once per acquire/reclaim | reclaim eligibility |
| `heartbeat_utc` in progress | `progress/host-<id>.json` | every `heartbeat_sec` tick | observability only |

`scan_shards` consults only the first. The second is fresh by
design but operationally unused for reclaim decisions. This work-
item wires it in.

Acceptance criterion: a worker SIGKILL'd immediately after a
successful acquire is reclaimed by a peer within
`max(2 × heartbeat_sec, 5s) + LIST round-trip`, not the full
`lease_timeout`. M5-class harness should observe the new bound.

## 2. Non-goals

- **No change to the four claim atoms.** `try_acquire`, `refresh`,
  `reclaim`, `complete` keep their current S3 calls and outcome
  enums. The cross-check is a caller-side eligibility predicate
  that runs *before* `reclaim`; `reclaim` itself is unchanged.
- **No new fence-trip path.** The owner's self-fencing remains the
  authoritative defense against R3 dupes; this work-item only
  changes the speed at which a peer becomes eligible to reclaim.
- **No protocol-level expiry.** We do not embed an expiry timestamp
  in the claim body. Expiry remains a caller-policy derivation
  from `claimed_utc + lease_timeout`, with the progress file as a
  faster optional fallback.

## 3. Schema change — `ProgressRecord`

`crates/migration-core/src/records.rs`. Add one field:

```rust
pub struct ProgressRecord {
    // existing fields unchanged ...
    pub host: String,
    pub started_utc: UtcTime,
    pub heartbeat_utc: UtcTime,
    pub current_shard: Option<String>,
    // ... etc ...

    /// Etag of the claim object this worker currently holds, if any.
    /// `None` means the worker is between shards (idle or scanning).
    /// `Some(etag)` is the proof-of-ownership tied to this progress
    /// record — a reclaimer can match it against the claim body's
    /// owning etag to detect a same-host_id restart without race.
    #[serde(default)]
    pub held_etag: Option<String>,

    /// Heartbeat interval the writing worker is configured with, in
    /// seconds. Reclaimers use it to compute a freshness threshold
    /// (`2 × held_heartbeat_sec`) that is calibrated against the
    /// *writer*, not assumed from the reader's config. `#[serde(default)]`
    /// so pre-cross-check progress objects parse as `0`, which the
    /// reclaimer treats as "missing → fall back to lease".
    #[serde(default)]
    pub heartbeat_sec: u64,
}
```

Both new fields are additive and tagged `#[serde(default)]`; old
progress files parse with `held_etag = None` and `heartbeat_sec = 0`,
which the cross-check predicate explicitly degrades safely on.

## 4. Heartbeat task wiring

`crates/migration-worker/src/heartbeat.rs::HeartbeatTask::write_progress`.

Currently the function builds a `ProgressRecord` from the read-only
snapshot of `ProgressState`. The held etag lives in a *different*
shared cell (`Arc<Mutex<Option<HeldClaim>>>`) which the task already
locks earlier in the tick. Refactor so `write_progress` either
takes the held-claim snapshot as a parameter or reads the cell
itself. The simplest, smallest change: pass an `Option<&HeldClaim>`
down from the loop body (which already snapshots it for the HEAD
step) into `write_progress`, and pass the configured
`heartbeat_sec` once at construction time on the task struct.

Don't add new locking — the existing snapshot is sufficient. The
held etag in the progress object is allowed to be one tick stale
relative to S3 reality.

> **Correction (F01, `docs/work-items/CLAIM_FRESH_GRACE.md`).** An
> earlier revision of this section claimed the one-tick staleness
> "is accounted for by the `2 × heartbeat_sec` threshold". That was
> wrong: the `heartbeat_utc` threshold only guards the matching-etag
> branch of the predicate. A one-tick-stale `held_etag` — progress
> absent at startup, `held_etag = None`, or the *previous* shard's
> etag — lands in the absent/mismatch branches, which returned
> "eligible" immediately and made every live, seconds-old claim
> stealable until its owner's next tick (a near-deterministic theft
> cascade at fleet startup). Those branches are instead guarded by
> the **fresh-claim grace window**: they return eligible only once
> `now - claim.claimed_utc > 2 × heartbeat_sec`. See §5.

## 5. Scan-shards algorithm change

`crates/migration-worker/src/orchestrator.rs::scan_shards`. The
current `Active`-claim branch:

```rust
ClaimState::Active => {
    all_terminal = false;
    let age = now.signed_duration_since(record.claimed_utc.0);
    let stale = age.to_std().map(|d| d > lease).unwrap_or(false);
    if stale && next.is_none() {
        next = Some(ClaimTarget::Stale { ... });
    }
}
```

becomes:

```rust
ClaimState::Active => {
    all_terminal = false;
    let stale_by_lease = ...;            // existing computation
    let stale_by_progress = check_progress_liveness(
        s3, &record.host, &e.etag, now,
    ).await?;
    if (stale_by_lease || stale_by_progress) && next.is_none() {
        next = Some(ClaimTarget::Stale { ... });
    }
}
```

where (as shipped, the predicate is pure & synchronous — the caller
does the progress GET and error handling; `claimed_utc` comes from
the claim body already parsed in `scan_shards`):

```rust
/// Returns `true` iff the per-host progress file confirms the
/// owning worker is no longer heartbeating against this specific
/// claim. Conservative on every uncertain edge — missing field,
/// fetch error, parse error, future timestamp — defaults to
/// `false` so the lease check remains the sole gate.
fn check_progress_liveness(
    progress_body: Option<&[u8]>,
    claim_etag: &str,
    claimed_utc: DateTime<Utc>,
    scanner_heartbeat_sec: u64,
    now: DateTime<Utc>,
) -> bool {
    // Fresh-claim grace (F01, CLAIM_FRESH_GRACE.md): the progress
    // object is only rewritten on heartbeat ticks, so between an
    // acquire and the owner's next tick it still describes the
    // PREVIOUS ownership window. The absent and None/mismatched-
    // held_etag branches therefore only fire once the claim itself
    // is strictly older than 2 × heartbeat_sec. Same conservative
    // time handling as the heartbeat math: a future claimed_utc
    // (clock skew) → negative age → not eligible.
    let grace_elapsed = |hb_sec: u64| -> bool {
        if hb_sec == 0 {
            return false;
        }
        let claim_age = now.signed_duration_since(claimed_utc);
        claim_age.num_seconds() > (hb_sec.saturating_mul(2)) as i64
    };

    let Some(body) = progress_body else {
        // No progress object at all — owner never started, crashed
        // before the first tick, or just hasn't ticked yet. Only
        // eligible once the grace window has elapsed; no writer-side
        // heartbeat_sec exists, so calibrate on the scanner's own.
        return grace_elapsed(scanner_heartbeat_sec);
    };
    let Ok(p) = serde_json::from_slice::<ProgressRecord>(body) else {
        // Parse failure — be conservative, defer to lease.
        return false;
    };
    // Old-schema progress object: heartbeat_sec=0 (default). Defer
    // to lease; we have no calibrated freshness window.
    if p.heartbeat_sec == 0 {
        return false;
    }
    // Same-host_id restart safety: the owning etag in the progress
    // record must match the claim's current etag. A mismatch means
    // the progress file belongs to an earlier acquire (different
    // ownership window) — stale, but only reclaim-eligible once the
    // claim has outlived the grace window (the owner may simply not
    // have ticked since acquiring).
    match p.held_etag.as_deref() {
        Some(e) if e == claim_etag => {}
        Some(_) | None => return grace_elapsed(p.heartbeat_sec),
    }
    // Both match: check freshness. 2× heartbeat_sec gives the
    // writer one missed tick of grace. No claim-age guard here — a
    // matching held_etag proves the progress record was written
    // inside this ownership window.
    let age = now.signed_duration_since(p.heartbeat_utc.0);
    let threshold_secs = (p.heartbeat_sec.saturating_mul(2)) as i64;
    age.num_seconds() > threshold_secs
}
```

Decision table (claim age = `now - claim.claimed_utc`; grace =
`2 × heartbeat_sec`, writer-supplied when a progress object exists,
scanner-local otherwise; all comparisons strict `>`, future
timestamps never satisfy them):

| Progress file state | Claim age | Eligible? |
|---|---|---|
| Absent | ≤ grace (scanner hb) | **no** — owner may not have ticked yet |
| Absent | > grace (scanner hb) | yes — owner never started / crashed pre-tick |
| Parse failure | any | no — defer to lease |
| `heartbeat_sec == 0` (old schema) | any | no — no calibrated window |
| `held_etag` `None` or mismatched | ≤ grace (writer hb) | **no** — progress one tick behind the acquire |
| `held_etag` `None` or mismatched | > grace (writer hb) | yes — orphan / same-host_id restart |
| `held_etag` matches, heartbeat age ≤ 2 × hb | any | no — alive |
| `held_etag` matches, heartbeat age > 2 × hb | any | yes — dead |

**Trade-off (intended).** Crash recovery via the progress cross-check
takes up to `2 × heartbeat_sec` longer for a worker that dies
immediately after acquiring: its claim must outlive the grace window
before the absent/mismatch branches may fire. The lease-based path is
unchanged. This is the intended trade — a bounded slowdown on one
crash pattern in exchange for live claims never being stealable in
the acquire-to-first-tick window.

Cost: one extra GET per `Active` claim per scan pass. At 100 workers
and 10k shards in steady state most claims are `Completed`, so this
adds GETs proportional to in-flight work, not total shards.

## 6. Safety analysis

Walk each failure mode against the v2 invariants from
`CLAIM_PROTOCOL.md`. The cross-check **adds** eligibility for
reclaim earlier than the lease window; it never **subtracts** any
existing safety.

| Scenario | Today | With cross-check | Verdict |
|---|---|---|---|
| Worker SIGKILL'd post-acquire | Reclaimable after `lease_timeout` | Reclaimable after `2 × heartbeat_sec` | Faster recovery; correctness unchanged (peer's `reclaim` still gates on `DELETE If-Match`) |
| Network partition (owner alive, peer blind) | Owner keeps committing renames until its next HEAD sees the new etag; peer reclaims after `lease_timeout` | Same, but peer reclaims after `2 × heartbeat_sec` — wider R3 dupe window | Bit-identical-content + atomic-rename argument still holds. Wider window = more duplicate work, no corruption. |
| Long GC / suspend on owner | `claimed_utc` still fresh from peer's POV → not stale | `heartbeat_utc` not refreshed → peer fast-reclaims | Owner's next refresh HEAD sees new etag → fences. Existing R3 protection unchanged. |
| Same-host_id restart | Self-claims logged but not reclaimed; sit until lease expires | New ownership = new claim etag, but old progress file still has old etag → cross-check sees mismatch → reclaim-eligible. New ownership also writes a fresh progress object with the new etag → never matches the stale claim. | Safe via etag matching. |
| Reclaimer clock ahead of owner | Lease math may fire early under skew today | Same skew applies to `heartbeat_utc` math too — but R7 (clock-jump fence) already bounds drift at `lease_timeout/2`. Net no worse than today. | Bounded by existing R7. |
| Progress file fetch fails (5xx) | n/a | Predicate returns `Err`; caller defers to lease-based check | Fail-safe (treat as alive). |
| Two reclaimers race after fast-reclaim | n/a | Both `reclaim()`; one wins DELETE If-Match, other gets LostRace | Identical to today's reclaim semantics. |

The only new behavioral exposure is **wider R3 dupe windows under
partition**, by roughly `lease_timeout / (2 × heartbeat_sec)` —
~3× at defaults. M5 already validates the dupe-safety argument
(`F` assertion); re-run it under the new threshold.

## 7. Backward / forward compatibility

- **Old worker reading new progress:** ignores the new fields
  (they're `#[serde(default)]`). No behavior change.
- **New worker reading old progress:** `held_etag = None`,
  `heartbeat_sec = 0`. Cross-check returns `false` for both
  conditions → defers to lease. Behavior is identical to today.
- **Mixed fleet during rollout:** new workers reclaim faster from
  new-worker deaths; reclaim of old-worker deaths reverts to lease.
  Acceptable — the only downside is the rollout window doesn't get
  the speedup uniformly. No correctness coupling.
- **`format_version` bump:** **not** required. Both fields are
  additive and serde-default. Document the schema addition in
  `SCHEMA_CONTRACT.md` under the `ProgressRecord` section.

## 8. Open decisions for the implementer

1. **Where does the reclaimer learn the owner's `heartbeat_sec`?**
   Spec above pulls it from `ProgressRecord.heartbeat_sec` (i.e.
   from the writer). Alternative: assume the reclaimer's local
   `cfg.worker.heartbeat_sec`. Recommendation: from-writer, because
   it survives mixed configurations (e.g. operator gradually
   tuning the value across the fleet). The writer-supplied value
   is also fail-safe — `0` (old schema) degrades to lease.

2. **What if `2 × heartbeat_sec` exceeds `lease_timeout`?**
   Misconfigured cluster, but we should still behave sanely. The
   cross-check threshold and lease check are independent (OR'd
   together); whichever fires first wins. No additional clamp
   needed — the cross-check just becomes a slow path under that
   config, and the lease check still works.

3. **Cache the progress GET inside a single scan pass.** A scan
   that finds 12 active shards owned by 3 hosts shouldn't do 12
   GETs — it should do 3. Build a `HashMap<host, ProgressRecord>`
   inside `scan_shards` and populate lazily. Optional; do it if
   the LIST→GET amplification matters.

4. **M5 assertion update.** The harness today expects the SIGSTOP'd
   worker to remain unreclaimed for `lease_timeout`. After this
   change, depending on test config it will be reclaimed earlier.
   Either tune the M5 harness's heartbeat/lease ratio so the two
   thresholds remain distinguishable, or add an explicit assertion
   that the *progress* path fires (and the lease path doesn't).
   See `M5_SELF_FENCE.md` §F.

5. **Reconcile with `log_self_owned_claims`.** Today
   `orchestrator.rs:697-718` only logs same-host_id orphaned
   claims. With this change, the operator-visible recovery time
   for a fast-restart drops to `2 × heartbeat_sec` because the
   peer-side cross-check sees a new etag and fast-reclaims. The
   self-claim reconciler is therefore *less* important — but
   still worth surfacing for the operator. Leave it as-is unless
   the implementer wants to bundle "self-restart reclaim"
   (the original gap #10 from the review) into the same PR.

## 9. Implementation checklist

In rough commit order. Each step should be independently buildable
and testable.

- [x] **records.rs**: add `held_etag: Option<String>` and
      `heartbeat_sec: u64` to `ProgressRecord`, both with
      `#[serde(default)]`. Add a round-trip serde test confirming
      old JSON (without the fields) still parses.
- [x] **heartbeat.rs**: thread the held-claim snapshot and
      configured `heartbeat_sec` into `write_progress`. Construct
      `ProgressRecord` with the new fields populated. No new
      locking. (`heartbeat_sec` derived from `self.interval.as_secs()`
      — no new field on `HeartbeatTask`.)
- [x] **orchestrator.rs**: introduce `check_progress_liveness`
      helper. Plumb it into the `ClaimState::Active` branch of
      `scan_shards`. OR the result with the existing `stale_by_lease`.
      (Refactored to a pure sync predicate over `Option<&[u8]>` so
      tests don't need async-trait machinery.)
- [ ] **orchestrator.rs**: optional intra-scan progress cache
      (decision §8.3). Skippable in the first PR — **deferred**.
- [x] **Unit tests**: new tests in `orchestrator.rs` (or a new
      `orchestrator_tests.rs` if the file is too crowded) for
      each row of the table in §6 — exercise the predicate
      against the existing `FakeStore`. (8 tests landed in
      `orchestrator::tests`.)
- [ ] **M5 harness update** (decision §8.4) — see
      `docs/work-items/M5_SELF_FENCE.md`. If the M5 timeouts no
      longer distinguish the two paths, retune them and update
      the assertion text. **Deferred** to its own work-item.
- [x] **Docs**: update `CLAIM_PROTOCOL.md` "Worker lifecycle" §1
      and "Race catalog" R3 / R10 to mention the cross-check.
      Update `SCHEMA_CONTRACT.md` `ProgressRecord` row.
- [x] **Manual verification on var204**: run the worker against a
      manifest with ≥4 shards, SIGKILL one worker mid-acquire,
      observe a peer reclaim in <90s (default config) and not at
      ~180s. Record the run-id and timings in this work-item's
      closing note. (Single-shard variant via
      `scripts/fast-reclaim-drill.sh`; see §"Hardware verification".)

## 10. Out of scope (deliberately)

- Embedding lease expiry in the claim body (reviewer suggestion
  #4) — adds a write per heartbeat to the claim object, regressing
  the v2 "owners never rewrite" property. Don't.
- A unified single-source-of-truth liveness file. Two cheap
  signals on S3 — claim body + progress file — already give us
  the eventual-correctness path (claim) and the fast-path
  (progress) without a third concept.
- Cross-host clock synchronization assumption changes. R7
  remains the operator-visible drift bound; the cross-check
  uses the *same* clock domain (reclaimer's wall clock vs the
  progress file's `heartbeat_utc`) so it's no more sensitive
  than the existing lease check.

## 11. Hardware verification — closed 2026-05-20

Verified on var204 via `scripts/fast-reclaim-drill.sh`. Single
result: **reclaim latency 12.52 s**, well under the 30 s pass
threshold and far below the 60 s lease window. Cross-check path
fired as designed; lease fallback was not used.

**Run config**

| | |
|---|---|
| Run dir | `m5/run/fastrec-20260520T234444Z/` |
| Bucket prefix | `fastrec-20260520T234444Z` |
| Source tree | 1000 files × 4096 bytes across 10 subdirs |
| Shard | 1010 rows / 168 480 bytes (single canonical shard) |
| `heartbeat_sec` | 5 |
| `lease_timeout_sec` | 60 |
| Cross-check threshold | `2 × heartbeat_sec = 10 s` |
| Pass budget | `< 30 s` (10 s threshold + worker-B startup slack) |
| Lease-fallback would-have-been | 60 s |

**Timeline**

- `23:48:17Z` — worker A started, claimed the shard (epoch 1).
- `23:48:26Z` — A SIGKILL'd after publishing a progress object
  with `held_etag` populated and ≥5 commit lines.
- `23:48:26Z` — worker B launched.
- `23:48:38Z` — B reclaimed (epoch 1 → 2, host=`fastrec-host-B`).
- `23:49:03Z` — B completed the shard.

Reclaim elapsed (sub-second precision): **12.52 s** from
`KILL_TS=2026-05-20T23:48:26.231359438Z`. Breakdown:

- ~5 s — cross-check eligibility window. A's last progress write
  landed just before SIGKILL, so the `now - heartbeat_utc > 10 s`
  predicate fires once the writer-side `heartbeat_sec` worth of
  age has accumulated past the last tick.
- ~5–6 s — worker B startup (sudo PAM, AWS SDK init, libnfs mount,
  manifest load, first scan iteration).
- ~1 s — S3 round-trips for the cross-check (LIST shards, GET
  claim body, GET `progress/host-fastrec-host-A.json`).

**Assertions (all PASS)**

```
A: reclaim latency 12.52s < 30s — fast-reclaim path fired (lease would have been 60s)
B: final claim state=completed host=fastrec-host-B
C: file count src=1000 dst=1000
D: SHA-256 match across 1000 files
E: B progress carries cross-check fields (heartbeat_sec=5, held_etag present)
F: failures/host-A.jsonl + failures/host-B.jsonl absent or empty
G: no B partials; 0 A partials (allowed — A was SIGKILL'd mid-write)
```

Assertion **E** is the load-bearing schema check: it confirms the
new `held_etag` + `heartbeat_sec` fields are actually being written
into `progress/host-<id>.json` end-to-end (worker heartbeat task →
S3 → reclaimer cross-check), not just present in the Rust type.
Without E green, A could pass for unrelated reasons (lease
misconfigured, race lucky) without exercising the new code path.

**Precondition gate (Phase 3)**

The drill refuses to proceed until A has *published* a progress
object with non-null `held_etag` and `heartbeat_sec == 5`. If those
fields are absent on S3 the cross-check degrades to lease-only on
B's side and we'd be measuring the wrong path. Failing loud at
preflight protects against false-green runs.

**M5 self-fence regression — also green**

Re-ran `scripts/m5-self-fence-test.sh` against var204 unchanged
(`heartbeat_sec=1`, `lease_timeout_sec=10`). Run dir
`m5/run/20260520T235829Z/`. All 7 M5 assertions PASS. The
observable shift from the pre-cross-check baseline:

- **Reclaim latency 5 s** (SIGSTOP at 23:59:08Z → B reclaim at
  23:59:13Z). Previously this took ~10 s — the full lease. The
  cross-check fires at `2 × heartbeat_sec = 2 s`; +3 s of B
  startup + first scan = 5 s. Two paths cleanly distinguished.
- M5 assertion F: 1047 total commits, **47 sequential duplicates**
  accepted, **0 concurrent renames within 1.0 s**. That's the R3
  at-least-once dupe pattern intact: A wrote 15 rows pre-SIGSTOP,
  B reclaimed and re-ran the full shard, A resumed → fenced →
  no further commits. Safety argument unchanged by the cross-check
  landing.
- M5 assertion D: A self-fenced cleanly with reason
  `"claim refresh: HEAD shows different etag"` (the v2 fence
  text). R4 + cross-check interaction is clean — heartbeat task
  sees the new etag on its first post-SIGCONT HEAD, no spurious
  fences, no shutdown hang.

Net: existing self-fence path preserved end-to-end, no regression,
recovery is now ~50 % faster at the M5 ratio.

## 12. References

- `CLAIM_PROTOCOL.md` §"Worker lifecycle", "Race catalog" R3 / R10
- `CLAIM_PROTOCOL_V2_DELETE_THEN_CREATE.md` §3
- `M5_SELF_FENCE.md` §F (dupe-safety assertion)
- `SCHEMA_CONTRACT.md` — `ProgressRecord` row to update
- `crates/migration-core/src/records.rs` — `ProgressRecord`
- `crates/migration-worker/src/heartbeat.rs` — `HeartbeatTask::write_progress`
- `crates/migration-worker/src/orchestrator.rs` — `scan_shards`
