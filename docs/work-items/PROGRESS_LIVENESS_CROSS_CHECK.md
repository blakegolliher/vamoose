# Progress-file cross-check for fast reclaim

Design spec for accelerating stale-claim detection by cross-checking
the per-host `progress/host-<id>.json` object against the per-shard
claim's `claimed_utc`. Reduces typical recovery time from
`lease_timeout` (180s default) to ~`2 × heartbeat_sec` (60s default)
without changing the v2 claim protocol's correctness story.

Companion docs: `CLAIM_PROTOCOL.md` "Race catalog" rows R3 / R10,
`CLAIM_PROTOCOL_V2_DELETE_THEN_CREATE.md` §3 (the underlying
protocol that this change builds on, not modifies).

Status: design only — no code in this work-item.

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
relative to S3 reality; the cross-check accounts for that by using
a `2 × heartbeat_sec` threshold rather than `1 ×`.

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

where:

```rust
/// Returns `true` iff the per-host progress file confirms the
/// owning worker is no longer heartbeating against this specific
/// claim. Conservative on every uncertain edge — missing field,
/// fetch error, parse error — defaults to `false` so the lease
/// check remains the sole gate.
async fn check_progress_liveness(
    s3: &S3Client,
    owner_host: &str,
    claim_etag: &str,
    now: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let key = layout::progress_key(owner_host);
    let Some((body, _)) = s3.get(&key).await? else {
        // No progress object at all — owner never started, or it
        // crashed before the first tick. Fast-reclaim eligible.
        return Ok(true);
    };
    let Ok(p) = serde_json::from_slice::<ProgressRecord>(&body) else {
        // Parse failure — be conservative, defer to lease.
        return Ok(false);
    };
    // Old-schema progress object: heartbeat_sec=0 (default). Defer
    // to lease; we have no calibrated freshness window.
    if p.heartbeat_sec == 0 {
        return Ok(false);
    }
    // Same-host_id restart safety: the owning etag in the progress
    // record must match the claim's current etag. A mismatch means
    // the progress file belongs to an earlier acquire (different
    // ownership window) — treat as stale.
    match p.held_etag.as_deref() {
        Some(e) if e == claim_etag => {}
        Some(_) | None => return Ok(true),
    }
    // Both match: check freshness. 2× heartbeat_sec gives the
    // writer one missed tick of grace.
    let age = now.signed_duration_since(p.heartbeat_utc.0);
    let threshold_secs = (p.heartbeat_sec.saturating_mul(2)) as i64;
    Ok(age.num_seconds() > threshold_secs)
}
```

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

- [ ] **records.rs**: add `held_etag: Option<String>` and
      `heartbeat_sec: u64` to `ProgressRecord`, both with
      `#[serde(default)]`. Add a round-trip serde test confirming
      old JSON (without the fields) still parses.
- [ ] **heartbeat.rs**: thread the held-claim snapshot and
      configured `heartbeat_sec` into `write_progress`. Construct
      `ProgressRecord` with the new fields populated. No new
      locking.
- [ ] **orchestrator.rs**: introduce `check_progress_liveness`
      helper. Plumb it into the `ClaimState::Active` branch of
      `scan_shards`. OR the result with the existing `stale_by_lease`.
- [ ] **orchestrator.rs**: optional intra-scan progress cache
      (decision §8.3). Skippable in the first PR.
- [ ] **Unit tests**: new tests in `orchestrator.rs` (or a new
      `orchestrator_tests.rs` if the file is too crowded) for
      each row of the table in §6 — exercise the predicate
      against the existing `FakeStore`.
- [ ] **M5 harness update** (decision §8.4) — see
      `docs/work-items/M5_SELF_FENCE.md`. If the M5 timeouts no
      longer distinguish the two paths, retune them and update
      the assertion text.
- [ ] **Docs**: update `CLAIM_PROTOCOL.md` "Worker lifecycle" §1
      and "Race catalog" R3 / R10 to mention the cross-check.
      Update `SCHEMA_CONTRACT.md` `ProgressRecord` row.
- [ ] **Manual verification on var204**: run the worker against a
      manifest with ≥4 shards, SIGKILL one worker mid-acquire,
      observe a peer reclaim in <90s (default config) and not at
      ~180s. Record the run-id and timings in this work-item's
      closing note.

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

## 11. References

- `CLAIM_PROTOCOL.md` §"Worker lifecycle", "Race catalog" R3 / R10
- `CLAIM_PROTOCOL_V2_DELETE_THEN_CREATE.md` §3
- `M5_SELF_FENCE.md` §F (dupe-safety assertion)
- `SCHEMA_CONTRACT.md` — `ProgressRecord` row to update
- `crates/migration-core/src/records.rs` — `ProgressRecord`
- `crates/migration-worker/src/heartbeat.rs` — `HeartbeatTask::write_progress`
- `crates/migration-worker/src/orchestrator.rs` — `scan_shards`
