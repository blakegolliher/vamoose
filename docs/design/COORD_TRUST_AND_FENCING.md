# Design: coord lease fencing (F19) and worker trust boundary (F20)

> **Historical decision record.** D1–D5 were implemented and the findings are
> landed. The problem statements and proposed sequencing below preserve the
> pre-implementation analysis. See [../CONTROL_PLANE.md](../CONTROL_PLANE.md)
> for the current architecture and source paths.

Status: DECIDED 2026-07-30 — every recommendation accepted by the
project owner. D1: Option A, delete-then-create refresh. D2–D5:
full bundle — strict EventKind allow-list + worker-id binding as
one work item, per-worker `client_seq` HWM + validate-whole-batch
stacked on it as a second. D6 out-of-scope list stands.
Execution: `docs/work-items/COORD_LEASE_REFRESH_FENCE.md`,
`docs/work-items/COORD_WORKER_EVENT_TRUST.md` (D2+D3), then
`docs/work-items/COORD_EVENT_IDEMPOTENCY.md` (D4+D5, stacked).
Ledger: F19, F20 in `docs/REVIEW_LEDGER.md`.
Prior art this must stay consistent with: the v2 claim atoms in
`migration-core/src/claim.rs` (untouchable), the F02 write gate
(`docs/work-items/COORD_LEASE_FENCE_WRITES.md`), F25 phase legality,
F24 bus caps, F21 auth defaults.
Binding rule: **no `PUT If-Match` anywhere** — var204 does not
enforce it (silently overwrites, returns 200).

Decision points for the project owner are marked **D1–D6**. Their original
recommendations are retained below; D1–D5 have since landed and D6 remains out
of scope.

---

## Part 1 — F19: the coord lease refresh can silently un-fence a takeover

### Problem

The coord's own lease (`coord/lease`, `layout.rs:38`) is acquired and
taken over with the proven conditional atoms (`put_if_absent`,
`delete_if_match` — `lease.rs:200`, `lease.rs:230-244`). But **refresh**
is HEAD-then-unconditional-PUT (`lease.rs:263-290`): compare our held
etag via HEAD, then rewrite the body with a fresh `expires_at` via a
plain last-write-wins `put`.

The interleave: deposed coord A HEADs and still sees its own etag →
candidate B completes a takeover (conditional delete + create, new
etag) → A's unconditional PUT lands on top, overwriting B's fresh
lease with A's stale body. **A's refresh returns Ok and A keeps
operating as a believed owner.** The F02 write gate never trips,
because the gate's only input is refresh returning `LeaseLost`
(`ticks.rs` → `runtime/lifecycle.rs`). Both coords now ingest,
assign colliding event seqs, and flush chunks to identical keys
(chunk keys carry no lease epoch — the residual F02 documented at
`COORD_LEASE_FENCE_WRITES.md:20-23`).

### Why the worker claim protocol doesn't have this bug

A v2 claim owner **never rewrites the claim object while holding it**
(`claim.rs:24-27`). The held etag is therefore stable for the claim's
lifetime, which makes HEAD-only refresh sound: any takeover
necessarily rotates the etag, so the deposed owner's next HEAD
observes `Lost`. The coord lease violates exactly this invariant —
it rewrites its own object every 10s tick to bump `expires_at`, so
it needs a write on the refresh path, and that write is the
unconditional PUT.

### D1 — how to fence the refresh

**Option A (recommended): delete-then-create refresh.** Reuse the
takeover atoms for refresh itself:

1. `delete_if_match(coord/lease, held_etag)` — if this 412s or the
   object is gone, someone took over: return `LeaseLost`.
2. `put_if_absent(coord/lease, body with new expires_at)` — if this
   loses (`AlreadyExists`), a candidate slipped into the gap: return
   `LeaseLost`.

Every failure mode of every step resolves to *the owner concluding
LeaseLost*, which trips the F02 write gate. The split-brain failure
mode of today's refresh becomes, at worst, an availability blip: a
crash or racing candidate in the delete→create gap means the lease
object briefly doesn't exist and the fastest candidate cold-acquires
— which is an ordinary takeover, detected by the old owner on its
next refresh. Uses only the two VAST-safe atoms; ~30 lines in
`lease.rs::refresh` plus tests.

Consequences to accept:
- The etag rotates every refresh (it already does). Candidates
  probing for takeover already re-HEAD and check `expires_at`
  freshness, and their `delete_if_match` on a stale etag fails
  closed. No change needed there.
- A transient store error mid-refresh (after delete, before create)
  drops the lease and forces a failover even though the owner was
  healthy. With TTL 30s / refresh 10s this costs one takeover cycle.
  This is the fail-safe direction; we accept it.

**Option B: stable-etag lease (fully mirror the worker pattern).**
Never rewrite while held; move liveness out of the lease body
(separate heartbeat object, or progress-as-liveness like worker
reclaim). Makes HEAD-only refresh sound with zero writes, but
redesigns takeover eligibility (no `expires_at` to read), touches
every lease test, and re-derives liveness rules the claim protocol
took a full campaign to get right. More correct-by-construction, much
bigger blast radius.

**Option C: fencing tokens on every coord write** (lease epoch in
chunk/snapshot keys or bodies, readers resolve). This is the
already-documented F02 out-of-scope item. It hardens the *writes*
rather than the *refresh*, requires reader-side resolution logic in
TUI/replay, and still leaves two believed-owners running. Defense in
depth at high cost; not a substitute for fixing refresh.

**Recommendation: A**, keeping the F02 gate as the containment layer
behind it. Optionally (cheap, diagnostic only): stamp `lease_id` into
snapshot and chunk *bodies* — not keys — so post-incident forensics
can attribute writes to a holder. That is not a fence and must not be
sold as one.

Test plan for A (FakeStore, no hardware): inject a takeover between
step 1 and step 2 (refresh must return LeaseLost, gate trips); inject
delete 412 (same); crash-window sim — delete succeeds, create loses
to a candidate (LeaseLost, no panic, clean shutdown path); plus the
existing `refresh_after_takeover_returns_lease_lost` stays green.

---

## Part 2 — F20: `POST /workers/{id}/events` trusts every cluster-secret holder with operator power

### Problem (all present-tense, verified against code)

- The route accepts **all 19 `EventKind` variants** from any
  `X-Cluster-Secret` holder — including operator/lifecycle kinds
  (`JobCreated`, `JobPaused`, `JobResumed`, `JobCancelled`,
  `JobCompleted`, `JobFailed`, `JobPhaseChanged`). The admin command
  path (`command.rs:70-117`) adds bearer auth + audit rows; the event
  route bypasses both. A worker can legally drive any non-terminal
  job to `Cancelled` with no audit trail.
- The URL `{id}` is parsed and **discarded** (`worker.rs:252-255`).
  No check that payload `worker_id` fields match the caller; one
  worker can fence another (`WorkerFenced` → `state.rs:238-243`) or
  inflate any job's counters (`ProgressDelta` → `state.rs:252-264`,
  attribution ignored).
- **No idempotency**: seqs are coord-assigned; the worker's resend
  buffer drops entries only on a 200 (`worker.rs:34-44`), so a lost
  response → resend → double-applied events, permanently baked into
  the log and snapshots.
- **Non-atomic batches**: entries are ingested one at a time and the
  handler `?`-propagates mid-loop (`worker.rs:257-269`); a failure at
  entry k leaves 0..k applied with no rollback and no record.
- F24 caps do not help here: they rate-limit the outbound bus only
  (`runtime/ingest.rs`); every ingested event still hits state, log,
  and snapshot.

Fact that makes this cheap to fix now: the production worker emits
exactly **one** EventKind today — `ProgressDelta`
(`coord_driver.rs:452`, `:835`; the `JobCreated` in coord_client.rs
is a test fixture). `ErrorEmitted` is a commented future enhancement
(`coord_driver.rs:91`). The allow-list can start maximally strict
with zero behavior change.

### D2 — EventKind allow-list for the worker route

**Recommended:** the worker route accepts exactly the worker-nature
kinds: `ProgressDelta`, `ErrorEmitted`, `WorkerStateChanged`,
`WorkerRecovered`, `ClaimConflictDetected`, `ClaimConflictResolved`,
`VerifyFileMismatch` — plus `WorkerFenced` **only for the caller's
own id** (self-fence report). Everything else → 403 with the kind
named, and a `warn!` log. Lifecycle kinds (`Job*`, `VerifyStarted`,
`VerifyCompleted`) become operator/coord-internal only; if a future
worker feature needs one, widening the list is a one-line reviewed
change. `WorkerJoined`/`WorkerLeft` are synthesized by
register/heartbeat paths, not accepted raw.

Alternative (rejected): per-kind capability flags in config —
flexibility nobody has asked for, and a config knob that weakens a
security boundary.

### D3 — bind the caller to the payload

**Recommended:** stop discarding the URL id. Require (a) the worker
is registered and known, (b) every payload `worker_id` field equals
the URL id (for the kinds that carry one). 403 on mismatch. This
plus D2 reduces a stolen cluster secret from "operator power over
every job and worker" to "can lie about its own progress" — which
D6's rate concern and the ledger's F24 residue can bound later.

### D4 — idempotency: per-worker client sequence

The resend-after-lost-200 double-apply needs a dedup key the coord
can check *after replay too* (an in-memory LRU dies with the
process and the log has already double-appended by then).

**Recommended:** worker stamps each entry with a per-worker,
monotonically increasing `client_seq` (its resend buffer already
maintains insertion order); the batch carries `worker_id` implicitly
via the URL. Coord keeps `last_client_seq: u64` per worker **in
`State`** (so snapshots/replay carry it), skips entries
`<= last_client_seq` as already-applied (returning their original
disposition as success), and rejects gaps going backwards. Schema
change: one optional field on `WorkerEventEntry` + one map in
`State`; old workers without the field keep today's semantics
(documented as at-least-once) until upgraded — no flag day.

Alternative (rejected): per-event UUID + bounded dedup window —
unbounded key space, window-sizing guesswork, and replay can't
reconstruct the window without carrying it in state anyway, at which
point the HWM is strictly simpler.

### D5 — batch atomicity

With D4 in place, full rollback is unnecessary: **validate the whole
batch up front** (deserialization already done; run D2 allow-list +
D3 binding + D4 seq checks on every entry, reject the batch wholesale
on any failure) — then the only mid-loop failure left is storage
errors, and the worker's retry of the same batch is made safe by the
D4 HWM (already-applied entries skip). Net effect: no partial
application is ever *final*; no rollback machinery needed.
Reject-wholesale also means one malformed entry can't smuggle
siblings in.

### D6 — explicitly out of scope here (tracked, not designed)

- Scoped admin tokens (today every bearer token is full-authority)
  and per-worker credentials instead of one shared cluster secret.
- Ingestion-side rate limiting (F24 residue: log-side coalescing).
- Default bind `0.0.0.0:8443` (F21 covered dev-mode; an
  authenticated-mode default-loopback discussion is separate).
- mTLS.

### Test plan (all CI-side, in-process coord as in existing
`coord_client_integration.rs` patterns; no new top-level test files —
fold into existing binaries)

- Allow-list: each rejected kind → 403 + nothing in state/log; each
  allowed kind still applies.
- Binding: URL/payload worker-id mismatch → 403; unregistered id →
  403; `WorkerFenced` for self OK, for another id → 403.
- Idempotency: send batch, drop the 200, resend → state and log
  identical to single-send (assert log chunk contents, not just
  counters); replay from log reconstructs the HWM.
- Atomicity: batch with one illegal entry → whole batch rejected,
  state unchanged; storage failure mid-batch then retry → no
  double-apply.

---

## Sequencing if approved

1. F19 Option A (small, self-contained, `lease.rs` + tests).
2. F20 D2+D3 (allow-list + binding — strict now, zero behavior
   change for today's worker).
3. F20 D4+D5 (client_seq HWM + validate-first batches — touches
   worker `coord_client`, coord `worker.rs`, `State`, snapshot
   schema; one work item, test-first).

Each becomes a standard test-first work item after sign-off. None of
this touches `migration-core/src/claim.rs`.
