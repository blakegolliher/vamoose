# Claim protocol v2 — delete-then-create

Design spec for revising vamoose's shard-claim protocol so that
every ownership transition is gated by a conditional primitive that
VAST S3 actually enforces. Spawned out of M5_NOTES.md "What the
harness reveals about the protocol". Status: implemented and
verified — v2 is the shipped protocol in `migration-core/src/claim.rs`
/ `s3.rs`, and the M5 self-fence harness passed against it on real
VAST hardware (2026-05; see M5_NOTES.md "Pass record", tag
`m5-pass-v2-claim-protocol`). Kept as the protocol spec. The two
companion docs are `M5_NOTES.md` (the finding) and
`M5_HARNESS_VERIFIED.md` (the test harness that will validate v2
once it lands).

## 1. Problem statement

The M5 self-fence verification on selab-var204 isolated a defect
in vamoose's claim protocol that has been silently present since
M1. The protocol assumes that `PUT If-Match: <etag>` returns
`412 Precondition Failed` when the etag is stale — that assumption
is RFC 9110 + S3-API-spec correct in general, but it is not
honored on the var204 endpoint. The endpoint silently accepts the
PUT and overwrites the object, returning `200 OK` with a fresh
etag. The `If-None-Match: *` first-claim path *is* enforced, which
is why every M2 / M3 single-worker verification has passed: the
first claim is mutually exclusive across workers, but no
subsequent state transition is.

The blast radius is wider than just M5. Three protocol transitions
rely on `PUT If-Match`:

- `claim::refresh` — heartbeat-side ownership-still-valid check.
- `claim::reclaim_stale` — a peer worker takes a stale claim.
- `claim::complete` — final state transition to `Completed`.

On the var204 endpoint each of these collapses to an unconditional
PUT — there is no race-loser, both contenders "succeed", and two
workers can simultaneously believe they own the same shard.
Single-worker workflows are incidentally safe because no second
writer ever races; multi-worker workflows have no mutual exclusion
past the initial claim.

## 2. Available primitives

VAST S3 documents and empirically supports three coordination
primitives that we can build on, plus one regular operation we
already use for observability:

```text
PUT  If-None-Match: *           atomic create-if-absent
DELETE If-Match: <etag>         atomic delete-if-current
DELETE If-Match: *              atomic delete-if-exists
HEAD                            cheap metadata read (etag in header)
```

Of those, the first three return 412 / 404 in well-defined cases
and can be retried safely. What we explicitly **do not** have, and
must therefore stop using:

```text
PUT  If-Match: <etag>           NOT enforced on var204 — silently overwrites
```

The supporting evidence and the CLI transcripts that demonstrated
the asymmetry live in `M5_NOTES.md §3`. The s3.rs doc-comment that
incorrectly claimed both `If-Match` and `If-None-Match: *` were
"confirmed supported" has been corrected in the same change set
that lands these docs.

## 3. Revised protocol — delete-then-create

The conceptual shift is small but pervasive: the claim object
becomes a *read-mostly* artifact. While a worker holds it, nobody
rewrites it. Ownership transitions are two atomic steps —
`DELETE` the old object, then `PUT If-None-Match: *` the new one
— rather than one CAS-style PUT. The protocol tolerates the brief
"no claim object exists" interregnum between those two steps
because anyone observing a 404 on the claim key proceeds via the
same first-claim path that already works correctly.

### 3.1 First-time claim (unchanged)

```text
PUT shards/<shard>.parquet.claim
    Body: { host, epoch=1, claimed_utc, state=Active }
    If-None-Match: *
  → 200, etag=E0  : we hold the claim.
  → 412           : someone else already claimed; bail and try
                    a different shard.
```

This is the only transition that already works correctly on
var204; v2 leaves it alone.

### 3.2 Heartbeat as detection (CHANGED)

In v1, the heartbeat task issues `PUT If-Match` on every tick to
both prove liveness and roll the etag forward. In v2, the
heartbeat task does *not* rewrite the claim. The tick loop becomes:

```text
on every tick:
  unconditional PUT progress/host-<id>.json   (observability;
                                              already worked in v1)
  HEAD shards/<shard>.parquet.claim           (cheap; etag in headers)
  if HEAD.etag != held.etag:
      fence.trip("claim object replaced under us")
      break
```

This collapses the heartbeat into a pure observer of ownership.
Liveness is still expressed — through the unconditional progress
write — but it no longer *rolls* the claim. Detection of "someone
else reclaimed" becomes an etag comparison instead of a
PUT-returns-412.

The lease itself is enforced by the body of the claim. The
`claimed_utc` field is wall-clock-stamped at claim time and not
refreshed; reclaimers compute lease age from the *observed*
`claimed_utc` rather than from any rewrite cadence. This is fine
because liveness is signaled separately via `progress/host-*.json`;
an aggregator that wants to flag a wedged owner reads the
heartbeat field there.

The previous contract — "the heartbeat refresh is the
self-fence trigger" — is preserved end-to-end. The only change is
the mechanism: HEAD-and-compare instead of PUT-returns-412.

### 3.3 Reclaim (CHANGED)

```text
1. HEAD shards/<shard>.parquet.claim
   → (observed_etag, observed_body)
   → 404: claim already gone — fall through to first-time claim
          path (3.1) directly.

2. parse observed_body.claimed_utc; compute lease_age.
   if lease_age <= LEASE_TIMEOUT_SEC: not our turn — wait.

3. DELETE shards/<shard>.parquet.claim
       If-Match: <observed_etag>
   → 200: we won the delete race; proceed to step 4.
   → 412: someone else mutated the claim under us; restart from
          step 1.
   → 404: somebody else already deleted it; proceed to step 4
          (the next PUT will collide with their PUT or create
          fresh — either way, race-safe).

4. PUT shards/<shard>.parquet.claim
       Body: { host=B, epoch=observed_epoch+1, claimed_utc=now,
               state=Active }
       If-None-Match: *
   → 200, etag=E_new : we now hold the claim.
   → 412             : another reclaimer raced through after our
                       successful DELETE. Bail; we lost. Restart
                       from step 1.
```

There is a brief interregnum between step 3 and step 4 where the
claim object does not exist. Any observer HEADing during that
window sees 404 — exactly the same state as a never-claimed shard
— and proceeds along path 3.1, whose mutual exclusion is
empirically enforced. The two-step sequence has no observable
state in which two workers can both succeed: at most one DELETE
wins, and at most one PUT-If-None-Match wins.

### 3.4 Complete (CHANGED)

The terminal-state transition has the same shape as reclaim, just
with a different new body:

```text
1. DELETE shards/<shard>.parquet.claim
       If-Match: <held.etag>
   → 200: we still owned it; proceed.
   → 412 or 404: we lost ownership during the shard. Trip fence;
                 do not rewrite.

2. PUT shards/<shard>.parquet.claim
       Body: { host=A, epoch=held.epoch, claimed_utc=held.claimed_utc,
               state=Completed }
       If-None-Match: *
   → 200: completion record written.
   → 412: someone reclaimed in between our DELETE and our PUT.
          Trip fence; the shard is now under another worker, who
          will re-process or re-complete on their own terms.
```

Once step 2 returns 200, the worker drops its held-claim state and
the heartbeat loop moves on to the next shard.

## 4. Code surface

The protocol is contained in `crates/migration-core/src/claim.rs`
and the `ClaimStore` trait it defines. v2 should land as a single
PR with a feature flag (see §7) so the change is reversible.

What changes:

- `crates/migration-core/src/claim.rs`. The `ClaimStore` trait
  loses `put_if_match` and gains `delete_if_match` and
  `head` (returning `(etag, body)` or `None` on 404). The
  high-level functions `refresh`, `reclaim_stale`, and `complete`
  are rewritten to follow §3.2 / §3.3 / §3.4 respectively.
  `try_acquire` (today's first-time claim) stays as-is.
- `crates/migration-core/src/s3.rs`. `put_if_match` is removed.
  `delete_if_match` is added, implemented via
  `aws_sdk_s3::Client::delete_object().if_match(etag)`. `head` is
  added if not already present (the trait has `get` today;
  HEAD is a separate cheaper call worth introducing rather than
  paying the body-read cost on every tick). The doc comment at
  the top of the file is updated to match the actually-supported
  primitives.
- `crates/migration-worker/src/heartbeat.rs`. The tick loop is
  restructured per §3.2: progress write first, then HEAD-and-compare
  on the held claim's key. The `RefreshOutcome::Refreshed` arm
  goes away because nothing refreshes anymore; the `Lost` arm
  fires on etag-mismatch instead of on 412.

What stays the same:

- `crates/migration-mover/*`. The mover never touches claims; it
  only does NFS operations and respects the fence via the
  shard processor's row-level checks. v2 does not affect the
  mover at all.
- `crates/migration-worker/src/fence.rs`. The fence itself is the
  same atomic-flag-plus-cancel-token primitive; only the
  *trigger* for tripping it changes (etag mismatch vs. 412).
- `crates/migration-worker/src/shard_processor.rs`. Row-level
  fence checks and the `Phase 2 dirs` ordering are unaffected.
- `crates/migration-worker/src/orchestrator.rs`. Touched only
  where it interacts with `claim::refresh` / `reclaim_stale` /
  `complete` signatures; the call shapes change minimally.

Tests in `claim.rs` rewrite their `MockStore` to match the new
trait; the four existing scenarios (refresh-succeeds,
refresh-loses, reclaim-succeeds, reclaim-contended) re-express
themselves naturally as pre/post DELETE-PUT sequences with the
expected 200 / 412 / 404 outcomes.

## 5. Correctness sketch

Walking through the four interleaving scenarios that any claim
protocol must handle:

**Scenario 1: fresh shard, two workers race for first-time claim.**
Both call `try_acquire` (path 3.1). Both issue
`PUT If-None-Match: *` against the same key. The S3 endpoint
serializes; exactly one returns 200, the other returns 412. The
loser bails and tries another shard. Identical to v1.

**Scenario 2: owner alive, third party tries reclaim.**
The third party HEADs (3.3 step 1), parses `claimed_utc`, computes
`lease_age`. The owner's heartbeat is recent, `lease_age <
LEASE_TIMEOUT_SEC`, the third party does not proceed past step 2.
No DELETE is issued; the owner remains the owner.

**Scenario 3: owner stalled (SIGSTOP), reclaimer attempts at
exactly the moment owner resumes.** The reclaimer HEADs and sees
a stale `claimed_utc`. It DELETEs `If-Match: <observed_etag>` —
this succeeds because the owner has not modified the claim during
the stall. The reclaimer PUTs `If-None-Match: *` — this succeeds
because the key is currently absent. The owner resumes, its next
heartbeat tick HEADs the claim key, sees a different etag (or 404
during the interregnum), and trips its fence. The owner stops
issuing rename commits before any further rows are committed.
This is precisely the property M5 was designed to verify.

**Scenario 4: two reclaimers race against each other.** Both HEAD
and observe the same `(etag, claimed_utc)`. Both reach step 3,
both issue `DELETE If-Match: <observed_etag>`. The S3 endpoint
serializes; exactly one returns 200 and proceeds to step 4. The
other returns 412 (etag changed, since the winner's DELETE
removed the object — actually the loser sees 404, since the
object is gone; either way, not 200). The loser restarts from
step 1 and finds either the winner's new claim or no claim at
all, and proceeds accordingly.

In none of the four scenarios can two workers simultaneously
believe they hold the same valid claim — every transition is
gated by a primitive the endpoint actually enforces.

## 6. Operational considerations

**HEAD cadence.** The heartbeat HEADs the claim key on every tick.
HEAD is cheap (no body transfer, just metadata) and the request
rate per shard is bounded by the heartbeat interval. At a 10s
default and a 100-host fleet, that's 10 HEAD/s aggregate per
active shard — well within ordinary S3 endpoint capacity. If the
cluster ever cares, the cadence can be throttled to every Nth
tick by trading a longer worst-case detection window for fewer
requests.

**Clock skew on lease_age.** `claimed_utc` is wall-clock from the
owner; the reclaimer uses its own wall-clock to compute age. With
typical NTP-bounded skew of <1s and a 60s lease, a skew of even
several seconds is irrelevant. Operators running across far-apart
clusters with poor NTP should already be tuning lease parameters.

**Aggregator.** Unchanged. The aggregator reads
`progress/host-*.json` for liveness and per-shard progress; that
object is unconditional-PUT and worked correctly in v1 and
continues to. The aggregator does not read claim objects directly,
so the protocol change is invisible to it.

**Failure modes.** The new protocol introduces one new transient
failure mode: the reclaimer's DELETE succeeds but its subsequent
PUT fails (network blip, PAM-gated retry, whatever). The claim
object does not exist for some window; the next observer treats
that as a fresh shard and reclaims via 3.1. The system
self-heals. There is no danger of "shard becomes unclaimable" —
absence of the claim key is a valid state v1 already handled at
startup.

## 7. Migration / staging

v2 should land behind a feature flag in the worker config:

```toml
[worker]
claim_protocol = "v1"   # or "v2"; default "v1" for first release.
```

For one or two releases the operator can pick which protocol to
run. v1 stays usable on endpoints that *do* honor `PUT If-Match`
(which is most non-VAST S3 implementations); v2 is the safe choice
on VAST. Mixed-protocol fleets are explicitly unsupported — every
worker on a given run must agree.

The success criterion for switching the default to v2 is a clean
M5 run on real VAST hardware: all seven assertions A–G PASS, with
the harness unmodified from its `m5-harness-verified` tagged
state. That run is the moment we know v2 actually solves the
protocol-enforcement gap. Once that lands, the v1 path can be
removed in a follow-up release.

## 8. Open questions

**HEAD load proportional to fleet size.** v1 already issues one
PUT per heartbeat per active shard; v2 replaces that with one
HEAD plus the existing unconditional progress PUT. Net request
count is ~the same. Worth confirming on a real 100-host run that
the endpoint handles the HEAD throughput proportionally.

**DELETE idempotency.** §3.3 step 3 expects 404 when the claim
key is absent (e.g. because another reclaimer beat us). Confirm
empirically that `DELETE If-Match: <etag>` on an absent key
returns 404 cleanly rather than some other error code we'd have
to special-case. The published VAST docs say 404; a quick CLI
check before implementation will confirm.

**Etag stability across HEAD vs. PUT response.** v2 assumes the
etag returned in the HEAD response header is byte-identical to
the etag the endpoint returned in the PUT response that created
or last modified the object. Standard S3 behavior, but worth a
sanity-check given the surprises in this protocol surface to
date.

**Cleanup of orphaned claim objects.** If a worker fleet exits
mid-run, claim objects for incomplete shards remain in S3 with
stale `claimed_utc`. v2 reclaim handles this naturally — any new
worker following 3.3 will see lease_age >> LEASE_TIMEOUT_SEC and
reclaim. There is no separate sweep needed. Worth documenting in
the runbook.

When v2 lands and the open questions are resolved, this doc gets
a closing entry pointing at the M5 run that demonstrates correct
multi-host self-fencing under the new protocol.
