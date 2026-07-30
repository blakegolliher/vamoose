//! Coord lease — single-writer election over `coord/lease`.
//!
//! Exactly one `vamoose coord` process owns the lease at a time. The
//! lease is short-lived (TTL ~ 30s) and refreshed every few seconds
//! by the holder. If the holder crashes or partitions, the lease
//! expires and another coord can take over.
//!
//! ## Primitives
//!
//! All three lease operations are built from the same VAST-safe
//! conditional primitives the v2 claim protocol uses:
//!
//! - **Cold acquire** — `PUT If-None-Match: *` on `coord/lease`. The
//!   only writer-side primitive VAST S3 honors atomically.
//! - **Refresh** — unconditional `PUT` while we hold it. Safe because
//!   only one coord can hold the lease at any time (enforced by the
//!   acquire path).
//! - **Takeover** — read current lease, verify expiry (with a clock-
//!   drift grace), `DELETE If-Match` the stale lease, then
//!   `PUT If-None-Match: *` a fresh one. Both conditionals lose
//!   cleanly under contention: the loser sees `EtagMismatch` /
//!   `AlreadyExists` and retries from the read.
//!
//! ## Failure modes
//!
//! - `Error::LeaseHeld` — another coord owns the lease and hasn't
//!   timed out yet. Caller backs off and retries.
//! - `Error::LeaseLost` — we owned the lease but someone took over
//!   between refreshes (we crashed past the TTL). Caller must stop
//!   writing every coord-owned key and exit.
//! - `Error::LeaseMalformed` — `coord/lease` exists but doesn't
//!   deserialize. Operator intervention required; coord refuses to
//!   take over a corrupt lease to avoid masking misconfiguration.
//!
//! ## Clock drift
//!
//! VAST and the coord hosts run NTP, but coord hosts may briefly
//! disagree by a few seconds. The takeover path requires
//! `expires_at + grace < now` (`grace` defaults to 5s) before
//! attempting the delete; this widens the takeover window slightly
//! but eliminates the race where two coords each think the other has
//! expired due to opposite-signed drift.
//!
//! ## Concurrent start
//!
//! Two coord processes starting at the same time both call
//! `try_acquire`. Both see the lease as missing, both call
//! `put_if_absent`. Exactly one gets `Created`; the other gets
//! `AlreadyExists`, reads the new lease, sees the other coord is the
//! holder, and returns `LeaseHeld`. The loser backs off; if the
//! winner crashes immediately, the loser succeeds on the next
//! attempt after TTL.

use crate::errors::{Error, Result};
use crate::layout::LEASE_KEY;
use crate::store::{CoordStore, PutOutcome};
use chrono::{DateTime, Duration, Utc};
use migration_core::claim::DeleteOutcome;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The process identity recorded in the lease.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Identity {
    /// UUID v4 minted at coord process start. Fresh per `vamoose
    /// coord` invocation — a crash-and-restart gets a new
    /// `holder_id`, which is why takeover after a holder crash waits
    /// for the previous lease's TTL to elapse before recovery
    /// succeeds (operator tunes TTL to match desired recovery RTO).
    pub holder_id: String,
    pub host: String,
    pub pid: u32,
}

impl Identity {
    /// Generate a fresh identity for the current process. Uses
    /// `hostname` from the workspace dep (already pulled in via
    /// `migration-worker`) — coord's caller is responsible for
    /// constructing one of these once at startup.
    pub fn fresh(host: String, pid: u32) -> Self {
        Self {
            holder_id: Uuid::new_v4().to_string(),
            host,
            pid,
        }
    }
}

/// The wire form of `coord/lease`. Stored as JSON.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseBody {
    pub holder_id: String,
    pub host: String,
    pub pid: u32,
    /// Wall-clock time at which the current holder first acquired or
    /// took over the lease. Refresh does not update this — only a
    /// takeover (or cold acquire) does.
    pub acquired_at: DateTime<Utc>,
    /// Wall-clock time after which another coord may take over.
    /// Refreshed every few seconds by the holder.
    pub expires_at: DateTime<Utc>,
    /// UUID v4 minted at acquire/takeover. Rotated on every takeover;
    /// stable across refreshes.
    pub lease_id: String,
}

/// A held lease — the body the holder last wrote plus the etag the
/// store returned. The etag is what makes refresh-after-takeover
/// detectable: if our last etag no longer matches the current object
/// (or the object is gone), someone else took over.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseHandle {
    pub body: LeaseBody,
    pub etag: String,
}

impl LeaseHandle {
    pub fn holder_id(&self) -> &str {
        &self.body.holder_id
    }
    pub fn lease_id(&self) -> &str {
        &self.body.lease_id
    }
    pub fn expires_at(&self) -> DateTime<Utc> {
        self.body.expires_at
    }
}

/// Tuning knobs for the lease protocol. Defaults match the build
/// prompt's "brief outage during failover" goal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseConfig {
    /// How long a freshly-acquired or refreshed lease lasts.
    pub ttl: Duration,
    /// Grace window past `expires_at` before takeover is attempted.
    /// Defends against modest clock drift between coord hosts.
    pub grace: Duration,
}

impl LeaseConfig {
    /// Default lease config — 30s TTL, 5s grace. With a 10s refresh
    /// cadence the holder writes three times per TTL window.
    pub fn default_for_prod() -> Self {
        Self {
            ttl: Duration::seconds(30),
            grace: Duration::seconds(5),
        }
    }
}

/// Outcome of a `try_acquire` call.
#[derive(Debug)]
pub enum AcquireOutcome {
    /// We acquired the lease (either cold or by takeover).
    Acquired(LeaseHandle),
    /// Another coord owns the lease and it has not yet expired.
    /// Caller backs off and tries again later.
    Held {
        holder_id: String,
        expires_at: DateTime<Utc>,
    },
}

/// Build a lease body for our identity at `now`, expiring after
/// `cfg.ttl`. Used by both cold acquire and takeover.
fn build_body(me: &Identity, cfg: LeaseConfig, now: DateTime<Utc>) -> LeaseBody {
    LeaseBody {
        holder_id: me.holder_id.clone(),
        host: me.host.clone(),
        pid: me.pid,
        acquired_at: now,
        expires_at: now + cfg.ttl,
        lease_id: Uuid::new_v4().to_string(),
    }
}

/// Bounded retry count for the takeover loop. Each iteration involves
/// at most one read + one conditional delete + one conditional put;
/// in steady state we converge after one or two passes even under
/// contention. Cap exists only to bound pathological cases where
/// many contenders are tossing the lease back and forth.
const MAX_TAKEOVER_ATTEMPTS: u32 = 8;

/// Try to acquire the lease, including taking over from an expired
/// holder. Returns `AcquireOutcome::Held` if another coord owns the
/// lease and hasn't timed out — the caller backs off and retries.
///
/// `now` is injected so tests can drive the lease past its expiry
/// without sleeping. Production callers pass `Utc::now()`.
pub async fn try_acquire(
    store: &dyn CoordStore,
    me: &Identity,
    cfg: LeaseConfig,
    now: DateTime<Utc>,
) -> Result<AcquireOutcome> {
    for _ in 0..MAX_TAKEOVER_ATTEMPTS {
        // 1. Cold acquire — works if no lease exists.
        let body = build_body(me, cfg, now);
        let payload = serde_json::to_vec(&body)?;
        match store.put_if_absent(LEASE_KEY, payload).await? {
            PutOutcome::Created(etag) => {
                return Ok(AcquireOutcome::Acquired(LeaseHandle { body, etag }));
            }
            PutOutcome::AlreadyExists => {}
        }

        // 2. Lease exists. Read it.
        let (current_body, current_etag) = match store.get(LEASE_KEY).await? {
            Some(o) => o,
            None => {
                // Disappeared between our PUT-if-absent and our GET.
                // Loop and retry the cold acquire — we may win it now.
                continue;
            }
        };
        let current: LeaseBody = serde_json::from_slice(&current_body)
            .map_err(|e| Error::LeaseMalformed(format!("parse: {e}")))?;

        // 3. Not yet expired? Caller backs off.
        if now < current.expires_at + cfg.grace {
            return Ok(AcquireOutcome::Held {
                holder_id: current.holder_id,
                expires_at: current.expires_at,
            });
        }

        // 4. Expired (past grace window) — attempt takeover.
        //    DELETE If-Match the stale lease. Loser of this race sees
        //    EtagMismatch / NotFound and loops to retry from step 1.
        match store.delete_if_match(LEASE_KEY, &current_etag).await? {
            DeleteOutcome::Deleted => {}
            DeleteOutcome::EtagMismatch | DeleteOutcome::NotFound => continue,
        }

        // 5. Lease key is now absent. Try to create our own.
        //    Loser here sees AlreadyExists and loops.
        let body = build_body(me, cfg, now);
        let payload = serde_json::to_vec(&body)?;
        match store.put_if_absent(LEASE_KEY, payload).await? {
            PutOutcome::Created(etag) => {
                return Ok(AcquireOutcome::Acquired(LeaseHandle { body, etag }));
            }
            PutOutcome::AlreadyExists => continue,
        }
    }

    // Fell out of the retry loop — pathological contention. Surface
    // as Other so the caller's backoff/exit path runs.
    Err(Error::Other(anyhow::anyhow!(
        "lease acquire failed after {MAX_TAKEOVER_ATTEMPTS} attempts",
    )))
}

/// Refresh an owned lease. The new body keeps `holder_id`,
/// `lease_id`, and `acquired_at` from the existing handle and pushes
/// `expires_at` forward by `cfg.ttl`.
///
/// Returns `Error::LeaseLost` if we no longer own the lease (the
/// store's current etag doesn't match ours, or the object is gone).
/// In that case the caller must stop writing and exit; another coord
/// has taken over and any further writes from us would clobber its
/// state.
pub async fn refresh(
    store: &dyn CoordStore,
    handle: &LeaseHandle,
    cfg: LeaseConfig,
    now: DateTime<Utc>,
) -> Result<LeaseHandle> {
    // Pre-flight: confirm the lease still has our etag. The store-
    // level write itself is unconditional (last-write-wins), so we
    // need this read-modify guard to detect takeover. The race
    // window between this head and the put is tight; we accept it
    // because the alternative — a conditional PUT — is the primitive
    // VAST S3 doesn't honor.
    match store.head(LEASE_KEY).await? {
        Some(etag) if etag == handle.etag => {}
        Some(_) | None => return Err(Error::LeaseLost),
    }

    let new_body = LeaseBody {
        expires_at: now + cfg.ttl,
        ..handle.body.clone()
    };
    let payload = serde_json::to_vec(&new_body)?;
    let new_etag = store.put(LEASE_KEY, payload).await?;
    Ok(LeaseHandle {
        body: new_body,
        etag: new_etag,
    })
}

/// Release the lease — delete `coord/lease` if it still has our
/// etag. A coord shutting down cleanly calls this to let the next
/// coord take over without waiting for TTL.
///
/// Treats `EtagMismatch` and `NotFound` as success — they mean the
/// lease is already not-ours, which is the same state we were trying
/// to reach.
pub async fn release(store: &dyn CoordStore, handle: &LeaseHandle) -> Result<()> {
    let outcome = store.delete_if_match(LEASE_KEY, &handle.etag).await?;
    match outcome {
        DeleteOutcome::Deleted | DeleteOutcome::EtagMismatch | DeleteOutcome::NotFound => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemStore;
    use chrono::TimeZone;

    fn me(holder: &str) -> Identity {
        Identity {
            holder_id: holder.to_string(),
            host: "host-a".to_string(),
            pid: 1234,
        }
    }

    fn cfg() -> LeaseConfig {
        LeaseConfig {
            ttl: Duration::seconds(30),
            grace: Duration::seconds(5),
        }
    }

    fn t0() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 5, 29, 14, 32, 0).unwrap()
    }

    fn acquired(o: AcquireOutcome) -> LeaseHandle {
        match o {
            AcquireOutcome::Acquired(h) => h,
            AcquireOutcome::Held { .. } => panic!("expected Acquired, got Held"),
        }
    }

    #[tokio::test]
    async fn cold_acquire_creates_lease() {
        let s = MemStore::new();
        let me = me("A");
        let h = acquired(try_acquire(&s, &me, cfg(), t0()).await.unwrap());
        assert_eq!(h.holder_id(), "A");
        assert_eq!(h.body.host, "host-a");
        assert_eq!(h.expires_at(), t0() + Duration::seconds(30));

        let stored = s.get(LEASE_KEY).await.unwrap().unwrap();
        assert_eq!(stored.1, h.etag);
    }

    #[tokio::test]
    async fn second_acquire_during_ttl_returns_held() {
        let s = MemStore::new();
        let me_a = me("A");
        let me_b = me("B");
        let _h_a = acquired(try_acquire(&s, &me_a, cfg(), t0()).await.unwrap());

        let out = try_acquire(&s, &me_b, cfg(), t0() + Duration::seconds(1))
            .await
            .unwrap();
        match out {
            AcquireOutcome::Held { holder_id, .. } => assert_eq!(holder_id, "A"),
            AcquireOutcome::Acquired(_) => panic!("B should not have acquired"),
        }
    }

    #[tokio::test]
    async fn takeover_succeeds_after_ttl_plus_grace() {
        let s = MemStore::new();
        let me_a = me("A");
        let me_b = me("B");
        let h_a = acquired(try_acquire(&s, &me_a, cfg(), t0()).await.unwrap());
        // Past expiry + grace.
        let later = h_a.expires_at() + Duration::seconds(6);
        let h_b = acquired(try_acquire(&s, &me_b, cfg(), later).await.unwrap());
        assert_eq!(h_b.holder_id(), "B");
        // Takeover mints a fresh lease_id — A's lease_id no longer in the store.
        assert_ne!(h_a.lease_id(), h_b.lease_id());
    }

    #[tokio::test]
    async fn takeover_during_grace_window_is_blocked() {
        let s = MemStore::new();
        let me_a = me("A");
        let me_b = me("B");
        let h_a = acquired(try_acquire(&s, &me_a, cfg(), t0()).await.unwrap());

        // Past expiry but inside the grace window.
        let during_grace = h_a.expires_at() + Duration::seconds(3);
        let out = try_acquire(&s, &me_b, cfg(), during_grace).await.unwrap();
        match out {
            AcquireOutcome::Held { holder_id, .. } => assert_eq!(holder_id, "A"),
            AcquireOutcome::Acquired(_) => panic!("grace window must block takeover"),
        }
    }

    #[tokio::test]
    async fn refresh_preserves_holder_and_lease_id() {
        let s = MemStore::new();
        let me_a = me("A");
        let h = acquired(try_acquire(&s, &me_a, cfg(), t0()).await.unwrap());
        let h2 = refresh(&s, &h, cfg(), t0() + Duration::seconds(5))
            .await
            .unwrap();
        assert_eq!(h2.holder_id(), h.holder_id());
        assert_eq!(h2.lease_id(), h.lease_id());
        assert_eq!(h2.body.acquired_at, h.body.acquired_at);
        assert_eq!(h2.expires_at(), t0() + Duration::seconds(35));
        // Etag rotates on every write.
        assert_ne!(h2.etag, h.etag);
    }

    #[tokio::test]
    async fn refresh_after_takeover_returns_lease_lost() {
        let s = MemStore::new();
        let me_a = me("A");
        let me_b = me("B");
        let h_a = acquired(try_acquire(&s, &me_a, cfg(), t0()).await.unwrap());

        // B takes over after A's lease expires.
        let later = h_a.expires_at() + Duration::seconds(10);
        let _h_b = acquired(try_acquire(&s, &me_b, cfg(), later).await.unwrap());

        // A tries to refresh — its etag no longer matches.
        let err = refresh(&s, &h_a, cfg(), later + Duration::seconds(1))
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::LeaseLost),
            "expected LeaseLost, got {err:?}",
        );
    }

    #[tokio::test]
    async fn refresh_after_release_returns_lease_lost() {
        let s = MemStore::new();
        let me_a = me("A");
        let h = acquired(try_acquire(&s, &me_a, cfg(), t0()).await.unwrap());
        release(&s, &h).await.unwrap();

        let err = refresh(&s, &h, cfg(), t0() + Duration::seconds(1))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::LeaseLost));
    }

    // === F19: refresh must be delete-then-create, never PUT ===

    /// The F19 headline: a refresh is exactly one conditional delete
    /// followed by one conditional create — the two VAST-safe atoms.
    /// An unconditional `PUT` (last-write-wins) anywhere in the
    /// refresh path is the split-brain bug: a deposed holder's PUT
    /// silently overwrites a completed takeover.
    #[tokio::test]
    #[ignore = "red against the status-quo unconditional PUT; fix lands in the next commit (F19)"]
    async fn refresh_never_writes_unconditionally() {
        let s = MemStore::new();
        let h = acquired(try_acquire(&s, &me("A"), cfg(), t0()).await.unwrap());
        s.clear_ops();

        refresh(&s, &h, cfg(), t0() + Duration::seconds(5))
            .await
            .unwrap();

        assert_eq!(
            s.ops(),
            vec![
                format!("DELETE_IF_MATCH {LEASE_KEY}"),
                format!("PUT_IF_ABSENT {LEASE_KEY}"),
            ],
            "refresh must be exactly delete_if_match then put_if_absent — \
             no unconditional PUT, ever",
        );
    }

    #[tokio::test]
    async fn refresh_rotates_etag_and_extends_expiry() {
        let s = MemStore::new();
        let h = acquired(try_acquire(&s, &me("A"), cfg(), t0()).await.unwrap());

        let h2 = refresh(&s, &h, cfg(), t0() + Duration::seconds(5))
            .await
            .unwrap();
        assert_ne!(h2.etag, h.etag, "every refresh write rotates the etag");
        assert_eq!(h2.expires_at(), t0() + Duration::seconds(35));
        assert!(h2.expires_at() > h.expires_at());
        assert_eq!(
            h2.lease_id(),
            h.lease_id(),
            "lease_id rotates only on takeover"
        );

        // The refreshed handle must itself be refreshable — the new
        // etag is the store's current one.
        let h3 = refresh(&s, &h2, cfg(), t0() + Duration::seconds(10))
            .await
            .unwrap();
        assert_eq!(h3.lease_id(), h.lease_id());
        assert_eq!(h3.expires_at(), t0() + Duration::seconds(40));
    }

    /// Wraps `MemStore` to force the create-race interleave inside
    /// refresh: the instant the holder's `delete_if_match` on the
    /// lease succeeds, a candidate's `put_if_absent` lands in the
    /// delete→create gap. The holder's own create must then lose
    /// with `AlreadyExists`.
    #[derive(Debug)]
    struct CreateRaceStore {
        inner: MemStore,
        candidate_body: Vec<u8>,
    }

    #[async_trait::async_trait]
    impl CoordStore for CreateRaceStore {
        async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
            self.inner.get(key).await
        }
        async fn head(&self, key: &str) -> Result<Option<String>> {
            self.inner.head(key).await
        }
        async fn put(&self, key: &str, body: Vec<u8>) -> Result<String> {
            self.inner.put(key, body).await
        }
        async fn put_if_absent(&self, key: &str, body: Vec<u8>) -> Result<PutOutcome> {
            self.inner.put_if_absent(key, body).await
        }
        async fn delete(&self, key: &str) -> Result<()> {
            self.inner.delete(key).await
        }
        async fn delete_if_match(&self, key: &str, etag: &str) -> Result<DeleteOutcome> {
            let out = self.inner.delete_if_match(key, etag).await?;
            if key == LEASE_KEY && out == DeleteOutcome::Deleted {
                // Candidate wins the race for the now-absent key.
                let seeded = self
                    .inner
                    .put_if_absent(key, self.candidate_body.clone())
                    .await?;
                assert!(
                    matches!(seeded, PutOutcome::Created(_)),
                    "race seed must land on the just-deleted key",
                );
            }
            Ok(out)
        }
        async fn list(&self, prefix: &str) -> Result<Vec<crate::store::ListEntry>> {
            self.inner.list(prefix).await
        }
    }

    #[tokio::test]
    #[ignore = "red against the status-quo unconditional PUT; fix lands in the next commit (F19)"]
    async fn refresh_loses_create_race_returns_lease_lost() {
        let candidate_body =
            serde_json::to_vec(&build_body(&me("B"), cfg(), t0() + Duration::seconds(5))).unwrap();
        let s = CreateRaceStore {
            inner: MemStore::new(),
            candidate_body: candidate_body.clone(),
        };
        let h = acquired(try_acquire(&s, &me("A"), cfg(), t0()).await.unwrap());

        // A's refresh: its delete succeeds, then B's create slips
        // into the gap before A's own create.
        let res = refresh(&s, &h, cfg(), t0() + Duration::seconds(5)).await;
        match res {
            // Must be the exact variant ticks.rs maps to
            // mark_lease_lost (the F02 write gate's only input).
            Err(Error::LeaseLost) => {}
            other => panic!(
                "candidate won the create race — refresh must return LeaseLost, got {other:?}",
            ),
        }

        // B's lease survives untouched: A never overwrote it.
        let (body, _etag) = s.inner.get(LEASE_KEY).await.unwrap().unwrap();
        assert_eq!(body, candidate_body, "the race winner's lease must survive");
    }

    #[tokio::test]
    async fn refresh_delete_conflict_returns_lease_lost() {
        let s = MemStore::new();
        let h = acquired(try_acquire(&s, &me("A"), cfg(), t0()).await.unwrap());

        // The lease object was rewritten out from under us — same
        // body, but the etag rotated, so our held etag is stale.
        s.put(LEASE_KEY, serde_json::to_vec(&h.body).unwrap())
            .await
            .unwrap();

        let err = refresh(&s, &h, cfg(), t0() + Duration::seconds(5))
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::LeaseLost),
            "stale etag at delete time must resolve to LeaseLost, got {err:?}",
        );
    }

    #[tokio::test]
    async fn release_then_reacquire_within_ttl_succeeds() {
        let s = MemStore::new();
        let me_a = me("A");
        let me_b = me("B");
        let h_a = acquired(try_acquire(&s, &me_a, cfg(), t0()).await.unwrap());
        release(&s, &h_a).await.unwrap();

        let h_b = acquired(
            try_acquire(&s, &me_b, cfg(), t0() + Duration::seconds(1))
                .await
                .unwrap(),
        );
        assert_eq!(h_b.holder_id(), "B");
    }

    #[tokio::test]
    async fn release_idempotent_after_takeover() {
        // A holds, B takes over, A releases. A's release is a delete-
        // if-match that sees the wrong etag — should be silent.
        let s = MemStore::new();
        let me_a = me("A");
        let me_b = me("B");
        let h_a = acquired(try_acquire(&s, &me_a, cfg(), t0()).await.unwrap());
        let later = h_a.expires_at() + Duration::seconds(10);
        let _h_b = acquired(try_acquire(&s, &me_b, cfg(), later).await.unwrap());

        // A's stale release does not touch B's lease.
        release(&s, &h_a).await.unwrap();
        let (_body, etag) = s.get(LEASE_KEY).await.unwrap().unwrap();
        assert!(!etag.is_empty());
    }

    #[tokio::test]
    async fn malformed_lease_propagates() {
        let s = MemStore::new();
        s.put(LEASE_KEY, b"not json".to_vec()).await.unwrap();
        let err = try_acquire(&s, &me("A"), cfg(), t0()).await.unwrap_err();
        assert!(
            matches!(err, Error::LeaseMalformed(_)),
            "expected LeaseMalformed, got {err:?}",
        );
    }

    /// Concurrent contenders simulated by interleaving the
    /// acquire+takeover steps from both sides. The deterministic
    /// orderings the implementation must handle correctly are exercised
    /// in the dedicated takeover/refresh tests above; this one just
    /// asserts the high-level invariant — at most one
    /// `AcquireOutcome::Acquired` per lease epoch.
    #[tokio::test]
    async fn concurrent_takeover_yields_exactly_one_winner() {
        let s = MemStore::new();
        let me_a = me("A");
        let h_a = acquired(try_acquire(&s, &me_a, cfg(), t0()).await.unwrap());
        let past_grace = h_a.expires_at() + Duration::seconds(6);

        // Two contenders, same lease epoch — Mem store serializes
        // through the mutex but the put_if_absent / delete_if_match
        // race is the same one S3 would adjudicate. Whichever lands
        // its put_if_absent first wins; the other sees Held.
        let b = try_acquire(&s, &me("B"), cfg(), past_grace).await.unwrap();
        let c = try_acquire(&s, &me("C"), cfg(), past_grace).await.unwrap();

        let acquired_count = [&b, &c]
            .iter()
            .filter(|o| matches!(o, AcquireOutcome::Acquired(_)))
            .count();
        assert_eq!(acquired_count, 1, "exactly one contender must win");
    }
}
