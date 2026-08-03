//! Claim protocol — v2 (delete-then-create).
//!
//! Workers coordinate ownership of parquet shards through S3 against
//! `shards/<shard>.parquet.claim`. v2 uses three primitives that VAST
//! S3 actually enforces:
//!
//! - `PUT If-None-Match: *`  — atomic create-if-absent (first claim,
//!   and the new-state half of every reclaim/complete).
//! - `DELETE If-Match: <etag>` — atomic delete-if-current (the
//!   old-state half of every reclaim/complete).
//! - HEAD / GET for etag-compare — used by `refresh` to detect that
//!   the claim object was replaced under us.
//!
//! v2 explicitly does **not** use `PUT If-Match` for ownership
//! transitions: that primitive is not enforced on the var204 endpoint
//! (silently overwrites and returns 200), which collapsed v1's
//! mutual-exclusion story for refresh / reclaim / complete. See
//! `docs/work-items/CLAIM_PROTOCOL_V2_DELETE_THEN_CREATE.md` for the
//! full design rationale.
//!
//! ## Invariant
//!
//! At any instant, the worker writing to dest paths derived from rows
//! in shard X must hold the most recent etag of `shards/<X>.claim`.
//! Because v2 owners do not rewrite the claim while holding it, the
//! held etag stays stable for the lifetime of the claim — until a
//! reclaimer deletes it.
//!
//! ## Lifecycle
//!
//! ```text
//!  acquire  PUT If-None-Match: *                  → owned (etag E0)
//!  refresh  HEAD shards/<X>.claim every tick       → still owned (etag == E0)
//!           or HEAD returns different etag / 404   → CLAIM LOST → self-fence
//!  complete DELETE If-Match: E0; PUT If-None-Match: * (state=Completed)
//!  reclaim  DELETE If-Match: <observed>; PUT If-None-Match: *
//! ```

use crate::errors::{Error, Result};
use crate::records::{ClaimRecord, ClaimState};
use crate::time::UtcTime;
use async_trait::async_trait;
use std::time::Duration;

/// Maximum attempts for the terminal-state PUT inside `complete` /
/// `fail`. Only counts transient errors — `PreconditionFailed` is a
/// real Lost signal and is never retried.
///
/// The reclaim path is intentionally NOT retried: a transient
/// failure there is safe to surface as `Err` (caller exits the loop;
/// peer eventually picks up the still-stale claim). Terminal-state
/// writes are different — the DELETE already succeeded, so the
/// claim object is transiently absent on S3; without the retry, the
/// next worker re-acquires the shard and silently redoes work.
const TERMINAL_PUT_ATTEMPTS: u32 = 3;
const TERMINAL_PUT_BACKOFF_BASE_MS: u64 = 1000;

/// PUT-If-None-Match with exponential backoff on transient errors.
/// Used by the new-state half of `complete` and `fail` after their
/// DELETE has already succeeded — at that point the claim object is
/// absent on S3, and a transient PUT failure that we surface as
/// `Err` means the next worker re-acquires the shard and redoes the
/// shard's work. Retrying for a few seconds is cheaper than that.
///
/// `Error::PreconditionFailed` is returned immediately without
/// retry: it means a fresh `try_acquire` won the absent-window
/// race, which is a genuine `Lost` for our caller. Retrying that
/// would just race the new owner.
async fn put_if_absent_with_retry(
    store: &dyn ClaimStore,
    key: &str,
    body: Vec<u8>,
    attempts: u32,
) -> Result<String> {
    debug_assert!(attempts >= 1, "attempts must be >= 1");
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        match store.put_if_absent(key, body.clone()).await {
            Ok(etag) => return Ok(etag),
            Err(Error::PreconditionFailed) => return Err(Error::PreconditionFailed),
            Err(e) if attempt >= attempts => return Err(e),
            Err(e) => {
                let delay_ms = TERMINAL_PUT_BACKOFF_BASE_MS << (attempt - 1);
                tracing::warn!(
                    key = %key,
                    attempt,
                    of = attempts,
                    delay_ms,
                    error = ?e,
                    "terminal-state PUT failed transiently; retrying after backoff",
                );
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
        }
    }
}

/// Outcome of a claim acquisition attempt.
#[derive(Debug)]
pub enum AcquireOutcome {
    /// Claim acquired; the etag returned is the proof of ownership.
    /// In v2 this etag stays valid until a reclaimer (or this worker's
    /// own `complete`) deletes the object.
    Acquired { etag: String, record: ClaimRecord },
    /// Another worker holds the claim. The reader can inspect
    /// `existing` to decide whether to wait or attempt a stale-reclaim.
    Contended {
        existing_etag: String,
        existing: ClaimRecord,
    },
}

/// Outcome of a heartbeat refresh (HEAD-and-compare in v2).
#[derive(Debug)]
pub enum RefreshOutcome {
    /// The claim object on S3 still has the etag we hold; we are
    /// still the owner.
    StillHeld { etag: String },
    /// The claim object's etag differs from ours, or the object is
    /// gone. Someone reclaimed — the worker must self-fence.
    Lost,
}

/// Outcome of a reclaim attempt (delete-then-create in v2). Either
/// arm of the two-step sequence may lose to a concurrent reclaimer;
/// `LostRace` is the unified "didn't win" signal.
#[derive(Debug)]
pub enum ReclaimOutcome {
    /// We won the delete race AND the subsequent create race; we now
    /// hold the claim with the returned etag.
    Won { etag: String, record: ClaimRecord },
    /// Either the DELETE got 412/404 (someone mutated the claim out
    /// from under us) or the PUT got 412 (someone created the new
    /// claim before we did). Caller should re-HEAD and reassess.
    LostRace,
}

/// Outcome of a `complete` attempt.
#[derive(Debug)]
pub enum CompleteOutcome {
    /// Completed record written; the shard is now in terminal state.
    Completed { etag: String },
    /// Either the DELETE didn't see our etag, or the PUT collided —
    /// in either case another worker took over. The fence should
    /// already be tripping via the heartbeat detection path; complete
    /// returns `Lost` so the caller can drop the held claim cleanly.
    Lost,
}

/// Outcome of a `fail` attempt. Mirrors `CompleteOutcome` — used to
/// mark a shard's claim as terminal-`Failed` when the shard cannot
/// be processed at all (e.g. corrupt parquet that won't decode).
/// Another worker reclaiming the shard would just re-encounter the
/// same error, so we want scanners to skip it and an operator to
/// intervene.
#[derive(Debug)]
pub enum FailOutcome {
    /// Failed record written; the shard is now in terminal state.
    Failed { etag: String },
    /// Either the DELETE didn't see our etag, or the PUT collided —
    /// another worker took over before we could mark Failed. Caller
    /// drops the held claim cleanly; the new owner will discover the
    /// same shard-fatal error and (eventually) mark it Failed itself.
    Lost,
}

/// Three-way result of `delete_if_match`. Maps the documented S3
/// outcomes (200 / 412 / 404) onto enum variants the protocol
/// distinguishes — none of them are errors per se; all three are
/// expected control-flow signals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteOutcome {
    /// 200 — the object was current on `etag` and is now deleted.
    Deleted,
    /// 412 — the object exists but its etag is not what we provided.
    EtagMismatch,
    /// 404 — the object does not exist.
    NotFound,
}

/// The S3-side operations the claim protocol needs. Implemented by
/// `s3::S3Client`. Abstracted as a trait so unit tests can drive a
/// fake without touching S3.
#[async_trait]
pub trait ClaimStore: Send + Sync {
    /// Conditional PUT with `If-None-Match: *`. Returns the new etag
    /// on success, `Error::PreconditionFailed` if the object already
    /// exists.
    async fn put_if_absent(&self, key: &str, body: Vec<u8>) -> Result<String>;

    /// Unconditional PUT. Used for non-claim objects: progress files,
    /// batch audit, failure logs. Default no-op for fakes that only
    /// exercise the claim path.
    async fn put_unconditional(&self, key: &str, body: Vec<u8>) -> Result<String> {
        let _ = (key, body);
        Ok(String::new())
    }

    /// HEAD an object — returns `(etag, body)` if present, `None` if
    /// absent. The body is read in full because callers (e.g.
    /// `claim::reclaim`) need the embedded `epoch` and `claimed_utc`
    /// fields to decide whether to proceed. Etag is returned in the
    /// unquoted form. Implementations may use HTTP HEAD or GET; for
    /// the few-hundred-byte claim objects the round-trip cost is the
    /// same.
    async fn head_object(&self, key: &str) -> Result<Option<(String, Vec<u8>)>>;

    /// Conditional DELETE with `If-Match: <etag>`. The three
    /// outcomes (200 / 412 / 404) all map to `DeleteOutcome`
    /// variants; callers must distinguish them to retry safely.
    async fn delete_if_match(&self, key: &str, etag: &str) -> Result<DeleteOutcome>;

    /// Unconditional GET. Returns `(body, etag)` or `None` if absent.
    /// Retained for back-compat with non-claim readers (manifest,
    /// shard parquet download). New v2 code paths should prefer
    /// [`Self::head_object`].
    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, String)>>;

    /// LIST objects under a prefix.
    async fn list(&self, prefix: &str) -> Result<Vec<ListEntry>>;
}

#[derive(Debug, Clone)]
pub struct ListEntry {
    pub key: String,
    pub etag: String,
    pub size: u64,
}

// =============================================================================
// High-level operations
// =============================================================================

/// Try to acquire the claim for `shard_filename`. Unchanged in v2.
///
/// 1. PUT with `If-None-Match: *`.
/// 2. On 412, GET the existing claim and return `Contended`.
pub async fn try_acquire(
    store: &dyn ClaimStore,
    shard_filename: &str,
    host: &str,
) -> Result<AcquireOutcome> {
    let key = crate::layout::claim_key(shard_filename);
    let record = ClaimRecord {
        host: host.to_string(),
        claimed_utc: UtcTime::now(),
        epoch: 1,
        state: ClaimState::Active,
    };
    let body = serde_json::to_vec(&record)?;

    match store.put_if_absent(&key, body).await {
        Ok(etag) => Ok(AcquireOutcome::Acquired { etag, record }),
        Err(Error::PreconditionFailed) => {
            // Read the contended claim so the caller can decide.
            let (existing_body, existing_etag) = store.get(&key).await?.ok_or_else(|| {
                Error::Other(anyhow::anyhow!(
                    "412 on PUT but GET returned None for {key}"
                ))
            })?;
            let existing: ClaimRecord = serde_json::from_slice(&existing_body)?;
            Ok(AcquireOutcome::Contended {
                existing_etag,
                existing,
            })
        }
        Err(e) => Err(e),
    }
}

/// Heartbeat detection — HEAD the claim object and compare its etag
/// against the etag we hold. Returns `StillHeld` only when the
/// observed etag exactly matches `held_etag`. Any other state (different
/// etag, or 404) is `Lost` and the caller must self-fence.
///
/// v2 owners do not rewrite the claim, so the held etag is stable
/// across the entire ownership window. `Lost` always means another
/// worker (or a manual operator) has replaced or removed the claim.
pub async fn refresh(
    store: &dyn ClaimStore,
    shard_filename: &str,
    held_etag: &str,
) -> Result<RefreshOutcome> {
    let key = crate::layout::claim_key(shard_filename);
    match store.head_object(&key).await? {
        Some((etag, _body)) if etag == held_etag => Ok(RefreshOutcome::StillHeld {
            etag: held_etag.to_string(),
        }),
        _ => Ok(RefreshOutcome::Lost),
    }
}

/// Attempt to reclaim a stale claim via delete-then-create.
///
/// The caller is responsible for first HEADing the claim, parsing its
/// `claimed_utc`, and confirming the lease has expired before invoking
/// this. v2 reclaim does not enforce lease semantics at the protocol
/// level — that policy is caller-side and observability-driven (lease
/// age + per-host progress liveness).
///
/// Step 1: `DELETE If-Match: <observed_etag>`. Etag mismatch or
/// not-found → another reclaimer beat us → `LostRace`.
///
/// Step 2: `PUT If-None-Match: *` with the new-owner body. 412 means
/// yet another reclaimer raced through after our DELETE → `LostRace`.
pub async fn reclaim(
    store: &dyn ClaimStore,
    shard_filename: &str,
    observed_etag: &str,
    new_host: &str,
    new_epoch: u64,
) -> Result<ReclaimOutcome> {
    let key = crate::layout::claim_key(shard_filename);

    match store.delete_if_match(&key, observed_etag).await? {
        DeleteOutcome::Deleted => {}
        DeleteOutcome::EtagMismatch | DeleteOutcome::NotFound => {
            return Ok(ReclaimOutcome::LostRace);
        }
    }

    let record = ClaimRecord {
        host: new_host.to_string(),
        claimed_utc: UtcTime::now(),
        epoch: new_epoch,
        state: ClaimState::Active,
    };
    let body = serde_json::to_vec(&record)?;

    match store.put_if_absent(&key, body).await {
        Ok(etag) => Ok(ReclaimOutcome::Won { etag, record }),
        Err(Error::PreconditionFailed) => Ok(ReclaimOutcome::LostRace),
        Err(e) => Err(e),
    }
}

/// Mark a shard `Completed` via delete-then-create.
///
/// Same shape as `reclaim` but with a terminal-state body. If either
/// step doesn't see our held etag (DELETE returned EtagMismatch /
/// NotFound, or PUT returned 412), another worker has taken over —
/// the fence should already be tripping via the heartbeat path.
/// `complete` returns `Lost` so the caller can drop its held-claim
/// state cleanly.
pub async fn complete(
    store: &dyn ClaimStore,
    shard_filename: &str,
    held_etag: &str,
    host: &str,
    epoch: u64,
) -> Result<CompleteOutcome> {
    let key = crate::layout::claim_key(shard_filename);

    match store.delete_if_match(&key, held_etag).await? {
        DeleteOutcome::Deleted => {}
        DeleteOutcome::EtagMismatch | DeleteOutcome::NotFound => {
            return Ok(CompleteOutcome::Lost);
        }
    }

    let record = ClaimRecord {
        host: host.to_string(),
        claimed_utc: UtcTime::now(),
        epoch,
        state: ClaimState::Completed,
    };
    let body = serde_json::to_vec(&record)?;

    match put_if_absent_with_retry(store, &key, body, TERMINAL_PUT_ATTEMPTS).await {
        Ok(etag) => Ok(CompleteOutcome::Completed { etag }),
        Err(Error::PreconditionFailed) => Ok(CompleteOutcome::Lost),
        Err(e) => Err(e),
    }
}

/// Mark a shard `Failed` via delete-then-create.
///
/// Identical shape to `complete` but writes `ClaimState::Failed`.
/// Called by the orchestrator when a shard is shard-fatal — i.e.
/// the error would re-occur for any worker that reclaimed and
/// retried (corrupt parquet, malformed row schema). Marking
/// `Failed` keeps scanners from picking the shard up again; the
/// operator needs to intervene.
///
/// Distinct from `complete` so the on-disk state distinguishes
/// "this shard finished cleanly" from "this shard couldn't be
/// processed." Both are terminal for the scanner.
pub async fn fail(
    store: &dyn ClaimStore,
    shard_filename: &str,
    held_etag: &str,
    host: &str,
    epoch: u64,
) -> Result<FailOutcome> {
    let key = crate::layout::claim_key(shard_filename);

    match store.delete_if_match(&key, held_etag).await? {
        DeleteOutcome::Deleted => {}
        DeleteOutcome::EtagMismatch | DeleteOutcome::NotFound => {
            return Ok(FailOutcome::Lost);
        }
    }

    let record = ClaimRecord {
        host: host.to_string(),
        claimed_utc: UtcTime::now(),
        epoch,
        state: ClaimState::Failed,
    };
    let body = serde_json::to_vec(&record)?;

    match put_if_absent_with_retry(store, &key, body, TERMINAL_PUT_ATTEMPTS).await {
        Ok(etag) => Ok(FailOutcome::Failed { etag }),
        Err(Error::PreconditionFailed) => Ok(FailOutcome::Lost),
        Err(e) => Err(e),
    }
}

/// In-memory mock `ClaimStore` shared by this crate's protocol unit
/// tests and (behind the `test-util` feature) by downstream crates'
/// integration tests — e.g. `migration-worker/tests/two_live_workers.rs`.
/// One canonical mock; do not fork divergent copies.
#[cfg(any(test, feature = "test-util"))]
pub mod test_util {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// One entry in the [`FakeStore`] operation log. Timestamped with
    /// the real wall clock (`chrono::Utc::now()`) — the same clock
    /// domain the claim records' `claimed_utc` fields live in — so
    /// tests can compute e.g. "age of the claim that a reclaim
    /// deleted" from the log alone.
    #[derive(Debug, Clone)]
    pub struct OpRecord {
        pub at: chrono::DateTime<chrono::Utc>,
        pub kind: OpKind,
    }

    #[derive(Debug, Clone)]
    pub enum OpKind {
        PutIfAbsent {
            key: String,
            body: Vec<u8>,
            /// `true` when the PUT created the object (owner/terminal
            /// state landed); `false` on 412/rigged failure.
            ok: bool,
        },
        PutUnconditional {
            key: String,
            body: Vec<u8>,
        },
        DeleteIfMatch {
            key: String,
            etag: String,
            outcome: DeleteOutcome,
            /// Body of the object that was removed, when
            /// `outcome == Deleted`. Lets tests parse the claim
            /// record that a reclaim/complete displaced.
            deleted_body: Option<Vec<u8>>,
        },
    }

    /// In-memory ClaimStore that mimics S3's v2 conditional semantics
    /// (`PUT If-None-Match: *`, `DELETE If-Match: <etag>`, HEAD).
    /// Drives the protocol unit tests. Every mutation is appended to
    /// an op log (see [`OpRecord`]) so concurrency tests can assert on
    /// exactly which transitions happened, not just on final state.
    pub struct FakeStore {
        inner: Mutex<HashMap<String, (Vec<u8>, String)>>,
        etag_counter: Mutex<u64>,
        ops: Mutex<Vec<OpRecord>>,
        /// Test rig for the terminal-state retry path. When non-zero,
        /// the next N `put_if_absent` calls return the configured
        /// error WITHOUT mutating state; the counter decrements on
        /// each failed attempt and the (N+1)-th call falls through
        /// to the normal logic. `put_failure_kind` selects between
        /// transient (retryable) and PreconditionFailed (not).
        put_failures_remaining: Mutex<u32>,
        put_failure_kind: Mutex<RiggedFailureKind>,
        /// F42 test rigs: same shape as the PUT rig, for `list` and
        /// `get`. Call counters let retry tests assert op counts
        /// (attempts made) without extending the op log.
        list_failures_remaining: Mutex<u32>,
        list_failure_kind: Mutex<RiggedFailureKind>,
        list_calls: Mutex<u64>,
        get_failures_remaining: Mutex<u32>,
        get_failure_kind: Mutex<RiggedFailureKind>,
        get_calls: Mutex<u64>,
    }

    #[derive(Clone, Copy)]
    pub enum RiggedFailureKind {
        Transient,
        PreconditionFailed,
    }

    impl FakeStore {
        pub fn new() -> Self {
            Self {
                inner: Mutex::new(HashMap::new()),
                etag_counter: Mutex::new(0),
                ops: Mutex::new(Vec::new()),
                put_failures_remaining: Mutex::new(0),
                put_failure_kind: Mutex::new(RiggedFailureKind::Transient),
                list_failures_remaining: Mutex::new(0),
                list_failure_kind: Mutex::new(RiggedFailureKind::Transient),
                list_calls: Mutex::new(0),
                get_failures_remaining: Mutex::new(0),
                get_failure_kind: Mutex::new(RiggedFailureKind::Transient),
                get_calls: Mutex::new(0),
            }
        }
        fn next_etag(&self) -> String {
            let mut c = self.etag_counter.lock().unwrap();
            *c += 1;
            format!("etag-{}", *c)
        }

        fn log(&self, kind: OpKind) {
            self.ops.lock().unwrap().push(OpRecord {
                at: chrono::Utc::now(),
                kind,
            });
        }

        /// Snapshot of the op log so far, in call order.
        pub fn op_log(&self) -> Vec<OpRecord> {
            self.ops.lock().unwrap().clone()
        }

        /// Configure the next `n` `put_if_absent` calls to return
        /// the given failure mode without mutating state. Used by
        /// terminal-state retry tests.
        pub fn rig_next_puts_to_fail(&self, n: u32, kind: RiggedFailureKind) {
            *self.put_failures_remaining.lock().unwrap() = n;
            *self.put_failure_kind.lock().unwrap() = kind;
        }

        /// How many rigged failures remain to consume.
        pub fn rigged_remaining(&self) -> u32 {
            *self.put_failures_remaining.lock().unwrap()
        }

        /// F42: configure the next `n` `list` calls to fail with the
        /// given kind without touching state. Same rigging style as
        /// [`Self::rig_next_puts_to_fail`].
        pub fn rig_next_lists_to_fail(&self, n: u32, kind: RiggedFailureKind) {
            *self.list_failures_remaining.lock().unwrap() = n;
            *self.list_failure_kind.lock().unwrap() = kind;
        }

        /// F42: configure the next `n` `get` calls to fail with the
        /// given kind without touching state.
        pub fn rig_next_gets_to_fail(&self, n: u32, kind: RiggedFailureKind) {
            *self.get_failures_remaining.lock().unwrap() = n;
            *self.get_failure_kind.lock().unwrap() = kind;
        }

        /// Total `list` calls made (rigged failures included) — lets
        /// retry tests assert attempt counts.
        pub fn list_calls(&self) -> u64 {
            *self.list_calls.lock().unwrap()
        }

        /// Total `get` calls made (rigged failures included).
        pub fn get_calls(&self) -> u64 {
            *self.get_calls.lock().unwrap()
        }

        /// Consume one rigged failure from `(remaining, kind)` if any
        /// is armed. Returns the error the caller should surface.
        fn consume_rigged(
            remaining: &Mutex<u32>,
            kind: &Mutex<RiggedFailureKind>,
            op: &str,
        ) -> Option<Error> {
            let mut rem = remaining.lock().unwrap();
            if *rem == 0 {
                return None;
            }
            *rem -= 1;
            Some(match *kind.lock().unwrap() {
                RiggedFailureKind::Transient => {
                    Error::Other(anyhow::anyhow!("rigged transient {op} failure"))
                }
                RiggedFailureKind::PreconditionFailed => Error::PreconditionFailed,
            })
        }
    }

    impl Default for FakeStore {
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait]
    impl ClaimStore for FakeStore {
        async fn put_if_absent(&self, key: &str, body: Vec<u8>) -> Result<String> {
            // Consume a rigged failure if one was configured.
            {
                let mut rem = self.put_failures_remaining.lock().unwrap();
                if *rem > 0 {
                    *rem -= 1;
                    let kind = *self.put_failure_kind.lock().unwrap();
                    self.log(OpKind::PutIfAbsent {
                        key: key.to_string(),
                        body,
                        ok: false,
                    });
                    return Err(match kind {
                        RiggedFailureKind::Transient => {
                            Error::Other(anyhow::anyhow!("rigged transient PUT failure"))
                        }
                        RiggedFailureKind::PreconditionFailed => Error::PreconditionFailed,
                    });
                }
            }
            let mut g = self.inner.lock().unwrap();
            if g.contains_key(key) {
                self.log(OpKind::PutIfAbsent {
                    key: key.to_string(),
                    body,
                    ok: false,
                });
                return Err(Error::PreconditionFailed);
            }
            let etag = self.next_etag();
            g.insert(key.to_string(), (body.clone(), etag.clone()));
            drop(g);
            self.log(OpKind::PutIfAbsent {
                key: key.to_string(),
                body,
                ok: true,
            });
            Ok(etag)
        }
        async fn put_unconditional(&self, key: &str, body: Vec<u8>) -> Result<String> {
            // Unlike the trait's default no-op, actually store the
            // object — worker integration tests need progress files
            // to be readable back through `get`.
            let etag = self.next_etag();
            self.inner
                .lock()
                .unwrap()
                .insert(key.to_string(), (body.clone(), etag.clone()));
            self.log(OpKind::PutUnconditional {
                key: key.to_string(),
                body,
            });
            Ok(etag)
        }
        async fn head_object(&self, key: &str) -> Result<Option<(String, Vec<u8>)>> {
            let g = self.inner.lock().unwrap();
            Ok(g.get(key).map(|(b, e)| (e.clone(), b.clone())))
        }
        async fn delete_if_match(&self, key: &str, etag: &str) -> Result<DeleteOutcome> {
            let mut g = self.inner.lock().unwrap();
            let (outcome, deleted_body) = match g.get(key) {
                Some((_, current)) if current == etag => {
                    let removed = g.remove(key).map(|(b, _)| b);
                    (DeleteOutcome::Deleted, removed)
                }
                Some(_) => (DeleteOutcome::EtagMismatch, None),
                None => (DeleteOutcome::NotFound, None),
            };
            drop(g);
            self.log(OpKind::DeleteIfMatch {
                key: key.to_string(),
                etag: etag.to_string(),
                outcome,
                deleted_body,
            });
            Ok(outcome)
        }
        async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
            *self.get_calls.lock().unwrap() += 1;
            if let Some(err) =
                Self::consume_rigged(&self.get_failures_remaining, &self.get_failure_kind, "GET")
            {
                return Err(err);
            }
            let g = self.inner.lock().unwrap();
            Ok(g.get(key).cloned())
        }
        async fn list(&self, prefix: &str) -> Result<Vec<ListEntry>> {
            *self.list_calls.lock().unwrap() += 1;
            if let Some(err) = Self::consume_rigged(
                &self.list_failures_remaining,
                &self.list_failure_kind,
                "LIST",
            ) {
                return Err(err);
            }
            let g = self.inner.lock().unwrap();
            let out = g
                .iter()
                .filter(|(k, _)| k.starts_with(prefix))
                .map(|(k, (b, e))| ListEntry {
                    key: k.clone(),
                    etag: e.clone(),
                    size: b.len() as u64,
                })
                .collect();
            Ok(out)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_util::{FakeStore, RiggedFailureKind};
    use super::*;
    use crate::layout;

    const SHARD: &str = "part-0042.parquet";

    // -------------------------------------------------------------------------
    // try_acquire / AcquireOutcome — minimal coverage retained for the
    // first-claim path that v2 inherits unchanged from v1.
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn acquire_succeeds_when_absent() {
        let s = FakeStore::new();
        match try_acquire(&s, SHARD, "host-A").await.unwrap() {
            AcquireOutcome::Acquired { record, .. } => {
                assert_eq!(record.host, "host-A");
                assert_eq!(record.epoch, 1);
                assert_eq!(record.state, ClaimState::Active);
            }
            other => panic!("expected Acquired, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn acquire_contended_when_present() {
        let s = FakeStore::new();
        let _ = try_acquire(&s, SHARD, "host-A").await.unwrap();
        match try_acquire(&s, SHARD, "host-B").await.unwrap() {
            AcquireOutcome::Contended { existing, .. } => {
                assert_eq!(existing.host, "host-A");
            }
            other => panic!("expected Contended, got {other:?}"),
        }
    }

    // -------------------------------------------------------------------------
    // v2 protocol scenarios — the four interleavings from
    // docs/work-items/CLAIM_PROTOCOL_V2_DELETE_THEN_CREATE.md §5.
    // -------------------------------------------------------------------------

    /// Scenario 1: fresh shard, two workers race for first-time claim.
    /// Exactly one wins via `PUT If-None-Match: *`; the other sees
    /// `Contended`.
    #[tokio::test]
    async fn test_v2_first_time_claim_race_two_workers() {
        let s = FakeStore::new();
        let r_a = try_acquire(&s, SHARD, "host-A").await.unwrap();
        let r_b = try_acquire(&s, SHARD, "host-B").await.unwrap();

        match (r_a, r_b) {
            (
                AcquireOutcome::Acquired { record: ra, .. },
                AcquireOutcome::Contended { existing, .. },
            ) => {
                assert_eq!(ra.host, "host-A");
                assert_eq!(existing.host, "host-A");
            }
            (a, b) => panic!("expected Acquired,Contended; got {a:?}, {b:?}"),
        }
    }

    /// Scenario 2: owner alive; the protocol exposes `claimed_utc` via
    /// HEAD so the caller can decide not to reclaim. Lease enforcement
    /// is caller policy; the protocol provides the read.
    #[tokio::test]
    async fn test_v2_owner_alive_third_party_reclaim_blocked() {
        let s = FakeStore::new();
        let _ = try_acquire(&s, SHARD, "host-A").await.unwrap();

        let key = layout::claim_key(SHARD);
        let (etag, body) = s.head_object(&key).await.unwrap().expect("present");
        assert!(!etag.is_empty(), "head_object must return an etag");
        let r: ClaimRecord = serde_json::from_slice(&body).unwrap();
        assert_eq!(r.host, "host-A");
        assert_eq!(r.state, ClaimState::Active);

        // Caller policy gate: lease still fresh → do not call reclaim.
        // We assert that the caller CAN make this decision because the
        // protocol returns a parseable claim body with a recent
        // claimed_utc field.
        let now = chrono::Utc::now();
        let age = now.signed_duration_since(r.claimed_utc.0).num_seconds();
        assert!(
            age < 60,
            "fresh claim should be within typical lease window (age={age}s)"
        );
    }

    /// Scenario 3: owner stalled, reclaimer wins via delete-then-create;
    /// owner's next `refresh` HEAD detects the etag change and returns
    /// `Lost` so the worker can self-fence.
    #[tokio::test]
    async fn test_v2_owner_stalled_reclaimer_wins_owner_detects_via_refresh() {
        let s = FakeStore::new();
        let AcquireOutcome::Acquired { etag: a_etag, .. } =
            try_acquire(&s, SHARD, "host-A").await.unwrap()
        else {
            panic!("acquire should succeed")
        };

        // host-B HEADs to observe the current claim (records its etag).
        let key = layout::claim_key(SHARD);
        let (observed_etag, _body) = s.head_object(&key).await.unwrap().expect("present");
        assert_eq!(observed_etag, a_etag);

        // host-B reclaims with the observed etag — wins the delete race
        // (A hasn't moved) and the create race (no concurrent reclaimer).
        let won = reclaim(&s, SHARD, &observed_etag, "host-B", 2)
            .await
            .unwrap();
        let new_etag = match won {
            ReclaimOutcome::Won { etag, record } => {
                assert_eq!(record.host, "host-B");
                assert_eq!(record.epoch, 2);
                etag
            }
            ReclaimOutcome::LostRace => panic!("expected Won"),
        };
        assert_ne!(new_etag, a_etag, "reclaim should mint a new etag");

        // host-A's heartbeat tick HEADs the claim and discovers the
        // etag has changed — must report Lost to trip the fence.
        match refresh(&s, SHARD, &a_etag).await.unwrap() {
            RefreshOutcome::Lost => {}
            RefreshOutcome::StillHeld { .. } => {
                panic!("owner with stale etag must observe Lost, not StillHeld");
            }
        }

        // host-B's own HEAD-and-compare confirms it is the owner.
        match refresh(&s, SHARD, &new_etag).await.unwrap() {
            RefreshOutcome::StillHeld { etag } => assert_eq!(etag, new_etag),
            RefreshOutcome::Lost => panic!("new owner must observe StillHeld"),
        }
    }

    /// Scenario 4: two reclaimers race against each other. Both HEAD
    /// the same `(etag, body)`, both attempt the delete-then-create
    /// sequence. At most one DELETE wins; the loser's PUT may fall
    /// through to `LostRace` either at the delete step (if interleaved
    /// after the winner's DELETE+PUT — the loser sees EtagMismatch
    /// because winner's PUT minted a new etag) or at the create step
    /// (if interleaved between the winner's DELETE and PUT — the
    /// loser's DELETE returns NotFound, which we also map to
    /// LostRace).
    #[tokio::test]
    async fn test_v2_two_reclaimers_race_only_one_wins() {
        let s = FakeStore::new();
        let _ = try_acquire(&s, SHARD, "host-A").await.unwrap();

        let key = layout::claim_key(SHARD);
        let (observed_etag, _) = s.head_object(&key).await.unwrap().expect("present");

        // host-B reclaims first (full sequence: DELETE then PUT).
        let r_b = reclaim(&s, SHARD, &observed_etag, "host-B", 2)
            .await
            .unwrap();
        // host-C tries to reclaim against the same observed etag from
        // BEFORE host-B's reclaim. host-C's DELETE will see a different
        // etag (host-B's new one) → EtagMismatch → LostRace.
        let r_c = reclaim(&s, SHARD, &observed_etag, "host-C", 3)
            .await
            .unwrap();

        match (r_b, r_c) {
            (ReclaimOutcome::Won { record, .. }, ReclaimOutcome::LostRace) => {
                assert_eq!(record.host, "host-B");
            }
            (b, c) => panic!("expected Won,LostRace; got {b:?}, {c:?}"),
        }

        // Sanity: head still shows host-B as the owner.
        let (_, body) = s.head_object(&key).await.unwrap().expect("present");
        let r: ClaimRecord = serde_json::from_slice(&body).unwrap();
        assert_eq!(r.host, "host-B");

        // Subvariant: a fourth worker HEADs after host-B's win and
        // races against an in-flight reclaim during the interregnum.
        // We simulate "DELETE succeeded but PUT not yet issued" by
        // deleting the object directly and then having two reclaimers
        // both attempt PUT-If-None-Match.
        let (b_etag, _) = s.head_object(&key).await.unwrap().expect("present");
        assert_eq!(
            s.delete_if_match(&key, &b_etag).await.unwrap(),
            DeleteOutcome::Deleted
        );
        // Now key is absent. host-D's reclaim with a fabricated etag
        // hits NotFound at DELETE step → LostRace; this matches §3.3
        // "DELETE NotFound → LostRace; restart from HEAD".
        let r_d = reclaim(&s, SHARD, "etag-stale", "host-D", 4).await.unwrap();
        assert!(
            matches!(r_d, ReclaimOutcome::LostRace),
            "DELETE on absent key → NotFound → LostRace"
        );
        // host-E does the right thing: HEAD returns absent, so it goes
        // through `try_acquire` instead, which uses PUT If-None-Match.
        match try_acquire(&s, SHARD, "host-E").await.unwrap() {
            AcquireOutcome::Acquired { record, .. } => {
                assert_eq!(record.host, "host-E");
                assert_eq!(record.epoch, 1);
            }
            other => panic!("expected Acquired, got {other:?}"),
        }
    }

    // -------------------------------------------------------------------------
    // Targeted tests for the new primitives + complete().
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn refresh_still_held_with_correct_etag() {
        let s = FakeStore::new();
        let AcquireOutcome::Acquired { etag, .. } = try_acquire(&s, SHARD, "host-A").await.unwrap()
        else {
            panic!()
        };

        match refresh(&s, SHARD, &etag).await.unwrap() {
            RefreshOutcome::StillHeld { etag: returned } => assert_eq!(returned, etag),
            RefreshOutcome::Lost => panic!("expected StillHeld"),
        }
    }

    #[tokio::test]
    async fn refresh_lost_when_object_absent() {
        let s = FakeStore::new();
        match refresh(&s, SHARD, "etag-doesnt-matter").await.unwrap() {
            RefreshOutcome::Lost => {}
            other => panic!("expected Lost on absent claim, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn complete_writes_completed_state() {
        let s = FakeStore::new();
        let AcquireOutcome::Acquired { etag, .. } = try_acquire(&s, SHARD, "host-A").await.unwrap()
        else {
            panic!()
        };

        match complete(&s, SHARD, &etag, "host-A", 5).await.unwrap() {
            CompleteOutcome::Completed {
                etag: completed_etag,
            } => {
                assert!(!completed_etag.is_empty());
                assert_ne!(completed_etag, etag, "complete mints a new etag");
            }
            CompleteOutcome::Lost => panic!("expected Completed"),
        }

        // The terminal-state record is what HEAD returns now.
        let key = layout::claim_key(SHARD);
        let (_, body) = s.head_object(&key).await.unwrap().expect("present");
        let r: ClaimRecord = serde_json::from_slice(&body).unwrap();
        assert_eq!(r.state, ClaimState::Completed);
        assert_eq!(r.epoch, 5);
        assert_eq!(r.host, "host-A");
    }

    #[tokio::test]
    async fn complete_lost_when_someone_reclaimed() {
        let s = FakeStore::new();
        let AcquireOutcome::Acquired { etag: a_etag, .. } =
            try_acquire(&s, SHARD, "host-A").await.unwrap()
        else {
            panic!()
        };

        // host-B reclaims while A was working.
        let _ = reclaim(&s, SHARD, &a_etag, "host-B", 2).await.unwrap();

        // host-A's complete() must observe Lost rather than overwriting.
        match complete(&s, SHARD, &a_etag, "host-A", 7).await.unwrap() {
            CompleteOutcome::Lost => {}
            other => panic!("expected Lost, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fail_writes_failed_state() {
        let s = FakeStore::new();
        let AcquireOutcome::Acquired { etag, .. } = try_acquire(&s, SHARD, "host-A").await.unwrap()
        else {
            panic!()
        };

        match fail(&s, SHARD, &etag, "host-A", 3).await.unwrap() {
            FailOutcome::Failed { etag: failed_etag } => {
                assert!(!failed_etag.is_empty());
                assert_ne!(failed_etag, etag, "fail mints a new etag");
            }
            FailOutcome::Lost => panic!("expected Failed"),
        }

        // The terminal-state record is what HEAD returns now.
        let key = layout::claim_key(SHARD);
        let (_, body) = s.head_object(&key).await.unwrap().expect("present");
        let r: ClaimRecord = serde_json::from_slice(&body).unwrap();
        assert_eq!(r.state, ClaimState::Failed);
        assert_eq!(r.epoch, 3);
        assert_eq!(r.host, "host-A");
    }

    #[tokio::test]
    async fn fail_lost_when_someone_reclaimed() {
        let s = FakeStore::new();
        let AcquireOutcome::Acquired { etag: a_etag, .. } =
            try_acquire(&s, SHARD, "host-A").await.unwrap()
        else {
            panic!()
        };

        // host-B reclaims while A was working.
        let _ = reclaim(&s, SHARD, &a_etag, "host-B", 2).await.unwrap();

        // host-A's fail() must observe Lost rather than overwriting.
        match fail(&s, SHARD, &a_etag, "host-A", 7).await.unwrap() {
            FailOutcome::Lost => {}
            other => panic!("expected Lost, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fail_state_is_terminal_for_scan() {
        // Regression guard: a Failed claim must HEAD back as Failed —
        // scan_shards keys off ClaimState to skip terminal shards.
        // If `fail` ever drifted to writing Active or Completed,
        // scanners would treat the shard as live or done; either is
        // wrong.
        let s = FakeStore::new();
        let AcquireOutcome::Acquired { etag, .. } = try_acquire(&s, SHARD, "host-A").await.unwrap()
        else {
            panic!()
        };
        let _ = fail(&s, SHARD, &etag, "host-A", 1).await.unwrap();

        let key = layout::claim_key(SHARD);
        let (_, body) = s.head_object(&key).await.unwrap().expect("present");
        let r: ClaimRecord = serde_json::from_slice(&body).unwrap();
        assert_eq!(r.state, ClaimState::Failed);
        assert_ne!(r.state, ClaimState::Active);
        assert_ne!(r.state, ClaimState::Completed);
    }

    // -------------------------------------------------------------------------
    // Terminal-state PUT retry (B1).
    //
    // The retry path lives inside `complete` and `fail`. It must:
    //  - Retry on transient errors (anything that isn't PreconditionFailed).
    //  - Return Lost immediately on PreconditionFailed without retrying —
    //    that's a real "fresh try_acquire won the absent window" signal,
    //    not a transient error.
    //  - Surface the last transient error if all attempts exhaust.
    // -------------------------------------------------------------------------

    /// Override the constant so tests don't actually sleep multiple
    /// seconds per attempt. We can't shadow `TERMINAL_PUT_BACKOFF_BASE_MS`
    /// at call time, but tokio's mock-time pause/advance suffices.
    use tokio::time::advance;

    #[tokio::test(start_paused = true)]
    async fn complete_retries_on_transient_then_succeeds() {
        // Two transient PUT failures, then success. With
        // TERMINAL_PUT_ATTEMPTS=3 we have one attempt to spare.
        // `start_paused = true` makes tokio auto-advance through
        // `tokio::time::sleep` calls when nothing else is pending,
        // so the test doesn't actually wait the backoff durations.
        let s = FakeStore::new();
        let AcquireOutcome::Acquired { etag, .. } = try_acquire(&s, SHARD, "host-A").await.unwrap()
        else {
            panic!()
        };
        s.rig_next_puts_to_fail(2, RiggedFailureKind::Transient);

        let outcome = complete(&s, SHARD, &etag, "host-A", 5).await.unwrap();
        match outcome {
            CompleteOutcome::Completed { etag: e } => assert!(!e.is_empty()),
            CompleteOutcome::Lost => panic!("expected Completed after retry"),
        }
        assert_eq!(
            s.rigged_remaining(),
            0,
            "all rigged failures should have been consumed"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn complete_does_not_retry_on_precondition_failed() {
        let s = FakeStore::new();
        let AcquireOutcome::Acquired { etag, .. } = try_acquire(&s, SHARD, "host-A").await.unwrap()
        else {
            panic!()
        };
        // Rig 3 PreconditionFailed responses. The retry helper must
        // return on the FIRST one without consuming the others.
        s.rig_next_puts_to_fail(3, RiggedFailureKind::PreconditionFailed);

        let outcome = complete(&s, SHARD, &etag, "host-A", 5).await.unwrap();
        match outcome {
            CompleteOutcome::Lost => {}
            other => panic!("expected Lost on PreconditionFailed, got {other:?}"),
        }
        assert_eq!(
            s.rigged_remaining(),
            2,
            "PreconditionFailed must NOT trigger retry; should have consumed exactly 1 of 3 rigged failures",
        );
        // Advance mock time to confirm no extra sleeps were scheduled.
        advance(Duration::from_secs(60)).await;
    }

    #[tokio::test(start_paused = true)]
    async fn complete_returns_last_error_after_exhausting_attempts() {
        let s = FakeStore::new();
        let AcquireOutcome::Acquired { etag, .. } = try_acquire(&s, SHARD, "host-A").await.unwrap()
        else {
            panic!()
        };
        // Rig MORE failures than TERMINAL_PUT_ATTEMPTS — every attempt
        // hits a transient and the helper finally surfaces the error.
        s.rig_next_puts_to_fail(TERMINAL_PUT_ATTEMPTS + 5, RiggedFailureKind::Transient);

        let err = complete(&s, SHARD, &etag, "host-A", 5).await.unwrap_err();
        match err {
            Error::Other(_) => {} // simulated transient
            other => panic!("expected Error::Other (transient), got {other:?}"),
        }
        // Exactly TERMINAL_PUT_ATTEMPTS rigged failures should have
        // been consumed; the remaining (5) are still queued.
        assert_eq!(s.rigged_remaining(), 5);
    }

    #[tokio::test(start_paused = true)]
    async fn fail_uses_same_retry_path() {
        // Sanity: fail() goes through put_if_absent_with_retry just
        // like complete(). One transient failure → second attempt
        // succeeds → Failed state landed.
        let s = FakeStore::new();
        let AcquireOutcome::Acquired { etag, .. } = try_acquire(&s, SHARD, "host-A").await.unwrap()
        else {
            panic!()
        };
        s.rig_next_puts_to_fail(1, RiggedFailureKind::Transient);

        let outcome = fail(&s, SHARD, &etag, "host-A", 2).await.unwrap();
        match outcome {
            FailOutcome::Failed { .. } => {}
            FailOutcome::Lost => panic!("expected Failed after retry"),
        }
        assert_eq!(s.rigged_remaining(), 0);

        let key = layout::claim_key(SHARD);
        let (_, body) = s.head_object(&key).await.unwrap().expect("present");
        let r: ClaimRecord = serde_json::from_slice(&body).unwrap();
        assert_eq!(r.state, ClaimState::Failed);
    }

    #[tokio::test]
    async fn delete_if_match_outcomes() {
        let s = FakeStore::new();
        let key = layout::claim_key(SHARD);

        // NotFound on absent.
        assert_eq!(
            s.delete_if_match(&key, "any").await.unwrap(),
            DeleteOutcome::NotFound
        );

        let _ = try_acquire(&s, SHARD, "host-A").await.unwrap();
        let (current_etag, _) = s.head_object(&key).await.unwrap().expect("present");

        // EtagMismatch when wrong etag.
        assert_eq!(
            s.delete_if_match(&key, "wrong-etag").await.unwrap(),
            DeleteOutcome::EtagMismatch
        );
        // Object still present after a mismatched delete.
        assert!(s.head_object(&key).await.unwrap().is_some());

        // Deleted when correct.
        assert_eq!(
            s.delete_if_match(&key, &current_etag).await.unwrap(),
            DeleteOutcome::Deleted
        );
        assert!(s.head_object(&key).await.unwrap().is_none());
    }
}
