//! Self-fencing.
//!
//! The single most important correctness primitive in the worker. See
//! DESIGN.md "S3 claim protocol and worker lifecycle" — the goal is to prevent dual-writer
//! corruption when this worker has been declared dead by the rest of
//! the fleet but is itself still alive and copying.
//!
//! ## Invariant
//!
//! At any instant, the worker writing to dest paths derived from rows
//! in shard X must hold a valid claim on X.
//!
//! ## Mechanism
//!
//! - The mover loop checks `claim_valid()` before issuing each new
//!   file's RENAME (the commit point).
//! - The heartbeat task sets the flag to `false` on:
//!     - HTTP 412 on heartbeat refresh (lost claim).
//!     - HTTP 5xx repeated past retry budget.
//!     - Local clock jump > LEASE_TIMEOUT/2.
//! - When `claim_valid()` returns false, the mover cancels in-flight
//!   work, **does not RENAME**, and exits the shard.
//!
//! ## Why this lives in `migration-core`
//!
//! `Fence` is a primitive shared between the worker (`migration-worker`)
//! — which trips it from the heartbeat task and consults it between
//! rows — and the mover (`migration-mover`) — which consults it right
//! before each commit-point op per R8 in `docs/CLAIM_PROTOCOL.md`.
//! Putting it in `migration-core` avoids a `migration-mover ->
//! migration-worker` dependency edge that would close a cycle.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Why a fence tripped. Decides what the worker process does after
/// the orderly fenced shutdown: a claim that was actually taken from
/// under us (or a clock we cannot trust) needs an operator, while a
/// store that merely went away for a full lease window is a network
/// blip the supervisor should simply restart through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FenceCause {
    /// The claim object changed under us — a peer reclaimed the shard.
    OwnershipLost,
    /// The claim could not be confirmed for a full lease window
    /// because the store was unreachable (DNS, connect timeouts, 5xx
    /// storms). Ownership was surrendered defensively, not lost.
    StoreUnreachable,
    /// Wall clock jumped against the monotonic clock by more than
    /// half a lease; lease arithmetic can no longer be trusted.
    ClockJump,
}

#[derive(Clone)]
pub struct Fence {
    valid: Arc<AtomicBool>,
    cancel: CancellationToken,
    reason: Arc<std::sync::Mutex<Option<(FenceCause, String)>>>,
}

impl Fence {
    pub fn new() -> Self {
        Self {
            valid: Arc::new(AtomicBool::new(true)),
            cancel: CancellationToken::new(),
            reason: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Cheap check before issuing each new file's commit.
    pub fn is_valid(&self) -> bool {
        self.valid.load(Ordering::Acquire)
    }

    /// Trip the fence for a lost claim. Idempotent — first caller
    /// wins on `reason`. Shorthand for
    /// [`trip_with_cause`](Self::trip_with_cause) with
    /// [`FenceCause::OwnershipLost`].
    pub fn trip(&self, reason: impl Into<String>) {
        self.trip_with_cause(FenceCause::OwnershipLost, reason);
    }

    /// Trip the fence, recording why. Idempotent — the first trip
    /// keeps its cause and reason.
    pub fn trip_with_cause(&self, cause: FenceCause, reason: impl Into<String>) {
        let r = reason.into();
        if self.valid.swap(false, Ordering::AcqRel) {
            tracing::warn!(reason = %r, ?cause, "fence tripped; self-fencing worker");
            *self.reason.lock().unwrap() = Some((cause, r));
            self.cancel.cancel();
        }
    }

    /// Close the fence as part of an orderly shutdown. Same effect as
    /// [`trip`](Self::trip) — the heartbeat wakes and writes its final
    /// progress record, movers stop committing — but logged at `info`:
    /// nothing was lost and no peer took anything, so the
    /// "self-fencing" WARN would send an operator hunting for a
    /// problem that does not exist. Idempotent; a real trip that
    /// happened first keeps its reason.
    pub fn close_for_shutdown(&self) {
        if self.valid.swap(false, Ordering::AcqRel) {
            tracing::info!("worker shutting down; fence closed");
            *self.reason.lock().unwrap() = Some((
                FenceCause::OwnershipLost,
                "worker shutting down".to_string(),
            ));
            self.cancel.cancel();
        }
    }

    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    pub fn reason(&self) -> Option<String> {
        self.reason.lock().unwrap().as_ref().map(|(_, r)| r.clone())
    }

    /// Why the fence tripped, if it has. `None` while the fence is
    /// still valid.
    pub fn cause(&self) -> Option<FenceCause> {
        self.reason.lock().unwrap().as_ref().map(|(c, _)| *c)
    }
}

impl Default for Fence {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::{Fence, FenceCause};

    #[test]
    fn close_for_shutdown_invalidates_and_cancels() {
        let f = Fence::new();
        assert!(f.is_valid());
        f.close_for_shutdown();
        assert!(!f.is_valid());
        assert!(f.cancel_token().is_cancelled());
        assert_eq!(f.reason().as_deref(), Some("worker shutting down"));
    }

    /// A real trip that happened first is not relabelled by the
    /// shutdown close — the operator must still see why the worker
    /// fenced.
    #[test]
    fn shutdown_close_keeps_an_earlier_trip_reason() {
        let f = Fence::new();
        f.trip("claim lost");
        f.close_for_shutdown();
        assert_eq!(f.reason().as_deref(), Some("claim lost"));
    }

    /// The cause travels with the reason: a store-unreachable trip
    /// is reported as such, and a later trip (or shutdown close)
    /// does not relabel it.
    #[test]
    fn first_trip_keeps_its_cause() {
        let f = Fence::new();
        assert_eq!(f.cause(), None);
        f.trip_with_cause(FenceCause::StoreUnreachable, "HEAD failing");
        f.trip("claim lost");
        f.close_for_shutdown();
        assert_eq!(f.cause(), Some(FenceCause::StoreUnreachable));
        assert_eq!(f.reason().as_deref(), Some("HEAD failing"));
        // Plain `trip` is a lost claim.
        let g = Fence::new();
        g.trip("etag changed");
        assert_eq!(g.cause(), Some(FenceCause::OwnershipLost));
    }
}
