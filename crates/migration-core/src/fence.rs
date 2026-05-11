//! Self-fencing.
//!
//! The single most important correctness primitive in the worker. See
//! DESIGN.md "Self-fencing" — the goal is to prevent dual-writer
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

#[derive(Clone)]
pub struct Fence {
    valid: Arc<AtomicBool>,
    cancel: CancellationToken,
    reason: Arc<std::sync::Mutex<Option<String>>>,
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

    /// Trip the fence. Idempotent — first caller wins on `reason`.
    pub fn trip(&self, reason: impl Into<String>) {
        let r = reason.into();
        if self.valid.swap(false, Ordering::AcqRel) {
            tracing::warn!(reason = %r, "fence tripped; self-fencing worker");
            *self.reason.lock().unwrap() = Some(r);
            self.cancel.cancel();
        }
    }

    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    pub fn reason(&self) -> Option<String> {
        self.reason.lock().unwrap().clone()
    }
}

impl Default for Fence {
    fn default() -> Self {
        Self::new()
    }
}
