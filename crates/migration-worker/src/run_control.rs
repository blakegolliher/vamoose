//! Worker-internal coord-driven run mode.
//!
//! The coord publishes a [`ControlMode`] on every heartbeat response.
//! The worker's heartbeat tick flips a `RunControl` to match; the
//! orchestrator's claim loop and the shard processor consult their
//! [`RunControlReader`] before starting any new unit of work.
//!
//! The R-rules forbid mid-commit interrupts — the loop never aborts
//! an in-flight shard. The four modes only gate **what happens next**:
//!
//! - `Run` — claim the next shard.
//! - `Pause` — block until the mode leaves `Pause`. Operator can
//!   resume (back to `Run`) or escalate (`Drain`/`Cancel`). No data
//!   is in flight while paused.
//! - `Drain` — finish whatever the worker is already doing, then
//!   exit cleanly. No new shards claimed.
//! - `Cancel` — same shape as `Drain` from the orchestrator's
//!   perspective (in-flight finishes, no new claims). The label
//!   propagates through audit so the operator can tell the
//!   difference.
//!
//! Implementation is a thin wrapper around `tokio::sync::watch` so
//! `Pause → Run` wake-ups are O(1) rather than a busy-poll. The
//! struct is `Arc`-shaped so the heartbeat task (writer) and the
//! orchestrator (reader) can hold cheap clones.

use migration_control_protocol::schema::ControlMode;
use std::sync::Arc;
use tokio::sync::watch;

#[derive(Debug, Clone)]
pub struct RunControl(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    tx: watch::Sender<ControlMode>,
}

impl RunControl {
    /// Build a new RunControl in `Run` mode.
    pub fn new() -> Self {
        let (tx, _) = watch::channel(ControlMode::Run);
        Self(Arc::new(Inner { tx }))
    }

    /// Build a RunControl seeded with a specific mode. Convenience
    /// for tests that want to start paused/cancelled.
    pub fn with_mode(mode: ControlMode) -> Self {
        let (tx, _) = watch::channel(mode);
        Self(Arc::new(Inner { tx }))
    }

    /// Current mode. Lock-free (a watch borrow).
    pub fn mode(&self) -> ControlMode {
        *self.0.tx.borrow()
    }

    /// Update the mode. Skips the watch send when the new mode
    /// equals the current one, so a heartbeat-driven `set(Run)`
    /// repeated every tick does NOT wake idle subscribers.
    ///
    /// Single-writer expected (the heartbeat task). Concurrent
    /// writers would race on the "is it the same?" check but the
    /// underlying watch is still consistent — the worst case is a
    /// spurious wake, never a missed update.
    pub fn set(&self, mode: ControlMode) {
        if *self.0.tx.borrow() != mode {
            self.0.tx.send_replace(mode);
        }
    }

    /// Subscribe for change notifications. Each call gives the
    /// caller its own `RunControlReader` with an independent
    /// `changed()` cursor. Cheap (just clones a watch::Receiver).
    pub fn subscribe(&self) -> RunControlReader {
        RunControlReader {
            rx: self.0.tx.subscribe(),
        }
    }

    /// `true` for `Drain` or `Cancel` — the orchestrator stops
    /// claiming new shards but lets in-flight work finish.
    pub fn is_terminating(&self) -> bool {
        matches!(self.mode(), ControlMode::Drain | ControlMode::Cancel)
    }

    /// `true` for `Pause`.
    pub fn is_paused(&self) -> bool {
        matches!(self.mode(), ControlMode::Pause)
    }
}

impl Default for RunControl {
    fn default() -> Self {
        Self::new()
    }
}

/// Read-side handle. Independent `changed()` cursor per instance.
#[derive(Debug, Clone)]
pub struct RunControlReader {
    rx: watch::Receiver<ControlMode>,
}

impl RunControlReader {
    pub fn mode(&self) -> ControlMode {
        *self.rx.borrow()
    }

    pub fn is_terminating(&self) -> bool {
        matches!(self.mode(), ControlMode::Drain | ControlMode::Cancel)
    }

    pub fn is_paused(&self) -> bool {
        matches!(self.mode(), ControlMode::Pause)
    }

    /// Block until the mode is no longer `Pause`. Returns the mode
    /// the reader observed when it woke. If the [`RunControl`]
    /// `Sender` is dropped while this is waiting, returns
    /// `ControlMode::Cancel` — the writer going away during a pause
    /// is functionally the same as the operator telling us to stop.
    ///
    /// Safe to call when already not paused — returns the current
    /// mode immediately without awaiting.
    pub async fn wait_while_paused(&mut self) -> ControlMode {
        loop {
            let cur = *self.rx.borrow_and_update();
            if cur != ControlMode::Pause {
                return cur;
            }
            if self.rx.changed().await.is_err() {
                return ControlMode::Cancel;
            }
        }
    }

    /// Block until the mode CHANGES from whatever the reader last
    /// observed. Returns the new mode, or `Cancel` if the sender
    /// dropped. Useful for tests; not used by the claim loop.
    pub async fn changed(&mut self) -> ControlMode {
        if self.rx.changed().await.is_err() {
            return ControlMode::Cancel;
        }
        *self.rx.borrow()
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn new_starts_in_run_mode() {
        let rc = RunControl::new();
        assert_eq!(rc.mode(), ControlMode::Run);
        assert!(!rc.is_terminating());
        assert!(!rc.is_paused());
    }

    #[test]
    fn with_mode_seeds_initial_state() {
        let rc = RunControl::with_mode(ControlMode::Pause);
        assert_eq!(rc.mode(), ControlMode::Pause);
        assert!(rc.is_paused());
    }

    #[test]
    fn set_propagates_to_a_subscriber() {
        let rc = RunControl::new();
        let reader = rc.subscribe();
        rc.set(ControlMode::Pause);
        assert_eq!(reader.mode(), ControlMode::Pause);
        assert!(reader.is_paused());
        rc.set(ControlMode::Cancel);
        assert_eq!(reader.mode(), ControlMode::Cancel);
        assert!(reader.is_terminating());
    }

    #[tokio::test]
    async fn wait_while_paused_returns_immediately_when_not_paused() {
        let rc = RunControl::new();
        let mut reader = rc.subscribe();
        // Run → no wait.
        let mode = tokio::time::timeout(Duration::from_millis(50), reader.wait_while_paused())
            .await
            .expect("must not block");
        assert_eq!(mode, ControlMode::Run);
    }

    #[tokio::test]
    async fn wait_while_paused_unblocks_when_writer_resumes() {
        let rc = RunControl::with_mode(ControlMode::Pause);
        let mut reader = rc.subscribe();

        let rc_writer = rc.clone();
        let writer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            rc_writer.set(ControlMode::Run);
        });

        let mode = tokio::time::timeout(Duration::from_secs(1), reader.wait_while_paused())
            .await
            .expect("unblock");
        assert_eq!(mode, ControlMode::Run);
        writer.await.unwrap();
    }

    #[tokio::test]
    async fn wait_while_paused_returns_cancel_if_sender_dropped() {
        let rc = RunControl::with_mode(ControlMode::Pause);
        let mut reader = rc.subscribe();

        let waiter = tokio::spawn(async move { reader.wait_while_paused().await });
        // Drop the writer while the reader is parked.
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(rc);

        let mode = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("future joined")
            .expect("task ok");
        assert_eq!(mode, ControlMode::Cancel);
    }

    #[tokio::test]
    async fn idempotent_set_does_not_wake_subscribers() {
        // set(Run) → set(Run) → set(Run) should NOT trigger
        // `changed()` on a subscriber, because the value never moved.
        let rc = RunControl::new();
        let mut reader = rc.subscribe();

        // Spurious set with same value.
        rc.set(ControlMode::Run);
        rc.set(ControlMode::Run);

        // changed() must NOT fire within a short timeout — the
        // value did not actually change.
        let res = tokio::time::timeout(Duration::from_millis(50), reader.changed()).await;
        assert!(res.is_err(), "changed() must not fire on same-value set");
    }

    #[tokio::test]
    async fn changed_fires_on_distinct_set() {
        let rc = RunControl::new();
        let mut reader = rc.subscribe();
        let rc_writer = rc.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            rc_writer.set(ControlMode::Pause);
        });
        let mode = tokio::time::timeout(Duration::from_secs(1), reader.changed())
            .await
            .expect("must fire");
        assert_eq!(mode, ControlMode::Pause);
    }

    #[test]
    fn is_terminating_classifies_only_drain_and_cancel() {
        assert!(!RunControl::with_mode(ControlMode::Run).is_terminating());
        assert!(!RunControl::with_mode(ControlMode::Pause).is_terminating());
        assert!(RunControl::with_mode(ControlMode::Drain).is_terminating());
        assert!(RunControl::with_mode(ControlMode::Cancel).is_terminating());
    }

    #[test]
    fn multiple_subscribers_observe_same_mode() {
        let rc = RunControl::new();
        let r1 = rc.subscribe();
        let r2 = rc.subscribe();
        rc.set(ControlMode::Drain);
        assert_eq!(r1.mode(), ControlMode::Drain);
        assert_eq!(r2.mode(), ControlMode::Drain);
        assert!(r1.is_terminating());
        assert!(r2.is_terminating());
    }
}
