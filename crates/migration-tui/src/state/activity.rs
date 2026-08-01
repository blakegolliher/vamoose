use chrono::{DateTime, Duration as ChronoDuration, Utc};
use migration_control_protocol::schema::{ErrorClass, WorkerId};
use std::collections::VecDeque;

// =============================================================================
// Per-job recent-errors tail
// =============================================================================

/// How many recent errors per job the TUI keeps. The coord's
/// [`migration_control_protocol::schema::ErrorBucket`] aggregation captures totals + sample paths, but
/// the Errors tab needs a chronological tail for the operator to
/// scan — and the tail can't be reconstructed from the bucket
/// (`sample_paths` is unordered, deduped, and capped). So the
/// client maintains its own.
pub const RECENT_ERRORS_PER_JOB: usize = 50;

/// One captured `ErrorEmitted` event, materialized client-side so
/// the Errors tab's recent tail has the full payload without
/// re-querying the coord.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecentError {
    pub at: DateTime<Utc>,
    pub worker_id: WorkerId,
    pub class: ErrorClass,
    pub path: String,
    pub retryable: bool,
    pub message: String,
}
/// Bounded ring of [`RecentError`]s, oldest first. Pushes that
/// would exceed [`RECENT_ERRORS_PER_JOB`] drop the head.
#[derive(Debug, Clone, Default)]
pub struct RecentErrors {
    entries: VecDeque<RecentError>,
}

/// One captured `VerifyFileMismatch` event. The coord doesn't keep
/// these on the snapshot (Phase 1 reducer just streams them) — the
/// TUI keeps a chronological ring so the Verify tab can show the
/// recent picture.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecentVerifyMismatch {
    pub at: DateTime<Utc>,
    pub path: String,
    pub expected: String,
    pub got: String,
}

/// Per-job verify mismatches ring. Same bounded shape as
/// [`RecentErrors`]; oldest pushed out at the cap.
pub const RECENT_VERIFY_MISMATCHES_PER_JOB: usize = 50;

#[derive(Debug, Clone, Default)]
pub struct RecentVerifyMismatches {
    entries: VecDeque<RecentVerifyMismatch>,
}

impl RecentVerifyMismatches {
    pub fn push(&mut self, e: RecentVerifyMismatch) {
        if self.entries.len() >= RECENT_VERIFY_MISMATCHES_PER_JOB {
            self.entries.pop_front();
        }
        self.entries.push_back(e);
    }
    pub fn tail(&self, n: usize) -> impl Iterator<Item = &RecentVerifyMismatch> {
        let skip = self.entries.len().saturating_sub(n);
        self.entries.iter().skip(skip)
    }
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Last observed verify lifecycle for a job. The coord drives the
/// phase transition via `VerifyCompleted`'s mismatch count but
/// doesn't keep the timestamps + counts on the snapshot — those
/// are captured here so the Verify tab can show "started Xs ago,
/// completed with N mismatches".
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VerifyStatus {
    pub last_started: Option<DateTime<Utc>>,
    pub last_completed: Option<DateTime<Utc>>,
    pub last_mismatches: Option<u64>,
}

impl RecentErrors {
    pub fn push(&mut self, e: RecentError) {
        if self.entries.len() >= RECENT_ERRORS_PER_JOB {
            self.entries.pop_front();
        }
        self.entries.push_back(e);
    }

    /// Tail of `n` most-recent entries, newest LAST (matches the
    /// internal order so callers can iterate normally and have the
    /// most recent at the bottom of the table).
    pub fn tail(&self, n: usize) -> impl Iterator<Item = &RecentError> {
        let skip = self.entries.len().saturating_sub(n);
        self.entries.iter().skip(skip)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

// =============================================================================
// Rolling per-job throughput windows
// =============================================================================

/// Longest window the TUI tracks. Samples older than this are
/// pruned on every push so the buffer stays bounded.
pub(super) const MAX_WINDOW_SECS: i64 = 5 * 60;

/// Per-job rolling samples of `ProgressDelta` events for client-
/// side throughput estimation. The TUI computes rolling 1s / 1m /
/// 5m windows from this rather than having the coord ship a wider
/// per-tick payload.
#[derive(Debug, Clone, Default)]
pub struct ProgressDeltaHistory {
    /// Bounded ring of `(wall_clock, bytes_delta)`. Sorted oldest-
    /// first so pruning is a single `pop_front` per stale entry.
    samples: VecDeque<(DateTime<Utc>, u64)>,
}

impl ProgressDeltaHistory {
    /// Record one ProgressDelta. Prunes anything older than the
    /// longest window the TUI maintains.
    pub fn push(&mut self, at: DateTime<Utc>, bytes_delta: u64) {
        let cutoff = at - ChronoDuration::seconds(MAX_WINDOW_SECS);
        while let Some(&(t, _)) = self.samples.front() {
            if t < cutoff {
                self.samples.pop_front();
            } else {
                break;
            }
        }
        self.samples.push_back((at, bytes_delta));
    }

    /// Bytes-per-second over the most recent `window_secs`. Returns
    /// 0.0 when the window is empty (no samples yet, or all pruned
    /// because the worker stopped emitting).
    pub fn bytes_per_sec(&self, window_secs: i64, now: DateTime<Utc>) -> f64 {
        if window_secs <= 0 {
            return 0.0;
        }
        let cutoff = now - ChronoDuration::seconds(window_secs);
        let total: u64 = self
            .samples
            .iter()
            .filter(|(t, _)| *t >= cutoff)
            .map(|(_, b)| *b)
            .sum();
        total as f64 / window_secs as f64
    }

    /// Sample count (mostly for tests and diagnostics).
    pub fn len(&self) -> usize {
        self.samples.len()
    }

    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }
}
