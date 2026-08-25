//! Backpressure gate.
//!
//! Per DESIGN.md "Batching and backpressure": when the worker's recent failure
//! rate exceeds a threshold OR sustained throughput drops below a
//! floor, stop claiming new shards. The current shard finishes; new
//! claims wait. The orchestrator checks `degraded()` before each scan
//! pass and sleeps if degraded so we don't pile every host onto a
//! struggling dest.
//!
//! The M3 implementation tracks the last completed shard. Sliding
//! per-row windows are overkill at this evaluation cadence
//! (shard-completion = minutes); the previous shard is the right
//! signal for "are we drowning right now".
//!
//! Recovery (F14): degradation is not a one-way trap. Inputs only
//! change when a shard completes, but `degraded()` blocks claiming
//! shards — so without an exit path a tripped worker idles forever.
//! After a cooldown the gate hands out a single *probe* token: the
//! orchestrator claims exactly one shard, processes it, and feeds the
//! outcome back through `update()`. A healthy outcome clears
//! degradation; an unhealthy one re-degrades with the cooldown
//! doubled (capped). Timestamps come from `tokio::time::Instant`, so
//! paused-time tests control the clock; no `SystemTime::now()`.

use std::time::Duration;
use tokio::time::Instant;

/// Initial wait after tripping degradation before the gate allows a
/// single probe claim.
pub const PROBE_COOLDOWN_INITIAL: Duration = Duration::from_secs(5 * 60);
/// Ceiling for the exponentially-backed-off (×2 per failed probe)
/// cooldown.
pub const PROBE_COOLDOWN_MAX: Duration = Duration::from_secs(30 * 60);

/// Recovery state riding alongside the trip inputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProbeState {
    /// Not degraded; no probe bookkeeping.
    Healthy,
    /// Degraded; the gate stays closed until `until`, after which
    /// `try_claim_probe()` yields one token.
    Cooldown {
        reason: DegradedReason,
        until: Instant,
    },
    /// Degraded with the single probe token handed out; the gate
    /// stays closed until the probe's shard feeds `update()` (or the
    /// orchestrator returns the token via `reset_probe()`).
    Probing { reason: DegradedReason },
}

#[derive(Debug, Clone)]
pub struct Backpressure {
    /// Failure percentage of the most recent shard's files.
    last_failure_pct: f32,
    /// Most recent rolling MB/s sample from `ThroughputCounter`.
    last_throughput_mb_s: f64,
    /// Threshold for `last_failure_pct` above which we degrade.
    failure_pct_threshold: f32,
    /// Throughput floor in MB/s; a *positive* sample below this trips
    /// degradation. Zero throughput before the first shard finishes
    /// is normal and explicitly excluded. A floor of 0 (the default)
    /// disables the check.
    throughput_floor_mb_s: u64,
    /// Cooldown/probe recovery state machine.
    probe: ProbeState,
    /// Current cooldown length. Reset to `PROBE_COOLDOWN_INITIAL` on
    /// recovery or a fresh trip; doubled (capped at
    /// `PROBE_COOLDOWN_MAX`) on each failed probe.
    cooldown: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DegradedReason {
    FailureRate,
    ThroughputFloor,
}

impl DegradedReason {
    pub fn as_str(self) -> &'static str {
        match self {
            DegradedReason::FailureRate => "failure_rate_high",
            DegradedReason::ThroughputFloor => "throughput_low",
        }
    }
}

impl Backpressure {
    pub fn new(failure_pct_threshold: f32, throughput_floor_mb_s: u64) -> Self {
        Self {
            last_failure_pct: 0.0,
            last_throughput_mb_s: 0.0,
            failure_pct_threshold,
            throughput_floor_mb_s,
            probe: ProbeState::Healthy,
            cooldown: PROBE_COOLDOWN_INITIAL,
        }
    }

    /// Record the outcome of a completed shard and the latest
    /// throughput sample. Called by the orchestrator after each shard.
    /// Also drives the recovery state machine: a healthy outcome
    /// clears degradation (and resets the backoff); an unhealthy one
    /// (re-)degrades — doubling the cooldown if this was a probe
    /// shard's outcome.
    pub fn update(&mut self, files_ok: u64, files_failed: u64, throughput_mb_s: f64) {
        let total = files_ok + files_failed;
        self.last_failure_pct = if total > 0 {
            (files_failed as f64 / total as f64 * 100.0) as f32
        } else {
            0.0
        };
        self.last_throughput_mb_s = throughput_mb_s;
        self.transition(Instant::now());
    }

    /// Trip conditions — unchanged from M3. Pure function of the last
    /// shard's inputs.
    fn eval_inputs(&self) -> Option<DegradedReason> {
        if self.last_failure_pct > self.failure_pct_threshold {
            return Some(DegradedReason::FailureRate);
        }
        // Zero throughput means "no data yet" not "throughput
        // collapsed" — ignore until we have a real sample.
        if self.last_throughput_mb_s > 0.0
            && (self.last_throughput_mb_s as u64) < self.throughput_floor_mb_s
        {
            return Some(DegradedReason::ThroughputFloor);
        }
        None
    }

    /// Advance the recovery state machine on fresh inputs.
    fn transition(&mut self, now: Instant) {
        match self.eval_inputs() {
            None => {
                // Healthy shard: full recovery, backoff resets.
                self.probe = ProbeState::Healthy;
                self.cooldown = PROBE_COOLDOWN_INITIAL;
            }
            Some(reason) => {
                self.cooldown = match self.probe {
                    // Probe shard came back unhealthy: back off harder.
                    ProbeState::Probing { .. } => (self.cooldown * 2).min(PROBE_COOLDOWN_MAX),
                    // Fresh trip from healthy: start at the default.
                    ProbeState::Healthy => PROBE_COOLDOWN_INITIAL,
                    // Already cooling down (shouldn't normally see an
                    // update here — the gate blocks claims — but a
                    // straggler outcome must not double the backoff).
                    ProbeState::Cooldown { .. } => self.cooldown,
                };
                self.probe = ProbeState::Cooldown {
                    reason,
                    until: now + self.cooldown,
                };
            }
        }
    }

    /// Are we currently degraded? `None` = healthy, `Some(reason)` =
    /// stop claiming new shards (modulo the single probe token —
    /// see [`Backpressure::try_claim_probe`]).
    pub fn degraded(&self) -> Option<DegradedReason> {
        match self.probe {
            ProbeState::Healthy => None,
            ProbeState::Cooldown { reason, .. } | ProbeState::Probing { reason } => Some(reason),
        }
    }

    /// While degraded: once the cooldown has elapsed, hand out the
    /// single probe token. Returns `true` at most once per cooldown
    /// window; the caller must claim/process exactly one shard and
    /// feed the outcome to `update()` — or return the token via
    /// [`Backpressure::reset_probe`] if no shard could be claimed.
    /// Always `false` when healthy.
    pub fn try_claim_probe(&mut self) -> bool {
        match self.probe {
            ProbeState::Cooldown { reason, until } if Instant::now() >= until => {
                self.probe = ProbeState::Probing { reason };
                true
            }
            _ => false,
        }
    }

    /// Is the single probe token currently handed out?
    pub fn is_probing(&self) -> bool {
        matches!(self.probe, ProbeState::Probing { .. })
    }

    /// Return a consumed probe token whose shard never produced an
    /// `update()` (nothing claimable, lost race, shard-fatal error).
    /// The probe becomes immediately eligible again — that pass told
    /// us nothing about destination health, so the backoff is neither
    /// doubled nor restarted. No-op unless probing.
    pub fn reset_probe(&mut self) {
        if let ProbeState::Probing { reason } = self.probe {
            self.probe = ProbeState::Cooldown {
                reason,
                until: Instant::now(),
            };
        }
    }

    /// Recovery-phase suffix for the operator-facing status string:
    /// `"probe-pending"` while cooling down, `"probing"` while the
    /// probe shard is in flight. `None` when healthy.
    pub fn probe_phase(&self) -> Option<&'static str> {
        match self.probe {
            ProbeState::Healthy => None,
            ProbeState::Cooldown { .. } => Some("probe-pending"),
            ProbeState::Probing { .. } => Some("probing"),
        }
    }

    pub fn last_failure_pct(&self) -> f32 {
        self.last_failure_pct
    }

    pub fn last_throughput_mb_s(&self) -> f64 {
        self.last_throughput_mb_s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn healthy_until_failures_exceed_threshold() {
        let mut bp = Backpressure::new(5.0, 100);
        bp.update(95, 5, 1000.0); // 5% — at threshold, not over
        assert_eq!(bp.degraded(), None);
        bp.update(94, 6, 1000.0); // 6% — over
        assert_eq!(bp.degraded(), Some(DegradedReason::FailureRate));
    }

    #[test]
    fn throughput_floor_trips_degradation() {
        let mut bp = Backpressure::new(5.0, 100);
        bp.update(100, 0, 50.0); // way below 100 MB/s floor
        assert_eq!(bp.degraded(), Some(DegradedReason::ThroughputFloor));
    }

    #[test]
    fn zero_throughput_does_not_trip_floor() {
        // Pre-first-shard state: cumulative=0 → rate=0. Should not
        // be flagged as "throughput collapsed."
        let mut bp = Backpressure::new(5.0, 100);
        bp.update(0, 0, 0.0);
        assert_eq!(bp.degraded(), None);
    }

    #[test]
    fn empty_shard_does_not_trip_failure_rate() {
        let mut bp = Backpressure::new(5.0, 100);
        bp.update(0, 0, 1000.0);
        assert_eq!(bp.degraded(), None);
    }

    // ---- F14 recovery: cooldown + single-probe state machine -------
    //
    // These use tokio paused time (`start_paused`); `Backpressure`
    // reads `tokio::time::Instant`, which the paused runtime controls
    // via `tokio::time::advance`.

    use std::time::Duration;

    /// Acceptance test 1 (red before fix): degradation is no longer a
    /// one-way trap. After the cooldown elapses the gate hands out
    /// exactly ONE probe token — not a full reopening.
    #[tokio::test(start_paused = true)]
    async fn degraded_expires_into_probe_after_cooldown() {
        let mut bp = Backpressure::new(5.0, 100);
        bp.update(90, 10, 1000.0); // 10% failures → degraded
        assert_eq!(bp.degraded(), Some(DegradedReason::FailureRate));

        // Inside the cooldown: still fully gated, no probe.
        assert!(!bp.try_claim_probe());
        tokio::time::advance(PROBE_COOLDOWN_INITIAL - Duration::from_secs(1)).await;
        assert!(!bp.try_claim_probe());
        assert_eq!(bp.degraded(), Some(DegradedReason::FailureRate));

        // Past the cooldown: exactly one probe token.
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(bp.try_claim_probe());
        // Still degraded — a probe is not a reopening.
        assert_eq!(bp.degraded(), Some(DegradedReason::FailureRate));
        // Single flight: no second token while the probe is out.
        assert!(!bp.try_claim_probe());
        assert!(bp.is_probing());
    }

    /// Acceptance test 2 (red before fix): a healthy shard outcome fed
    /// through `update()` after the probe clears degradation entirely
    /// and normal claiming resumes.
    #[tokio::test(start_paused = true)]
    async fn probe_success_clears_degraded() {
        let mut bp = Backpressure::new(5.0, 100);
        bp.update(90, 10, 1000.0);
        assert_eq!(bp.degraded(), Some(DegradedReason::FailureRate));

        tokio::time::advance(PROBE_COOLDOWN_INITIAL).await;
        assert!(bp.try_claim_probe());

        // Probe shard came back healthy.
        bp.update(100, 0, 1000.0);
        assert_eq!(bp.degraded(), None);
        assert!(!bp.is_probing());
        // Healthy again: no probe bookkeeping lingers.
        assert!(!bp.try_claim_probe());

        // And a later fresh trip starts from the DEFAULT cooldown
        // again (backoff reset on recovery).
        bp.update(90, 10, 1000.0);
        tokio::time::advance(PROBE_COOLDOWN_INITIAL).await;
        assert!(bp.try_claim_probe());
    }

    /// Acceptance test 3: an unhealthy probe outcome re-degrades with
    /// an exponentially longer cooldown (×2), capped at 30 min.
    #[tokio::test(start_paused = true)]
    async fn probe_failure_redegrades_with_longer_cooldown() {
        let mut bp = Backpressure::new(5.0, 100);
        bp.update(90, 10, 1000.0);
        assert_eq!(bp.degraded(), Some(DegradedReason::FailureRate));

        // First probe after the default 5-min cooldown → fails.
        tokio::time::advance(PROBE_COOLDOWN_INITIAL).await;
        assert!(bp.try_claim_probe());
        bp.update(90, 10, 1000.0); // still unhealthy
        assert_eq!(bp.degraded(), Some(DegradedReason::FailureRate));
        assert!(!bp.is_probing());

        // Next window is doubled: 10 min. The old 5-min mark must NOT
        // yield a token.
        tokio::time::advance(PROBE_COOLDOWN_INITIAL).await;
        assert!(!bp.try_claim_probe());
        tokio::time::advance(PROBE_COOLDOWN_INITIAL).await; // t = 10 min
        assert!(bp.try_claim_probe());

        // Fail again: 20 min...
        bp.update(90, 10, 1000.0);
        tokio::time::advance(2 * PROBE_COOLDOWN_INITIAL).await;
        assert!(!bp.try_claim_probe());
        tokio::time::advance(2 * PROBE_COOLDOWN_INITIAL).await; // t = 20 min
        assert!(bp.try_claim_probe());

        // ...then capped at 30 min (not 40).
        bp.update(90, 10, 1000.0);
        tokio::time::advance(PROBE_COOLDOWN_MAX).await;
        assert!(bp.try_claim_probe());

        // Still capped on the next failure (stays 30 min).
        bp.update(90, 10, 1000.0);
        tokio::time::advance(PROBE_COOLDOWN_MAX - Duration::from_secs(1)).await;
        assert!(!bp.try_claim_probe());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(bp.try_claim_probe());
    }

    /// Acceptance test 4 (regression): a healthy worker sees no probe
    /// bookkeeping at all; `degraded()` stays `None` however far time
    /// advances.
    #[tokio::test(start_paused = true)]
    async fn healthy_worker_unaffected() {
        let mut bp = Backpressure::new(5.0, 100);
        assert_eq!(bp.degraded(), None);
        assert!(!bp.try_claim_probe());
        assert!(!bp.is_probing());

        bp.update(100, 0, 1000.0);
        assert_eq!(bp.degraded(), None);
        assert!(!bp.try_claim_probe());

        tokio::time::advance(Duration::from_secs(3600)).await;
        assert_eq!(bp.degraded(), None);
        assert!(!bp.try_claim_probe());
        assert!(!bp.is_probing());
    }

    /// An abandoned probe (token consumed but no shard ever fed
    /// `update()` — nothing claimable, lost race, shard-fatal) can be
    /// returned via `reset_probe()` and retried without doubling the
    /// backoff.
    #[tokio::test(start_paused = true)]
    async fn reset_probe_returns_the_token() {
        let mut bp = Backpressure::new(5.0, 100);
        bp.update(90, 10, 1000.0);
        tokio::time::advance(PROBE_COOLDOWN_INITIAL).await;
        assert!(bp.try_claim_probe());
        assert!(bp.is_probing());

        bp.reset_probe();
        assert!(!bp.is_probing());
        assert_eq!(bp.degraded(), Some(DegradedReason::FailureRate));
        // Immediately eligible again — no extra wait, no doubling.
        assert!(bp.try_claim_probe());
    }
}
