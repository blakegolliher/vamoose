//! Backpressure gate.
//!
//! Per DESIGN.md "Backpressure": when the worker's recent failure
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
    /// is normal and explicitly excluded.
    throughput_floor_mb_s: u64,
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
        }
    }

    /// Record the outcome of a completed shard and the latest
    /// throughput sample. Called by the orchestrator after each shard.
    pub fn update(&mut self, files_ok: u64, files_failed: u64, throughput_mb_s: f64) {
        let total = files_ok + files_failed;
        self.last_failure_pct = if total > 0 {
            (files_failed as f64 / total as f64 * 100.0) as f32
        } else {
            0.0
        };
        self.last_throughput_mb_s = throughput_mb_s;
    }

    /// Are we currently degraded? `None` = healthy, `Some(reason)` =
    /// stop claiming new shards until conditions change.
    pub fn degraded(&self) -> Option<DegradedReason> {
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
}
