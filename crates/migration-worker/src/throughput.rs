//! Throughput counter for the rolling MB/s number the heartbeat
//! publishes into `progress/host-<id>.json`.
//!
//! Design intentionally simple for M3:
//!
//! - Atomic `cumulative_bytes` updated by every successful copy.
//! - The heartbeat samples it on each tick. Δbytes ÷ Δt = instantaneous
//!   MB/s for the tick.
//! - A single 60-second sliding average is held in a `Mutex<Window>` —
//!   contention is one push per heartbeat tick (every 30s by default),
//!   so it's effectively free.
//!
//! A more sophisticated implementation (per-thread counters, moving
//! averages over many windows, EWMA) is M3.5+. The single-window
//! moving average is enough to drive the M3 "degraded" backpressure
//! gate.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Cheap-clone byte counter for successful file copies.
#[derive(Clone, Default)]
pub struct ThroughputCounter {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    cumulative_bytes: AtomicU64,
    samples: Mutex<Window>,
}

impl ThroughputCounter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record `n` successful bytes copied. Called by the shard
    /// processor after each successful row. Cheap (one atomic add).
    pub fn add(&self, n: u64) {
        self.inner.cumulative_bytes.fetch_add(n, Ordering::Relaxed);
    }

    /// Sample the counter into the rolling window. Called by the
    /// heartbeat task on each tick. Returns the smoothed MB/s over
    /// the last `window_secs` seconds (or shorter if we haven't run
    /// that long yet).
    pub fn sample_mb_s(&self, window_secs: u64) -> f64 {
        let now = Instant::now();
        let cumulative = self.inner.cumulative_bytes.load(Ordering::Relaxed);
        let mut win = self.inner.samples.lock().unwrap();
        win.push(now, cumulative, window_secs);
        win.mb_per_sec()
    }
}

/// Bounded ring of `(instant, cumulative_bytes)` samples. We retain
/// samples within `window_secs`; older samples are dropped. The MB/s
/// rate is `(latest - oldest_in_window).bytes / window.duration`.
#[derive(Default)]
struct Window {
    /// Oldest first. Bounded only by time — at one push per
    /// heartbeat tick (~30s), 60s window holds 2-3 samples.
    samples: Vec<(Instant, u64)>,
}

impl Window {
    fn push(&mut self, now: Instant, cumulative: u64, window_secs: u64) {
        // Drop samples older than the window.
        let cutoff = now.checked_sub(std::time::Duration::from_secs(window_secs));
        if let Some(cutoff) = cutoff {
            self.samples.retain(|(t, _)| *t >= cutoff);
        }
        self.samples.push((now, cumulative));
    }

    fn mb_per_sec(&self) -> f64 {
        if self.samples.len() < 2 {
            return 0.0;
        }
        let (t0, b0) = self.samples.first().copied().unwrap();
        let (t1, b1) = self.samples.last().copied().unwrap();
        let secs = t1.duration_since(t0).as_secs_f64();
        if secs <= 0.0 {
            return 0.0;
        }
        let bytes = b1.saturating_sub(b0) as f64;
        bytes / secs / (1024.0 * 1024.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn empty_counter_is_zero() {
        let c = ThroughputCounter::new();
        assert_eq!(c.sample_mb_s(60), 0.0);
    }

    #[test]
    fn single_sample_still_zero() {
        // Need at least two samples to compute a rate.
        let c = ThroughputCounter::new();
        c.add(1024 * 1024);
        assert_eq!(c.sample_mb_s(60), 0.0);
    }

    #[test]
    fn rate_after_two_samples() {
        let c = ThroughputCounter::new();
        // First sample: 0 bytes at t0.
        c.sample_mb_s(60);
        c.add(10 * 1024 * 1024); // +10 MiB
        std::thread::sleep(Duration::from_millis(10));
        let rate = c.sample_mb_s(60);
        // Hard to assert an exact rate (depends on sleep precision)
        // but it should be way more than 0 and finite.
        assert!(rate > 0.0, "rate was {rate}");
        assert!(rate.is_finite());
    }
}
