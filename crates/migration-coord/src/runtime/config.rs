use crate::events::EventLogConfig;
use crate::lease::LeaseConfig;
use chrono::{DateTime, Utc};

/// Source of wall-clock `at` timestamps for events the runtime
/// ingests. Production uses [`SystemClock`]; tests use
/// `FixedClock` (the test module) to drive event timing
/// deterministically.
pub trait Clock: Send + Sync + std::fmt::Debug {
    fn now(&self) -> DateTime<Utc>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

// =============================================================================
// Config
// =============================================================================

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub lease: LeaseConfig,
    pub events: EventLogConfig,
    /// Capacity of the SSE broadcast channel. Each subscriber sees
    /// the most recent N events queued for it; once full, the
    /// subscriber sees `RecvError::Lagged(skipped)` and the SSE
    /// handler emits a synthetic `Resync` event telling the client
    /// to re-fetch the snapshot. 1024 is a reasonable starting
    /// point — sized for "one slow subscriber falls behind a
    /// 10k-events-per-second burst for ~100ms before resync".
    pub bus_capacity: usize,
    /// How long [`super::CoordRuntime::start`] backs off between failed lease-acquire
    /// attempts. Tests inject a short value; production defaults
    /// to 2s.
    pub lease_retry_interval: std::time::Duration,
    /// Maximum attempts before [`super::CoordRuntime::start`] gives up. `None` means
    /// retry forever — the production default. Tests pin a finite
    /// cap.
    pub lease_retry_max_attempts: Option<u32>,
}

impl RuntimeConfig {
    pub fn default_for_prod() -> Self {
        Self {
            lease: LeaseConfig::default_for_prod(),
            events: EventLogConfig::default(),
            bus_capacity: 1024,
            lease_retry_interval: std::time::Duration::from_secs(2),
            lease_retry_max_attempts: None,
        }
    }
}
