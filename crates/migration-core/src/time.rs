//! Time utilities.
//!
//! Everything in the system uses UTC. We do not use local time anywhere
//! because workers can be in different timezones. The wire format is
//! RFC 3339 / ISO 8601.
//!
//! Lease and heartbeat decisions are made against `Utc::now()` on the
//! reading worker, **not** the timestamp in the claim — that timestamp
//! is informational. The authoritative ownership signal is the S3
//! object etag.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct UtcTime(pub DateTime<Utc>);

impl UtcTime {
    pub fn now() -> Self {
        Self(Utc::now())
    }
}

impl std::fmt::Display for UtcTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // RFC 3339 with Z suffix.
        write!(
            f,
            "{}",
            self.0.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        )
    }
}

/// Standard heartbeat interval. See DESIGN.md `[worker]` config.
pub const DEFAULT_HEARTBEAT_SEC: u64 = 30;

/// Standard lease timeout. 6× heartbeat — tight enough to recover from
/// dead workers quickly at wire rate, loose enough to absorb GC pauses
/// and brief S3 hiccups.
pub const DEFAULT_LEASE_TIMEOUT_SEC: u64 = 180;
