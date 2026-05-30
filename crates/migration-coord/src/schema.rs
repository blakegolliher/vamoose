//! Wire + snapshot schema for the coord.
//!
//! Two kinds of types live here:
//!
//! - **Snapshot-facing types** (`Job`, `Worker`, `Progress`,
//!   `ThroughputWindows`, `EtaEstimate`, `Health`, `ErrorBucket`) —
//!   serialized into `state/snapshot.json` and returned from REST
//!   endpoints. The TUI and (later) the web dashboard consume these
//!   verbatim.
//!
//! - **Event-log types** (`EventEnvelope`, `EventKind`) — the
//!   append-only log under `events/{job_id}/...` and
//!   `events/_cluster/...`. Every event carries a coord-assigned
//!   monotonic `seq`, a coord-assigned `at` timestamp, and a
//!   `schema_version`. The kind discriminator (`"JobCreated"` etc.)
//!   doubles as the SSE event name on the wire.
//!
//! There is no separate "in-memory derived state" type; the coord's
//! live state is just `HashMap<JobId, Job>` and `HashMap<WorkerId,
//! Worker>`. The reducer lives in `state.rs` (Phase 1 follow-up).
//!
//! ## Schema versioning
//!
//! Both snapshots and event-log chunks carry `schema_version: u8`. A
//! coord that loads a snapshot or event with a `schema_version` higher
//! than `SCHEMA_VERSION` refuses to start. The first cut is version 1
//! — bumped only on incompatible changes (renamed fields, removed
//! variants, changed semantics). Additive changes that survive
//! round-trip under `#[serde(default)]` do not need a bump.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

/// Wire schema version. Bumped only on incompatible changes.
///
/// Snapshots and event-log envelopes both carry this value. Coord
/// refuses to load anything with `schema_version > SCHEMA_VERSION`.
pub const SCHEMA_VERSION: u8 = 1;

// =============================================================================
// Identifiers
// =============================================================================

/// Opaque job identifier. Set by the operator at job creation
/// (e.g. `"bobby-migration"`). Must not contain `/` so it composes
/// cleanly with [`crate::layout`] keys.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(transparent)]
pub struct JobId(pub String);

impl JobId {
    /// Construct a `JobId` after rejecting characters that would break
    /// the S3 key layout. The only reserved character is `/`, which
    /// would put job state in the wrong prefix; everything else
    /// (including underscores, hyphens, and unicode) is allowed.
    pub fn new(s: impl Into<String>) -> Result<Self, InvalidJobId> {
        let s = s.into();
        if s.is_empty() {
            return Err(InvalidJobId::Empty);
        }
        if s.contains('/') {
            return Err(InvalidJobId::ContainsSlash);
        }
        Ok(Self(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum InvalidJobId {
    #[error("job id may not be empty")]
    Empty,
    #[error("job id may not contain '/'")]
    ContainsSlash,
}

/// Coord-assigned worker identity. A UUID v4 minted at register time.
///
/// Workers also carry an operator-meaningful `host_id` (hostname+pid)
/// on the `Worker` struct, which is what shows up in human-facing
/// output. The UUID is used for routing only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(transparent)]
pub struct WorkerId(pub Uuid);

impl WorkerId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for WorkerId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for WorkerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Opaque shard identifier. Today this is the parquet shard filename
/// minus `.parquet`, matching `migration_core::layout`. Coord treats it
/// as opaque — only the worker interprets the value.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(transparent)]
pub struct ShardId(pub String);

// =============================================================================
// Job + lifecycle
// =============================================================================

/// Job lifecycle phase. Linear progression except for `Paused` (re-entry
/// from any active phase) and the terminal trio (`Completed`, `Failed`,
/// `Cancelled`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Phase {
    Planned,
    Scanning,
    Copying,
    Verifying,
    Cutover,
    Paused,
    Completed,
    Failed,
    Cancelled,
}

impl Phase {
    pub fn is_terminal(self) -> bool {
        matches!(self, Phase::Completed | Phase::Failed | Phase::Cancelled)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhaseTransition {
    pub from: Phase,
    pub to: Phase,
    pub at: DateTime<Utc>,
    pub reason: String,
}

/// Aggregate progress counters for a job. Worker-reported deltas are
/// folded into these by the reducer.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Progress {
    /// Total files discovered (set by scan completion). Zero while
    /// scanning is in progress.
    pub files_total: u64,
    pub files_done: u64,
    pub bytes_total: u64,
    pub bytes_done: u64,
    pub errors_total: u64,
    pub conflicts_resolved: u64,
    pub fence_events: u64,
}

/// A single throughput window (rolling). Units are bytes/sec for the
/// byte metrics; the TUI converts to MB/s for display.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ThroughputWindow {
    pub instant: f64,
    pub smoothed: f64,
    pub peak: f64,
}

/// 1-second, 1-minute, 5-minute rolling throughput windows. Each is
/// maintained client-side in the TUI from `ProgressDelta` events; the
/// snapshot carries the coord's view (used by REST consumers and as
/// the seed for TUI clients).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ThroughputWindows {
    pub one_sec: ThroughputWindow,
    pub one_min: ThroughputWindow,
    pub five_min: ThroughputWindow,
}

/// Per-phase ETA estimate with a p50/p95 confidence band.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PhaseEta {
    /// Duration from `at` to expected completion. `None` until the
    /// coord has enough signal (typically after the first
    /// `ProgressDelta`).
    pub p50_secs: Option<u64>,
    pub p95_secs: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct EtaEstimate {
    pub scan: PhaseEta,
    pub copy: PhaseEta,
    pub verify: PhaseEta,
}

/// Health rollup. Reasons are operator-visible strings; the reducer
/// produces them from coord-observable signals (stuck workers, error
/// rate, claim contention).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", content = "reasons")]
pub enum Health {
    OnTrack,
    AtRisk(Vec<String>),
    Blocked(Vec<String>),
}

impl Default for Health {
    fn default() -> Self {
        Self::OnTrack
    }
}

/// Policy for resolving src/dst conflicts mid-migration. Stored on
/// `JobConfig` and pinned for the life of the job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictPolicy {
    /// Skip the destination file when it exists.
    Skip,
    /// Overwrite the destination file unconditionally.
    Overwrite,
    /// Fail the file (and emit a `ClaimConflictDetected` event).
    Fail,
}

/// How to handle source ACLs on the destination side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AclHandling {
    /// Preserve POSIX mode + owner/group only.
    PosixOnly,
    /// Preserve POSIX mode + NFSv4 ACLs where available.
    Nfsv4Acls,
    /// Drop all ACL metadata.
    Drop,
}

/// Post-copy verification depth.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VerifyMode {
    /// No verification.
    None,
    /// Stat (size + mtime) only.
    Stat,
    /// Full byte-level checksum.
    Checksum,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParallelismCaps {
    pub workers_max: Option<u32>,
    pub per_worker_inflight: Option<u32>,
    pub per_shard_inflight: Option<u32>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RateCaps {
    pub bytes_per_sec_global: Option<u64>,
    pub bytes_per_sec_per_worker: Option<u64>,
}

/// Immutable job configuration. Written to `jobs/{job_id}/config.json`
/// at creation and never mutated. `claim_version` defaults to `2`
/// (today's protocol); `1` is rejected.
///
/// Free-form fields (`exclusions`, the four caps structs) are kept
/// explicit rather than a `serde_json::Value` blob so the wire shape
/// is reviewable.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobConfig {
    pub source: String,
    pub dest: String,
    #[serde(default = "default_claim_version")]
    pub claim_version: u8,
    #[serde(default = "default_conflict_policy")]
    pub conflict_policy: ConflictPolicy,
    #[serde(default)]
    pub exclusions: Vec<String>,
    #[serde(default)]
    pub parallelism: ParallelismCaps,
    #[serde(default)]
    pub rate_caps: RateCaps,
    #[serde(default = "default_acl_handling")]
    pub acl_handling: AclHandling,
    #[serde(default = "default_verify_mode")]
    pub verify_mode: VerifyMode,
}

fn default_claim_version() -> u8 {
    2
}
fn default_conflict_policy() -> ConflictPolicy {
    ConflictPolicy::Fail
}
fn default_acl_handling() -> AclHandling {
    AclHandling::PosixOnly
}
fn default_verify_mode() -> VerifyMode {
    VerifyMode::Stat
}

/// Hex-encoded config hash. Computed at job creation from a
/// deterministic JSON encoding of `JobConfig` (Phase 1 stub: the
/// reducer will fill this in; the type just carries the value).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ConfigHash(pub String);

/// Snapshot view of a job. The TUI's job-list and overview tab
/// consume this verbatim.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Job {
    pub id: JobId,
    pub name: String,
    pub source: String,
    pub dest: String,
    /// Admin-token label that created the job.
    pub owner: String,
    pub created_at: DateTime<Utc>,
    pub config_hash: ConfigHash,
    pub config: JobConfig,
    pub phase: Phase,
    #[serde(default)]
    pub phase_history: Vec<PhaseTransition>,
    #[serde(default)]
    pub progress: Progress,
    #[serde(default)]
    pub throughput: ThroughputWindows,
    #[serde(default)]
    pub eta: EtaEstimate,
    #[serde(default)]
    pub health: Health,
    #[serde(default)]
    pub assigned_workers: Vec<WorkerId>,
}

// =============================================================================
// Workers
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkerState {
    Idle,
    Scanning,
    Copying,
    Verifying,
    Draining,
    Fenced,
    Failed,
    Disconnected,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WorkerCounters {
    pub files_per_sec: f64,
    pub bytes_per_sec: f64,
    pub errors_per_min: f64,
}

/// Control mode returned by the coord in every heartbeat response.
/// The worker flips its local `RunControl` to match; the coord does
/// not retry — the next heartbeat carries the same mode if it is still
/// in effect.
///
/// Mapping from job phase:
/// - `Phase::Paused` → `Pause` (operator can resume)
/// - `Phase::Cancelled | Failed` → `Cancel` (terminal, worker exits)
/// - everything else → `Run`
///
/// `Drain` is reserved for a future distinct "finish in-flight, then
/// exit cleanly" phase. v1 routes drain through `JobPaused` (matches
/// the Phase 2 caveat) so workers see it as `Pause`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ControlMode {
    Run,
    Pause,
    Drain,
    Cancel,
}

impl ControlMode {
    /// Derive the control mode the worker should observe for a job
    /// currently in `phase`. Terminal phases (`Completed`, `Failed`,
    /// `Cancelled`) all map to `Cancel` — if a worker is still
    /// heartbeating after the job's terminal transition it's a stale
    /// process, and the right answer is "exit cleanly".
    pub fn for_phase(phase: Phase) -> Self {
        match phase {
            Phase::Paused => Self::Pause,
            Phase::Completed | Phase::Failed | Phase::Cancelled => Self::Cancel,
            Phase::Planned
            | Phase::Scanning
            | Phase::Copying
            | Phase::Verifying
            | Phase::Cutover => Self::Run,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Worker {
    pub id: WorkerId,
    /// Job the worker registered against. Set by the reducer from the
    /// `WorkerJoined` event's `job_id`. Denormalized for O(1) lookup
    /// in the heartbeat path (which needs the job's phase to derive
    /// the control envelope).
    pub job_id: JobId,
    pub host: String,
    pub pid: u32,
    /// Worker-process start time. Used together with `host` and `pid`
    /// to detect stale registrations on re-register.
    pub start_time: DateTime<Utc>,
    pub version: String,
    pub joined_at: DateTime<Utc>,
    pub last_heartbeat: DateTime<Utc>,
    pub state: WorkerState,
    #[serde(default)]
    pub assigned_shard: Option<ShardId>,
    #[serde(default)]
    pub queue_depth: u32,
    #[serde(default)]
    pub inflight_ops: u32,
    #[serde(default)]
    pub counters: WorkerCounters,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub fence_reason: Option<String>,
}

// =============================================================================
// Errors (operator-facing aggregation)
// =============================================================================

/// Classes the coord aggregates `ErrorEmitted` events into. `Other`
/// is a free-form bucket the worker can use for anything that doesn't
/// fit cleanly.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "class", content = "value")]
pub enum ErrorClass {
    /// NFSv3 error code (NFSERR_*).
    Nfs3Err(u32),
    ClaimConflict,
    Permission,
    Timeout,
    ChecksumMismatch,
    Other(String),
}

/// Aggregated error bucket — populated by the reducer from
/// `ErrorEmitted` events. `sample_paths` is capped at
/// `ERROR_SAMPLE_CAP` by the reducer (newest-wins on overflow).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorBucket {
    pub class: ErrorClass,
    pub count: u64,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    #[serde(default)]
    pub sample_paths: Vec<String>,
    pub retryable: bool,
}

/// Cap on sample paths per error bucket. Enforced by the reducer.
pub const ERROR_SAMPLE_CAP: usize = 10;

// =============================================================================
// Event log
// =============================================================================

/// Envelope every event-log entry is wrapped in. The wire shape is a
/// flat JSON object: `seq`, `at`, `schema_version` at the top level,
/// then the tag and payload from `EventKind` flattened in.
///
/// Example wire form:
///
/// ```json
/// {"seq":17,"at":"2026-05-29T14:32:00Z","schema_version":1,
///  "kind":"WorkerJoined","worker_id":"...","host":"host-a","pid":42,
///  "start_time":"2026-05-29T14:31:55Z","version":"0.6.0"}
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub seq: u64,
    pub at: DateTime<Utc>,
    #[serde(default = "default_schema_version")]
    pub schema_version: u8,
    /// Worker-local timestamp for the event, if reported. Diagnostic
    /// only — coord never compares it across nodes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_at: Option<DateTime<Utc>>,
    #[serde(flatten)]
    pub kind: EventKind,
}

fn default_schema_version() -> u8 {
    SCHEMA_VERSION
}

/// All event kinds the system emits. The `kind` discriminator on the
/// wire doubles as the SSE event name.
///
/// `WorkerHeartbeat` is deliberately *not* present here — heartbeats
/// drive state derivation but never enter the event log or SSE
/// stream. They flow over `POST /workers/{id}/heartbeat` only.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum EventKind {
    JobCreated {
        job_id: JobId,
        name: String,
        source: String,
        dest: String,
        owner: String,
        config_hash: ConfigHash,
    },
    JobPhaseChanged {
        job_id: JobId,
        from: Phase,
        to: Phase,
        reason: String,
    },
    JobPaused {
        job_id: JobId,
        reason: String,
    },
    JobResumed {
        job_id: JobId,
        reason: String,
    },
    JobCancelled {
        job_id: JobId,
        reason: String,
    },
    JobCompleted {
        job_id: JobId,
    },
    JobFailed {
        job_id: JobId,
        reason: String,
    },

    WorkerJoined {
        worker_id: WorkerId,
        job_id: JobId,
        host: String,
        pid: u32,
        /// Worker-process start time. Combined with `host` and `pid`,
        /// uniquely identifies a worker instance across restarts —
        /// the coord uses this triple to detect stale registrations
        /// and mark prior WorkerIds as Disconnected.
        start_time: DateTime<Utc>,
        version: String,
    },
    WorkerLeft {
        worker_id: WorkerId,
        reason: String,
    },
    WorkerStateChanged {
        worker_id: WorkerId,
        from: WorkerState,
        to: WorkerState,
    },
    WorkerFenced {
        worker_id: WorkerId,
        reason: String,
    },
    WorkerRecovered {
        worker_id: WorkerId,
    },

    ProgressDelta {
        job_id: JobId,
        worker_id: WorkerId,
        files_delta: u64,
        bytes_delta: u64,
        errors_delta: u64,
    },
    ErrorEmitted {
        job_id: JobId,
        worker_id: WorkerId,
        class: ErrorClass,
        path: String,
        retryable: bool,
        message: String,
    },

    ClaimConflictDetected {
        job_id: JobId,
        shard_id: ShardId,
        holder: WorkerId,
        contender: WorkerId,
    },
    ClaimConflictResolved {
        job_id: JobId,
        shard_id: ShardId,
        winner: WorkerId,
    },

    VerifyStarted {
        job_id: JobId,
    },
    VerifyFileMismatch {
        job_id: JobId,
        path: String,
        expected: String,
        got: String,
    },
    VerifyCompleted {
        job_id: JobId,
        mismatches: u64,
    },
}

impl EventKind {
    /// SSE event name — the value of the `event:` line for this kind.
    /// Matches the serde tag exactly so a client switching on the
    /// SSE event name and a client decoding the `kind` field see the
    /// same value.
    pub fn name(&self) -> &'static str {
        match self {
            Self::JobCreated { .. } => "JobCreated",
            Self::JobPhaseChanged { .. } => "JobPhaseChanged",
            Self::JobPaused { .. } => "JobPaused",
            Self::JobResumed { .. } => "JobResumed",
            Self::JobCancelled { .. } => "JobCancelled",
            Self::JobCompleted { .. } => "JobCompleted",
            Self::JobFailed { .. } => "JobFailed",
            Self::WorkerJoined { .. } => "WorkerJoined",
            Self::WorkerLeft { .. } => "WorkerLeft",
            Self::WorkerStateChanged { .. } => "WorkerStateChanged",
            Self::WorkerFenced { .. } => "WorkerFenced",
            Self::WorkerRecovered { .. } => "WorkerRecovered",
            Self::ProgressDelta { .. } => "ProgressDelta",
            Self::ErrorEmitted { .. } => "ErrorEmitted",
            Self::ClaimConflictDetected { .. } => "ClaimConflictDetected",
            Self::ClaimConflictResolved { .. } => "ClaimConflictResolved",
            Self::VerifyStarted { .. } => "VerifyStarted",
            Self::VerifyFileMismatch { .. } => "VerifyFileMismatch",
            Self::VerifyCompleted { .. } => "VerifyCompleted",
        }
    }

    /// `job_id` the event pertains to, if any. Used by the event-log
    /// writer to route between `events/{job_id}/...` and
    /// `events/_cluster/...`.
    pub fn job_id(&self) -> Option<&JobId> {
        match self {
            Self::JobCreated { job_id, .. }
            | Self::JobPhaseChanged { job_id, .. }
            | Self::JobPaused { job_id, .. }
            | Self::JobResumed { job_id, .. }
            | Self::JobCancelled { job_id, .. }
            | Self::JobCompleted { job_id, .. }
            | Self::JobFailed { job_id, .. }
            | Self::WorkerJoined { job_id, .. }
            | Self::ProgressDelta { job_id, .. }
            | Self::ErrorEmitted { job_id, .. }
            | Self::ClaimConflictDetected { job_id, .. }
            | Self::ClaimConflictResolved { job_id, .. }
            | Self::VerifyStarted { job_id }
            | Self::VerifyFileMismatch { job_id, .. }
            | Self::VerifyCompleted { job_id, .. } => Some(job_id),

            // Worker lifecycle that doesn't carry a job_id lives on
            // the cluster-wide event log.
            Self::WorkerLeft { .. }
            | Self::WorkerStateChanged { .. }
            | Self::WorkerFenced { .. }
            | Self::WorkerRecovered { .. } => None,
        }
    }
}

// =============================================================================
// Snapshot
// =============================================================================

/// Serialized form of the full coord state at a point in time. Loaded
/// at startup, then events with `seq > last_seq` are replayed on top.
///
/// `audit_seq_today` is the per-day audit sequence counter — the
/// snapshot persists it so a coord restart on the same UTC day picks
/// up where it left off rather than colliding with already-written
/// audit keys.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    #[serde(default = "default_schema_version")]
    pub schema_version: u8,
    pub written_at: DateTime<Utc>,
    /// Highest `seq` reflected in this snapshot's job/worker tables.
    pub last_seq: u64,
    #[serde(default)]
    pub jobs: BTreeMap<JobId, Job>,
    #[serde(default)]
    pub workers: BTreeMap<WorkerId, Worker>,
    /// Per-job error buckets, keyed by job id.
    #[serde(default)]
    pub error_buckets: BTreeMap<JobId, Vec<ErrorBucket>>,
    /// Per-day audit sequence (UTC). Reset at midnight by the reducer.
    #[serde(default)]
    pub audit_seq_today: u64,
    /// Date the `audit_seq_today` counter applies to, `YYYY-MM-DD`
    /// UTC. Empty on fresh start.
    #[serde(default)]
    pub audit_seq_date: String,
}

impl Snapshot {
    pub fn empty(now: DateTime<Utc>) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            written_at: now,
            last_seq: 0,
            jobs: BTreeMap::new(),
            workers: BTreeMap::new(),
            error_buckets: BTreeMap::new(),
            audit_seq_today: 0,
            audit_seq_date: String::new(),
        }
    }
}

// =============================================================================
// Audit
// =============================================================================

/// One line of the audit log. Written to
/// `audit/<YYYY-MM-DD>/<seq:020>.jsonl` — one entry per file (the
/// `seq` portion of the key is the per-day audit sequence the
/// snapshot persists).
///
/// `token_label` is the admin-token *label* (never the token
/// itself); the auth middleware materializes it from the bearer
/// header.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditEntry {
    pub at: DateTime<Utc>,
    pub command_id: String,
    pub token_label: String,
    pub action: String,
    pub target: String,
    #[serde(default)]
    pub args: serde_json::Value,
    pub result: AuditResult,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "detail")]
pub enum AuditResult {
    Accepted,
    Rejected(String),
}

// =============================================================================
// Tests — round-trip JSON for every variant the reducer or wire layer
// will see.
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 5, 29, 14, 32, 0).unwrap()
    }

    fn jid(s: &str) -> JobId {
        JobId::new(s).unwrap()
    }

    fn wid() -> WorkerId {
        WorkerId(Uuid::from_u128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef))
    }

    fn round_trip<T: Serialize + for<'de> Deserialize<'de> + PartialEq + std::fmt::Debug>(v: T) {
        let s = serde_json::to_string(&v).unwrap();
        let back: T = serde_json::from_str(&s).unwrap();
        assert_eq!(v, back, "round-trip failed for: {s}");
    }

    #[test]
    fn job_id_rejects_slash_and_empty() {
        assert!(JobId::new("").is_err());
        assert!(JobId::new("ok/bad").is_err());
        assert!(JobId::new("bobby-migration").is_ok());
        // Unicode is fine; the layout module passes it through verbatim.
        assert!(JobId::new("マイグレ").is_ok());
    }

    #[test]
    fn job_id_serializes_as_bare_string() {
        let id = jid("bobby");
        let s = serde_json::to_string(&id).unwrap();
        assert_eq!(s, "\"bobby\"");
    }

    #[test]
    fn phase_round_trip() {
        for p in [
            Phase::Planned,
            Phase::Scanning,
            Phase::Copying,
            Phase::Verifying,
            Phase::Cutover,
            Phase::Paused,
            Phase::Completed,
            Phase::Failed,
            Phase::Cancelled,
        ] {
            round_trip(p);
        }
    }

    #[test]
    fn worker_state_round_trip() {
        for s in [
            WorkerState::Idle,
            WorkerState::Scanning,
            WorkerState::Copying,
            WorkerState::Verifying,
            WorkerState::Draining,
            WorkerState::Fenced,
            WorkerState::Failed,
            WorkerState::Disconnected,
        ] {
            round_trip(s);
        }
    }

    #[test]
    fn error_class_round_trip() {
        round_trip(ErrorClass::Nfs3Err(13));
        round_trip(ErrorClass::ClaimConflict);
        round_trip(ErrorClass::Permission);
        round_trip(ErrorClass::Timeout);
        round_trip(ErrorClass::ChecksumMismatch);
        round_trip(ErrorClass::Other("custom".to_string()));
    }

    #[test]
    fn health_serializes_with_tag_and_reasons() {
        let s = serde_json::to_string(&Health::OnTrack).unwrap();
        assert_eq!(s, "{\"status\":\"OnTrack\"}");

        let s = serde_json::to_string(&Health::AtRisk(vec!["lag".into()])).unwrap();
        assert_eq!(s, "{\"status\":\"AtRisk\",\"reasons\":[\"lag\"]}");

        round_trip(Health::Blocked(vec!["fence event".into()]));
    }

    #[test]
    fn job_config_defaults_when_minimal() {
        let s = r#"{"source":"nfs://a","dest":"nfs://b"}"#;
        let cfg: JobConfig = serde_json::from_str(s).unwrap();
        assert_eq!(cfg.claim_version, 2);
        assert_eq!(cfg.conflict_policy, ConflictPolicy::Fail);
        assert_eq!(cfg.acl_handling, AclHandling::PosixOnly);
        assert_eq!(cfg.verify_mode, VerifyMode::Stat);
        assert!(cfg.exclusions.is_empty());
    }

    #[test]
    fn event_envelope_flattens_kind() {
        let env = EventEnvelope {
            seq: 17,
            at: at(),
            schema_version: SCHEMA_VERSION,
            worker_at: None,
            kind: EventKind::JobCreated {
                job_id: jid("bobby"),
                name: "bobby-migration".into(),
                source: "nfs://src".into(),
                dest: "nfs://dst".into(),
                owner: "blake".into(),
                config_hash: ConfigHash("deadbeef".into()),
            },
        };
        let s = serde_json::to_string(&env).unwrap();
        // Flattened: kind sits at the top alongside seq/at/schema_version.
        assert!(s.contains("\"seq\":17"));
        assert!(s.contains("\"schema_version\":1"));
        assert!(s.contains("\"kind\":\"JobCreated\""));
        assert!(s.contains("\"name\":\"bobby-migration\""));
        round_trip(env);
    }

    #[test]
    fn every_event_kind_round_trips() {
        let job = jid("bobby");
        let kinds = vec![
            EventKind::JobCreated {
                job_id: job.clone(),
                name: "x".into(),
                source: "s".into(),
                dest: "d".into(),
                owner: "o".into(),
                config_hash: ConfigHash("ab".into()),
            },
            EventKind::JobPhaseChanged {
                job_id: job.clone(),
                from: Phase::Scanning,
                to: Phase::Copying,
                reason: "scan done".into(),
            },
            EventKind::JobPaused {
                job_id: job.clone(),
                reason: "operator".into(),
            },
            EventKind::JobResumed {
                job_id: job.clone(),
                reason: "operator".into(),
            },
            EventKind::JobCancelled {
                job_id: job.clone(),
                reason: "operator".into(),
            },
            EventKind::JobCompleted {
                job_id: job.clone(),
            },
            EventKind::JobFailed {
                job_id: job.clone(),
                reason: "perm denied".into(),
            },
            EventKind::WorkerJoined {
                worker_id: wid(),
                job_id: job.clone(),
                host: "h".into(),
                pid: 1,
                start_time: DateTime::<Utc>::from_timestamp(0, 0).unwrap(),
                version: "0.6.0".into(),
            },
            EventKind::WorkerLeft {
                worker_id: wid(),
                reason: "drain".into(),
            },
            EventKind::WorkerStateChanged {
                worker_id: wid(),
                from: WorkerState::Copying,
                to: WorkerState::Draining,
            },
            EventKind::WorkerFenced {
                worker_id: wid(),
                reason: "self-fence R7".into(),
            },
            EventKind::WorkerRecovered { worker_id: wid() },
            EventKind::ProgressDelta {
                job_id: job.clone(),
                worker_id: wid(),
                files_delta: 5,
                bytes_delta: 1024,
                errors_delta: 0,
            },
            EventKind::ErrorEmitted {
                job_id: job.clone(),
                worker_id: wid(),
                class: ErrorClass::Nfs3Err(13),
                path: "/a/b".into(),
                retryable: true,
                message: "EACCES".into(),
            },
            EventKind::ClaimConflictDetected {
                job_id: job.clone(),
                shard_id: ShardId("part-0042".into()),
                holder: wid(),
                contender: wid(),
            },
            EventKind::ClaimConflictResolved {
                job_id: job.clone(),
                shard_id: ShardId("part-0042".into()),
                winner: wid(),
            },
            EventKind::VerifyStarted {
                job_id: job.clone(),
            },
            EventKind::VerifyFileMismatch {
                job_id: job.clone(),
                path: "/a/b".into(),
                expected: "h1".into(),
                got: "h2".into(),
            },
            EventKind::VerifyCompleted {
                job_id: job.clone(),
                mismatches: 1,
            },
        ];

        for kind in kinds {
            let name = kind.name();
            let env = EventEnvelope {
                seq: 1,
                at: at(),
                schema_version: SCHEMA_VERSION,
                worker_at: None,
                kind,
            };
            let s = serde_json::to_string(&env).unwrap();
            assert!(
                s.contains(&format!("\"kind\":\"{name}\"")),
                "missing kind tag for {name}: {s}",
            );
            let back: EventEnvelope = serde_json::from_str(&s).unwrap();
            assert_eq!(env, back, "round-trip failed for {name}");
        }
    }

    #[test]
    fn event_routes_to_job_or_cluster_log() {
        let job = jid("bobby");
        let job_evt = EventKind::JobPaused {
            job_id: job.clone(),
            reason: "x".into(),
        };
        assert_eq!(job_evt.job_id(), Some(&job));

        let cluster_evt = EventKind::WorkerLeft {
            worker_id: wid(),
            reason: "x".into(),
        };
        assert_eq!(cluster_evt.job_id(), None);
    }

    #[test]
    fn snapshot_round_trip_empty() {
        let snap = Snapshot::empty(at());
        round_trip(snap);
    }

    #[test]
    fn worker_round_trip_with_optional_fields() {
        let w = Worker {
            id: wid(),
            job_id: JobId::new("bobby").unwrap(),
            host: "h".into(),
            pid: 42,
            start_time: at(),
            version: "0.6.0".into(),
            joined_at: at(),
            last_heartbeat: at(),
            state: WorkerState::Copying,
            assigned_shard: Some(ShardId("part-0042".into())),
            queue_depth: 7,
            inflight_ops: 3,
            counters: WorkerCounters {
                files_per_sec: 12.5,
                bytes_per_sec: 1_000_000.0,
                errors_per_min: 0.5,
            },
            last_error: Some("EACCES".into()),
            fence_reason: None,
        };
        round_trip(w);
    }

    /// Schema-version mismatch in either direction must be a hard
    /// signal — newer events refuse to load, older events get the
    /// default version applied silently.
    #[test]
    fn missing_schema_version_defaults_to_current() {
        let s = format!(
            r#"{{"seq":1,"at":"{}","kind":"VerifyStarted","job_id":"bobby"}}"#,
            at().to_rfc3339(),
        );
        let env: EventEnvelope = serde_json::from_str(&s).unwrap();
        assert_eq!(env.schema_version, SCHEMA_VERSION);
    }
}
