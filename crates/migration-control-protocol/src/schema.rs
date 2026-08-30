//! Control-plane wire and snapshot schema.
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
//! There is no separate "in-memory derived state" type; coordinator
//! live state is a [`Snapshot`]. Its pure event reducer lives in
//! [`crate::reducer`]. Persistence and replay orchestration remain in
//! `migration-coord`.
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
/// cleanly with coordinator object-store keys.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(transparent)]
pub struct JobId(pub String);

impl JobId {
    /// Maximum id length in bytes (bytes, not chars — S3 key limits
    /// are byte-based and the id is embedded in every per-job key).
    pub const MAX_BYTES: usize = 128;

    /// Construct a `JobId` after rejecting values that would break the
    /// S3 key layout or operator tooling (F37):
    ///
    /// - empty, and `/` anywhere (wrong-prefix keys);
    /// - the reserved name `_cluster` (its event log would land at
    ///   `events/_cluster/` — the cluster-events prefix);
    /// - `.` / `..` (path-traversal-shaped when keys are mirrored to a
    ///   filesystem);
    /// - ASCII control characters incl. DEL (break log lines, S3 key
    ///   handling, and TUI rendering);
    /// - anything over [`Self::MAX_BYTES`] bytes.
    ///
    /// Everything else — underscores, hyphens, dots inside a longer
    /// id, unicode — is allowed.
    pub fn new(s: impl Into<String>) -> Result<Self, InvalidJobId> {
        let s = s.into();
        if s.is_empty() {
            return Err(InvalidJobId::Empty);
        }
        if s.contains('/') {
            return Err(InvalidJobId::ContainsSlash);
        }
        if s == "_cluster" {
            return Err(InvalidJobId::Reserved);
        }
        if s == "." || s == ".." {
            return Err(InvalidJobId::DotSegment);
        }
        // `is_ascii_control` covers U+0000..=U+001F and DEL (U+007F).
        if s.chars().any(|c| c.is_ascii_control()) {
            return Err(InvalidJobId::ControlChar);
        }
        if s.len() > Self::MAX_BYTES {
            return Err(InvalidJobId::TooLong { len: s.len() });
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
    #[error("job id '_cluster' is reserved (cluster-events prefix)")]
    Reserved,
    #[error("job id may not be '.' or '..'")]
    DotSegment,
    #[error("job id may not contain control characters")]
    ControlChar,
    #[error("job id is {len} bytes; maximum is {max}", max = JobId::MAX_BYTES)]
    TooLong { len: usize },
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

    /// Position in the linear pipeline for active phases; `None` for
    /// `Paused` and the terminal trio. Used by
    /// [`phase_transition_allowed`] to enforce forward-only
    /// progression among active phases.
    fn pipeline_rank(self) -> Option<u8> {
        match self {
            Phase::Planned => Some(0),
            Phase::Scanning => Some(1),
            Phase::Copying => Some(2),
            Phase::Verifying => Some(3),
            Phase::Cutover => Some(4),
            Phase::Paused | Phase::Completed | Phase::Failed | Phase::Cancelled => None,
        }
    }

    /// An active phase is one where work can proceed — neither
    /// `Paused` nor terminal.
    pub fn is_active(self) -> bool {
        self.pipeline_rank().is_some()
    }

    /// `pause`/`drain` are only meaningful for a job that is
    /// currently doing (or about to do) work.
    pub fn can_pause(self) -> bool {
        self.is_active()
    }

    /// `resume` is only meaningful for a paused job — resuming
    /// anything else would rewind the pipeline (ledger F25).
    pub fn can_resume(self) -> bool {
        self == Phase::Paused
    }
}

/// Whether a phase transition is legal (ledger F25). Encodes the
/// rule stated on [`Phase`]: linear (forward-only, skips allowed)
/// progression through the active phases, `Paused` re-entry from and
/// back to any active phase, and the terminal trio reachable from
/// any non-terminal phase but absorbing once entered.
///
/// A same-phase "transition" is not a transition — callers drop it
/// as a no-op before consulting this.
pub fn phase_transition_allowed(from: Phase, to: Phase) -> bool {
    if from == to || from.is_terminal() {
        // Terminal states are absorbing; self-loops are no-ops.
        return false;
    }
    if to.is_terminal() {
        // Cancel/complete/fail is legal from any non-terminal phase.
        return true;
    }
    if to == Phase::Paused {
        return from.is_active();
    }
    if from == Phase::Paused {
        return to.is_active();
    }
    // Both active: forward-only. Skipping phases is allowed (a job
    // can go Planned -> Copying); moving backwards is not.
    match (from.pipeline_rank(), to.pipeline_rank()) {
        (Some(f), Some(t)) => f < t,
        _ => false,
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
#[derive(Default)]
pub enum Health {
    #[default]
    OnTrack,
    AtRisk(Vec<String>),
    Blocked(Vec<String>),
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

/// Immutable job configuration carried in the control-plane snapshot.
/// `claim_version` defaults to `2` (today's claim protocol).
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

/// Hex-encoded hash of the job configuration, supplied by the job creator and
/// carried unchanged by the reducer.
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

/// Per-worker cumulative counters carried by `ProgressSync`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerCum {
    pub worker_id: WorkerId,
    pub files_done: u64,
    pub bytes_done: u64,
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
    /// Cumulative counters accumulated by the reducer from this
    /// worker's ProgressDelta events. Serde-defaulted so pre-field
    /// snapshots load unchanged.
    #[serde(default)]
    pub files_done: u64,
    #[serde(default)]
    pub bytes_done: u64,
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
    /// Latest per-RPC latency window the worker reported in its
    /// heartbeat (source NFS, destination NFS, S3). Heartbeat-only —
    /// never enters the event log; `None` until the first heartbeat
    /// that carries one.
    #[serde(default)]
    pub latency: Option<LatencySummary>,
}

/// One op's latency over a heartbeat window. `side` is `"src"`,
/// `"dst"`, or `"s3"`; `op` is the NFS procedure or S3 call name.
/// Mirrors `migration_core::latency::OpLatency` field for field — the
/// worker converts; this crate stays free of the core dependency.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpLatency {
    pub side: String,
    pub op: String,
    pub count: u64,
    pub mean_us: u64,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub max_us: u64,
    pub total_us: u64,
}

/// A worker's latency window: per-op statistics plus the derived
/// busy share per side — the fraction of `pairs × window` its NFS
/// connection pairs spent waiting on that server — and the share of
/// the window spent inside S3 calls. A side near 100 % is the
/// bottleneck; both low means the client is.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LatencySummary {
    pub window_secs: f64,
    pub pairs: u32,
    pub src_busy_pct: f64,
    pub dst_busy_pct: f64,
    pub s3_wait_pct: f64,
    pub ops: Vec<OpLatency>,
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

/// Cap on distinct error-class buckets per job (ledger F24).
/// `ErrorClass::Other(String)` is free-form, so without a cap one
/// worker emitting distinct strings grows live state, every
/// snapshot, and replay cost without bound. Once a job has this
/// many buckets, further new classes fold into a single catch-all
/// bucket (class `Other(ERROR_OVERFLOW_CLASS)`) — so the bucket
/// vector holds at most `ERROR_BUCKET_CAP + 1` entries. Identities
/// past the cap are dropped; counts stay exact.
pub const ERROR_BUCKET_CAP: usize = 64;

/// Class label of the catch-all bucket new error classes fold into
/// once a job is at [`ERROR_BUCKET_CAP`].
pub const ERROR_OVERFLOW_CLASS: &str = "(overflow)";

/// Cap on `Job.phase_history` entries (ledger F24). Oldest entries
/// are dropped first; resume-target derivation only looks at the
/// most recent entries, so trimming the front is safe. 256 is weeks
/// of pause/resume cycles at human cadence.
pub const PHASE_HISTORY_CAP: usize = 256;

/// Wire cap (COORD_PLAN §3.3, ledger F24): minimum interval between
/// `ProgressDelta` broadcasts on the SSE bus per (job, worker) — the
/// 1 Hz coalescing rule. State and the event log still see every
/// delta; only the bus is capped.
pub const PROGRESS_STREAM_MIN_INTERVAL_MS: i64 = 1000;

/// Wire cap (COORD_PLAN §3.3, ledger F24): maximum `ErrorEmitted`
/// broadcasts per error class per second on the SSE bus. Excess
/// events still reach state (folding into `ErrorBucket.count`) and
/// the event log; only the bus is capped.
pub const ERROR_STREAM_MAX_PER_SEC: u32 = 10;

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
    /// Per-worker idempotency stamp (ledger F20, D4). Present only on
    /// events a stamping worker submitted through the events route.
    /// It rides the durable stream so replay reconstructs the
    /// per-worker high-water mark in [`Snapshot::last_client_seq`] —
    /// an in-memory dedup table would die with the process. Additive
    /// and backward-compatible: old log chunks deserialize as `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_seq: Option<u64>,
    /// Caller attribution for events submitted on the worker events
    /// route (F20 residue): the registered worker id from the URL,
    /// stamped by the coord after the phase-1 trust boundary
    /// validated it. Rides the durable stream so the reducer — live
    /// and on replay — can advance the client_seq high-water mark
    /// even for stamped kinds whose payload carries no caller
    /// attribution (`ClaimConflictDetected`/`Resolved`,
    /// `VerifyFileMismatch` — their worker fields are conflict
    /// roles). `None` on admin/internal ingest paths and on
    /// envelopes from older coords, for which the reducer falls back
    /// to [`EventKind::attributed_worker`], byte-identical to the
    /// old behavior. Additive `#[serde(default)]`, no SCHEMA_VERSION
    /// bump — same precedent as `client_seq` (PR #40).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_worker: Option<WorkerId>,
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
        /// Planned totals for progress display (0 = unknown). Added
        /// for seeded jobs whose manifest is known at creation;
        /// serde-defaulted so historical logs replay unchanged.
        #[serde(default)]
        total_files: u64,
        #[serde(default)]
        total_bytes: u64,
    },
    /// Authoritative cumulative counters, emitted by the coord's
    /// flush tick (~1 Hz per active job). Rates and progress derived
    /// client-side from these ABSOLUTES are immune to ProgressDelta
    /// stream capping/loss — deltas remain for responsiveness, sync
    /// frames are the truth. Carries a fresh seq like any event, so
    /// every monotonic-seq guard passes; the reducer treats it as a
    /// floor (max-merge) so replay stays deterministic.
    ProgressSync {
        job_id: JobId,
        files_done: u64,
        bytes_done: u64,
        workers: Vec<WorkerCum>,
    },
    /// Install or correct a job's planned totals after creation —
    /// the normal case: totals become known when a scan or index
    /// build finishes, which is after JobCreated. Operator-nature;
    /// rejected on the worker events route.
    JobTotalsSet {
        job_id: JobId,
        total_files: u64,
        total_bytes: u64,
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
            Self::JobTotalsSet { .. } => "JobTotalsSet",
            Self::ProgressSync { .. } => "ProgressSync",
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
            | Self::JobTotalsSet { job_id, .. }
            | Self::ProgressSync { job_id, .. }
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

    /// The worker this event is *attributed to* — exactly the kinds
    /// whose payload `worker_id` the events route binds to the caller
    /// (F20 D3). The reducer pairs this with
    /// [`EventEnvelope::client_seq`] to maintain the per-worker
    /// high-water mark in [`Snapshot::last_client_seq`].
    ///
    /// Deliberately `None` for `ClaimConflictDetected` /
    /// `ClaimConflictResolved` / `VerifyFileMismatch`: their worker
    /// fields (`holder`, `contender`, `winner`) are conflict *roles*
    /// naming other parties, not caller attribution. And `None` for
    /// operator/register-path kinds, which never carry a stamp.
    pub fn attributed_worker(&self) -> Option<WorkerId> {
        match self {
            Self::ProgressDelta { worker_id, .. }
            | Self::ErrorEmitted { worker_id, .. }
            | Self::WorkerStateChanged { worker_id, .. }
            | Self::WorkerFenced { worker_id, .. }
            | Self::WorkerRecovered { worker_id } => Some(*worker_id),

            Self::JobCreated { .. }
            | Self::JobTotalsSet { .. }
            | Self::ProgressSync { .. }
            | Self::JobPhaseChanged { .. }
            | Self::JobPaused { .. }
            | Self::JobResumed { .. }
            | Self::JobCancelled { .. }
            | Self::JobCompleted { .. }
            | Self::JobFailed { .. }
            | Self::WorkerJoined { .. }
            | Self::WorkerLeft { .. }
            | Self::ClaimConflictDetected { .. }
            | Self::ClaimConflictResolved { .. }
            | Self::VerifyStarted { .. }
            | Self::VerifyFileMismatch { .. }
            | Self::VerifyCompleted { .. } => None,
        }
    }
}

// =============================================================================
// Snapshot
// =============================================================================

/// Serialized form of the full coord state at a point in time. Loaded
/// at startup, then events with `seq > last_seq` are replayed on top.
///
/// `audit_seq_today` is the per-day audit sequence counter. The
/// snapshot persists it so a restart on the same UTC day usually
/// resumes the numbering, but that is an optimization only — a crash
/// inside the snapshot window rewinds the counter, and the audit
/// writer recovers by allocating keys with `put_if_absent` and
/// skipping past collisions (ledger F22).
///
/// The audit counters and `last_client_seq` are inherited coordinator
/// persistence/replay bookkeeping. They remain here because this exact
/// serialized snapshot is already the version-1 wire and storage contract;
/// this extraction does not endorse them as the ideal long-term client shape.
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
    /// Per-worker `client_seq` high-water mark (ledger F20, D4).
    /// Maintained by the reducer from envelopes carrying worker
    /// attribution plus a `client_seq` stamp, so snapshots and replay
    /// carry it like every other piece of reducer state. The events
    /// route skips entries at or below the caller's mark as
    /// already-applied. Old snapshots deserialize to empty (additive).
    #[serde(default)]
    pub last_client_seq: BTreeMap<WorkerId, u64>,
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
            last_client_seq: BTreeMap::new(),
        }
    }
}

// =============================================================================
// HTTP wire bodies
// =============================================================================

/// Body accepted by `POST /workers/register`.
#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterBody {
    pub job_id: String,
    pub host: String,
    pub pid: u32,
    /// Worker-process start time. Coord pairs `(host, pid, start_time)`
    /// to detect stale registrations across restarts.
    pub start_time: DateTime<Utc>,
    pub version: String,
}

/// Response from `POST /workers/register`.
#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterResponse {
    pub worker_id: WorkerId,
    /// WorkerIds the coord marked Disconnected as a side effect of
    /// this register — any prior workers on `(job_id, host)` whose
    /// `(pid, start_time)` differs from the new registration. The
    /// caller can use this for debugging; clients ignore it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub superseded: Vec<WorkerId>,
}

/// Body accepted by `POST /workers/{id}/heartbeat`.
#[derive(Debug, Serialize, Deserialize)]
pub struct HeartbeatBody {
    pub state: WorkerState,
    #[serde(default)]
    pub files_per_sec: f64,
    #[serde(default)]
    pub bytes_per_sec: f64,
    #[serde(default)]
    pub errors_per_min: f64,
    #[serde(default)]
    pub inflight_ops: u32,
    #[serde(default)]
    pub queue_depth: u32,
    /// Per-RPC latency over the worker's last heartbeat window.
    /// Optional on the wire so older workers keep heartbeating.
    #[serde(default)]
    pub latency: Option<LatencySummary>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ControlEnvelope {
    pub mode: ControlMode,
}

/// Response from `POST /workers/{id}/heartbeat`. The worker reads
/// `control.mode` and flips its local `RunControl` to match on every
/// heartbeat. `last_seq` lets the worker spot a coord restart (a
/// backwards jump is the trigger to flush its event buffer).
/// `server_time` is clock-skew diagnostics only — workers never use
/// it for fence decisions.
#[derive(Debug, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    pub control: ControlEnvelope,
    pub last_seq: u64,
    pub server_time: DateTime<Utc>,
}

/// One entry in a worker event batch.
#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerEventEntry {
    #[serde(flatten)]
    pub kind: EventKind,
    #[serde(default)]
    pub worker_at: Option<DateTime<Utc>>,
    /// Per-worker idempotency stamp (ledger F20, D4). Absent means a
    /// pre-upgrade worker: the entry keeps at-least-once semantics
    /// (applied on every send). Present means the coord dedups it
    /// against the caller's replay-durable high-water mark.
    #[serde(default)]
    pub client_seq: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EventsBatchBody {
    pub events: Vec<WorkerEventEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EventsBatchResponse {
    /// Seqs assigned to the APPLIED entries, in batch order.
    /// `seqs.len() + deduped == events.len()`. For a worker that
    /// does not stamp `client_seq` nothing is ever deduped, so this
    /// keeps its original shape (one seq per entry). This is the
    /// ledger F20 D4 response-shape choice.
    pub seqs: Vec<u64>,
    /// Entries skipped as already-applied (`client_seq` at or below
    /// the caller's high-water mark). The whole batch is settled
    /// either way — the worker drops its resend buffer on any 200.
    #[serde(default)]
    pub deduped: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FenceBody {
    pub reason: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FenceResponse {
    pub seq: u64,
}

/// `POST /workers/{id}/leave` — a worker announcing an orderly exit
/// (all shards terminal, `systemctl stop`, coord-requested drain).
/// The coord emits `WorkerLeft{reason}` so the worker reads as
/// Disconnected at once instead of after the liveness timeout.
#[derive(Debug, Serialize, Deserialize)]
pub struct LeaveBody {
    pub reason: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LeaveResponse {
    pub seq: u64,
}

/// Query parameters for `GET /stream`.
#[derive(Debug, Deserialize)]
pub struct StreamParams {
    /// Per-job filter. `None` selects the cluster-wide stream.
    pub job_id: Option<String>,
}

/// Query parameters for paginated job reads.
#[derive(Debug, Deserialize)]
pub struct ListJobsParams {
    /// `JobId` from a previous response's `next_cursor` (excluded
    /// from this page).
    pub cursor: Option<String>,
    /// Page size. Default 50, capped at 500.
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListJobsResponse {
    pub jobs: Vec<Job>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListWorkersResponse {
    pub workers: Vec<Worker>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListErrorsResponse {
    pub buckets: Vec<ErrorBucket>,
}

/// Query parameters for event-log reads.
#[derive(Debug, Deserialize)]
pub struct ListEventsParams {
    /// Lower bound (exclusive). Default 0.
    pub since: Option<u64>,
    /// Max events to return. Default 200, capped at 1000.
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListEventsResponse {
    pub events: Vec<EventEnvelope>,
    /// Seq of the last event returned (or the original `since` if
    /// the page was empty). Use as `since` on the next request.
    pub next_since: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HealthzResponse {
    pub status: String,
    pub last_seq: u64,
    pub subscriber_count: usize,
    pub lease_lost: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CommandAccepted {
    pub command_id: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ReasonBody {
    /// Optional human-readable reason. Defaults to `"operator"` in
    /// the audit row and phase history if not supplied.
    #[serde(default)]
    pub reason: Option<String>,
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

// =============================================================================
// Prepare progress
// =============================================================================

/// Where `vamoose prepare` is, as published by the preparing host to
/// `prepare/progress.json` in the bucket and served by the coord at
/// `GET /prepare`. Not an event: it precedes the job (the manifest it
/// ends with is what seeds the job) and is overwritten in place.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PrepareProgress {
    #[serde(default = "prepare_progress_schema_version")]
    pub schema_version: u32,
    pub run_id: String,
    /// Host running `prepare`, and its pid, so a stale object from a
    /// prepare that died is recognisable (`updated_utc` stops moving).
    pub host: String,
    pub pid: u32,
    pub source: String,
    pub dest: String,
    pub phase: PreparePhase,
    pub started_utc: DateTime<Utc>,
    pub updated_utc: DateTime<Utc>,
    pub scan: PrepareScan,
    pub index: PrepareIndex,
    /// Failure text when `phase == Failed`.
    #[serde(default)]
    pub message: Option<String>,
}

pub const PREPARE_PROGRESS_SCHEMA_VERSION: u32 = 1;

fn prepare_progress_schema_version() -> u32 {
    PREPARE_PROGRESS_SCHEMA_VERSION
}

/// The three stages of `prepare`, then how it ended.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PreparePhase {
    /// nfs-walker is scanning the source.
    Scan,
    /// The canonical rewrite is producing shards; each is uploaded as
    /// it lands.
    Index,
    /// Verifying the index and creating `manifest.json`.
    Publish,
    /// `manifest.json` is in the bucket; the run has started.
    Done,
    /// `prepare` exited with an error; see `message`. Re-running it
    /// resumes.
    Failed,
}

impl PreparePhase {
    pub fn as_str(self) -> &'static str {
        match self {
            PreparePhase::Scan => "scan",
            PreparePhase::Index => "index",
            PreparePhase::Publish => "publish",
            PreparePhase::Done => "done",
            PreparePhase::Failed => "failed",
        }
    }
}

/// Scan counters, from nfs-walker's own progress log.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct PrepareScan {
    pub files: u64,
    pub dirs: u64,
    pub errors: u64,
    pub rate_per_sec: u64,
    pub elapsed_secs: u64,
    pub complete: bool,
}

/// Index (rewrite + upload) counters.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct PrepareIndex {
    /// Shards the scan produced (known once the scan is complete).
    pub shards_total: Option<u64>,
    pub shards_rewritten: u64,
    pub shards_uploaded: u64,
    /// Rows in the uploaded shards.
    pub rows_uploaded: u64,
    pub bytes_uploaded: u64,
}

/// `GET /prepare`: the latest progress the coord has read from the
/// bucket, and how long ago the preparing host wrote it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PrepareResponse {
    pub progress: Option<PrepareProgress>,
    /// Seconds since `progress.updated_utc`, by the coord's clock.
    pub age_secs: Option<u64>,
}

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

    // ------------------------------------------------------------------
    // F37: tightened JobId validation
    // ------------------------------------------------------------------

    /// `_cluster` is reserved: a job named `_cluster` would put its
    /// event log at `events/_cluster/` — the cluster-events prefix
    /// (`layout::CLUSTER_EVENTS_PREFIX`).
    #[test]
    fn job_id_rejects_reserved_cluster() {
        assert!(JobId::new("_cluster").is_err());
        // Only the exact reserved name — other underscore ids are
        // fine (nothing else collides in the layout).
        assert!(JobId::new("_clusterish").is_ok());
        assert!(JobId::new("my_cluster").is_ok());
    }

    /// `.` / `..` are path-traversal-shaped in S3 keys and confuse
    /// every tool that mirrors keys onto a filesystem.
    #[test]
    fn job_id_rejects_dot_segments() {
        assert!(JobId::new(".").is_err());
        assert!(JobId::new("..").is_err());
        // Dots inside a longer id stay legal.
        assert!(JobId::new("v1.2-migration").is_ok());
        assert!(JobId::new("..almost").is_ok());
    }

    /// Control characters break log lines, S3 key handling, and TUI
    /// rendering.
    #[test]
    fn job_id_rejects_control_chars() {
        assert!(JobId::new("bad\nid").is_err());
        assert!(JobId::new("bad\tid").is_err());
        assert!(JobId::new("bad\0id").is_err());
        assert!(JobId::new("bad\x1bid").is_err());
        assert!(JobId::new("del\u{7f}id").is_err());
    }

    /// Unbounded ids blow up key lengths and UI columns; cap at 128
    /// bytes (bytes, not chars — S3 key limits are byte-based).
    #[test]
    fn job_id_rejects_over_128_bytes() {
        assert!(JobId::new("a".repeat(128)).is_ok());
        assert!(JobId::new("a".repeat(129)).is_err());
        // Multi-byte: 43 × "マ" (3 bytes each) = 129 bytes.
        assert!(JobId::new("マ".repeat(43)).is_err());
    }

    /// Every id style in use across the codebase's tests, fixtures,
    /// and docs stays accepted.
    #[test]
    fn job_id_existing_valid_ids_still_accepted() {
        for id in [
            "bobby",
            "bobby-migration",
            "bobby-mig",
            "test-bobby",
            "test-archive",
            "alpha",
            "job-1",
            "マイグレ",
            "my_job",
        ] {
            assert!(JobId::new(id).is_ok(), "{id:?} must stay valid");
        }
    }

    /// Full legality matrix for [`phase_transition_allowed`] (ledger
    /// F25): Paused re-entry from active phases only, forward-only
    /// progression through the pipeline, terminal trio reachable from
    /// any non-terminal phase and absorbing once entered.
    #[test]
    fn legal_matrix_table() {
        use Phase::*;
        const ALL: [Phase; 9] = [
            Planned, Scanning, Copying, Verifying, Cutover, Paused, Completed, Failed, Cancelled,
        ];
        let active = [Planned, Scanning, Copying, Verifying, Cutover];
        let terminal = [Completed, Failed, Cancelled];

        for from in ALL {
            for to in ALL {
                let got = phase_transition_allowed(from, to);
                let want = if from == to || terminal.contains(&from) {
                    // Self-loops are no-ops; terminal is absorbing.
                    false
                } else if terminal.contains(&to) {
                    // Any non-terminal job can be cancelled/completed/
                    // failed.
                    true
                } else if to == Paused {
                    // Pause re-entry from active phases only.
                    active.contains(&from)
                } else if from == Paused {
                    // Resume lands on any active phase (the prior one).
                    active.contains(&to)
                } else {
                    // Active -> active: forward-only, skips allowed.
                    let rank = |p: Phase| active.iter().position(|&a| a == p).unwrap();
                    rank(from) < rank(to)
                };
                assert_eq!(
                    got, want,
                    "phase_transition_allowed({from:?}, {to:?}) should be {want}",
                );
            }
        }

        // Predicate helpers agree with the matrix.
        for p in ALL {
            assert_eq!(p.can_pause(), active.contains(&p), "can_pause({p:?})");
            assert_eq!(p.can_resume(), p == Paused, "can_resume({p:?})");
            assert_eq!(p.is_terminal(), terminal.contains(&p));
            assert_eq!(p.is_active(), active.contains(&p));
        }
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
            client_seq: None,
            from_worker: None,
            kind: EventKind::JobCreated {
                job_id: jid("bobby"),
                name: "bobby-migration".into(),
                source: "nfs://src".into(),
                dest: "nfs://dst".into(),
                owner: "blake".into(),
                config_hash: ConfigHash("deadbeef".into()),
                total_files: 0,
                total_bytes: 0,
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
                total_files: 0,
                total_bytes: 0,
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
                client_seq: None,
                from_worker: None,
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
            files_done: 0,
            bytes_done: 0,
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
            latency: None,
        };
        round_trip(w);
    }

    /// Schema-version mismatch in either direction must be a hard
    /// signal — newer events refuse to load, older events get the
    /// default version applied silently.
    #[test]
    fn prepare_progress_round_trips_and_defaults_its_version() {
        let p = PrepareProgress {
            schema_version: PREPARE_PROGRESS_SCHEMA_VERSION,
            run_id: "run-1".into(),
            host: "h".into(),
            pid: 7,
            source: "nfs://s/x".into(),
            dest: "nfs://d/y/v3".into(),
            phase: PreparePhase::Index,
            started_utc: at(),
            updated_utc: at(),
            scan: PrepareScan {
                files: 10,
                dirs: 2,
                errors: 0,
                rate_per_sec: 5,
                elapsed_secs: 2,
                complete: true,
            },
            index: PrepareIndex {
                shards_total: Some(4),
                shards_rewritten: 2,
                shards_uploaded: 1,
                rows_uploaded: 3,
                bytes_uploaded: 100,
            },
            message: None,
        };
        let json = serde_json::to_string(&p).unwrap();
        assert!(json.contains("\"phase\":\"index\""), "{json}");
        assert_eq!(serde_json::from_str::<PrepareProgress>(&json).unwrap(), p);
        // An object written without the version field is version 1.
        let mut v: serde_json::Value = serde_json::from_str(&json).unwrap();
        v.as_object_mut().unwrap().remove("schema_version");
        let back: PrepareProgress = serde_json::from_value(v).unwrap();
        assert_eq!(back.schema_version, 1);
        assert_eq!(PreparePhase::Publish.as_str(), "publish");
    }

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
