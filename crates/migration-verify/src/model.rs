use migration_core::records::{Endpoint, MigrationOptions};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;

pub const REPORT_SCHEMA_VERSION: u32 = 2;

/// Identifier of the deterministic sample-ranking algorithm recorded in every
/// sampled report so the selection can be reproduced exactly.
pub const SAMPLE_ALGORITHM: &str = "sha256-smallest-v1";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VerificationMode {
    /// Complete namespace and metadata comparison; no content reads.
    Metadata,
    /// Complete metadata comparison plus SHA-256 of a deterministic sample and
    /// every mandatory-risk regular file.
    Sample,
}

impl VerificationMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Metadata => "metadata",
            Self::Sample => "sample",
        }
    }
}

/// Closed V2 set of reasons a regular-file pair enters the content sample.
/// The declaration order is the deterministic order reasons are reported in
/// and the bit order they are persisted with; append only.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum RiskReason {
    /// Any V1 comparison difference on an eligible regular-file pair.
    MetadataMismatch,
    /// Source size is exactly one below, at, or one above a mover bucket
    /// threshold.
    BucketBoundary,
    /// Smallest eligible member of a detected source hardlink group.
    HardlinkGroup,
    /// Path appeared in a migration failure record.
    MigrationFailure,
    /// Path appeared in a migration downgrade record.
    MigrationDowngrade,
    /// Regular file belonging to a shard whose terminal claim had epoch > 1.
    RetriedShard,
    /// Selected from the eligible remainder by seeded rank.
    Seeded,
}

impl RiskReason {
    pub const ALL: [RiskReason; 7] = [
        Self::MetadataMismatch,
        Self::BucketBoundary,
        Self::HardlinkGroup,
        Self::MigrationFailure,
        Self::MigrationDowngrade,
        Self::RetriedShard,
        Self::Seeded,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::MetadataMismatch => "metadata_mismatch",
            Self::BucketBoundary => "bucket_boundary",
            Self::HardlinkGroup => "hardlink_group",
            Self::MigrationFailure => "migration_failure",
            Self::MigrationDowngrade => "migration_downgrade",
            Self::RetriedShard => "retried_shard",
            Self::Seeded => "seeded",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|reason| reason.as_str() == value)
    }

    /// Reasons that originate from migration history rather than from the
    /// verifier's own scans. Only these may appear in a risk-evidence artifact.
    pub fn is_history(self) -> bool {
        matches!(
            self,
            Self::MigrationFailure | Self::MigrationDowngrade | Self::RetriedShard
        )
    }

    pub(crate) fn bit(self) -> u32 {
        1 << (self as u32)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConsistencyBoundary {
    pub writers_stopped: bool,
    pub source_snapshot_id: Option<String>,
    pub destination_snapshot_id: Option<String>,
}

impl ConsistencyBoundary {
    pub fn validate(&self) -> anyhow::Result<()> {
        for (label, value) in [
            ("source", self.source_snapshot_id.as_deref()),
            ("destination", self.destination_snapshot_id.as_deref()),
        ] {
            if value.is_some_and(|id| id.trim().is_empty()) {
                anyhow::bail!("{label} snapshot identifier cannot be empty");
            }
        }
        let snapshots = self.source_snapshot_id.is_some() && self.destination_snapshot_id.is_some();
        let partial_snapshot =
            self.source_snapshot_id.is_some() ^ self.destination_snapshot_id.is_some();
        if partial_snapshot {
            anyhow::bail!("source and destination snapshot identifiers must be supplied together");
        }
        if !self.writers_stopped && !snapshots {
            anyhow::bail!(
                "verification requires --writers-stopped or both source and destination snapshot identifiers"
            );
        }
        Ok(())
    }
}

/// Sample-mode inputs. Every field is part of the request fingerprint, so a
/// resumed verification cannot silently change its selection or its content
/// read policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SampleRequest {
    /// Target total number of content jobs; mandatory risk may exceed it.
    pub sample_files: u64,
    pub sample_seed: u64,
    /// Long-lived blocking workers, each owning one fresh source and one
    /// fresh destination libnfs context.
    pub content_workers: u32,
    /// Bytes per positional read; `1..=i32::MAX`.
    pub content_read_size: u32,
    /// Canonical, already-published `risk-evidence.jsonl` inside `work_dir`.
    /// The digest, not the path, identifies the request.
    pub risk_evidence: ArtifactDigest,
    /// The operator asserted that the run has no failure, downgrade, or
    /// retry history instead of supplying evidence.
    pub risk_history_asserted_empty: bool,
}

#[derive(Debug, Clone)]
pub struct VerificationRequest {
    pub verification_id: String,
    pub run_id: String,
    pub source: Endpoint,
    pub destination: Endpoint,
    pub options: MigrationOptions,
    pub exclusions: Vec<String>,
    pub consistency: ConsistencyBoundary,
    /// Directory containing the resumable observation database.
    pub work_dir: PathBuf,
    pub report_path: PathBuf,
    pub mismatch_path: PathBuf,
    pub rpc_timeout_ms: u32,
    pub directory_batch_size: u32,
    pub mode: VerificationMode,
    /// Required exactly when `mode` is [`VerificationMode::Sample`].
    pub sample: Option<SampleRequest>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VerificationStatus {
    Passed,
    Mismatched,
    Failed,
    Inconclusive,
}

impl VerificationStatus {
    pub fn exit_code(self) -> i32 {
        match self {
            Self::Passed => 0,
            Self::Failed => 1,
            Self::Mismatched => 2,
            Self::Inconclusive => 3,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum MismatchKind {
    MissingDestination,
    UnexpectedDestination,
    Type,
    Size,
    Content,
    Mode,
    Owner,
    Mtime,
    SymlinkTarget,
    HardlinkGroup,
    UnsupportedFeature,
    UnreadableSource,
    UnreadableDestination,
    UnstableSource,
    UnstableDestination,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MismatchRecord {
    pub schema_version: u32,
    pub verification_id: String,
    pub run_id: String,
    pub path_b64: String,
    pub kind: MismatchKind,
    pub expected: Value,
    pub observed: Value,
    pub source_observation: Option<ObservedMetadata>,
    pub destination_observation: Option<ObservedMetadata>,
    pub worker_id: Option<String>,
    pub shard_id: String,
    pub observed_utc: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum FileType {
    Regular,
    Directory,
    Symlink,
    Fifo,
    Socket,
    BlockDevice,
    CharacterDevice,
    Unknown,
}

impl FileType {
    pub(crate) fn as_i64(self) -> i64 {
        match self {
            Self::Unknown => 0,
            Self::Regular => 1,
            Self::Directory => 2,
            Self::Symlink => 3,
            Self::Fifo => 4,
            Self::Socket => 5,
            Self::BlockDevice => 6,
            Self::CharacterDevice => 7,
        }
    }

    pub(crate) fn from_i64(value: i64) -> Self {
        match value {
            1 => Self::Regular,
            2 => Self::Directory,
            3 => Self::Symlink,
            4 => Self::Fifo,
            5 => Self::Socket,
            6 => Self::BlockDevice,
            7 => Self::CharacterDevice,
            _ => Self::Unknown,
        }
    }

    pub(crate) fn is_unsupported_v1(self) -> bool {
        matches!(
            self,
            Self::Fifo | Self::Socket | Self::BlockDevice | Self::CharacterDevice | Self::Unknown
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObservedMetadata {
    pub file_type: FileType,
    pub size: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub mtime_sec: i64,
    pub mtime_nsec: u32,
    pub symlink_target_b64: Option<String>,
    pub hardlink_group_b64: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ComparisonPolicy {
    pub namespace: bool,
    pub file_type: bool,
    pub regular_file_size: bool,
    pub mode: bool,
    pub owner: bool,
    pub mtime_microsecond_precision: bool,
    pub symlink_target: bool,
    pub hardlink_equivalence: bool,
    pub content: bool,
    pub xattrs: bool,
    pub acls: bool,
    pub sparse_extents: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ObservationCounts {
    pub source_entries: u64,
    pub destination_entries: u64,
    pub entries_on_both_sides: u64,
    pub source_by_type: BTreeMap<String, u64>,
    pub destination_by_type: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactDigest {
    pub path: String,
    pub sha256: String,
    pub bytes: u64,
}

/// How the content sample was chosen. Present only for sample mode. Every
/// path count is a unique-path count; `selection_reasons` counts selected
/// paths carrying each reason, so its values need not sum to
/// `selected_files`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SamplePolicyReport {
    pub algorithm: String,
    pub seed: u64,
    pub requested_files: u64,
    pub content_workers: u32,
    pub content_read_size: u32,
    pub eligible_files: u64,
    pub selected_files: u64,
    /// Unique mandatory targets before eligibility filtering; partitioned by
    /// `risk_selected_files` and `risk_ineligible_files`.
    pub risk_candidates: u64,
    pub risk_selected_files: u64,
    pub risk_ineligible_files: u64,
    pub seeded_selected_files: u64,
    pub selection_reasons: BTreeMap<RiskReason, u64>,
    pub risk_evidence_artifact: ArtifactDigest,
    pub risk_history_asserted_empty: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VerificationReport {
    pub schema_version: u32,
    pub verification_id: String,
    pub run_id: String,
    pub request_fingerprint_sha256: String,
    pub software_version: String,
    pub source: Endpoint,
    pub destination: Endpoint,
    pub consistency: ConsistencyBoundary,
    pub exclusions: Vec<String>,
    pub mode: VerificationMode,
    pub comparison_policy: ComparisonPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_policy: Option<SamplePolicyReport>,
    pub started_utc: String,
    pub completed_utc: String,
    pub resumed: bool,
    pub counts: ObservationCounts,
    /// False only when an operational failure stopped requested content work
    /// before every selected job was terminal.
    pub content_complete: bool,
    pub source_files_hashed: u64,
    pub source_logical_bytes_hashed: u64,
    pub destination_files_hashed: u64,
    pub destination_logical_bytes_hashed: u64,
    pub content_matches: u64,
    pub content_mismatches: u64,
    /// Number of mismatch JSONL records, including unreadable and unstable
    /// records.
    pub mismatch_count: u64,
    pub mismatches_by_kind: BTreeMap<MismatchKind, u64>,
    /// Unique `(side, path)` pairs across the scan and content phases.
    pub unreadable_entries: u64,
    /// Unique `(side, path)` pairs across the scan and content phases.
    pub unstable_entries: u64,
    pub operational_errors: Vec<String>,
    pub mismatch_artifact: ArtifactDigest,
    pub status: VerificationStatus,
}

#[derive(Debug, Clone)]
pub struct VerificationResult {
    pub report: VerificationReport,
    pub report_path: PathBuf,
}
