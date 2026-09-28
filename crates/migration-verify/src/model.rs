use migration_core::records::{Endpoint, MigrationOptions};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;

pub const REPORT_SCHEMA_VERSION: u32 = 1;

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
    pub mode: String,
    pub comparison_policy: ComparisonPolicy,
    pub started_utc: String,
    pub completed_utc: String,
    pub resumed: bool,
    pub counts: ObservationCounts,
    pub mismatch_count: u64,
    pub mismatches_by_kind: BTreeMap<MismatchKind, u64>,
    pub unreadable_entries: u64,
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
