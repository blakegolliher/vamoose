//! Independent Vamoose verification.
//!
//! V1 performs fresh libnfs scans of both exports, checkpoints observations in
//! SQLite, compares paths in byte order, and publishes immutable JSON/JSONL
//! artifacts. It intentionally does not trust the migration index or mover
//! outcomes as proof.

mod artifact;
mod model;
mod risk;
mod sample;
mod scanner;
mod store;

pub use model::{
    ArtifactDigest, ComparisonPolicy, ConsistencyBoundary, FileType, MismatchKind, MismatchRecord,
    ObservationCounts, ObservedMetadata, RiskReason, SamplePolicyReport, SampleRequest,
    VerificationMode, VerificationReport, VerificationRequest, VerificationResult,
    VerificationStatus, REPORT_SCHEMA_VERSION, SAMPLE_ALGORITHM,
};
pub use risk::{
    risk_evidence_path, stage_asserted_empty, stage_local_artifact,
    validate_artifact as validate_risk_artifact, RiskEvidenceRecord, RiskEvidenceStager,
    RISK_EVIDENCE_FILENAME,
};

use anyhow::{Context, Result};
use artifact::{create_temp, digest_file, ensure_parent, publish_existing_temp, publish_json};
use base64::Engine;
use migration_core::records::{EndpointKind, MigrationOptions};
use sample::Ranker;
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use store::{Entry, ObservationIssue, Side, Store};

pub fn verify(request: VerificationRequest) -> Result<VerificationResult> {
    validate_request(&request)?;
    let request_fingerprint = request_fingerprint(&request)?;
    if let Some(report) = load_terminal_report(&request, &request_fingerprint)? {
        return Ok(VerificationResult {
            report,
            report_path: request.report_path,
        });
    }

    std::fs::create_dir_all(&request.work_dir)
        .with_context(|| format!("creating {}", request.work_dir.display()))?;
    ensure_parent(&request.report_path)?;
    ensure_parent(&request.mismatch_path)?;
    let _lock = VerificationLock::acquire(&request.work_dir.join("verification.lock"))?;
    if let Some(report) = load_terminal_report(&request, &request_fingerprint)? {
        return Ok(VerificationResult {
            report,
            report_path: request.report_path,
        });
    }
    let exclusions = scanner::compile_exclusions(&request.exclusions)?;
    let started_now = chrono::Utc::now().to_rfc3339();
    let database_path = request.work_dir.join("observations.sqlite");
    let (mut store, resumed) = Store::open(&database_path, &request_fingerprint, &started_now)?;

    scanner::scan_side(
        &mut store,
        Side::Source,
        &request.source,
        request.rpc_timeout_ms,
        request.directory_batch_size,
        &exclusions,
    )?;
    scanner::scan_side(
        &mut store,
        Side::Destination,
        &request.destination,
        request.rpc_timeout_ms,
        request.directory_batch_size,
        &exclusions,
    )?;
    store.materialize_hardlinks()?;
    if let Some(sample) = request.sample.as_ref() {
        prepare_selection(&mut store, &request, sample)?;
    }

    // Mismatch rows must be byte-for-byte reproducible after a crash between
    // publishing the JSONL and publishing the terminal report.
    let observed_utc = store.started_utc()?;
    let mut emitter = MismatchEmitter::new(&request, &observed_utc)?;
    let issues = store.issues()?;
    for issue in &issues {
        emitter.emit(issue_record(&request, issue, &observed_utc))?;
    }
    let unstable = store.unstable()?;
    for (side, path, detail) in &unstable {
        emitter.emit(MismatchRecord {
            schema_version: REPORT_SCHEMA_VERSION,
            verification_id: request.verification_id.clone(),
            run_id: request.run_id.clone(),
            path_b64: path_b64(path),
            kind: match side {
                Side::Source => MismatchKind::UnstableSource,
                Side::Destination => MismatchKind::UnstableDestination,
            },
            expected: json!("stable directory during enumeration"),
            observed: json!(detail),
            source_observation: None,
            destination_observation: None,
            worker_id: None,
            shard_id: "metadata-v1".to_string(),
            observed_utc: observed_utc.clone(),
        })?;
    }

    store.for_each_joined(|source, destination| {
        emit_entry_differences(&request, &mut emitter, source, destination, &observed_utc)
            .map(|_| ())
    })?;
    let (mismatch_artifact, mismatch_count, mismatches_by_kind) = emitter.finish()?;
    let counts = store.counts()?;
    let mut operational_errors: Vec<String> = issues
        .iter()
        .take(100)
        .map(|issue| {
            format!(
                "{} {} {}: {}",
                issue.side.label(),
                issue.operation,
                display_path(&issue.path),
                issue.detail
            )
        })
        .collect();
    let (sample_policy, content_complete) = match request.sample.as_ref() {
        Some(sample) => {
            operational_errors
                .push("sampled content hashing is not available in this build".to_string());
            (
                Some(sample_policy_report(sample, &store.selection_summary()?)),
                false,
            )
        }
        None => (None, true),
    };
    let status = if !issues.is_empty() || !content_complete {
        VerificationStatus::Failed
    } else if !unstable.is_empty() {
        VerificationStatus::Inconclusive
    } else if mismatch_count != 0 {
        VerificationStatus::Mismatched
    } else {
        VerificationStatus::Passed
    };
    let unreadable_entries = issues
        .iter()
        .map(|issue| (issue.side as u8, issue.path.as_slice()))
        .collect::<BTreeSet<_>>()
        .len() as u64;
    let report = VerificationReport {
        schema_version: REPORT_SCHEMA_VERSION,
        verification_id: request.verification_id.clone(),
        run_id: request.run_id.clone(),
        request_fingerprint_sha256: request_fingerprint,
        software_version: env!("CARGO_PKG_VERSION").to_string(),
        source: request.source.clone(),
        destination: request.destination.clone(),
        consistency: request.consistency.clone(),
        exclusions: request.exclusions.clone(),
        mode: request.mode,
        comparison_policy: ComparisonPolicy {
            namespace: true,
            file_type: true,
            regular_file_size: true,
            mode: request.options.preserve_mode,
            owner: request.options.preserve_owner,
            mtime_microsecond_precision: request.options.preserve_times,
            symlink_target: true,
            hardlink_equivalence: true,
            content: false,
            xattrs: false,
            acls: false,
            sparse_extents: false,
        },
        sample_policy,
        started_utc: store.started_utc()?,
        completed_utc: chrono::Utc::now().to_rfc3339(),
        resumed,
        counts,
        content_complete,
        source_files_hashed: 0,
        source_logical_bytes_hashed: 0,
        destination_files_hashed: 0,
        destination_logical_bytes_hashed: 0,
        content_matches: 0,
        content_mismatches: 0,
        mismatch_count,
        mismatches_by_kind,
        unreadable_entries,
        unstable_entries: unstable.len() as u64,
        operational_errors,
        mismatch_artifact,
        status,
    };
    publish_json(&request.report_path, &report)?;
    Ok(VerificationResult {
        report,
        report_path: request.report_path,
    })
}

fn validate_request(request: &VerificationRequest) -> Result<()> {
    request.consistency.validate()?;
    if request.verification_id.trim().is_empty() {
        anyhow::bail!("verification id cannot be empty");
    }
    if request.run_id.trim().is_empty() {
        anyhow::bail!("run id cannot be empty");
    }
    if request.directory_batch_size == 0 {
        anyhow::bail!("directory batch size must be greater than zero");
    }
    if request.rpc_timeout_ms == 0 {
        anyhow::bail!("RPC timeout must be greater than zero");
    }
    for (label, endpoint) in [
        ("source", &request.source),
        ("destination", &request.destination),
    ] {
        if endpoint.kind != EndpointKind::Nfs {
            anyhow::bail!("{label} endpoint must be NFS for verification V1");
        }
        if !endpoint.root.starts_with('/') {
            anyhow::bail!("{label} root must be absolute: {:?}", endpoint.root);
        }
        if endpoint.root.as_bytes().contains(&0) {
            anyhow::bail!("{label} root contains NUL");
        }
        if endpoint
            .root
            .split('/')
            .any(|component| component == "." || component == "..")
        {
            anyhow::bail!(
                "{label} root contains a dot path component: {:?}",
                endpoint.root
            );
        }
    }
    if request.report_path == request.mismatch_path {
        anyhow::bail!("report and mismatch paths must be different");
    }
    match (request.mode, request.sample.as_ref()) {
        (VerificationMode::Metadata, None) => {}
        (VerificationMode::Metadata, Some(_)) => {
            anyhow::bail!("metadata mode does not accept sample options")
        }
        (VerificationMode::Sample, None) => anyhow::bail!("sample mode requires sample options"),
        (VerificationMode::Sample, Some(sample)) => {
            if sample.sample_files == 0 {
                anyhow::bail!("sample file count must be greater than zero");
            }
            if sample.content_workers == 0 {
                anyhow::bail!("content workers must be greater than zero");
            }
            if sample.content_read_size == 0 || sample.content_read_size > i32::MAX as u32 {
                anyhow::bail!("content read size must be between 1 and {} bytes", i32::MAX);
            }
            let expected = risk::risk_evidence_path(&request.work_dir);
            if Path::new(&sample.risk_evidence.path) != expected {
                anyhow::bail!(
                    "risk evidence artifact must be {}, got {}",
                    expected.display(),
                    sample.risk_evidence.path
                );
            }
            if sample.risk_history_asserted_empty && sample.risk_evidence.bytes != 0 {
                anyhow::bail!("an asserted-empty risk history cannot carry evidence lines");
            }
        }
    }
    migration_core::overlap::check(&request.source, &request.destination)
        .context("verification source and destination must be independent")?;
    Ok(())
}

fn sample_policy_report(
    sample: &SampleRequest,
    summary: &store::SelectionSummary,
) -> SamplePolicyReport {
    SamplePolicyReport {
        algorithm: SAMPLE_ALGORITHM.to_string(),
        seed: sample.sample_seed,
        requested_files: sample.sample_files,
        content_workers: sample.content_workers,
        content_read_size: sample.content_read_size,
        eligible_files: summary.eligible_files,
        selected_files: summary.selected_files,
        risk_candidates: summary.risk_selected_files + summary.risk_ineligible_files,
        risk_selected_files: summary.risk_selected_files,
        risk_ineligible_files: summary.risk_ineligible_files,
        seeded_selected_files: summary.seeded_selected_files,
        selection_reasons: summary.reasons.clone(),
        risk_evidence_artifact: sample.risk_evidence.clone(),
        risk_history_asserted_empty: sample.risk_history_asserted_empty,
    }
}

/// Validates the risk artifact, imports it, and builds the content selection
/// unless a complete selection is already committed for this request.
fn prepare_selection(
    store: &mut Store,
    request: &VerificationRequest,
    sample: &SampleRequest,
) -> Result<()> {
    risk::validate_artifact(&sample.risk_evidence)?;
    if store.selection_complete()? {
        return Ok(());
    }
    store.import_risk_paths(Path::new(&sample.risk_evidence.path))?;
    let ranker = Ranker::new(&request.run_id, sample.sample_seed);
    store.build_selection(sample.sample_files, &ranker, |source, destination| {
        !compare_entries(request, Some(source), Some(destination)).is_empty()
    })
}

#[derive(Serialize)]
struct Identity<'a> {
    schema_version: u32,
    verification_id: &'a str,
    run_id: &'a str,
    source: &'a migration_core::records::Endpoint,
    destination: &'a migration_core::records::Endpoint,
    options: &'a MigrationOptions,
    exclusions: &'a [String],
    consistency: &'a ConsistencyBoundary,
    rpc_timeout_ms: u32,
    directory_batch_size: u32,
    mode: VerificationMode,
    sample: Option<SampleIdentity<'a>>,
}

/// Sample inputs that identify a request. The risk artifact contributes its
/// digest, never its filesystem path.
#[derive(Serialize)]
struct SampleIdentity<'a> {
    sample_files: u64,
    sample_seed: u64,
    content_workers: u32,
    content_read_size: u32,
    risk_evidence_sha256: &'a str,
    risk_evidence_bytes: u64,
    risk_history_asserted_empty: bool,
}

fn request_fingerprint(request: &VerificationRequest) -> Result<String> {
    let identity = serde_json::to_vec(&Identity {
        schema_version: REPORT_SCHEMA_VERSION,
        verification_id: &request.verification_id,
        run_id: &request.run_id,
        source: &request.source,
        destination: &request.destination,
        options: &request.options,
        exclusions: &request.exclusions,
        consistency: &request.consistency,
        rpc_timeout_ms: request.rpc_timeout_ms,
        directory_batch_size: request.directory_batch_size,
        mode: request.mode,
        sample: request.sample.as_ref().map(|sample| SampleIdentity {
            sample_files: sample.sample_files,
            sample_seed: sample.sample_seed,
            content_workers: sample.content_workers,
            content_read_size: sample.content_read_size,
            risk_evidence_sha256: &sample.risk_evidence.sha256,
            risk_evidence_bytes: sample.risk_evidence.bytes,
            risk_history_asserted_empty: sample.risk_history_asserted_empty,
        }),
    })?;
    Ok(hex::encode(Sha256::digest(identity)))
}

fn load_terminal_report(
    request: &VerificationRequest,
    request_fingerprint: &str,
) -> Result<Option<VerificationReport>> {
    if !request.report_path.exists() {
        return Ok(None);
    }
    let report: VerificationReport = serde_json::from_slice(
        &std::fs::read(&request.report_path)
            .with_context(|| format!("reading {}", request.report_path.display()))?,
    )
    .with_context(|| format!("parsing {}", request.report_path.display()))?;
    if report.schema_version != REPORT_SCHEMA_VERSION {
        anyhow::bail!(
            "terminal report {} uses unsupported schema version {}",
            request.report_path.display(),
            report.schema_version
        );
    }
    if report.verification_id != request.verification_id || report.run_id != request.run_id {
        anyhow::bail!(
            "terminal report {} belongs to verification {:?}, run {:?}",
            request.report_path.display(),
            report.verification_id,
            report.run_id
        );
    }
    if report.request_fingerprint_sha256 != request_fingerprint {
        anyhow::bail!(
            "terminal report {} belongs to a different verification request",
            request.report_path.display()
        );
    }
    if Path::new(&report.mismatch_artifact.path) != request.mismatch_path {
        anyhow::bail!(
            "terminal report {} names mismatch artifact {}, expected {}",
            request.report_path.display(),
            report.mismatch_artifact.path,
            request.mismatch_path.display()
        );
    }
    let digest = digest_file(&request.mismatch_path).with_context(|| {
        format!(
            "validating terminal mismatch artifact {}",
            request.mismatch_path.display()
        )
    })?;
    if digest.sha256 != report.mismatch_artifact.sha256
        || digest.bytes != report.mismatch_artifact.bytes
    {
        anyhow::bail!(
            "terminal mismatch artifact {} failed digest validation",
            request.mismatch_path.display()
        );
    }
    if let Some(policy) = report.sample_policy.as_ref() {
        let expected = risk::risk_evidence_path(&request.work_dir);
        if Path::new(&policy.risk_evidence_artifact.path) != expected {
            anyhow::bail!(
                "terminal report {} names risk evidence {}, expected {}",
                request.report_path.display(),
                policy.risk_evidence_artifact.path,
                expected.display()
            );
        }
        risk::validate_artifact(&policy.risk_evidence_artifact)?;
    }
    Ok(Some(report))
}

fn issue_record(
    request: &VerificationRequest,
    issue: &ObservationIssue,
    observed_utc: &str,
) -> MismatchRecord {
    MismatchRecord {
        schema_version: REPORT_SCHEMA_VERSION,
        verification_id: request.verification_id.clone(),
        run_id: request.run_id.clone(),
        path_b64: path_b64(&issue.path),
        kind: match issue.side {
            Side::Source => MismatchKind::UnreadableSource,
            Side::Destination => MismatchKind::UnreadableDestination,
        },
        expected: json!("readable entry"),
        observed: json!({"operation": issue.operation, "error": issue.detail}),
        source_observation: None,
        destination_observation: None,
        worker_id: None,
        shard_id: "metadata-v1".to_string(),
        observed_utc: observed_utc.to_string(),
    }
}

/// One V1 comparison difference for a joined path.
pub(crate) struct Difference {
    kind: MismatchKind,
    expected: Value,
    observed: Value,
}

/// Pure V1 comparison policy for one joined path. Shared by mismatch emission
/// and by sample selection, so `metadata_mismatch` can never disagree with the
/// records the report carries.
pub(crate) fn compare_entries(
    request: &VerificationRequest,
    source: Option<&Entry>,
    destination: Option<&Entry>,
) -> Vec<Difference> {
    let mut differences = Vec::new();
    let mut emit = |kind: MismatchKind, expected: Value, observed: Value| {
        differences.push(Difference {
            kind,
            expected,
            observed,
        });
    };

    let (source, destination) = match (source, destination) {
        (Some(source), None) => {
            emit(
                MismatchKind::MissingDestination,
                json!(source.observed()),
                Value::Null,
            );
            if source.file_type.is_unsupported_v1() {
                emit(
                    MismatchKind::UnsupportedFeature,
                    json!("V1-supported regular file, directory, or symlink"),
                    json!(source.file_type),
                );
            }
            return differences;
        }
        (None, Some(destination)) => {
            emit(
                MismatchKind::UnexpectedDestination,
                Value::Null,
                json!(destination.observed()),
            );
            if destination.file_type.is_unsupported_v1() {
                emit(
                    MismatchKind::UnsupportedFeature,
                    json!("V1-supported regular file, directory, or symlink"),
                    json!(destination.file_type),
                );
            }
            return differences;
        }
        (Some(source), Some(destination)) => (source, destination),
        (None, None) => return differences,
    };

    if source.file_type.is_unsupported_v1() || destination.file_type.is_unsupported_v1() {
        emit(
            MismatchKind::UnsupportedFeature,
            json!("V1-supported regular file, directory, or symlink"),
            json!({"source": source.file_type, "destination": destination.file_type}),
        );
    }
    if source.file_type != destination.file_type {
        emit(
            MismatchKind::Type,
            json!(source.file_type),
            json!(destination.file_type),
        );
    }
    if source.file_type == FileType::Regular
        && destination.file_type == FileType::Regular
        && source.size != destination.size
    {
        emit(
            MismatchKind::Size,
            json!(source.size),
            json!(destination.size),
        );
    }
    if request.options.preserve_mode && (source.mode & 0o7777) != (destination.mode & 0o7777) {
        emit(
            MismatchKind::Mode,
            json!(source.mode & 0o7777),
            json!(destination.mode & 0o7777),
        );
    }
    if request.options.preserve_owner
        && (source.uid != destination.uid || source.gid != destination.gid)
    {
        emit(
            MismatchKind::Owner,
            json!({"uid": source.uid, "gid": source.gid}),
            json!({"uid": destination.uid, "gid": destination.gid}),
        );
    }
    if request.options.preserve_times
        && (source.mtime_sec, source.mtime_nsec / 1_000)
            != (destination.mtime_sec, destination.mtime_nsec / 1_000)
    {
        emit(
            MismatchKind::Mtime,
            json!({"sec": source.mtime_sec, "usec": source.mtime_nsec / 1_000}),
            json!({"sec": destination.mtime_sec, "usec": destination.mtime_nsec / 1_000}),
        );
    }
    if source.file_type == FileType::Symlink
        && destination.file_type == FileType::Symlink
        && source.symlink_target != destination.symlink_target
    {
        emit(
            MismatchKind::SymlinkTarget,
            json!(source.symlink_target.as_ref().map(|v| path_b64(v))),
            json!(destination.symlink_target.as_ref().map(|v| path_b64(v))),
        );
    }
    if source.hardlink_group != destination.hardlink_group {
        emit(
            MismatchKind::HardlinkGroup,
            json!(source.hardlink_group.as_ref().map(|v| path_b64(v))),
            json!(destination.hardlink_group.as_ref().map(|v| path_b64(v))),
        );
    }
    differences
}

/// Emits every V1 difference for a joined path. Returns whether any record
/// was written.
fn emit_entry_differences(
    request: &VerificationRequest,
    emitter: &mut MismatchEmitter,
    source: Option<Entry>,
    destination: Option<Entry>,
    observed_utc: &str,
) -> Result<bool> {
    let path = source
        .as_ref()
        .map(|e| e.path.as_slice())
        .or_else(|| destination.as_ref().map(|e| e.path.as_slice()))
        .expect("joined row has at least one side");
    let source_observation = source.as_ref().map(Entry::observed);
    let destination_observation = destination.as_ref().map(Entry::observed);
    let differences = compare_entries(request, source.as_ref(), destination.as_ref());
    let any = !differences.is_empty();
    for difference in differences {
        emitter.emit(MismatchRecord {
            schema_version: REPORT_SCHEMA_VERSION,
            verification_id: request.verification_id.clone(),
            run_id: request.run_id.clone(),
            path_b64: path_b64(path),
            kind: difference.kind,
            expected: difference.expected,
            observed: difference.observed,
            source_observation: source_observation.clone(),
            destination_observation: destination_observation.clone(),
            worker_id: None,
            shard_id: "metadata-v1".to_string(),
            observed_utc: observed_utc.to_string(),
        })?;
    }
    Ok(any)
}

struct MismatchEmitter {
    writer: BufWriter<File>,
    temp_path: PathBuf,
    final_path: PathBuf,
    count: u64,
    by_kind: BTreeMap<MismatchKind, u64>,
}

impl MismatchEmitter {
    fn new(request: &VerificationRequest, _observed_utc: &str) -> Result<Self> {
        let (temp_path, file) = create_temp(&request.mismatch_path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            temp_path,
            final_path: request.mismatch_path.clone(),
            count: 0,
            by_kind: BTreeMap::new(),
        })
    }

    fn emit(&mut self, record: MismatchRecord) -> Result<()> {
        serde_json::to_writer(&mut self.writer, &record)?;
        self.writer.write_all(b"\n")?;
        self.count += 1;
        *self.by_kind.entry(record.kind).or_insert(0) += 1;
        Ok(())
    }

    fn finish(mut self) -> Result<(ArtifactDigest, u64, BTreeMap<MismatchKind, u64>)> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        let digest = digest_file(&self.temp_path)?;
        publish_existing_temp(&self.temp_path, &self.final_path, &digest.sha256)?;
        Ok((
            ArtifactDigest {
                path: self.final_path.display().to_string(),
                sha256: digest.sha256,
                bytes: digest.bytes,
            },
            self.count,
            self.by_kind,
        ))
    }
}

struct VerificationLock {
    _file: File,
}

impl VerificationLock {
    fn acquire(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .with_context(|| format!("opening verification lock {}", path.display()))?;
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc != 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::WouldBlock {
                anyhow::bail!(
                    "verification work directory {} is already active",
                    path.parent().unwrap_or(path).display()
                );
            }
            return Err(error).with_context(|| format!("locking {}", path.display()));
        }
        Ok(Self { _file: file })
    }
}

fn path_b64(path: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(path)
}

fn display_path(path: &[u8]) -> String {
    if path.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", String::from_utf8_lossy(path))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use migration_core::records::{Endpoint, ServerSideCopy};

    fn entry(path: &[u8], file_type: FileType) -> Entry {
        Entry {
            path: path.to_vec(),
            file_type,
            size: 10,
            mode: 0o100644,
            uid: 10,
            gid: 20,
            mtime_sec: 100,
            mtime_nsec: 123_456_789,
            ctime_sec: 100,
            ctime_nsec: 0,
            dev: 1,
            ino: 1,
            nlink: 1,
            rdev: 0,
            symlink_target: None,
            hardlink_group: None,
        }
    }

    fn request(dir: &Path) -> VerificationRequest {
        VerificationRequest {
            verification_id: "verify-1".into(),
            run_id: "run-1".into(),
            source: Endpoint {
                kind: EndpointKind::Nfs,
                url: "nfs://source/export".into(),
                root: "/".into(),
            },
            destination: Endpoint {
                kind: EndpointKind::Nfs,
                url: "nfs://destination/export".into(),
                root: "/".into(),
            },
            options: MigrationOptions {
                preserve_owner: true,
                preserve_mode: true,
                preserve_times: true,
                preserve_xattr: false,
                server_side_copy: ServerSideCopy::Off,
            },
            exclusions: vec![],
            consistency: ConsistencyBoundary {
                writers_stopped: true,
                source_snapshot_id: None,
                destination_snapshot_id: None,
            },
            work_dir: dir.join("work"),
            report_path: dir.join("report.json"),
            mismatch_path: dir.join("mismatches.jsonl"),
            rpc_timeout_ms: 1,
            directory_batch_size: 100,
            mode: VerificationMode::Metadata,
            sample: None,
        }
    }

    /// Sample-mode request with an asserted-empty risk artifact staged in
    /// the work directory.
    fn sample_request(dir: &Path, sample_files: u64, sample_seed: u64) -> VerificationRequest {
        let mut request = request(dir);
        let risk_evidence = stage_asserted_empty(&request.work_dir).unwrap();
        request.mode = VerificationMode::Sample;
        request.sample = Some(SampleRequest {
            sample_files,
            sample_seed,
            content_workers: 2,
            content_read_size: 8,
            risk_evidence,
            risk_history_asserted_empty: true,
        });
        request
    }

    fn regular(path: &[u8], size: u64) -> Entry {
        let mut entry = entry(path, FileType::Regular);
        entry.size = size;
        entry
    }

    /// Opens the request's observation database with the request identity.
    fn open_store(request: &VerificationRequest) -> Store {
        let fingerprint = request_fingerprint(request).unwrap();
        std::fs::create_dir_all(&request.work_dir).unwrap();
        Store::open(
            &request.work_dir.join("observations.sqlite"),
            &fingerprint,
            "2026-09-28T00:00:00Z",
        )
        .unwrap()
        .0
    }

    /// Inserts identical regular files on both sides and builds the selection.
    fn select_population(
        request: &VerificationRequest,
        paths: &[&[u8]],
    ) -> Vec<(Vec<u8>, sample::ReasonSet)> {
        let mut store = open_store(request);
        for path in paths {
            let file = regular(path, 10);
            store.insert_test_entry(Side::Source, &file).unwrap();
            store.insert_test_entry(Side::Destination, &file).unwrap();
        }
        store.materialize_hardlinks().unwrap();
        prepare_selection(&mut store, request, request.sample.as_ref().unwrap()).unwrap();
        store.selected_jobs().unwrap()
    }

    fn reasons_of(reasons: sample::ReasonSet) -> Vec<RiskReason> {
        reasons.iter().collect()
    }

    fn kinds_for(source: Option<Entry>, destination: Option<Entry>) -> Vec<MismatchKind> {
        let dir = tempfile::tempdir().unwrap();
        let request = request(dir.path());
        let observed = chrono::Utc::now().to_rfc3339();
        let mut emitter = MismatchEmitter::new(&request, &observed).unwrap();
        emit_entry_differences(&request, &mut emitter, source, destination, &observed).unwrap();
        let (_, _, kinds) = emitter.finish().unwrap();
        kinds.into_keys().collect()
    }

    #[test]
    fn detects_missing_extra_and_same_size_metadata_corruption() {
        assert_eq!(
            kinds_for(Some(entry(b"a", FileType::Regular)), None),
            vec![MismatchKind::MissingDestination]
        );
        assert_eq!(
            kinds_for(None, Some(entry(b"a", FileType::Regular))),
            vec![MismatchKind::UnexpectedDestination]
        );
        let source = entry(b"a", FileType::Regular);
        let mut destination = source.clone();
        destination.mode = 0o100600;
        destination.uid = 99;
        destination.mtime_nsec += 1_000;
        assert_eq!(
            kinds_for(Some(source), Some(destination)),
            vec![MismatchKind::Mode, MismatchKind::Owner, MismatchKind::Mtime]
        );
    }

    #[test]
    fn mtime_comparison_uses_nfs_write_precision() {
        let mut source = entry(b"a", FileType::Regular);
        source.mtime_nsec = 123_456_000;
        let mut destination = source.clone();
        destination.mtime_nsec += 999;
        assert!(kinds_for(Some(source), Some(destination)).is_empty());
    }

    #[test]
    fn detects_v1_type_size_link_and_special_file_corruption() {
        let source = entry(b"a", FileType::Regular);
        let mut destination = source.clone();
        destination.file_type = FileType::Directory;
        assert_eq!(
            kinds_for(Some(source.clone()), Some(destination)),
            vec![MismatchKind::Type]
        );

        let mut destination = source.clone();
        destination.size += 1;
        assert_eq!(
            kinds_for(Some(source.clone()), Some(destination)),
            vec![MismatchKind::Size]
        );

        let mut source_link = entry(b"link", FileType::Symlink);
        source_link.symlink_target = Some(b"target-a".to_vec());
        let mut destination_link = source_link.clone();
        destination_link.symlink_target = Some(b"target-b".to_vec());
        assert_eq!(
            kinds_for(Some(source_link), Some(destination_link)),
            vec![MismatchKind::SymlinkTarget]
        );

        let mut source_hardlink = source.clone();
        source_hardlink.hardlink_group = Some(b"a".to_vec());
        let mut destination_hardlink = source.clone();
        destination_hardlink.hardlink_group = Some(b"b".to_vec());
        assert_eq!(
            kinds_for(Some(source_hardlink), Some(destination_hardlink)),
            vec![MismatchKind::HardlinkGroup]
        );

        let fifo = entry(b"pipe", FileType::Fifo);
        assert_eq!(
            kinds_for(Some(fifo.clone()), Some(fifo)),
            vec![MismatchKind::UnsupportedFeature]
        );
    }

    #[test]
    fn disabled_preservation_policies_do_not_claim_false_mismatches() {
        let dir = tempfile::tempdir().unwrap();
        let mut request = request(dir.path());
        request.options.preserve_mode = false;
        request.options.preserve_owner = false;
        request.options.preserve_times = false;
        let source = entry(b"a", FileType::Regular);
        let mut destination = source.clone();
        destination.mode = 0;
        destination.uid = 99;
        destination.gid = 100;
        destination.mtime_sec += 10;
        let observed = chrono::Utc::now().to_rfc3339();
        let mut emitter = MismatchEmitter::new(&request, &observed).unwrap();
        emit_entry_differences(
            &request,
            &mut emitter,
            Some(source),
            Some(destination),
            &observed,
        )
        .unwrap();
        let (_, count, kinds) = emitter.finish().unwrap();
        assert_eq!(count, 0);
        assert!(kinds.is_empty());
    }

    #[test]
    fn mismatch_artifact_preserves_non_utf8_paths() {
        let dir = tempfile::tempdir().unwrap();
        let request = request(dir.path());
        let observed = chrono::Utc::now().to_rfc3339();
        let mut emitter = MismatchEmitter::new(&request, &observed).unwrap();
        emit_entry_differences(
            &request,
            &mut emitter,
            Some(entry(b"bad-\xff-name", FileType::Regular)),
            None,
            &observed,
        )
        .unwrap();
        emitter.finish().unwrap();
        let line = std::fs::read_to_string(&request.mismatch_path).unwrap();
        let record: MismatchRecord = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(record.path_b64)
                .unwrap(),
            b"bad-\xff-name"
        );
    }

    #[test]
    fn hardlink_materialization_compares_membership_not_inode_numbers() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("observations.sqlite");
        let (mut store, _) = Store::open(&db, "identity", "now").unwrap();
        let mut source_a = entry(b"a", FileType::Regular);
        source_a.nlink = 2;
        let mut source_b = source_a.clone();
        source_b.path = b"b".to_vec();
        let mut destination_a = source_a.clone();
        destination_a.dev = 9;
        destination_a.ino = 90;
        let mut destination_b = source_b.clone();
        destination_b.dev = 9;
        destination_b.ino = 90;
        store.insert_test_entry(Side::Source, &source_a).unwrap();
        store.insert_test_entry(Side::Source, &source_b).unwrap();
        store
            .insert_test_entry(Side::Destination, &destination_a)
            .unwrap();
        store
            .insert_test_entry(Side::Destination, &destination_b)
            .unwrap();
        store.materialize_hardlinks().unwrap();
        let mut mismatches = 0;
        store
            .for_each_joined(|source, destination| {
                if source.unwrap().hardlink_group != destination.unwrap().hardlink_group {
                    mismatches += 1;
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(mismatches, 0);

        // Splitting the destination group must flag both members even though
        // inode numbers are unrelated across exports.
        destination_b.ino = 91;
        store
            .insert_test_entry(Side::Destination, &destination_b)
            .unwrap();
        store.materialize_hardlinks().unwrap();
        let mut mismatches = 0;
        store
            .for_each_joined(|source, destination| {
                if source.unwrap().hardlink_group != destination.unwrap().hardlink_group {
                    mismatches += 1;
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(mismatches, 2);
    }

    #[test]
    fn sqlite_round_trips_full_width_nfs_identifiers() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("observations.sqlite");
        let (mut store, _) = Store::open(&db, "identity", "now").unwrap();
        let mut extreme = entry(b"high-bit", FileType::Regular);
        extreme.size = u64::MAX;
        extreme.dev = u64::MAX - 1;
        extreme.ino = u64::MAX - 2;
        extreme.rdev = u64::MAX - 3;
        store.insert_test_entry(Side::Source, &extreme).unwrap();
        store
            .insert_test_entry(Side::Destination, &extreme)
            .unwrap();
        store
            .for_each_joined(|source, destination| {
                assert_eq!(source.unwrap().size, u64::MAX);
                assert_eq!(destination.unwrap().ino, u64::MAX - 2);
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn sqlite_resume_rejects_a_changed_request_identity() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("observations.sqlite");
        let (_, resumed) = Store::open(&db, "identity-a", "start").unwrap();
        assert!(!resumed);
        let (_, resumed) = Store::open(&db, "identity-a", "later").unwrap();
        assert!(resumed);
        assert!(Store::open(&db, "identity-b", "later").is_err());
    }

    #[test]
    fn mismatch_publication_is_idempotent_after_report_crash_window() {
        let dir = tempfile::tempdir().unwrap();
        let request = request(dir.path());
        let observed = "2026-09-28T00:00:00Z";
        let source = entry(b"missing", FileType::Regular);

        let mut first = MismatchEmitter::new(&request, observed).unwrap();
        emit_entry_differences(&request, &mut first, Some(source.clone()), None, observed).unwrap();
        let (first_digest, _, _) = first.finish().unwrap();

        let mut resumed = MismatchEmitter::new(&request, observed).unwrap();
        emit_entry_differences(&request, &mut resumed, Some(source), None, observed).unwrap();
        let (resumed_digest, _, _) = resumed.finish().unwrap();
        assert_eq!(first_digest, resumed_digest);
    }

    #[test]
    fn work_directory_lock_excludes_concurrent_verifiers() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("verification.lock");
        let first = VerificationLock::acquire(&path).unwrap();
        assert!(VerificationLock::acquire(&path).is_err());
        drop(first);
        VerificationLock::acquire(&path).unwrap();
    }

    #[test]
    fn consistency_and_roots_are_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let mut request = request(dir.path());
        request.consistency.writers_stopped = false;
        assert!(validate_request(&request).is_err());

        request.consistency.source_snapshot_id = Some("source-snap".into());
        assert!(validate_request(&request).is_err());

        request.consistency.destination_snapshot_id = Some("destination-snap".into());
        assert!(validate_request(&request).is_ok());

        request.consistency.source_snapshot_id = Some("  ".into());
        assert!(validate_request(&request).is_err());
        request.consistency.source_snapshot_id = Some("source-snap".into());

        request.source.root = "/safe/../escape".into();
        assert!(validate_request(&request).is_err());

        request.source.root = "/".into();
        request.destination = request.source.clone();
        assert!(validate_request(&request).is_err());

        request.destination.url = "nfs://destination/export".into();
        request.rpc_timeout_ms = 0;
        assert!(validate_request(&request).is_err());

        request.rpc_timeout_ms = 1;
        request.directory_batch_size = 0;
        assert!(validate_request(&request).is_err());
    }

    #[test]
    fn request_fingerprint_covers_every_sample_input() {
        let dir = tempfile::tempdir().unwrap();
        let base = sample_request(dir.path(), 10, 0);
        let baseline = request_fingerprint(&base).unwrap();
        assert_ne!(baseline, request_fingerprint(&request(dir.path())).unwrap());

        let mut variants: Vec<VerificationRequest> = Vec::new();
        for mutate in [
            (|s: &mut SampleRequest| s.sample_files = 11) as fn(&mut SampleRequest),
            |s| s.sample_seed = 1,
            |s| s.content_workers = 3,
            |s| s.content_read_size = 9,
            |s| s.risk_evidence.sha256 = "0".repeat(64),
            |s| s.risk_evidence.bytes = 1,
            |s| s.risk_history_asserted_empty = false,
        ] {
            let mut variant = base.clone();
            mutate(variant.sample.as_mut().unwrap());
            variants.push(variant);
        }
        let mut seen = BTreeSet::new();
        seen.insert(baseline.clone());
        for variant in &variants {
            let fingerprint = request_fingerprint(variant).unwrap();
            assert!(
                seen.insert(fingerprint),
                "variant did not change the fingerprint"
            );
        }
        // Only the artifact digest identifies the evidence, never its path.
        let mut moved = base.clone();
        moved.sample.as_mut().unwrap().risk_evidence.path = "/elsewhere".into();
        assert_eq!(request_fingerprint(&moved).unwrap(), baseline);

        // A database opened under one identity refuses every variant.
        let db = dir.path().join("fp.sqlite");
        Store::open(&db, &baseline, "now").unwrap();
        for variant in &variants {
            assert!(Store::open(&db, &request_fingerprint(variant).unwrap(), "now").is_err());
        }
    }

    #[test]
    fn sample_requests_are_validated_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let mut request = sample_request(dir.path(), 10, 0);
        assert!(validate_request(&request).is_ok());

        let mut metadata_with_sample = request.clone();
        metadata_with_sample.mode = VerificationMode::Metadata;
        assert!(validate_request(&metadata_with_sample).is_err());

        let mut sample_without_options = request.clone();
        sample_without_options.sample = None;
        assert!(validate_request(&sample_without_options).is_err());

        for mutate in [
            (|s: &mut SampleRequest| s.sample_files = 0) as fn(&mut SampleRequest),
            |s| s.content_workers = 0,
            |s| s.content_read_size = 0,
            |s| s.content_read_size = i32::MAX as u32 + 1,
            |s| s.risk_evidence.path = "/elsewhere/risk-evidence.jsonl".into(),
            |s| s.risk_evidence.bytes = 5,
        ] {
            let mut variant = request.clone();
            mutate(variant.sample.as_mut().unwrap());
            assert!(validate_request(&variant).is_err());
        }
        request.sample.as_mut().unwrap().content_read_size = i32::MAX as u32;
        assert!(validate_request(&request).is_ok());
    }

    #[test]
    fn v1_databases_are_rejected_with_a_schema_message() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("observations.sqlite");
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.execute_batch(
            "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
             INSERT INTO meta VALUES('identity','v1'),('started_utc','then');",
        )
        .unwrap();
        drop(conn);
        let error = match Store::open(&db, "v1", "now") {
            Ok(_) => panic!("V1 database must be rejected"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("schema version 1"), "{error}");
        let error = match Store::open(&db, "other", "now") {
            Ok(_) => panic!("V1 database must be rejected"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("schema version 1"), "{error}");
    }

    #[test]
    fn selection_is_invariant_to_insertion_order_and_restart() {
        let paths: Vec<Vec<u8>> = (0..200).map(|i| format!("f{i:03}").into_bytes()).collect();
        let forward: Vec<&[u8]> = paths.iter().map(Vec::as_slice).collect();
        let mut reversed = forward.clone();
        reversed.reverse();

        let dir_a = tempfile::tempdir().unwrap();
        let request_a = sample_request(dir_a.path(), 50, 7);
        let selected_a = select_population(&request_a, &forward);
        let dir_b = tempfile::tempdir().unwrap();
        let request_b = sample_request(dir_b.path(), 50, 7);
        let selected_b = select_population(&request_b, &reversed);
        assert_eq!(selected_a, selected_b);
        assert_eq!(selected_a.len(), 50);
        assert!(selected_a
            .iter()
            .all(|(_, reasons)| reasons_of(*reasons) == vec![RiskReason::Seeded]));

        // Restart with the committed marker: nothing is rebuilt and the set
        // is identical.
        let mut store = open_store(&request_a);
        assert!(store.selection_complete().unwrap());
        prepare_selection(&mut store, &request_a, request_a.sample.as_ref().unwrap()).unwrap();
        assert_eq!(store.selected_jobs().unwrap(), selected_a);

        // Restart without the marker (crash during selection) rebuilds the
        // exact same set.
        store.clear_selection_marker().unwrap();
        prepare_selection(&mut store, &request_a, request_a.sample.as_ref().unwrap()).unwrap();
        assert_eq!(store.selected_jobs().unwrap(), selected_a);
        assert!(store.selection_complete().unwrap());
        let summary = store.selection_summary().unwrap();
        assert_eq!(summary.eligible_files, 200);
        assert_eq!(summary.selected_files, 50);
        assert_eq!(summary.seeded_selected_files, 50);
        assert_eq!(summary.risk_selected_files, 0);
    }

    #[test]
    fn different_seeds_change_the_seeded_selection() {
        let paths: Vec<Vec<u8>> = (0..200).map(|i| format!("f{i:03}").into_bytes()).collect();
        let slice: Vec<&[u8]> = paths.iter().map(Vec::as_slice).collect();
        let dir_a = tempfile::tempdir().unwrap();
        let selected_a = select_population(&sample_request(dir_a.path(), 50, 0), &slice);
        let dir_b = tempfile::tempdir().unwrap();
        let selected_b = select_population(&sample_request(dir_b.path(), 50, 1), &slice);
        assert_ne!(selected_a, selected_b);
        assert_eq!(selected_b.len(), 50);
    }

    #[test]
    fn raw_non_utf8_paths_rank_and_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let request = sample_request(dir.path(), 10, 0);
        let selected = select_population(&request, &[b"bad-\xff-name", b"ok", b"\xfe\xff/\x80"]);
        let paths: Vec<&[u8]> = selected.iter().map(|(path, _)| path.as_slice()).collect();
        // SQLite orders raw path BLOBs by memcmp, so the high-bit path sorts
        // last; the bytes must survive selection unchanged.
        assert_eq!(
            paths,
            vec![&b"bad-\xff-name"[..], &b"ok"[..], &b"\xfe\xff/\x80"[..]]
        );
    }

    #[test]
    fn selection_has_no_duplicates_and_selects_min_of_requested_and_eligible() {
        let paths: Vec<Vec<u8>> = (0..10).map(|i| format!("f{i}").into_bytes()).collect();
        let slice: Vec<&[u8]> = paths.iter().map(Vec::as_slice).collect();
        let dir = tempfile::tempdir().unwrap();
        let selected = select_population(&sample_request(dir.path(), 50, 0), &slice);
        assert_eq!(selected.len(), 10);
        let unique: BTreeSet<_> = selected.iter().map(|(path, _)| path.clone()).collect();
        assert_eq!(unique.len(), 10);
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            select_population(&sample_request(dir.path(), 3, 0), &slice).len(),
            3
        );
    }

    #[test]
    fn mandatory_risk_is_never_evicted_and_may_exceed_the_target() {
        let dir = tempfile::tempdir().unwrap();
        let request = sample_request(dir.path(), 2, 0);
        let mut store = open_store(&request);
        for i in 0..20 {
            let file = regular(format!("clean{i:02}").as_bytes(), 10);
            store.insert_test_entry(Side::Source, &file).unwrap();
            store.insert_test_entry(Side::Destination, &file).unwrap();
        }
        for i in 0..5 {
            let file = regular(format!("drift{i}").as_bytes(), 10);
            let mut changed = file.clone();
            changed.uid += 1;
            store.insert_test_entry(Side::Source, &file).unwrap();
            store
                .insert_test_entry(Side::Destination, &changed)
                .unwrap();
        }
        store.materialize_hardlinks().unwrap();
        prepare_selection(&mut store, &request, request.sample.as_ref().unwrap()).unwrap();
        let selected = store.selected_jobs().unwrap();
        assert_eq!(selected.len(), 5);
        assert!(selected
            .iter()
            .all(|(path, reasons)| path.starts_with(b"drift")
                && reasons_of(*reasons) == vec![RiskReason::MetadataMismatch]));
        let summary = store.selection_summary().unwrap();
        assert_eq!(summary.risk_selected_files, 5);
        assert_eq!(summary.seeded_selected_files, 0);
        assert_eq!(summary.reasons[&RiskReason::MetadataMismatch], 5);

        // A larger target tops up with seeded paths and keeps every
        // mandatory one.
        let dir = tempfile::tempdir().unwrap();
        let request = sample_request(dir.path(), 8, 0);
        let mut store = open_store(&request);
        for i in 0..20 {
            let file = regular(format!("clean{i:02}").as_bytes(), 10);
            store.insert_test_entry(Side::Source, &file).unwrap();
            store.insert_test_entry(Side::Destination, &file).unwrap();
        }
        for i in 0..5 {
            let file = regular(format!("drift{i}").as_bytes(), 10);
            let mut changed = file.clone();
            changed.size += 1;
            store.insert_test_entry(Side::Source, &file).unwrap();
            store
                .insert_test_entry(Side::Destination, &changed)
                .unwrap();
        }
        store.materialize_hardlinks().unwrap();
        prepare_selection(&mut store, &request, request.sample.as_ref().unwrap()).unwrap();
        let selected = store.selected_jobs().unwrap();
        assert_eq!(selected.len(), 8);
        assert_eq!(
            selected
                .iter()
                .filter(|(_, reasons)| reasons.contains(RiskReason::MetadataMismatch))
                .count(),
            5
        );
        assert_eq!(
            selected
                .iter()
                .filter(|(_, reasons)| reasons_of(*reasons) == vec![RiskReason::Seeded])
                .count(),
            3
        );
    }

    #[test]
    fn multiple_reasons_union_deterministically() {
        let dir = tempfile::tempdir().unwrap();
        let mut request = sample_request(dir.path(), 1, 0);
        let mut stager = RiskEvidenceStager::new(&request.work_dir).unwrap();
        for reason in [RiskReason::MigrationDowngrade, RiskReason::MigrationFailure] {
            stager
                .add(&RiskEvidenceRecord {
                    path: b"g/root".to_vec(),
                    reason,
                    source: "s".into(),
                    source_etag: "e".into(),
                })
                .unwrap();
        }
        // Stager refuses to replace the asserted-empty artifact, so use a
        // fresh work directory for the evidence-bearing request.
        drop(stager);
        let dir = tempfile::tempdir().unwrap();
        request = sample_request(dir.path(), 1, 0);
        std::fs::remove_file(&request.sample.as_ref().unwrap().risk_evidence.path).unwrap();
        let mut stager = RiskEvidenceStager::new(&request.work_dir).unwrap();
        for reason in [RiskReason::MigrationDowngrade, RiskReason::MigrationFailure] {
            stager
                .add(&RiskEvidenceRecord {
                    path: b"g/root".to_vec(),
                    reason,
                    source: "s".into(),
                    source_etag: "e".into(),
                })
                .unwrap();
        }
        let sample = request.sample.as_mut().unwrap();
        sample.risk_evidence = stager.publish().unwrap();
        sample.risk_history_asserted_empty = false;

        let mut store = open_store(&request);
        let mut root = regular(b"g/root", 1 << 20);
        root.nlink = 2;
        root.ino = 77;
        let mut sibling = root.clone();
        sibling.path = b"g/sibling".to_vec();
        let mut root_dst = root.clone();
        root_dst.mode = 0o100600;
        root_dst.ino = 900;
        let mut sibling_dst = root_dst.clone();
        sibling_dst.path = b"g/sibling".to_vec();
        for (side, entry) in [
            (Side::Source, &root),
            (Side::Source, &sibling),
            (Side::Destination, &root_dst),
            (Side::Destination, &sibling_dst),
        ] {
            store.insert_test_entry(side, entry).unwrap();
        }
        store.materialize_hardlinks().unwrap();
        prepare_selection(&mut store, &request, request.sample.as_ref().unwrap()).unwrap();
        let selected = store.selected_jobs().unwrap();
        assert_eq!(selected.len(), 2, "{selected:?}");
        assert_eq!(selected[0].0, b"g/root");
        assert_eq!(
            reasons_of(selected[0].1),
            vec![
                RiskReason::MetadataMismatch,
                RiskReason::BucketBoundary,
                RiskReason::HardlinkGroup,
                RiskReason::MigrationFailure,
                RiskReason::MigrationDowngrade,
            ]
        );
        assert_eq!(
            reasons_of(selected[1].1),
            vec![RiskReason::MetadataMismatch, RiskReason::BucketBoundary]
        );
        let summary = store.selection_summary().unwrap();
        assert_eq!(summary.risk_selected_files, 2);
        assert_eq!(summary.risk_ineligible_files, 0);
        assert_eq!(summary.reasons[&RiskReason::HardlinkGroup], 1);
        assert_eq!(summary.reasons[&RiskReason::MigrationFailure], 1);
    }

    #[test]
    fn bucket_boundaries_are_mandatory_for_every_transition() {
        let dir = tempfile::tempdir().unwrap();
        let request = sample_request(dir.path(), 1, 0);
        let mut store = open_store(&request);
        let thresholds = sample::bucket_thresholds();
        assert_eq!(thresholds.len(), 2);
        let mut expected = BTreeSet::new();
        for (index, threshold) in thresholds.iter().enumerate() {
            for delta in -2i64..=2 {
                let size = (*threshold as i64 + delta) as u64;
                let path = format!("t{index}/{delta:+}").into_bytes();
                let file = regular(&path, size);
                store.insert_test_entry(Side::Source, &file).unwrap();
                store.insert_test_entry(Side::Destination, &file).unwrap();
                if delta.abs() <= 1 {
                    expected.insert(path);
                }
            }
        }
        store.materialize_hardlinks().unwrap();
        prepare_selection(&mut store, &request, request.sample.as_ref().unwrap()).unwrap();
        let selected = store.selected_jobs().unwrap();
        let boundary: BTreeSet<Vec<u8>> = selected
            .iter()
            .filter(|(_, reasons)| reasons.contains(RiskReason::BucketBoundary))
            .map(|(path, _)| path.clone())
            .collect();
        assert_eq!(boundary, expected);
        assert_eq!(selected.len(), expected.len());
    }

    #[test]
    fn one_deterministic_eligible_member_is_selected_per_hardlink_group() {
        let dir = tempfile::tempdir().unwrap();
        let request = sample_request(dir.path(), 1, 0);
        let mut store = open_store(&request);
        // Group g: a, b, c on the source; a is missing on the destination so
        // b (the smallest eligible member) is the representative.
        let mut member = regular(b"g/a", 10);
        member.nlink = 3;
        member.ino = 11;
        for name in [&b"g/a"[..], b"g/b", b"g/c"] {
            let mut entry = member.clone();
            entry.path = name.to_vec();
            store.insert_test_entry(Side::Source, &entry).unwrap();
        }
        let mut destination = member.clone();
        destination.ino = 1100;
        destination.nlink = 2;
        for name in [&b"g/b"[..], b"g/c"] {
            let mut entry = destination.clone();
            entry.path = name.to_vec();
            store.insert_test_entry(Side::Destination, &entry).unwrap();
        }
        // Group h has no eligible member at all.
        let mut orphan = regular(b"h/x", 10);
        orphan.nlink = 2;
        orphan.ino = 22;
        let mut orphan_y = orphan.clone();
        orphan_y.path = b"h/y".to_vec();
        store.insert_test_entry(Side::Source, &orphan).unwrap();
        store.insert_test_entry(Side::Source, &orphan_y).unwrap();
        store.materialize_hardlinks().unwrap();
        prepare_selection(&mut store, &request, request.sample.as_ref().unwrap()).unwrap();
        let selected = store.selected_jobs().unwrap();
        let hardlink: Vec<&[u8]> = selected
            .iter()
            .filter(|(_, reasons)| reasons.contains(RiskReason::HardlinkGroup))
            .map(|(path, _)| path.as_slice())
            .collect();
        assert_eq!(hardlink, vec![&b"g/b"[..]]);
        assert_eq!(
            store.risk_ineligible_paths().unwrap(),
            vec![b"h/x".to_vec()]
        );
        let summary = store.selection_summary().unwrap();
        assert_eq!(summary.risk_ineligible_files, 1);
        assert_eq!(summary.reasons[&RiskReason::HardlinkGroup], 1);
    }

    #[test]
    fn risk_inputs_are_hints_and_ineligible_paths_are_never_opened() {
        let dir = tempfile::tempdir().unwrap();
        let mut request = sample_request(dir.path(), 1, 0);
        std::fs::remove_file(&request.sample.as_ref().unwrap().risk_evidence.path).unwrap();
        let mut stager = RiskEvidenceStager::new(&request.work_dir).unwrap();
        for path in [&b"present"[..], b"missing", b"collided", b"link", b"ghost"] {
            stager
                .add(&RiskEvidenceRecord {
                    path: path.to_vec(),
                    reason: RiskReason::MigrationFailure,
                    source: "failures/host-a/part-0000-e1.jsonl".into(),
                    source_etag: "e".into(),
                })
                .unwrap();
        }
        let sample = request.sample.as_mut().unwrap();
        sample.risk_evidence = stager.publish().unwrap();
        sample.risk_history_asserted_empty = false;

        let mut store = open_store(&request);
        let present = regular(b"present", 10);
        store.insert_test_entry(Side::Source, &present).unwrap();
        store
            .insert_test_entry(Side::Destination, &present)
            .unwrap();
        store
            .insert_test_entry(Side::Source, &regular(b"missing", 10))
            .unwrap();
        store
            .insert_test_entry(Side::Source, &regular(b"collided", 10))
            .unwrap();
        store
            .insert_test_entry(Side::Destination, &entry(b"collided", FileType::Directory))
            .unwrap();
        let link = entry(b"link", FileType::Symlink);
        store.insert_test_entry(Side::Source, &link).unwrap();
        store.insert_test_entry(Side::Destination, &link).unwrap();
        store.materialize_hardlinks().unwrap();
        prepare_selection(&mut store, &request, request.sample.as_ref().unwrap()).unwrap();
        let selected = store.selected_jobs().unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].0, b"present");
        assert_eq!(
            reasons_of(selected[0].1),
            vec![RiskReason::MigrationFailure]
        );
        assert_eq!(
            store.risk_ineligible_paths().unwrap(),
            vec![
                b"collided".to_vec(),
                b"ghost".to_vec(),
                b"link".to_vec(),
                b"missing".to_vec()
            ]
        );
        let summary = store.selection_summary().unwrap();
        assert_eq!(summary.eligible_files, 1);
        assert_eq!(summary.risk_selected_files, 1);
        assert_eq!(summary.risk_ineligible_files, 4);
    }
}
