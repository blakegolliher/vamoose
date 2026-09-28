//! Independent Vamoose verification.
//!
//! V1 performs fresh libnfs scans of both exports, checkpoints observations in
//! SQLite, compares paths in byte order, and publishes immutable JSON/JSONL
//! artifacts. It intentionally does not trust the migration index or mover
//! outcomes as proof.

mod model;
mod scanner;
mod store;

pub use model::{
    ArtifactDigest, ComparisonPolicy, ConsistencyBoundary, FileType, MismatchKind, MismatchRecord,
    ObservationCounts, ObservedMetadata, VerificationReport, VerificationRequest,
    VerificationResult, VerificationStatus, REPORT_SCHEMA_VERSION,
};

use anyhow::{Context, Result};
use base64::Engine;
use migration_core::records::{EndpointKind, MigrationOptions};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
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
    })?;
    let (mismatch_artifact, mismatch_count, mismatches_by_kind) = emitter.finish()?;
    let counts = store.counts()?;
    let status = if !issues.is_empty() {
        VerificationStatus::Failed
    } else if !unstable.is_empty() {
        VerificationStatus::Inconclusive
    } else if mismatch_count != 0 {
        VerificationStatus::Mismatched
    } else {
        VerificationStatus::Passed
    };
    let operational_errors = issues
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
        mode: "metadata".to_string(),
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
        started_utc: store.started_utc()?,
        completed_utc: chrono::Utc::now().to_rfc3339(),
        resumed,
        counts,
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
    migration_core::overlap::check(&request.source, &request.destination)
        .context("verification source and destination must be independent")?;
    Ok(())
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

fn emit_entry_differences(
    request: &VerificationRequest,
    emitter: &mut MismatchEmitter,
    source: Option<Entry>,
    destination: Option<Entry>,
    observed_utc: &str,
) -> Result<()> {
    let path = source
        .as_ref()
        .map(|e| e.path.as_slice())
        .or_else(|| destination.as_ref().map(|e| e.path.as_slice()))
        .expect("joined row has at least one side");
    let source_observation = source.as_ref().map(Entry::observed);
    let destination_observation = destination.as_ref().map(Entry::observed);
    let mut emit = |kind: MismatchKind, expected: Value, observed: Value| {
        emitter.emit(MismatchRecord {
            schema_version: REPORT_SCHEMA_VERSION,
            verification_id: request.verification_id.clone(),
            run_id: request.run_id.clone(),
            path_b64: path_b64(path),
            kind,
            expected,
            observed,
            source_observation: source_observation.clone(),
            destination_observation: destination_observation.clone(),
            worker_id: None,
            shard_id: "metadata-v1".to_string(),
            observed_utc: observed_utc.to_string(),
        })
    };

    let (source, destination) = match (source.as_ref(), destination.as_ref()) {
        (Some(source), None) => {
            emit(
                MismatchKind::MissingDestination,
                json!(source.observed()),
                Value::Null,
            )?;
            if source.file_type.is_unsupported_v1() {
                emit(
                    MismatchKind::UnsupportedFeature,
                    json!("V1-supported regular file, directory, or symlink"),
                    json!(source.file_type),
                )?;
            }
            return Ok(());
        }
        (None, Some(destination)) => {
            emit(
                MismatchKind::UnexpectedDestination,
                Value::Null,
                json!(destination.observed()),
            )?;
            if destination.file_type.is_unsupported_v1() {
                emit(
                    MismatchKind::UnsupportedFeature,
                    json!("V1-supported regular file, directory, or symlink"),
                    json!(destination.file_type),
                )?;
            }
            return Ok(());
        }
        (Some(source), Some(destination)) => (source, destination),
        (None, None) => unreachable!(),
    };

    if source.file_type.is_unsupported_v1() || destination.file_type.is_unsupported_v1() {
        emit(
            MismatchKind::UnsupportedFeature,
            json!("V1-supported regular file, directory, or symlink"),
            json!({"source": source.file_type, "destination": destination.file_type}),
        )?;
    }
    if source.file_type != destination.file_type {
        emit(
            MismatchKind::Type,
            json!(source.file_type),
            json!(destination.file_type),
        )?;
    }
    if source.file_type == FileType::Regular
        && destination.file_type == FileType::Regular
        && source.size != destination.size
    {
        emit(
            MismatchKind::Size,
            json!(source.size),
            json!(destination.size),
        )?;
    }
    if request.options.preserve_mode && (source.mode & 0o7777) != (destination.mode & 0o7777) {
        emit(
            MismatchKind::Mode,
            json!(source.mode & 0o7777),
            json!(destination.mode & 0o7777),
        )?;
    }
    if request.options.preserve_owner
        && (source.uid != destination.uid || source.gid != destination.gid)
    {
        emit(
            MismatchKind::Owner,
            json!({"uid": source.uid, "gid": source.gid}),
            json!({"uid": destination.uid, "gid": destination.gid}),
        )?;
    }
    if request.options.preserve_times
        && (source.mtime_sec, source.mtime_nsec / 1_000)
            != (destination.mtime_sec, destination.mtime_nsec / 1_000)
    {
        emit(
            MismatchKind::Mtime,
            json!({"sec": source.mtime_sec, "usec": source.mtime_nsec / 1_000}),
            json!({"sec": destination.mtime_sec, "usec": destination.mtime_nsec / 1_000}),
        )?;
    }
    if source.file_type == FileType::Symlink
        && destination.file_type == FileType::Symlink
        && source.symlink_target != destination.symlink_target
    {
        emit(
            MismatchKind::SymlinkTarget,
            json!(source.symlink_target.as_ref().map(|v| path_b64(v))),
            json!(destination.symlink_target.as_ref().map(|v| path_b64(v))),
        )?;
    }
    if source.hardlink_group != destination.hardlink_group {
        emit(
            MismatchKind::HardlinkGroup,
            json!(source.hardlink_group.as_ref().map(|v| path_b64(v))),
            json!(destination.hardlink_group.as_ref().map(|v| path_b64(v))),
        )?;
    }
    Ok(())
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

struct FileDigest {
    sha256: String,
    bytes: u64,
}

fn digest_file(path: &Path) -> Result<FileDigest> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let bytes = std::io::copy(&mut file, &mut hasher)?;
    Ok(FileDigest {
        sha256: hex::encode(hasher.finalize()),
        bytes,
    })
}

fn publish_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let (temporary, mut file) = create_temp(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    let digest = digest_file(&temporary)?;
    publish_existing_temp(&temporary, path, &digest.sha256)
}

fn publish_existing_temp(temporary: &Path, final_path: &Path, sha256: &str) -> Result<()> {
    match std::fs::hard_link(temporary, final_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let existing = digest_file(final_path)?;
            if existing.sha256 != sha256 {
                anyhow::bail!(
                    "refusing to replace existing terminal artifact {} (digest {} != {})",
                    final_path.display(),
                    existing.sha256,
                    sha256
                );
            }
        }
        Err(error) => {
            return Err(error).with_context(|| format!("publishing {}", final_path.display()))
        }
    }
    std::fs::remove_file(temporary)?;
    sync_parent(final_path)
}

fn sync_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn create_temp(path: &Path) -> Result<(PathBuf, File)> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("verification-artifact");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    for _ in 0..100 {
        let sequence = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let temporary = path.with_file_name(format!(
            ".{name}.{}.{}.{}.tmp",
            std::process::id(),
            now,
            sequence
        ));
        match OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
        {
            Ok(file) => return Ok((temporary, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error).with_context(|| format!("creating {}", temporary.display()))
            }
        }
    }
    anyhow::bail!(
        "could not allocate a temporary artifact beside {}",
        path.display()
    )
}

fn ensure_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    Ok(())
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
        }
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
}
