//! Resumable pre-flight converter: read nfs-walker Parquet shards and
//! atomically write canonical-schema shards with machine-readable
//! checkpoints. See `README.md` and `docs/work-items/SHIM_PLAN.md` for
//! the schema translation and file-type synthesis constraints.
//!
//! Usable two ways: the `mig-walker-rewrite` binary (a thin `main.rs`
//! over [`run_rewrite`], used by `vamoose prepare` as a subprocess),
//! and in-process via this library (used by `mongoose prepare`, which
//! ships as a single binary). [`Cli`] doubles as the programmatic
//! argument struct.

use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{
    Array, ArrayRef, BinaryBuilder, Int32Array, Int32Builder, Int64Array, Int64Builder,
    StringArray, UInt16Array, UInt32Array, UInt64Array, UInt64Builder, UInt8Builder,
};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use clap::Parser;
use migration_core::schema::{
    self, FileTypeTag, KV_CONTRACT_VERSION, KV_FORMAT_VERSION, KV_ROW_COUNT, KV_SHARD_INDEX,
    KV_WALKER_VERSION,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::format::KeyValue;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs::{File, Metadata};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

// =============================================================================
// CLI
// =============================================================================

#[derive(Parser, Debug)]
#[command(
    name = "mig-walker-rewrite",
    about = "Translate nfs-walker parquet shards to the canonical migration schema."
)]
pub struct Cli {
    /// Walker parquet location. Accepts either:
    ///   - the walker output root (contains `scans/<scan_id>/`), or
    ///   - a `scans/<scan_id>/` directory containing part files directly.
    ///
    /// Pre-RocksDB-removal walker layouts (flat directory of part files)
    /// are also accepted for backwards compat.
    #[arg(short = 'i', long)]
    pub input: PathBuf,

    /// Directory to write canonical shards into. Created if absent.
    /// Refuses to clobber non-empty unless --resume is supplied.
    #[arg(short = 'o', long)]
    pub output: PathBuf,

    /// Export root prefix to strip from absolute walker paths.
    /// Example: --source-root /bgolliher/vamoose-source.
    #[arg(long)]
    pub source_root: String,

    /// Walker version string for parquet KV metadata.
    #[arg(long, default_value = "shim-via-unknown")]
    pub walker_version: String,

    /// Resume from validated per-shard entries in the rewrite report.
    #[arg(long)]
    pub resume: bool,

    /// Machine-readable checkpoint report. Defaults to
    /// <output>/rewrite-report.json.
    #[arg(long)]
    pub report: Option<PathBuf>,

    /// Per-row trace logging.
    #[arg(short, long)]
    pub verbose: bool,
}

pub fn run_rewrite(args: &Cli) -> Result<()> {
    let scan_dir = resolve_scan_dir(&args.input)?;
    if scan_dir != args.input {
        tracing::info!(
            input = %args.input.display(),
            scan_dir = %scan_dir.display(),
            "resolved walker scan directory under scans/<scan_id>/",
        );
    }
    let mut inputs: Vec<PathBuf> = std::fs::read_dir(&scan_dir)
        .with_context(|| format!("reading scan dir {}", scan_dir.display()))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_file() && p.extension().and_then(|s| s.to_str()) == Some("parquet"))
        .collect();
    inputs.sort();

    if inputs.is_empty() {
        bail!("no parquet files found in {}", scan_dir.display());
    }

    std::fs::create_dir_all(&args.output)
        .with_context(|| format!("creating output dir {}", args.output.display()))?;
    if !args.resume && std::fs::read_dir(&args.output)?.next().is_some() {
        bail!("output directory not empty: {}", args.output.display());
    }

    let scan_dir = std::fs::canonicalize(&scan_dir)
        .with_context(|| format!("canonicalizing {}", scan_dir.display()))?;
    let output_dir = std::fs::canonicalize(&args.output)
        .with_context(|| format!("canonicalizing {}", args.output.display()))?;
    let report_path = args
        .report
        .clone()
        .unwrap_or_else(|| output_dir.join("rewrite-report.json"));
    let report_path = absolute_path(&report_path)?;

    validate_resume_directory(&output_dir, &report_path, &inputs, args.resume)?;
    let mut report = load_or_create_report(
        &report_path,
        &scan_dir,
        &output_dir,
        &args.source_root,
        &args.walker_version,
        args.resume,
    )?;
    let mut checkpoints: HashMap<String, ShardCheckpoint> = report
        .shards
        .drain(..)
        .map(|checkpoint| (checkpoint.input_name.clone(), checkpoint))
        .collect();

    let input_names: HashSet<String> = inputs
        .iter()
        .map(|input| input.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    if let Some(stale) = checkpoints
        .keys()
        .find(|name| !input_names.contains(name.as_str()))
    {
        bail!("rewrite report contains shard no longer present in input: {stale}");
    }

    let source_root = args.source_root.as_bytes();

    for (shard_idx, input) in inputs.iter().enumerate() {
        let input = std::fs::canonicalize(input)
            .with_context(|| format!("canonicalizing input shard {}", input.display()))?;
        let input_name = input.file_name().unwrap().to_string_lossy().into_owned();
        let output = output_dir.join(input.file_name().unwrap());
        let partial = partial_path(&output);
        let fingerprint = source_fingerprint(&std::fs::metadata(&input)?)?;
        if args.resume {
            if let Some(checkpoint) = checkpoints.get(&input_name) {
                if checkpoint_is_valid(checkpoint, &output, shard_idx as u32, &fingerprint) {
                    if partial.exists() {
                        std::fs::remove_file(&partial).with_context(|| {
                            format!("removing stale partial {}", partial.display())
                        })?;
                    }
                    tracing::info!(
                        input = %input.display(),
                        output = %output.display(),
                        shard_idx,
                        rows = checkpoint.rows,
                        "validated checkpoint; shard already rewritten",
                    );
                    continue;
                }
                tracing::warn!(
                    input = %input.display(),
                    output = %output.display(),
                    shard_idx,
                    "checkpoint or output failed validation; rewriting shard",
                );
            }
        }

        if partial.exists() {
            std::fs::remove_file(&partial)
                .with_context(|| format!("removing stale partial {}", partial.display()))?;
        }
        let rows = rewrite_shard(
            &input,
            &partial,
            shard_idx as u32,
            source_root,
            &args.walker_version,
        )
        .with_context(|| format!("rewriting shard {}", input.display()))?;
        File::open(&partial)?.sync_all()?;
        std::fs::rename(&partial, &output).with_context(|| {
            format!(
                "atomically activating rewritten shard {} as {}",
                partial.display(),
                output.display()
            )
        })?;
        File::open(&output_dir)?.sync_all()?;
        let output_bytes = std::fs::metadata(&output)?.len();
        let output_sha256 = file_sha256(&output)?;
        checkpoints.insert(
            input_name.clone(),
            ShardCheckpoint {
                input_name,
                shard_index: shard_idx as u32,
                input_bytes: fingerprint.bytes,
                input_modified_unix_ns: fingerprint.modified_unix_ns,
                output_name: output.file_name().unwrap().to_string_lossy().into_owned(),
                output_bytes,
                output_sha256,
                rows,
            },
        );
        update_report(&mut report, &checkpoints, false);
        write_report_atomic(&report_path, &report)?;
        tracing::info!(
            input = %input.display(),
            output = %output.display(),
            shard_idx,
            rows,
            "shard rewritten",
        );
    }

    update_report(&mut report, &checkpoints, true);
    write_report_atomic(&report_path, &report)?;
    tracing::info!(
        report = %report_path.display(),
        shards = report.shards.len(),
        rows = report.total_rows,
        "rewrite complete; machine-readable report committed",
    );
    Ok(())
}

// =============================================================================
// Resumable rewrite report
// =============================================================================

const REWRITE_REPORT_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RewriteReport {
    schema_version: u32,
    input_dir: String,
    output_dir: String,
    source_root: String,
    walker_version: String,
    started_unix_seconds: u64,
    updated_unix_seconds: u64,
    complete: bool,
    total_rows: u64,
    total_output_bytes: u64,
    shards: Vec<ShardCheckpoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ShardCheckpoint {
    input_name: String,
    shard_index: u32,
    input_bytes: u64,
    input_modified_unix_ns: u64,
    output_name: String,
    output_bytes: u64,
    output_sha256: String,
    rows: u64,
}

#[derive(Debug, Clone, Copy)]
struct SourceFingerprint {
    bytes: u64,
    modified_unix_ns: u64,
}

fn unix_seconds_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn absolute_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()
            .context("resolving current directory")?
            .join(path))
    }
}

fn source_fingerprint(metadata: &Metadata) -> Result<SourceFingerprint> {
    let modified_unix_ns = metadata
        .modified()
        .context("reading input shard modification time")?
        .duration_since(UNIX_EPOCH)
        .context("input shard modification time predates Unix epoch")?
        .as_nanos()
        .try_into()
        .context("input shard modification time does not fit in u64 nanoseconds")?;
    Ok(SourceFingerprint {
        bytes: metadata.len(),
        modified_unix_ns,
    })
}

fn file_sha256(path: &Path) -> Result<String> {
    let file =
        File::open(path).with_context(|| format!("opening {} for SHA256", path.display()))?;
    let mut reader = std::io::BufReader::new(file);
    let mut digest = Sha256::new();
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .with_context(|| format!("hashing {}", path.display()))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn partial_path(output: &Path) -> PathBuf {
    let mut name = output.as_os_str().to_owned();
    name.push(".partial");
    PathBuf::from(name)
}

fn validate_resume_directory(
    output_dir: &Path,
    report_path: &Path,
    inputs: &[PathBuf],
    resume: bool,
) -> Result<()> {
    if !resume {
        return Ok(());
    }
    let mut allowed: HashSet<PathBuf> = HashSet::new();
    for input in inputs {
        let output = output_dir.join(input.file_name().unwrap());
        allowed.insert(output.clone());
        allowed.insert(partial_path(&output));
    }
    if report_path.parent() == Some(output_dir) {
        allowed.insert(report_path.to_path_buf());
        allowed.insert(partial_path(report_path));
    }
    for entry in std::fs::read_dir(output_dir)
        .with_context(|| format!("reading output directory {}", output_dir.display()))?
    {
        let path = entry?.path();
        if !allowed.contains(&path) {
            bail!(
                "resume refuses unexpected entry in output directory: {}",
                path.display()
            );
        }
    }
    Ok(())
}

fn load_or_create_report(
    report_path: &Path,
    scan_dir: &Path,
    output_dir: &Path,
    source_root: &str,
    walker_version: &str,
    resume: bool,
) -> Result<RewriteReport> {
    let input_dir = scan_dir.to_string_lossy().into_owned();
    let output_dir_string = output_dir.to_string_lossy().into_owned();
    if report_path.exists() {
        if !resume {
            bail!(
                "rewrite report already exists; use --resume: {}",
                report_path.display()
            );
        }
        let bytes = std::fs::read(report_path)
            .with_context(|| format!("reading rewrite report {}", report_path.display()))?;
        let report: RewriteReport = serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing rewrite report {}", report_path.display()))?;
        if report.schema_version != REWRITE_REPORT_VERSION {
            bail!(
                "unsupported rewrite report schema version {}",
                report.schema_version
            );
        }
        if report.input_dir != input_dir
            || report.output_dir != output_dir_string
            || report.source_root != source_root
            || report.walker_version != walker_version
        {
            bail!("rewrite report context does not match input/output/source-root/walker-version");
        }
        return Ok(report);
    }

    if let Some(parent) = report_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating report directory {}", parent.display()))?;
    }
    let now = unix_seconds_now();
    Ok(RewriteReport {
        schema_version: REWRITE_REPORT_VERSION,
        input_dir,
        output_dir: output_dir_string,
        source_root: source_root.to_string(),
        walker_version: walker_version.to_string(),
        started_unix_seconds: now,
        updated_unix_seconds: now,
        complete: false,
        total_rows: 0,
        total_output_bytes: 0,
        shards: Vec::new(),
    })
}

fn checkpoint_is_valid(
    checkpoint: &ShardCheckpoint,
    output: &Path,
    shard_index: u32,
    fingerprint: &SourceFingerprint,
) -> bool {
    if checkpoint.shard_index != shard_index
        || checkpoint.input_bytes != fingerprint.bytes
        || checkpoint.input_modified_unix_ns != fingerprint.modified_unix_ns
        || checkpoint.output_name != output.file_name().unwrap().to_string_lossy()
    {
        return false;
    }
    let Ok(metadata) = std::fs::metadata(output) else {
        return false;
    };
    if !metadata.is_file() || metadata.len() != checkpoint.output_bytes {
        return false;
    }
    if file_sha256(output).ok().as_deref() != Some(checkpoint.output_sha256.as_str()) {
        return false;
    }
    let Ok(reader) = migration_core::shard::ShardReader::open(output) else {
        return false;
    };
    reader.rows() == checkpoint.rows && reader.shard_index() == Some(shard_index)
}

fn update_report(
    report: &mut RewriteReport,
    checkpoints: &HashMap<String, ShardCheckpoint>,
    complete: bool,
) {
    report.shards = checkpoints.values().cloned().collect();
    report
        .shards
        .sort_by_key(|checkpoint| checkpoint.shard_index);
    report.total_rows = report.shards.iter().map(|checkpoint| checkpoint.rows).sum();
    report.total_output_bytes = report
        .shards
        .iter()
        .map(|checkpoint| checkpoint.output_bytes)
        .sum();
    report.complete = complete;
    report.updated_unix_seconds = unix_seconds_now();
}

fn write_report_atomic(report_path: &Path, report: &RewriteReport) -> Result<()> {
    let partial = partial_path(report_path);
    if partial.exists() {
        std::fs::remove_file(&partial)
            .with_context(|| format!("removing stale report partial {}", partial.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(report)?;
    let mut file = File::create(&partial)
        .with_context(|| format!("creating report partial {}", partial.display()))?;
    file.write_all(&bytes)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    std::fs::rename(&partial, report_path).with_context(|| {
        format!(
            "atomically activating rewrite report {}",
            report_path.display()
        )
    })?;
    if let Some(parent) = report_path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

// =============================================================================
// Walker layout discovery
// =============================================================================

/// Resolve `--input` to the directory that actually contains the part
/// files. The post-RocksDB-removal walker writes
/// `<output>/scans/<scan_id>/part-rNN-SSSSS.parquet`, so callers can
/// pass either the output root or the scan directory itself. Older
/// flat layouts (part files directly under the input directory) are
/// also accepted unchanged.
fn resolve_scan_dir(input: &Path) -> Result<PathBuf> {
    if !input.is_dir() {
        bail!("input is not a directory: {}", input.display());
    }
    // If the input already contains part files, use it as-is.
    if dir_has_parquet(input)? {
        return Ok(input.to_path_buf());
    }
    // Walker output root: contains a `scans/` subdir with one or more
    // scan_id directories under it.
    let scans_root = input.join("scans");
    if scans_root.is_dir() {
        let scan_dirs: Vec<PathBuf> = std::fs::read_dir(&scans_root)
            .with_context(|| format!("reading {}", scans_root.display()))?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.is_dir())
            .collect();
        match scan_dirs.len() {
            0 => bail!(
                "walker output at {} has scans/ but no scan_id subdirs",
                input.display(),
            ),
            1 => return Ok(scan_dirs.into_iter().next().unwrap()),
            n => bail!(
                "walker output at {} has {n} scan_id subdirs under scans/; \
                 pass --input pointed at the specific scan directory instead",
                input.display(),
            ),
        }
    }
    bail!(
        "no parquet files found at {} and no scans/<scan_id>/ subdir present",
        input.display(),
    )
}

fn dir_has_parquet(dir: &Path) -> Result<bool> {
    for e in std::fs::read_dir(dir)
        .with_context(|| format!("reading {}", dir.display()))?
        .flatten()
    {
        let p = e.path();
        if p.is_file() && p.extension().and_then(|s| s.to_str()) == Some("parquet") {
            return Ok(true);
        }
    }
    Ok(false)
}

// =============================================================================
// Shard-level driver
// =============================================================================

/// Rewrite one walker shard into one canonical shard. Returns the row
/// count that was written.
fn rewrite_shard(
    input: &Path,
    output: &Path,
    shard_idx: u32,
    source_root: &[u8],
    walker_version: &str,
) -> Result<u64> {
    let file = File::open(input).with_context(|| format!("opening {}", input.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;

    let mut output_batches: Vec<RecordBatch> = Vec::new();
    let mut output_schema: Option<Arc<ArrowSchema>> = None;
    let mut row_offset_in_shard: u64 = 0;

    for batch in reader {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }
        let translated = translate_batch(&batch, shard_idx, row_offset_in_shard, source_root)?;
        row_offset_in_shard += batch.num_rows() as u64;
        output_schema.get_or_insert_with(|| translated.schema());
        output_batches.push(translated);
    }

    // If the input was empty we still emit an empty canonical shard so
    // the downstream view is consistent with the input shard set.
    let schema = match output_schema {
        Some(s) => s,
        None => Arc::new(canonical_plus_legacy_schema(&[])),
    };

    write_shard(
        output,
        schema,
        &output_batches,
        shard_idx,
        walker_version,
        row_offset_in_shard,
    )?;

    Ok(row_offset_in_shard)
}

fn write_shard(
    output: &Path,
    schema: Arc<ArrowSchema>,
    batches: &[RecordBatch],
    shard_idx: u32,
    walker_version: &str,
    row_count: u64,
) -> Result<()> {
    let kv = vec![
        KeyValue {
            key: KV_FORMAT_VERSION.into(),
            value: Some(schema::FORMAT_VERSION.to_string()),
        },
        KeyValue {
            key: KV_CONTRACT_VERSION.into(),
            value: Some(schema::CONTRACT_VERSION.to_string()),
        },
        KeyValue {
            key: KV_SHARD_INDEX.into(),
            value: Some(shard_idx.to_string()),
        },
        KeyValue {
            key: KV_WALKER_VERSION.into(),
            value: Some(walker_version.to_string()),
        },
        KeyValue {
            key: KV_ROW_COUNT.into(),
            value: Some(row_count.to_string()),
        },
    ];
    // ZSTD like the walker's own output. The builder default is
    // UNCOMPRESSED, which made the 600M index 241 GB in S3 (~400 B per
    // row against the walker's ~68) — every worker downloads a shard
    // of that before copying, and `prepare` keeps every shard on local
    // disk until it is uploaded.
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_key_value_metadata(Some(kv))
        .build();

    let file = File::create(output).with_context(|| format!("creating {}", output.display()))?;
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.close()?;
    Ok(())
}

// =============================================================================
// Translation
// =============================================================================

/// Translate one walker `RecordBatch` into one canonical `RecordBatch`.
///
/// `row_offset_in_shard` is the running row count within this shard
/// before this batch — the new batch's first row gets `row_id =
/// make_row_id(shard_idx, row_offset_in_shard)`.
fn translate_batch(
    input: &RecordBatch,
    shard_idx: u32,
    row_offset_in_shard: u64,
    source_root: &[u8],
) -> Result<RecordBatch> {
    let n = input.num_rows();

    // Required walker columns. Pluck them up front; clear errors if
    // absent or wrong type.
    let walker_path = req_string(input, "path")?;
    let walker_file_type = req_string(input, "file_type")?;
    let walker_permissions = req_u16(input, "permissions")?;
    let walker_mtime_us = opt_int64_required_col(input, "mtime_us")?;
    let walker_atime_us = opt_int64_optional_col(input, "atime_us");
    // High-precision time columns (walker schema, post-2026-05-04).
    // When present, prefer these over splitting `*_us` because they
    // preserve full nanosecond precision. When absent, fall back to
    // splitting the `*_us` value via split_us.
    let walker_mtime_sec = opt_int64_optional_col(input, "mtime_sec");
    let walker_mtime_nsec = opt_int32_optional_col(input, "mtime_nsec");
    let walker_atime_sec = opt_int64_optional_col(input, "atime_sec");
    let walker_atime_nsec = opt_int32_optional_col(input, "atime_nsec");
    let walker_inode = req_u64(input, "inode")?;
    let walker_nlink = req_u32(input, "nlink")?;
    let walker_uid = req_u32(input, "uid")?;
    let walker_gid = req_u32(input, "gid")?;
    let walker_size = req_u64(input, "size")?;

    // ----- canonical builders -----
    let mut row_id_b = UInt64Builder::with_capacity(n);
    let mut path_b = BinaryBuilder::with_capacity(n, n * 64);
    let mut mode_b = arrow::array::UInt32Builder::with_capacity(n);
    let mut file_type_b = UInt8Builder::with_capacity(n);
    let mut mtime_sec_b = Int64Builder::with_capacity(n);
    let mut mtime_nsec_b = Int32Builder::with_capacity(n);
    let mut atime_sec_b = Int64Builder::with_capacity(n);
    let mut atime_nsec_b = Int32Builder::with_capacity(n);
    let mut fsid_b = UInt64Builder::with_capacity(n);
    let mut symt_b = BinaryBuilder::with_capacity(n, 0);
    let mut xattr_b = BinaryBuilder::with_capacity(n, 0);

    for i in 0..n {
        let row_in_shard = row_offset_in_shard + i as u64;
        row_id_b.append_value(schema::make_row_id(shard_idx, row_in_shard));

        let raw_path = walker_path.value(i).as_bytes();
        let translated_path = strip_source_root(raw_path, source_root)?;
        path_b.append_value(&translated_path);

        let mime = walker_file_type.value(i);
        let tag = file_type_tag_from_mime(mime);
        file_type_b.append_value(tag as u8);

        let s_ifmt = match tag {
            FileTypeTag::Dir => libc::S_IFDIR,
            FileTypeTag::Symlink => libc::S_IFLNK,
            _ => libc::S_IFREG,
        } as u32;
        mode_b.append_value((walker_permissions.value(i) as u32) | s_ifmt);

        match (walker_mtime_sec, walker_mtime_nsec) {
            (Some(sec_arr), Some(nsec_arr)) if !sec_arr.is_null(i) && !nsec_arr.is_null(i) => {
                // High-precision path — full nanosecond fidelity from libnfs.
                mtime_sec_b.append_value(sec_arr.value(i));
                mtime_nsec_b.append_value(nsec_arr.value(i));
            }
            _ if walker_mtime_us.is_null(i) => {
                mtime_sec_b.append_null();
                mtime_nsec_b.append_null();
            }
            _ => {
                // Legacy fallback: split mtime_us into (sec, nsec).
                // Loses precision below microseconds.
                let (sec_v, nsec_v) = split_us(walker_mtime_us.value(i));
                mtime_sec_b.append_value(sec_v);
                mtime_nsec_b.append_value(nsec_v);
            }
        }

        match (walker_atime_sec, walker_atime_nsec) {
            (Some(sec_arr), Some(nsec_arr)) if !sec_arr.is_null(i) && !nsec_arr.is_null(i) => {
                // High-precision path.
                atime_sec_b.append_value(sec_arr.value(i));
                atime_nsec_b.append_value(nsec_arr.value(i));
            }
            _ => match walker_atime_us {
                Some(arr) if !arr.is_null(i) => {
                    // Legacy fallback: split atime_us.
                    let (sec_v, nsec_v) = split_us(arr.value(i));
                    atime_sec_b.append_value(sec_v);
                    atime_nsec_b.append_value(nsec_v);
                }
                _ => {
                    atime_sec_b.append_null();
                    atime_nsec_b.append_null();
                }
            },
        }

        // Walker doesn't capture fsid, symlink_target, or xattr_blob.
        // Mover handles null fsid with a one-time WARN; missing
        // symlink_target falls back to nfs_readlink at the destination.
        fsid_b.append_null();
        symt_b.append_null();
        xattr_b.append_null();
    }

    let canonical_arrays: Vec<(Field, ArrayRef)> = vec![
        (
            Field::new(schema::COL_ROW_ID, DataType::UInt64, false),
            Arc::new(row_id_b.finish()),
        ),
        (
            Field::new(schema::COL_PATH, DataType::Binary, false),
            Arc::new(path_b.finish()),
        ),
        // Pulled directly from walker: same name and width.
        (
            Field::new(schema::COL_SIZE, DataType::UInt64, false),
            Arc::new(walker_size.clone()) as ArrayRef,
        ),
        (
            Field::new(schema::COL_MODE, DataType::UInt32, false),
            Arc::new(mode_b.finish()),
        ),
        (
            Field::new(schema::COL_FILE_TYPE, DataType::UInt8, false),
            Arc::new(file_type_b.finish()),
        ),
        (
            Field::new(schema::COL_MTIME_SEC, DataType::Int64, true),
            Arc::new(mtime_sec_b.finish()),
        ),
        (
            Field::new(schema::COL_MTIME_NSEC, DataType::Int32, true),
            Arc::new(mtime_nsec_b.finish()),
        ),
        (
            Field::new(schema::COL_ATIME_SEC, DataType::Int64, true),
            Arc::new(atime_sec_b.finish()),
        ),
        (
            Field::new(schema::COL_ATIME_NSEC, DataType::Int32, true),
            Arc::new(atime_nsec_b.finish()),
        ),
        (
            Field::new(schema::COL_UID, DataType::UInt32, true),
            Arc::new(walker_uid.clone()) as ArrayRef,
        ),
        (
            Field::new(schema::COL_GID, DataType::UInt32, true),
            Arc::new(walker_gid.clone()) as ArrayRef,
        ),
        (
            Field::new(schema::COL_NLINK, DataType::UInt32, true),
            Arc::new(walker_nlink.clone()) as ArrayRef,
        ),
        (
            Field::new(schema::COL_INODE, DataType::UInt64, true),
            Arc::new(walker_inode.clone()) as ArrayRef,
        ),
        (
            Field::new(schema::COL_FSID, DataType::UInt64, true),
            Arc::new(fsid_b.finish()),
        ),
        (
            Field::new(schema::COL_XATTR_BLOB, DataType::Binary, true),
            Arc::new(xattr_b.finish()),
        ),
        (
            Field::new(schema::COL_SYMLINK_TARGET, DataType::Binary, true),
            Arc::new(symt_b.finish()),
        ),
    ];

    // Legacy passthrough: copy every input column we haven't already
    // consumed as a canonical column (size/uid/gid/nlink/inode share
    // the canonical name). Renames per SCHEMA_CONTRACT.md.
    let mut legacy: Vec<(Field, ArrayRef)> = Vec::new();
    for (idx, field) in input.schema().fields().iter().enumerate() {
        let new_name: &str = match field.name().as_str() {
            // Renames defined by the contract.
            "path" => "path_legacy",
            "file_type" => "file_type_mime",
            // Already covered by canonical columns of the same name.
            "size" | "uid" | "gid" | "nlink" | "inode" | "mtime_sec" | "mtime_nsec"
            | "atime_sec" | "atime_nsec" => continue,
            other => other,
        };
        let array = input.column(idx).clone();
        legacy.push((
            Field::new(new_name, field.data_type().clone(), field.is_nullable()),
            array,
        ));
    }

    let mut all_fields: Vec<Field> = Vec::with_capacity(canonical_arrays.len() + legacy.len());
    let mut all_arrays: Vec<ArrayRef> = Vec::with_capacity(canonical_arrays.len() + legacy.len());
    for (f, a) in canonical_arrays.into_iter().chain(legacy) {
        all_fields.push(f);
        all_arrays.push(a);
    }
    let schema = Arc::new(ArrowSchema::new(all_fields));
    let batch = RecordBatch::try_new(schema, all_arrays)?;
    Ok(batch)
}

/// Build the output schema for an input we never got to inspect (zero
/// batches). Canonical columns only; legacy passthrough is per-batch
/// data so we can't synthesize it without rows. Used for empty input
/// shards.
fn canonical_plus_legacy_schema(_legacy_fields: &[Field]) -> ArrowSchema {
    ArrowSchema::new(vec![
        Field::new(schema::COL_ROW_ID, DataType::UInt64, false),
        Field::new(schema::COL_PATH, DataType::Binary, false),
        Field::new(schema::COL_SIZE, DataType::UInt64, false),
        Field::new(schema::COL_MODE, DataType::UInt32, false),
        Field::new(schema::COL_FILE_TYPE, DataType::UInt8, false),
        Field::new(schema::COL_MTIME_SEC, DataType::Int64, true),
        Field::new(schema::COL_MTIME_NSEC, DataType::Int32, true),
        Field::new(schema::COL_ATIME_SEC, DataType::Int64, true),
        Field::new(schema::COL_ATIME_NSEC, DataType::Int32, true),
        Field::new(schema::COL_UID, DataType::UInt32, true),
        Field::new(schema::COL_GID, DataType::UInt32, true),
        Field::new(schema::COL_NLINK, DataType::UInt32, true),
        Field::new(schema::COL_INODE, DataType::UInt64, true),
        Field::new(schema::COL_FSID, DataType::UInt64, true),
        Field::new(schema::COL_XATTR_BLOB, DataType::Binary, true),
        Field::new(schema::COL_SYMLINK_TARGET, DataType::Binary, true),
    ])
}

// =============================================================================
// Pure helpers (heavily unit-tested)
// =============================================================================

/// Split microseconds-since-epoch into `(seconds, nanoseconds)` with
/// `nsec ∈ [0, 1_000_000_000)` even for negative inputs. The contract
/// forbids negative `mtime_nsec`; integer `/` and `%` would produce
/// them for pre-epoch timestamps, so use Euclidean division.
pub fn split_us(us: i64) -> (i64, i32) {
    let sec = us.div_euclid(1_000_000);
    let rem = us.rem_euclid(1_000_000) as i32;
    (sec, rem * 1000)
}

/// Strip the export-root prefix from a walker absolute path, leaving
/// the export-relative path with leading slash.
///
/// - `path == source_root` yields `b"/"`.
/// - `path == source_root + "/<rest>"` yields `b"/<rest>"`.
/// - anything else is an error: walker scanned outside the declared
///   export and we'd produce a corrupted path.
pub fn strip_source_root(path: &[u8], source_root: &[u8]) -> Result<Vec<u8>> {
    if path == source_root {
        return Ok(b"/".to_vec());
    }
    // Build "<source_root>/" and require the input to start with it.
    let mut prefix = source_root.to_vec();
    if !prefix.ends_with(b"/") {
        prefix.push(b'/');
    }
    if let Some(rest) = path.strip_prefix(prefix.as_slice()) {
        let mut out = Vec::with_capacity(rest.len() + 1);
        out.push(b'/');
        out.extend_from_slice(rest);
        Ok(out)
    } else {
        Err(anyhow!(
            "walker path does not start with --source-root: path={:?}, source_root={:?}",
            String::from_utf8_lossy(path),
            String::from_utf8_lossy(source_root),
        ))
    }
}

/// Map walker's MIME-style `file_type` string to a canonical
/// `FileTypeTag`. Walker only ever distinguishes "directory" and
/// "symlink" explicitly — every other entry is bucketed as Regular,
/// which is the limitation called out in `README.md`.
pub fn file_type_tag_from_mime(mime: &str) -> FileTypeTag {
    match mime {
        "directory" => FileTypeTag::Dir,
        "symlink" => FileTypeTag::Symlink,
        _ => FileTypeTag::Regular,
    }
}

// =============================================================================
// Column-pluck helpers — produce typed views with clear errors.
// =============================================================================

fn req_string<'a>(b: &'a RecordBatch, name: &str) -> Result<&'a StringArray> {
    let arr = b
        .column_by_name(name)
        .ok_or_else(|| anyhow!("walker shard missing required column `{name}`"))?;
    arr.as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow!("walker column `{name}` is not Utf8"))
}

fn req_u16<'a>(b: &'a RecordBatch, name: &str) -> Result<&'a UInt16Array> {
    let arr = b
        .column_by_name(name)
        .ok_or_else(|| anyhow!("walker shard missing required column `{name}`"))?;
    arr.as_any()
        .downcast_ref::<UInt16Array>()
        .ok_or_else(|| anyhow!("walker column `{name}` is not UInt16"))
}

fn req_u32<'a>(b: &'a RecordBatch, name: &str) -> Result<&'a UInt32Array> {
    let arr = b
        .column_by_name(name)
        .ok_or_else(|| anyhow!("walker shard missing required column `{name}`"))?;
    arr.as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| anyhow!("walker column `{name}` is not UInt32"))
}

fn req_u64<'a>(b: &'a RecordBatch, name: &str) -> Result<&'a UInt64Array> {
    let arr = b
        .column_by_name(name)
        .ok_or_else(|| anyhow!("walker shard missing required column `{name}`"))?;
    arr.as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| anyhow!("walker column `{name}` is not UInt64"))
}

/// Required column — must be present, may carry nulls.
fn opt_int64_required_col<'a>(b: &'a RecordBatch, name: &str) -> Result<&'a Int64Array> {
    let arr = b
        .column_by_name(name)
        .ok_or_else(|| anyhow!("walker shard missing required column `{name}`"))?;
    arr.as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow!("walker column `{name}` is not Int64"))
}

/// Optional column — absence returns None, type mismatch is silently
/// ignored (treated as absent). Walker is the only writer today and its
/// shape is known.
fn opt_int64_optional_col<'a>(b: &'a RecordBatch, name: &str) -> Option<&'a Int64Array> {
    b.column_by_name(name)
        .and_then(|a| a.as_any().downcast_ref::<Int64Array>())
}

/// Sibling of `opt_int64_optional_col` for Int32 columns. Used for the
/// nanosecond-component columns walker emits alongside the legacy
/// microsecond-encoded `*_us` columns.
fn opt_int32_optional_col<'a>(b: &'a RecordBatch, name: &str) -> Option<&'a Int32Array> {
    b.column_by_name(name)
        .and_then(|a| a.as_any().downcast_ref::<Int32Array>())
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        StringArray, UInt16Builder, UInt32Array, UInt32Builder, UInt64Array, UInt64Builder,
    };
    use migration_core::shard::ShardReader;
    use std::path::PathBuf;

    // ---------------- pure helpers ----------------

    #[test]
    fn split_us_positive_round_numbers() {
        assert_eq!(split_us(0), (0, 0));
        assert_eq!(split_us(1_000_000), (1, 0));
        assert_eq!(split_us(1_500_000), (1, 500_000_000));
    }

    #[test]
    fn split_us_handles_negative_correctly() {
        // (-1500 us) is 1500 us before epoch = 0 sec − 1500 us.
        // Per `struct timespec`, that's (-1, 998_500_000), NOT
        // (-1, -1_500_000) and NOT (0, -1_500_000).
        let (s, ns) = split_us(-1500);
        assert_eq!(s, -1);
        assert_eq!(ns, 998_500_000);
        assert!((0..1_000_000_000).contains(&ns));

        let (s, ns) = split_us(-1_000_000);
        assert_eq!((s, ns), (-1, 0));

        let (s, ns) = split_us(-1_000_001);
        assert_eq!(s, -2);
        assert_eq!(ns, 999_999_000);
    }

    #[test]
    fn strip_source_root_basic_cases() {
        let sr = b"/src-test";
        assert_eq!(
            strip_source_root(b"/src-test", sr).unwrap(),
            b"/".to_vec(),
            "path == source_root should yield /",
        );
        assert_eq!(
            strip_source_root(b"/src-test/m2-verify", sr).unwrap(),
            b"/m2-verify".to_vec(),
        );
        assert_eq!(
            strip_source_root(b"/src-test/m2-verify/empty.bin", sr).unwrap(),
            b"/m2-verify/empty.bin".to_vec(),
        );
    }

    #[test]
    fn strip_source_root_rejects_non_prefix() {
        // `/src-testing` shares a prefix but isn't under `/src-test`.
        // Without the `+ "/"` guard this would silently produce
        // `/ing/foo`.
        let err = strip_source_root(b"/src-testing/foo", b"/src-test").unwrap_err();
        assert!(
            err.to_string()
                .contains("does not start with --source-root"),
            "{err}"
        );

        let err = strip_source_root(b"/elsewhere", b"/src-test").unwrap_err();
        assert!(err.to_string().contains("does not start with"));
    }

    #[test]
    fn strip_source_root_tolerates_trailing_slash_on_root() {
        let sr = b"/src-test/";
        assert_eq!(
            strip_source_root(b"/src-test/foo", sr).unwrap(),
            b"/foo".to_vec(),
        );
    }

    fn touch(path: &Path) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, b"").unwrap();
    }

    #[test]
    fn resolve_scan_dir_accepts_flat_layout() {
        // Pre-RocksDB-removal walker layout: part files sit directly under input.
        let work = tempdir("flat");
        touch(&work.join("part-r00-00000.parquet"));
        let resolved = resolve_scan_dir(&work).expect("flat layout");
        assert_eq!(resolved, work);
    }

    #[test]
    fn resolve_scan_dir_descends_into_scans_scan_id() {
        // Post-RocksDB-removal walker writes scans/<scan_id>/part-*.parquet.
        let work = tempdir("nested");
        let scan_id = "11111111-2222-3333-4444-555555555555";
        let scan_dir = work.join("scans").join(scan_id);
        touch(&scan_dir.join("part-r00-00000.parquet"));
        std::fs::write(scan_dir.join("metadata.json"), b"{}").unwrap();
        let resolved = resolve_scan_dir(&work).expect("nested layout");
        assert_eq!(resolved, scan_dir);
    }

    #[test]
    fn resolve_scan_dir_accepts_scan_dir_directly() {
        // Passing the inner `scans/<scan_id>/` directly should also work,
        // because it satisfies the dir-has-parquet short-circuit.
        let work = tempdir("inner");
        let scan_id = "deadbeef-dead-beef-dead-beefdeadbeef";
        let scan_dir = work.join("scans").join(scan_id);
        touch(&scan_dir.join("part-r00-00000.parquet"));
        let resolved = resolve_scan_dir(&scan_dir).expect("scan_dir directly");
        assert_eq!(resolved, scan_dir);
    }

    #[test]
    fn resolve_scan_dir_rejects_multiple_scans() {
        // Pointing at an output root that accumulated multiple scan_ids
        // is ambiguous — the harness must point at the specific one.
        let work = tempdir("multi");
        for scan_id in ["aaaa-1", "bbbb-2"] {
            let scan_dir = work.join("scans").join(scan_id);
            touch(&scan_dir.join("part-r00-00000.parquet"));
        }
        let err = resolve_scan_dir(&work).expect_err("multi-scan must be rejected");
        let msg = format!("{err:#}");
        assert!(msg.contains("2 scan_id subdirs"), "{msg}");
    }

    #[test]
    fn resolve_scan_dir_rejects_empty_input() {
        let work = tempdir("empty");
        let err = resolve_scan_dir(&work).expect_err("empty must be rejected");
        let msg = format!("{err:#}");
        assert!(msg.contains("no parquet files found"), "{msg}");
    }

    #[test]
    fn file_type_tag_translation_table() {
        assert_eq!(file_type_tag_from_mime("directory"), FileTypeTag::Dir);
        assert_eq!(file_type_tag_from_mime("symlink"), FileTypeTag::Symlink);
        // Walker actually emits "file" for regular files; spec table
        // says "anything else" -> Regular.
        assert_eq!(file_type_tag_from_mime("file"), FileTypeTag::Regular);
        assert_eq!(
            file_type_tag_from_mime("application/pdf"),
            FileTypeTag::Regular,
        );
        assert_eq!(file_type_tag_from_mime("text/plain"), FileTypeTag::Regular);
        assert_eq!(file_type_tag_from_mime(""), FileTypeTag::Regular);
    }

    #[test]
    fn row_id_matches_make_row_id() {
        // Sanity: the formula in translate_batch must agree with the
        // canonical helper. If make_row_id ever changes, this catches
        // the drift.
        for shard in [0u32, 5, 1234] {
            for offset in [0u64, 1, 999_999, (1 << 20)] {
                assert_eq!(
                    schema::make_row_id(shard, offset),
                    ((shard as u64) << 40) | offset,
                );
            }
        }
    }

    // ---------------- integration: walker batch → parquet → ShardReader ----------------

    /// Build a minimal walker-shape batch with three rows: a directory
    /// (which should map to the export root after stripping), a
    /// regular file, and a symlink. Only the columns the shim consumes
    /// or passes through need to be present.
    fn synthetic_walker_batch(source_root: &str) -> RecordBatch {
        let dir_path = source_root.to_string();
        let file_path = format!("{source_root}/file.bin");
        let link_path = format!("{source_root}/link");

        let path = StringArray::from(vec![
            dir_path.as_str(),
            file_path.as_str(),
            link_path.as_str(),
        ]);
        let file_type = StringArray::from(vec!["directory", "file", "symlink"]);
        let mut perms = UInt16Builder::new();
        perms.append_value(0o755);
        perms.append_value(0o644);
        perms.append_value(0o777);
        let perms = perms.finish();

        let mut mtime = arrow::array::Int64Builder::new();
        mtime.append_value(1_700_000_000_000_001);
        mtime.append_value(1_700_000_000_000_002);
        mtime.append_null();
        let mtime = mtime.finish();

        let mut atime = arrow::array::Int64Builder::new();
        atime.append_value(1_700_000_000_000_010);
        atime.append_null();
        atime.append_value(-1500); // negative atime — exercises split_us euclidean math
        let atime = atime.finish();

        let mut inode = UInt64Builder::new();
        inode.append_value(100);
        inode.append_value(101);
        inode.append_value(102);
        let inode = inode.finish();

        let mut nlink = UInt32Builder::new();
        nlink.append_value(2);
        nlink.append_value(1);
        nlink.append_value(1);
        let nlink = nlink.finish();

        let uid = UInt32Array::from(vec![1000u32, 1000, 1000]);
        let gid = UInt32Array::from(vec![1000u32, 1000, 1000]);
        let size = UInt64Array::from(vec![4096u64, 1234, 0]);

        // A couple of legacy passthrough columns to verify they survive.
        let filename = StringArray::from(vec!["", "file.bin", "link"]);
        let parent_path = StringArray::from(vec!["/", source_root, source_root]);

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("path", DataType::Utf8, false),
            Field::new("file_type", DataType::Utf8, false),
            Field::new("permissions", DataType::UInt16, false),
            Field::new("mtime_us", DataType::Int64, true),
            Field::new("atime_us", DataType::Int64, true),
            Field::new("inode", DataType::UInt64, false),
            Field::new("nlink", DataType::UInt32, false),
            Field::new("uid", DataType::UInt32, false),
            Field::new("gid", DataType::UInt32, false),
            Field::new("size", DataType::UInt64, false),
            Field::new("filename", DataType::Utf8, false),
            Field::new("parent_path", DataType::Utf8, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(path) as ArrayRef,
                Arc::new(file_type),
                Arc::new(perms),
                Arc::new(mtime),
                Arc::new(atime),
                Arc::new(inode),
                Arc::new(nlink),
                Arc::new(uid),
                Arc::new(gid),
                Arc::new(size),
                Arc::new(filename),
                Arc::new(parent_path),
            ],
        )
        .unwrap()
    }

    /// Copy of `batch` with the named column removed — the "walker
    /// stopped emitting a column" drift shape.
    fn drop_column(batch: &RecordBatch, name: &str) -> RecordBatch {
        let idx = batch.schema().index_of(name).unwrap();
        let keep: Vec<usize> = (0..batch.num_columns()).filter(|&i| i != idx).collect();
        batch.project(&keep).unwrap()
    }

    /// Copy of `batch` with the named column replaced by `array`
    /// (same name, different arrow type) — the "walker changed a
    /// column's type" drift shape.
    fn replace_column(batch: &RecordBatch, name: &str, array: ArrayRef) -> RecordBatch {
        let idx = batch.schema().index_of(name).unwrap();
        let mut fields: Vec<Field> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields[idx] = Field::new(name, array.data_type().clone(), array.null_count() > 0);
        let mut columns = batch.columns().to_vec();
        columns[idx] = array;
        RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).unwrap()
    }

    /// Write a walker-shape batch to `<dir>/part-r00-00000.parquet`
    /// the way the walker would (no KV footer — the shim adds that).
    fn write_walker_parquet(dir: &Path, batch: &RecordBatch) -> PathBuf {
        std::fs::create_dir_all(dir).unwrap();
        let path = dir.join("part-r00-00000.parquet");
        let file = File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
        path
    }

    fn tempdir(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        let p = std::env::temp_dir().join(format!(
            "mig-walker-rewrite-{tag}-{}-{nanos:08x}",
            std::process::id(),
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    /// End-to-end: build a walker-shape parquet, run the shim,
    /// open the result with `ShardReader::open`, walk it.
    ///
    /// This is the contract conformance check for the shim per
    /// SHIM_PLAN.md "Tests / Integration test".
    #[test]
    fn round_trip_through_shard_reader() {
        let source_root = "/src-test";
        let work = tempdir("rt");
        let in_dir = work.join("in");
        let out_dir = work.join("out");
        std::fs::create_dir_all(&in_dir).unwrap();

        // Write the synthetic walker shard to disk under the name the
        // shim will see.
        let in_path = in_dir.join("part-r00-00000.parquet");
        let batch = synthetic_walker_batch(source_root);
        let file = File::create(&in_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Drive rewrite_shard directly (skipping the CLI parser).
        let out_path = out_dir.join("part-r00-00000.parquet");
        std::fs::create_dir_all(&out_dir).unwrap();
        let rows = rewrite_shard(&in_path, &out_path, 0, source_root.as_bytes(), "test")
            .expect("rewrite_shard");
        assert_eq!(rows, 3);

        // Open with the production reader. This exercises:
        //   - REQUIRED_COLUMNS validation
        //   - KV footer parsing (format/contract/shard_index/walker_version/row_count)
        //   - first-row shard_index high-bits cross-check
        //   - per-row file_type range check (1..=7)
        let reader = ShardReader::open(&out_path).expect("ShardReader::open");
        assert_eq!(reader.rows(), 3);
        assert_eq!(reader.shard_index(), Some(0));

        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            File::open(&out_path).unwrap(),
        )
        .unwrap();
        // Every column chunk is ZSTD: the reader above (the workers')
        // just decoded it, and the footer says so.
        let footer = builder.metadata().row_group(0);
        for column in footer.columns() {
            assert!(
                matches!(column.compression(), Compression::ZSTD(_)),
                "{} is {:?}, expected ZSTD",
                column.column_path(),
                column.compression()
            );
        }
        let parquet_schema = builder.schema().clone();
        let mut field_names = std::collections::HashSet::new();
        for field in parquet_schema.fields() {
            assert!(
                field_names.insert(field.name()),
                "rewritten schema must not contain duplicate field: {}",
                field.name()
            );
        }

        let rows: Vec<_> = reader
            .into_rows()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .expect("iterate rows");
        assert_eq!(rows.len(), 3);

        // Row 0: directory at the export root.
        assert_eq!(rows[0].path, b"/", "dir at root strips to /");
        assert_eq!(rows[0].file_type, FileTypeTag::Dir);
        assert_eq!(rows[0].mode & libc::S_IFMT, libc::S_IFDIR);
        assert_eq!(rows[0].mode & 0o7777, 0o755);
        assert_eq!(rows[0].mtime_sec, Some(1_700_000_000));
        assert_eq!(rows[0].mtime_nsec, Some(1000)); // 1 us = 1000 ns

        // Row 1: regular file under root.
        assert_eq!(rows[1].path, b"/file.bin");
        assert_eq!(rows[1].file_type, FileTypeTag::Regular);
        assert_eq!(rows[1].mode & libc::S_IFMT, libc::S_IFREG);
        assert_eq!(rows[1].mode & 0o7777, 0o644);
        assert_eq!(rows[1].size, 1234);
        assert_eq!(rows[1].atime_sec, None, "null mtime/atime preserved");
        assert_eq!(rows[1].atime_nsec, None);

        // Row 2: symlink, with a *negative* atime to exercise the
        // euclidean split.
        assert_eq!(rows[2].path, b"/link");
        assert_eq!(rows[2].file_type, FileTypeTag::Symlink);
        assert_eq!(rows[2].mode & libc::S_IFMT, libc::S_IFLNK);
        assert_eq!(rows[2].mode & 0o7777, 0o777);
        assert_eq!(rows[2].atime_sec, Some(-1));
        assert_eq!(rows[2].atime_nsec, Some(998_500_000));

        // row_id high bits == shard_index for every row.
        for r in &rows {
            assert_eq!((r.row_id >> 40) as u32, 0);
        }
        // row_id low bits cover the shard sequentially.
        assert_eq!(rows[0].row_id & ((1 << 40) - 1), 0);
        assert_eq!(rows[1].row_id & ((1 << 40) - 1), 1);
        assert_eq!(rows[2].row_id & ((1 << 40) - 1), 2);

        // fsid/symlink_target/xattr_blob are nulled by the shim — the
        // mover handles each gracefully (WARN, readlink fallback,
        // null-safe respectively). See SHIM_PLAN.md.
        for r in &rows {
            assert_eq!(r.fsid, None);
            assert_eq!(r.xattr_blob, None);
            assert_eq!(r.symlink_target, None);
        }
    }

    #[test]
    fn rewrite_report_resumes_valid_shards_and_repairs_corruption() {
        let source_root = "/src-test";
        let work = tempdir("resume");
        let in_dir = work.join("in");
        let out_dir = work.join("out");
        let batch = synthetic_walker_batch(source_root);
        write_walker_parquet(&in_dir, &batch);

        let mut args = Cli {
            input: in_dir,
            output: out_dir.clone(),
            source_root: source_root.to_string(),
            walker_version: "test-walker".to_string(),
            resume: false,
            report: None,
            verbose: false,
        };
        run_rewrite(&args).expect("initial rewrite");

        let report_path = out_dir.join("rewrite-report.json");
        let report: RewriteReport =
            serde_json::from_slice(&std::fs::read(&report_path).unwrap()).unwrap();
        assert!(report.complete);
        assert_eq!(report.total_rows, 3);
        assert_eq!(report.shards.len(), 1);
        let output = out_dir.join("part-r00-00000.parquet");
        let original_bytes = report.shards[0].output_bytes;
        let original_modified = std::fs::metadata(&output).unwrap().modified().unwrap();

        // A fully valid checkpoint is skipped: the canonical shard is not
        // opened for writing, so its modification time remains identical.
        // A stale uncommitted partial is safe to discard once the final and
        // checkpoint both validate.
        std::fs::write(partial_path(&output), b"stale partial").unwrap();
        args.resume = true;
        std::thread::sleep(std::time::Duration::from_millis(20));
        run_rewrite(&args).expect("resume valid output");
        assert_eq!(
            std::fs::metadata(&output).unwrap().modified().unwrap(),
            original_modified
        );
        assert!(!partial_path(&output).exists());

        // A changed output size invalidates the checkpoint. Resume rewrites
        // that shard through a partial and atomically restores a valid file.
        use std::io::Write as _;
        std::fs::OpenOptions::new()
            .append(true)
            .open(&output)
            .unwrap()
            .write_all(b"corrupt")
            .unwrap();
        assert_ne!(std::fs::metadata(&output).unwrap().len(), original_bytes);
        run_rewrite(&args).expect("resume repairs corrupt output");
        assert_eq!(std::fs::metadata(&output).unwrap().len(), original_bytes);
        assert!(migration_core::shard::ShardReader::open(&output).is_ok());
    }

    #[test]
    fn rewrite_resume_refuses_untracked_output_entries() {
        let source_root = "/src-test";
        let work = tempdir("resume-unknown");
        let in_dir = work.join("in");
        let out_dir = work.join("out");
        write_walker_parquet(&in_dir, &synthetic_walker_batch(source_root));
        std::fs::create_dir_all(&out_dir).unwrap();
        std::fs::write(out_dir.join("operator-notes.txt"), b"do not overwrite").unwrap();

        let args = Cli {
            input: in_dir,
            output: out_dir,
            source_root: source_root.to_string(),
            walker_version: "test-walker".to_string(),
            resume: true,
            report: None,
            verbose: false,
        };
        let error = run_rewrite(&args).expect_err("unknown output must block resume");
        assert!(
            format!("{error:#}").contains("unexpected entry"),
            "{error:#}"
        );
    }

    #[test]
    fn walker_shim_rejects_missing_input_column() {
        // Every column `translate_batch` requires to be PRESENT.
        // Dropping any one of them must fail the shard rewrite with
        // the "missing required column" error naming the column —
        // never silently produce a canonical shard.
        let required = [
            "path",
            "file_type",
            "permissions",
            "mtime_us",
            "inode",
            "nlink",
            "uid",
            "gid",
            "size",
        ];
        let source_root = "/src-test";
        for missing in required {
            let work = tempdir(&format!("miss-{missing}"));
            let batch = drop_column(&synthetic_walker_batch(source_root), missing);
            let in_path = write_walker_parquet(&work.join("in"), &batch);
            let out_dir = work.join("out");
            std::fs::create_dir_all(&out_dir).unwrap();
            let err = rewrite_shard(
                &in_path,
                &out_dir.join("part-r00-00000.parquet"),
                0,
                source_root.as_bytes(),
                "test",
            )
            .expect_err("shim must reject a walker shard missing a required column");
            let msg = format!("{err:#}");
            assert!(
                msg.contains(&format!("missing required column `{missing}`")),
                "dropping `{missing}`: unexpected error: {msg}",
            );
        }
    }

    #[test]
    fn walker_shim_rejects_mistyped_input_column() {
        // Same drift class, wrong-type flavor: the column is present
        // under the right name but carries a different arrow type.
        // The pluck helpers must fail with the "is not <Type>" error,
        // not decode garbage. Three representative type families:
        // Utf8 (path), UInt16 (permissions), Int64 (mtime_us).
        let source_root = "/src-test";
        let cases: [(&str, ArrayRef, &str); 3] = [
            (
                "path",
                // Correct bytes, wrong physical type (Binary vs Utf8).
                Arc::new(arrow::array::BinaryArray::from_vec(vec![
                    b"/src-test".as_slice(),
                    b"/src-test/file.bin",
                    b"/src-test/link",
                ])),
                "walker column `path` is not Utf8",
            ),
            (
                "permissions",
                // Wider integer than the walker schema promises.
                Arc::new(UInt32Array::from(vec![0o755u32, 0o644, 0o777])),
                "walker column `permissions` is not UInt16",
            ),
            (
                "mtime_us",
                Arc::new(StringArray::from(vec!["1700000000000001"; 3])),
                "walker column `mtime_us` is not Int64",
            ),
        ];
        for (name, array, want) in cases {
            let work = tempdir(&format!("mistyped-{name}"));
            let batch = replace_column(&synthetic_walker_batch(source_root), name, array);
            let in_path = write_walker_parquet(&work.join("in"), &batch);
            let out_dir = work.join("out");
            std::fs::create_dir_all(&out_dir).unwrap();
            let err = rewrite_shard(
                &in_path,
                &out_dir.join("part-r00-00000.parquet"),
                0,
                source_root.as_bytes(),
                "test",
            )
            .expect_err("shim must reject a walker shard with a mistyped required column");
            let msg = format!("{err:#}");
            assert!(
                msg.contains(want),
                "retyping `{name}`: unexpected error: {msg}"
            );
        }
    }

    #[test]
    fn rejects_path_outside_source_root() {
        let work = tempdir("oob");
        let in_dir = work.join("in");
        let out_dir = work.join("out");
        std::fs::create_dir_all(&in_dir).unwrap();
        std::fs::create_dir_all(&out_dir).unwrap();

        // Build a walker batch where one path is *not* under
        // `--source-root` — the shim should refuse rather than emit a
        // garbage path.
        let batch = synthetic_walker_batch("/src-test");
        let in_path = in_dir.join("part-r00-00000.parquet");
        let file = File::create(&in_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let out_path = out_dir.join("part-r00-00000.parquet");
        let err = rewrite_shard(&in_path, &out_path, 0, b"/wrong-root", "test")
            .expect_err("must reject wrong --source-root");
        assert!(
            format!("{err:#}").contains("does not start with --source-root"),
            "unexpected error: {err:#}"
        );
    }
}
