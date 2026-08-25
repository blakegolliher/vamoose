//! `vamoose prepare` — turn the source tree into the immutable index
//! the workers consume: scan with nfs-walker, convert to the canonical
//! schema, upload the shards with verification, and publish
//! `manifest.json` with a conditional create.
//!
//! Every stage checkpoints under `<work_dir>/<run_id>/` and re-running
//! the command resumes where it stopped:
//!
//! ```text
//! <work_dir>/<run_id>/
//!   run.json          identity: run id, source, destination, bucket
//!   scan/attempt-NNNN/walk.parquet   nfs-walker output (one dir per attempt)
//!   scan.json         which attempt completed, walker version + digest
//!   canonical/        canonical shards in flight (mig-walker-rewrite,
//!                     resumable); each is removed once the bucket holds it
//!   rewrite.json      mig-walker-rewrite's own checkpoint
//!   upload.json       per-shard upload checkpoint
//!   manifest.json     the manifest as published
//! <work_dir>/latest   run id of the most recent run, for implicit resume
//! ```
//!
//! The rewrite and the upload overlap: shards are uploaded as the
//! rewrite reports them and deleted locally once verified in the
//! bucket, so `work_dir` holds the scan plus a shard or two, never the
//! whole index (`--keep-index` keeps the shards).
//!
//! The moment `manifest.json` lands in the bucket, enabled workers start
//! claiming shards and the coordinator seeds the job from it.

pub(crate) mod checkpoint;
pub(crate) mod tools;
mod upload;

use crate::config::Config;
use anyhow::{Context, Result};
use checkpoint::{read_json_opt, sha256_file, utc_now, write_json_atomic};
use clap::Args as ClapArgs;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(ClapArgs, Debug)]
pub struct Args {
    /// Run identifier; also the coordinator job id. Default: resume the
    /// most recent unfinished run, else `run-<UTC timestamp>`.
    #[arg(long)]
    pub run_id: Option<String>,

    /// Start a new run even if the previous one is unfinished.
    #[arg(long)]
    pub fresh: bool,

    /// Use an existing nfs-walker output directory instead of scanning.
    #[arg(long, value_name = "DIR")]
    pub scan_dir: Option<PathBuf>,

    /// Override `[prepare] source_root`.
    #[arg(long)]
    pub source_root: Option<String>,

    /// Override `[prepare] dest_root`.
    #[arg(long)]
    pub dest_root: Option<String>,

    /// Override `[prepare] walker_bin`.
    #[arg(long)]
    pub walker_bin: Option<PathBuf>,

    /// Override `[prepare] work_dir`.
    #[arg(long)]
    pub work_dir: Option<PathBuf>,

    /// Keep the canonical shards under `<work_dir>/<run_id>/canonical/`
    /// after they are uploaded (default: remove each once the bucket
    /// holds it).
    #[arg(long)]
    pub keep_index: bool,
}

/// Identity of one run, written first and compared on every resume so
/// a config edit mid-run cannot silently mix two migrations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RunSpec {
    run_id: String,
    created_utc: String,
    bucket: String,
    endpoint: String,
    source: upload::EndpointSpec,
    dest: upload::EndpointSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ScanCheckpoint {
    complete: bool,
    /// Directory holding the part files (what the rewrite reads).
    scan_dir: PathBuf,
    /// `None` when the scan was supplied with `--scan-dir` and no
    /// walker could be located to describe it.
    walker_bin: Option<PathBuf>,
    walker_sha256: Option<String>,
    walker_version: String,
    scan_url: String,
    finished_utc: String,
}

/// Resolved settings for this invocation (flags over `[prepare]`).
#[derive(Debug, Clone)]
struct Settings {
    work_dir: PathBuf,
    source_root: String,
    dest_root: String,
    walker_bin: Option<PathBuf>,
    walker_workers: usize,
    exclude: Vec<String>,
    shard_size_mb: u64,
}

impl Settings {
    fn resolve(args: &Args, cfg: &Config) -> Self {
        let p = cfg.prepare();
        Self {
            work_dir: args.work_dir.clone().unwrap_or(p.work_dir),
            source_root: normalize_root(args.source_root.as_deref().unwrap_or(&p.source_root)),
            dest_root: normalize_root(args.dest_root.as_deref().unwrap_or(&p.dest_root)),
            walker_bin: args
                .walker_bin
                .clone()
                .or(p.walker_bin)
                .or_else(|| cfg.walker().and_then(|w| w.binary_path.clone())),
            walker_workers: p.walker_workers,
            exclude: p.exclude,
            shard_size_mb: p.shard_size_mb,
        }
    }
}

/// Roots are absolute paths inside the export: `/`, `/a/b`. Accept
/// `a/b` and trailing slashes from operators.
fn normalize_root(raw: &str) -> String {
    let trimmed = raw.trim().trim_matches('/');
    if trimmed.is_empty() {
        "/".to_string()
    } else {
        format!("/{trimmed}")
    }
}

fn default_run_id() -> String {
    format!("run-{}", chrono::Utc::now().format("%Y%m%dT%H%M%SZ"))
}

/// Which run id this invocation works on: explicit, else the run whose
/// manifest the bucket already holds, else the latest unfinished local
/// one (unless `--fresh`), else a new one.
///
/// One migration per bucket, so a published manifest *is* the run: a
/// no-arg `prepare` on any node lands on it (already prepared, or
/// verify/resume from local checkpoints) instead of minting an id that
/// the manifest check below could only refuse. Found on the rig: after
/// a finished run, `prepare` minted a fresh id, refused, and left
/// `latest` pointing at that phantom so every later call refused too.
fn choose_run_id(
    explicit: Option<&str>,
    fresh: bool,
    published: Option<&str>,
    work_dir: &Path,
) -> Result<(String, bool)> {
    if let Some(id) = explicit {
        return Ok((id.to_string(), false));
    }
    if let Some(id) = published {
        return Ok((id.to_string(), true));
    }
    if !fresh {
        if let Ok(latest) = std::fs::read_to_string(work_dir.join("latest")) {
            let latest = latest.trim().to_string();
            if !latest.is_empty() {
                let done = read_json_opt::<upload::UploadCheckpoint>(
                    &work_dir.join(&latest).join("upload.json"),
                )?
                .is_some_and(|cp| cp.complete);
                if !done {
                    return Ok((latest, true));
                }
            }
        }
    }
    Ok((default_run_id(), false))
}

pub async fn run(args: Args, config_path: Option<PathBuf>) -> Result<()> {
    let (cfg, cfg_path) = Config::load_with_path(config_path)?;
    let settings = Settings::resolve(&args, &cfg);
    let storage = cfg.storage().clone();
    let (worker_cfg, _) = cfg
        .into_worker_config()
        .with_context(|| format!("{} must carry [mover] src_url/dst_url", cfg_path.display()))?;

    // Root is required by nfs-walker (reserved ports for AUTH_SYS).
    // SAFETY: geteuid has no preconditions.
    if unsafe { libc::geteuid() } != 0 {
        tracing::warn!("vamoose prepare is not running as root; nfs-walker usually needs sudo");
    }

    let s3 = migration_core::s3::S3Client::from_config(
        &storage.endpoint,
        &storage.region,
        &storage.bucket,
        storage.profile.as_deref(),
        storage.verify_tls,
    )
    .await?;
    let published = published_manifest(&s3).await?;
    let (run_id, resumed) = choose_run_id(
        args.run_id.as_deref(),
        args.fresh,
        published.as_ref().map(|m| m.run_id.as_str()),
        &settings.work_dir,
    )?;
    migration_coord::schema::JobId::new(run_id.clone())
        .map_err(|e| anyhow::anyhow!("run id {run_id:?} is not usable as a job id: {e}"))?;
    let run_dir = settings.work_dir.join(&run_id);
    let created_now = !run_dir.exists();
    std::fs::create_dir_all(&run_dir).with_context(|| format!("creating {}", run_dir.display()))?;

    let spec = RunSpec {
        run_id: run_id.clone(),
        created_utc: utc_now(),
        bucket: storage.bucket.clone(),
        endpoint: storage.endpoint.clone(),
        source: upload::EndpointSpec {
            url: worker_cfg.mover.src_url.clone(),
            root: settings.source_root.clone(),
        },
        dest: upload::EndpointSpec {
            url: worker_cfg.mover.dst_url.clone(),
            root: settings.dest_root.clone(),
        },
    };
    let spec = ensure_run_spec(&run_dir.join("run.json"), spec)?;
    // `latest` is written only once the bucket agrees this is the run,
    // so a refusal below never leaves a pointer at a dir nothing owns.
    let remember_latest = || -> Result<()> {
        write_json_atomic(
            &settings.work_dir.join("latest.json"),
            &serde_json::json!({"run_id": run_id}),
        )?;
        std::fs::write(settings.work_dir.join("latest"), format!("{run_id}\n"))?;
        Ok(())
    };

    println!(
        "vamoose prepare\n  run     {run_id}{}\n  source  {}\n  dest    {}\n  bucket  s3://{}\n  work    {}\n",
        if resumed { " (resuming)" } else { "" },
        tools::scan_url(&spec.source.url, &spec.source.root),
        tools::scan_url(&spec.dest.url, &spec.dest.root),
        spec.bucket,
        run_dir.display()
    );

    // Talk to the bucket before any long stage: bad credentials fail
    // here, and a bucket that already belongs to another run is refused
    // before a scan is wasted on it. One run per bucket.
    match published {
        Some(existing) if existing.run_id == run_id => {
            remember_latest()?;
            let done = read_json_opt::<upload::UploadCheckpoint>(&run_dir.join("upload.json"))?
                .is_some_and(|cp| cp.complete);
            if done {
                println!(
                    "already prepared: s3://{}/manifest.json is run {run_id} ({} shards, {} rows). \
                     Nothing to do; workers claim from it.",
                    spec.bucket,
                    existing.shards.len(),
                    existing.total_rows
                );
                return Ok(());
            }
            println!(
                "  bucket already holds this run's manifest; verifying the index against it\n"
            );
        }
        // Only reachable with an explicit --run-id that is not the
        // bucket's; a no-arg call resolves to the published run above.
        Some(existing) => {
            if created_now {
                let _ = std::fs::remove_dir_all(&run_dir);
            }
            anyhow::bail!(
                "bucket s3://{} already holds manifest.json for run {:?} ({} shards, {} rows). \
                 One migration per bucket: use a fresh bucket for a new run, or re-run with \
                 --run-id {:?} (or no --run-id) to verify or resume that one.",
                spec.bucket,
                existing.run_id,
                existing.shards.len(),
                existing.total_rows,
                existing.run_id,
            )
        }
        None => remember_latest()?,
    }

    // ---- 1. scan ---------------------------------------------------
    println!("[1/3] scan");
    let scan = ensure_scan(&run_dir, &spec, &settings, args.scan_dir.as_deref()).await?;
    println!(
        "  scan dir {}\n  walker   {}\n",
        scan.scan_dir.display(),
        scan.walker_version
    );

    // ---- 2. rewrite, uploading shards as they finish ---------------
    println!("[2/3] canonical rewrite + streaming upload");
    let rewrite_report = run_dir.join("rewrite.json");
    let rewrite = tools::RewriteInvocation {
        input: scan.scan_dir.clone(),
        output: run_dir.join("canonical"),
        // Not `spec.source.root`: the scan was anchored there already.
        source_root: tools::REWRITE_SOURCE_ROOT.to_string(),
        walker_version: scan.walker_version.clone(),
        report: rewrite_report.clone(),
    };
    std::fs::create_dir_all(&rewrite.output)?;
    let context = upload::UploadContext {
        run_id: run_id.clone(),
        bucket: storage.bucket.clone(),
        endpoint: storage.endpoint.clone(),
        rewrite_identity: upload::rewrite_identity(
            &rewrite.input.to_string_lossy(),
            &rewrite.source_root,
            &rewrite.walker_version,
        ),
        source: spec.source.clone(),
        dest: spec.dest.clone(),
    };
    // Opened before the rewrite starts: a checkpoint from another run
    // or bucket is refused before any work, and the bucket's manifest
    // (if this run's) is read once.
    let mut session =
        upload::UploadSession::open(&s3, context, &run_dir.join("upload.json")).await?;
    let rewrite_bin = tools::find_sibling("mig-walker-rewrite")?;
    let mut child = tools::spawn_stage("mig-walker-rewrite", &rewrite_bin, &rewrite.args())?;
    let mut seen = std::collections::BTreeSet::new();
    let mut streamed = 0usize;
    let status = loop {
        let exited = child.try_wait().context("waiting for mig-walker-rewrite")?;
        streamed +=
            upload::stream_uploads(&mut session, &rewrite_report, &mut seen, args.keep_index)
                .await?;
        if let Some(status) = exited {
            break status;
        }
        tokio::time::sleep(STREAM_POLL).await;
    };
    if !status.success() {
        anyhow::bail!(
            "mig-walker-rewrite failed: {} exited with {status} ({streamed} shards were \
             uploaded and are checkpointed; re-run to resume)",
            rewrite_bin.display()
        );
    }
    let (_report, plans) = upload::load_rewrite_plan(&rewrite_report, &session.uploaded())?;
    println!(
        "  {} shards, {} rows ({streamed} uploaded while the rewrite ran)\n",
        plans.len(),
        plans.iter().map(|p| p.rows).sum::<u64>()
    );

    // ---- 3. remaining shards + manifest ----------------------------
    println!("[3/3] verify index and publish manifest");
    for plan in &plans {
        if plan.path.is_file() || !session.is_recorded(plan) {
            session.ensure_shard(plan).await?;
        }
        if !args.keep_index && plan.path.is_file() {
            std::fs::remove_file(&plan.path)
                .with_context(|| format!("removing uploaded shard {}", plan.path.display()))?;
        }
    }
    let manifest = session
        .publish(
            upload::CopyOptions {
                preserve_owner: worker_cfg.copy.preserve_owner,
                preserve_mode: worker_cfg.copy.preserve_mode,
                preserve_times: worker_cfg.copy.preserve_times,
                preserve_xattr: worker_cfg.copy.preserve_xattr,
            },
            &run_dir.join("manifest.json"),
        )
        .await?;

    println!(
        "\nprepared: s3://{}/manifest.json ({} shards, {} rows)\n\
         Workers claim shards from here on; watch with `vamoose tui` or `vamoose status --watch`.",
        storage.bucket,
        manifest.shards.len(),
        manifest.total_rows
    );
    Ok(())
}

/// How often the rewrite report is re-read for newly finished shards
/// while `mig-walker-rewrite` runs. A shard takes seconds to minutes to
/// produce; two seconds keeps at most one finished shard waiting.
const STREAM_POLL: std::time::Duration = std::time::Duration::from_secs(2);

/// The manifest currently in the bucket, if any.
async fn published_manifest(
    s3: &migration_core::s3::S3Client,
) -> Result<Option<migration_core::records::Manifest>> {
    use migration_core::claim::ClaimStore as _;
    let Some((body, _etag)) = s3.get(migration_core::layout::MANIFEST_KEY).await? else {
        return Ok(None);
    };
    serde_json::from_slice(&body).context("parsing manifest.json already in the bucket")
}

/// Write the run spec on first use; on resume, refuse a spec that
/// names a different source, destination, or bucket.
fn ensure_run_spec(path: &Path, fresh: RunSpec) -> Result<RunSpec> {
    match read_json_opt::<RunSpec>(path)? {
        Some(existing) => {
            let same = existing.run_id == fresh.run_id
                && existing.bucket == fresh.bucket
                && existing.endpoint == fresh.endpoint
                && existing.source == fresh.source
                && existing.dest == fresh.dest;
            if !same {
                anyhow::bail!(
                    "run {} was created for s3://{} {}{} -> {}{}; the configuration now says \
                     s3://{} {}{} -> {}{}. Restore the configuration or start a new run with \
                     --fresh.",
                    existing.run_id,
                    existing.bucket,
                    existing.source.url,
                    existing.source.root,
                    existing.dest.url,
                    existing.dest.root,
                    fresh.bucket,
                    fresh.source.url,
                    fresh.source.root,
                    fresh.dest.url,
                    fresh.dest.root,
                );
            }
            Ok(existing)
        }
        None => {
            write_json_atomic(path, &fresh)?;
            Ok(fresh)
        }
    }
}

/// Reuse a completed scan checkpoint, adopt an operator-supplied scan
/// directory, or run nfs-walker into a fresh attempt directory.
async fn ensure_scan(
    run_dir: &Path,
    spec: &RunSpec,
    settings: &Settings,
    provided: Option<&Path>,
) -> Result<ScanCheckpoint> {
    let checkpoint_path = run_dir.join("scan.json");
    if let Some(cp) = read_json_opt::<ScanCheckpoint>(&checkpoint_path)? {
        if cp.complete && tools::resolve_scan_dir(&cp.scan_dir).is_ok() {
            println!("  scan checkpoint valid; not rescanning");
            return Ok(cp);
        }
    }

    let scan_url = tools::scan_url(&spec.source.url, &spec.source.root);

    // A supplied scan only needs the walker to label its version; a
    // real scan needs it to run.
    let walker = match tools::find_walker(settings.walker_bin.as_deref()) {
        Ok(bin) => Some(bin),
        Err(e) if provided.is_some() => {
            tracing::warn!(error = %e, "no nfs-walker found; recording the supplied scan unlabelled");
            None
        }
        Err(e) => return Err(e),
    };
    let (walker_bin, walker_sha256, walker_version) = match &walker {
        Some(bin) => (
            Some(bin.clone()),
            Some(sha256_file(bin)?),
            tools::walker_version(bin).await?,
        ),
        None => (
            None,
            None,
            "unknown (scan supplied with --scan-dir)".to_string(),
        ),
    };

    let scan_dir = match provided {
        Some(dir) => {
            // Absolute, so the checkpoint survives a resume from another
            // working directory.
            let resolved = std::fs::canonicalize(tools::resolve_scan_dir(dir)?)?;
            println!("  using existing scan {}", resolved.display());
            resolved
        }
        None => {
            let walker_bin = walker.as_deref().expect("walker resolved for a real scan");
            println!(
                "  walker   {} ({}; {})",
                walker_bin.display(),
                walker_version,
                tools::walker_lock()
                    .describe(walker_sha256.as_deref().unwrap_or(""), &walker_version)
            );
            tools::check_walker_flags(walker_bin, &walker_version).await?;
            let attempt_dir = next_attempt_dir(&run_dir.join("scan"))?;
            std::fs::create_dir_all(&attempt_dir)?;
            let invocation = tools::WalkerInvocation {
                scan_url: scan_url.clone(),
                output: attempt_dir.join("walk.parquet"),
                workers: settings.walker_workers,
                exclude: settings.exclude.clone(),
                shard_size_mb: settings.shard_size_mb,
                log: attempt_dir.join("walker-progress.jsonl"),
            };
            println!(
                "  scanning {scan_url} -> {} ({} workers)",
                invocation.output.display(),
                invocation.workers
            );
            tools::run_stage("nfs-walker", walker_bin, &invocation.args()).await?;
            tools::resolve_scan_dir(&invocation.output)?
        }
    };

    let cp = ScanCheckpoint {
        complete: true,
        scan_dir,
        walker_bin,
        walker_sha256,
        walker_version,
        scan_url,
        finished_utc: utc_now(),
    };
    write_json_atomic(&checkpoint_path, &cp)?;
    Ok(cp)
}

/// `scan/attempt-0001`, `attempt-0002`, …: an interrupted scan's
/// directory is never reused, so partial output cannot pass for a
/// complete one.
fn next_attempt_dir(scan_root: &Path) -> Result<PathBuf> {
    for n in 1..10_000u32 {
        let candidate = scan_root.join(format!("attempt-{n:04}"));
        if !candidate.exists() {
            return Ok(candidate);
        }
    }
    anyhow::bail!("too many scan attempts under {}", scan_root.display())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roots_normalize_to_absolute_without_trailing_slash() {
        assert_eq!(normalize_root(""), "/");
        assert_eq!(normalize_root("/"), "/");
        assert_eq!(normalize_root("data"), "/data");
        assert_eq!(normalize_root("/data/"), "/data");
        assert_eq!(normalize_root(" /a/b/ "), "/a/b");
    }

    #[test]
    fn default_run_id_is_a_valid_job_id() {
        let id = default_run_id();
        assert!(id.starts_with("run-"));
        migration_coord::schema::JobId::new(id).unwrap();
    }

    #[test]
    fn choose_run_id_resumes_unfinished_latest_unless_fresh() {
        let dir = tempfile::tempdir().unwrap();
        // Nothing yet: a new id.
        let (id, resumed) = choose_run_id(None, false, None, dir.path()).unwrap();
        assert!(id.starts_with("run-") && !resumed);
        // Explicit wins.
        assert_eq!(
            choose_run_id(Some("mine"), false, None, dir.path()).unwrap(),
            ("mine".to_string(), false)
        );
        // The bucket's manifest outranks everything but an explicit id,
        // even --fresh: the id is taken, minting another only refuses.
        assert_eq!(
            choose_run_id(None, true, Some("run-pub"), dir.path()).unwrap(),
            ("run-pub".to_string(), true)
        );
        // An unfinished latest is resumed…
        std::fs::write(dir.path().join("latest"), "run-x\n").unwrap();
        assert_eq!(
            choose_run_id(None, false, None, dir.path()).unwrap(),
            ("run-x".to_string(), true)
        );
        // …unless --fresh.
        let (id, resumed) = choose_run_id(None, true, None, dir.path()).unwrap();
        assert!(id != "run-x" && !resumed);
        // A finished latest is not resumed.
        let cp = upload::UploadCheckpoint {
            complete: true,
            started_utc: utc_now(),
            updated_utc: utc_now(),
            context: upload::UploadContext {
                run_id: "run-x".into(),
                bucket: "b".into(),
                endpoint: "e".into(),
                rewrite_identity: "r".into(),
                source: upload::EndpointSpec {
                    url: "u".into(),
                    root: "/".into(),
                },
                dest: upload::EndpointSpec {
                    url: "v".into(),
                    root: "/".into(),
                },
            },
            manifest_created_utc: utc_now(),
            shards: vec![],
            manifest_sha256: None,
            total_rows: 0,
        };
        write_json_atomic(&dir.path().join("run-x").join("upload.json"), &cp).unwrap();
        let (id, resumed) = choose_run_id(None, false, None, dir.path()).unwrap();
        assert!(id != "run-x" && !resumed);
    }

    #[test]
    fn run_spec_is_sticky_per_run() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("run.json");
        let spec = RunSpec {
            run_id: "r".into(),
            created_utc: utc_now(),
            bucket: "b".into(),
            endpoint: "e".into(),
            source: upload::EndpointSpec {
                url: "nfs://s/x".into(),
                root: "/".into(),
            },
            dest: upload::EndpointSpec {
                url: "nfs://d/y".into(),
                root: "/".into(),
            },
        };
        let first = ensure_run_spec(&path, spec.clone()).unwrap();
        assert_eq!(first, spec);
        let mut later = spec.clone();
        later.created_utc = "2099-01-01T00:00:00Z".into();
        assert_eq!(
            ensure_run_spec(&path, later).unwrap(),
            spec,
            "timestamp is not identity"
        );
        let mut moved = spec.clone();
        moved.dest.root = "/elsewhere".into();
        let err = ensure_run_spec(&path, moved).unwrap_err();
        assert!(format!("{err:#}").contains("--fresh"));
    }

    #[test]
    fn attempt_dirs_never_reuse() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("scan");
        assert_eq!(next_attempt_dir(&root).unwrap(), root.join("attempt-0001"));
        std::fs::create_dir_all(root.join("attempt-0001")).unwrap();
        assert_eq!(next_attempt_dir(&root).unwrap(), root.join("attempt-0002"));
    }
}
