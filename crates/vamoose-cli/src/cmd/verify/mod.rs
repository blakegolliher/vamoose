//! `vamoose verify` — independent source/destination observation, metadata
//! comparison, and sampled content verification. The verifier is
//! intentionally local and single-process; its SQLite checkpoint and terminal
//! artifacts are inputs to the later distributed verifier rather than
//! throwaway output.

mod risk_history;

use crate::config::Config;
use anyhow::{Context, Result};
use clap::{Args as ClapArgs, ValueEnum};
use migration_core::claim::ClaimStore;
use migration_core::records::{Manifest, RUN_FORMAT_VERSION};
use migration_core::s3::S3Client;
use migration_verify::{
    ConsistencyBoundary, SampleRequest, VerificationMode, VerificationRequest, VerificationStatus,
};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

pub const DEFAULT_SAMPLE_FILES: u64 = 10_000;
pub const DEFAULT_SAMPLE_SEED: u64 = 0;
pub const DEFAULT_CONTENT_WORKERS: u32 = 8;
pub const DEFAULT_CONTENT_READ_SIZE: u32 = 4 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Mode {
    Metadata,
    Sample,
    Full,
}

#[derive(ClapArgs, Debug)]
#[command(
    after_help = "Exit codes:\n  0  verified\n  1  operational failure\n  2  mismatches found\n  3  source or destination changed during verification"
)]
pub struct Args {
    /// Verification depth: metadata compares every entry; sample adds SHA-256
    /// content reads of a deterministic sample plus every risk-selected file;
    /// full is reserved.
    #[arg(long, value_enum, default_value_t = Mode::Metadata)]
    pub mode: Mode,

    /// Read a local manifest instead of the configured run's S3 manifest.
    #[arg(long, value_name = "PATH")]
    pub manifest: Option<PathBuf>,

    /// Stable id used for resume and artifact identity.
    #[arg(long)]
    pub verification_id: Option<String>,

    /// Root directory for resumable verifier state.
    #[arg(long, default_value = "/var/lib/vamoose/verify")]
    pub work_dir: PathBuf,

    /// Terminal JSON report path. Defaults inside the verification work dir.
    #[arg(long, value_name = "PATH")]
    pub report: Option<PathBuf>,

    /// Mismatch JSONL path. Defaults inside the verification work dir.
    #[arg(long, value_name = "PATH")]
    pub mismatches: Option<PathBuf>,

    /// Assert that writers remain stopped for the complete verification window.
    #[arg(long)]
    pub writers_stopped: bool,

    /// Immutable source snapshot identifier; must be paired with destination.
    #[arg(long)]
    pub source_snapshot_id: Option<String>,

    /// Immutable destination snapshot identifier; must be paired with source.
    #[arg(long)]
    pub destination_snapshot_id: Option<String>,

    /// Override the mover's per-RPC timeout in milliseconds.
    #[arg(long)]
    pub rpc_timeout_ms: Option<u32>,

    /// Maximum directory names fetched per resumable libnfs batch.
    #[arg(long, default_value_t = 10_000)]
    pub directory_batch_size: u32,

    // Sample-only options stay optional at parse time so an explicit flag can
    // be told apart from a default; defaults apply after the mode is checked.
    /// Target number of content-verified files for --mode sample; mandatory
    /// risk paths may exceed it (default 10000).
    #[arg(long, value_name = "N")]
    pub sample_files: Option<u64>,

    /// Seed for the deterministic sample ranking (default 0).
    #[arg(long, value_name = "U64")]
    pub sample_seed: Option<u64>,

    /// Content workers, each owning fresh source and destination libnfs
    /// contexts (default 8).
    #[arg(long, value_name = "N")]
    pub content_workers: Option<u32>,

    /// Bytes per positional content read, 1..=2147483647 (default 4194304).
    #[arg(long, value_name = "BYTES")]
    pub content_read_size: Option<u32>,

    /// Canonical risk-evidence JSONL for a --manifest sample verification;
    /// configured S3 runs discover their history automatically.
    #[arg(long, value_name = "PATH", conflicts_with = "assume_no_risk_history")]
    pub risk_evidence: Option<PathBuf>,

    /// Assert that a --manifest sample verification has no failure,
    /// downgrade, or retried-shard history.
    #[arg(long)]
    pub assume_no_risk_history: bool,

    /// Print the terminal report as JSON instead of a human summary.
    #[arg(long)]
    pub json: bool,
}

/// Where a sample verification's risk history comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
enum RiskSource {
    /// Discovered from the configured run's claims, sinks, and retried shards.
    Configured,
    /// Operator-supplied canonical artifact for a local manifest.
    LocalArtifact(PathBuf),
    /// Operator assertion that a local manifest's run has no history.
    AssertedEmpty,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SampleOptions {
    sample_files: u64,
    sample_seed: u64,
    content_workers: u32,
    content_read_size: u32,
    risk: RiskSource,
}

/// Validates the sample-only flags against the selected mode and applies the
/// documented defaults. Metadata mode rejects every sample-only flag.
fn sample_options(args: &Args, mode: VerificationMode) -> Result<Option<SampleOptions>> {
    let supplied: Vec<&str> = [
        (args.sample_files.is_some(), "--sample-files"),
        (args.sample_seed.is_some(), "--sample-seed"),
        (args.content_workers.is_some(), "--content-workers"),
        (args.content_read_size.is_some(), "--content-read-size"),
        (args.risk_evidence.is_some(), "--risk-evidence"),
        (args.assume_no_risk_history, "--assume-no-risk-history"),
    ]
    .into_iter()
    .filter_map(|(present, flag)| present.then_some(flag))
    .collect();
    match mode {
        VerificationMode::Metadata => {
            if !supplied.is_empty() {
                anyhow::bail!(
                    "{} require --mode sample; --mode metadata reads no content",
                    supplied.join(", ")
                );
            }
            Ok(None)
        }
        VerificationMode::Sample => {
            let sample_files = args.sample_files.unwrap_or(DEFAULT_SAMPLE_FILES);
            if sample_files == 0 {
                anyhow::bail!("--sample-files must be greater than zero");
            }
            let content_workers = args.content_workers.unwrap_or(DEFAULT_CONTENT_WORKERS);
            if content_workers == 0 {
                anyhow::bail!("--content-workers must be greater than zero");
            }
            let content_read_size = args.content_read_size.unwrap_or(DEFAULT_CONTENT_READ_SIZE);
            if content_read_size == 0 || content_read_size > i32::MAX as u32 {
                anyhow::bail!(
                    "--content-read-size must be between 1 and {} bytes",
                    i32::MAX
                );
            }
            let risk = if args.manifest.is_some() {
                match (args.risk_evidence.as_ref(), args.assume_no_risk_history) {
                    (Some(path), false) => RiskSource::LocalArtifact(path.clone()),
                    (None, true) => RiskSource::AssertedEmpty,
                    (None, false) => anyhow::bail!(
                        "--mode sample with --manifest requires exactly one of \
                         --risk-evidence PATH or --assume-no-risk-history, so a local \
                         manifest cannot silently omit the run's failure and retry history"
                    ),
                    (Some(_), true) => anyhow::bail!(
                        "--risk-evidence and --assume-no-risk-history are mutually exclusive"
                    ),
                }
            } else {
                if args.risk_evidence.is_some() || args.assume_no_risk_history {
                    anyhow::bail!(
                        "--risk-evidence and --assume-no-risk-history apply only to --manifest \
                         runs; a configured S3 run discovers its risk history automatically"
                    );
                }
                RiskSource::Configured
            };
            Ok(Some(SampleOptions {
                sample_files,
                sample_seed: args.sample_seed.unwrap_or(DEFAULT_SAMPLE_SEED),
                content_workers,
                content_read_size,
                risk,
            }))
        }
    }
}

pub async fn run(args: Args, config_path: Option<PathBuf>) -> Result<VerificationStatus> {
    let mode = match args.mode {
        Mode::Metadata => VerificationMode::Metadata,
        Mode::Sample => VerificationMode::Sample,
        Mode::Full => anyhow::bail!(
            "verification mode full is not implemented yet; use --mode metadata or --mode sample"
        ),
    };
    let sample_options = sample_options(&args, mode)?;
    let config = match Config::load(config_path.clone()) {
        Ok(config) => Some(config),
        Err(_) if args.manifest.is_some() && config_path.is_none() => None,
        Err(error) => return Err(error),
    };
    let s3 = match (args.manifest.as_deref(), config.as_ref()) {
        (Some(_), _) => None,
        (None, Some(config)) => Some(s3_client(config).await?),
        (None, None) => {
            anyhow::bail!("verification requires --manifest or a configured S3 run")
        }
    };
    let manifest = match args.manifest.as_deref() {
        Some(path) => load_local_manifest(path)?,
        None => load_configured_manifest(s3.as_ref().expect("configured run")).await?,
    };
    if manifest.format_version != RUN_FORMAT_VERSION {
        anyhow::bail!(
            "manifest format {} is not supported by verifier format {}",
            manifest.format_version,
            RUN_FORMAT_VERSION
        );
    }
    let verification_id = args.verification_id.unwrap_or_else(|| {
        let run_fingerprint = hex::encode(Sha256::digest(manifest.run_id.as_bytes()));
        format!(
            "verify-{}-{}",
            chrono::Utc::now().format("%Y%m%dT%H%M%SZ"),
            &run_fingerprint[..12]
        )
    });
    migration_coord::schema::JobId::new(verification_id.clone()).map_err(|error| {
        anyhow::anyhow!(
            "verification id {verification_id:?} is not safe for artifact paths: {error}"
        )
    })?;
    let verification_dir = args.work_dir.join(&verification_id);
    let report_path = args
        .report
        .unwrap_or_else(|| verification_dir.join("report.json"));
    let mismatch_path = args
        .mismatches
        .unwrap_or_else(|| verification_dir.join("mismatches.jsonl"));
    let rpc_timeout_ms = args.rpc_timeout_ms.unwrap_or_else(|| {
        config
            .as_ref()
            .and_then(Config::mover)
            .map_or(migration_mover::DEFAULT_RPC_TIMEOUT_MS, |m| {
                m.rpc_timeout_ms
            })
    });

    // Risk history is staged before the blocking verifier starts; the
    // artifact's digest becomes part of the request identity.
    let sample = match sample_options {
        None => None,
        Some(options) => {
            std::fs::create_dir_all(&verification_dir)
                .with_context(|| format!("creating {}", verification_dir.display()))?;
            let (risk_evidence, risk_history_asserted_empty) = match options.risk {
                RiskSource::Configured => {
                    let s3 = s3.as_ref().expect("configured run has an S3 client");
                    (
                        risk_history::stage_configured_run(s3, &manifest, &verification_dir)
                            .await?,
                        false,
                    )
                }
                RiskSource::LocalArtifact(path) => {
                    let dir = verification_dir.clone();
                    (
                        tokio::task::spawn_blocking(move || {
                            migration_verify::stage_local_artifact(&dir, &path)
                        })
                        .await
                        .context("risk evidence staging panicked")??,
                        false,
                    )
                }
                RiskSource::AssertedEmpty => (
                    migration_verify::stage_asserted_empty(&verification_dir)?,
                    true,
                ),
            };
            Some(SampleRequest {
                sample_files: options.sample_files,
                sample_seed: options.sample_seed,
                content_workers: options.content_workers,
                content_read_size: options.content_read_size,
                risk_evidence,
                risk_history_asserted_empty,
            })
        }
    };

    let request = VerificationRequest {
        verification_id: verification_id.clone(),
        run_id: manifest.run_id.clone(),
        source: manifest.source,
        destination: manifest.dest,
        options: manifest.options,
        exclusions: manifest.exclusions,
        consistency: ConsistencyBoundary {
            writers_stopped: args.writers_stopped,
            source_snapshot_id: args.source_snapshot_id,
            destination_snapshot_id: args.destination_snapshot_id,
        },
        work_dir: verification_dir,
        report_path,
        mismatch_path,
        rpc_timeout_ms,
        directory_batch_size: args.directory_batch_size,
        mode,
        sample,
    };
    let result = tokio::task::spawn_blocking(move || migration_verify::verify(request))
        .await
        .context("verification task panicked")??;

    if args.json {
        println!("{}", serde_json::to_string_pretty(&result.report)?);
    } else {
        print!("{}", human_summary(&result));
    }
    Ok(result.report.status)
}

fn human_summary(result: &migration_verify::VerificationResult) -> String {
    let report = &result.report;
    let mut out = format!(
        "verification {}: {:?}\n  mode                 {}\n  source entries       {}\n  destination entries  {}\n  compared paths       {}\n  mismatches           {}\n  unreadable           {}\n  unstable             {}\n",
        report.verification_id,
        report.status,
        report.mode.as_str(),
        report.counts.source_entries,
        report.counts.destination_entries,
        report.counts.entries_on_both_sides,
        report.mismatch_count,
        report.unreadable_entries,
        report.unstable_entries,
    );
    if let Some(policy) = report.sample_policy.as_ref() {
        out.push_str(&format!(
            "  sample selected      {} of {} eligible (requested {}, seed {})\n  risk selected        {} (ineligible {}, seeded {})\n  content hashed       {} source / {} destination files, {} / {} bytes\n  content matches      {}\n  content mismatches   {}\n  content complete     {}\n  risk evidence        {} ({} bytes, sha256 {}{})\n",
            policy.selected_files,
            policy.eligible_files,
            policy.requested_files,
            policy.seed,
            policy.risk_selected_files,
            policy.risk_ineligible_files,
            policy.seeded_selected_files,
            report.source_files_hashed,
            report.destination_files_hashed,
            report.source_logical_bytes_hashed,
            report.destination_logical_bytes_hashed,
            report.content_matches,
            report.content_mismatches,
            report.content_complete,
            policy.risk_evidence_artifact.path,
            policy.risk_evidence_artifact.bytes,
            &policy.risk_evidence_artifact.sha256[..12],
            if policy.risk_history_asserted_empty {
                ", asserted empty by operator"
            } else {
                ""
            },
        ));
    }
    for error in &report.operational_errors {
        out.push_str(&format!("  operational error    {error}\n"));
    }
    out.push_str(&format!(
        "  report               {}\n  mismatch details     {}\n",
        result.report_path.display(),
        report.mismatch_artifact.path,
    ));
    out
}

async fn s3_client(config: &Config) -> Result<S3Client> {
    let storage = config.storage();
    Ok(S3Client::from_config(
        &storage.endpoint,
        &storage.region,
        &storage.bucket,
        storage.profile.as_deref(),
        storage.verify_tls,
    )
    .await?
    .with_prefix(&storage.prefix))
}

fn load_local_manifest(path: &Path) -> Result<Manifest> {
    let bytes =
        std::fs::read(path).with_context(|| format!("reading manifest {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parsing manifest {}", path.display()))
}

async fn load_configured_manifest(s3: &S3Client) -> Result<Manifest> {
    let (bytes, _) = ClaimStore::get(s3, "manifest.json")
        .await?
        .ok_or_else(|| anyhow::anyhow!("{} has no manifest.json", s3.location()))?;
    serde_json::from_slice(&bytes).context("parsing configured manifest.json")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{Cli, Command};
    use clap::{CommandFactory, Parser};

    fn parse(extra: &[&str]) -> Args {
        let mut argv = vec!["vamoose", "verify"];
        argv.extend_from_slice(extra);
        match Cli::parse_from(argv).command {
            Command::Verify(args) => args,
            _ => unreachable!("verify subcommand"),
        }
    }

    fn try_parse(extra: &[&str]) -> Result<Args, clap::Error> {
        let mut argv = vec!["vamoose", "verify"];
        argv.extend_from_slice(extra);
        Cli::try_parse_from(argv).map(|cli| match cli.command {
            Command::Verify(args) => args,
            _ => unreachable!("verify subcommand"),
        })
    }

    #[test]
    fn verify_help_documents_modes_flags_and_exit_codes() {
        let mut command = Cli::command();
        let help = command
            .find_subcommand_mut("verify")
            .expect("verify subcommand")
            .render_long_help()
            .to_string();
        for expected in [
            "metadata",
            "sample",
            "full",
            "Exit codes",
            "mismatches",
            "--sample-files",
            "--sample-seed",
            "--content-workers",
            "--content-read-size",
            "--risk-evidence",
            "--assume-no-risk-history",
        ] {
            assert!(help.contains(expected), "missing {expected:?} in:\n{help}");
        }
    }

    #[test]
    fn sample_defaults_apply_only_after_the_mode_is_validated() {
        let args = parse(&[
            "--mode",
            "sample",
            "--manifest",
            "m.json",
            "--assume-no-risk-history",
        ]);
        assert!(args.sample_files.is_none(), "clap must not inject defaults");
        let options = sample_options(&args, VerificationMode::Sample)
            .unwrap()
            .unwrap();
        assert_eq!(
            options,
            SampleOptions {
                sample_files: DEFAULT_SAMPLE_FILES,
                sample_seed: DEFAULT_SAMPLE_SEED,
                content_workers: DEFAULT_CONTENT_WORKERS,
                content_read_size: DEFAULT_CONTENT_READ_SIZE,
                risk: RiskSource::AssertedEmpty,
            }
        );
        assert_eq!(DEFAULT_SAMPLE_FILES, 10_000);
        assert_eq!(DEFAULT_CONTENT_WORKERS, 8);
        assert_eq!(DEFAULT_CONTENT_READ_SIZE, 4_194_304);

        let args = parse(&[
            "--mode",
            "sample",
            "--manifest",
            "m.json",
            "--risk-evidence",
            "risk.jsonl",
            "--sample-files",
            "25",
            "--sample-seed",
            "7",
            "--content-workers",
            "3",
            "--content-read-size",
            "65536",
        ]);
        let options = sample_options(&args, VerificationMode::Sample)
            .unwrap()
            .unwrap();
        assert_eq!(options.sample_files, 25);
        assert_eq!(options.sample_seed, 7);
        assert_eq!(options.content_workers, 3);
        assert_eq!(options.content_read_size, 65_536);
        assert_eq!(
            options.risk,
            RiskSource::LocalArtifact(PathBuf::from("risk.jsonl"))
        );

        // A configured run discovers its own history.
        let args = parse(&["--mode", "sample"]);
        assert_eq!(
            sample_options(&args, VerificationMode::Sample)
                .unwrap()
                .unwrap()
                .risk,
            RiskSource::Configured
        );
    }

    #[test]
    fn metadata_mode_rejects_every_sample_only_flag() {
        let args = parse(&[]);
        assert!(sample_options(&args, VerificationMode::Metadata)
            .unwrap()
            .is_none());
        for flags in [
            &["--sample-files", "5"][..],
            &["--sample-seed", "1"],
            &["--content-workers", "2"],
            &["--content-read-size", "4096"],
            &["--risk-evidence", "risk.jsonl"],
            &["--assume-no-risk-history"],
        ] {
            let args = parse(flags);
            let error = sample_options(&args, VerificationMode::Metadata)
                .unwrap_err()
                .to_string();
            assert!(error.contains(flags[0]), "{error}");
            assert!(error.contains("--mode sample"), "{error}");
        }
    }

    #[test]
    fn local_manifests_must_state_their_risk_history_and_conflicts_are_enforced() {
        let args = parse(&["--mode", "sample", "--manifest", "m.json"]);
        let error = sample_options(&args, VerificationMode::Sample)
            .unwrap_err()
            .to_string();
        assert!(error.contains("exactly one of"), "{error}");

        assert!(try_parse(&[
            "--mode",
            "sample",
            "--manifest",
            "m.json",
            "--risk-evidence",
            "risk.jsonl",
            "--assume-no-risk-history",
        ])
        .is_err());

        for flags in [
            &["--mode", "sample", "--risk-evidence", "risk.jsonl"][..],
            &["--mode", "sample", "--assume-no-risk-history"],
        ] {
            let args = parse(flags);
            let error = sample_options(&args, VerificationMode::Sample)
                .unwrap_err()
                .to_string();
            assert!(error.contains("apply only to --manifest"), "{error}");
        }
    }

    #[test]
    fn sample_values_are_validated() {
        for flags in [
            &["--sample-files", "0"][..],
            &["--content-workers", "0"],
            &["--content-read-size", "0"],
            &["--content-read-size", "2147483648"],
        ] {
            let mut argv = vec![
                "--mode",
                "sample",
                "--manifest",
                "m.json",
                "--assume-no-risk-history",
            ];
            argv.extend_from_slice(flags);
            let args = parse(&argv);
            let error = sample_options(&args, VerificationMode::Sample)
                .unwrap_err()
                .to_string();
            assert!(error.contains(flags[0]), "{error}");
        }
        let args = parse(&[
            "--mode",
            "sample",
            "--manifest",
            "m.json",
            "--assume-no-risk-history",
            "--content-read-size",
            "2147483647",
        ]);
        assert!(sample_options(&args, VerificationMode::Sample).is_ok());
    }

    #[tokio::test]
    async fn full_mode_still_fails_closed() {
        let args = parse(&["--mode", "full", "--manifest", "m.json"]);
        let error = run(args, None).await.unwrap_err().to_string();
        assert!(error.contains("not implemented"), "{error}");
    }

    #[test]
    fn human_summary_reports_sample_evidence() {
        use migration_core::records::{Endpoint, EndpointKind};
        use migration_verify::{
            ArtifactDigest, ComparisonPolicy, ObservationCounts, SamplePolicyReport,
            VerificationReport, VerificationResult,
        };
        let endpoint = Endpoint {
            kind: EndpointKind::Nfs,
            url: "nfs://h/e".into(),
            root: "/".into(),
        };
        let report = VerificationReport {
            schema_version: migration_verify::REPORT_SCHEMA_VERSION,
            verification_id: "v".into(),
            run_id: "r".into(),
            request_fingerprint_sha256: "f".into(),
            software_version: "0".into(),
            source: endpoint.clone(),
            destination: endpoint,
            consistency: ConsistencyBoundary {
                writers_stopped: true,
                source_snapshot_id: None,
                destination_snapshot_id: None,
            },
            exclusions: vec![],
            mode: VerificationMode::Sample,
            comparison_policy: ComparisonPolicy {
                namespace: true,
                file_type: true,
                regular_file_size: true,
                mode: true,
                owner: true,
                mtime_microsecond_precision: true,
                symlink_target: true,
                hardlink_equivalence: true,
                content: true,
                xattrs: false,
                acls: false,
                sparse_extents: false,
            },
            sample_policy: Some(SamplePolicyReport {
                algorithm: migration_verify::SAMPLE_ALGORITHM.into(),
                seed: 3,
                requested_files: 10,
                content_workers: 2,
                content_read_size: 8,
                eligible_files: 40,
                selected_files: 12,
                risk_candidates: 5,
                risk_selected_files: 4,
                risk_ineligible_files: 1,
                seeded_selected_files: 8,
                selection_reasons: Default::default(),
                risk_evidence_artifact: ArtifactDigest {
                    path: "/w/risk-evidence.jsonl".into(),
                    sha256: "abcdef0123456789".into(),
                    bytes: 0,
                },
                risk_history_asserted_empty: true,
            }),
            started_utc: "s".into(),
            completed_utc: "c".into(),
            resumed: false,
            counts: ObservationCounts::default(),
            content_complete: true,
            source_files_hashed: 12,
            source_logical_bytes_hashed: 100,
            destination_files_hashed: 12,
            destination_logical_bytes_hashed: 100,
            content_matches: 11,
            content_mismatches: 1,
            mismatch_count: 1,
            mismatches_by_kind: Default::default(),
            unreadable_entries: 0,
            unstable_entries: 0,
            operational_errors: vec!["content worker 0: mount failed".into()],
            mismatch_artifact: ArtifactDigest {
                path: "/w/mismatches.jsonl".into(),
                sha256: "0".into(),
                bytes: 1,
            },
            status: VerificationStatus::Mismatched,
        };
        let summary = human_summary(&VerificationResult {
            report,
            report_path: PathBuf::from("/w/report.json"),
        });
        for expected in [
            "mode                 sample",
            "sample selected      12 of 40 eligible (requested 10, seed 3)",
            "risk selected        4 (ineligible 1, seeded 8)",
            "content hashed       12 source / 12 destination files, 100 / 100 bytes",
            "content mismatches   1",
            "content complete     true",
            "asserted empty by operator",
            "operational error    content worker 0: mount failed",
            "mismatch details     /w/mismatches.jsonl",
        ] {
            assert!(
                summary.contains(expected),
                "missing {expected:?} in:\n{summary}"
            );
        }
    }
}
