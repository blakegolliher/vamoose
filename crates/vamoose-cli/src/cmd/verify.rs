//! `vamoose verify` — independent source/destination observation and metadata
//! comparison. V1 is intentionally local and single-process; its SQLite
//! checkpoint and terminal artifacts are inputs to the later distributed
//! verifier rather than throwaway output.

use crate::config::Config;
use anyhow::{Context, Result};
use clap::{Args as ClapArgs, ValueEnum};
use migration_core::claim::ClaimStore;
use migration_core::records::{Manifest, RUN_FORMAT_VERSION};
use migration_verify::{
    ConsistencyBoundary, VerificationMode, VerificationRequest, VerificationStatus,
};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

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
    /// Verification depth. V1 implements metadata; sample and full are reserved.
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

    /// Print the terminal report as JSON instead of a human summary.
    #[arg(long)]
    pub json: bool,
}

pub async fn run(args: Args, config_path: Option<PathBuf>) -> Result<VerificationStatus> {
    if args.mode != Mode::Metadata {
        anyhow::bail!(
            "verification mode {:?} is not implemented yet; V1 supports --mode metadata",
            args.mode
        );
    }
    let config = match Config::load(config_path.clone()) {
        Ok(config) => Some(config),
        Err(_) if args.manifest.is_some() && config_path.is_none() => None,
        Err(error) => return Err(error),
    };
    let manifest = load_manifest(args.manifest.as_deref(), config.as_ref()).await?;
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
        mode: VerificationMode::Metadata,
        sample: None,
    };
    let result = tokio::task::spawn_blocking(move || migration_verify::verify(request))
        .await
        .context("verification task panicked")??;

    if args.json {
        println!("{}", serde_json::to_string_pretty(&result.report)?);
    } else {
        println!(
            "verification {}: {:?}\n  source entries       {}\n  destination entries  {}\n  compared paths       {}\n  mismatches           {}\n  unreadable           {}\n  unstable             {}\n  report               {}\n  mismatch details     {}",
            result.report.verification_id,
            result.report.status,
            result.report.counts.source_entries,
            result.report.counts.destination_entries,
            result.report.counts.entries_on_both_sides,
            result.report.mismatch_count,
            result.report.unreadable_entries,
            result.report.unstable_entries,
            result.report_path.display(),
            result.report.mismatch_artifact.path,
        );
    }
    Ok(result.report.status)
}

async fn load_manifest(path: Option<&Path>, config: Option<&Config>) -> Result<Manifest> {
    if let Some(path) = path {
        let bytes =
            std::fs::read(path).with_context(|| format!("reading manifest {}", path.display()))?;
        return serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing manifest {}", path.display()));
    }
    let config = config.ok_or_else(|| {
        anyhow::anyhow!("verification requires --manifest or a configured S3 run")
    })?;
    let storage = config.storage();
    let s3 = migration_core::s3::S3Client::from_config(
        &storage.endpoint,
        &storage.region,
        &storage.bucket,
        storage.profile.as_deref(),
        storage.verify_tls,
    )
    .await?
    .with_prefix(&storage.prefix);
    let (bytes, _) = ClaimStore::get(&s3, "manifest.json")
        .await?
        .ok_or_else(|| anyhow::anyhow!("{} has no manifest.json", s3.location()))?;
    serde_json::from_slice(&bytes).context("parsing configured manifest.json")
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    #[test]
    fn verify_help_documents_modes_and_exit_codes() {
        let mut command = crate::cli::Cli::command();
        let help = command
            .find_subcommand_mut("verify")
            .expect("verify subcommand")
            .render_long_help()
            .to_string();
        for expected in ["metadata", "sample", "full", "Exit codes", "mismatches"] {
            assert!(help.contains(expected), "missing {expected:?} in:\n{help}");
        }
    }
}
