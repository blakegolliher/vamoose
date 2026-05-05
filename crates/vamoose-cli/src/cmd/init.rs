//! `vamoose init` — create the per-prefix marker objects so the
//! bucket layout is visible (and listable) before any worker has
//! claimed a shard. This is purely cosmetic for tooling like
//! `vamoose status` that wants to enumerate prefixes; the worker
//! itself doesn't depend on these markers.

use crate::config::Config;
use clap::Args as ClapArgs;
use migration_core::claim::ClaimStore;
use migration_core::layout;
use migration_core::s3::S3Client;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(ClapArgs)]
pub struct Args {
    /// Bucket URI in the form `s3://bucket-name`. When omitted the
    /// bucket from the loaded config is used.
    pub bucket: Option<String>,
    /// Overwrite existing layout markers (no-op if none exist).
    #[arg(long)]
    pub force: bool,
}

pub async fn run(args: Args, config_path: Option<PathBuf>) -> anyhow::Result<()> {
    let cfg = Config::load(config_path)?;
    let bucket = match args.bucket {
        Some(b) => b
            .strip_prefix("s3://")
            .map(|s| s.trim_end_matches('/').to_string())
            .unwrap_or(b),
        None => cfg.global.bucket.clone(),
    };
    if bucket != cfg.global.bucket {
        anyhow::bail!(
            "bucket arg ({bucket}) does not match config bucket ({}); \
             pass the same bucket or omit the arg",
            cfg.global.bucket
        );
    }

    let verify_tls = !cfg.s3.no_verify_ssl.unwrap_or(false);
    let s3 = Arc::new(
        S3Client::from_config(
            &cfg.s3.endpoint,
            &cfg.s3.region,
            &bucket,
            cfg.s3.profile.as_deref(),
            verify_tls,
        )
        .await?,
    );
    let store: Arc<dyn ClaimStore> = s3.clone();

    let prefixes = [
        layout::INDEX_PREFIX,
        layout::SHARDS_PREFIX,
        layout::PROGRESS_PREFIX,
        layout::BATCHES_PREFIX,
        layout::FAILURES_PREFIX,
        layout::DOWNGRADES_PREFIX,
    ];

    println!("vamoose init s3://{bucket}\n");
    for p in &prefixes {
        let key = format!("{p}.keep");
        let exists = matches!(store.head_object(&key).await, Ok(Some(_)));
        if exists && !args.force {
            println!("  skip   {key} (exists; use --force to overwrite)");
            continue;
        }
        // Use unconditional PUT here — markers are idempotent and
        // we want --force to work whether or not the key exists.
        s3.put(&key, b"vamoose layout marker\n".to_vec()).await?;
        println!("  create {key}");
    }
    println!("\ndone.");
    Ok(())
}
