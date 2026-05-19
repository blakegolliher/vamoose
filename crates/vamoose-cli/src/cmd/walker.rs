//! `vamoose walker` (stub) — will eventually wrap nfs-walker
//! invocation, run the schema-rewrite shim against its output,
//! and upload canonical parquet shards to S3 in one step. For
//! now invoke `nfs-walker` and `mig-walker-rewrite` directly.
//!
//! Walker is post-RocksDB-removal, so the workflow is single-step:
//!   nfs-walker <nfs-url> -o <out>.parquet
//! produces `<out>.parquet/scans/<scan_id>/part-rNN-SSSSS.parquet`
//! + `metadata.json` directly. There is no longer a rocks intermediate
//! or `export-parquet` follow-up step. See
//! scripts/m5-self-fence-test.sh for the shape this stub will
//! eventually replace.

use clap::Args as ClapArgs;
use std::path::PathBuf;

#[derive(ClapArgs)]
pub struct Args {
    /// Source NFS URL to scan (nfs://host/export[/path]).
    #[arg(long)]
    pub src: Option<String>,
    /// Output directory for canonical parquet shards.
    #[arg(long)]
    pub out: Option<PathBuf>,
    /// Number of walker threads (default from config).
    #[arg(long)]
    pub threads: Option<usize>,
}

pub async fn run(_args: Args, _config_path: Option<PathBuf>) -> anyhow::Result<()> {
    anyhow::bail!(
        "`vamoose walker` is not yet implemented. \
         Invoke nfs-walker (single-step parquet output: \
         `nfs-walker <nfs-url> -o <out>.parquet`) and then \
         `mig-walker-rewrite --input <out>.parquet --output ...` directly; \
         see scripts/m5-self-fence-test.sh for the shape."
    );
}
