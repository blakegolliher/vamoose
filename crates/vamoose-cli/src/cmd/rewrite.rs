//! `vamoose rewrite` is a safe stub. Invoke the `mig-walker-rewrite` binary
//! directly to convert legacy walker Parquet into the canonical schema.
//!
//! The shim accepts either the walker output root (auto-descends
//! into `scans/<scan_id>/`) or a specific scan subdirectory.

use clap::Args as ClapArgs;
use std::path::PathBuf;

#[derive(ClapArgs)]
pub struct Args {
    /// Input directory of legacy walker parquet.
    #[arg(long)]
    pub input: Option<PathBuf>,
    /// Output directory for canonical parquet.
    #[arg(long)]
    pub output: Option<PathBuf>,
    /// Source root prefix to strip from paths during rewrite.
    #[arg(long)]
    pub source_root: Option<String>,
}

pub async fn run(_args: Args, _config_path: Option<PathBuf>) -> anyhow::Result<()> {
    anyhow::bail!(
        "`vamoose rewrite` is not yet implemented. \
         Invoke `cargo run --release -p mig-walker-rewrite -- \
         --input <walker-out> --output <canonical-out> --source-root <path>` \
         directly for now. The shim auto-descends into \
         `scans/<scan_id>/` when given a walker output root."
    );
}
