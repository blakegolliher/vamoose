//! `vamoose rewrite` (stub) — will eventually call into the
//! `mig-walker-rewrite` library to convert legacy walker parquet
//! into the canonical schema. For now invoke that crate's binary
//! directly.

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
         Invoke `cargo run --release -p mig-walker-rewrite -- ...` directly for now."
    );
}
