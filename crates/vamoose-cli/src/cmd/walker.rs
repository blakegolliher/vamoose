//! `vamoose walker` (stub) — will eventually wrap nfs-walker
//! invocation, run the schema-rewrite shim against its output,
//! and upload canonical parquet shards to S3 in one step. For
//! now invoke `nfs-walker` and `mig-walker-rewrite` directly.

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
         Invoke nfs-walker and mig-walker-rewrite directly for now; \
         see scripts/m5-self-fence-test.sh for the shape."
    );
}
