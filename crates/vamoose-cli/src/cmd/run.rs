//! `vamoose run` is a safe stub. Operators currently script the end-to-end
//! pipeline by chaining nfs-walker, mig-walker-rewrite, S3 upload, and
//! `vamoose worker`; see scripts/manual-verify.sh.

use clap::Args as ClapArgs;
use std::path::PathBuf;

#[derive(ClapArgs)]
pub struct Args {
    /// Number of worker processes to spawn (default: 1).
    #[arg(long, default_value = "1")]
    pub workers: usize,
}

pub async fn run(_args: Args, _config_path: Option<PathBuf>) -> anyhow::Result<()> {
    anyhow::bail!(
        "`vamoose run` (end-to-end pipeline) is not yet implemented. \
         Use `vamoose walker` + `vamoose rewrite` (when implemented), \
         then `vamoose worker` per host."
    );
}
