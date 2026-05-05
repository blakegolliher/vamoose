//! `vamoose aggr` (stub) — will eventually run the mig-aggr TUI
//! / metrics exporter / verify subcommands inline. For now invoke
//! the standalone `mig-aggr` binary directly. `vamoose status`
//! covers the read-only one-shot use case.

use clap::Args as ClapArgs;
use std::path::PathBuf;

#[derive(ClapArgs)]
pub struct Args {
    /// What to run: watch | metrics | verify | summary.
    #[arg(long)]
    pub mode: Option<String>,
}

pub async fn run(_args: Args, _config_path: Option<PathBuf>) -> anyhow::Result<()> {
    anyhow::bail!(
        "`vamoose aggr` is not yet implemented. \
         Use `vamoose status` for the read-only summary, or invoke `mig-aggr` directly."
    );
}
