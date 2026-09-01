//! `mig-walker-rewrite` binary — thin CLI over the library. The
//! translation itself lives in `lib.rs` so `mongoose` can run it
//! in-process.

use anyhow::Result;
use clap::Parser;
use mig_walker_rewrite::{run_rewrite, Cli};

fn main() -> Result<()> {
    let args = Cli::parse();

    let level = if args.verbose { "trace" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level)),
        )
        .init();

    run_rewrite(&args)
}
