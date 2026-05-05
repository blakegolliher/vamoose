//! `vamoose` — unified CLI binary.
//!
//! Composes the existing crates (`migration-core`, `migration-mover`,
//! `migration-worker`, `migration-aggr`) into a single binary with
//! subcommand dispatch. The protocol, schema, claim logic, and
//! orchestrator are all owned by the underlying crates; this binary
//! is only wiring.

use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod config;
mod cmd;

#[derive(Parser)]
#[command(name = "vamoose", version, about = "Distributed NFS migration with S3 coordination")]
struct Cli {
    /// Path to config file (default: vamoose.toml in CWD).
    #[arg(short, long, env = "VAMOOSE_CONFIG", global = true)]
    config: Option<PathBuf>,

    /// Logging filter (default: info).
    #[arg(long, env = "RUST_LOG", global = true)]
    log: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run a migration worker.
    Worker(cmd::worker::Args),
    /// Walk a source filesystem and emit parquet shards.
    Walker(cmd::walker::Args),
    /// Rewrite legacy walker output to the canonical schema.
    Rewrite(cmd::rewrite::Args),
    /// Aggregate worker progress to a single status snapshot.
    Aggr(cmd::aggr::Args),
    /// Show current migration status (one-shot or watch).
    Status(cmd::status::Args),
    /// Verify environment health (S3, NFS, perms).
    Doctor(cmd::doctor::Args),
    /// Initialize an empty bucket layout.
    Init(cmd::init::Args),
    /// End-to-end pipeline: walker + rewrite + workers.
    Run(cmd::run::Args),
}

fn init_tracing(filter: Option<&str>) {
    let env = match filter {
        Some(f) => tracing_subscriber::EnvFilter::try_new(f)
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        None => tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
    };
    tracing_subscriber::fmt().with_env_filter(env).init();
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.log.as_deref());

    match cli.command {
        Command::Worker(a)  => cmd::worker::run(a, cli.config).await,
        Command::Walker(a)  => cmd::walker::run(a, cli.config).await,
        Command::Rewrite(a) => cmd::rewrite::run(a, cli.config).await,
        Command::Aggr(a)    => cmd::aggr::run(a, cli.config).await,
        Command::Status(a)  => cmd::status::run(a, cli.config).await,
        Command::Doctor(a)  => cmd::doctor::run(a, cli.config).await,
        Command::Init(a)    => cmd::init::run(a, cli.config).await,
        Command::Run(a)     => cmd::run::run(a, cli.config).await,
    }
}
