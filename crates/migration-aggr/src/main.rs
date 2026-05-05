//! `mig-aggr` — read-only observability sidecar.
//!
//! Subcommands:
//!   - `watch`            : TUI dashboard.
//!   - `summary`          : one-shot JSON summary to stdout.
//!   - `metrics`          : Prometheus exporter on /metrics.
//!   - `inspect <shard>`  : pre-flight parquet analysis (size hist,
//!                          dir rollups, hardlink groups).
//!   - `verify`           : diff src vs dst metadata after a run.
//!   - `clean-partials`   : sweep `.partial` files left by fenced workers.

use clap::{Parser, Subcommand};

mod inspect;
mod metrics_server;
mod summary;
mod tui;
mod verify;

#[derive(Debug, Parser)]
#[command(name = "mig-aggr", version)]
struct Cli {
    /// S3 endpoint URL (e.g. https://vast-s3.example.com).
    #[arg(long, env = "MIG_S3_ENDPOINT")]
    endpoint: String,

    /// S3 region.
    #[arg(long, env = "MIG_S3_REGION", default_value = "us-east-1")]
    region: String,

    /// Run bucket.
    #[arg(long, env = "MIG_S3_BUCKET")]
    bucket: String,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    Watch,
    Summary {
        #[arg(long, default_value = "json")]
        format: String,
    },
    Metrics {
        #[arg(long, default_value = "0.0.0.0:9090")]
        listen: String,
    },
    Inspect {
        shard: String,
    },
    Verify,
    CleanPartials {
        #[arg(long)]
        dry_run: bool,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Watch => tui::run(&cli.endpoint, &cli.region, &cli.bucket).await,
        Cmd::Summary { format } => summary::run(&cli.endpoint, &cli.region, &cli.bucket, &format).await,
        Cmd::Metrics { listen } => {
            metrics_server::run(&cli.endpoint, &cli.region, &cli.bucket, &listen).await
        }
        Cmd::Inspect { shard } => inspect::run(&cli.endpoint, &cli.region, &cli.bucket, &shard).await,
        Cmd::Verify => verify::run(&cli.endpoint, &cli.region, &cli.bucket).await,
        Cmd::CleanPartials { dry_run } => {
            verify::clean_partials(&cli.endpoint, &cli.region, &cli.bucket, dry_run).await
        }
    }
}
