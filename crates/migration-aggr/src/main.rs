//! `mig-aggr` — read-only observability sidecar.
//!
//! Implemented subcommand:
//!   - `clean-partials`   : lease-aware sweep of orphaned `.partial`
//!     files left by fenced/killed workers. Dry run by default;
//!     `--delete` arms deletion, gated on claim liveness.
//!
//! The observability subcommands (`watch`, `summary`, `metrics`,
//! `inspect <shard>`, `verify`) are not implemented: each returns a
//! clear error (never a panic) pointing at the alternative — the
//! migration-tui dashboard covers live observability.

use clap::{Parser, Subcommand};

mod clean_partials;
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
    /// Sweep orphaned `.partial` files from the destination export.
    CleanPartials {
        /// Locally mounted destination export root to sweep.
        #[arg(long)]
        dest_root: std::path::PathBuf,
        /// List matching files without deleting. This is the default
        /// behavior; the flag is accepted as an explicit no-op.
        #[arg(long, conflicts_with = "delete")]
        dry_run: bool,
        /// Arm deletion. Without this flag nothing is ever removed.
        #[arg(long)]
        delete: bool,
        /// Override the claim-liveness gate (operator asserts the run
        /// is dead). The gate's finding is still printed.
        #[arg(long, requires = "delete")]
        force: bool,
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
        Cmd::Summary { format } => {
            summary::run(&cli.endpoint, &cli.region, &cli.bucket, &format).await
        }
        Cmd::Metrics { listen } => {
            metrics_server::run(&cli.endpoint, &cli.region, &cli.bucket, &listen).await
        }
        Cmd::Inspect { shard } => {
            inspect::run(&cli.endpoint, &cli.region, &cli.bucket, &shard).await
        }
        Cmd::Verify => verify::run(&cli.endpoint, &cli.region, &cli.bucket).await,
        Cmd::CleanPartials {
            dest_root,
            // Dry run is the default; the flag is a no-op alias and
            // clap rejects combining it with --delete.
            dry_run: _,
            delete,
            force,
        } => {
            clean_partials::run(
                &cli.endpoint,
                &cli.region,
                &cli.bucket,
                &dest_root,
                delete,
                force,
            )
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    /// Acceptance test 6: the five observability stubs must return
    /// `Err` (with a message naming the subcommand) — never panic.
    #[tokio::test]
    async fn stub_subcommands_bail_not_panic() {
        let cases: Vec<(&str, anyhow::Result<()>)> = vec![
            ("watch", super::tui::run("http://e", "r", "b").await),
            (
                "summary",
                super::summary::run("http://e", "r", "b", "json").await,
            ),
            (
                "metrics",
                super::metrics_server::run("http://e", "r", "b", "127.0.0.1:0").await,
            ),
            (
                "inspect",
                super::inspect::run("http://e", "r", "b", "part-0000.parquet").await,
            ),
            ("verify", super::verify::run("http://e", "r", "b").await),
        ];
        for (name, result) in cases {
            let err = result.expect_err("stub subcommands must return Err, not succeed");
            assert!(
                err.to_string().contains(name),
                "error for `{name}` must name the subcommand: {err}"
            );
        }
    }
}
