//! `mig-worker` — the worker binary.
//!
//! Reads `manifest.json`, claims a shard via S3 conditional PUT,
//! downloads it to local scratch, and runs the mover over its rows.
//! Heartbeats every 30s, self-fences on claim loss.

use clap::Parser;
use migration_worker::{config, orchestrator};

#[derive(Debug, Parser)]
#[command(name = "mig-worker", version)]
struct Cli {
    /// Path to a TOML config file. See DESIGN.md "Configuration".
    #[arg(short, long)]
    config: std::path::PathBuf,

    /// Override the worker host id (default: from config or auto).
    #[arg(long)]
    host_id: Option<String>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    let cfg = config::Config::load(&cli.config)?;
    let host_id = cli
        .host_id
        .or_else(|| cfg.worker.host_id.clone().filter(|s| s != "auto"))
        .unwrap_or_else(config::auto_host_id);

    tracing::info!(host_id = %host_id, "mig-worker starting");

    let result = orchestrator::run(cfg, host_id).await;

    // After orchestrator::run() returns, bypass the tokio runtime's
    // drop and any Drop chains by exiting via libc::_exit. All durable
    // state is committed to S3 by this point — claims released,
    // heartbeat stopped, shard processor terminated, downgrade /
    // failure logs flushed. Drops past this point only matter for
    // in-process resources (libnfs contexts, AWS connection pools)
    // whose loss is harmless on shutdown. Empirically the tokio
    // runtime drop wedges on a stale libnfs context after a long
    // SIGSTOP/SIGCONT cycle; this avoids it entirely.
    let exit_code: i32 = match &result {
        Ok(()) => {
            let msg: &[u8] = b"mig-worker: clean exit via libc::_exit(0)\n";
            unsafe {
                libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len());
            }
            0
        }
        Err(e) => {
            // tracing may be down by here; eprintln directly.
            eprintln!("mig-worker: error exit: {e:#}");
            let msg: &[u8] = b"mig-worker: error exit via libc::_exit(1)\n";
            unsafe {
                libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len());
            }
            1
        }
    };
    unsafe { libc::_exit(exit_code) }
}
