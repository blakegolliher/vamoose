//! `vamoose worker` — wraps the existing `migration-worker`
//! orchestrator. The unified `Config` is mapped field-by-field onto
//! `migration_worker::config::Config`; fields the unified TOML omits
//! are filled with defaults (mostly delegated to the existing
//! `serde(default)` handlers).

use crate::config::Config;
use clap::Args as ClapArgs;
use migration_worker::config as wcfg;
use std::path::PathBuf;

#[derive(ClapArgs)]
pub struct Args {
    /// Override worker host_id (default: `<hostname>-<pid>`).
    #[arg(long)]
    pub id: Option<String>,
}

pub async fn run(args: Args, config_path: Option<PathBuf>) -> anyhow::Result<()> {
    let cfg = Config::load(config_path)?;

    let host_id = args
        .id
        .or_else(|| cfg.worker.as_ref().and_then(|w| w.host_id.clone()))
        .unwrap_or_else(|| {
            let host = hostname::get()
                .ok()
                .and_then(|s| s.into_string().ok())
                .unwrap_or_else(|| "unknown".to_string());
            format!("{}-{}", host, std::process::id())
        });

    let worker_cfg = build_worker_config(&cfg)?;

    tracing::info!(host_id = %host_id, "vamoose worker starting");
    migration_worker::orchestrator::run(worker_cfg, host_id).await
}

/// Adapter: unified `Config` → existing `migration_worker::config::Config`.
///
/// The fields the unified config exposes are passed through directly.
/// Everything else gets sensible production defaults — operators who
/// need fine-grained tuning of the inflight profile, server-side-copy
/// policy, backpressure thresholds, etc. can extend the unified
/// config or fall back to invoking `mig-worker` with a verbose TOML.
fn build_worker_config(cfg: &Config) -> anyhow::Result<wcfg::Config> {
    let worker = cfg.worker.clone().unwrap_or_else(default_worker);
    let copy = cfg.copy.clone().unwrap_or_default();

    let no_verify = cfg.s3.no_verify_ssl.unwrap_or(false);

    Ok(wcfg::Config {
        run: wcfg::RunCfg {
            bucket: cfg.global.bucket.clone(),
            endpoint: cfg.s3.endpoint.clone(),
            region: cfg.s3.region.clone(),
            profile: cfg.s3.profile.clone(),
            verify_tls: !no_verify,
        },
        worker: wcfg::WorkerCfg {
            host_id: worker.host_id.clone(),
            heartbeat_sec: worker.heartbeat_sec,
            lease_timeout_sec: worker.lease_timeout_sec,
        },
        shard: wcfg::ShardCfg {
            local_scratch: worker.local_scratch.clone(),
            max_in_flight: 1,
        },
        mover: wcfg::MoverCfg {
            strategy_default: "libnfs_io_uring".to_string(),
            src_url: cfg.nfs.src_url.clone(),
            dst_url: cfg.nfs.dst_url.clone(),
            nfs_connections: worker.concurrency.max(1) as u32,
            pipeline_depth: 8,
            io_uring_queue_depth: 256,
            fixed_buffer_count: 256,
            fixed_buffer_size: "1 MiB".to_string(),
        },
        batch: wcfg::BatchCfg {
            bytes_budget: worker.bytes_budget.clone(),
            files_budget: 100_000,
            inflight_small: 256,
            inflight_medium: 16,
            inflight_large: 4,
            large_stripe_size: "4 MiB".to_string(),
            large_stripe_depth: 32,
        },
        copy: wcfg::CopyCfg {
            preserve_owner: copy.preserve_owner,
            preserve_mode: copy.preserve_mode,
            preserve_times: copy.preserve_times,
            preserve_xattr: copy.preserve_xattr,
            server_side_copy: copy
                .server_side_copy
                .clone()
                .unwrap_or_else(|| "off".to_string()),
            require_chown_capability: true,
            require_unchanged_size: false,
        },
        backpressure: wcfg::BackpressureCfg {
            failure_pct_window_sec: 60,
            failure_pct_threshold: 5.0,
            throughput_floor_mb_s: 100,
        },
    })
}

fn default_worker() -> crate::config::Worker {
    crate::config::Worker {
        host_id: None,
        heartbeat_sec: 30,
        lease_timeout_sec: 180,
        concurrency: 16,
        local_scratch: PathBuf::from("/tmp/vamoose-scratch"),
        bytes_budget: "8 GiB".to_string(),
    }
}
