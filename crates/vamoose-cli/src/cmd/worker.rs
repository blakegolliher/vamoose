//! `vamoose worker` — wraps the existing `migration-worker`
//! orchestrator. The unified `Config` is mapped field-by-field onto
//! `migration_worker::config::Config`; fields the unified TOML omits
//! are filled with defaults (mostly delegated to the existing
//! `serde(default)` handlers).

use crate::config::Config;
use anyhow::Context;
use clap::Args as ClapArgs;
use migration_worker::config as wcfg;
use std::path::PathBuf;

#[derive(ClapArgs)]
pub struct Args {
    /// Override worker host_id (default: `<hostname>-<pid>`).
    #[arg(long)]
    pub id: Option<String>,
    /// Route regular-file copies through the bucketed async libnfs
    /// pool (Phase 2 of the multi-pass mover). Off by default during
    /// the rollout. Non-regular rows (symlinks / hardlinks / dirs /
    /// empty / skip) still use the sync path either way.
    #[arg(long)]
    pub use_bucketed_pool: bool,
}

pub async fn run(args: Args, config_path: Option<PathBuf>) -> anyhow::Result<()> {
    let path = config_path.unwrap_or_else(|| PathBuf::from("vamoose.toml"));
    let text = std::fs::read_to_string(&path)
        .with_context(|| format!("reading config from {}", path.display()))?;

    let (mut worker_cfg, host_id_from_cfg) = parse_worker_config(&text)
        .with_context(|| format!("loading config at {}", path.display()))?;

    // CLI override always wins over the config-file value (which
    // defaults to false anyway). Passing the flag on a config that
    // also sets `[mover].use_bucketed_pool = true` is redundant but
    // not an error.
    if args.use_bucketed_pool {
        worker_cfg.mover.use_bucketed_pool = true;
    }

    let host_id = args.id.or(host_id_from_cfg).unwrap_or_else(|| {
        let host = hostname::get()
            .ok()
            .and_then(|s| s.into_string().ok())
            .unwrap_or_else(|| "unknown".to_string());
        format!("{}-{}", host, std::process::id())
    });

    tracing::info!(host_id = %host_id, "vamoose worker starting");
    migration_worker::orchestrator::run(worker_cfg, host_id).await
}

/// Dual-format config parse (the seam every worker start funnels
/// through — F31 pins its behavior with tests below).
///
/// Try the unified vamoose schema first; if that fails, fall back to
/// the legacy migration-worker schema. The M5 harness still emits the
/// legacy format because it needs to set concurrency-bounding knobs
/// ([shard].max_in_flight, [mover].nfs_connections,
/// [batch].inflight_*) that the unified Config doesn't yet surface.
///
/// A subtly-invalid unified config cannot silently fall through to
/// legacy: the legacy schema requires the `[run]`/`[shard]`/`[mover]`/
/// `[batch]` tables a unified file doesn't have, so both parses fail
/// and the error reports both messages (pinned by
/// `subtly_invalid_unified_does_not_silently_fall_through`).
///
/// Returns the worker config plus the config-carried host id (if any).
fn parse_worker_config(text: &str) -> anyhow::Result<(wcfg::Config, Option<String>)> {
    match toml::from_str::<Config>(text) {
        Ok(unified) => {
            tracing::debug!("config: unified format detected");
            let cfg_host = unified.worker.as_ref().and_then(|w| w.host_id.clone());
            Ok((build_worker_config(&unified)?, cfg_host))
        }
        Err(unified_err) => match toml::from_str::<wcfg::Config>(text) {
            Ok(legacy) => {
                tracing::debug!("config: legacy worker format detected");
                let cfg_host = legacy.worker.host_id.clone();
                Ok((legacy, cfg_host))
            }
            Err(legacy_err) => {
                anyhow::bail!(
                    "config parse failed in both formats:\n  \
                     unified: {}\n  \
                     legacy:  {}",
                    unified_err,
                    legacy_err
                );
            }
        },
    }
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
            use_bucketed_pool: false,
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
        // Coord wiring is not exposed in the unified vamoose.toml
        // yet (Phase 3.5). Operators opt in via `mig-worker` with a
        // worker.toml `[coord]` block until the unified config grows
        // a [coord] section of its own.
        coord: None,
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

// =============================================================================
// F31: tests over the dual-format config seam
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// The 6-field starter config from the `config.rs` module doc.
    const MINIMAL_UNIFIED: &str = r#"
        [global]
        bucket = "vamoose-test"

        [s3]
        endpoint = "http://127.0.0.1:9000"

        [nfs]
        src_url   = "nfs://src-filer/export"
        dst_url   = "nfs://dst-filer/export"
        src_mount = "/mnt/src"
        dst_mount = "/mnt/dst"
        src_root  = "/data"
        dst_root  = "/data"
    "#;

    /// Minimal legacy migration-worker TOML (what the M5 harness
    /// emits): required tables present, everything else defaulted.
    const MINIMAL_LEGACY: &str = r#"
        [run]
        bucket   = "vamoose-test"
        endpoint = "http://127.0.0.1:9000"
        region   = "us-east-1"

        [worker]

        [shard]
        local_scratch = "/tmp/vamoose-scratch"

        [mover]
        src_url = "nfs://src-filer/export"
        dst_url = "nfs://dst-filer/export"

        [batch]
        bytes_budget = "8 GiB"

        [copy]

        [backpressure]
    "#;

    #[test]
    fn minimal_unified_config_parses() {
        let (cfg, host) = parse_worker_config(MINIMAL_UNIFIED).expect("minimal unified must parse");
        assert_eq!(cfg.run.bucket, "vamoose-test");
        assert_eq!(cfg.mover.src_url, "nfs://src-filer/export");
        // Adapter defaults applied for fields the unified TOML omits.
        assert_eq!(cfg.mover.nfs_connections, 16);
        assert_eq!(cfg.batch.bytes_budget, "8 GiB");
        assert!(host.is_none(), "no [worker].host_id configured");
    }

    #[test]
    fn legacy_worker_toml_falls_back() {
        let (cfg, host) = parse_worker_config(MINIMAL_LEGACY).expect("legacy format must parse");
        assert_eq!(cfg.run.bucket, "vamoose-test");
        assert_eq!(
            cfg.shard.local_scratch,
            PathBuf::from("/tmp/vamoose-scratch")
        );
        assert!(host.is_none());
    }

    /// A unified config with a subtle mistake (wrong type on a
    /// `[worker]` field) must NOT silently fall through to a legacy
    /// run — it fails, and the error carries BOTH parse messages so
    /// the operator sees the real unified problem. (Structurally a
    /// unified file can't parse as legacy — legacy requires `[run]` /
    /// `[shard]` / `[mover]` / `[batch]` — this test pins that.)
    #[test]
    fn subtly_invalid_unified_does_not_silently_fall_through() {
        let broken = format!("{MINIMAL_UNIFIED}\n[worker]\nheartbeat_sec = \"not-a-number\"\n");
        let err = parse_worker_config(&broken).expect_err("must not run with a mistyped config");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("unified:") && msg.contains("legacy:"),
            "error must report both format failures, got: {msg}",
        );
        assert!(
            msg.contains("heartbeat_sec"),
            "the real (unified) mistake must be visible, got: {msg}",
        );
    }

    /// Worker without `[nfs]` has no source/destination — must error,
    /// and the error must point at `nfs`.
    #[test]
    fn missing_nfs_section_errors_for_worker() {
        let no_nfs = r#"
            [global]
            bucket = "vamoose-test"

            [s3]
            endpoint = "http://127.0.0.1:9000"
        "#;
        let err = parse_worker_config(no_nfs).expect_err("missing [nfs] must fail for the worker");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("nfs"),
            "error must name the nfs section, got: {msg}"
        );
    }
}
