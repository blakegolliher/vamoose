//! Worker configuration loaded from TOML. Mirrors the `[run]`,
//! `[worker]`, `[shard]`, `[mover]`, `[batch]`, `[copy]`,
//! `[backpressure]` sections in DESIGN.md.

use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub run: RunCfg,
    pub worker: WorkerCfg,
    pub shard: ShardCfg,
    pub mover: MoverCfg,
    pub batch: BatchCfg,
    pub copy: CopyCfg,
    pub backpressure: BackpressureCfg,
}

#[derive(Debug, Deserialize)]
pub struct RunCfg {
    pub bucket: String,
    pub endpoint: String,
    pub region: String,
    /// Optional AWS credentials profile name. If omitted, the SDK uses
    /// its default credential chain (env vars, default profile, IMDS,
    /// etc.). Set this when the operator's credentials live under a
    /// non-default profile name (e.g. `var204` for lab clusters), or
    /// when running under `sudo` where root's `HOME` would otherwise
    /// hide the user's `~/.aws/credentials`.
    #[serde(default)]
    pub profile: Option<String>,
    /// Whether to verify the TLS certificate of the S3 endpoint.
    /// Default: true. Set to false for lab/dev environments with
    /// self-signed certs. Equivalent to `aws-cli --no-verify-ssl`.
    #[serde(default = "default_verify_tls")]
    pub verify_tls: bool,
}
fn default_verify_tls() -> bool { true }

#[derive(Debug, Deserialize)]
pub struct WorkerCfg {
    #[serde(default)]
    pub host_id: Option<String>,
    #[serde(default = "default_heartbeat_sec")]
    pub heartbeat_sec: u64,
    #[serde(default = "default_lease_timeout_sec")]
    pub lease_timeout_sec: u64,
}

fn default_heartbeat_sec() -> u64 { migration_core::time::DEFAULT_HEARTBEAT_SEC }
fn default_lease_timeout_sec() -> u64 { migration_core::time::DEFAULT_LEASE_TIMEOUT_SEC }

#[derive(Debug, Deserialize)]
pub struct ShardCfg {
    pub local_scratch: std::path::PathBuf,
    #[serde(default = "one")]
    pub max_in_flight: u32,
}
fn one() -> u32 { 1 }

#[derive(Debug, Deserialize)]
pub struct MoverCfg {
    #[serde(default = "default_strategy")]
    pub strategy_default: String,
    pub src_url: String,
    pub dst_url: String,
    #[serde(default = "default_nfs_connections")]
    pub nfs_connections: u32,
    #[serde(default = "default_pipeline_depth")]
    pub pipeline_depth: u32,
    #[serde(default = "default_io_uring_qd")]
    pub io_uring_queue_depth: u32,
    #[serde(default = "default_fixed_buf_count")]
    pub fixed_buffer_count: u32,
    #[serde(default = "default_fixed_buf_size")]
    pub fixed_buffer_size: String,
}
fn default_strategy() -> String { "libnfs_io_uring".into() }
fn default_nfs_connections() -> u32 { 16 }
fn default_pipeline_depth() -> u32 { 8 }
fn default_io_uring_qd() -> u32 { 256 }
fn default_fixed_buf_count() -> u32 { 256 }
fn default_fixed_buf_size() -> String { "1 MiB".into() }

#[derive(Debug, Deserialize)]
pub struct BatchCfg {
    pub bytes_budget: String,
    #[serde(default = "default_files_budget")]
    pub files_budget: u64,
    #[serde(default = "default_inflight_small")]
    pub inflight_small: usize,
    #[serde(default = "default_inflight_medium")]
    pub inflight_medium: usize,
    #[serde(default = "default_inflight_large")]
    pub inflight_large: usize,
    #[serde(default = "default_large_stripe_size")]
    pub large_stripe_size: String,
    #[serde(default = "default_large_stripe_depth")]
    pub large_stripe_depth: usize,
}
fn default_files_budget() -> u64 { 100_000 }
fn default_inflight_small() -> usize { 256 }
fn default_inflight_medium() -> usize { 16 }
fn default_inflight_large() -> usize { 4 }
fn default_large_stripe_size() -> String { "4 MiB".into() }
fn default_large_stripe_depth() -> usize { 32 }

#[derive(Debug, Deserialize)]
pub struct CopyCfg {
    #[serde(default = "t")] pub preserve_owner: bool,
    #[serde(default = "t")] pub preserve_mode: bool,
    #[serde(default = "t")] pub preserve_times: bool,
    #[serde(default = "t")] pub preserve_xattr: bool,
    #[serde(default = "default_ssc")]
    pub server_side_copy: String,
    /// True (default): refuse to start if `preserve_owner` is on but
    /// the worker doesn't hold `CAP_CHOWN`. False: downgrade — log a
    /// startup WARN and treat per-file `chown` EPERM as a non-fatal
    /// warning recorded to `failures/`.
    #[serde(default = "t")] pub require_chown_capability: bool,
    /// True: verify bytes-written equals the row's `size` after each
    /// file copy and fail the row with `SIZE_CHANGED` on mismatch.
    /// Default false per SCHEMA_CONTRACT.md "Size semantics" — source
    /// truth wins.
    #[serde(default = "default_false")] pub require_unchanged_size: bool,
}
fn t() -> bool { true }
fn default_false() -> bool { false }
// NFSv3 baseline: server-side COPY is never selected. See
// BUGFIX_PLAN.md "Fix 4". The field stays for forward compatibility.
fn default_ssc() -> String { "off".into() }

#[derive(Debug, Deserialize)]
pub struct BackpressureCfg {
    #[serde(default = "default_failure_window")]
    pub failure_pct_window_sec: u64,
    #[serde(default = "default_failure_threshold")]
    pub failure_pct_threshold: f32,
    #[serde(default = "default_throughput_floor")]
    pub throughput_floor_mb_s: u64,
}
fn default_failure_window() -> u64 { 60 }
fn default_failure_threshold() -> f32 { 5.0 }
fn default_throughput_floor() -> u64 { 100 }

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let s = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("read config {}: {e}", path.display()))?;
        let cfg: Config = toml::from_str(&s)
            .map_err(|e| anyhow::anyhow!("parse config {}: {e}", path.display()))?;
        Ok(cfg)
    }
}

/// Generate a stable host id when none is configured. Combines
/// hostname with a random tail so two restarts of the same host don't
/// collide on stale claims.
pub fn auto_host_id() -> String {
    let host = hostname::get()
        .ok()
        .and_then(|s| s.into_string().ok())
        .unwrap_or_else(|| "unknown".to_string());
    let mut suffix = [0u8; 4];
    if getrandom::getrandom(&mut suffix).is_err() {
        // best-effort fallback
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        suffix.copy_from_slice(&nanos.to_le_bytes());
    }
    format!("{host}-{}", hex::encode(suffix))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `[run]` without the new fields must keep working — `profile`
    /// defaults to None, `verify_tls` defaults to true. Existing
    /// worker.toml files in operators' configs should not need editing
    /// to upgrade.
    #[test]
    fn run_cfg_defaults_when_new_fields_omitted() {
        let toml_str = r#"
            bucket   = "vamoose"
            endpoint = "https://s3.example.com"
            region   = "us-east-1"
        "#;
        let run: RunCfg = toml::from_str(toml_str).unwrap();
        assert_eq!(run.bucket, "vamoose");
        assert_eq!(run.endpoint, "https://s3.example.com");
        assert_eq!(run.region, "us-east-1");
        assert_eq!(run.profile, None);
        assert!(run.verify_tls, "verify_tls must default to true (safe)");
    }

    #[test]
    fn examples_worker_toml_parses() {
        // Sanity: the checked-in example loads end-to-end. Catches
        // typos and out-of-sync defaults the per-section tests would
        // miss.
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../examples/worker.toml");
        let s = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        let cfg: Config = toml::from_str(&s)
            .unwrap_or_else(|e| panic!("parse {}: {e}", path.display()));
        // Just spot-check fields that come from the new [run] block.
        assert_eq!(cfg.run.bucket, "vamoose");
        assert_eq!(
            cfg.run.endpoint,
            "https://main.selab-var204.selab.vastdata.com",
        );
        assert_eq!(cfg.run.profile.as_deref(), Some("var204"));
        assert!(!cfg.run.verify_tls);
    }

    #[test]
    fn run_cfg_picks_up_profile_and_verify_tls() {
        // The exact config the user said worker.toml should accept
        // when this change ships — see WORKER_S3_CONFIG.md.
        let toml_str = r#"
            bucket     = "vamoose"
            endpoint   = "https://main.selab-var204.selab.vastdata.com"
            region     = "us-east-1"
            profile    = "var204"
            verify_tls = false
        "#;
        let run: RunCfg = toml::from_str(toml_str).unwrap();
        assert_eq!(run.bucket, "vamoose");
        assert_eq!(
            run.endpoint,
            "https://main.selab-var204.selab.vastdata.com",
        );
        assert_eq!(run.region, "us-east-1");
        assert_eq!(run.profile.as_deref(), Some("var204"));
        assert!(!run.verify_tls);
    }
}
