//! Unified vamoose configuration. Superset of the existing
//! `migration-worker` config (`crates/migration-worker/src/config.rs`)
//! plus surfaces the walker, aggregator, and doctor need.
//!
//! The mapping `Config -> migration_worker::config::Config` lives in
//! `cmd::worker`; sensible defaults are applied for any field the
//! unified TOML omits so an operator can start from a minimal
//! 6-field config and still get a working worker.

use anyhow::Context;
use serde::Deserialize;
use std::path::PathBuf;

// `aggr` is the published section header for future aggregator config;
// no current cmd reads it. Keep the schema and silence dead_code.
#[allow(dead_code)]
#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub global: Global,
    pub s3: S3,
    pub nfs: Nfs,
    #[serde(default)]
    pub worker: Option<Worker>,
    #[serde(default)]
    pub walker: Option<Walker>,
    #[serde(default)]
    pub aggr: Option<Aggr>,
    #[serde(default)]
    pub copy: Option<Copy>,
    #[serde(default)]
    pub logging: Option<Logging>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Global {
    pub bucket: String,
}

// `access_key` / `secret_key` are the explicit-credential path the
// doctor validates; current commands rely on `profile` + the default
// SDK chain. Keep the fields published.
#[allow(dead_code)]
#[derive(Deserialize, Debug, Clone)]
pub struct S3 {
    pub endpoint: String,
    /// AWS region. Defaults to `us-east-1` when omitted.
    #[serde(default = "default_region")]
    pub region: String,
    /// AWS profile (preferred). Falls through to the default
    /// credential chain when None.
    #[serde(default)]
    pub profile: Option<String>,
    /// Explicit access key (alternative to `profile`). Both
    /// access_key and secret_key must be set together; partial
    /// configuration is rejected by the doctor.
    #[serde(default)]
    pub access_key: Option<String>,
    #[serde(default)]
    pub secret_key: Option<String>,
    /// When true, skip TLS cert verification (lab/dev only).
    #[serde(default)]
    pub no_verify_ssl: Option<bool>,
}

fn default_region() -> String {
    "us-east-1".to_string()
}

#[derive(Deserialize, Debug, Clone)]
pub struct Nfs {
    pub src_url: String,
    pub dst_url: String,
    pub src_mount: String,
    pub dst_mount: String,
    pub src_root: String,
    pub dst_root: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Worker {
    #[serde(default)]
    pub host_id: Option<String>,
    #[serde(default = "default_heartbeat_sec")]
    pub heartbeat_sec: u64,
    #[serde(default = "default_lease_timeout_sec")]
    pub lease_timeout_sec: u64,
    /// Number of concurrent libnfs contexts the mover keeps open.
    /// Maps to `[mover].nfs_connections` in the existing worker config.
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    /// Local scratch directory for the parquet shard download. Defaults
    /// to `/tmp/vamoose-scratch` when omitted.
    #[serde(default = "default_scratch")]
    pub local_scratch: PathBuf,
    /// Per-batch byte budget as a TOML size string ("8 GiB", "4 MiB",
    /// …). Defaults to "8 GiB" when omitted.
    #[serde(default = "default_bytes_budget")]
    pub bytes_budget: String,
}

fn default_heartbeat_sec() -> u64 {
    30
}
fn default_lease_timeout_sec() -> u64 {
    180
}
fn default_concurrency() -> usize {
    16
}
fn default_scratch() -> PathBuf {
    PathBuf::from("/tmp/vamoose-scratch")
}
fn default_bytes_budget() -> String {
    "8 GiB".to_string()
}

// `threads` is part of the walker section schema; the current walker
// invocation runs the binary as-is and doesn't override it. Keep the
// field for the documented config surface.
#[allow(dead_code)]
#[derive(Deserialize, Debug, Clone, Default)]
pub struct Walker {
    #[serde(default = "default_walker_threads")]
    pub threads: usize,
    /// Path to the `nfs-walker` binary. When None, vamoose looks for
    /// it on `$PATH`.
    #[serde(default)]
    pub binary_path: Option<PathBuf>,
}

fn default_walker_threads() -> usize {
    16
}

// Aggregator config; no command consumes it yet. Schema published.
#[allow(dead_code)]
#[derive(Deserialize, Debug, Clone, Default)]
pub struct Aggr {
    #[serde(default = "default_aggr_refresh")]
    pub refresh_interval_sec: u64,
}

fn default_aggr_refresh() -> u64 {
    5
}

#[derive(Deserialize, Debug, Clone)]
pub struct Copy {
    #[serde(default = "default_true")]
    pub preserve_owner: bool,
    #[serde(default = "default_true")]
    pub preserve_mode: bool,
    #[serde(default = "default_true")]
    pub preserve_times: bool,
    #[serde(default = "default_true")]
    pub preserve_xattr: bool,
    /// "auto" / "force" / "off". Defaults to "off" — NFSv3 baseline
    /// per docs/CORRECTNESS_RULES.md never selects server-side COPY.
    #[serde(default = "default_ssc")]
    pub server_side_copy: Option<String>,
}

impl Default for Copy {
    fn default() -> Self {
        Self {
            preserve_owner: true,
            preserve_mode: true,
            preserve_times: true,
            preserve_xattr: true,
            server_side_copy: default_ssc(),
        }
    }
}

fn default_true() -> bool {
    true
}
fn default_ssc() -> Option<String> {
    Some("off".to_string())
}

/// Rotating worker logs. The active log file (`path`) is plain text so
/// `tail -F` and `grep` work; rotated archives are gzipped. When
/// `s3_upload` is true, archives are shipped to `[global].bucket` under
/// `s3_prefix/{host_id}/{startup_ts}/...` and live on disk only until
/// the upload succeeds or `max_archives` evicts them.
#[derive(Deserialize, Debug, Clone)]
pub struct Logging {
    #[serde(default = "default_log_path")]
    pub path: PathBuf,
    /// Rotation threshold as a TOML size string ("50 MiB", "1 GiB", …).
    #[serde(default = "default_log_max_bytes")]
    pub max_bytes: String,
    #[serde(default = "default_log_max_archives")]
    pub max_archives: usize,
    #[serde(default = "default_true")]
    pub s3_upload: bool,
    #[serde(default = "default_log_s3_prefix")]
    pub s3_prefix: String,
    /// Uploader scan interval (seconds).
    #[serde(default = "default_log_poll_secs")]
    pub poll_secs: u64,
}

impl Default for Logging {
    fn default() -> Self {
        Self {
            path: default_log_path(),
            max_bytes: default_log_max_bytes(),
            max_archives: default_log_max_archives(),
            s3_upload: true,
            s3_prefix: default_log_s3_prefix(),
            poll_secs: default_log_poll_secs(),
        }
    }
}

fn default_log_path() -> PathBuf {
    PathBuf::from("./vamoose.log")
}
fn default_log_max_bytes() -> String {
    "50 MiB".to_string()
}
fn default_log_max_archives() -> usize {
    10
}
fn default_log_s3_prefix() -> String {
    "logs".to_string()
}
fn default_log_poll_secs() -> u64 {
    10
}

impl Config {
    pub fn load(path: Option<PathBuf>) -> anyhow::Result<Self> {
        let path = path.unwrap_or_else(|| PathBuf::from("vamoose.toml"));
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("reading config from {}", path.display()))?;
        let cfg: Config = toml::from_str(&text)
            .with_context(|| format!("parsing config at {}", path.display()))?;
        Ok(cfg)
    }

    /// Best-effort path-aware variant: returns the resolved config
    /// path alongside the parsed body so error messages and doctor
    /// output can show the operator which file was actually loaded.
    pub fn load_with_path(path: Option<PathBuf>) -> anyhow::Result<(Self, PathBuf)> {
        let resolved = path.unwrap_or_else(|| PathBuf::from("vamoose.toml"));
        let text = std::fs::read_to_string(&resolved)
            .with_context(|| format!("reading config from {}", resolved.display()))?;
        let cfg: Config = toml::from_str(&text)
            .with_context(|| format!("parsing config at {}", resolved.display()))?;
        Ok((cfg, resolved))
    }
}
