//! Composition-level configuration for the unified CLI.
//!
//! The existing worker `[run]` format is canonical. This module adds
//! the CLI-only `[nfs]`, `[walker]`, `[aggr]`, and `[logging]` sections,
//! normalizes storage settings for control-plane commands, and retains
//! the older `[global]`/`[s3]` shape as a compatibility input.
//!
//! Concrete worker sections remain owned by
//! [`migration_worker::config`]; this module only assembles those
//! sections and projects compatibility input into them.

use anyhow::{Context, Result};
use migration_worker::config as wcfg;
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug)]
pub(crate) struct Config {
    source: SourceFormat,
    storage: StorageSettings,
    nfs: Option<Nfs>,
    walker: Option<Walker>,
    #[allow(dead_code)]
    aggr: Option<Aggr>,
    logging: Option<Logging>,
    coord_server: Option<CoordServer>,
    prepare: Option<Prepare>,
    worker: WorkerInput,
}

/// `vamoose prepare` settings. Every field has a default; the roots
/// are the paths inside the source and destination exports that the
/// migration covers and are recorded in `manifest.json`.
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct Prepare {
    /// Scratch and checkpoints: `<work_dir>/<run_id>/`.
    #[serde(default = "default_prepare_work_dir")]
    pub(crate) work_dir: PathBuf,
    /// Path inside `[mover] src_url` to scan and migrate.
    #[serde(default = "default_export_root")]
    pub(crate) source_root: String,
    /// Path inside `[mover] dst_url` to write into.
    #[serde(default = "default_export_root")]
    pub(crate) dest_root: String,
    /// nfs-walker executable. Default: the packaged
    /// `/usr/libexec/vamoose/nfs-walker`, then `nfs-walker` on PATH.
    #[serde(default)]
    pub(crate) walker_bin: Option<PathBuf>,
    /// nfs-walker GETATTR worker threads.
    #[serde(default = "default_walker_workers")]
    pub(crate) walker_workers: usize,
    /// nfs-walker `--exclude` patterns.
    #[serde(default)]
    pub(crate) exclude: Vec<String>,
    /// Parquet part-file rotation size handed to nfs-walker; one part
    /// becomes one index shard, i.e. one unit of claimable work.
    #[serde(default = "default_shard_size_mb")]
    pub(crate) shard_size_mb: u64,
}

impl Default for Prepare {
    fn default() -> Self {
        Self {
            work_dir: default_prepare_work_dir(),
            source_root: default_export_root(),
            dest_root: default_export_root(),
            walker_bin: None,
            walker_workers: default_walker_workers(),
            exclude: Vec::new(),
            shard_size_mb: default_shard_size_mb(),
        }
    }
}

fn default_prepare_work_dir() -> PathBuf {
    PathBuf::from("/var/lib/vamoose/prepare")
}
fn default_export_root() -> String {
    "/".to_string()
}
fn default_walker_workers() -> usize {
    32
}
fn default_shard_size_mb() -> u64 {
    512
}

/// Coordinator-daemon and TUI settings. They live in the same `[coord]`
/// table as the worker's client wiring so an operator file has one
/// coordinator block: the worker crate deserializes only the fields it
/// knows and ignores these, and this struct ignores the worker's.
#[derive(Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CoordServer {
    /// Bind address for `vamoose coord` (default `0.0.0.0:8443`).
    #[serde(default)]
    pub(crate) listen: Option<String>,
    #[serde(default)]
    pub(crate) tls_cert: Option<PathBuf>,
    #[serde(default)]
    pub(crate) tls_key: Option<PathBuf>,
    /// Serve plain HTTP. Must be explicit: a coord without TLS files
    /// and without this flag refuses to start.
    #[serde(default)]
    pub(crate) no_tls: bool,
    /// Admin bearer tokens, one per line (`token<TAB>label`, label
    /// optional). The coord loads every line; the TUI uses the first.
    #[serde(default)]
    pub(crate) admin_tokens_file: Option<PathBuf>,
    /// Permit dev mode (no tokens, no cluster secret) on a
    /// non-loopback bind. Lab-only escape hatch.
    #[serde(default)]
    pub(crate) allow_unauthenticated_nonloopback: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SourceFormat {
    Canonical,
    Compatibility,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StorageSettings {
    pub(crate) bucket: String,
    pub(crate) endpoint: String,
    pub(crate) region: String,
    pub(crate) profile: Option<String>,
    pub(crate) verify_tls: bool,
}

#[derive(Clone, Debug)]
pub(crate) enum LoggingPolicy {
    MinimalFallback,
    Standard(Logging),
}

#[derive(Debug)]
enum WorkerInput {
    Canonical(Box<CanonicalWorkerInput>),
    Compatibility(CompatibilityWorkerInput),
}

#[derive(Debug)]
struct CanonicalWorkerInput {
    run: wcfg::RunCfg,
    worker: Option<wcfg::WorkerCfg>,
    shard: Option<wcfg::ShardCfg>,
    mover: Option<wcfg::MoverCfg>,
    batch: Option<wcfg::BatchCfg>,
    copy: Option<wcfg::CopyCfg>,
    backpressure: Option<wcfg::BackpressureCfg>,
    coord: Option<wcfg::CoordCfg>,
}

#[derive(Debug)]
struct CompatibilityWorkerInput {
    worker: Option<CompatibilityWorker>,
    copy: Option<CompatibilityCopy>,
}

#[derive(Deserialize, Debug)]
struct CanonicalInput {
    run: wcfg::RunCfg,
    #[serde(default)]
    worker: Option<wcfg::WorkerCfg>,
    #[serde(default)]
    shard: Option<wcfg::ShardCfg>,
    #[serde(default)]
    mover: Option<wcfg::MoverCfg>,
    #[serde(default)]
    batch: Option<wcfg::BatchCfg>,
    #[serde(default)]
    copy: Option<wcfg::CopyCfg>,
    #[serde(default)]
    backpressure: Option<wcfg::BackpressureCfg>,
    #[serde(default)]
    coord: Option<wcfg::CoordCfg>,
    #[serde(default)]
    nfs: Option<Nfs>,
    #[serde(default)]
    walker: Option<Walker>,
    #[serde(default)]
    aggr: Option<Aggr>,
    #[serde(default)]
    logging: Option<Logging>,
    #[serde(default)]
    prepare: Option<Prepare>,
}

/// Private representation of the older vamoose configuration shape.
#[derive(Deserialize, Debug)]
struct CompatibilityInput {
    global: CompatibilityGlobal,
    s3: CompatibilityS3,
    #[serde(default)]
    nfs: Option<Nfs>,
    #[serde(default)]
    worker: Option<CompatibilityWorker>,
    #[serde(default)]
    walker: Option<Walker>,
    #[serde(default)]
    aggr: Option<Aggr>,
    #[serde(default)]
    copy: Option<CompatibilityCopy>,
    #[serde(default)]
    logging: Option<Logging>,
}

#[derive(Deserialize, Debug)]
struct CompatibilityGlobal {
    bucket: String,
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct CompatibilityS3 {
    endpoint: String,
    #[serde(default = "default_region")]
    region: String,
    #[serde(default)]
    profile: Option<String>,
    #[serde(default)]
    access_key: Option<String>,
    #[serde(default)]
    secret_key: Option<String>,
    #[serde(default)]
    no_verify_ssl: Option<bool>,
}

fn default_region() -> String {
    "us-east-1".to_string()
}

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct Nfs {
    pub(crate) src_url: String,
    pub(crate) dst_url: String,
    pub(crate) src_mount: String,
    pub(crate) dst_mount: String,
    pub(crate) src_root: String,
    pub(crate) dst_root: String,
}

#[derive(Deserialize, Debug)]
struct CompatibilityWorker {
    #[serde(default)]
    host_id: Option<String>,
    #[serde(default = "default_heartbeat_sec")]
    heartbeat_sec: u64,
    #[serde(default = "default_lease_timeout_sec")]
    lease_timeout_sec: u64,
    #[serde(default = "default_concurrency")]
    concurrency: usize,
    #[serde(default = "default_scratch")]
    local_scratch: PathBuf,
    #[serde(default = "default_bytes_budget")]
    bytes_budget: String,
}

impl Default for CompatibilityWorker {
    fn default() -> Self {
        Self {
            host_id: None,
            heartbeat_sec: default_heartbeat_sec(),
            lease_timeout_sec: default_lease_timeout_sec(),
            concurrency: default_concurrency(),
            local_scratch: default_scratch(),
            bytes_budget: default_bytes_budget(),
        }
    }
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

#[allow(dead_code)]
#[derive(Deserialize, Debug, Clone, Default)]
pub(crate) struct Walker {
    #[serde(default = "default_walker_threads")]
    pub(crate) threads: usize,
    #[serde(default)]
    pub(crate) binary_path: Option<PathBuf>,
}

fn default_walker_threads() -> usize {
    16
}

#[allow(dead_code)]
#[derive(Deserialize, Debug, Clone, Default)]
struct Aggr {
    #[serde(default = "default_aggr_refresh")]
    refresh_interval_sec: u64,
}

fn default_aggr_refresh() -> u64 {
    5
}

#[derive(Deserialize, Debug)]
struct CompatibilityCopy {
    #[serde(default = "default_true")]
    preserve_owner: bool,
    #[serde(default = "default_true")]
    preserve_mode: bool,
    #[serde(default = "default_true")]
    preserve_times: bool,
    #[serde(default = "default_true")]
    preserve_xattr: bool,
    #[serde(default = "default_ssc")]
    server_side_copy: Option<String>,
}

impl Default for CompatibilityCopy {
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

/// Rotating CLI logs. The active file is plain text and rotated
/// archives are gzipped. Uploads use the normalized storage settings,
/// regardless of which input format supplied them.
#[derive(Deserialize, Debug, Clone)]
pub(crate) struct Logging {
    #[serde(default = "default_log_path")]
    pub(crate) path: PathBuf,
    #[serde(default = "default_log_max_bytes")]
    pub(crate) max_bytes: String,
    #[serde(default = "default_log_max_archives")]
    pub(crate) max_archives: usize,
    #[serde(default = "default_true")]
    pub(crate) s3_upload: bool,
    #[serde(default = "default_log_s3_prefix")]
    pub(crate) s3_prefix: String,
    #[serde(default = "default_log_poll_secs")]
    pub(crate) poll_secs: u64,
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

/// System-wide configuration directory. Packages install the example
/// files here and the systemd units run with this directory's files.
pub(crate) const SYSTEM_CONFIG_DIR: &str = "/etc/vamoose";

/// Default configuration file when no `--config` / `VAMOOSE_CONFIG`
/// is given and the working directory has no `vamoose.toml`.
pub(crate) const SYSTEM_CONFIG_PATH: &str = "/etc/vamoose/vamoose.toml";

/// Candidate paths, in order, for a command started without an
/// explicit configuration path:
///
/// 1. `/etc/vamoose/workers/<instance>.toml` when `VAMOOSE_INSTANCE`
///    is set (the `vamoose-worker@.service` template exports its
///    instance name so one host can run differently tuned workers);
/// 2. `./vamoose.toml` — the development convenience;
/// 3. `/etc/vamoose/vamoose.toml` — the packaged default.
///
/// A per-instance file is optional: an instance with no dedicated
/// file falls through to the shared system file, so the common
/// single-worker-per-host install needs exactly one config.
pub(crate) fn default_config_candidates(instance: Option<&std::ffi::OsStr>) -> Vec<PathBuf> {
    let mut out = Vec::with_capacity(3);
    if let Some(name) = instance.filter(|n| !n.is_empty()) {
        let mut file = std::ffi::OsString::from(name);
        file.push(".toml");
        out.push(PathBuf::from(SYSTEM_CONFIG_DIR).join("workers").join(file));
    }
    out.push(PathBuf::from("vamoose.toml"));
    out.push(PathBuf::from(SYSTEM_CONFIG_PATH));
    out
}

/// Pick the first existing candidate from [`default_config_candidates`].
/// `exists` is injected so the search order is unit-testable without
/// touching `/etc`.
pub(crate) fn resolve_default_path(
    instance: Option<&std::ffi::OsStr>,
    exists: impl Fn(&std::path::Path) -> bool,
) -> Result<PathBuf> {
    let candidates = default_config_candidates(instance);
    if let Some(found) = candidates.iter().find(|c| exists(c)) {
        return Ok(found.clone());
    }
    let tried = candidates
        .iter()
        .map(|c| c.display().to_string())
        .collect::<Vec<_>>()
        .join(", ");
    anyhow::bail!(
        "no configuration file found (tried {tried}); pass --config <path>, set \
         VAMOOSE_CONFIG, or install one at {SYSTEM_CONFIG_PATH} \
         (see {SYSTEM_CONFIG_PATH}.example)"
    )
}

impl Config {
    pub(crate) fn load(path: Option<PathBuf>) -> Result<Self> {
        Self::load_with_path(path).map(|(config, _)| config)
    }

    pub(crate) fn load_with_path(path: Option<PathBuf>) -> Result<(Self, PathBuf)> {
        let resolved = match path {
            Some(p) => p,
            None => resolve_default_path(
                std::env::var_os("VAMOOSE_INSTANCE").as_deref(),
                |candidate| candidate.is_file(),
            )?,
        };
        let text = std::fs::read_to_string(&resolved)
            .with_context(|| format!("reading config from {}", resolved.display()))?;
        let config = Self::parse(&text)
            .with_context(|| format!("parsing config at {}", resolved.display()))?;
        Ok((config, resolved))
    }

    fn parse(text: &str) -> Result<Self> {
        let roots: toml::Table = toml::from_str(text).context("invalid TOML configuration")?;
        let has_run = roots.contains_key("run");
        let has_compatibility_root = roots.contains_key("global") || roots.contains_key("s3");

        if has_run && has_compatibility_root {
            anyhow::bail!(
                "configuration mixes canonical [run] with compatibility [global]/[s3] roots; choose one format"
            );
        }

        if has_run {
            let input: CanonicalInput =
                toml::from_str(text).context("invalid canonical [run] configuration")?;
            let coord_server = match roots.get("coord") {
                Some(table) => Some(
                    table
                        .clone()
                        .try_into::<CoordServer>()
                        .context("invalid [coord] coordinator/TUI settings")?,
                ),
                None => None,
            };
            return Ok(Self::from_canonical(input, coord_server));
        }

        if has_compatibility_root {
            let input: CompatibilityInput = toml::from_str(text)
                .context("invalid compatibility [global]/[s3] configuration")?;
            return Ok(Self::from_compatibility(input));
        }

        anyhow::bail!(
            "configuration must contain canonical [run] or compatibility [global] and [s3] sections"
        )
    }

    fn from_canonical(input: CanonicalInput, coord_server: Option<CoordServer>) -> Self {
        let storage = StorageSettings {
            bucket: input.run.bucket.clone(),
            endpoint: input.run.endpoint.clone(),
            region: input.run.region.clone(),
            profile: input.run.profile.clone(),
            verify_tls: input.run.verify_tls,
        };
        Self {
            source: SourceFormat::Canonical,
            storage,
            nfs: input.nfs,
            walker: input.walker,
            aggr: input.aggr,
            logging: input.logging,
            coord_server,
            prepare: input.prepare,
            worker: WorkerInput::Canonical(Box::new(CanonicalWorkerInput {
                run: input.run,
                worker: input.worker,
                shard: input.shard,
                mover: input.mover,
                batch: input.batch,
                copy: input.copy,
                backpressure: input.backpressure,
                coord: input.coord,
            })),
        }
    }

    fn from_compatibility(input: CompatibilityInput) -> Self {
        let storage = StorageSettings {
            bucket: input.global.bucket,
            endpoint: input.s3.endpoint,
            region: input.s3.region,
            profile: input.s3.profile,
            verify_tls: !input.s3.no_verify_ssl.unwrap_or(false),
        };
        Self {
            source: SourceFormat::Compatibility,
            storage,
            nfs: input.nfs,
            walker: input.walker,
            aggr: input.aggr,
            logging: input.logging,
            coord_server: None,
            prepare: None,
            worker: WorkerInput::Compatibility(CompatibilityWorkerInput {
                worker: input.worker,
                copy: input.copy,
            }),
        }
    }

    pub(crate) fn storage(&self) -> &StorageSettings {
        &self.storage
    }

    pub(crate) fn nfs(&self) -> Option<&Nfs> {
        self.nfs.as_ref()
    }

    /// The canonical `[mover]` table (source and destination URLs and
    /// the libnfs tunables). `None` for the compatibility format or
    /// when the table is absent.
    pub(crate) fn mover(&self) -> Option<&wcfg::MoverCfg> {
        match &self.worker {
            WorkerInput::Canonical(input) => input.mover.as_ref(),
            WorkerInput::Compatibility(_) => None,
        }
    }

    pub(crate) fn walker(&self) -> Option<&Walker> {
        self.walker.as_ref()
    }

    /// The worker-shaped `[coord]` client wiring (URL, job, secret).
    /// `None` for the compatibility format or when the table is absent.
    pub(crate) fn coord(&self) -> Option<&wcfg::CoordCfg> {
        match &self.worker {
            WorkerInput::Canonical(input) => input.coord.as_ref(),
            WorkerInput::Compatibility(_) => None,
        }
    }

    /// Coordinator-daemon / TUI extras from the same `[coord]` table.
    pub(crate) fn coord_server(&self) -> Option<&CoordServer> {
        self.coord_server.as_ref()
    }

    /// `[prepare]` settings, defaulted when the table is absent.
    pub(crate) fn prepare(&self) -> Prepare {
        self.prepare.clone().unwrap_or_default()
    }

    pub(crate) fn logging_policy(&self) -> LoggingPolicy {
        match (self.source, self.logging.clone()) {
            (SourceFormat::Canonical, None) => LoggingPolicy::MinimalFallback,
            (_, Some(logging)) => LoggingPolicy::Standard(logging),
            (SourceFormat::Compatibility, None) => LoggingPolicy::Standard(Logging::default()),
        }
    }

    pub(crate) fn into_worker_config(self) -> Result<(wcfg::Config, Option<String>)> {
        match self.worker {
            WorkerInput::Canonical(input) => canonical_worker_config(*input),
            WorkerInput::Compatibility(input) => {
                compatibility_worker_config(input, self.storage, self.nfs)
            }
        }
    }
}

fn canonical_worker_config(input: CanonicalWorkerInput) -> Result<(wcfg::Config, Option<String>)> {
    // Only `[mover]` carries settings without a production default
    // (the source and destination URLs); every other section falls
    // back to the worker crate's defaults so a minimal operator file
    // is `[run]` + `[mover]`.
    let Some(mover) = input.mover else {
        anyhow::bail!(
            "canonical [run] configuration cannot start `vamoose worker`; missing required \
             section [mover] (src_url / dst_url)"
        );
    };
    let worker = input.worker.unwrap_or_default();
    let shard = input.shard.unwrap_or_default();
    let batch = input.batch.unwrap_or_default();
    let copy = input.copy.unwrap_or_default();
    let backpressure = input.backpressure.unwrap_or_default();
    let host_id = worker.host_id.clone();
    Ok((
        wcfg::Config {
            run: input.run,
            worker,
            shard,
            mover,
            batch,
            copy,
            backpressure,
            coord: input.coord,
        },
        host_id,
    ))
}

fn compatibility_worker_config(
    input: CompatibilityWorkerInput,
    storage: StorageSettings,
    nfs: Option<Nfs>,
) -> Result<(wcfg::Config, Option<String>)> {
    let nfs = nfs.ok_or_else(|| {
        anyhow::anyhow!(
            "config has no [nfs] section, but `vamoose worker` requires one \
             (src_url/dst_url/mounts/roots define the copy endpoints); \
             add an [nfs] section to the config"
        )
    })?;
    let worker = input.worker.unwrap_or_default();
    let copy = input.copy.unwrap_or_default();
    let host_id = worker.host_id.clone();

    Ok((
        wcfg::Config {
            run: wcfg::RunCfg {
                bucket: storage.bucket,
                endpoint: storage.endpoint,
                region: storage.region,
                profile: storage.profile,
                verify_tls: storage.verify_tls,
            },
            worker: wcfg::WorkerCfg {
                host_id: worker.host_id,
                heartbeat_sec: worker.heartbeat_sec,
                lease_timeout_sec: worker.lease_timeout_sec,
            },
            shard: wcfg::ShardCfg {
                local_scratch: worker.local_scratch,
                max_in_flight: 1,
            },
            mover: wcfg::MoverCfg {
                strategy_default: "libnfs_io_uring".to_string(),
                src_url: nfs.src_url,
                dst_url: nfs.dst_url,
                nfs_connections: worker.concurrency.max(1) as u32,
                rpc_timeout_ms: migration_mover::DEFAULT_RPC_TIMEOUT_MS,
                pipeline_depth: 8,
                io_uring_queue_depth: 256,
                fixed_buffer_count: 256,
                fixed_buffer_size: "1 MiB".to_string(),
                use_bucketed_pool: false,
                use_raw_fh: false,
                direct_commit: false,
            },
            batch: wcfg::BatchCfg {
                bytes_budget: worker.bytes_budget,
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
                server_side_copy: copy.server_side_copy.unwrap_or_else(|| "off".to_string()),
                require_chown_capability: true,
                require_unchanged_size: false,
            },
            backpressure: wcfg::BackpressureCfg {
                failure_pct_window_sec: 60,
                failure_pct_threshold: 5.0,
                throughput_floor_mb_s: 100,
            },
            coord: None,
        },
        host_id,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL_COMPATIBILITY: &str = r#"
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

    const RUN_ONLY: &str = r#"
        [run]
        bucket = "control-bucket"
        endpoint = "https://s3.example.test"
        region = "moon-1"
        profile = "operator"
        verify_tls = false
    "#;

    const FULL_CANONICAL: &str = r#"
        [run]
        bucket = "full-bucket"
        endpoint = "https://storage.example.test"
        region = "region-9"
        profile = "full-profile"
        verify_tls = false

        [worker]
        host_id = "configured-host"
        heartbeat_sec = 7
        lease_timeout_sec = 41

        [shard]
        local_scratch = "/scratch/custom"
        max_in_flight = 3

        [mover]
        strategy_default = "custom-strategy"
        src_url = "nfs://src/custom"
        dst_url = "nfs://dst/custom"
        nfs_connections = 23
        rpc_timeout_ms = 4321
        pipeline_depth = 9
        io_uring_queue_depth = 99
        fixed_buffer_count = 88
        fixed_buffer_size = "3 MiB"
        use_bucketed_pool = true

        [batch]
        bytes_budget = "17 GiB"
        files_budget = 12345
        inflight_small = 33
        inflight_medium = 22
        inflight_large = 11
        large_stripe_size = "7 MiB"
        large_stripe_depth = 19

        [copy]
        preserve_owner = false
        preserve_mode = false
        preserve_times = false
        preserve_xattr = false
        server_side_copy = "force"
        require_chown_capability = false
        require_unchanged_size = true

        [backpressure]
        failure_pct_window_sec = 17
        failure_pct_threshold = 2.5
        throughput_floor_mb_s = 777

        [coord]
        url = "https://coord.example.test"
        job_id = "job-17"
        cluster_secret_env = "CLUSTER_SECRET"
        heartbeat_sec = 13
        events_flush_sec = 4
        buffer_max_bytes = 98765
        verify_tls = false
        request_timeout_sec = 23
    "#;

    #[test]
    fn examples_worker_toml_is_canonical_and_projects_to_worker() {
        let config = Config::parse(include_str!("../../../examples/worker.toml"))
            .expect("worker example must parse as composition config");
        assert_eq!(config.source, SourceFormat::Canonical);
        assert_eq!(config.storage().bucket, "vamoose");

        let (worker, _) = config
            .into_worker_config()
            .expect("worker example must project");
        assert_eq!(worker.mover.rpc_timeout_ms, 60_000);
        assert_eq!(worker.mover.strategy_default, "libnfs_io_uring");
        assert_eq!(worker.mover.pipeline_depth, 8);
        assert_eq!(worker.mover.io_uring_queue_depth, 256);
        assert_eq!(worker.mover.fixed_buffer_count, 256);
        assert_eq!(worker.mover.fixed_buffer_size, "1 MiB");
        assert_eq!(worker.copy.server_side_copy, "off");
        assert_eq!(worker.batch.inflight_small, 256);
    }

    #[test]
    fn canonical_run_only_is_valid_for_control_commands() {
        let config = Config::parse(RUN_ONLY).expect("[run]-only config must parse");
        assert_eq!(config.source, SourceFormat::Canonical);
        assert_eq!(config.storage().bucket, "control-bucket");
        assert_eq!(config.storage().endpoint, "https://s3.example.test");
        assert_eq!(config.storage().region, "moon-1");
        assert_eq!(config.storage().profile.as_deref(), Some("operator"));
        assert!(!config.storage().verify_tls);
    }

    #[test]
    fn canonical_cli_only_sections_remain_available_to_doctor() {
        let input = format!(
            r#"{RUN_ONLY}
                [nfs]
                src_url = "nfs://src/export"
                dst_url = "nfs://dst/export"
                src_mount = "/mnt/src"
                dst_mount = "/mnt/dst"
                src_root = "/source"
                dst_root = "/destination"

                [walker]
                binary_path = "/opt/vamoose/nfs-walker"
            "#
        );
        let config = Config::parse(&input).expect("canonical CLI-only sections must parse");
        assert_eq!(config.nfs().unwrap().src_mount, "/mnt/src");
        assert_eq!(
            config.walker().unwrap().binary_path.as_deref(),
            Some(std::path::Path::new("/opt/vamoose/nfs-walker"))
        );
    }

    #[test]
    fn canonical_run_only_worker_error_names_mover() {
        let error = Config::parse(RUN_ONLY)
            .expect("composition parse must succeed")
            .into_worker_config()
            .expect_err("worker projection must fail");
        let message = error.to_string();
        assert!(message.contains("[mover]"), "missing [mover] in: {message}");
    }

    /// The minimal operator file: `[run]` plus the two NFS URLs. Every
    /// other worker section takes the crate defaults.
    #[test]
    fn run_plus_mover_projects_with_defaults() {
        let text = r#"
            [run]
            bucket = "b"
            endpoint = "https://s3.example.test"
            region = "us-east-1"

            [mover]
            src_url = "nfs://src/export"
            dst_url = "nfs://dst/export"
        "#;
        let (worker, host_id) = Config::parse(text)
            .expect("parse")
            .into_worker_config()
            .expect("minimal file must project");
        assert_eq!(host_id, None);
        assert_eq!(worker.worker.heartbeat_sec, 30);
        assert_eq!(worker.worker.lease_timeout_sec, 180);
        assert_eq!(
            worker.shard.local_scratch,
            std::path::PathBuf::from("/var/lib/vamoose/scratch")
        );
        assert_eq!(worker.batch.bytes_budget, "8 GiB");
        assert!(worker.copy.preserve_owner);
        assert_eq!(worker.backpressure.throughput_floor_mb_s, 100);
        assert!(worker.coord.is_none());
    }

    /// The shipped quickstart example is the slim file the packages
    /// install as `/etc/vamoose/vamoose.toml.example`; it must project
    /// to a worker config and expose the coordinator settings.
    #[test]
    fn examples_vamoose_toml_projects_and_carries_coord_settings() {
        let config = Config::parse(include_str!("../../../examples/vamoose.toml"))
            .expect("quickstart example must parse");
        let server = config
            .coord_server()
            .cloned()
            .expect("[coord] must yield server settings");
        assert!(server.no_tls);
        assert_eq!(
            server.admin_tokens_file.as_deref(),
            Some(std::path::Path::new("/etc/vamoose/admin-token"))
        );
        let client = config.coord().expect("[coord] must yield client wiring");
        assert_eq!(client.url, "http://node1.example.com:8443");
        assert_eq!(
            client.job_id, None,
            "job_id follows the manifest by default"
        );
        assert_eq!(
            client.cluster_secret_env.as_deref(),
            Some("VAMOOSE_CLUSTER_SECRET")
        );
        let (worker, _) = config.into_worker_config().expect("must project");
        assert!(worker.mover.use_raw_fh);
        assert_eq!(worker.mover.nfs_connections, 32);
    }

    /// `[coord]` extras must not break the worker projection, and a
    /// file without `[coord]` yields no server settings.
    #[test]
    fn coord_server_settings_are_optional_and_parsed_from_the_same_table() {
        let text = r#"
            [run]
            bucket = "b"
            endpoint = "https://s3.example.test"
            region = "us-east-1"

            [coord]
            url = "https://coord.example.test:8443"
            listen = "127.0.0.1:9000"
            tls_cert = "/etc/vamoose/coord.crt"
            tls_key = "/etc/vamoose/coord.key"
        "#;
        let config = Config::parse(text).expect("parse");
        let server = config.coord_server().expect("server settings");
        assert_eq!(server.listen.as_deref(), Some("127.0.0.1:9000"));
        assert!(!server.no_tls);
        assert_eq!(
            server.tls_cert.as_deref(),
            Some(std::path::Path::new("/etc/vamoose/coord.crt"))
        );
        assert_eq!(
            config.coord().map(|c| c.url.as_str()),
            Some("https://coord.example.test:8443")
        );

        let none = Config::parse(RUN_ONLY).expect("parse");
        assert!(none.coord_server().is_none());
        assert!(none.coord().is_none());
    }

    #[test]
    fn canonical_worker_projection_preserves_all_fields_and_coord() {
        let (worker, host_id) = Config::parse(FULL_CANONICAL)
            .expect("full canonical config must parse")
            .into_worker_config()
            .expect("full canonical config must project");

        assert_eq!(host_id.as_deref(), Some("configured-host"));
        assert_eq!(worker.run.bucket, "full-bucket");
        assert_eq!(worker.run.endpoint, "https://storage.example.test");
        assert_eq!(worker.run.region, "region-9");
        assert_eq!(worker.run.profile.as_deref(), Some("full-profile"));
        assert!(!worker.run.verify_tls);
        assert_eq!(worker.worker.heartbeat_sec, 7);
        assert_eq!(worker.worker.lease_timeout_sec, 41);
        assert_eq!(worker.shard.local_scratch, PathBuf::from("/scratch/custom"));
        assert_eq!(worker.shard.max_in_flight, 3);
        assert_eq!(worker.mover.strategy_default, "custom-strategy");
        assert_eq!(worker.mover.src_url, "nfs://src/custom");
        assert_eq!(worker.mover.dst_url, "nfs://dst/custom");
        assert_eq!(worker.mover.nfs_connections, 23);
        assert_eq!(worker.mover.rpc_timeout_ms, 4321);
        assert_eq!(worker.mover.pipeline_depth, 9);
        assert_eq!(worker.mover.io_uring_queue_depth, 99);
        assert_eq!(worker.mover.fixed_buffer_count, 88);
        assert_eq!(worker.mover.fixed_buffer_size, "3 MiB");
        assert!(worker.mover.use_bucketed_pool);
        assert_eq!(worker.batch.bytes_budget, "17 GiB");
        assert_eq!(worker.batch.files_budget, 12_345);
        assert_eq!(worker.batch.inflight_small, 33);
        assert_eq!(worker.batch.inflight_medium, 22);
        assert_eq!(worker.batch.inflight_large, 11);
        assert_eq!(worker.batch.large_stripe_size, "7 MiB");
        assert_eq!(worker.batch.large_stripe_depth, 19);
        assert!(!worker.copy.preserve_owner);
        assert!(!worker.copy.preserve_mode);
        assert!(!worker.copy.preserve_times);
        assert!(!worker.copy.preserve_xattr);
        assert_eq!(worker.copy.server_side_copy, "force");
        assert!(!worker.copy.require_chown_capability);
        assert!(worker.copy.require_unchanged_size);
        assert_eq!(worker.backpressure.failure_pct_window_sec, 17);
        assert_eq!(worker.backpressure.failure_pct_threshold, 2.5);
        assert_eq!(worker.backpressure.throughput_floor_mb_s, 777);

        let coord = worker.coord.expect("[coord] must survive projection");
        assert_eq!(coord.url, "https://coord.example.test");
        assert_eq!(coord.job_id.as_deref(), Some("job-17"));
        assert_eq!(coord.cluster_secret_env.as_deref(), Some("CLUSTER_SECRET"));
        assert_eq!(coord.heartbeat_sec, 13);
        assert_eq!(coord.events_flush_sec, 4);
        assert_eq!(coord.buffer_max_bytes, 98_765);
        assert!(!coord.verify_tls);
        assert_eq!(coord.request_timeout_sec, 23);
    }

    #[test]
    fn compatibility_input_and_worker_defaults_are_preserved() {
        let config = Config::parse(MINIMAL_COMPATIBILITY).expect("compatibility config parses");
        assert_eq!(config.source, SourceFormat::Compatibility);
        assert_eq!(config.storage().bucket, "vamoose-test");
        assert_eq!(config.storage().region, "us-east-1");
        assert!(config.storage().verify_tls);

        let (worker, host_id) = config
            .into_worker_config()
            .expect("worker projection succeeds");
        assert!(host_id.is_none());
        assert_eq!(worker.worker.heartbeat_sec, 30);
        assert_eq!(worker.worker.lease_timeout_sec, 180);
        assert_eq!(
            worker.shard.local_scratch,
            PathBuf::from("/tmp/vamoose-scratch")
        );
        assert_eq!(worker.shard.max_in_flight, 1);
        assert_eq!(worker.mover.strategy_default, "libnfs_io_uring");
        assert_eq!(worker.mover.nfs_connections, 16);
        assert_eq!(
            worker.mover.rpc_timeout_ms,
            migration_mover::DEFAULT_RPC_TIMEOUT_MS
        );
        assert_eq!(worker.mover.pipeline_depth, 8);
        assert_eq!(worker.mover.io_uring_queue_depth, 256);
        assert_eq!(worker.mover.fixed_buffer_count, 256);
        assert_eq!(worker.mover.fixed_buffer_size, "1 MiB");
        assert!(!worker.mover.use_bucketed_pool);
        assert_eq!(worker.batch.bytes_budget, "8 GiB");
        assert_eq!(worker.batch.files_budget, 100_000);
        assert_eq!(worker.batch.inflight_small, 256);
        assert_eq!(worker.batch.inflight_medium, 16);
        assert_eq!(worker.batch.inflight_large, 4);
        assert_eq!(worker.batch.large_stripe_size, "4 MiB");
        assert_eq!(worker.batch.large_stripe_depth, 32);
        assert!(worker.copy.preserve_owner);
        assert!(worker.copy.preserve_mode);
        assert!(worker.copy.preserve_times);
        assert!(worker.copy.preserve_xattr);
        assert_eq!(worker.copy.server_side_copy, "off");
        assert!(worker.copy.require_chown_capability);
        assert!(!worker.copy.require_unchanged_size);
        assert_eq!(worker.backpressure.failure_pct_window_sec, 60);
        assert_eq!(worker.backpressure.failure_pct_threshold, 5.0);
        assert_eq!(worker.backpressure.throughput_floor_mb_s, 100);
        assert!(worker.coord.is_none());
    }

    #[test]
    fn compatibility_worker_requires_nfs() {
        let error = Config::parse(
            r#"
                [global]
                bucket = "test"
                [s3]
                endpoint = "http://localhost:9000"
            "#,
        )
        .expect("control config parses")
        .into_worker_config()
        .expect_err("worker requires [nfs]");
        let message = error.to_string();
        assert!(message.contains("[nfs]"));
        assert!(message.contains("worker"));
        assert!(message.contains("add an [nfs] section"));
    }

    #[test]
    fn compatibility_concurrency_is_clamped_to_one() {
        let input = format!("{MINIMAL_COMPATIBILITY}\n[worker]\nconcurrency = 0\n");
        let (worker, _) = Config::parse(&input).unwrap().into_worker_config().unwrap();
        assert_eq!(worker.mover.nfs_connections, 1);
    }

    #[test]
    fn compatibility_worker_projection_preserves_exposed_overrides() {
        let input = format!(
            r#"{MINIMAL_COMPATIBILITY}
                [worker]
                host_id = "compat-host"
                heartbeat_sec = 11
                lease_timeout_sec = 73
                concurrency = 27
                local_scratch = "/compat/scratch"
                bytes_budget = "19 GiB"

                [copy]
                preserve_owner = false
                preserve_mode = false
                preserve_times = false
                preserve_xattr = false
                server_side_copy = "force"
            "#
        );
        let (worker, host_id) = Config::parse(&input).unwrap().into_worker_config().unwrap();
        assert_eq!(host_id.as_deref(), Some("compat-host"));
        assert_eq!(worker.worker.heartbeat_sec, 11);
        assert_eq!(worker.worker.lease_timeout_sec, 73);
        assert_eq!(worker.shard.local_scratch, PathBuf::from("/compat/scratch"));
        assert_eq!(worker.mover.nfs_connections, 27);
        assert_eq!(worker.batch.bytes_budget, "19 GiB");
        assert!(!worker.copy.preserve_owner);
        assert!(!worker.copy.preserve_mode);
        assert!(!worker.copy.preserve_times);
        assert!(!worker.copy.preserve_xattr);
        assert_eq!(worker.copy.server_side_copy, "force");
    }

    #[test]
    fn tls_polarity_and_defaults_are_normalized() {
        let cases = [
            (RUN_ONLY.to_string(), false),
            (
                RUN_ONLY.replace("verify_tls = false", "verify_tls = true"),
                true,
            ),
            (MINIMAL_COMPATIBILITY.to_string(), true),
            (
                MINIMAL_COMPATIBILITY.replace(
                    "endpoint = \"http://127.0.0.1:9000\"",
                    "endpoint = \"http://127.0.0.1:9000\"\nno_verify_ssl = true",
                ),
                false,
            ),
            (
                MINIMAL_COMPATIBILITY.replace(
                    "endpoint = \"http://127.0.0.1:9000\"",
                    "endpoint = \"http://127.0.0.1:9000\"\nno_verify_ssl = false",
                ),
                true,
            ),
        ];

        for (input, expected) in cases {
            assert_eq!(
                Config::parse(&input).unwrap().storage().verify_tls,
                expected
            );
        }

        let canonical_default = RUN_ONLY.replace("        verify_tls = false\n", "");
        assert!(
            Config::parse(&canonical_default)
                .unwrap()
                .storage()
                .verify_tls
        );
    }

    #[test]
    fn logging_absence_policy_depends_on_source_format() {
        assert!(matches!(
            Config::parse(RUN_ONLY).unwrap().logging_policy(),
            LoggingPolicy::MinimalFallback
        ));
        assert!(matches!(
            Config::parse(MINIMAL_COMPATIBILITY)
                .unwrap()
                .logging_policy(),
            LoggingPolicy::Standard(_)
        ));
    }

    #[test]
    fn explicit_logging_is_honored_in_both_formats() {
        for input in [RUN_ONLY, MINIMAL_COMPATIBILITY] {
            let input =
                format!("{input}\n[logging]\npath = \"/tmp/explicit.log\"\ns3_upload = false\n");
            match Config::parse(&input).unwrap().logging_policy() {
                LoggingPolicy::Standard(logging) => {
                    assert_eq!(logging.path, PathBuf::from("/tmp/explicit.log"));
                    assert!(!logging.s3_upload);
                }
                LoggingPolicy::MinimalFallback => panic!("explicit logging was discarded"),
            }
        }
    }

    #[test]
    fn mixed_format_roots_are_rejected() {
        let mixed = format!(
            "{RUN_ONLY}\n[global]\nbucket = \"other\"\n[s3]\nendpoint = \"http://other\"\n"
        );
        let message = Config::parse(&mixed).unwrap_err().to_string();
        assert!(message.contains("mixes canonical [run]"));
        assert!(message.contains("[global]/[s3]"));
    }

    #[test]
    fn malformed_canonical_does_not_fall_through() {
        let malformed = RUN_ONLY.replace("bucket = \"control-bucket\"", "bucket = 17");
        let message = Config::parse(&malformed).unwrap_err().to_string();
        assert!(message.contains("canonical [run]"));
        assert!(!message.contains("compatibility [global]/[s3]"));
        assert!(format!("{:#}", Config::parse(&malformed).unwrap_err()).contains("bucket"));
    }

    #[test]
    fn malformed_compatibility_does_not_fall_through() {
        let malformed =
            MINIMAL_COMPATIBILITY.replace("endpoint = \"http://127.0.0.1:9000\"", "endpoint = 17");
        let message = Config::parse(&malformed).unwrap_err().to_string();
        assert!(message.contains("compatibility [global]/[s3]"));
        assert!(!message.contains("canonical [run] configuration"));
        assert!(format!("{:#}", Config::parse(&malformed).unwrap_err()).contains("endpoint"));
    }
}

#[cfg(test)]
mod default_path_tests {
    use super::*;
    use std::ffi::OsStr;
    use std::path::Path;

    #[test]
    fn candidates_prefer_instance_then_cwd_then_system() {
        let with = default_config_candidates(Some(OsStr::new("main")));
        assert_eq!(
            with,
            vec![
                PathBuf::from("/etc/vamoose/workers/main.toml"),
                PathBuf::from("vamoose.toml"),
                PathBuf::from(SYSTEM_CONFIG_PATH),
            ]
        );
        let without = default_config_candidates(None);
        assert_eq!(
            without,
            vec![
                PathBuf::from("vamoose.toml"),
                PathBuf::from(SYSTEM_CONFIG_PATH)
            ]
        );
        // An empty instance name is the same as none.
        assert_eq!(default_config_candidates(Some(OsStr::new(""))), without);
    }

    #[test]
    fn instance_without_dedicated_file_falls_through_to_system_file() {
        let resolved = resolve_default_path(Some(OsStr::new("main")), |p| {
            p == Path::new(SYSTEM_CONFIG_PATH)
        })
        .unwrap();
        assert_eq!(resolved, PathBuf::from(SYSTEM_CONFIG_PATH));
    }

    #[test]
    fn dedicated_instance_file_wins_over_shared_file() {
        let resolved = resolve_default_path(Some(OsStr::new("fast")), |_| true).unwrap();
        assert_eq!(resolved, PathBuf::from("/etc/vamoose/workers/fast.toml"));
    }

    #[test]
    fn cwd_file_wins_over_system_file() {
        let resolved = resolve_default_path(None, |p| {
            p == Path::new("vamoose.toml") || p == Path::new(SYSTEM_CONFIG_PATH)
        })
        .unwrap();
        assert_eq!(resolved, PathBuf::from("vamoose.toml"));
    }

    #[test]
    fn nothing_found_names_every_candidate() {
        let err = resolve_default_path(Some(OsStr::new("main")), |_| false).unwrap_err();
        let msg = format!("{err:#}");
        for needle in [
            "/etc/vamoose/workers/main.toml",
            "vamoose.toml",
            SYSTEM_CONFIG_PATH,
            "--config",
        ] {
            assert!(msg.contains(needle), "missing {needle:?} in {msg}");
        }
    }
}
