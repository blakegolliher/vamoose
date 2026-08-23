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
    worker: WorkerInput,
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

impl Config {
    pub(crate) fn load(path: Option<PathBuf>) -> Result<Self> {
        Self::load_with_path(path).map(|(config, _)| config)
    }

    pub(crate) fn load_with_path(path: Option<PathBuf>) -> Result<(Self, PathBuf)> {
        let resolved = path.unwrap_or_else(|| PathBuf::from("vamoose.toml"));
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
            return Ok(Self::from_canonical(input));
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

    fn from_canonical(input: CanonicalInput) -> Self {
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

    pub(crate) fn walker(&self) -> Option<&Walker> {
        self.walker.as_ref()
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
    let missing = [
        ("worker", input.worker.is_none()),
        ("shard", input.shard.is_none()),
        ("mover", input.mover.is_none()),
        ("batch", input.batch.is_none()),
        ("copy", input.copy.is_none()),
        ("backpressure", input.backpressure.is_none()),
    ]
    .into_iter()
    .filter_map(|(section, is_missing)| is_missing.then_some(format!("[{section}]")))
    .collect::<Vec<_>>();

    if !missing.is_empty() {
        anyhow::bail!(
            "canonical [run] configuration cannot start `vamoose worker`; missing required worker sections: {}",
            missing.join(", ")
        );
    }

    let worker = input.worker.context("missing [worker] after validation")?;
    let shard = input.shard.context("missing [shard] after validation")?;
    let mover = input.mover.context("missing [mover] after validation")?;
    let batch = input.batch.context("missing [batch] after validation")?;
    let copy = input.copy.context("missing [copy] after validation")?;
    let backpressure = input
        .backpressure
        .context("missing [backpressure] after validation")?;
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
    fn canonical_run_only_worker_error_names_every_missing_section() {
        let error = Config::parse(RUN_ONLY)
            .expect("composition parse must succeed")
            .into_worker_config()
            .expect_err("worker projection must fail");
        let message = error.to_string();
        for section in [
            "[worker]",
            "[shard]",
            "[mover]",
            "[batch]",
            "[copy]",
            "[backpressure]",
        ] {
            assert!(message.contains(section), "missing {section} in: {message}");
        }
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
        assert_eq!(coord.job_id, "job-17");
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
