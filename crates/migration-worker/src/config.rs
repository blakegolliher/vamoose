//! Worker configuration loaded from TOML. Mirrors the `[run]`,
//! `[worker]`, `[shard]`, `[mover]`, `[batch]`, `[copy]`,
//! `[backpressure]` sections in DESIGN.md.

use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub run: RunCfg,
    /// Every section except `[run]` and `[mover]` has production
    /// defaults, so a minimal operator file needs only the bucket and
    /// the two NFS URLs.
    #[serde(default)]
    pub worker: WorkerCfg,
    #[serde(default)]
    pub shard: ShardCfg,
    pub mover: MoverCfg,
    #[serde(default)]
    pub batch: BatchCfg,
    #[serde(default)]
    pub copy: CopyCfg,
    #[serde(default)]
    pub backpressure: BackpressureCfg,
    /// Coord wiring. When absent, the worker runs in legacy S3-only
    /// mode (heartbeat to S3, no HTTP traffic). When present, the
    /// worker registers with the coord and observes pause/resume
    /// commands via the heartbeat response.
    #[serde(default)]
    pub coord: Option<CoordCfg>,
}

#[derive(Debug, Deserialize)]
pub struct RunCfg {
    pub bucket: String,
    /// Key prefix inside the bucket (`"v4"` → every object under
    /// `v4/`). Empty = bucket root. Lets one bucket hold several runs;
    /// every host of a run must use the same value.
    #[serde(default)]
    pub prefix: String,
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
fn default_verify_tls() -> bool {
    true
}

#[derive(Debug, Deserialize)]
pub struct WorkerCfg {
    #[serde(default)]
    pub host_id: Option<String>,
    #[serde(default = "default_heartbeat_sec")]
    pub heartbeat_sec: u64,
    #[serde(default = "default_lease_timeout_sec")]
    pub lease_timeout_sec: u64,
}

impl Default for WorkerCfg {
    fn default() -> Self {
        Self {
            host_id: None,
            heartbeat_sec: default_heartbeat_sec(),
            lease_timeout_sec: default_lease_timeout_sec(),
        }
    }
}

fn default_heartbeat_sec() -> u64 {
    migration_core::time::DEFAULT_HEARTBEAT_SEC
}
fn default_lease_timeout_sec() -> u64 {
    migration_core::time::DEFAULT_LEASE_TIMEOUT_SEC
}

// `max_in_flight` is part of the published worker config schema
// (DESIGN.md "Configuration") even though no Rust path reads it yet;
// silence dead_code so removing the field doesn't become the path of
// least resistance.
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct ShardCfg {
    #[serde(default = "default_local_scratch")]
    pub local_scratch: std::path::PathBuf,
    #[serde(default = "one")]
    pub max_in_flight: u32,
}
impl Default for ShardCfg {
    fn default() -> Self {
        Self {
            local_scratch: default_local_scratch(),
            max_in_flight: one(),
        }
    }
}
/// Matches the directory the packaged systemd units expect; the
/// orchestrator creates it on startup.
fn default_local_scratch() -> std::path::PathBuf {
    std::path::PathBuf::from("/var/lib/vamoose/scratch")
}
fn one() -> u32 {
    1
}

// The canonical operator schema retains historical strategy/tuning fields.
// They continue to deserialize and project losslessly, but do not select or
// tune an executable path today. The orchestrator reads source/destination
// URLs from the manifest and uses nfs_connections, rpc_timeout_ms, and
// use_bucketed_pool. Keep the compatibility surface intact and silence
// dead_code at the struct level.
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct MoverCfg {
    /// Compatibility name for the regular-file libnfs strategy. Parsed and
    /// retained, but strategy selection is fixed to the implemented libnfs
    /// path.
    #[serde(default = "default_strategy")]
    pub strategy_default: String,
    pub src_url: String,
    pub dst_url: String,
    #[serde(default = "default_nfs_connections")]
    pub nfs_connections: u32,
    /// F12: per-RPC timeout (milliseconds) applied to every libnfs
    /// context at creation — the sync `MultiPool` pairs and the six
    /// bucketed-async contexts alike — bounding each RPC including
    /// the mount itself. `0` = leave the libnfs built-in default
    /// untouched (`nfs_set_timeout` is never called). Defaults to
    /// 60_000, which matches libnfs's implicit 60 s. Timed-out RPCs
    /// surface as `"Command timed out"` and classify as
    /// retryable/worker-local, never shard corruption.
    #[serde(default = "default_rpc_timeout_ms")]
    pub rpc_timeout_ms: u32,
    /// Compatibility-reserved; does not tune the current sync or bucketed
    /// libnfs implementation.
    #[serde(default = "default_pipeline_depth")]
    pub pipeline_depth: u32,
    /// Compatibility-reserved for a possible future io_uring design; no
    /// io_uring implementation is present.
    #[serde(default = "default_io_uring_qd")]
    pub io_uring_queue_depth: u32,
    /// Compatibility-reserved for a possible future fixed-buffer design.
    #[serde(default = "default_fixed_buf_count")]
    pub fixed_buffer_count: u32,
    /// Compatibility-reserved for a possible future fixed-buffer design.
    #[serde(default = "default_fixed_buf_size")]
    pub fixed_buffer_size: String,
    /// When true, route regular-file copies through the bucketed
    /// async libnfs pool ([`migration_mover::AsyncBucketedFileMover`]) instead of the
    /// sync `MultiPool`. Non-regular rows (symlinks / hardlinks /
    /// dirs / empty / skip) still use the sync path. CLI override
    /// via `vamoose worker --use-bucketed-pool`. Off by default.
    #[serde(default)]
    pub use_bucketed_pool: bool,
    /// When true, regular-file copies use the raw NFSv3 filehandle
    /// path (cached parent-dir filehandles, attrs at CREATE, one
    /// SETATTR, RENAME by dir fh, READDIRPLUS child-FH prefetch) — ~5
    /// RPCs per small file instead of the path-based API's
    /// per-component LOOKUP storm. See
    /// `migration_mover::libnfs::raw`. Off by default.
    #[serde(default)]
    pub use_raw_fh: bool,
    /// Raw-FH path only (no effect unless `use_raw_fh` is set): CREATE
    /// destination files under their final name and skip the
    /// `.partial` + RENAME publish — 4 RPCs per typical small file
    /// instead of 5. Trades atomic publish for throughput: a crash can leave a
    /// torn file visible at the final path; a re-run heals it (CREATE
    /// is UNCHECKED with size=0, so it truncates). Use only when
    /// nothing consumes the destination namespace mid-migration. Off
    /// by default.
    #[serde(default)]
    pub direct_commit: bool,
}
fn default_strategy() -> String {
    "libnfs_io_uring".into()
}
fn default_nfs_connections() -> u32 {
    16
}
fn default_rpc_timeout_ms() -> u32 {
    migration_mover::DEFAULT_RPC_TIMEOUT_MS
}
fn default_pipeline_depth() -> u32 {
    8
}
fn default_io_uring_qd() -> u32 {
    256
}
fn default_fixed_buf_count() -> u32 {
    256
}
fn default_fixed_buf_size() -> String {
    "1 MiB".into()
}

#[derive(Debug, Deserialize)]
pub struct BatchCfg {
    #[serde(default = "default_bytes_budget")]
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
impl Default for BatchCfg {
    fn default() -> Self {
        Self {
            bytes_budget: default_bytes_budget(),
            files_budget: default_files_budget(),
            inflight_small: default_inflight_small(),
            inflight_medium: default_inflight_medium(),
            inflight_large: default_inflight_large(),
            large_stripe_size: default_large_stripe_size(),
            large_stripe_depth: default_large_stripe_depth(),
        }
    }
}
fn default_bytes_budget() -> String {
    "8 GiB".into()
}
fn default_files_budget() -> u64 {
    100_000
}
fn default_inflight_small() -> usize {
    256
}
fn default_inflight_medium() -> usize {
    16
}
fn default_inflight_large() -> usize {
    4
}
fn default_large_stripe_size() -> String {
    "4 MiB".into()
}
fn default_large_stripe_depth() -> usize {
    32
}

#[derive(Debug, Deserialize)]
pub struct CopyCfg {
    #[serde(default = "t")]
    pub preserve_owner: bool,
    #[serde(default = "t")]
    pub preserve_mode: bool,
    #[serde(default = "t")]
    pub preserve_times: bool,
    #[serde(default = "t")]
    pub preserve_xattr: bool,
    /// Compatibility-reserved NFSv4.2 policy. The NFSv3 mover never selects
    /// server-side COPY.
    #[serde(default = "default_ssc")]
    pub server_side_copy: String,
    /// True (default): refuse to start if `preserve_owner` is on but
    /// the worker doesn't hold `CAP_CHOWN`. False: downgrade — log a
    /// startup WARN and treat per-file `chown` EPERM as a non-fatal
    /// warning recorded to `failures/`.
    #[serde(default = "t")]
    pub require_chown_capability: bool,
    /// True: verify bytes-written equals the row's `size` after each
    /// file copy and fail the row with `SIZE_CHANGED` on mismatch.
    /// Default false per SCHEMA_CONTRACT.md "Size semantics" — source
    /// truth wins.
    #[serde(default = "default_false")]
    pub require_unchanged_size: bool,
}
impl Default for CopyCfg {
    fn default() -> Self {
        Self {
            preserve_owner: true,
            preserve_mode: true,
            preserve_times: true,
            preserve_xattr: true,
            server_side_copy: default_ssc(),
            require_chown_capability: true,
            require_unchanged_size: false,
        }
    }
}
fn t() -> bool {
    true
}
fn default_false() -> bool {
    false
}
fn default_ssc() -> String {
    "off".into()
}

// `failure_pct_window_sec` is the window for the future sliding
// failure-rate gate (M3 evaluates per-shard, not over a window);
// schema is published in examples/worker.toml. Silence dead_code.
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct BackpressureCfg {
    #[serde(default = "default_failure_window")]
    pub failure_pct_window_sec: u64,
    #[serde(default = "default_failure_threshold")]
    pub failure_pct_threshold: f32,
    #[serde(default = "default_throughput_floor")]
    pub throughput_floor_mb_s: u64,
}
impl Default for BackpressureCfg {
    fn default() -> Self {
        Self {
            failure_pct_window_sec: default_failure_window(),
            failure_pct_threshold: default_failure_threshold(),
            throughput_floor_mb_s: default_throughput_floor(),
        }
    }
}
fn default_failure_window() -> u64 {
    60
}
fn default_failure_threshold() -> f32 {
    5.0
}
/// Off by default. The floor is a MB/s number, so any tree of small
/// files trips it on a perfectly healthy host: a lab tree averaging
/// 4.5 KB per file copies at ~2K files/s ≈ 9 MB/s, and every worker
/// went `throughput_low`, slept the 5-minute probe cooldown, "failed"
/// the probe shard, and doubled the cooldown to 30 minutes while
/// nothing was wrong. Set it deliberately when the workload's
/// per-file size makes a MB/s floor meaningful.
fn default_throughput_floor() -> u64 {
    0
}

/// Coord wiring. Optional — when `[coord]` is omitted from the TOML
/// the worker runs in legacy S3-only mode (existing behavior, no HTTP
/// traffic to a coord). When present, the worker registers with the
/// coord on startup and consults the heartbeat response's control
/// envelope to decide whether to keep claiming new shards.
#[derive(Debug, Deserialize, Clone)]
pub struct CoordCfg {
    /// Coord base URL, e.g. `https://coord.example:8443`.
    pub url: String,
    /// Job the worker is associated with. Defaults to the run ID in
    /// the bucket's `manifest.json`, which is also what `vamoose
    /// coord` seeds from that manifest; set it only to join a job
    /// that was seeded under a different id. The coord requires the
    /// job to exist; registration keeps retrying until it does.
    #[serde(default)]
    pub job_id: Option<String>,
    /// Env var holding the worker cluster secret. The variable's
    /// value is sent as `X-Cluster-Secret` on every request. When
    /// None, no secret header is set — matches coord dev mode.
    #[serde(default)]
    pub cluster_secret_env: Option<String>,
    /// Heartbeat cadence (seconds). Coord-driven pause/resume
    /// commands are observed within one tick.
    #[serde(default = "default_coord_heartbeat_sec")]
    pub heartbeat_sec: u64,
    /// Event-batch flush cadence (seconds). The driver pulls events
    /// from the in-process channel and POSTs them once per tick.
    #[serde(default = "default_coord_events_flush_sec")]
    pub events_flush_sec: u64,
    /// Cap on the outbound event buffer in serialized bytes. On
    /// overflow oldest events are dropped (with a counter); losing
    /// tail telemetry is preferable to wedging the copy loop.
    #[serde(default = "default_coord_buffer_max_bytes")]
    pub buffer_max_bytes: u64,
    /// TLS certificate verification. Default true. Set false for
    /// lab/dev with self-signed coord certs.
    #[serde(default = "t")]
    pub verify_tls: bool,
    /// Per-request HTTP timeout (seconds). The driver retries on
    /// timeout (treated as a transport error).
    #[serde(default = "default_coord_request_timeout_sec")]
    pub request_timeout_sec: u64,
}

fn default_coord_heartbeat_sec() -> u64 {
    5
}
fn default_coord_events_flush_sec() -> u64 {
    1
}
fn default_coord_buffer_max_bytes() -> u64 {
    64 * 1024 * 1024
}
fn default_coord_request_timeout_sec() -> u64 {
    10
}

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
        let path =
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/worker.toml");
        let s = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        let cfg: Config =
            toml::from_str(&s).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()));
        // Just spot-check fields that come from the new [run] block.
        assert_eq!(cfg.run.bucket, "vamoose");
        assert_eq!(cfg.run.endpoint, "https://s3.example.com");
        // The shipped example must not pin a site profile or disable
        // TLS verification: the default credential chain and verified
        // TLS are the production defaults.
        assert_eq!(cfg.run.profile, None);
        assert!(cfg.run.verify_tls);
        assert_eq!(cfg.mover.strategy_default, "libnfs_io_uring");
        assert_eq!(cfg.mover.pipeline_depth, 8);
        assert_eq!(cfg.mover.io_uring_queue_depth, 256);
        assert_eq!(cfg.mover.fixed_buffer_count, 256);
        assert_eq!(cfg.mover.fixed_buffer_size, "1 MiB");
        assert_eq!(cfg.copy.server_side_copy, "off");
        // The example does not declare [coord] today, so the optional
        // field defaults to None. Existing operator configs must
        // continue to parse without edits.
        assert!(cfg.coord.is_none());
    }

    #[test]
    fn canonical_config_with_cli_only_sections_still_parses_for_mig_worker() {
        let path =
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/worker.toml");
        let mut text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        text.push_str(
            r#"

                [nfs]
                src_url = "nfs://src/export"
                dst_url = "nfs://dst/export"
                src_mount = "/mnt/src"
                dst_mount = "/mnt/dst"
                src_root = "/source"
                dst_root = "/destination"

                [walker]
                threads = 8

                [aggr]
                refresh_interval_sec = 3

                [logging]
                path = "/tmp/vamoose.log"
                s3_upload = false
            "#,
        );

        let cfg: Config = toml::from_str(&text)
            .expect("mig-worker must continue ignoring unified-CLI-only tables");
        assert_eq!(cfg.run.bucket, "vamoose");
        assert_eq!(cfg.mover.rpc_timeout_ms, 60_000);
    }

    /// F12: `[mover] rpc_timeout_ms` defaults to 60_000 ms when the
    /// key is omitted — existing operator TOMLs upgrade to an
    /// explicit-and-configurable version of the timeout libnfs was
    /// already applying implicitly.
    #[test]
    fn mover_cfg_rpc_timeout_defaults_to_60000() {
        let toml_str = r#"
            src_url = "nfs://src-server/source-export"
            dst_url = "nfs://dst-server/dest-export"
        "#;
        let m: MoverCfg = toml::from_str(toml_str).unwrap();
        assert_eq!(
            m.rpc_timeout_ms, 60_000,
            "rpc_timeout_ms must default to libnfs's implicit 60s",
        );
    }

    /// F12: explicit values parse; `0` is the documented "leave the
    /// library default untouched — never call nfs_set_timeout" value.
    #[test]
    fn mover_cfg_rpc_timeout_parses_override_and_zero() {
        let m: MoverCfg = toml::from_str(
            r#"
            src_url        = "nfs://src/export"
            dst_url        = "nfs://dst/export"
            rpc_timeout_ms = 5000
        "#,
        )
        .unwrap();
        assert_eq!(m.rpc_timeout_ms, 5_000);

        let m: MoverCfg = toml::from_str(
            r#"
            src_url        = "nfs://src/export"
            dst_url        = "nfs://dst/export"
            rpc_timeout_ms = 0
        "#,
        )
        .unwrap();
        assert_eq!(m.rpc_timeout_ms, 0, "0 = leave libnfs default");
    }

    /// `direct_commit` drops the `.partial` + RENAME atomic publish,
    /// so it must be a deliberate opt-in: absent key parses to false,
    /// and existing operator TOMLs keep the safe behavior unedited.
    #[test]
    fn mover_cfg_direct_commit_defaults_off_and_parses() {
        let m: MoverCfg = toml::from_str(
            r#"
            src_url = "nfs://src/export"
            dst_url = "nfs://dst/export"
        "#,
        )
        .unwrap();
        assert!(!m.direct_commit, "direct_commit must default to false");

        let m: MoverCfg = toml::from_str(
            r#"
            src_url       = "nfs://src/export"
            dst_url       = "nfs://dst/export"
            use_raw_fh    = true
            direct_commit = true
        "#,
        )
        .unwrap();
        assert!(m.use_raw_fh);
        assert!(m.direct_commit);
    }

    #[test]
    fn coord_cfg_defaults_when_only_required_fields_set() {
        let toml_str = r#"
            url    = "https://coord.example:8443"
            job_id = "bobby-mig"
        "#;
        let c: CoordCfg = toml::from_str(toml_str).unwrap();
        assert_eq!(c.url, "https://coord.example:8443");
        assert_eq!(c.job_id.as_deref(), Some("bobby-mig"));
        assert_eq!(c.cluster_secret_env, None);
        assert_eq!(c.heartbeat_sec, 5);
        assert_eq!(c.events_flush_sec, 1);
        assert_eq!(c.buffer_max_bytes, 64 * 1024 * 1024);
        assert!(c.verify_tls);
        assert_eq!(c.request_timeout_sec, 10);
    }

    #[test]
    fn coord_cfg_picks_up_overrides() {
        let toml_str = r#"
            url                  = "https://coord:8443"
            job_id               = "bobby-mig"
            cluster_secret_env   = "VAMOOSE_CLUSTER_SECRET"
            heartbeat_sec        = 30
            events_flush_sec     = 2
            buffer_max_bytes     = 16384
            verify_tls           = false
            request_timeout_sec  = 5
        "#;
        let c: CoordCfg = toml::from_str(toml_str).unwrap();
        assert_eq!(c.heartbeat_sec, 30);
        assert_eq!(c.events_flush_sec, 2);
        assert_eq!(c.buffer_max_bytes, 16_384);
        assert!(!c.verify_tls);
        assert_eq!(
            c.cluster_secret_env.as_deref(),
            Some("VAMOOSE_CLUSTER_SECRET")
        );
        assert_eq!(c.request_timeout_sec, 5);
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
        assert_eq!(run.endpoint, "https://main.selab-var204.selab.vastdata.com",);
        assert_eq!(run.region, "us-east-1");
        assert_eq!(run.profile.as_deref(), Some("var204"));
        assert!(!run.verify_tls);
    }
}
