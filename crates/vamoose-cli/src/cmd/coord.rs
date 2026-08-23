//! `vamoose coord` — control-plane HTTP daemon.
//!
//! Composes [`migration_coord`] into the unified CLI binary:
//!
//! 1. Builds an [`S3Client`] from the shared `vamoose.toml`
//!    (reusing the bucket / endpoint / TLS / profile knobs the
//!    worker already understands).
//! 2. Wraps it as a [`migration_coord::store::S3Store`].
//! 3. Loads admin tokens + cluster secret per CLI flags.
//! 4. Starts the [`migration_coord::runtime::CoordRuntime`] (lease
//!    acquire + replay).
//! 5. Spawns the three background tick loops (lease refresh,
//!    snapshot cadence, flush_aged).
//! 6. Binds the HTTP listener (`--no-tls` plain, otherwise rustls
//!    PEM cert/key) and serves until Ctrl-C.
//! 7. On Ctrl-C: cancels the shutdown token, drains in-flight
//!    requests, gracefully shuts down the runtime (flush_log →
//!    write_snapshot → release lease).

use crate::config::Config;
use clap::Args as ClapArgs;
use migration_coord::lease::Identity;
use migration_coord::runtime::{CoordRuntime, RuntimeConfig, SystemClock};
use migration_coord::server::auth::AuthConfig;
use migration_coord::server::{build_router, listen, AppState};
use migration_coord::store::{CoordStore, S3Store};
use migration_coord::ticks::{self, TickerConfig};
use migration_core::claim::ClaimStore as _;
use migration_core::s3::S3Client;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Bind address when neither `--listen` nor `[coord] listen` is set.
const DEFAULT_LISTEN: &str = "0.0.0.0:8443";

#[derive(ClapArgs, Debug)]
pub struct Args {
    /// Address to bind the HTTP listener to. Overrides `[coord]
    /// listen`; default 0.0.0.0:8443.
    #[arg(long)]
    pub listen: Option<SocketAddr>,

    /// TLS certificate PEM file. If omitted (and `--no-tls` is
    /// not set), the listener requires a cert; the operator
    /// either passes both `--tls-cert`/`--tls-key` or opts into
    /// `--no-tls` explicitly.
    #[arg(long)]
    pub tls_cert: Option<PathBuf>,

    /// TLS key PEM file.
    #[arg(long)]
    pub tls_key: Option<PathBuf>,

    /// Run without TLS — plain HTTP. Intended for local
    /// development or a reverse-proxy-terminated deployment.
    #[arg(long)]
    pub no_tls: bool,

    /// Admin tokens file. Format: `<token>\t<label>` per line,
    /// label optional. Lines beginning with `#` are comments. If
    /// the file is absent or empty AND `--cluster-secret-env` is
    /// also absent, the coord runs in dev mode (no auth).
    #[arg(long)]
    pub admin_tokens: Option<PathBuf>,

    /// Environment variable holding the worker cluster secret. The
    /// variable's value is read at startup; the workers send it via
    /// `X-Cluster-Secret`.
    #[arg(long)]
    pub cluster_secret_env: Option<String>,

    /// DANGEROUS: permit dev mode (no auth at all) on a non-loopback
    /// bind. Default is to refuse startup — an unauthenticated coord
    /// on `0.0.0.0` accepts job control from anyone who can reach the
    /// port. Intended only for isolated lab networks.
    #[arg(long)]
    pub allow_unauthenticated_nonloopback: bool,

    /// Seed a job into the registry at startup if it does not already
    /// exist (idempotent across restarts — replay wins when the job is
    /// already in the log). Overrides `[coord] job_id`. When neither is
    /// set, the coord waits for the bucket's manifest.json and seeds
    /// the job under the manifest's run id — the same id workers
    /// default to. There is deliberately no job-create HTTP route.
    #[arg(long)]
    pub seed_job: Option<String>,

    /// Human-readable name for `--seed-job` (defaults to the job id).
    #[arg(long)]
    pub seed_job_name: Option<String>,

    /// `source` field recorded on the seeded job (display only).
    #[arg(long, default_value = "nfs://unspecified")]
    pub seed_source: String,

    /// `dest` field recorded on the seeded job (display only).
    #[arg(long, default_value = "nfs://unspecified")]
    pub seed_dest: String,

    /// Planned total files for the seeded job (drives percent/ETA in
    /// the TUI; 0 = unknown). Typically the manifest's `total_rows`.
    #[arg(long, default_value_t = 0)]
    pub seed_total_files: u64,

    /// Planned total bytes for the seeded job (0 = unknown).
    #[arg(long, default_value_t = 0)]
    pub seed_total_bytes: u64,
}

/// The coordinator's effective settings: CLI flags win, then the
/// `[coord]` table of the configuration file, then defaults.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Effective {
    listen: SocketAddr,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
    no_tls: bool,
    admin_tokens: Option<PathBuf>,
    cluster_secret_env: Option<String>,
    allow_unauthenticated_nonloopback: bool,
    /// Explicit job id (`--seed-job` or `[coord] job_id`); `None`
    /// means "follow the manifest".
    seed_job: Option<String>,
}

fn effective_settings(
    args: &Args,
    server: Option<&crate::config::CoordServer>,
    client: Option<&migration_worker::config::CoordCfg>,
) -> anyhow::Result<Effective> {
    let listen = match (args.listen, server.and_then(|s| s.listen.as_deref())) {
        (Some(l), _) => l,
        (None, Some(text)) => text
            .parse::<SocketAddr>()
            .map_err(|e| anyhow::anyhow!("[coord] listen {text:?} is not host:port: {e}"))?,
        (None, None) => DEFAULT_LISTEN.parse().expect("static default parses"),
    };
    Ok(Effective {
        listen,
        tls_cert: args
            .tls_cert
            .clone()
            .or_else(|| server.and_then(|s| s.tls_cert.clone())),
        tls_key: args
            .tls_key
            .clone()
            .or_else(|| server.and_then(|s| s.tls_key.clone())),
        no_tls: args.no_tls || server.is_some_and(|s| s.no_tls),
        admin_tokens: args
            .admin_tokens
            .clone()
            .or_else(|| server.and_then(|s| s.admin_tokens_file.clone())),
        cluster_secret_env: args
            .cluster_secret_env
            .clone()
            .or_else(|| client.and_then(|c| c.cluster_secret_env.clone())),
        allow_unauthenticated_nonloopback: args.allow_unauthenticated_nonloopback
            || server.is_some_and(|s| s.allow_unauthenticated_nonloopback),
        seed_job: args
            .seed_job
            .clone()
            .or_else(|| client.and_then(|c| c.job_id.clone())),
    })
}

pub async fn run(args: Args, config_path: Option<PathBuf>) -> anyhow::Result<()> {
    // 1. Config + S3 client.
    let cfg = Config::load(config_path)?;
    let eff = effective_settings(&args, cfg.coord_server(), cfg.coord())?;
    let s3 = build_s3(cfg.storage()).await?;
    let store: Arc<dyn CoordStore> = Arc::new(S3Store::new(s3.clone()));

    // 2. Auth. F21: dev mode (no tokens, no cluster secret) must not
    //    silently bind a non-loopback address — refuse startup unless
    //    the operator opted in explicitly.
    let auth = build_auth(
        eff.admin_tokens.as_deref(),
        eff.cluster_secret_env.as_deref(),
    )?;
    check_dev_mode_bind(
        auth.is_dev_mode(),
        &eff.listen,
        eff.allow_unauthenticated_nonloopback,
    )?;
    if auth.is_dev_mode() {
        tracing::warn!(
            "coord running in DEV MODE — no admin tokens, no cluster secret. \
             Audit log will record token_label='dev-mode'. Do not use in production."
        );
        if !eff.listen.ip().is_loopback() {
            tracing::warn!(
                listen = %eff.listen,
                "DEV MODE on a NON-LOOPBACK bind (--allow-unauthenticated-nonloopback): \
                 anyone who can reach this port has full unauthenticated job control",
            );
        }
    }

    // 3. Identity for the lease.
    let me = Identity::fresh(
        hostname::get()
            .ok()
            .and_then(|h| h.into_string().ok())
            .unwrap_or_else(|| "unknown".to_string()),
        std::process::id(),
    );

    // 4. Bring up the runtime.
    let rt_cfg = RuntimeConfig::default_for_prod();
    tracing::info!(holder_id = %me.holder_id, listen = %eff.listen, "coord: starting runtime");
    let runtime = CoordRuntime::start(store, Arc::new(SystemClock), me, rt_cfg.clone()).await?;

    // 4b. Seed the job registry. After replay, so an existing job
    //     (from a previous coord generation's log) wins and no
    //     duplicate JobCreated is appended. An explicit id (flag or
    //     `[coord] job_id`) fails fast on a bad value; an omitted id
    //     follows the bucket's manifest, which may not exist yet, so
    //     the seeding runs in the background and the HTTP listener
    //     comes up regardless.
    let seed = SeedSpec {
        explicit_job: eff.seed_job.clone(),
        name: args.seed_job_name.clone(),
        source: args.seed_source.clone(),
        dest: args.seed_dest.clone(),
        total_files: args.seed_total_files,
        total_bytes: args.seed_total_bytes,
    };
    if let Some(job) = &seed.explicit_job {
        migration_coord::schema::JobId::new(job.clone())
            .map_err(|e| anyhow::anyhow!("seed job id {job:?}: {e}"))?;
    }

    // 5. Background ticks + signal handler + seeding.
    let shutdown = CancellationToken::new();
    listen::install_signal_handler(shutdown.clone());
    let seed_handle = tokio::spawn(seed_job_from_manifest(
        runtime.clone(),
        s3.clone(),
        seed,
        shutdown.clone(),
    ));
    let ticker_cfg = TickerConfig::default_for_prod();
    let ticks_handle = tokio::spawn(ticks::run_all(
        runtime.clone(),
        ticker_cfg.clone(),
        shutdown.clone(),
    ));

    // 6. Router + HTTP listener.
    let router = build_router(AppState::with_auth(runtime.clone(), auth));

    if eff.no_tls {
        tracing::info!(addr = %eff.listen, "coord: binding plain HTTP");
        listen::serve_plain(eff.listen, router, shutdown.clone()).await?;
    } else {
        let cert = eff.tls_cert.as_deref().ok_or_else(|| {
            anyhow::anyhow!(
                "TLS certificate required: set --tls-cert/--tls-key (or [coord] tls_cert/tls_key), \
                 or opt into plain HTTP with --no-tls / [coord] no_tls = true"
            )
        })?;
        let key = eff.tls_key.as_deref().ok_or_else(|| {
            anyhow::anyhow!("--tls-key / [coord] tls_key is required with a certificate")
        })?;
        tracing::info!(
            addr = %eff.listen,
            cert = %cert.display(),
            "coord: binding HTTPS",
        );
        listen::serve_tls(eff.listen, cert, key, router, shutdown.clone()).await?;
    }

    // 7. Graceful shutdown of the runtime — flush log, snapshot,
    //    release the lease. If the lease was lost, the runtime
    //    refuses every write and this returns Err so the process
    //    exits nonzero (deposed, not clean).
    tracing::info!("coord: serving stopped, shutting runtime down");
    let shutdown_result = finish_shutdown(&runtime, ticker_cfg.history_keep).await;

    // 8. Wait for the background tasks to observe the cancel and finish.
    if let Err(e) = seed_handle.await {
        tracing::error!(error = %e, "coord: seed task join failed");
    }
    match ticks_handle.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::error!(error = %e, "coord: ticks loop errored"),
        Err(e) => tracing::error!(error = %e, "coord: ticks join failed"),
    }

    if shutdown_result.is_ok() {
        tracing::info!("coord: clean exit");
    }
    shutdown_result
}

/// Final runtime teardown after the HTTP listener stops.
///
/// Returns `Err` when the coord is exiting because the lease was
/// lost — the caller propagates it so the process exit code
/// distinguishes "deposed by a successor" from a clean shutdown.
/// Any other shutdown failure is logged and swallowed (best-effort
/// teardown, same as before).
async fn finish_shutdown(runtime: &CoordRuntime, history_keep: usize) -> anyhow::Result<()> {
    match runtime.shutdown(history_keep).await {
        Ok(()) => Ok(()),
        Err(migration_coord::Error::LeaseLost) => {
            let buffered = runtime.buffered_event_count().await;
            tracing::error!(
                buffered_events = buffered,
                "coord: lease lost — {buffered} buffered event(s) NOT flushed and no final \
                 snapshot written. This is the safe outcome: a successor coord owns the log \
                 and replayed from the last durable state; our buffered events were never \
                 acknowledged as durable, and flushing them now would clobber the \
                 successor's chunks.",
            );
            Err(anyhow::anyhow!(
                "lease lost — exited without flushing {buffered} buffered event(s); \
                 successor coord owns the event log"
            ))
        }
        Err(e) => {
            tracing::error!(error = %e, "coord: runtime shutdown failed");
            Ok(())
        }
    }
}

async fn build_s3(storage: &crate::config::StorageSettings) -> anyhow::Result<S3Client> {
    let client = S3Client::from_config(
        &storage.endpoint,
        &storage.region,
        &storage.bucket,
        storage.profile.as_deref(),
        storage.verify_tls,
    )
    .await?;
    Ok(client)
}

/// F21 gate: refuse to start an unauthenticated (dev-mode) coord on a
/// non-loopback bind unless the operator passed the explicit escape
/// hatch. Pure over its three inputs so the policy is unit-testable
/// without binding sockets. Default DENY: with no tokens and no
/// cluster secret, the default `--listen 0.0.0.0:8443` would otherwise
/// expose full unauthenticated job control to the network.
fn check_dev_mode_bind(
    dev_mode: bool,
    listen: &SocketAddr,
    allow_unauthenticated_nonloopback: bool,
) -> anyhow::Result<()> {
    if !dev_mode || listen.ip().is_loopback() || allow_unauthenticated_nonloopback {
        return Ok(());
    }
    anyhow::bail!(
        "refusing to bind {listen} without authentication: no admin tokens and no \
         cluster secret are configured (dev mode), and {ip} is not a loopback \
         address. Configure auth via --admin-tokens and/or --cluster-secret-env, \
         bind a loopback address (e.g. --listen 127.0.0.1:8443), or — for \
         isolated lab networks only — pass --allow-unauthenticated-nonloopback.",
        ip = listen.ip(),
    )
}

fn build_auth(
    admin_tokens: Option<&std::path::Path>,
    cluster_secret_env: Option<&str>,
) -> anyhow::Result<AuthConfig> {
    let mut auth = AuthConfig::default();

    if let Some(path) = admin_tokens {
        let body = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("read admin tokens file {}: {e}", path.display()))?;
        auth.admin_tokens = AuthConfig::parse_admin_tokens_file(&body)?;
    }

    if let Some(var) = cluster_secret_env {
        let value = std::env::var(var)
            .map_err(|e| anyhow::anyhow!("read cluster secret from env ${var}: {e}"))?;
        if value.is_empty() {
            anyhow::bail!("cluster secret env ${var} is set but empty");
        }
        auth.cluster_secret = Some(value);
    }

    Ok(auth)
}

/// What to seed. `explicit_job = None` means "the manifest's run id".
#[derive(Debug, Clone)]
struct SeedSpec {
    explicit_job: Option<String>,
    name: Option<String>,
    source: String,
    dest: String,
    total_files: u64,
    total_bytes: u64,
}

/// Poll cadence while the bucket has no `manifest.json` yet.
const MANIFEST_POLL: std::time::Duration = std::time::Duration::from_secs(15);

/// Seed the control-plane job, waiting for the bucket's manifest when
/// no explicit id was given. Idempotent: an already-registered job
/// (replayed from the log, or seeded by a previous generation) is
/// left untouched. Runs until seeded or shutdown; errors are logged
/// and retried because a coord that serves the TUI is more useful than
/// one that exits over a transient S3 hiccup.
async fn seed_job_from_manifest(
    runtime: CoordRuntime,
    s3: S3Client,
    spec: SeedSpec,
    shutdown: CancellationToken,
) {
    let mut announced_wait = false;
    loop {
        let manifest = match load_manifest(&s3).await {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "coord: manifest read failed; retrying");
                None
            }
        };
        let job = match (&spec.explicit_job, &manifest) {
            (Some(id), _) => Some(id.clone()),
            (None, Some(m)) => Some(m.run_id.clone()),
            (None, None) => None,
        };
        if let Some(job) = job {
            match seed_once(&runtime, &job, &spec, manifest.as_ref()).await {
                // Seeded with manifest facts (or none are coming): done.
                Ok(()) if manifest.is_some() || spec.explicit_job.is_none() => return,
                // Explicit id seeded before the manifest exists: keep
                // polling so its totals land via JobTotalsSet later.
                Ok(()) => {
                    if !announced_wait {
                        announced_wait = true;
                        tracing::info!(
                            job,
                            "coord: job seeded without a manifest; will install totals when \
                             manifest.json appears",
                        );
                    }
                }
                Err(e) => tracing::warn!(job, error = %e, "coord: seeding failed; retrying"),
            }
        } else if !announced_wait {
            announced_wait = true;
            tracing::info!(
                bucket = %s3.bucket(),
                "coord: no manifest.json in bucket yet; will seed the job when `vamoose prepare` \
                 publishes one",
            );
        }
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(MANIFEST_POLL) => {}
        }
    }
}

async fn load_manifest(s3: &S3Client) -> anyhow::Result<Option<migration_core::records::Manifest>> {
    let Some((body, _etag)) = s3.get(migration_core::layout::MANIFEST_KEY).await? else {
        return Ok(None);
    };
    Ok(Some(serde_json::from_slice(&body)?))
}

/// The `JobCreated` event for a seed: manifest facts win over the
/// display-only CLI defaults so the TUI shows real source, destination,
/// and totals (percent / ETA) without the operator retyping them.
fn seed_event(
    job_id: migration_coord::schema::JobId,
    spec: &SeedSpec,
    manifest: Option<&migration_core::records::Manifest>,
) -> migration_coord::schema::EventKind {
    let (source, dest, total_files, total_bytes, config_hash) = match manifest {
        Some(m) => (
            format!("{}{}", m.source.url, m.source.root),
            format!("{}{}", m.dest.url, m.dest.root),
            m.total_rows,
            m.shards.iter().map(|s| s.bytes).sum(),
            format!("manifest:{}", m.run_id),
        ),
        None => (
            spec.source.clone(),
            spec.dest.clone(),
            spec.total_files,
            spec.total_bytes,
            "seeded-via-cli".to_string(),
        ),
    };
    migration_coord::schema::EventKind::JobCreated {
        name: spec
            .name
            .clone()
            .unwrap_or_else(|| job_id.as_str().to_string()),
        job_id,
        source,
        dest,
        owner: whoami_owner(),
        config_hash: migration_coord::schema::ConfigHash(config_hash),
        total_files,
        total_bytes,
    }
}

async fn seed_once(
    runtime: &CoordRuntime,
    job: &str,
    spec: &SeedSpec,
    manifest: Option<&migration_core::records::Manifest>,
) -> anyhow::Result<()> {
    let job_id = migration_coord::schema::JobId::new(job.to_string())
        .map_err(|e| anyhow::anyhow!("seed job id {job:?}: {e}"))?;
    if let Some(existing) = runtime.job_view(&job_id).await {
        // Totals learned late — the usual case when the job was seeded
        // (explicit id) before the manifest existed: install them now
        // so the TUI gains percent/ETA without recreating the job.
        let (total_files, total_bytes) = match seed_event(job_id.clone(), spec, manifest) {
            migration_coord::schema::EventKind::JobCreated {
                total_files,
                total_bytes,
                ..
            } => (total_files, total_bytes),
            _ => unreachable!("seed_event builds JobCreated"),
        };
        let known = total_files != 0 || total_bytes != 0;
        let differs = existing.progress.files_total != total_files
            || existing.progress.bytes_total != total_bytes;
        if known && differs {
            let seq = runtime
                .ingest(migration_coord::schema::EventKind::JobTotalsSet {
                    job_id: job_id.clone(),
                    total_files,
                    total_bytes,
                })
                .await?;
            tracing::info!(job = %job_id, seq, total_files, total_bytes, "installed job totals");
        } else {
            tracing::info!(job = %job_id, "seed job already present (replayed); skipping");
        }
        return Ok(());
    }
    let seq = runtime
        .ingest(seed_event(job_id.clone(), spec, manifest))
        .await?;
    tracing::info!(
        job = %job_id,
        seq,
        from_manifest = manifest.is_some(),
        "seeded job into registry",
    );
    Ok(())
}

/// Owner string for seeded jobs: the invoking user, best-effort.
fn whoami_owner() -> String {
    std::env::var("SUDO_USER")
        .or_else(|_| std::env::var("USER"))
        .unwrap_or_else(|_| "operator".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use migration_core::records::{Endpoint, EndpointKind, Manifest, MigrationOptions, ShardEntry};

    fn bare_args() -> Args {
        Args {
            listen: None,
            tls_cert: None,
            tls_key: None,
            no_tls: false,
            admin_tokens: None,
            cluster_secret_env: None,
            allow_unauthenticated_nonloopback: false,
            seed_job: None,
            seed_job_name: None,
            seed_source: "nfs://unspecified".into(),
            seed_dest: "nfs://unspecified".into(),
            seed_total_files: 0,
            seed_total_bytes: 0,
        }
    }

    fn server_cfg() -> crate::config::CoordServer {
        crate::config::CoordServer {
            listen: Some("127.0.0.1:9443".into()),
            tls_cert: Some("/etc/vamoose/coord.crt".into()),
            tls_key: Some("/etc/vamoose/coord.key".into()),
            no_tls: false,
            admin_tokens_file: Some("/etc/vamoose/admin-token".into()),
            allow_unauthenticated_nonloopback: false,
        }
    }

    fn client_cfg() -> migration_worker::config::CoordCfg {
        toml::from_str(
            r#"
            url = "http://node1:8443"
            job_id = "from-config"
            cluster_secret_env = "VAMOOSE_CLUSTER_SECRET"
            "#,
        )
        .unwrap()
    }

    /// No flags, no config: the historical defaults.
    #[test]
    fn effective_settings_defaults() {
        let eff = effective_settings(&bare_args(), None, None).unwrap();
        assert_eq!(eff.listen, DEFAULT_LISTEN.parse::<SocketAddr>().unwrap());
        assert!(!eff.no_tls);
        assert_eq!(eff.tls_cert, None);
        assert_eq!(eff.admin_tokens, None);
        assert_eq!(eff.cluster_secret_env, None);
        assert_eq!(eff.seed_job, None, "no id means follow the manifest");
    }

    /// The `[coord]` table supplies every knob when flags are absent.
    #[test]
    fn effective_settings_from_config() {
        let eff =
            effective_settings(&bare_args(), Some(&server_cfg()), Some(&client_cfg())).unwrap();
        assert_eq!(eff.listen, "127.0.0.1:9443".parse::<SocketAddr>().unwrap());
        assert_eq!(
            eff.tls_cert.as_deref(),
            Some(std::path::Path::new("/etc/vamoose/coord.crt"))
        );
        assert_eq!(
            eff.admin_tokens.as_deref(),
            Some(std::path::Path::new("/etc/vamoose/admin-token"))
        );
        assert_eq!(
            eff.cluster_secret_env.as_deref(),
            Some("VAMOOSE_CLUSTER_SECRET")
        );
        assert_eq!(eff.seed_job.as_deref(), Some("from-config"));
    }

    /// Flags override the configuration file.
    #[test]
    fn effective_settings_flags_win() {
        let mut args = bare_args();
        args.listen = Some("0.0.0.0:1".parse().unwrap());
        args.no_tls = true;
        args.seed_job = Some("from-flag".into());
        args.cluster_secret_env = Some("OTHER".into());
        let eff = effective_settings(&args, Some(&server_cfg()), Some(&client_cfg())).unwrap();
        assert_eq!(eff.listen, "0.0.0.0:1".parse::<SocketAddr>().unwrap());
        assert!(eff.no_tls);
        assert_eq!(eff.seed_job.as_deref(), Some("from-flag"));
        assert_eq!(eff.cluster_secret_env.as_deref(), Some("OTHER"));
    }

    #[test]
    fn effective_settings_rejects_bad_listen_text() {
        let mut server = server_cfg();
        server.listen = Some("not-an-address".into());
        let err = effective_settings(&bare_args(), Some(&server), None).unwrap_err();
        assert!(format!("{err:#}").contains("listen"), "{err:#}");
    }

    fn manifest() -> Manifest {
        Manifest {
            format_version: 2,
            run_id: "run-2026".into(),
            created_utc: migration_core::time::UtcTime(
                Utc.with_ymd_and_hms(2026, 8, 24, 0, 0, 0).unwrap(),
            ),
            shards: vec![
                ShardEntry {
                    key: "index/part-0000.parquet".into(),
                    rows: 10,
                    bytes: 100,
                    etag: "a".into(),
                },
                ShardEntry {
                    key: "index/part-0001.parquet".into(),
                    rows: 5,
                    bytes: 50,
                    etag: "b".into(),
                },
            ],
            total_rows: 15,
            source: Endpoint {
                kind: EndpointKind::Nfs,
                url: "nfs://src/export".into(),
                root: "/data".into(),
            },
            dest: Endpoint {
                kind: EndpointKind::Nfs,
                url: "nfs://dst/export".into(),
                root: "/data".into(),
            },
            options: MigrationOptions::default(),
        }
    }

    fn spec(explicit: Option<&str>) -> SeedSpec {
        SeedSpec {
            explicit_job: explicit.map(str::to_string),
            name: None,
            source: "nfs://cli-src".into(),
            dest: "nfs://cli-dst".into(),
            total_files: 7,
            total_bytes: 70,
        }
    }

    /// Manifest facts populate the seeded job; the CLI display
    /// defaults are only used without a manifest.
    #[test]
    fn seed_event_prefers_manifest_facts() {
        let id = JobId::new("run-2026").unwrap();
        match seed_event(id.clone(), &spec(None), Some(&manifest())) {
            EventKind::JobCreated {
                job_id,
                name,
                source,
                dest,
                total_files,
                total_bytes,
                config_hash,
                ..
            } => {
                assert_eq!(job_id, id);
                assert_eq!(name, "run-2026");
                assert_eq!(source, "nfs://src/export/data");
                assert_eq!(dest, "nfs://dst/export/data");
                assert_eq!(total_files, 15);
                assert_eq!(total_bytes, 150);
                assert_eq!(config_hash.0, "manifest:run-2026");
            }
            other => panic!("unexpected {other:?}"),
        }
        match seed_event(id, &spec(Some("run-2026")), None) {
            EventKind::JobCreated {
                source,
                dest,
                total_files,
                total_bytes,
                config_hash,
                ..
            } => {
                assert_eq!(source, "nfs://cli-src");
                assert_eq!(dest, "nfs://cli-dst");
                assert_eq!(total_files, 7);
                assert_eq!(total_bytes, 70);
                assert_eq!(config_hash.0, "seeded-via-cli");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    /// An explicit id seeded before the manifest exists gains the
    /// manifest's totals later through JobTotalsSet, without a second
    /// JobCreated.
    #[tokio::test]
    async fn seed_once_installs_late_totals_from_the_manifest() {
        let (rt, _store) = fresh_runtime().await;
        let mut early = spec(Some("run-2026"));
        early.total_files = 0;
        early.total_bytes = 0;
        seed_once(&rt, "run-2026", &early, None).await.unwrap();
        let id = JobId::new("run-2026").unwrap();
        assert_eq!(rt.job_view(&id).await.unwrap().progress.files_total, 0);
        let m = manifest();
        seed_once(&rt, "run-2026", &early, Some(&m)).await.unwrap();
        let job = rt.job_view(&id).await.unwrap();
        assert_eq!(job.progress.files_total, 15);
        assert_eq!(job.progress.bytes_total, 150);
        let seq = rt.last_seq().await;
        seed_once(&rt, "run-2026", &early, Some(&m)).await.unwrap();
        assert_eq!(rt.last_seq().await, seq, "matching totals append nothing");
    }

    /// Seeding is idempotent and takes its id from the manifest when
    /// no explicit id is configured.
    #[tokio::test]
    async fn seed_once_uses_manifest_run_id_and_is_idempotent() {
        let (rt, _store) = fresh_runtime().await;
        let m = manifest();
        seed_once(&rt, &m.run_id, &spec(None), Some(&m))
            .await
            .unwrap();
        let job = rt.job_view(&JobId::new("run-2026").unwrap()).await.unwrap();
        assert_eq!(job.progress.files_total, 15);
        let seq_after_first = rt.last_seq().await;
        seed_once(&rt, &m.run_id, &spec(None), Some(&m))
            .await
            .unwrap();
        assert_eq!(
            rt.last_seq().await,
            seq_after_first,
            "second seed must append nothing"
        );
    }
    use migration_coord::runtime::test_clock::FixedClock;
    use migration_coord::schema::{ConfigHash, EventKind, JobId};
    use migration_coord::store::MemStore;

    async fn fresh_runtime() -> (CoordRuntime, Arc<MemStore>) {
        let mem = Arc::new(MemStore::new());
        let store: Arc<dyn CoordStore> = mem.clone();
        let clock = FixedClock::new(Utc.with_ymd_and_hms(2026, 7, 3, 12, 0, 0).unwrap());
        let me = Identity::fresh("test-host".to_string(), 42);
        let rt = CoordRuntime::start(store, clock, me, RuntimeConfig::default_for_prod())
            .await
            .unwrap();
        (rt, mem)
    }

    fn job_created() -> EventKind {
        EventKind::JobCreated {
            job_id: JobId::new("bobby").unwrap(),
            name: "bobby-migration".into(),
            source: "nfs://src".into(),
            dest: "nfs://dst".into(),
            owner: "test".into(),
            config_hash: ConfigHash("ab".into()),
            total_files: 0,
            total_bytes: 0,
        }
    }

    /// F02 acceptance test 6: exiting because the lease was lost
    /// must be distinguishable from a clean shutdown — the seam
    /// `run()` maps to the process exit code returns `Err`.
    #[tokio::test]
    async fn cmd_coord_lease_lost_exit_is_nonzero() {
        let (rt, store) = fresh_runtime().await;
        rt.ingest(job_created()).await.unwrap();
        rt.mark_lease_lost().await;

        let writes_before = store.write_count();
        let res = finish_shutdown(&rt, 3).await;
        assert!(
            res.is_err(),
            "lease-lost shutdown must map to a nonzero exit, got {res:?}",
        );
        assert_eq!(
            store.write_count(),
            writes_before,
            "lease-lost shutdown must not write to the store",
        );
    }

    /// Regression guard: a clean shutdown still exits zero.
    #[tokio::test]
    async fn cmd_coord_clean_shutdown_exit_is_ok() {
        let (rt, _store) = fresh_runtime().await;
        rt.ingest(job_created()).await.unwrap();
        let res = finish_shutdown(&rt, 3).await;
        assert!(res.is_ok(), "clean shutdown must exit zero, got {res:?}");
    }

    // ------------------------------------------------------------------
    // F21: dev mode (no auth) must not bind non-loopback by default
    // ------------------------------------------------------------------

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    /// Dev mode + a non-loopback bind is an open unauthenticated
    /// control plane. Startup must refuse, and the error must name
    /// the ways out: configure auth (`--admin-tokens` /
    /// `--cluster-secret-env`) or opt in explicitly
    /// (`--allow-unauthenticated-nonloopback`).
    #[test]
    fn dev_mode_nonloopback_refused() {
        for bind in ["0.0.0.0:8443", "[::]:8443", "10.1.2.3:8443"] {
            let err = check_dev_mode_bind(true, &addr(bind), false)
                .expect_err(&format!("dev mode on {bind} must be refused"));
            let msg = format!("{err:#}");
            for flag in [
                "--admin-tokens",
                "--cluster-secret-env",
                "--allow-unauthenticated-nonloopback",
            ] {
                assert!(msg.contains(flag), "error must name {flag}, got: {msg}");
            }
        }
    }

    /// Loopback binds stay fine in dev mode — that is what dev mode
    /// is for (the caller logs the existing DEV MODE warning).
    #[test]
    fn dev_mode_loopback_ok() {
        for bind in ["127.0.0.1:8443", "[::1]:8443"] {
            check_dev_mode_bind(true, &addr(bind), false)
                .unwrap_or_else(|e| panic!("loopback {bind} must be allowed in dev mode: {e:#}"));
        }
    }

    /// With auth configured, any bind address is acceptable.
    #[test]
    fn authed_any_bind_ok() {
        for bind in ["0.0.0.0:8443", "[::]:8443", "10.1.2.3:8443", "127.0.0.1:1"] {
            check_dev_mode_bind(false, &addr(bind), false)
                .unwrap_or_else(|e| panic!("authed bind {bind} must be allowed: {e:#}"));
        }
    }

    /// The explicit escape hatch overrides the refusal (lab flows);
    /// it changes nothing when auth is configured or the bind is
    /// loopback.
    #[test]
    fn escape_hatch_allows_dev_mode_nonloopback() {
        check_dev_mode_bind(true, &addr("0.0.0.0:8443"), true)
            .expect("--allow-unauthenticated-nonloopback must permit the bind");
    }
}
