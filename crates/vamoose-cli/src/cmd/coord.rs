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
use migration_core::s3::S3Client;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[derive(ClapArgs, Debug)]
pub struct Args {
    /// Address to bind the HTTP listener to.
    #[arg(long, default_value = "0.0.0.0:8443")]
    pub listen: SocketAddr,

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
    /// already in the log). This is the bootstrap for a fresh coord:
    /// there is deliberately no job-create HTTP route yet, and workers'
    /// /workers/register 404s for unknown jobs. The id must match the
    /// workers' `[coord] job_id`.
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

pub async fn run(args: Args, config_path: Option<PathBuf>) -> anyhow::Result<()> {
    // 1. Config + S3 client.
    let cfg = Config::load(config_path)?;
    let s3 = build_s3(cfg.storage()).await?;
    let store: Arc<dyn CoordStore> = Arc::new(S3Store::new(s3));

    // 2. Auth. F21: dev mode (no tokens, no cluster secret) must not
    //    silently bind a non-loopback address — refuse startup unless
    //    the operator opted in explicitly.
    let auth = build_auth(&args)?;
    check_dev_mode_bind(
        auth.is_dev_mode(),
        &args.listen,
        args.allow_unauthenticated_nonloopback,
    )?;
    if auth.is_dev_mode() {
        tracing::warn!(
            "coord running in DEV MODE — no admin tokens, no cluster secret. \
             Audit log will record token_label='dev-mode'. Do not use in production."
        );
        if !args.listen.ip().is_loopback() {
            tracing::warn!(
                listen = %args.listen,
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
    tracing::info!(holder_id = %me.holder_id, listen = %args.listen, "coord: starting runtime");
    let runtime = CoordRuntime::start(store, Arc::new(SystemClock), me, rt_cfg.clone()).await?;

    // 4b. Seed the job registry if asked. After replay, so an
    //     existing job (from a previous coord generation's log) wins
    //     and no duplicate JobCreated is appended.
    if let Some(job) = &args.seed_job {
        let job_id = migration_coord::schema::JobId::new(job.clone())
            .map_err(|e| anyhow::anyhow!("--seed-job: {e}"))?;
        if runtime.job_view(&job_id).await.is_some() {
            tracing::info!(job = %job_id, "seed job already present (replayed); skipping");
        } else {
            let seq = runtime
                .ingest(migration_coord::schema::EventKind::JobCreated {
                    job_id: job_id.clone(),
                    name: args.seed_job_name.clone().unwrap_or_else(|| job.clone()),
                    source: args.seed_source.clone(),
                    dest: args.seed_dest.clone(),
                    owner: whoami_owner(),
                    config_hash: migration_coord::schema::ConfigHash("seeded-via-cli".into()),
                    total_files: args.seed_total_files,
                    total_bytes: args.seed_total_bytes,
                })
                .await?;
            tracing::info!(job = %job_id, seq, "seeded job into registry");
        }
    }

    // 5. Background ticks + signal handler.
    let shutdown = CancellationToken::new();
    listen::install_signal_handler(shutdown.clone());
    let ticker_cfg = TickerConfig::default_for_prod();
    let ticks_handle = tokio::spawn(ticks::run_all(
        runtime.clone(),
        ticker_cfg.clone(),
        shutdown.clone(),
    ));

    // 6. Router + HTTP listener.
    let router = build_router(AppState::with_auth(runtime.clone(), auth));

    if args.no_tls {
        tracing::info!(addr = %args.listen, "coord: binding plain HTTP");
        listen::serve_plain(args.listen, router, shutdown.clone()).await?;
    } else {
        let cert = args
            .tls_cert
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("--tls-cert is required unless --no-tls is set"))?;
        let key = args
            .tls_key
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("--tls-key is required unless --no-tls is set"))?;
        tracing::info!(
            addr = %args.listen,
            cert = %cert.display(),
            "coord: binding HTTPS",
        );
        listen::serve_tls(args.listen, cert, key, router, shutdown.clone()).await?;
    }

    // 7. Graceful shutdown of the runtime — flush log, snapshot,
    //    release the lease. If the lease was lost, the runtime
    //    refuses every write and this returns Err so the process
    //    exits nonzero (deposed, not clean).
    tracing::info!("coord: serving stopped, shutting runtime down");
    let shutdown_result = finish_shutdown(&runtime, ticker_cfg.history_keep).await;

    // 8. Wait for the ticks task to observe the cancel and finish.
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

fn build_auth(args: &Args) -> anyhow::Result<AuthConfig> {
    let mut auth = AuthConfig::default();

    if let Some(path) = &args.admin_tokens {
        let body = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("read admin tokens file {}: {e}", path.display()))?;
        auth.admin_tokens = AuthConfig::parse_admin_tokens_file(&body)?;
    }

    if let Some(var) = &args.cluster_secret_env {
        let value = std::env::var(var)
            .map_err(|e| anyhow::anyhow!("read cluster secret from env ${var}: {e}"))?;
        if value.is_empty() {
            anyhow::bail!("cluster secret env ${var} is set but empty");
        }
        auth.cluster_secret = Some(value);
    }

    Ok(auth)
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
