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
}

pub async fn run(args: Args, config_path: Option<PathBuf>) -> anyhow::Result<()> {
    // 1. Config + S3 client.
    let cfg = Config::load(config_path)?;
    let s3 = build_s3(&cfg).await?;
    let store: Arc<dyn CoordStore> = Arc::new(S3Store::new(s3));

    // 2. Auth.
    let auth = build_auth(&args)?;
    if auth.is_dev_mode() {
        tracing::warn!(
            "coord running in DEV MODE — no admin tokens, no cluster secret. \
             Audit log will record token_label='dev-mode'. Do not use in production."
        );
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

async fn build_s3(cfg: &Config) -> anyhow::Result<S3Client> {
    let verify_tls = !cfg.s3.no_verify_ssl.unwrap_or(false);
    let client = S3Client::from_config(
        &cfg.s3.endpoint,
        &cfg.s3.region,
        &cfg.global.bucket,
        cfg.s3.profile.as_deref(),
        verify_tls,
    )
    .await?;
    Ok(client)
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
}
