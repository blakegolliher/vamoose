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
    //    release the lease.
    tracing::info!("coord: serving stopped, shutting runtime down");
    if let Err(e) = runtime.shutdown(ticker_cfg.history_keep).await {
        tracing::error!(error = %e, "coord: runtime shutdown failed");
    }

    // 8. Wait for the ticks task to observe the cancel and finish.
    match ticks_handle.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::error!(error = %e, "coord: ticks loop errored"),
        Err(e) => tracing::error!(error = %e, "coord: ticks join failed"),
    }

    tracing::info!("coord: clean exit");
    Ok(())
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
