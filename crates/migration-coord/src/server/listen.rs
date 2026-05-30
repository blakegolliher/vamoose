//! HTTP(S) listen helpers + graceful shutdown.
//!
//! Two ways to bring up the coord listener:
//!
//! - [`serve_plain`] — plain HTTP. Use behind a TLS-terminating
//!   reverse proxy, or for local development.
//! - [`serve_tls`] — HTTPS via `axum-server` + rustls. Reads the
//!   cert+key PEM files at startup.
//!
//! Both honor a [`tokio_util::sync::CancellationToken`] for
//! graceful shutdown: when the token fires, the server stops
//! accepting new connections and finishes in-flight requests
//! before the helper returns.

use crate::errors::Result;
use crate::Error;
use axum::Router;
use std::net::SocketAddr;
use std::path::Path;
use tokio_util::sync::CancellationToken;

/// Bind a plain HTTP listener on `addr` and serve `router` until
/// `shutdown` is cancelled.
pub async fn serve_plain(
    addr: SocketAddr,
    router: Router,
    shutdown: CancellationToken,
) -> Result<()> {
    let handle = axum_server::Handle::new();
    let handle_clone = handle.clone();
    tokio::spawn(async move {
        shutdown.cancelled().await;
        // 10s drain window — in-flight requests get a chance to
        // finish before the server hangs up.
        handle_clone.graceful_shutdown(Some(std::time::Duration::from_secs(10)));
    });

    axum_server::bind(addr)
        .handle(handle)
        .serve(router.into_make_service())
        .await
        .map_err(|e| Error::Other(anyhow::anyhow!("axum_server bind {addr}: {e}")))?;
    Ok(())
}

/// Bind an HTTPS listener on `addr` using `cert` + `key` PEM files
/// and serve `router` until `shutdown` is cancelled.
pub async fn serve_tls(
    addr: SocketAddr,
    cert_path: &Path,
    key_path: &Path,
    router: Router,
    shutdown: CancellationToken,
) -> Result<()> {
    let tls_config = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert_path, key_path)
        .await
        .map_err(|e| {
            Error::Other(anyhow::anyhow!(
                "load TLS cert {cp:?} / key {kp:?}: {e}",
                cp = cert_path,
                kp = key_path,
            ))
        })?;

    let handle = axum_server::Handle::new();
    let handle_clone = handle.clone();
    tokio::spawn(async move {
        shutdown.cancelled().await;
        handle_clone.graceful_shutdown(Some(std::time::Duration::from_secs(10)));
    });

    axum_server::bind_rustls(addr, tls_config)
        .handle(handle)
        .serve(router.into_make_service())
        .await
        .map_err(|e| Error::Other(anyhow::anyhow!("axum_server bind_rustls {addr}: {e}")))?;
    Ok(())
}

/// Helper for the CLI: install a Ctrl-C handler that cancels the
/// given token. Idempotent — multiple invocations are fine.
pub fn install_signal_handler(shutdown: CancellationToken) {
    tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                tracing::info!("SIGINT received — initiating graceful shutdown");
                shutdown.cancel();
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to install Ctrl-C handler");
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;

    fn loopback(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    #[tokio::test]
    async fn serve_plain_listens_and_shuts_down_on_cancel() {
        let router = Router::new().route("/healthz", get(|| async { "ok" }));
        let shutdown = CancellationToken::new();
        // Port 0 → kernel assigns a free port. We don't connect in
        // this test — just verify start-and-stop completes within a
        // bounded time after cancel.
        let shutdown_clone = shutdown.clone();
        let server =
            tokio::spawn(async move { serve_plain(loopback(0), router, shutdown_clone).await });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        shutdown.cancel();
        let res = tokio::time::timeout(std::time::Duration::from_secs(15), server)
            .await
            .expect("serve_plain should return after cancel");
        res.unwrap().unwrap();
    }
}
