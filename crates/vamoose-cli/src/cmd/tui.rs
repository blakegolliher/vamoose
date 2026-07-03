//! `vamoose tui` — operator dashboard.
//!
//! Connects to a running `vamoose coord` over HTTP+SSE and renders
//! a live jobs view to the terminal. See `crates/migration-tui` for
//! the rendering layer; this module is just the CLI surface.

use clap::Args as ClapArgs;
use migration_tui::app::{self, RunOpts};
use migration_tui::client::Client;
use std::path::PathBuf;
use std::time::Duration;

#[derive(ClapArgs, Debug)]
pub struct Args {
    /// Coord base URL — e.g. `https://coord.example:8443` or
    /// `http://localhost:8443` for a `--no-tls` coord.
    #[arg(long, env = "VAMOOSE_COORD_URL")]
    pub coord_url: String,

    /// Admin bearer token. Read directly from this flag or its env
    /// var. Mutually exclusive with `--admin-token-file`.
    #[arg(long, env = "VAMOOSE_ADMIN_TOKEN", hide_env_values = true)]
    pub admin_token: Option<String>,

    /// Path to a file whose contents are the admin bearer token.
    /// Trailing whitespace is trimmed. Useful when the operator
    /// prefers not to surface the token in shell history / env.
    #[arg(long, conflicts_with = "admin_token")]
    pub admin_token_file: Option<PathBuf>,

    /// Skip TLS certificate verification. Required for lab coords
    /// with self-signed certs (matches the worker's `verify_tls`
    /// shape).
    #[arg(long)]
    pub insecure: bool,

    /// Per-request HTTP timeout, in seconds. Defaults to 10. The
    /// SSE stream itself is not bounded by this — it stays open
    /// for the lifetime of the TUI.
    #[arg(long, default_value_t = 10)]
    pub request_timeout_sec: u64,

    /// Write logs to this file (rotating). The TUI never logs to
    /// stderr — tracing-fmt output would corrupt the alternate
    /// screen (F39, COORD_PLAN §3.7) — so without this flag log
    /// events are discarded.
    #[arg(long)]
    pub log_file: Option<PathBuf>,
}

pub async fn run(args: Args, _config_path: Option<PathBuf>) -> anyhow::Result<()> {
    let token = resolve_token(
        args.admin_token.as_deref(),
        args.admin_token_file.as_deref(),
    )?;
    let verify_tls = !args.insecure;
    let client = Client::new(
        &args.coord_url,
        token.as_deref(),
        verify_tls,
        Duration::from_secs(args.request_timeout_sec),
    )?;
    app::run(client, RunOpts::default()).await?;
    Ok(())
}

fn resolve_token(
    inline: Option<&str>,
    file: Option<&std::path::Path>,
) -> anyhow::Result<Option<String>> {
    match (inline, file) {
        (Some(_), Some(_)) => {
            // clap's `conflicts_with` already enforces this; second
            // belt for callers building Args programmatically.
            anyhow::bail!("--admin-token and --admin-token-file are mutually exclusive");
        }
        (Some(t), None) => {
            let t = t.trim();
            if t.is_empty() {
                Ok(None)
            } else {
                Ok(Some(t.to_string()))
            }
        }
        (None, Some(path)) => {
            let body = std::fs::read_to_string(path)
                .map_err(|e| anyhow::anyhow!("read --admin-token-file {}: {e}", path.display()))?;
            let trimmed = body.trim();
            if trimmed.is_empty() {
                anyhow::bail!("--admin-token-file {} is empty", path.display());
            }
            Ok(Some(trimmed.to_string()))
        }
        (None, None) => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn resolve_inline_token_passes_through() {
        let t = resolve_token(Some("abc"), None).unwrap();
        assert_eq!(t.as_deref(), Some("abc"));
    }

    #[test]
    fn resolve_inline_token_trims_whitespace() {
        let t = resolve_token(Some("  abc\n"), None).unwrap();
        assert_eq!(t.as_deref(), Some("abc"));
    }

    #[test]
    fn resolve_inline_token_empty_becomes_none() {
        let t = resolve_token(Some("   "), None).unwrap();
        assert!(t.is_none());
    }

    #[test]
    fn resolve_token_file_strips_trailing_newline() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(b"my-token\n").unwrap();
        let t = resolve_token(None, Some(f.path())).unwrap();
        assert_eq!(t.as_deref(), Some("my-token"));
    }

    #[test]
    fn resolve_empty_token_file_errors() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(b"\n   \n").unwrap();
        let err = resolve_token(None, Some(f.path())).unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn resolve_both_inline_and_file_errors() {
        let err = resolve_token(
            Some("a"),
            Some(std::path::Path::new("/tmp/does-not-matter")),
        )
        .unwrap_err();
        assert!(err.to_string().contains("mutually exclusive"));
    }
}
