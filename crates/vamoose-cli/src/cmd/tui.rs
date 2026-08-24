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
    /// `http://localhost:8443` for a `--no-tls` coord. Defaults to
    /// `[coord] url` from the configuration file.
    #[arg(long, env = "VAMOOSE_COORD_URL")]
    pub coord_url: Option<String>,

    /// Admin bearer token. Read directly from this flag or its env
    /// var. Mutually exclusive with `--admin-token-file`.
    #[arg(long, env = "VAMOOSE_ADMIN_TOKEN", hide_env_values = true)]
    pub admin_token: Option<String>,

    /// Path to a file holding the admin bearer token: the first
    /// non-comment line, up to an optional TAB-separated label (the
    /// same format `vamoose coord` reads). Defaults to `[coord]
    /// admin_tokens_file` from the configuration file.
    #[arg(long, conflicts_with = "admin_token")]
    pub admin_token_file: Option<PathBuf>,

    /// Skip TLS certificate verification. Required for lab coords
    /// with self-signed certs. Also implied by `[coord] verify_tls =
    /// false` in the configuration file.
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

/// Connection settings after merging flags with the configuration
/// file's `[coord]` table (flags win).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Connection {
    coord_url: String,
    token_file: Option<PathBuf>,
    verify_tls: bool,
}

fn resolve_connection(
    args: &Args,
    client: Option<&migration_worker::config::CoordCfg>,
    server: Option<&crate::config::CoordServer>,
) -> anyhow::Result<Connection> {
    let coord_url = args
        .coord_url
        .clone()
        .or_else(|| client.map(|c| c.url.clone()))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no coordinator URL: pass --coord-url (or VAMOOSE_COORD_URL), or set [coord] url \
                 in the configuration file"
            )
        })?;
    let token_file = if args.admin_token.is_some() {
        None
    } else {
        args.admin_token_file
            .clone()
            .or_else(|| server.and_then(|s| s.admin_tokens_file.clone()))
    };
    let verify_tls = !args.insecure && client.is_none_or(|c| c.verify_tls);
    Ok(Connection {
        coord_url,
        token_file,
        verify_tls,
    })
}

pub async fn run(args: Args, config_path: Option<PathBuf>) -> anyhow::Result<()> {
    // The configuration file is optional when every setting arrives
    // by flag, so a missing file only matters if something is needed
    // from it.
    let config = match crate::config::Config::load(config_path) {
        Ok(cfg) => Some(cfg),
        Err(e) if args.coord_url.is_some() => {
            tracing::debug!(error = %e, "tui: no configuration file; using flags only");
            None
        }
        Err(e) => return Err(e),
    };
    let conn = resolve_connection(
        &args,
        config.as_ref().and_then(|c| c.coord()),
        config.as_ref().and_then(|c| c.coord_server()),
    )?;
    let token = resolve_token(args.admin_token.as_deref(), conn.token_file.as_deref())?;
    let client = Client::new(
        &conn.coord_url,
        token.as_deref(),
        conn.verify_tls,
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
            match first_token(&body) {
                Some(token) => Ok(Some(token)),
                None => anyhow::bail!(
                    "--admin-token-file {} is empty (no token line)",
                    path.display()
                ),
            }
        }
        (None, None) => Ok(None),
    }
}

/// First token in an admin-tokens file: skips blank and `#` lines, and
/// drops an optional TAB-separated label — the coord's file format —
/// so one file serves both the daemon and the TUI. A bare single-token
/// file (the old `--admin-token-file` shape) still works unchanged.
fn first_token(body: &str) -> Option<String> {
    body.lines()
        .map(|l| l.trim_end_matches('\r'))
        .filter(|l| !l.trim().is_empty() && !l.trim_start().starts_with('#'))
        .map(|l| l.split('\t').next().unwrap_or("").trim().to_string())
        .find(|t| !t.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn bare_args() -> Args {
        Args {
            coord_url: None,
            admin_token: None,
            admin_token_file: None,
            insecure: false,
            request_timeout_sec: 10,
            log_file: None,
        }
    }

    fn client_cfg(verify_tls: bool) -> migration_worker::config::CoordCfg {
        toml::from_str(&format!(
            r#"
            url = "http://node1:8443"
            verify_tls = {verify_tls}
            "#
        ))
        .unwrap()
    }

    fn server_cfg() -> crate::config::CoordServer {
        crate::config::CoordServer {
            admin_tokens_file: Some("/etc/vamoose/admin-token".into()),
            ..Default::default()
        }
    }

    #[test]
    fn connection_comes_from_config_when_flags_absent() {
        let conn = resolve_connection(&bare_args(), Some(&client_cfg(false)), Some(&server_cfg()))
            .unwrap();
        assert_eq!(conn.coord_url, "http://node1:8443");
        assert_eq!(
            conn.token_file.as_deref(),
            Some(std::path::Path::new("/etc/vamoose/admin-token"))
        );
        assert!(
            !conn.verify_tls,
            "[coord] verify_tls = false implies --insecure"
        );
    }

    #[test]
    fn connection_flags_win_and_inline_token_suppresses_file() {
        let mut args = bare_args();
        args.coord_url = Some("https://other:8443".into());
        args.admin_token = Some("abc".into());
        let conn = resolve_connection(&args, Some(&client_cfg(true)), Some(&server_cfg())).unwrap();
        assert_eq!(conn.coord_url, "https://other:8443");
        assert_eq!(conn.token_file, None);
        assert!(conn.verify_tls);
    }

    #[test]
    fn connection_without_any_url_is_an_error() {
        let err = resolve_connection(&bare_args(), None, None).unwrap_err();
        assert!(format!("{err:#}").contains("--coord-url"), "{err:#}");
    }

    #[test]
    fn first_token_accepts_coord_file_format_and_bare_token() {
        assert_eq!(first_token("abc\n"), Some("abc".into()));
        assert_eq!(first_token("  abc  \n"), Some("abc".into()));
        assert_eq!(
            first_token("# operators\n\nsecret-1\tblake\nsecret-2\tother\n"),
            Some("secret-1".into())
        );
        assert_eq!(first_token("\n# only comments\n"), None);
        assert_eq!(first_token("\tlabel-without-token\n"), None);
    }

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
