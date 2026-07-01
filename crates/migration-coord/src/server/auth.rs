//! Auth middleware — admin bearer for humans, cluster secret for
//! workers.
//!
//! The coord supports two authentication models in one process:
//!
//! - **Admin bearer** — every human-facing endpoint requires
//!   `Authorization: Bearer <token>`. Multiple tokens are allowed,
//!   each with an optional operator-meaningful label; the label
//!   travels into the audit log. The token file format is
//!   `<token>\t<label>` per line (label optional). Comments
//!   (`#` prefix) and blank lines are skipped.
//!
//! - **Cluster secret** — every worker-facing endpoint requires
//!   `X-Cluster-Secret: <secret>`. One shared secret per
//!   deployment, env-loaded.
//!
//! `/healthz` is exempt and reachable without any credential — the
//! `/healthz` route is mounted *outside* the auth layer.
//!
//! ## Dev mode
//!
//! If the [`AuthConfig`] has no admin tokens AND no cluster
//! secret, the middleware logs a warning at startup and allows
//! every request through. The audit log records the token label
//! `"dev-mode"` so post-merge inspection can flag the requests.
//!
//! ## Where the label goes
//!
//! Successful admin auth inserts an [`AdminLabel`] into
//! `request.extensions()`. Command handlers extract it via
//! `Extension<AdminLabel>` and pass it to
//! [`crate::runtime::CoordRuntime::record_audit`]. Read handlers
//! ignore the label — they neither audit nor write events.

use super::{ApiError, AppState};
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;
use std::collections::HashMap;

const DEV_MODE_LABEL: &str = "dev-mode";

/// Admin-token label carried in request extensions after successful
/// auth. The audit log records this value verbatim.
#[derive(Debug, Clone)]
pub struct AdminLabel(pub String);

impl AdminLabel {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Configuration consumed at startup. Cheap to clone (small map +
/// optional secret).
#[derive(Debug, Clone, Default)]
pub struct AuthConfig {
    /// token → label. Multiple tokens allowed; label may be empty
    /// (then the audit log records the empty string).
    pub admin_tokens: HashMap<String, String>,
    pub cluster_secret: Option<String>,
}

impl AuthConfig {
    /// True if neither admin tokens nor cluster secret are
    /// configured — the dev-mode pass-through.
    pub fn is_dev_mode(&self) -> bool {
        self.admin_tokens.is_empty() && self.cluster_secret.is_none()
    }

    /// Parse an admin-tokens file. Lines are `<token>\t<label>` (or
    /// just `<token>`); `#` and blank lines are skipped. Returns
    /// the resulting `HashMap<token, label>`.
    pub fn parse_admin_tokens_file(body: &str) -> Result<HashMap<String, String>, AuthConfigError> {
        let mut out = HashMap::new();
        for (lineno, raw) in body.lines().enumerate() {
            // Strip line endings only — `body.lines()` already drops
            // the newline but defend against embedded `\r`.
            let line = raw.trim_end_matches('\r');
            // Blank lines and `#` comments — recognized only when
            // the whole line is whitespace or starts with `#` after
            // trimming. Critically, we do NOT trim before splitting
            // on `\t` below; a line that's just a tab is an empty
            // token, not a blank line.
            let view = line.trim_start();
            if line.trim().is_empty() || view.starts_with('#') {
                continue;
            }
            let mut parts = line.splitn(2, '\t');
            let token = parts.next().unwrap_or("");
            let label = parts.next().unwrap_or("");
            if token.is_empty() {
                return Err(AuthConfigError::EmptyToken { line: lineno + 1 });
            }
            if out.contains_key(token) {
                return Err(AuthConfigError::DuplicateToken { line: lineno + 1 });
            }
            out.insert(token.to_string(), label.to_string());
        }
        Ok(out)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AuthConfigError {
    #[error("admin-tokens file: empty token on line {line}")]
    EmptyToken { line: usize },
    #[error("admin-tokens file: duplicate token on line {line}")]
    DuplicateToken { line: usize },
}

// =============================================================================
// Middleware
// =============================================================================

/// Require a valid admin bearer token. On success, inserts the
/// matched [`AdminLabel`] into request extensions for downstream
/// handlers.
///
/// Dev-mode pass-through: when [`AuthConfig::is_dev_mode`] is true,
/// the middleware accepts every request and stamps the
/// [`AdminLabel`] with `"dev-mode"`.
pub async fn require_admin(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Result<Response, ApiError> {
    if state.auth.is_dev_mode() {
        req.extensions_mut()
            .insert(AdminLabel(DEV_MODE_LABEL.to_string()));
        return Ok(next.run(req).await);
    }

    let token = bearer_token(&req).ok_or_else(|| {
        ApiError::unauthorized("missing_token", "Authorization: Bearer <token> required")
    })?;
    let label = state
        .auth
        .admin_tokens
        .get(token)
        .ok_or_else(|| ApiError::unauthorized("invalid_token", "unknown admin token"))?
        .clone();
    req.extensions_mut().insert(AdminLabel(label));
    Ok(next.run(req).await)
}

/// Require the cluster shared secret on `X-Cluster-Secret`. Dev
/// mode passes through (logged at startup).
pub async fn require_cluster_secret(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Result<Response, ApiError> {
    if state.auth.is_dev_mode() {
        return Ok(next.run(req).await);
    }
    let expected = state.auth.cluster_secret.as_deref().ok_or_else(|| {
        ApiError::unauthorized(
            "cluster_secret_unconfigured",
            "cluster secret required but not configured",
        )
    })?;
    let header = req
        .headers()
        .get("x-cluster-secret")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            ApiError::unauthorized("missing_cluster_secret", "X-Cluster-Secret header required")
        })?;
    if !constant_time_eq(header.as_bytes(), expected.as_bytes()) {
        return Err(ApiError::unauthorized(
            "invalid_cluster_secret",
            "cluster secret mismatch",
        ));
    }
    Ok(next.run(req).await)
}

fn bearer_token(req: &Request) -> Option<&str> {
    let value = req.headers().get("authorization")?.to_str().ok()?;
    value.strip_prefix("Bearer ").map(|t| t.trim())
}

/// Constant-time bytewise compare. Defends against timing attacks
/// on the cluster secret check. The cost is trivial — secrets are
/// well under a kilobyte.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut acc: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        acc |= x ^ y;
    }
    acc == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_admin_tokens_file_basic() {
        let body = "abc\toperator\n\
                    def\tblake\n\
                    \n\
                    # comment\n\
                    bare-token\n";
        let tokens = AuthConfig::parse_admin_tokens_file(body).unwrap();
        assert_eq!(tokens.get("abc").map(String::as_str), Some("operator"));
        assert_eq!(tokens.get("def").map(String::as_str), Some("blake"));
        assert_eq!(tokens.get("bare-token").map(String::as_str), Some(""));
    }

    #[test]
    fn parse_admin_tokens_file_rejects_duplicates_and_empty() {
        assert!(matches!(
            AuthConfig::parse_admin_tokens_file("abc\n\nabc\n").unwrap_err(),
            AuthConfigError::DuplicateToken { .. }
        ));
        assert!(matches!(
            AuthConfig::parse_admin_tokens_file("\tlabel\n").unwrap_err(),
            AuthConfigError::EmptyToken { .. }
        ));
    }

    #[test]
    fn dev_mode_when_both_unconfigured() {
        let cfg = AuthConfig::default();
        assert!(cfg.is_dev_mode());
    }

    #[test]
    fn not_dev_mode_with_admin_tokens() {
        let mut cfg = AuthConfig::default();
        cfg.admin_tokens.insert("t".into(), "l".into());
        assert!(!cfg.is_dev_mode());
    }

    #[test]
    fn not_dev_mode_with_cluster_secret() {
        let cfg = AuthConfig {
            cluster_secret: Some("s".into()),
            ..AuthConfig::default()
        };
        assert!(!cfg.is_dev_mode());
    }

    #[test]
    fn constant_time_eq_basic() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
        assert!(!constant_time_eq(b"abcd", b"abc"));
    }
}
