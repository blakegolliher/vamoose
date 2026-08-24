//! Best-effort loading of `/etc/vamoose/vamoose.env` for interactive
//! commands.
//!
//! The systemd units read that file through `EnvironmentFile=`; an
//! operator running `vamoose prepare` or `vamoose status` by hand should
//! not have to re-export the same variables. Only variables that are
//! not already set are added, so the environment always wins, and an
//! unreadable or absent file is silently ignored.

use std::path::Path;

/// Secrets file installed by the packages (mode 0600).
pub(crate) const SYSTEM_ENV_PATH: &str = "/etc/vamoose/vamoose.env";

/// Parse systemd `EnvironmentFile` syntax far enough for a secrets
/// file: `KEY=VALUE` per line, optional `export ` prefix, `#` comments,
/// blank lines, and optional single or double quotes around the value.
pub(crate) fn parse(body: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for raw in body.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line).trim_start();
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        let valid_key = key
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            && key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
        if !valid_key {
            continue;
        }
        let value = value.trim();
        let value = match value.chars().next() {
            Some(q @ ('"' | '\'')) if value.len() >= 2 && value.ends_with(q) => {
                &value[1..value.len() - 1]
            }
            _ => value,
        };
        out.push((key.to_string(), value.to_string()));
    }
    out
}

/// Apply `pairs` to the process environment for keys that are unset.
/// Returns the keys that were set.
pub(crate) fn apply_missing(pairs: &[(String, String)]) -> Vec<String> {
    let mut set = Vec::new();
    for (key, value) in pairs {
        if std::env::var_os(key).is_none() {
            // Single-threaded at this point (called before the runtime
            // spawns workers), which is the precondition for set_var.
            std::env::set_var(key, value);
            set.push(key.clone());
        }
    }
    set
}

pub(crate) fn load_if_present(path: &Path) {
    if let Ok(body) = std::fs::read_to_string(path) {
        apply_missing(&parse(&body));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_systemd_style_lines() {
        let body = "# secrets\n\
                    AWS_ACCESS_KEY_ID=abc\n\
                    export AWS_SECRET_ACCESS_KEY=\"s e c\"\n\
                    VAMOOSE_CLUSTER_SECRET='x'\n\
                    \n\
                    not a pair\n\
                    9BAD=1\n\
                    TRAILING = spaced \n";
        let pairs = parse(body);
        assert_eq!(
            pairs,
            vec![
                ("AWS_ACCESS_KEY_ID".to_string(), "abc".to_string()),
                ("AWS_SECRET_ACCESS_KEY".to_string(), "s e c".to_string()),
                ("VAMOOSE_CLUSTER_SECRET".to_string(), "x".to_string()),
                ("TRAILING".to_string(), "spaced".to_string()),
            ]
        );
    }

    #[test]
    fn existing_environment_wins() {
        std::env::set_var("VAMOOSE_ENVFILE_TEST_PRESET", "keep");
        let set = apply_missing(&[
            ("VAMOOSE_ENVFILE_TEST_PRESET".into(), "lose".into()),
            ("VAMOOSE_ENVFILE_TEST_NEW".into(), "new".into()),
        ]);
        assert_eq!(set, vec!["VAMOOSE_ENVFILE_TEST_NEW".to_string()]);
        assert_eq!(
            std::env::var("VAMOOSE_ENVFILE_TEST_PRESET").unwrap(),
            "keep"
        );
        assert_eq!(std::env::var("VAMOOSE_ENVFILE_TEST_NEW").unwrap(), "new");
        std::env::remove_var("VAMOOSE_ENVFILE_TEST_PRESET");
        std::env::remove_var("VAMOOSE_ENVFILE_TEST_NEW");
    }

    #[test]
    fn missing_file_is_ignored() {
        load_if_present(Path::new("/nonexistent/vamoose.env"));
    }
}
