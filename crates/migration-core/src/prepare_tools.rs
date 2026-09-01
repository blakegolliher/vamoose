//! Locating and invoking the two external prepare stages: `nfs-walker`
//! (the scanner, shipped alongside vamoose) and `mig-walker-rewrite`
//! (the canonical-schema converter from this workspace).
//!
//! Shared by `vamoose prepare` (distributed, S3-backed) and `mongoose
//! prepare` (single-host, local-only) so the two front-ends drive the
//! same scanner and rewriter with the same argument shapes, discovery
//! rules, and version/flag checks.

use anyhow::{Context, Result};
use std::ffi::OsString;
use std::path::{Path, PathBuf};

/// Where the packages install the bundled scanner.
pub const PACKAGED_WALKER: &str = "/usr/libexec/vamoose/nfs-walker";

/// Resolve the scanner: explicit configuration, `../libexec/nfs-walker`
/// next to the running binary (release bundles under a prefix), the
/// packaged path, then `nfs-walker` on PATH.
pub fn find_walker(configured: Option<&Path>) -> Result<PathBuf> {
    if let Some(path) = configured {
        if path.is_file() {
            return Ok(path.to_path_buf());
        }
        anyhow::bail!(
            "configured nfs-walker {} is not a file (walker_bin in [prepare])",
            path.display()
        );
    }
    if let Some(prefix) = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().and_then(Path::parent).map(Path::to_path_buf))
    {
        let bundled = prefix.join("libexec").join("nfs-walker");
        if bundled.is_file() {
            return Ok(bundled);
        }
    }
    let packaged = Path::new(PACKAGED_WALKER);
    if packaged.is_file() {
        return Ok(packaged.to_path_buf());
    }
    which("nfs-walker").ok_or_else(|| {
        anyhow::anyhow!(
            "nfs-walker not found: expected {PACKAGED_WALKER} (from the vamoose package built \
             with NFS_WALKER_BIN=...) or nfs-walker on PATH, or set [prepare] walker_bin"
        )
    })
}

/// Resolve a workspace sibling executable (`mig-walker-rewrite`):
/// next to the running binary first, then PATH.
pub fn find_sibling(name: &str) -> Result<PathBuf> {
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let candidate = dir.join(name);
            if candidate.is_file() {
                return Ok(candidate);
            }
        }
    }
    which(name).ok_or_else(|| {
        anyhow::anyhow!(
            "{name} not found next to the running binary or on PATH (it ships in the same package)"
        )
    })
}

pub fn which(name: &str) -> Option<PathBuf> {
    let path_var = std::env::var_os("PATH")?;
    std::env::split_paths(&path_var)
        .map(|dir| dir.join(name))
        .find(|candidate| candidate.is_file())
}

/// The URL nfs-walker scans: the export URL with the source root
/// appended (a root of `/` scans the export itself).
pub fn scan_url(src_url: &str, source_root: &str) -> String {
    let base = src_url.trim_end_matches('/');
    let root = source_root.trim();
    if root.is_empty() || root == "/" {
        base.to_string()
    } else {
        format!("{base}/{}", root.trim_matches('/'))
    }
}

/// The `--source-root` handed to `mig-walker-rewrite`. The scan URL
/// already carries the configured source root (see [`scan_url`]), so
/// the walker emits paths relative to that root and there is nothing
/// left to strip; the copy stage re-attaches the manifest's
/// `source.root` via `join_root`. Passing the configured root here
/// instead makes the rewrite reject every row of a sub-tree run
/// ("walker path does not start with --source-root"), because the
/// walker never saw the export root.
pub const REWRITE_SOURCE_ROOT: &str = "/";

/// Everything that shapes one scan; kept as data so the argument list
/// is unit-testable and recorded in the scan checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalkerInvocation {
    pub scan_url: String,
    pub output: PathBuf,
    pub workers: usize,
    pub exclude: Vec<String>,
    pub shard_size_mb: u64,
    pub log: PathBuf,
}

impl WalkerInvocation {
    pub fn args(&self) -> Vec<OsString> {
        let mut args: Vec<OsString> = vec![
            self.scan_url.clone().into(),
            "--output".into(),
            self.output.clone().into_os_string(),
            "--workers".into(),
            self.workers.to_string().into(),
            "--parquet-file-size-mb".into(),
            self.shard_size_mb.to_string().into(),
            "--log".into(),
            self.log.clone().into_os_string(),
            "--log-fmt".into(),
            "json".into(),
        ];
        for pattern in &self.exclude {
            args.push("--exclude".into());
            args.push(pattern.clone().into());
        }
        args
    }

    /// Every long option [`WalkerInvocation::args`] can emit — what a
    /// scanner must accept for prepare to drive it.
    pub fn long_flags() -> &'static [&'static str] {
        &[
            "--output",
            "--workers",
            "--parquet-file-size-mb",
            "--log",
            "--log-fmt",
            "--exclude",
        ]
    }
}

/// Arguments for the canonical rewrite of one scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RewriteInvocation {
    pub input: PathBuf,
    pub output: PathBuf,
    pub source_root: String,
    pub walker_version: String,
    pub report: PathBuf,
}

impl RewriteInvocation {
    pub fn args(&self) -> Vec<OsString> {
        vec![
            "--input".into(),
            self.input.clone().into_os_string(),
            "--output".into(),
            self.output.clone().into_os_string(),
            "--source-root".into(),
            self.source_root.clone().into(),
            "--walker-version".into(),
            self.walker_version.clone().into(),
            "--resume".into(),
            "--report".into(),
            self.report.clone().into_os_string(),
        ]
    }
}

/// Start an external stage with inherited stdio, so its own progress
/// output reaches the operator's terminal, and hand back the child:
/// the caller polls it while relaying progress. The child is killed
/// if dropped before it exits, so an interrupted `prepare` does not
/// leave a scan or rewrite running unattended.
pub fn spawn_stage(label: &str, bin: &Path, args: &[OsString]) -> Result<tokio::process::Child> {
    tokio::process::Command::new(bin)
        .args(args)
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("starting {label} ({})", bin.display()))
}

/// `nfs-walker --version`, trimmed (e.g. `nfs-walker 0.1.0`).
pub async fn walker_version(bin: &Path) -> Result<String> {
    let out = tokio::process::Command::new(bin)
        .arg("--version")
        .output()
        .await
        .with_context(|| format!("running {} --version", bin.display()))?;
    if !out.status.success() {
        anyhow::bail!("{} --version exited with {}", bin.display(), out.status);
    }
    let text = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if text.is_empty() {
        anyhow::bail!("{} --version printed nothing", bin.display());
    }
    Ok(text)
}

/// `packaging/nfs-walker.lock.json`: the nfs-walker build vamoose is
/// developed and packaged against. Embedded so `prepare` and `doctor`
/// can say which branch a mismatching scanner should come from.
#[derive(Debug, Clone, serde::Deserialize, PartialEq, Eq)]
pub struct WalkerLock {
    pub source_url: String,
    pub source_git_ref: String,
    pub source_git_sha: String,
    pub version: String,
    pub artifact_sha256: String,
}

const WALKER_LOCK_JSON: &str = include_str!("../../../packaging/nfs-walker.lock.json");

pub fn walker_lock() -> WalkerLock {
    serde_json::from_str(WALKER_LOCK_JSON).expect("packaging/nfs-walker.lock.json parses")
}

impl WalkerLock {
    /// `pinned` / `NOT the pinned build (...)` for a binary with this
    /// digest and `--version` line.
    pub fn describe(&self, sha256: &str, version: &str) -> String {
        if sha256 == self.artifact_sha256 {
            format!(
                "pinned build ({} @ {})",
                self.source_git_ref,
                &self.source_git_sha[..12]
            )
        } else {
            format!(
                "NOT the pinned build: {version} sha256 {}…; vamoose is built against {} @ {} \
                 ({}), see packaging/nfs-walker.lock.json",
                &sha256[..12.min(sha256.len())],
                self.source_git_ref,
                &self.source_git_sha[..12],
                self.version,
            )
        }
    }
}

/// `nfs-walker --help`, raw.
pub async fn walker_help(bin: &Path) -> Result<String> {
    let out = tokio::process::Command::new(bin)
        .arg("--help")
        .output()
        .await
        .with_context(|| format!("running {} --help", bin.display()))?;
    if !out.status.success() {
        anyhow::bail!("{} --help exited with {}", bin.display(), out.status);
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// The long options a `--help` text advertises: every `--name` token
/// that starts a line (after whitespace, optionally preceded by a
/// short alias like `-o, `).
pub fn help_flags(help: &str) -> Vec<String> {
    help.lines()
        .filter_map(|line| {
            let line = line.trim_start();
            let line = line
                .strip_prefix(|c: char| c == '-')
                .and_then(|rest| rest.strip_prefix(|c: char| c.is_ascii_alphanumeric()))
                .and_then(|rest| rest.strip_prefix(", "))
                .unwrap_or(line);
            let flag = line.strip_prefix("--")?;
            let name: String = flag
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '-')
                .collect();
            (!name.is_empty()).then(|| format!("--{name}"))
        })
        .collect()
}

/// The flags in `required` that `help` does not advertise.
pub fn missing_flags(help: &str, required: &[&str]) -> Vec<String> {
    let have = help_flags(help);
    required
        .iter()
        .filter(|f| !have.iter().any(|h| h == *f))
        .map(|f| f.to_string())
        .collect()
}

/// Refuse to scan with a scanner that lacks any flag prepare passes
/// — another nfs-walker branch exits 2 on the first unknown option,
/// after the operator has already waited for the mount. Names the
/// missing flags and the pinned branch.
pub async fn check_walker_flags(bin: &Path, version: &str) -> Result<()> {
    let help = walker_help(bin).await?;
    let missing = missing_flags(&help, WalkerInvocation::long_flags());
    if missing.is_empty() {
        return Ok(());
    }
    let lock = walker_lock();
    anyhow::bail!(
        "{} ({version}) does not accept {}; vamoose prepare is built against nfs-walker {} @ {} \
         ({}) — install the package built with that scanner, or point [prepare] walker_bin at it",
        bin.display(),
        missing.join(", "),
        lock.source_git_ref,
        &lock.source_git_sha[..12],
        lock.version,
    )
}

/// The directory holding the scan's part files. nfs-walker writes
/// `<output>/scans/<scan_id>/part-*.parquet`; older layouts put the
/// parts directly under `<output>`. Exactly one completed scan is
/// accepted so a stray second attempt can never be mistaken for the
/// intended one.
pub fn resolve_scan_dir(walk_root: &Path) -> Result<PathBuf> {
    if has_parquet(walk_root) {
        return Ok(walk_root.to_path_buf());
    }
    let scans = walk_root.join("scans");
    let mut candidates: Vec<PathBuf> = match std::fs::read_dir(&scans) {
        Ok(entries) => entries
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.is_dir() && has_parquet(p))
            .collect(),
        Err(_) => Vec::new(),
    };
    candidates.sort();
    match candidates.len() {
        1 => Ok(candidates.remove(0)),
        n => anyhow::bail!(
            "expected exactly one completed scans/<scan_id> with part files below {}; found {n}",
            walk_root.display()
        ),
    }
}

fn has_parquet(dir: &Path) -> bool {
    std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .any(|e| e.path().extension().is_some_and(|x| x == "parquet"))
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_url_appends_root_once() {
        assert_eq!(scan_url("nfs://h/export", "/"), "nfs://h/export");
        assert_eq!(scan_url("nfs://h/export/", ""), "nfs://h/export");
        assert_eq!(scan_url("nfs://h/export", "/data"), "nfs://h/export/data");
        assert_eq!(scan_url("nfs://h/export/", "/data/"), "nfs://h/export/data");
        assert_eq!(scan_url("nfs://h/export", "a/b"), "nfs://h/export/a/b");
    }

    #[test]
    fn walker_args_match_the_harness_shape() {
        let inv = WalkerInvocation {
            scan_url: "nfs://h/export/data".into(),
            output: "/w/scan/attempt-0001/walk.parquet".into(),
            workers: 8,
            exclude: vec![".snapshot".into(), "tmp".into()],
            shard_size_mb: 256,
            log: "/w/scan/attempt-0001/walker.jsonl".into(),
        };
        let args: Vec<String> = inv
            .args()
            .into_iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            args,
            vec![
                "nfs://h/export/data",
                "--output",
                "/w/scan/attempt-0001/walk.parquet",
                "--workers",
                "8",
                "--parquet-file-size-mb",
                "256",
                "--log",
                "/w/scan/attempt-0001/walker.jsonl",
                "--log-fmt",
                "json",
                "--exclude",
                ".snapshot",
                "--exclude",
                "tmp",
            ]
        );
    }

    #[test]
    fn rewrite_args_always_resume_with_a_report() {
        let inv = RewriteInvocation {
            input: "/w/scan/scans/abc".into(),
            output: "/w/canonical".into(),
            source_root: "/data".into(),
            walker_version: "nfs-walker 0.1.0".into(),
            report: "/w/rewrite.json".into(),
        };
        let args: Vec<String> = inv
            .args()
            .into_iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert!(args.contains(&"--resume".to_string()));
        assert_eq!(args[0..2], ["--input", "/w/scan/scans/abc"]);
        assert_eq!(args[args.len() - 2..], ["--report", "/w/rewrite.json"]);
    }

    #[test]
    fn lock_parses_and_names_a_commit() {
        let lock = walker_lock();
        assert_eq!(lock.source_git_sha.len(), 40, "full commit sha");
        assert!(lock.source_git_sha.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(lock.artifact_sha256.len(), 64);
        assert!(!lock.source_git_ref.is_empty());
        assert!(lock.version.starts_with("nfs-walker "));
        assert!(lock
            .describe(&lock.artifact_sha256, &lock.version)
            .starts_with("pinned build"));
        let other = lock.describe("deadbeefdeadbeefdeadbeef", "nfs-walker 0.2.0");
        assert!(other.starts_with("NOT the pinned build"), "{other}");
        assert!(other.contains(&lock.source_git_ref), "{other}");
    }

    /// Every long flag `args()` can emit is in `long_flags()`, so the
    /// probe cannot silently fall behind the invocation.
    #[test]
    fn long_flags_cover_args() {
        let inv = WalkerInvocation {
            scan_url: "nfs://h/export".into(),
            output: "/w/walk.parquet".into(),
            workers: 1,
            exclude: vec!["x".into()],
            shard_size_mb: 1,
            log: "/w/log".into(),
        };
        for arg in inv.args() {
            let arg = arg.to_string_lossy();
            if arg.starts_with("--") {
                assert!(
                    WalkerInvocation::long_flags().contains(&arg.as_ref()),
                    "{arg} emitted by args() but not listed in long_flags()"
                );
            }
        }
    }

    /// The `--help` shapes clap prints: bare long options, short
    /// aliases, values, and the `--log-interval-secs` near-miss that
    /// once fooled a grep for `--log`.
    #[test]
    fn help_flags_and_missing_flags() {
        let help = "\
Usage: nfs-walker [OPTIONS] <URL>

Options:
  -o, --output <PATH>            Output directory
      --workers <N>              GETATTR workers [default: 32]
      --log-interval-secs <SECS> Progress cadence
      --log-fmt <FMT>            text|json
      --exclude <GLOB>           May repeat
  -h, --help                     Print help
";
        assert_eq!(
            help_flags(help),
            vec![
                "--output",
                "--workers",
                "--log-interval-secs",
                "--log-fmt",
                "--exclude",
                "--help"
            ]
        );
        assert_eq!(
            missing_flags(help, WalkerInvocation::long_flags()),
            vec!["--parquet-file-size-mb", "--log"]
        );
        assert!(missing_flags(help, &["--output", "--exclude"]).is_empty());
    }

    #[test]
    fn scan_url_and_rewrite_root_agree_on_who_strips_the_root() {
        // The root goes into the scan URL, so the rewrite must strip
        // nothing: a walker path from a sub-tree scan has no `/data`
        // prefix to remove.
        assert_eq!(scan_url("nfs://h/export/", "/data/"), "nfs://h/export/data");
        assert_eq!(scan_url("nfs://h/export", "/"), "nfs://h/export");
        assert_eq!(REWRITE_SOURCE_ROOT, "/");
    }

    #[test]
    fn resolve_scan_dir_requires_exactly_one_completed_scan() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("walk.parquet");
        std::fs::create_dir_all(root.join("scans").join("empty")).unwrap();
        assert!(resolve_scan_dir(&root).is_err(), "no parts anywhere");

        let one = root.join("scans").join("20260824");
        std::fs::create_dir_all(&one).unwrap();
        std::fs::write(one.join("part-r00-00000.parquet"), b"x").unwrap();
        assert_eq!(resolve_scan_dir(&root).unwrap(), one);

        let two = root.join("scans").join("20260825");
        std::fs::create_dir_all(&two).unwrap();
        std::fs::write(two.join("part-r00-00000.parquet"), b"x").unwrap();
        assert!(resolve_scan_dir(&root).is_err(), "ambiguous");

        // Flat legacy layout: parts directly under the root.
        let flat = dir.path().join("flat");
        std::fs::create_dir_all(&flat).unwrap();
        std::fs::write(flat.join("part-r00-00000.parquet"), b"x").unwrap();
        assert_eq!(resolve_scan_dir(&flat).unwrap(), flat);
    }

    #[test]
    fn find_walker_rejects_a_configured_non_file() {
        let dir = tempfile::tempdir().unwrap();
        let err = find_walker(Some(&dir.path().join("nope"))).unwrap_err();
        assert!(format!("{err:#}").contains("walker_bin"));
        let real = dir.path().join("nfs-walker");
        std::fs::write(&real, b"#!/bin/sh\n").unwrap();
        assert_eq!(find_walker(Some(&real)).unwrap(), real);
    }
}
