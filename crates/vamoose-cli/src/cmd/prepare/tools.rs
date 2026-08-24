//! Locating and invoking the two external stages: `nfs-walker` (the
//! scanner, shipped alongside vamoose) and `mig-walker-rewrite` (the
//! canonical-schema converter from this workspace).

use anyhow::{Context, Result};
use std::ffi::OsString;
use std::path::{Path, PathBuf};

/// Where the packages install the bundled scanner.
pub(crate) const PACKAGED_WALKER: &str = "/usr/libexec/vamoose/nfs-walker";

/// Resolve the scanner: explicit configuration, `../libexec/nfs-walker`
/// next to the running binary (release bundles under a prefix), the
/// packaged path, then `nfs-walker` on PATH.
pub(crate) fn find_walker(configured: Option<&Path>) -> Result<PathBuf> {
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
/// next to the running `vamoose` binary first, then PATH.
pub(crate) fn find_sibling(name: &str) -> Result<PathBuf> {
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
            "{name} not found next to vamoose or on PATH (it ships in the same package)"
        )
    })
}

pub(crate) fn which(name: &str) -> Option<PathBuf> {
    let path_var = std::env::var_os("PATH")?;
    std::env::split_paths(&path_var)
        .map(|dir| dir.join(name))
        .find(|candidate| candidate.is_file())
}

/// The URL nfs-walker scans: the export URL with the source root
/// appended (a root of `/` scans the export itself).
pub(crate) fn scan_url(src_url: &str, source_root: &str) -> String {
    let base = src_url.trim_end_matches('/');
    let root = source_root.trim();
    if root.is_empty() || root == "/" {
        base.to_string()
    } else {
        format!("{base}/{}", root.trim_matches('/'))
    }
}

/// Everything that shapes one scan; kept as data so the argument list
/// is unit-testable and recorded in the scan checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WalkerInvocation {
    pub(crate) scan_url: String,
    pub(crate) output: PathBuf,
    pub(crate) workers: usize,
    pub(crate) exclude: Vec<String>,
    pub(crate) shard_size_mb: u64,
    pub(crate) log: PathBuf,
}

impl WalkerInvocation {
    pub(crate) fn args(&self) -> Vec<OsString> {
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
}

/// Arguments for the canonical rewrite of one scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RewriteInvocation {
    pub(crate) input: PathBuf,
    pub(crate) output: PathBuf,
    pub(crate) source_root: String,
    pub(crate) walker_version: String,
    pub(crate) report: PathBuf,
}

impl RewriteInvocation {
    pub(crate) fn args(&self) -> Vec<OsString> {
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

/// Run an external stage with inherited stdio so its own progress
/// output reaches the operator's terminal. Fails on a non-zero exit.
pub(crate) async fn run_stage(label: &str, bin: &Path, args: &[OsString]) -> Result<()> {
    let status = tokio::process::Command::new(bin)
        .args(args)
        .status()
        .await
        .with_context(|| format!("starting {label} ({})", bin.display()))?;
    if !status.success() {
        anyhow::bail!("{label} failed: {} exited with {status}", bin.display());
    }
    Ok(())
}

/// `nfs-walker --version`, trimmed (e.g. `nfs-walker 0.1.0`).
pub(crate) async fn walker_version(bin: &Path) -> Result<String> {
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

/// The directory holding the scan's part files. nfs-walker writes
/// `<output>/scans/<scan_id>/part-*.parquet`; older layouts put the
/// parts directly under `<output>`. Exactly one completed scan is
/// accepted so a stray second attempt can never be mistaken for the
/// intended one.
pub(crate) fn resolve_scan_dir(walk_root: &Path) -> Result<PathBuf> {
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
