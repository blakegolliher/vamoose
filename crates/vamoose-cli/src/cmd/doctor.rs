//! `vamoose doctor` — environment health check.
//!
//! Each check produces one PASS / WARN / FAIL / SKIP line. Exit code
//! is non-zero only on FAIL; WARN is operator-actionable but doesn't
//! block; SKIP means the check's config section is absent (the NFS
//! block when `[nfs]` is omitted — F45a) and never affects the exit
//! code. The S3 conditional-primitive checks here are the same
//! contract the v2 claim protocol depends on; if those fail, no
//! amount of worker tuning will save the run.

use crate::config::Config;
use clap::Args as ClapArgs;
use migration_core::claim::{ClaimStore, DeleteOutcome};
use migration_core::layout;
use migration_core::s3::S3Client;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(ClapArgs)]
pub struct Args {}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
enum Status {
    Pass,
    Warn,
    Fail,
    /// The check did not run because its config section is absent
    /// (F45a: `[nfs]` is optional). Explicitly reported so a skipped
    /// check never silently disappears; does not affect the exit
    /// code.
    Skip,
}

struct Checks {
    pass: u32,
    warn: u32,
    fail: u32,
    skip: u32,
    /// Every recorded line, in order — the unit-test seam for
    /// asserting what doctor reported without capturing stdout.
    entries: Vec<(Status, String, String)>,
}

impl Checks {
    fn new() -> Self {
        Self {
            pass: 0,
            warn: 0,
            fail: 0,
            skip: 0,
            entries: Vec::new(),
        }
    }
    fn record(&mut self, status: Status, label: &str, detail: impl AsRef<str>) {
        let tag = match status {
            Status::Pass => "PASS",
            Status::Warn => "WARN",
            Status::Fail => "FAIL",
            Status::Skip => "SKIP",
        };
        println!("{label}: {tag}  {}", detail.as_ref());
        match status {
            Status::Pass => self.pass += 1,
            Status::Warn => self.warn += 1,
            Status::Fail => self.fail += 1,
            Status::Skip => self.skip += 1,
        }
        self.entries
            .push((status, label.to_string(), detail.as_ref().to_string()));
    }
}

pub async fn run(_args: Args, config_path: Option<PathBuf>) -> anyhow::Result<()> {
    println!("vamoose doctor\n");

    let mut checks = Checks::new();

    let (cfg, cfg_path) = match Config::load_with_path(config_path) {
        Ok(p) => {
            checks.record(
                Status::Pass,
                "config",
                format!("{} parsed cleanly", p.1.display()),
            );
            p
        }
        Err(e) => {
            checks.record(Status::Fail, "config", format!("{e:#}"));
            print_summary(&checks);
            std::process::exit(2);
        }
    };

    let _ = cfg_path; // resolved path retained for context; not re-used

    // Build S3 client.
    let s3_client = match build_s3(&cfg).await {
        Ok(c) => c,
        Err(e) => {
            checks.record(Status::Fail, "s3 client", format!("{e:#}"));
            print_summary(&checks);
            std::process::exit(2);
        }
    };
    let s3: Arc<dyn ClaimStore> = s3_client.clone();

    // 2/3. S3 reachability + bucket exists. A LIST against an empty
    // prefix is the cheapest signal that auth + endpoint + bucket all
    // resolved correctly.
    match s3.list("").await {
        Ok(_) => {
            checks.record(
                Status::Pass,
                "s3 reach",
                format!("LIST s3://{}/ → 200", cfg.global.bucket),
            );
            checks.record(
                Status::Pass,
                "s3 bucket",
                format!("{} exists", cfg.global.bucket),
            );
        }
        Err(e) => {
            checks.record(Status::Fail, "s3 reach", format!("{e}"));
            print_summary(&checks);
            std::process::exit(2);
        }
    }

    // 4/5. Conditional PUT/DELETE — the actual contract the v2
    // claim protocol depends on. Probe key is namespaced under the
    // sentinel doctor/ prefix so it can't collide with claim/index
    // objects.
    let probe = "doctor/precondition-probe";
    // Best-effort: clear any leftover from a prior doctor run.
    if let Ok(Some((etag, _))) = s3.head_object(probe).await {
        let _ = s3.delete_if_match(probe, &etag).await;
    }

    let etag = match s3.put_if_absent(probe, b"vamoose-doctor".to_vec()).await {
        Ok(e) => e,
        Err(e) => {
            checks.record(
                Status::Fail,
                "s3 put-if-none-match",
                format!("first PUT failed: {e}"),
            );
            print_summary(&checks);
            std::process::exit(2);
        }
    };
    match s3.put_if_absent(probe, b"vamoose-doctor".to_vec()).await {
        Err(migration_core::Error::PreconditionFailed) => {
            checks.record(
                Status::Pass,
                "s3 put-if-none-match",
                format!("PUT /{probe} → 200, repeat → 412"),
            );
        }
        Ok(_) => {
            checks.record(
                Status::Fail,
                "s3 put-if-none-match",
                "second PUT succeeded — endpoint does not enforce If-None-Match",
            );
        }
        Err(e) => {
            checks.record(
                Status::Fail,
                "s3 put-if-none-match",
                format!("second PUT errored: {e}"),
            );
        }
    }

    match s3.delete_if_match(probe, &etag).await {
        Ok(DeleteOutcome::Deleted) => {
            // Now repeat — should be NotFound.
            match s3.delete_if_match(probe, &etag).await {
                Ok(DeleteOutcome::NotFound) => {
                    checks.record(
                        Status::Pass,
                        "s3 delete-if-match",
                        format!("DELETE /{probe} → 204, repeat → 404"),
                    );
                }
                other => {
                    checks.record(
                        Status::Fail,
                        "s3 delete-if-match",
                        format!("repeat DELETE returned {other:?} (expected NotFound)"),
                    );
                }
            }
        }
        other => {
            checks.record(
                Status::Fail,
                "s3 delete-if-match",
                format!("first DELETE returned {other:?} (expected Deleted)"),
            );
        }
    }

    // 6. Layout keys. Missing layout is a WARN — operator can run
    // `vamoose init` to materialize. Already-populated layout is the
    // normal mid-run state.
    let prefixes = [
        layout::INDEX_PREFIX,
        layout::SHARDS_PREFIX,
        layout::PROGRESS_PREFIX,
        layout::FAILURES_PREFIX,
    ];
    let mut missing = Vec::new();
    for p in &prefixes {
        match s3.list(p).await {
            Ok(entries) if entries.is_empty() => missing.push(*p),
            Ok(_) => {}
            Err(e) => {
                checks.record(Status::Fail, "s3 layout", format!("LIST {p}: {e}"));
                missing.clear();
                break;
            }
        }
    }
    if missing.len() == prefixes.len() {
        checks.record(
            Status::Warn,
            "s3 layout",
            format!(
                "{} all empty (run `vamoose init` to create marker objects)",
                missing.join(", "),
            ),
        );
    } else if !missing.is_empty() {
        checks.record(
            Status::Warn,
            "s3 layout",
            format!("partial layout: empty prefixes = {}", missing.join(", ")),
        );
    } else {
        checks.record(
            Status::Pass,
            "s3 layout",
            "all expected prefixes have at least one object",
        );
    }

    // 7-11. NFS checks (or explicit SKIPs when `[nfs]` is absent).
    run_nfs_checks(&mut checks, cfg.nfs.as_ref());

    // 12. Walker binary on PATH (or at the configured path).
    check_walker(&mut checks, &cfg);

    print_summary(&checks);

    if checks.fail > 0 {
        std::process::exit(1);
    }
    Ok(())
}

async fn build_s3(cfg: &Config) -> anyhow::Result<Arc<S3Client>> {
    let verify_tls = !cfg.s3.no_verify_ssl.unwrap_or(false);
    let client = S3Client::from_config(
        &cfg.s3.endpoint,
        &cfg.s3.region,
        &cfg.global.bucket,
        cfg.s3.profile.as_deref(),
        verify_tls,
    )
    .await?;
    Ok(Arc::new(client))
}

/// The labels of the five NFS checks, in report order. One SKIP line
/// per label is emitted when the config has no `[nfs]` section, so
/// the block's shape is identical whether or not the section exists.
const NFS_CHECK_LABELS: [&str; 5] = [
    "nfs src parse",
    "nfs dst parse",
    "nfs src mount",
    "nfs dst mount",
    "nfs roots differ",
];

/// Checks 7-11: the NFS block. `None` means the config has no
/// `[nfs]` section (it is optional as of F45a) — each check must
/// still be reported, as an explicit SKIP naming the absent section.
fn run_nfs_checks(checks: &mut Checks, nfs: Option<&crate::config::Nfs>) {
    let Some(nfs) = nfs else {
        for label in NFS_CHECK_LABELS {
            checks.record(
                Status::Skip,
                label,
                "no [nfs] section in config — check skipped",
            );
        }
        return;
    };

    // 7/9. NFS URL parsing. libnfs URLs are `nfs://host/export[/path]`.
    check_nfs_url(checks, NFS_CHECK_LABELS[0], &nfs.src_url);
    check_nfs_url(checks, NFS_CHECK_LABELS[1], &nfs.dst_url);

    // 8. Source mount: must exist + be readable. We don't try a
    // libnfs mount here — that requires sudo and is expensive; the
    // mount-point check is sufficient signal for a doctor pass.
    check_mount(checks, NFS_CHECK_LABELS[2], &nfs.src_mount, false);
    // 10. Dest mount: must exist + be writable. Probe by attempting
    // to create + remove a marker file in the dst_root.
    check_mount(checks, NFS_CHECK_LABELS[3], &nfs.dst_mount, true);

    // 11. src_root vs dst_root must differ when src and dst URLs
    // resolve to the same host:export — otherwise the worker copies
    // in place and the per-file overlap guard refuses to start.
    if nfs.src_url == nfs.dst_url && nfs.src_root == nfs.dst_root {
        checks.record(
            Status::Fail,
            NFS_CHECK_LABELS[4],
            format!(
                "src and dst URLs identical AND src_root == dst_root ({})",
                nfs.src_root
            ),
        );
    } else {
        checks.record(
            Status::Pass,
            NFS_CHECK_LABELS[4],
            format!("src_root={} dst_root={}", nfs.src_root, nfs.dst_root),
        );
    }
}

fn check_nfs_url(checks: &mut Checks, label: &str, url: &str) {
    if let Some(rest) = url.strip_prefix("nfs://") {
        if rest
            .split('/')
            .next()
            .map(|h| !h.is_empty())
            .unwrap_or(false)
            && rest.contains('/')
        {
            checks.record(Status::Pass, label, url);
            return;
        }
    }
    checks.record(
        Status::Fail,
        label,
        format!("expected nfs://host/export[/path], got {url}"),
    );
}

fn check_mount(checks: &mut Checks, label: &str, mount: &str, writable: bool) {
    let p = Path::new(mount);
    if !p.exists() {
        checks.record(Status::Fail, label, format!("{mount} does not exist"));
        return;
    }
    if !p.is_dir() {
        checks.record(Status::Fail, label, format!("{mount} is not a directory"));
        return;
    }
    // Readability: try to read the dir.
    if std::fs::read_dir(p).is_err() {
        checks.record(Status::Fail, label, format!("{mount} not readable"));
        return;
    }
    if writable {
        let probe = p.join(".vamoose-doctor-probe");
        match std::fs::write(&probe, b"") {
            Ok(()) => {
                let _ = std::fs::remove_file(&probe);
                checks.record(Status::Pass, label, format!("{mount} mounted, writable"));
            }
            Err(e) => {
                checks.record(Status::Fail, label, format!("{mount} not writable: {e}"));
            }
        }
    } else {
        checks.record(Status::Pass, label, format!("{mount} mounted, readable"));
    }
}

fn check_walker(checks: &mut Checks, cfg: &Config) {
    let configured = cfg.walker.as_ref().and_then(|w| w.binary_path.clone());
    if let Some(path) = configured {
        if path.is_file() {
            checks.record(Status::Pass, "walker binary", format!("{}", path.display()));
        } else {
            checks.record(
                Status::Fail,
                "walker binary",
                format!("configured path {} is not a file", path.display()),
            );
        }
        return;
    }
    if let Ok(found) = which("nfs-walker") {
        checks.record(
            Status::Pass,
            "walker binary",
            format!("{}", found.display()),
        );
    } else {
        checks.record(
            Status::Warn,
            "walker binary",
            "nfs-walker not on PATH (set walker.binary_path in config or install it)",
        );
    }
}

fn which(name: &str) -> Result<PathBuf, ()> {
    let path_var = std::env::var_os("PATH").ok_or(())?;
    for dir in std::env::split_paths(&path_var) {
        let candidate = dir.join(name);
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    Err(())
}

fn print_summary(c: &Checks) {
    if c.skip > 0 {
        println!(
            "\nSummary: {} PASS, {} WARN, {} FAIL, {} SKIP",
            c.pass, c.warn, c.fail, c.skip
        );
    } else {
        println!(
            "\nSummary: {} PASS, {} WARN, {} FAIL",
            c.pass, c.warn, c.fail
        );
    }
}

// =============================================================================
// F45a: NFS checks with an optional [nfs] section
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Nfs;

    fn nfs_section() -> Nfs {
        Nfs {
            src_url: "nfs://src-filer/export".into(),
            dst_url: "nfs://dst-filer/export".into(),
            // Real paths so the mount checks exercise their genuine
            // logic: "/" is always a readable dir; the temp dir is
            // always writable.
            src_mount: "/".into(),
            dst_mount: std::env::temp_dir().to_string_lossy().into_owned(),
            src_root: "/data-src".into(),
            dst_root: "/data-dst".into(),
        }
    }

    /// F45a acceptance: with no `[nfs]` section, every NFS check is
    /// reported as an explicit SKIP naming the absent section — not
    /// failed, and not silently dropped from the report.
    #[test]
    fn nfs_checks_skip_explicitly_without_nfs_section() {
        let mut checks = Checks::new();
        run_nfs_checks(&mut checks, None);
        assert_eq!(
            checks.skip,
            NFS_CHECK_LABELS.len() as u32,
            "every NFS check must be reported SKIPPED when [nfs] is absent",
        );
        assert_eq!(checks.fail, 0, "absent [nfs] is not a failure");
        assert_eq!(checks.pass, 0);
        assert_eq!(checks.warn, 0);
        let labels: Vec<&str> = checks.entries.iter().map(|(_, l, _)| l.as_str()).collect();
        assert_eq!(
            labels,
            NFS_CHECK_LABELS.to_vec(),
            "the skipped block must report the same labels in the same order",
        );
        for (status, label, detail) in &checks.entries {
            assert_eq!(*status, Status::Skip, "{label} must be Skip");
            assert!(
                detail.contains("no [nfs] section"),
                "{label} skip detail must say why (no [nfs] section), got: {detail}",
            );
        }
    }

    /// With `[nfs]` present the block runs for real — nothing is
    /// skipped and all five checks report.
    #[test]
    fn nfs_checks_run_when_section_present() {
        let nfs = nfs_section();
        let mut checks = Checks::new();
        run_nfs_checks(&mut checks, Some(&nfs));
        assert_eq!(checks.skip, 0, "present [nfs] must not skip anything");
        assert_eq!(
            checks.entries.len(),
            NFS_CHECK_LABELS.len(),
            "all five NFS checks must report",
        );
        // This particular Nfs is fully healthy on any host: valid
        // URLs, readable "/", writable temp dir, differing roots.
        assert_eq!(checks.fail, 0, "healthy [nfs] must not fail");
        assert_eq!(checks.pass, NFS_CHECK_LABELS.len() as u32);
    }
}
