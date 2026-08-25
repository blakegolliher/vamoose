//! `vamoose doctor` — environment health check.
//!
//! Each check produces one PASS / WARN / FAIL / SKIP line. Exit code
//! is non-zero only on FAIL; WARN is operator-actionable but doesn't
//! block; SKIP means the check's config section is absent (the NFS
//! block when neither `[mover]` nor the legacy `[nfs]` is present —
//! F45a) and never affects the exit code. The S3 conditional-primitive
//! checks here are the same contract the v2 claim protocol depends
//! on; if those fail, no amount of worker tuning will save the run.
//! The NFS block mounts the exports the workers will use, over libnfs
//! (reserved port: run as root, like the services), and proves the
//! roots exist and the destination root takes a file.

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
pub(crate) enum IncompleteReason {
    ConfigurationLoad,
    S3ClientConstruction,
    S3Reachability,
    ConditionalOperation,
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub(crate) enum DoctorOutcome {
    Healthy,
    ChecksFailed,
    Incomplete(IncompleteReason),
}

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

pub async fn run(_args: Args, config_path: Option<PathBuf>) -> anyhow::Result<DoctorOutcome> {
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
            return Ok(DoctorOutcome::Incomplete(
                IncompleteReason::ConfigurationLoad,
            ));
        }
    };

    let _ = cfg_path; // resolved path retained for context; not re-used

    // Build S3 client.
    let s3_client = match build_s3(cfg.storage()).await {
        Ok(c) => c,
        Err(e) => {
            checks.record(Status::Fail, "s3 client", format!("{e:#}"));
            print_summary(&checks);
            return Ok(DoctorOutcome::Incomplete(
                IncompleteReason::S3ClientConstruction,
            ));
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
                format!("LIST s3://{}/ → 200", cfg.storage().bucket),
            );
            checks.record(
                Status::Pass,
                "s3 bucket",
                format!("{} exists", cfg.storage().bucket),
            );
        }
        Err(e) => {
            checks.record(Status::Fail, "s3 reach", format!("{e}"));
            print_summary(&checks);
            return Ok(DoctorOutcome::Incomplete(IncompleteReason::S3Reachability));
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
            return Ok(DoctorOutcome::Incomplete(
                IncompleteReason::ConditionalOperation,
            ));
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

    // The contract the claim and lease code rely on: a DELETE with a
    // stale etag must be refused (412) and leave the object in place;
    // one with the current etag removes it. What a repeat DELETE of
    // the now-missing key returns is *not* part of the contract —
    // AWS answers 204, VAST answers 204, some stores 404 — so it is
    // not probed (an earlier version failed VAST on exactly that).
    match s3
        .delete_if_match(probe, "0000000000deadbeef0000000000cafe")
        .await
    {
        Ok(DeleteOutcome::EtagMismatch) => match s3.delete_if_match(probe, &etag).await {
            Ok(DeleteOutcome::Deleted) => match s3.head_object(probe).await {
                Ok(None) => checks.record(
                    Status::Pass,
                    "s3 delete-if-match",
                    format!("DELETE /{probe} with stale etag → 412, with current etag → 204"),
                ),
                Ok(Some(_)) => checks.record(
                    Status::Fail,
                    "s3 delete-if-match",
                    "DELETE with the current etag returned 204 but the object is still there",
                ),
                Err(e) => checks.record(
                    Status::Fail,
                    "s3 delete-if-match",
                    format!("HEAD after DELETE errored: {e}"),
                ),
            },
            other => checks.record(
                Status::Fail,
                "s3 delete-if-match",
                format!("DELETE with the current etag returned {other:?} (expected Deleted)"),
            ),
        },
        Ok(DeleteOutcome::Deleted) => {
            checks.record(
                Status::Fail,
                "s3 delete-if-match",
                "DELETE with a stale etag succeeded — endpoint does not enforce If-Match on \
                 DELETE; a fenced worker could remove a claim it no longer owns",
            );
        }
        other => {
            let _ = s3.delete_if_match(probe, &etag).await;
            checks.record(
                Status::Fail,
                "s3 delete-if-match",
                format!("DELETE with a stale etag returned {other:?} (expected EtagMismatch)"),
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

    // 7-11. NFS checks against the exports the workers will use, or
    // explicit SKIPs when the config names none.
    let prepare = cfg.prepare();
    let target = match (cfg.mover(), cfg.nfs()) {
        (Some(mover), _) => Some(NfsTarget::Mover {
            src_url: &mover.src_url,
            dst_url: &mover.dst_url,
            source_root: &prepare.source_root,
            dest_root: &prepare.dest_root,
        }),
        (None, Some(nfs)) => Some(NfsTarget::Legacy(nfs)),
        (None, None) => None,
    };
    let rpc_timeout_ms = cfg
        .mover()
        .map(|m| m.rpc_timeout_ms)
        .unwrap_or(migration_mover::DEFAULT_RPC_TIMEOUT_MS);
    let probe = |url: &str, root: &str, write: bool| {
        tokio::task::block_in_place(|| probe_export(url, root, write, rpc_timeout_ms))
    };
    run_nfs_checks(&mut checks, target, &probe);

    // 12. Walker binary on PATH (or at the configured path).
    check_walker(&mut checks, &cfg);

    print_summary(&checks);

    Ok(if checks.fail > 0 {
        DoctorOutcome::ChecksFailed
    } else {
        DoctorOutcome::Healthy
    })
}

async fn build_s3(storage: &crate::config::StorageSettings) -> anyhow::Result<Arc<S3Client>> {
    let client = S3Client::from_config(
        &storage.endpoint,
        &storage.region,
        &storage.bucket,
        storage.profile.as_deref(),
        storage.verify_tls,
    )
    .await?;
    Ok(Arc::new(client))
}

/// The labels of the five NFS checks, in report order. One SKIP line
/// per label is emitted when the config names no exports, so the
/// block's shape is identical whether or not they are configured.
const NFS_CHECK_LABELS: [&str; 5] = [
    "nfs src parse",
    "nfs dst parse",
    "nfs src reach",
    "nfs dst reach",
    "nfs roots differ",
];

/// What the NFS block checks: the canonical `[mover]` URLs with the
/// `[prepare]` roots (what `prepare` scans and the workers copy), or
/// the legacy `[nfs]` section with its kernel mount points.
#[derive(Debug, Clone, Copy)]
enum NfsTarget<'a> {
    Mover {
        src_url: &'a str,
        dst_url: &'a str,
        source_root: &'a str,
        dest_root: &'a str,
    },
    Legacy(&'a crate::config::Nfs),
}

/// Mounts `url` and looks at `root` inside it; with `write`, also
/// creates and removes a marker file there. `Ok` carries the PASS
/// detail, `Err` the FAIL detail. Injected so the block is testable
/// without an NFS server.
type ExportProbe<'p> = &'p dyn Fn(&str, &str, bool) -> Result<String, String>;

/// Checks 7-11: the NFS block. `None` means the config names no
/// exports (both `[mover]` and the legacy `[nfs]` are optional —
/// F45a) — each check must still be reported, as an explicit SKIP
/// naming the absent section.
fn run_nfs_checks(checks: &mut Checks, target: Option<NfsTarget<'_>>, probe: ExportProbe<'_>) {
    let Some(target) = target else {
        for label in NFS_CHECK_LABELS {
            checks.record(
                Status::Skip,
                label,
                "no [mover] section in config (nor legacy [nfs]) — check skipped",
            );
        }
        return;
    };

    let (src_url, dst_url, src_root, dst_root) = match target {
        NfsTarget::Mover {
            src_url,
            dst_url,
            source_root,
            dest_root,
        } => (src_url, dst_url, source_root, dest_root),
        NfsTarget::Legacy(nfs) => (
            nfs.src_url.as_str(),
            nfs.dst_url.as_str(),
            nfs.src_root.as_str(),
            nfs.dst_root.as_str(),
        ),
    };

    // 7/9. NFS URL parsing. libnfs URLs are `nfs://host/export[/path]`.
    let src_ok = check_nfs_url(checks, NFS_CHECK_LABELS[0], src_url);
    let dst_ok = check_nfs_url(checks, NFS_CHECK_LABELS[1], dst_url);

    match target {
        NfsTarget::Mover { .. } => {
            // 8/10. Mount each export the way the workers will and look
            // at the configured root: the source must be there, the
            // destination must take a file.
            for (label, url, root, write, ok) in [
                (NFS_CHECK_LABELS[2], src_url, src_root, false, src_ok),
                (NFS_CHECK_LABELS[3], dst_url, dst_root, true, dst_ok),
            ] {
                if !ok {
                    checks.record(Status::Fail, label, "URL did not parse (see above)");
                    continue;
                }
                match probe(url, root, write) {
                    Ok(detail) => checks.record(Status::Pass, label, detail),
                    Err(detail) => checks.record(Status::Fail, label, detail),
                }
            }
        }
        NfsTarget::Legacy(nfs) => {
            // 8. Source mount point: must exist + be readable.
            check_mount(checks, NFS_CHECK_LABELS[2], &nfs.src_mount, false);
            // 10. Dest mount point: must exist + be writable. Probe by
            // creating + removing a marker file.
            check_mount(checks, NFS_CHECK_LABELS[3], &nfs.dst_mount, true);
        }
    }

    // 11. When src and dst URLs name the same export the roots must
    // not coincide or nest — otherwise the worker copies in place
    // (or into its own source) and the per-file overlap guard
    // refuses to start.
    check_roots_differ(checks, src_url, dst_url, src_root, dst_root);
}

/// Normalise a root for comparison: `""`, `"/"`, `"/a/"`, `"a"` →
/// `"/"`, `"/"`, `"/a"`, `"/a"`.
fn norm_root(root: &str) -> String {
    let inner = root.trim().trim_matches('/');
    if inner.is_empty() {
        "/".to_string()
    } else {
        format!("/{inner}")
    }
}

fn check_roots_differ(
    checks: &mut Checks,
    src_url: &str,
    dst_url: &str,
    src_root: &str,
    dst_root: &str,
) {
    let same_export = src_url.trim_end_matches('/') == dst_url.trim_end_matches('/');
    let (s, d) = (norm_root(src_root), norm_root(dst_root));
    let nested = |outer: &str, inner: &str| {
        outer == "/"
            || inner
                .strip_prefix(outer)
                .is_some_and(|rest| rest.starts_with('/'))
    };
    if same_export && s == d {
        checks.record(
            Status::Fail,
            NFS_CHECK_LABELS[4],
            format!("src and dst URLs identical AND source_root == dest_root ({s})"),
        );
    } else if same_export && (nested(&s, &d) || nested(&d, &s)) {
        checks.record(
            Status::Fail,
            NFS_CHECK_LABELS[4],
            format!(
                "src and dst URLs identical and the roots nest (source_root={s} dest_root={d})"
            ),
        );
    } else {
        checks.record(
            Status::Pass,
            NFS_CHECK_LABELS[4],
            format!("source_root={s} dest_root={d}"),
        );
    }
}

/// Mount `url` over libnfs (NFSv3, reserved port — root, like the
/// services), stat `root` inside it and, for the destination, create
/// and remove a marker file there. Blocking: bounded by the mover's
/// RPC timeout.
fn probe_export(url: &str, root: &str, write: bool, rpc_timeout_ms: u32) -> Result<String, String> {
    use migration_mover::libnfs::ops;
    let hint = || {
        if unsafe { libc::geteuid() } != 0 {
            " (not root: libnfs needs a reserved port — run `sudo vamoose doctor`)"
        } else {
            ""
        }
    };
    let mut ctx = migration_mover::NfsContext::mount_url(url, rpc_timeout_ms)
        .map_err(|e| format!("mount {url}: {e:#}{}", hint()))?;
    let root = norm_root(root);
    ops::stat_times(&mut ctx, root.as_bytes())
        .map_err(|e| format!("{url} mounted, but {root}: {}", e.error))?;
    if !write {
        return Ok(format!("{url} mounted, {root} present"));
    }
    let marker = if root == "/" {
        "/.vamoose-doctor-probe".to_string()
    } else {
        format!("{root}/.vamoose-doctor-probe")
    };
    let fh = ops::create_write(&mut ctx, marker.as_bytes(), 0o600).map_err(|e| {
        format!(
            "{url} mounted, {root} present, but create {marker}: {}",
            e.error
        )
    })?;
    ops::close_quietly(&mut ctx, fh);
    ops::unlink(&mut ctx, marker.as_bytes()).map_err(|e| {
        format!(
            "{url}: created {marker} but could not remove it: {}",
            e.error
        )
    })?;
    Ok(format!("{url} mounted, {root} present and writable"))
}

fn check_nfs_url(checks: &mut Checks, label: &str, url: &str) -> bool {
    if let Some(rest) = url.strip_prefix("nfs://") {
        if rest
            .split('/')
            .next()
            .map(|h| !h.is_empty())
            .unwrap_or(false)
            && rest.contains('/')
        {
            checks.record(Status::Pass, label, url);
            return true;
        }
    }
    checks.record(
        Status::Fail,
        label,
        format!("expected nfs://host/export[/path], got {url}"),
    );
    false
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

/// Same resolution as `vamoose prepare`: `[prepare] walker_bin`, the
/// legacy `[walker] binary_path`, the packaged path, then PATH.
fn check_walker(checks: &mut Checks, cfg: &Config) {
    let configured = cfg
        .prepare()
        .walker_bin
        .or_else(|| cfg.walker().and_then(|w| w.binary_path.clone()));
    match crate::cmd::prepare::tools::find_walker(configured.as_deref()) {
        Ok(found) => checks.record(
            Status::Pass,
            "walker binary",
            format!("{}", found.display()),
        ),
        Err(e) if configured.is_some() => {
            checks.record(Status::Fail, "walker binary", format!("{e:#}"));
        }
        Err(_) => checks.record(
            Status::Warn,
            "walker binary",
            format!(
                "nfs-walker not found at {} or on PATH; `vamoose prepare` needs it (install the \
                 package built with NFS_WALKER_BIN, or set [prepare] walker_bin)",
                crate::cmd::prepare::tools::PACKAGED_WALKER
            ),
        ),
    }
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

    /// A probe that must never be reached (legacy and skip paths).
    fn no_probe(url: &str, _root: &str, _write: bool) -> Result<String, String> {
        panic!("probe must not run for {url}")
    }

    fn healthy_probe(url: &str, root: &str, write: bool) -> Result<String, String> {
        Ok(format!("{url} {root} write={write}"))
    }

    fn labels(checks: &Checks) -> Vec<&str> {
        checks.entries.iter().map(|(_, l, _)| l.as_str()).collect()
    }

    fn status_of<'a>(checks: &'a Checks, label: &str) -> (&'a Status, &'a str) {
        let (s, _, d) = checks
            .entries
            .iter()
            .find(|(_, l, _)| l == label)
            .unwrap_or_else(|| panic!("no entry for {label}"));
        (s, d.as_str())
    }

    /// F45a acceptance: with neither `[mover]` nor `[nfs]`, every NFS
    /// check is reported as an explicit SKIP naming the absent section
    /// — not failed, and not silently dropped from the report.
    #[test]
    fn nfs_checks_skip_explicitly_without_exports() {
        let mut checks = Checks::new();
        run_nfs_checks(&mut checks, None, &no_probe);
        assert_eq!(
            checks.skip,
            NFS_CHECK_LABELS.len() as u32,
            "every NFS check must be reported SKIPPED when no exports are configured",
        );
        assert_eq!(checks.fail, 0, "absent sections are not a failure");
        assert_eq!(checks.pass, 0);
        assert_eq!(checks.warn, 0);
        assert_eq!(
            labels(&checks),
            NFS_CHECK_LABELS.to_vec(),
            "the skipped block must report the same labels in the same order",
        );
        for (status, label, detail) in &checks.entries {
            assert_eq!(*status, Status::Skip, "{label} must be Skip");
            assert!(
                detail.contains("no [mover] section"),
                "{label} skip detail must say why (no [mover] section), got: {detail}",
            );
        }
    }

    /// With the legacy `[nfs]` section the block runs against the
    /// kernel mount points — nothing is skipped, all five report.
    #[test]
    fn nfs_checks_run_when_legacy_section_present() {
        let nfs = nfs_section();
        let mut checks = Checks::new();
        run_nfs_checks(&mut checks, Some(NfsTarget::Legacy(&nfs)), &no_probe);
        assert_eq!(checks.skip, 0, "present [nfs] must not skip anything");
        assert_eq!(labels(&checks), NFS_CHECK_LABELS.to_vec());
        // This particular Nfs is fully healthy on any host: valid
        // URLs, readable "/", writable temp dir, differing roots.
        assert_eq!(checks.fail, 0, "healthy [nfs] must not fail");
        assert_eq!(checks.pass, NFS_CHECK_LABELS.len() as u32);
    }

    /// The quickstart shape — `[mover]` URLs plus `[prepare]` roots —
    /// probes both exports and passes every check on a healthy pair.
    #[test]
    fn mover_checks_probe_both_exports() {
        let mut checks = Checks::new();
        run_nfs_checks(
            &mut checks,
            Some(NfsTarget::Mover {
                src_url: "nfs://src/export",
                dst_url: "nfs://dst/export",
                source_root: "/projects",
                dest_root: "/",
            }),
            &healthy_probe,
        );
        assert_eq!(labels(&checks), NFS_CHECK_LABELS.to_vec());
        assert_eq!(checks.fail, 0);
        assert_eq!(checks.skip, 0);
        assert_eq!(checks.pass, 5);
    }

    /// The probes are called with the configured roots and only the
    /// destination is asked to write.
    #[test]
    fn mover_probe_arguments() {
        let calls = std::cell::RefCell::new(Vec::new());
        let probe = |url: &str, root: &str, write: bool| {
            calls
                .borrow_mut()
                .push((url.to_string(), root.to_string(), write));
            Ok(String::new())
        };
        let mut checks = Checks::new();
        run_nfs_checks(
            &mut checks,
            Some(NfsTarget::Mover {
                src_url: "nfs://src/export",
                dst_url: "nfs://dst/export",
                source_root: "/projects",
                dest_root: "/landing",
            }),
            &probe,
        );
        assert_eq!(
            calls.into_inner(),
            vec![
                (
                    "nfs://src/export".to_string(),
                    "/projects".to_string(),
                    false
                ),
                ("nfs://dst/export".to_string(), "/landing".to_string(), true),
            ]
        );
    }

    /// A failing probe is a FAIL on its own line and nothing else.
    #[test]
    fn mover_probe_failure_is_reported_per_export() {
        let probe = |url: &str, _root: &str, _write: bool| {
            if url.starts_with("nfs://dst") {
                Err("mount nfs://dst/export: timed out".to_string())
            } else {
                Ok("fine".to_string())
            }
        };
        let mut checks = Checks::new();
        run_nfs_checks(
            &mut checks,
            Some(NfsTarget::Mover {
                src_url: "nfs://src/export",
                dst_url: "nfs://dst/export",
                source_root: "/",
                dest_root: "/",
            }),
            &probe,
        );
        assert_eq!(checks.fail, 1);
        let (status, detail) = status_of(&checks, "nfs dst reach");
        assert_eq!(*status, Status::Fail);
        assert!(detail.contains("timed out"), "{detail}");
        assert_eq!(*status_of(&checks, "nfs src reach").0, Status::Pass);
        assert_eq!(*status_of(&checks, "nfs roots differ").0, Status::Pass);
    }

    /// An unparseable URL fails its parse line and its reach line
    /// without ever probing.
    #[test]
    fn mover_bad_url_is_not_probed() {
        let mut checks = Checks::new();
        run_nfs_checks(
            &mut checks,
            Some(NfsTarget::Mover {
                src_url: "src-filer:/export",
                dst_url: "nfs://dst/export",
                source_root: "/",
                dest_root: "/",
            }),
            &|url, root, write| {
                assert_eq!(url, "nfs://dst/export", "only the good URL is probed");
                healthy_probe(url, root, write)
            },
        );
        assert_eq!(*status_of(&checks, "nfs src parse").0, Status::Fail);
        assert_eq!(*status_of(&checks, "nfs src reach").0, Status::Fail);
        assert_eq!(*status_of(&checks, "nfs dst reach").0, Status::Pass);
        assert_eq!(checks.fail, 2);
    }

    /// Same export: equal or nested roots are refused; distinct
    /// siblings pass. Different exports always pass.
    #[test]
    fn roots_differ_rules() {
        let cases = [
            ("nfs://a/x", "nfs://a/x", "/", "/", Status::Fail),
            ("nfs://a/x", "nfs://a/x/", "/data", "data/", Status::Fail),
            ("nfs://a/x", "nfs://a/x", "/", "/dst", Status::Fail),
            ("nfs://a/x", "nfs://a/x", "/src", "/src/copy", Status::Fail),
            ("nfs://a/x", "nfs://a/x", "/src", "/srcopy", Status::Pass),
            ("nfs://a/x", "nfs://a/x", "/src", "/dst", Status::Pass),
            ("nfs://a/x", "nfs://b/x", "/", "/", Status::Pass),
        ];
        for (su, du, sr, dr, want) in cases {
            let mut checks = Checks::new();
            check_roots_differ(&mut checks, su, du, sr, dr);
            let (got, detail) = status_of(&checks, "nfs roots differ");
            assert_eq!(got, &want, "{su} {du} {sr} {dr}: {detail}");
        }
    }

    #[test]
    fn norm_root_shapes() {
        for (input, want) in [
            ("", "/"),
            ("/", "/"),
            ("/a/", "/a"),
            ("a", "/a"),
            (" /a/b ", "/a/b"),
        ] {
            assert_eq!(norm_root(input), want, "{input:?}");
        }
    }
}
