//! `mig-aggr clean-partials` — lease-aware sweep of orphaned
//! `.partial` files on a locally mounted destination export.
//!
//! Fenced or killed workers leave hidden `.<base>.<host>.<pid>.partial`
//! siblings behind (the mover writes into a partial and renames on
//! success; see `migration-mover/src/paths.rs::partial_path`). Nothing
//! else in the system cleans them up. The sweep is dry-run by default:
//! deletion must be armed with `--delete`, and is gated on run
//! liveness read (read-only) from the claim bucket.
//!
//! The name matcher is deliberately reimplemented here instead of
//! depending on migration-mover: that crate links libnfs via FFI and
//! mig-aggr must stay FFI-free. A pinning test
//! (`partial_name_matcher_pins_mover_format`) holds the two in sync.

use anyhow::{bail, Context};
use chrono::{DateTime, SecondsFormat, Utc};
use migration_core::claim::ClaimStore;
use migration_core::s3::S3Client;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

/// A `.partial` file the sweep found (and, when armed, will delete).
#[derive(Debug)]
pub struct Candidate {
    pub path: PathBuf,
    pub size: u64,
    /// Missing only on filesystems that cannot report mtime.
    pub mtime: Option<SystemTime>,
}

/// A claim whose lease is still live (or that cannot be proven dead),
/// blocking deletion.
#[derive(Debug)]
pub struct LiveClaim {
    pub key: String,
    /// Human-readable reason, e.g. `host host-a, age 12s`.
    pub detail: String,
}

pub struct SweepOptions {
    /// Deletion armed (`--delete`). Off means dry run — the default.
    pub delete: bool,
    /// Liveness gate overridden (`--force`).
    pub force: bool,
}

#[derive(Debug, Default)]
pub struct SweepReport {
    pub candidates: Vec<Candidate>,
    /// What the liveness gate saw. Non-empty with `force` means the
    /// gate was overridden, not clean.
    pub live: Vec<LiveClaim>,
    pub deleted_files: u64,
    pub deleted_bytes: u64,
    pub failed_deletions: u64,
}

/// Does `name` match the mover's partial-file shape
/// `.<base>.<host>.<pid>.partial`?
///
/// Pinned to `migration-mover/src/paths.rs::partial_path`, which
/// emits `<dir>/.<base>.<host>.<pid>.partial` — a hidden dot-prefixed
/// sibling whose name has at least four dot-separated fields ending
/// in `partial`, with a numeric `<pid>`. `<base>` may itself contain
/// dots, so the trailing fields are parsed from the right. Operates
/// on raw bytes because source basenames are not guaranteed UTF-8 and
/// `partial_path` round-trips them byte-for-byte.
pub fn is_partial_name(name: &OsStr) -> bool {
    use std::os::unix::ffi::OsStrExt;
    let bytes = name.as_bytes();
    let Some(rest) = bytes.strip_prefix(b".") else {
        return false;
    };
    let Some(rest) = rest.strip_suffix(b".partial") else {
        return false;
    };
    // rest is `<base>.<host>.<pid>`; split from the right so a dotted
    // base stays intact.
    let mut fields = rest.rsplitn(3, |&b| b == b'.');
    let (Some(pid), Some(host), Some(base)) = (fields.next(), fields.next(), fields.next()) else {
        return false;
    };
    !pid.is_empty() && pid.iter().all(u8::is_ascii_digit) && !host.is_empty() && !base.is_empty()
}

/// Walk `root` and collect every regular file whose name matches the
/// partial pattern. Never follows symlinks: symlinked directories are
/// not descended into and a symlink whose name matches is not a
/// candidate (`DirEntry::file_type` does not traverse links).
pub fn plan_sweep(root: &Path) -> anyhow::Result<Vec<Candidate>> {
    fn walk(dir: &Path, out: &mut Vec<Candidate>) -> anyhow::Result<()> {
        for entry in std::fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
            let entry = entry.with_context(|| format!("reading an entry of {}", dir.display()))?;
            let file_type = entry
                .file_type()
                .with_context(|| format!("stat {}", entry.path().display()))?;
            if file_type.is_dir() {
                walk(&entry.path(), out)?;
            } else if file_type.is_file() && is_partial_name(&entry.file_name()) {
                // DirEntry::metadata also never traverses symlinks.
                let md = entry
                    .metadata()
                    .with_context(|| format!("stat {}", entry.path().display()))?;
                out.push(Candidate {
                    path: entry.path(),
                    size: md.len(),
                    mtime: md.modified().ok(),
                });
            }
        }
        Ok(())
    }

    let mut out = Vec::new();
    walk(root, &mut out)?;
    // Deterministic order for reporting and tests.
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

/// The lease window used to judge claim liveness. The freshness rule
/// mirrors the worker's reclaim policy (`stale_by_lease` in
/// `migration-worker/src/orchestrator.rs::scan_shards`): a claim is
/// stale only once `now - claimed_utc` exceeds the lease window.
/// migration-core's `DEFAULT_LEASE_TIMEOUT_SEC` is the protocol
/// default; the sweep has no access to a worker config that might
/// lengthen it, so operators running longer leases should wait it out
/// or use `--force` deliberately.
const LEASE: std::time::Duration =
    std::time::Duration::from_secs(migration_core::time::DEFAULT_LEASE_TIMEOUT_SEC);

/// List claims in the bucket and return the ones whose lease is still
/// live. READ-ONLY: list/get only, no writes.
pub async fn live_claims(
    store: &dyn ClaimStore,
    now: DateTime<Utc>,
) -> anyhow::Result<Vec<LiveClaim>> {
    let entries = store
        .list(migration_core::layout::SHARDS_PREFIX)
        .await
        .context("listing claims in the run bucket")?;
    let mut live = Vec::new();
    for entry in &entries {
        if migration_core::layout::shard_from_claim_key(&entry.key).is_none() {
            // Not a claim object (e.g. a stray shard upload).
            continue;
        }
        let Some((body, _etag)) = store
            .get(&entry.key)
            .await
            .with_context(|| format!("reading claim {}", entry.key))?
        else {
            // Deleted between LIST and GET — a completing worker; in
            // any case no longer a claim we could judge.
            continue;
        };
        let Ok(record) = serde_json::from_slice::<migration_core::records::ClaimRecord>(&body)
        else {
            // Unparseable claim record: we cannot prove the run is
            // dead, so it blocks deletion. (The worker likewise
            // treats an unparseable claim as needing an operator.)
            live.push(LiveClaim {
                key: entry.key.clone(),
                detail: "unparseable claim record".to_string(),
            });
            continue;
        };
        if !matches!(record.state, migration_core::records::ClaimState::Active) {
            // Completed/Failed are terminal — never live.
            continue;
        }
        let age = now.signed_duration_since(record.claimed_utc.0);
        // Same math as the worker's stale_by_lease: only a positive
        // age beyond the lease counts as expired. A future
        // `claimed_utc` (clock skew) fails the to_std() conversion
        // and is treated as live — conservative for deletion.
        let stale = age.to_std().map(|d| d > LEASE).unwrap_or(false);
        if !stale {
            live.push(LiveClaim {
                key: entry.key.clone(),
                detail: format!(
                    "host {}, age {}s, lease {}s",
                    record.host,
                    age.num_seconds(),
                    LEASE.as_secs(),
                ),
            });
        }
    }
    Ok(live)
}

fn format_live(live: &[LiveClaim]) -> String {
    live.iter()
        .map(|c| format!("  {} ({})\n", c.key, c.detail))
        .collect()
}

/// Plan the sweep, apply the liveness gate, and (when armed) delete.
///
/// Returns `Err` when deletion is armed but the liveness gate sees
/// live claims and `--force` was not given — the caller surfaces that
/// as a non-zero exit. Per-file deletion failures are counted in the
/// report instead (the caller decides the final exit status).
pub async fn sweep(
    store: &dyn ClaimStore,
    dest_root: &Path,
    opts: &SweepOptions,
    now: DateTime<Utc>,
) -> anyhow::Result<SweepReport> {
    let candidates = plan_sweep(dest_root)?;
    let live = live_claims(store, now).await?;

    if opts.delete && !opts.force && !live.is_empty() {
        bail!(
            "refusing to delete: {} live claim(s) say the run may still have \
             active workers (re-run with --force if the run is known dead):\n{}",
            live.len(),
            format_live(&live),
        );
    }

    let mut report = SweepReport {
        candidates,
        live,
        ..Default::default()
    };

    if opts.delete {
        for candidate in &report.candidates {
            // Re-check right before unlinking: only something that is
            // still a regular file goes away. `remove_file` has
            // unlink semantics (never follows a symlink), so even a
            // link swapped in after this check would itself be
            // unlinked, never its target.
            match std::fs::symlink_metadata(&candidate.path) {
                Ok(md) if md.is_file() => match std::fs::remove_file(&candidate.path) {
                    Ok(()) => {
                        report.deleted_files += 1;
                        report.deleted_bytes += candidate.size;
                    }
                    Err(e) => {
                        // Reported and skipped, not fatal; the caller
                        // exits non-zero at the end if any failed.
                        tracing::error!(
                            path = %candidate.path.display(),
                            error = %e,
                            "failed to delete partial",
                        );
                        report.failed_deletions += 1;
                    }
                },
                Ok(_) => {
                    tracing::warn!(
                        path = %candidate.path.display(),
                        "no longer a regular file since planning; skipped",
                    );
                }
                Err(e) => {
                    tracing::error!(
                        path = %candidate.path.display(),
                        error = %e,
                        "failed to stat partial before deletion",
                    );
                    report.failed_deletions += 1;
                }
            }
        }
    }

    Ok(report)
}

/// Entry point for `mig-aggr clean-partials`.
pub async fn run(
    endpoint: &str,
    region: &str,
    bucket: &str,
    dest_root: &Path,
    delete: bool,
    force: bool,
) -> anyhow::Result<()> {
    let store = S3Client::from_env(endpoint, region, bucket)
        .await
        .context("connecting to the run bucket")?;
    let opts = SweepOptions { delete, force };
    let report = sweep(&store, dest_root, &opts, Utc::now()).await?;

    if !report.live.is_empty() {
        // With --force the gate is overridden but still reported, so
        // the operator sees what it would have said.
        println!(
            "liveness gate: {} live claim(s){}:",
            report.live.len(),
            if force {
                " — overridden by --force"
            } else {
                ""
            },
        );
        print!("{}", format_live(&report.live));
    }

    let total_bytes: u64 = report.candidates.iter().map(|c| c.size).sum();
    for c in &report.candidates {
        let mtime = c
            .mtime
            .map(|t| DateTime::<Utc>::from(t).to_rfc3339_opts(SecondsFormat::Secs, true))
            .unwrap_or_else(|| "-".to_string());
        println!("{}\t{} bytes\tmtime {mtime}", c.path.display(), c.size);
    }

    if delete {
        println!(
            "deleted {} file(s), {} bytes",
            report.deleted_files, report.deleted_bytes,
        );
        if report.failed_deletions > 0 {
            bail!(
                "{} deletion(s) failed; see log output above",
                report.failed_deletions,
            );
        }
    } else {
        println!(
            "dry run: {} matching partial file(s), {total_bytes} bytes total; \
             re-run with --delete to remove them",
            report.candidates.len(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use migration_core::claim::test_util::FakeStore;
    use migration_core::layout;
    use migration_core::records::{ClaimRecord, ClaimState};
    use migration_core::time::UtcTime;

    /// Seed the fake bucket with a claim whose `claimed_utc` is
    /// `age_secs` in the past relative to `now`.
    async fn seed_claim(
        store: &FakeStore,
        shard: &str,
        host: &str,
        age_secs: i64,
        state: ClaimState,
        now: DateTime<Utc>,
    ) {
        let record = ClaimRecord {
            host: host.to_string(),
            claimed_utc: UtcTime(now - chrono::Duration::seconds(age_secs)),
            epoch: 1,
            state,
        };
        store
            .put_if_absent(
                &layout::claim_key(shard),
                serde_json::to_vec(&record).unwrap(),
            )
            .await
            .unwrap();
    }

    /// Acceptance test 1: the matcher pins the exact name shape that
    /// `migration-mover/src/paths.rs::partial_path` produces —
    /// `.<base>.<host>.<pid>.partial`, hidden dot-prefixed sibling,
    /// at least four dot-separated fields ending in `partial`, with a
    /// numeric `<pid>`. If `partial_path` ever changes shape, this
    /// test (and the literal below, lifted from its `nested_path`
    /// unit test) must change with it.
    #[test]
    fn partial_name_matcher_pins_mover_format() {
        assert!(is_partial_name(OsStr::new(
            ".data.bin.host-a.12345.partial"
        )));
        // Literal produced by `partial_path(b"/foo/bar.txt", "host-A", 123)`
        // → `/foo/.bar.txt.host-A.123.partial` (paths.rs `nested_path`).
        assert!(is_partial_name(OsStr::new(".bar.txt.host-A.123.partial")));

        // Not hidden — the mover always dot-prefixes.
        assert!(!is_partial_name(OsStr::new(
            "data.bin.host-a.12345.partial"
        )));
        // Non-numeric pid field.
        assert!(!is_partial_name(OsStr::new(".data.bin.host-a.pid.partial")));
        // Plain hidden file.
        assert!(!is_partial_name(OsStr::new(".hidden")));
        // Final (post-rename) name.
        assert!(!is_partial_name(OsStr::new("data.bin")));
    }

    /// Acceptance test 2: the default invocation (no `--delete`)
    /// reports matches and deletes nothing.
    #[tokio::test]
    async fn dry_run_deletes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let matching = root.join(".data.bin.host-a.12345.partial");
        let sub = root.join("sub");
        std::fs::create_dir(&sub).unwrap();
        let nested_matching = sub.join(".file.host-b.7.partial");
        let plain = root.join("data.bin");
        std::fs::write(&matching, b"abc").unwrap();
        std::fs::write(&nested_matching, b"defg").unwrap();
        std::fs::write(&plain, b"final").unwrap();

        let store = FakeStore::new(); // empty bucket → no live claims
        let opts = SweepOptions {
            delete: false,
            force: false,
        };
        let report = sweep(&store, root, &opts, Utc::now()).await.unwrap();

        let found: Vec<&Path> = report.candidates.iter().map(|c| c.path.as_path()).collect();
        assert!(found.contains(&matching.as_path()), "found: {found:?}");
        assert!(
            found.contains(&nested_matching.as_path()),
            "found: {found:?}"
        );
        assert_eq!(report.candidates.len(), 2, "found: {found:?}");
        assert_eq!(report.deleted_files, 0);
        assert_eq!(report.deleted_bytes, 0);

        // Nothing was touched.
        assert!(matching.exists());
        assert!(nested_matching.exists());
        assert!(plain.exists());
    }

    /// Acceptance test 3: with `--delete` armed and a dead store,
    /// matching regular files are removed; the final file, a decoy
    /// DIRECTORY whose name matches, and a symlink whose name matches
    /// all survive.
    #[tokio::test]
    async fn delete_removes_only_matches() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let matching = root.join(".a.txt.host-a.1.partial");
        let sub = root.join("sub");
        std::fs::create_dir(&sub).unwrap();
        let nested_matching = sub.join(".b.bin.host-a.2.partial");
        let plain = root.join("a.txt");
        std::fs::write(&matching, b"12345").unwrap();
        std::fs::write(&nested_matching, b"123").unwrap();
        std::fs::write(&plain, b"final").unwrap();

        // Decoy: a DIRECTORY whose name matches the pattern.
        let decoy_dir = root.join(".decoy.host-a.3.partial");
        std::fs::create_dir(&decoy_dir).unwrap();
        // Decoy: a symlink whose name matches the pattern.
        let decoy_link = root.join(".link.host-a.4.partial");
        std::os::unix::fs::symlink(&plain, &decoy_link).unwrap();

        // Dead store: one Active claim past the lease window, one
        // fresh but terminal (Completed) claim. Neither is live.
        let now = Utc::now();
        let store = FakeStore::new();
        let stale_age = (migration_core::time::DEFAULT_LEASE_TIMEOUT_SEC + 60) as i64;
        seed_claim(
            &store,
            "part-0000.parquet",
            "host-a",
            stale_age,
            ClaimState::Active,
            now,
        )
        .await;
        seed_claim(
            &store,
            "part-0001.parquet",
            "host-b",
            1,
            ClaimState::Completed,
            now,
        )
        .await;

        let opts = SweepOptions {
            delete: true,
            force: false,
        };
        let report = sweep(&store, root, &opts, now).await.unwrap();

        assert!(!matching.exists(), "matching partial must be deleted");
        assert!(
            !nested_matching.exists(),
            "nested matching partial must be deleted"
        );
        assert!(plain.exists(), "final file must survive");
        assert!(decoy_dir.is_dir(), "decoy directory must survive");
        assert!(
            decoy_link.symlink_metadata().is_ok(),
            "decoy symlink must survive"
        );

        assert_eq!(report.deleted_files, 2);
        assert_eq!(report.deleted_bytes, 5 + 3);
        assert_eq!(report.failed_deletions, 0);
    }

    /// Acceptance test 4: one live claim → the sweep refuses (Err →
    /// non-zero exit) and deletes nothing.
    #[tokio::test]
    async fn live_lease_blocks_delete() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let matching = root.join(".a.txt.host-a.1.partial");
        std::fs::write(&matching, b"12345").unwrap();

        let now = Utc::now();
        let store = FakeStore::new();
        seed_claim(
            &store,
            "part-0000.parquet",
            "host-a",
            5,
            ClaimState::Active,
            now,
        )
        .await;

        let opts = SweepOptions {
            delete: true,
            force: false,
        };
        let err = sweep(&store, root, &opts, now)
            .await
            .expect_err("live lease must block deletion");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("shards/part-0000.parquet.claim"),
            "error must print the live claims: {msg}"
        );
        assert!(matching.exists(), "nothing may be deleted when blocked");
    }

    /// Acceptance test 5: `--force --delete` with the same live store
    /// proceeds — and the gate's finding is still reported.
    #[tokio::test]
    async fn force_overrides_liveness() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let matching = root.join(".a.txt.host-a.1.partial");
        std::fs::write(&matching, b"12345").unwrap();

        let now = Utc::now();
        let store = FakeStore::new();
        seed_claim(
            &store,
            "part-0000.parquet",
            "host-a",
            5,
            ClaimState::Active,
            now,
        )
        .await;

        let opts = SweepOptions {
            delete: true,
            force: true,
        };
        let report = sweep(&store, root, &opts, now).await.unwrap();

        assert!(!matching.exists(), "forced deletion must proceed");
        assert_eq!(report.deleted_files, 1);
        assert_eq!(report.deleted_bytes, 5);
        assert!(
            !report.live.is_empty(),
            "the overridden gate's finding must still be reported"
        );
    }
}
