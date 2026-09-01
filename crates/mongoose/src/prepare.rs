//! `mongoose prepare` — build the local migration index: scan with the
//! compiled-in nfs-walker library, rewrite to canonical parquet shards
//! with the compiled-in mig-walker-rewrite library, and write the
//! local manifest. One binary, no external tools. Every stage
//! checkpoints under the work dir and re-running resumes.

use crate::cli::PrepareArgs;
use crate::manifest::{self, LocalManifest};
use crate::scan::{self, ScanParams};
use crate::util::{raise_fd_limit, utc_now};
use crate::workdir::{default_run_id, ensure_run_spec, normalize_root, RunSpec, WorkDir};
use anyhow::{Context, Result};
use migration_core::prepare_tools as tools;
use migration_core::records::{Endpoint, EndpointKind, MigrationOptions};

pub async fn run(args: &PrepareArgs) -> Result<LocalManifest> {
    // Root is required by the embedded scanner and the mover (reserved
    // ports for AUTH_SYS). SAFETY: geteuid has no preconditions.
    if unsafe { libc::geteuid() } != 0 {
        tracing::warn!("mongoose prepare is not running as root; NFS mounts usually need sudo");
    }
    raise_fd_limit();

    let wd = WorkDir::new(&args.work_dir);
    let source = Endpoint {
        kind: EndpointKind::Nfs,
        url: args.src.trim_end_matches('/').to_string(),
        root: normalize_root(&args.source_root),
    };
    let dest = Endpoint {
        kind: EndpointKind::Nfs,
        url: args.dst.trim_end_matches('/').to_string(),
        root: normalize_root(&args.dest_root),
    };
    // Refuse an overlapping source/destination before any long stage.
    migration_core::overlap::check(&source, &dest)?;

    std::fs::create_dir_all(wd.root())
        .with_context(|| format!("creating {}", wd.root().display()))?;
    let spec = ensure_run_spec(
        &wd.run_json(),
        RunSpec {
            run_id: args.run_id.clone().unwrap_or_else(default_run_id),
            created_utc: utc_now(),
            source,
            dest,
        },
    )?;

    println!(
        "mongoose prepare\n  run     {}\n  source  {}\n  dest    {}\n  work    {}\n",
        spec.run_id,
        tools::scan_url(&spec.source.url, &spec.source.root),
        tools::scan_url(&spec.dest.url, &spec.dest.root),
        wd.root().display()
    );

    if let Some(existing) = manifest::load(&wd)? {
        println!(
            "already prepared: {} ({} shards, {} rows). Nothing to do; run `mongoose copy \
             --work-dir {}` to migrate.",
            wd.manifest_json().display(),
            existing.shards.len(),
            existing.total_rows,
            wd.root().display()
        );
        return Ok(existing);
    }

    // ---- 1. scan ---------------------------------------------------
    println!("[1/3] scan");
    let scan = scan::ensure_scan(
        &wd,
        &ScanParams {
            scan_url: tools::scan_url(&spec.source.url, &spec.source.root),
            workers: args.walker_workers,
            shard_size_mb: args.shard_size_mb,
            exclude: args.exclude.clone(),
            scan_dir_override: args.scan_dir.clone(),
        },
    )
    .await?;
    println!(
        "  scan dir {}\n  walker   {}\n",
        scan.scan_dir.display(),
        scan.walker_version
    );

    // ---- 2. canonical rewrite --------------------------------------
    println!("[2/3] canonical rewrite");
    scan::ensure_canonical(&wd, &scan).await?;

    // ---- 3. verify shards and write the manifest -------------------
    println!("[3/3] verify shards and write manifest");
    let m = manifest::build(
        &wd,
        &spec.run_id,
        spec.source.clone(),
        spec.dest.clone(),
        MigrationOptions::default(),
    )?;
    if args.purge_intermediates {
        scan::purge_scan_output(&wd);
    }
    println!(
        "\nprepared: {} ({} shards, {} rows, {} bytes of shard index)\n\
         Copy with `mongoose copy --work-dir {}`.",
        wd.manifest_json().display(),
        m.shards.len(),
        m.total_rows,
        m.total_bytes,
        wd.root().display()
    );
    Ok(m)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::PrepareArgs;

    fn args(
        src: &str,
        dst: &str,
        src_root: &str,
        dst_root: &str,
        work: &std::path::Path,
    ) -> PrepareArgs {
        PrepareArgs {
            src: src.into(),
            dst: dst.into(),
            source_root: src_root.into(),
            dest_root: dst_root.into(),
            work_dir: work.to_path_buf(),
            walker_workers: 1,
            shard_size_mb: 1,
            exclude: vec![],
            scan_dir: None,
            run_id: None,
            purge_intermediates: false,
        }
    }

    #[tokio::test]
    async fn overlapping_source_and_dest_are_refused_before_any_work() {
        let dir = tempfile::tempdir().unwrap();
        // Identical export + dest root nested under source root: the
        // classic truncate-your-source misconfiguration.
        let a = args("nfs://h/export", "nfs://h/export", "/", "/dst", dir.path());
        let err = run(&a).await.unwrap_err();
        assert!(format!("{err:#}").contains("overlap"), "{err:#}");
        assert!(
            !dir.path().join("run.json").exists(),
            "refused before writing anything"
        );
    }
}
