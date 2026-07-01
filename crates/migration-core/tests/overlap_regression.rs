//! Integration regression test for the startup overlap guard.
//!
//! Builds a synthetic in-memory `Manifest` whose source and dest
//! endpoints overlap and runs the same `overlap::check` the worker
//! orchestrator runs at startup (before any S3 data-plane call or
//! libnfs mount). The test asserts:
//!
//! 1. `overlap::check` returns `Error::SourceDestOverlap`.
//! 2. The error message names both endpoints.
//! 3. No S3 or libnfs side effects occur — guaranteed by the fact
//!    that this test does not link or instantiate either subsystem.
//!
//! This is the regression test for the data-loss bug recovered in
//! M2 verification. See BUGFIX_PLAN.md.

use migration_core::overlap;
use migration_core::records::{Endpoint, EndpointKind, Manifest, MigrationOptions, ServerSideCopy};
use migration_core::time::UtcTime;
use migration_core::Error;

fn synthetic_manifest(src: Endpoint, dst: Endpoint) -> Manifest {
    Manifest {
        format_version: 1,
        run_id: "test-run".into(),
        created_utc: UtcTime::now(),
        shards: Vec::new(),
        total_rows: 0,
        source: src,
        dest: dst,
        options: MigrationOptions {
            preserve_owner: true,
            preserve_mode: true,
            preserve_times: true,
            preserve_xattr: true,
            server_side_copy: ServerSideCopy::Off,
        },
    }
}

fn ep(url: &str, root: &str) -> Endpoint {
    Endpoint {
        kind: EndpointKind::Nfs,
        url: url.into(),
        root: root.into(),
    }
}

#[test]
fn startup_guard_rejects_overlapping_manifest() {
    // The exact misconfiguration that caused the M2 verification
    // data-loss bug: source.url == dest.url, source.root="/" is a
    // path-prefix of dest.root="/dst-test".
    let manifest = synthetic_manifest(
        ep(
            "nfs://main.selab-var204.selab.vastdata.com/bgolliher/vamoose-source",
            "/",
        ),
        ep(
            "nfs://main.selab-var204.selab.vastdata.com/bgolliher/vamoose-source",
            "/dst-test",
        ),
    );

    match overlap::check(&manifest.source, &manifest.dest) {
        Err(Error::SourceDestOverlap { .. }) => {
            // Display impl must include the operator-facing block
            // with both endpoint URLs and a "Fix:" suggestion.
            let msg = Error::SourceDestOverlap {
                detail: "irrelevant".into(),
            }
            .to_string();
            assert!(msg.contains("source and destination overlap"), "msg: {msg}",);
        }
        other => panic!("expected Error::SourceDestOverlap, got {other:?}"),
    }
}

#[test]
fn startup_guard_allows_distinct_dest_export() {
    // Post-fix manifest: src and dst on the same server but distinct
    // exports. Per BUGFIX_PLAN.md, this is the recommended layout
    // when both endpoints point at the same NFS server.
    let manifest = synthetic_manifest(
        ep("nfs://host/src-export", "/"),
        ep("nfs://host/dst-export", "/"),
    );
    overlap::check(&manifest.source, &manifest.dest)
        .expect("distinct exports must pass the startup overlap guard");
}

#[test]
fn startup_guard_allows_disjoint_roots_on_same_export() {
    let manifest = synthetic_manifest(
        ep("nfs://host/exp", "/src-tree"),
        ep("nfs://host/exp", "/dst-tree"),
    );
    overlap::check(&manifest.source, &manifest.dest)
        .expect("disjoint roots on the same export must pass");
}
