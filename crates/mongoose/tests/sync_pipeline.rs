//! End-to-end resync pipeline over fixture data, no NFS: a synthetic
//! walker scan is rewritten by the real `mig_walker_rewrite` library,
//! a second scan with churn is classified against it, the delta
//! shards are emitted, and the result is read back through the same
//! `ShardReader` the copy loop uses.

use arrow::array::{
    ArrayRef, Int32Builder, Int64Builder, StringBuilder, UInt16Builder, UInt32Builder,
    UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use migration_core::shard::ShardReader;
use mongoose::manifest;
use mongoose::progress::CopyProgress;
use mongoose::workdir::WorkDir;
use parquet::arrow::ArrowWriter;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

/// One source row as the walker would report it.
struct WalkerRow {
    path: &'static str,
    file_type: &'static str,
    size: u64,
    mtime_us: i64,
    ctime_us: i64,
}

fn wrow(path: &'static str, size: u64, mtime_us: i64, ctime_us: i64) -> WalkerRow {
    WalkerRow {
        path,
        file_type: "file",
        size,
        mtime_us,
        ctime_us,
    }
}

/// Write one walker-schema parquet part with the columns
/// `mig_walker_rewrite::translate_batch` requires, plus the
/// `ctime_*` columns that ride the legacy passthrough into the
/// canonical shards (what the classifier's tuple reads).
fn write_walker_part(path: &Path, rows: &[WalkerRow]) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("file_type", DataType::Utf8, false),
        Field::new("permissions", DataType::UInt16, false),
        Field::new("mtime_us", DataType::Int64, true),
        Field::new("ctime_sec", DataType::Int64, true),
        Field::new("ctime_nsec", DataType::Int32, true),
        Field::new("inode", DataType::UInt64, false),
        Field::new("nlink", DataType::UInt32, false),
        Field::new("uid", DataType::UInt32, false),
        Field::new("gid", DataType::UInt32, false),
        Field::new("size", DataType::UInt64, false),
    ]));
    let mut path_b = StringBuilder::new();
    let mut ft_b = StringBuilder::new();
    let mut perm_b = UInt16Builder::new();
    let mut mtime_b = Int64Builder::new();
    let mut ct_s = Int64Builder::new();
    let mut ct_n = Int32Builder::new();
    let mut inode_b = UInt64Builder::new();
    let mut nlink_b = UInt32Builder::new();
    let mut uid_b = UInt32Builder::new();
    let mut gid_b = UInt32Builder::new();
    let mut size_b = UInt64Builder::new();
    for (i, r) in rows.iter().enumerate() {
        path_b.append_value(r.path);
        ft_b.append_value(r.file_type);
        perm_b.append_value(0o644);
        mtime_b.append_value(r.mtime_us);
        ct_s.append_value(r.ctime_us.div_euclid(1_000_000));
        ct_n.append_value((r.ctime_us.rem_euclid(1_000_000) * 1000) as i32);
        inode_b.append_value(1000 + i as u64);
        nlink_b.append_value(1);
        uid_b.append_value(0);
        gid_b.append_value(0);
        size_b.append_value(r.size);
    }
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(path_b.finish()),
        Arc::new(ft_b.finish()),
        Arc::new(perm_b.finish()),
        Arc::new(mtime_b.finish()),
        Arc::new(ct_s.finish()),
        Arc::new(ct_n.finish()),
        Arc::new(inode_b.finish()),
        Arc::new(nlink_b.finish()),
        Arc::new(uid_b.finish()),
        Arc::new(gid_b.finish()),
        Arc::new(size_b.finish()),
    ];
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut w = ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

fn endpoint(url: &str) -> migration_core::records::Endpoint {
    migration_core::records::Endpoint {
        kind: migration_core::records::EndpointKind::Nfs,
        url: url.into(),
        root: "/".into(),
    }
}

/// Run the real rewrite over a fixture scan dir into `wd`, then build
/// the local manifest — exactly what prepare/sync do per pass.
fn rewrite_and_build(wd: &WorkDir, scan_dir: &Path, run_id: &str) -> manifest::LocalManifest {
    std::fs::create_dir_all(wd.canonical_dir()).unwrap();
    mig_walker_rewrite::run_rewrite(&mig_walker_rewrite::Cli {
        input: scan_dir.to_path_buf(),
        output: wd.canonical_dir(),
        source_root: "/".into(),
        walker_version: "nfs-walker test (fixture)".into(),
        resume: true,
        report: Some(wd.rewrite_json()),
        verbose: false,
    })
    .unwrap();
    manifest::build(
        wd,
        run_id,
        endpoint("nfs://s/e"),
        endpoint("nfs://d/e"),
        migration_core::records::MigrationOptions::default(),
    )
    .unwrap()
}

#[test]
fn resync_pipeline_classifies_and_emits_a_copyable_delta() {
    let tmp = tempfile::tempdir().unwrap();
    let root = WorkDir::new(tmp.path());

    // ---- Pass 0: the original tree --------------------------------
    let scan1 = tmp.path().join("fixture-scan-1");
    write_walker_part(
        &scan1.join("part-r00-00000.parquet"),
        &[
            wrow("/a", 10, 1_000_000, 1_000_000),
            wrow("/b", 10, 1_000_000, 1_000_000),
            wrow("/c", 10, 1_000_000, 1_000_000),
            wrow("/d", 10, 1_000_000, 1_000_000),
            wrow("/e", 10, 1_000_000, 1_000_000),
        ],
    );
    let m0 = rewrite_and_build(&root, &scan1, "run-sync-test");
    assert_eq!(m0.total_rows, 5);

    // Mark pass 0 as fully copied so a sync would be admissible.
    let mut p = CopyProgress::fresh("run-sync-test", m0.shards.len() as u64);
    for s in &m0.shards {
        p.completed_shards.push(s.path.clone());
    }
    p.write(&root).unwrap();

    // ---- Pass 1: churn --------------------------------------------
    // /b rewritten (mtime+ctime move), /d deleted, /f new, /e failed
    // last pass (pending). /a and /c untouched.
    let scan2 = tmp.path().join("fixture-scan-2");
    write_walker_part(
        &scan2.join("part-r00-00000.parquet"),
        &[
            wrow("/a", 10, 1_000_000, 1_000_000),
            wrow("/b", 12, 2_000_000, 2_000_000),
            wrow("/c", 10, 1_000_000, 1_000_000),
            wrow("/e", 10, 1_000_000, 1_000_000),
            wrow("/f", 10, 1_000_000, 1_000_000),
        ],
    );
    let pass_wd = WorkDir::new(root.pass_dir(1));
    std::fs::create_dir_all(pass_wd.root()).unwrap();
    let m1 = rewrite_and_build(&pass_wd, &scan2, "run-sync-test");
    assert_eq!(m1.total_rows, 5);

    // ---- Classify --------------------------------------------------
    let pending: HashSet<Vec<u8>> = [b"/e".to_vec()].into_iter().collect();
    let base_shards: Vec<_> = m0.shards.iter().map(|s| root.shard_path(&s.path)).collect();
    let cur_shards: Vec<_> = m1
        .shards
        .iter()
        .map(|s| pass_wd.shard_path(&s.path))
        .collect();
    let counts = migration_resync::classify(
        &base_shards,
        &cur_shards,
        &pending,
        &pass_wd.classify_dir(),
        8,
    )
    .unwrap();
    assert_eq!(counts.new, 1, "/f");
    assert_eq!(counts.dirty_tuple, 1, "/b");
    assert_eq!(counts.dirty_pending, 1, "/e");
    assert_eq!(counts.unchanged, 2, "/a /c");
    assert_eq!(counts.deleted, 1, "/d");
    assert_eq!(counts.keep_rows, 3);

    // ---- Emit the delta and read it back like the copy loop -------
    let delta = mongoose::delta::emit(&pass_wd, &m1)
        .unwrap()
        .expect("non-empty delta");
    assert_eq!(delta.shards.len(), 1);
    assert_eq!(delta.total_rows, 3);
    let delta_path = pass_wd.shard_path(&delta.shards[0].path);

    let reader = ShardReader::open(&delta_path).expect("delta shard passes ShardReader validation");
    assert_eq!(reader.rows(), 3);
    assert_eq!(reader.shard_index(), Some(0), "KV shard_index preserved");
    let mut paths: Vec<String> = reader
        .into_rows()
        .unwrap()
        .map(|r| String::from_utf8(r.unwrap().path).unwrap())
        .collect();
    paths.sort();
    assert_eq!(paths, vec!["/b", "/e", "/f"]);

    // The delta manifest is loadable exactly like a copy manifest.
    let loaded = manifest::load_file(&pass_wd.delta_manifest_json())
        .unwrap()
        .expect("delta manifest written");
    assert_eq!(loaded.total_rows, 3);
    assert_eq!(loaded.shards[0].path, delta.shards[0].path);

    // An identical rescan classifies clean with no pending: cutover
    // convergence.
    let scan3 = tmp.path().join("fixture-scan-3");
    write_walker_part(
        &scan3.join("part-r00-00000.parquet"),
        &[
            wrow("/a", 10, 1_000_000, 1_000_000),
            wrow("/b", 12, 2_000_000, 2_000_000),
            wrow("/c", 10, 1_000_000, 1_000_000),
            wrow("/e", 10, 1_000_000, 1_000_000),
            wrow("/f", 10, 1_000_000, 1_000_000),
        ],
    );
    let pass2_wd = WorkDir::new(root.pass_dir(2));
    std::fs::create_dir_all(pass2_wd.root()).unwrap();
    let m2 = rewrite_and_build(&pass2_wd, &scan3, "run-sync-test");
    let cur2: Vec<_> = m2
        .shards
        .iter()
        .map(|s| pass2_wd.shard_path(&s.path))
        .collect();
    let counts2 = migration_resync::classify(
        &cur_shards,
        &cur2,
        &HashSet::new(),
        &pass2_wd.classify_dir(),
        8,
    )
    .unwrap();
    assert_eq!(counts2.keep_rows + counts2.deleted, 0, "converged");
    assert_eq!(counts2.unchanged, 5);
    assert!(
        mongoose::delta::emit(&pass2_wd, &m2).unwrap().is_none(),
        "empty delta emits nothing"
    );
}
