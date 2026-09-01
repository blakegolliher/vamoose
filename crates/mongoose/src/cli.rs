//! Command-line surface: `mongoose prepare | copy | run`.

use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "mongoose",
    version,
    about = "Single-host NFS-to-NFS data mover (scan + canonical shards + libnfs copy, all local)",
    long_about = "mongoose migrates one NFS export to another from a single host.\n\
                  `prepare` scans the source (nfs-walker), rewrites the scan into canonical\n\
                  parquet shards (mig-walker-rewrite), and writes a local manifest.\n\
                  `copy` processes those shards with the vamoose libnfs mover.\n\
                  `run` does both.\n\n\
                  Requires root (libnfs binds reserved ports for AUTH_SYS). NFSv3 only.\n\
                  One-pass migration: no delta/incremental comparison, no distributed\n\
                  execution, no S3."
)]
pub struct Cli {
    /// Log verbosity. Default is compact: warnings plus mongoose's own
    /// stage lines and progress ticks. -v restores full INFO from the
    /// embedded walker/rewrite/mover engines; -vv enables debug.
    /// RUST_LOG, when set, overrides this flag entirely.
    #[arg(short = 'v', long = "verbose", action = clap::ArgAction::Count, global = true)]
    pub verbose: u8,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Scan the source and build the local migration index (canonical
    /// shards + manifest.json) under --work-dir. Resumable: re-running
    /// picks up after the last completed stage.
    Prepare(PrepareArgs),
    /// Copy using an index prepared under --work-dir. Shards are
    /// processed sequentially; files within a shard copy concurrently.
    /// Resumable at shard granularity (completed shards are skipped).
    Copy(CopyArgs),
    /// Prepare, then copy.
    Run(RunArgs),
    /// One converging resync pass: rescan the source, classify
    /// changes against the previous pass, and copy only the delta.
    /// Repeat while the source is live; finish with --cutover after
    /// stopping source writers.
    Sync(SyncArgs),
}

#[derive(Args, Debug)]
pub struct PrepareArgs {
    /// Source export URL, e.g. nfs://source.example.com/export
    #[arg(long)]
    pub src: String,

    /// Destination export URL, e.g. nfs://dest.example.com/export
    #[arg(long)]
    pub dst: String,

    /// Path inside the source export to migrate ("/" = whole export).
    #[arg(long, default_value = "/")]
    pub source_root: String,

    /// Path inside the destination export to copy into.
    #[arg(long, default_value = "/")]
    pub dest_root: String,

    /// Local directory holding this run's index, checkpoints, and
    /// results. One migration per work dir.
    #[arg(long)]
    pub work_dir: PathBuf,

    /// nfs-walker GETATTR workers.
    #[arg(long, default_value_t = 32)]
    pub walker_workers: usize,

    /// Target size of each canonical parquet shard.
    #[arg(long, default_value_t = 512)]
    pub shard_size_mb: u64,

    /// Directory glob to exclude from the scan (repeatable).
    #[arg(long)]
    pub exclude: Vec<String>,

    /// Use an existing nfs-walker output directory instead of scanning.
    #[arg(long, value_name = "DIR")]
    pub scan_dir: Option<PathBuf>,

    /// Run identifier recorded in run.json and manifest.json
    /// (default: run-<UTC timestamp>).
    #[arg(long)]
    pub run_id: Option<String>,

    /// Delete the raw walker scan output once the canonical shards
    /// are committed. The scan is a pure intermediate that doubles
    /// the index footprint; canonical shards and the manifest are
    /// always kept. A --scan-dir outside the work dir is never
    /// touched.
    #[arg(long)]
    pub purge_intermediates: bool,
}

#[derive(Args, Debug)]
pub struct CopyArgs {
    /// Work dir holding a prepared index (see `mongoose prepare`).
    #[arg(long)]
    pub work_dir: PathBuf,

    #[command(flatten)]
    pub tuning: CopyTuning,
}

/// Mover tuning shared by `copy` and `run`.
#[derive(Args, Debug, Clone)]
pub struct CopyTuning {
    /// libnfs context pairs to pre-mount. Each pair costs two
    /// reserved ports; ~111 pairs is the observed per-host ceiling.
    #[arg(long, default_value_t = 32)]
    pub nfs_connections: u32,

    /// Concurrent in-flight files < 1 MiB.
    #[arg(long, default_value_t = 256)]
    pub inflight_small: usize,

    /// Concurrent in-flight files 1 MiB – 1 GiB.
    #[arg(long, default_value_t = 16)]
    pub inflight_medium: usize,

    /// Concurrent in-flight files > 1 GiB.
    #[arg(long, default_value_t = 4)]
    pub inflight_large: usize,

    /// Copy regular files through the raw NFSv3 filehandle fast path
    /// (~5 RPCs per small file instead of ~60-80).
    #[arg(long)]
    pub use_raw_fh: bool,

    /// Raw-FH path only: CREATE under the final name and skip the
    /// atomic `.partial` + RENAME publish. Faster, but a crash can
    /// leave a torn file visible; only safe while nothing consumes
    /// the destination.
    #[arg(long)]
    pub direct_commit: bool,

    /// Use the bucketed async libnfs pool for regular-file copies
    /// (opt-in; the sync MultiPool remains the default).
    #[arg(long)]
    pub bucketed_async: bool,

    /// Per-RPC libnfs timeout in milliseconds (0 = libnfs default).
    #[arg(long, default_value_t = migration_mover::DEFAULT_RPC_TIMEOUT_MS)]
    pub rpc_timeout_ms: u32,
}

#[derive(Args, Debug)]
pub struct RunArgs {
    #[command(flatten)]
    pub prepare: PrepareArgs,

    #[command(flatten)]
    pub tuning: CopyTuning,
}

#[derive(Args, Debug)]
pub struct SyncArgs {
    /// Work dir of a completed migration (see `mongoose run`).
    #[arg(long)]
    pub work_dir: PathBuf,

    #[command(flatten)]
    pub tuning: CopyTuning,

    /// nfs-walker GETATTR workers for the rescan.
    #[arg(long, default_value_t = 32)]
    pub walker_workers: usize,

    /// Target size of each canonical parquet shard for the rescan.
    #[arg(long, default_value_t = 512)]
    pub shard_size_mb: u64,

    /// Directory glob to exclude from the rescan (repeatable).
    #[arg(long)]
    pub exclude: Vec<String>,

    /// Use an existing nfs-walker output directory instead of
    /// rescanning.
    #[arg(long, value_name = "DIR")]
    pub scan_dir: Option<PathBuf>,

    /// Cutover verification: source writers must be stopped. Fails
    /// loudly if the classifier finds any drift (new, dirty, pending,
    /// or deleted rows); zero drift means the trees have converged.
    #[arg(long)]
    pub cutover: bool,

    /// With --cutover: copy the drift instead of failing on it.
    #[arg(long, requires = "cutover")]
    pub cutover_allow_drift: bool,

    /// Completed pass dirs to retain (older ones are pruned after the
    /// baseline advances).
    #[arg(long, default_value_t = 2)]
    pub keep_passes: u32,

    /// After the baseline advances, delete this pass's raw walker
    /// scan output and its delta shards. The pass's canonical shards
    /// are always kept — they are the next sync's baseline. A
    /// --scan-dir outside the work dir is never touched.
    #[arg(long)]
    pub purge_intermediates: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn prepare_parses_the_documented_invocation() {
        let cli = Cli::try_parse_from([
            "mongoose",
            "prepare",
            "--src",
            "nfs://source.example.com/export",
            "--dst",
            "nfs://dest.example.com/export",
            "--source-root",
            "/",
            "--dest-root",
            "/",
            "--work-dir",
            "/var/lib/mongoose/run-001",
            "--walker-workers",
            "32",
            "--shard-size-mb",
            "512",
            "--exclude",
            ".snapshot",
        ])
        .unwrap();
        let Command::Prepare(args) = cli.command else {
            panic!("expected prepare");
        };
        assert_eq!(args.src, "nfs://source.example.com/export");
        assert_eq!(args.dst, "nfs://dest.example.com/export");
        assert_eq!(args.work_dir, PathBuf::from("/var/lib/mongoose/run-001"));
        assert_eq!(args.walker_workers, 32);
        assert_eq!(args.shard_size_mb, 512);
        assert_eq!(args.exclude, vec![".snapshot".to_string()]);
    }

    #[test]
    fn copy_parses_the_documented_invocation() {
        let cli = Cli::try_parse_from([
            "mongoose",
            "copy",
            "--work-dir",
            "/var/lib/mongoose/run-001",
            "--nfs-connections",
            "32",
            "--inflight-small",
            "256",
            "--inflight-medium",
            "16",
            "--inflight-large",
            "4",
            "--use-raw-fh",
        ])
        .unwrap();
        let Command::Copy(args) = cli.command else {
            panic!("expected copy");
        };
        assert_eq!(args.tuning.nfs_connections, 32);
        assert_eq!(args.tuning.inflight_small, 256);
        assert!(args.tuning.use_raw_fh);
        assert!(!args.tuning.direct_commit, "direct commit is opt-in");
        assert!(!args.tuning.bucketed_async, "bucketed async is opt-in");
    }

    #[test]
    fn run_parses_prepare_and_copy_flags_together() {
        let cli = Cli::try_parse_from([
            "mongoose",
            "run",
            "--src",
            "nfs://s/e",
            "--dst",
            "nfs://d/e",
            "--work-dir",
            "/w",
            "--nfs-connections",
            "48",
            "--use-raw-fh",
        ])
        .unwrap();
        let Command::Run(args) = cli.command else {
            panic!("expected run");
        };
        assert_eq!(args.prepare.src, "nfs://s/e");
        assert_eq!(args.prepare.source_root, "/", "root defaults to /");
        assert_eq!(args.tuning.nfs_connections, 48);
        assert!(args.tuning.use_raw_fh);
    }

    #[test]
    fn prepare_requires_src_dst_and_work_dir() {
        assert!(Cli::try_parse_from(["mongoose", "prepare", "--src", "nfs://s/e"]).is_err());
        assert!(Cli::try_parse_from(["mongoose", "copy"]).is_err());
    }

    #[test]
    fn verbosity_counts_and_is_global() {
        let cli = Cli::try_parse_from(["mongoose", "copy", "--work-dir", "/w", "-vv"]).unwrap();
        assert_eq!(cli.verbose, 2);
        let cli = Cli::try_parse_from(["mongoose", "sync", "--work-dir", "/w"]).unwrap();
        assert_eq!(cli.verbose, 0, "compact by default");
    }

    #[test]
    fn purge_intermediates_parses_on_run_and_sync() {
        let cli = Cli::try_parse_from([
            "mongoose",
            "run",
            "--src",
            "nfs://s/e",
            "--dst",
            "nfs://d/e",
            "--work-dir",
            "/w",
            "--purge-intermediates",
        ])
        .unwrap();
        let Command::Run(args) = cli.command else {
            panic!("expected run");
        };
        assert!(args.prepare.purge_intermediates);

        let cli = Cli::try_parse_from([
            "mongoose",
            "sync",
            "--work-dir",
            "/w",
            "--purge-intermediates",
        ])
        .unwrap();
        let Command::Sync(args) = cli.command else {
            panic!("expected sync");
        };
        assert!(args.purge_intermediates);
        assert!(!args.cutover);
    }
}
