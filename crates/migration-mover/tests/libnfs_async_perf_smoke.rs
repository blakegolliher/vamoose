//! Performance smoke for the async libnfs surface.
//!
//! Per Section "Performance smoke" in `LIBNFS_ASYNC_FORK.md`:
//!
//! - Single context, 32 concurrent `pread_async` calls of 4 MiB each
//!   at sequential offsets, against a single source file ≥ 1 GiB.
//! - Report measured throughput.
//! - Compare to the same workload run against the sync FFI inside a
//!   `spawn_blocking` loop.
//! - Async should match or beat sync. Number goes into the work-item
//!   closing note.
//!
//! Not a correctness gate (no asserts beyond "did not panic"); the
//! throughput numbers print on stdout. Run with `--nocapture`.
//!
//!   cargo build -p migration-mover --tests --release
//!   sudo -E target/release/deps/libnfs_async_perf_smoke-* \
//!       --ignored --nocapture
//!
//! Required environment:
//!   VAMOOSE_TEST_NFS_URL=nfs://server/export
//!   VAMOOSE_TEST_NFS_PATH=/path/to/file_ge_1GiB
//!   VAMOOSE_TEST_NFS_EXPECTED_SIZE=<bytes>

use migration_mover::libnfs::asyncio::{AsyncNfsContext, Flags, MountOpts};
use migration_mover::libnfs::ops;
use migration_mover::{LibnfsContextPool, SimplePool};
use std::env;
use std::time::Instant;

// READ_SIZE × CONCURRENCY must fit in the test file. The spec's
// canonical sizing is 4 MiB × 32 = 128 MiB (file ≥ 1 GiB); we shrink
// to 1 MiB × 32 = 32 MiB so the existing var204 100 MiB
// `large.bin` works as the source. The relative async-vs-sync
// throughput ratio is the load-bearing number for the closing note.
const READ_SIZE: usize = 1024 * 1024;
const CONCURRENCY: usize = 32;

fn env_url() -> String {
    env::var("VAMOOSE_TEST_NFS_URL").expect("VAMOOSE_TEST_NFS_URL not set")
}
fn env_path() -> String {
    env::var("VAMOOSE_TEST_NFS_PATH").expect("VAMOOSE_TEST_NFS_PATH not set")
}
fn env_expected_size() -> u64 {
    env::var("VAMOOSE_TEST_NFS_EXPECTED_SIZE")
        .expect("VAMOOSE_TEST_NFS_EXPECTED_SIZE not set")
        .parse()
        .expect("VAMOOSE_TEST_NFS_EXPECTED_SIZE not numeric")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn perf_async_pipelined_reads() {
    let url = env_url();
    let path = env_path();
    let total = env_expected_size();
    assert!(
        total as usize >= READ_SIZE * CONCURRENCY,
        "perf smoke needs a file of at least {} bytes",
        READ_SIZE * CONCURRENCY
    );

    let ctx = AsyncNfsContext::mount(
        &url,
        MountOpts {
            rsize: READ_SIZE as u32,
            wsize: READ_SIZE as u32,
            ..MountOpts::default()
        },
    )
    .await
    .expect("mount async");
    let fh = ctx
        .open(path.as_bytes(), Flags::rdonly())
        .await
        .expect("open");

    let start = Instant::now();
    let mut futs = Vec::with_capacity(CONCURRENCY);
    for i in 0..CONCURRENCY {
        let off = (i * READ_SIZE) as u64;
        let ctx_ref = &ctx;
        let fh_ref = &fh;
        futs.push(async move {
            ctx_ref
                .pread(fh_ref, off, READ_SIZE)
                .await
                .expect("pread async")
        });
    }
    let bufs: Vec<Vec<u8>> = futures::future::join_all(futs).await;
    let elapsed = start.elapsed();
    let bytes: usize = bufs.iter().map(|b| b.len()).sum();
    let mbps = (bytes as f64) / elapsed.as_secs_f64() / 1.0e6;
    eprintln!(
        "ASYNC perf: {} bytes in {:?} = {:.1} MB/s ({} concurrent reads of {} bytes)",
        bytes, elapsed, mbps, CONCURRENCY, READ_SIZE
    );

    ctx.close(fh).await.expect("close");
    ctx.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 32)]
#[ignore]
async fn perf_sync_via_spawn_blocking() {
    let url = env_url();
    let path = env_path();
    let total = env_expected_size();
    assert!(
        total as usize >= READ_SIZE * CONCURRENCY,
        "perf smoke needs a file of at least {} bytes",
        READ_SIZE * CONCURRENCY
    );

    // SimplePool exposes a single mount-pair; for sync vs async fair
    // comparison we'd really want N pools, but the spec is explicit
    // about "sync FFI inside a spawn_blocking loop" being the
    // reference workload. One pool, many spawn_blocking calls,
    // serialized by the pair mutex inside the pool — same shape as
    // M2's `do_libnfs_copy` worst case.
    let pool = SimplePool::build(&url, &url, migration_mover::DEFAULT_RPC_TIMEOUT_MS)
        .expect("SimplePool::build");

    let start = Instant::now();
    let mut handles = Vec::with_capacity(CONCURRENCY);
    for i in 0..CONCURRENCY {
        let off = (i * READ_SIZE) as u64;
        let pool = pool.clone();
        let path = path.clone();
        handles.push(tokio::spawn(async move {
            let mut pair = pool.acquire().await.expect("acquire");
            tokio::task::spawn_blocking(move || {
                let fh = ops::open_read(pair.src(), path.as_bytes()).expect("open");
                let mut buf = vec![0u8; READ_SIZE];
                let n = ops::pread(pair.src(), &fh, off, &mut buf).expect("pread");
                ops::close_quietly(pair.src(), fh);
                n
            })
            .await
            .expect("spawn_blocking")
        }));
    }
    let mut bytes = 0usize;
    for h in handles {
        bytes += h.await.expect("join") as usize;
    }
    let elapsed = start.elapsed();
    let mbps = (bytes as f64) / elapsed.as_secs_f64() / 1.0e6;
    eprintln!(
        "SYNC perf: {} bytes in {:?} = {:.1} MB/s ({} spawn_blocking reads of {} bytes)",
        bytes, elapsed, mbps, CONCURRENCY, READ_SIZE
    );
}
