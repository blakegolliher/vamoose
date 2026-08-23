//! F13 acceptance tests — `docs/work-items/
//! WORKER_ERROR_CLASSIFICATION.md`.
//!
//! The orchestrator used to treat **every** `processor.process()`
//! error as shard-fatal and write a terminal `Failed` claim. That is
//! only correct for errors that are a property of the shard bytes
//! (corrupt parquet, undecodable rows). Errors that are a property of
//! the *worker* — a stale binary's `SchemaVersionMismatch`, scratch
//! I/O failures, S3 hiccups — must instead release the claim (via the
//! worker's own delete-if-match) and skip the shard locally, so a
//! healthy peer can process it.
//!
//! Tests 1–2 were written first and observed red (the classifier and
//! handler did not exist). Row-level per-file failures are NOT routed
//! through this classifier — they stay in the failure-sink path,
//! pinned by the existing `record_outcome_*` tests in
//! `src/shard_processor.rs`.

// F42 transient-retry acceptance tests live in a submodule of this
// binary rather than their own tests/*.rs file: every top-level test
// file links a separate full-workspace debug executable, and one
// binary too many has tipped CI runners into linker SIGBUS (disk
// exhaustion).
#[path = "worker_error_classification/lease_liveness.rs"]
mod lease_liveness;
#[path = "worker_error_classification/transient_retry.rs"]
mod transient_retry;

use std::collections::HashSet;
use std::time::Duration;

use migration_core::claim::test_util::{FakeStore, OpKind};
use migration_core::claim::{self, AcquireOutcome, ClaimStore, CompleteOutcome};
use migration_core::errors::Error;
use migration_core::layout;
use migration_core::records::{
    ClaimRecord, ClaimState, Endpoint, EndpointKind, Manifest, MigrationOptions, ShardEntry,
    RUN_FORMAT_VERSION,
};
use migration_core::time::UtcTime;
use migration_worker::heartbeat::HeldClaim;
use migration_worker::orchestrator::{
    classify_shard_error, handle_process_error, scan_shards, ClaimBodyCache, ProcessErrorContext,
    ShardErrorClass,
};
use tokio::sync::Mutex;

const HB_SEC: u64 = 30;

fn shard_name(i: usize) -> String {
    format!("part-{i:04}.parquet")
}

fn manifest_with_shards(n: usize) -> Manifest {
    Manifest {
        format_version: RUN_FORMAT_VERSION,
        run_id: "f13-error-classification".into(),
        created_utc: UtcTime::now(),
        shards: (1..=n)
            .map(|i| ShardEntry {
                key: format!("index/{}", shard_name(i)),
                rows: 10,
                bytes: 4096,
                // F40: fixture etags must be non-empty — production
                // code no longer skips verification on empty etags,
                // and no fixture may rely on that bypass.
                etag: format!("etag-fixture-{i:04}"),
            })
            .collect(),
        total_rows: (n as u64) * 10,
        source: Endpoint {
            kind: EndpointKind::Nfs,
            url: "nfs://src/export".into(),
            root: "/".into(),
        },
        dest: Endpoint {
            kind: EndpointKind::Nfs,
            url: "nfs://dst/export".into(),
            root: "/".into(),
        },
        options: MigrationOptions::default(),
    }
}

/// Acquire `shard` as `host` on the mock store, returning the held
/// claim cell + ownership proof the orchestrator would hold at the
/// point `processor.process()` errors.
async fn acquire_held(
    store: &FakeStore,
    shard: &str,
    host: &str,
) -> (Mutex<Option<HeldClaim>>, String, u64) {
    let AcquireOutcome::Acquired { etag, record } = claim::try_acquire(store, shard, host)
        .await
        .expect("acquire")
    else {
        panic!("first acquire of {shard} must succeed");
    };
    let cell = Mutex::new(Some(HeldClaim {
        shard: shard.to_string(),
        etag: etag.clone(),
        epoch: record.epoch,
    }));
    (cell, etag, record.epoch)
}

fn worker_local_io_error() -> anyhow::Error {
    // Local scratch EIO while reading the downloaded parquet.
    anyhow::Error::from(Error::Io(std::io::Error::other(
        "EIO reading local scratch parquet",
    )))
}

fn claim_state(store_body: &[u8]) -> ClaimState {
    serde_json::from_slice::<ClaimRecord>(store_body)
        .expect("claim record parses")
        .state
}

// ---------------------------------------------------------------------------
// Test 1 (red before fix): pure classifier table.
// ---------------------------------------------------------------------------

/// One row per error kind that can flow out of the shard-processing
/// path (plus the conservative defaults). Shard-fatal = a property of
/// the shard bytes, recurs for any worker; worker-local = a property
/// of this worker's binary/host/network, a healthy peer succeeds.
#[test]
fn classifier_table() {
    use ShardErrorClass::{Fatal, WorkerLocal};

    let cases: Vec<(Error, ShardErrorClass, &str)> = vec![
        // -- shard-fatal: the shard itself is bad ----------------------
        (
            Error::ShardCorrupt {
                shard: "part-0001.parquet".into(),
                source: anyhow::anyhow!("bad magic bytes"),
            },
            Fatal,
            "corrupt parquet (ShardCorrupt)",
        ),
        (
            Error::Parquet(parquet::errors::ParquetError::General(
                "footer decode failed".into(),
            )),
            Fatal,
            "parquet decode error",
        ),
        (
            Error::Arrow(arrow::error::ArrowError::ParseError(
                "record batch decode".into(),
            )),
            Fatal,
            "arrow decode error",
        ),
        (
            Error::CorruptRow {
                row_id: 42,
                reason: "file_type=0 (Unknown) forbidden".into(),
            },
            Fatal,
            "contract-violating row",
        ),
        (
            Error::MissingColumn("path"),
            Fatal,
            "required column missing from shard schema",
        ),
        (
            Error::Other(anyhow::anyhow!("column size: unexpected arrow type")),
            Fatal,
            "untyped decode-adjacent error (conservative default)",
        ),
        // -- worker-local: this worker is the problem ------------------
        (
            Error::SchemaVersionMismatch {
                expected: 1,
                actual: 2,
            },
            WorkerLocal,
            "stale worker binary vs shard format_version",
        ),
        (
            Error::Io(std::io::Error::other("EIO on local scratch")),
            WorkerLocal,
            "local scratch I/O error",
        ),
        (
            Error::S3(aws_sdk_s3::Error::NoSuchKey(
                aws_sdk_s3::types::error::NoSuchKey::builder().build(),
            )),
            WorkerLocal,
            "S3 download error",
        ),
        (
            Error::PreconditionFailed,
            WorkerLocal,
            "claim-plane contention is never shard-fatal",
        ),
        (
            Error::ClaimInvalidated("heartbeat lost".into()),
            WorkerLocal,
            "self-fence signal is a worker property",
        ),
        (
            Error::ManifestChanged {
                expected: "e1".into(),
                actual: "e2".into(),
            },
            WorkerLocal,
            "manifest swap: worker exits, shard is fine",
        ),
    ];

    for (err, want, why) in cases {
        let got = classify_shard_error(&err);
        assert_eq!(
            got, want,
            "{why}: {err:?} classified {got:?}, want {want:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// F12: RPC-timeout-shaped errors are transient/worker-local, never
// shard corruption (docs/work-items/PROTECTED_FFI_BATCH.md, Item 1
// step 3).
// ---------------------------------------------------------------------------

/// A libnfs RPC timeout surfacing as an untyped whole-shard error
/// must land in the retryable/WorkerLocal bucket: a timeout says
/// nothing about the shard bytes — any healthy peer (or this worker
/// a moment later) can process the shard. libnfs surfaces timeouts
/// as the nfs-level callback string `"Command timed out"` (err =
/// -EINTR; `lib/nfs_v3.c:check_nfs3_error`) and the rpc-level error
/// string `"command timed out"` (`lib/socket.c:rpc_timeout_scan`,
/// status `RPC_STATUS_TIMEOUT`).
#[test]
fn timeout_shaped_untyped_error_is_worker_local() {
    use migration_mover::libnfs::asyncio::NfsError;
    use migration_worker::orchestrator::classify_shard_error;

    // The exact message shape the async FFI surface renders for a
    // timed-out RPC: NfsError::Errno { errno: EINTR, detail:
    // "Command timed out" } (see callbacks.rs::err_from_cb).
    let async_shape = NfsError::Errno {
        errno: libc::EINTR,
        detail: "Command timed out".into(),
    };

    let cases: Vec<anyhow::Error> = vec![
        anyhow::anyhow!("pipelined read failed: {async_shape}"),
        // rpc-level error string (lowercase) from rpc_timeout_scan.
        anyhow::anyhow!("libnfs: command timed out"),
        // Explicit status tag, in case a future wrapper surfaces it.
        anyhow::anyhow!("mount failed: RPC_STATUS_TIMEOUT"),
    ];
    for c in cases {
        let msg = format!("{c:#}");
        let err = Error::Other(c);
        assert_eq!(
            classify_shard_error(&err),
            ShardErrorClass::WorkerLocal,
            "timeout-shaped error must be retryable (WorkerLocal), \
             not shard-fatal: {msg}",
        );
    }
}

/// BETA_POLISH_BATCH Item 3: the F12 carve-out keys on the libnfs
/// DETAIL string ("Command timed out"), which flows from libnfs
/// itself (`lib/nfs_v3.c:check_nfs3_error`, carried through the
/// callback's data slot) — not from `errno_name`. Adding the EINTR
/// arm to `errno_name` therefore changes only the errno prefix of
/// the rendered message, never the timeout detection. Pin BOTH
/// renderings — the numeric-fallback prefix (`errno=4`, what the
/// failures JSONL recorded before the EINTR arm) and the named
/// prefix (`EINTR`) — so the two representations stay covered
/// whichever way the message was produced.
#[test]
fn eintr_named_timeout_error_is_worker_local() {
    use migration_worker::orchestrator::classify_shard_error;

    let cases: Vec<anyhow::Error> = vec![
        // Pre-EINTR-arm rendering of NfsError::Errno { errno: 4,
        // detail: "Command timed out" }.
        anyhow::anyhow!("pipelined read failed: libnfs errno=4: Command timed out"),
        // Post-EINTR-arm rendering of the same error.
        anyhow::anyhow!("pipelined read failed: libnfs EINTR: Command timed out"),
    ];
    for c in cases {
        let msg = format!("{c:#}");
        let err = Error::Other(c);
        assert_eq!(
            classify_shard_error(&err),
            ShardErrorClass::WorkerLocal,
            "EINTR-shaped timeout must stay retryable (WorkerLocal): {msg}",
        );
    }
}

/// A kernel/std-shaped timeout (`io::ErrorKind::TimedOut`) already
/// classifies WorkerLocal via the `Error::Io` arm — pin it so the
/// F12 guarantee holds for both shapes.
#[test]
fn io_timeout_error_is_worker_local() {
    let err = Error::Io(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "libnfs: Command timed out",
    ));
    assert_eq!(classify_shard_error(&err), ShardErrorClass::WorkerLocal);
}

// ---------------------------------------------------------------------------
// Test 2 (red before fix): worker-local error releases the claim
// (absent → reclaimable by any peer), never writes Failed, and the
// shard lands in the per-run skip set.
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn worker_local_error_releases_claim_not_failed() {
    let store = FakeStore::new();
    let shard = shard_name(1);
    let key = layout::claim_key(&shard);
    let (current, etag, epoch) = acquire_held(&store, &shard, "host-A").await;
    let mut skip: HashSet<String> = HashSet::new();

    // Stub processor outcome: SchemaVersionMismatch from
    // ShardReader::open — a property of host-A's stale binary.
    let err = anyhow::Error::from(Error::SchemaVersionMismatch {
        expected: 1,
        actual: 2,
    });

    let class = handle_process_error(
        ProcessErrorContext {
            store: &store,
            host_id: "host-A",
            shard_filename: &shard,
            current: &current,
            fallback_etag: &etag,
            fallback_epoch: epoch,
            heartbeat_sec: HB_SEC,
        },
        &err,
        &mut skip,
    )
    .await;
    assert_eq!(class, ShardErrorClass::WorkerLocal);

    // The claim ends ABSENT — released via the worker's own
    // delete-if-match — never terminal Failed.
    assert!(
        store.head_object(&key).await.unwrap().is_none(),
        "claim must be released (absent), not left Active or marked Failed",
    );
    let log = store.op_log();
    assert!(
        log.iter().any(|op| matches!(
            &op.kind,
            OpKind::DeleteIfMatch { key: k, etag: e, .. } if k == &key && e == &etag
        )),
        "release must go through DELETE If-Match with the worker's own held etag",
    );
    assert!(
        !log.iter().any(|op| matches!(
            &op.kind,
            OpKind::PutIfAbsent { key: k, body, ok: true }
                if k == &key
                    && serde_json::from_slice::<ClaimRecord>(body)
                        .is_ok_and(|r| r.state == ClaimState::Failed)
        )),
        "a worker-local error must never write a Failed claim record",
    );

    // R4 discipline: the held-claim cell was cleared (before the S3
    // write) so the heartbeat can't spuriously fence on the release.
    assert!(
        current.lock().await.is_none(),
        "held-claim cell not cleared"
    );

    // The shard is in the local skip set so THIS worker won't thrash
    // re-claiming it...
    assert!(skip.contains(&shard), "shard missing from local skip set");

    // ...but a healthy peer acquires it immediately.
    assert!(
        matches!(
            claim::try_acquire(&store, &shard, "host-B").await.unwrap(),
            AcquireOutcome::Acquired { .. }
        ),
        "released shard must be immediately claimable by a healthy peer",
    );
}

// ---------------------------------------------------------------------------
// Test 3: shard-fatal errors keep today's behavior — terminal Failed
// with the fail() atom.
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn fatal_error_still_marks_failed() {
    let store = FakeStore::new();
    let shard = shard_name(1);
    let key = layout::claim_key(&shard);
    let (current, etag, epoch) = acquire_held(&store, &shard, "host-A").await;
    let mut skip: HashSet<String> = HashSet::new();

    // Stub processor outcome: the parquet won't decode — any worker
    // reclaiming this shard hits the same error.
    let err = anyhow::Error::from(Error::ShardCorrupt {
        shard: shard.clone(),
        source: anyhow::anyhow!("bad magic bytes"),
    });

    let class = handle_process_error(
        ProcessErrorContext {
            store: &store,
            host_id: "host-A",
            shard_filename: &shard,
            current: &current,
            fallback_etag: &etag,
            fallback_epoch: epoch,
            heartbeat_sec: HB_SEC,
        },
        &err,
        &mut skip,
    )
    .await;
    assert_eq!(class, ShardErrorClass::Fatal);

    // Terminal Failed record, with the worker's identity, as today.
    let (_, body) = store
        .head_object(&key)
        .await
        .unwrap()
        .expect("claim object must still exist (terminal Failed)");
    assert_eq!(claim_state(&body), ClaimState::Failed);
    let record: ClaimRecord = serde_json::from_slice(&body).unwrap();
    assert_eq!(record.host, "host-A");

    // Fatal errors are terminal for everyone: no skip-set entry needed.
    assert!(
        skip.is_empty(),
        "fatal path must not touch the skip set (the claim is terminal)",
    );
    assert!(
        current.lock().await.is_none(),
        "held-claim cell not cleared"
    );
}

// ---------------------------------------------------------------------------
// Test 4: a skipped shard must not wedge the worker's exit condition.
// all_terminal is computed from the true claim states; the skip set
// only gates next_target selection.
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn skip_set_does_not_block_all_terminal_exit() {
    let store = FakeStore::new();
    let manifest = manifest_with_shards(2);
    let lease = Duration::from_secs(180);
    let shard1 = shard_name(1);
    let shard2 = shard_name(2);

    // Worker A claims shard 1, hits a worker-local error, releases
    // and skips it.
    let (current, etag, epoch) = acquire_held(&store, &shard1, "host-A").await;
    let mut skip: HashSet<String> = HashSet::new();
    let err = worker_local_io_error();
    handle_process_error(
        ProcessErrorContext {
            store: &store,
            host_id: "host-A",
            shard_filename: &shard1,
            current: &current,
            fallback_etag: &etag,
            fallback_epoch: epoch,
            heartbeat_sec: HB_SEC,
        },
        &err,
        &mut skip,
    )
    .await;
    assert!(skip.contains(&shard1));

    // Worker A's next scan: shard 1 is Free again but skipped, so the
    // scanner must offer shard 2 — never the skipped shard.
    let mut cache = ClaimBodyCache::default();
    let scan = scan_shards(&store, &manifest, lease, HB_SEC, &mut cache, &skip)
        .await
        .unwrap();
    assert!(!scan.all_terminal, "nothing terminal yet");
    assert_eq!(
        scan.last_target_filename, shard2,
        "scanner must pass over the skipped shard and offer the next claimable one",
    );

    // A simulated healthy peer processes BOTH shards to Completed.
    for shard in [&shard1, &shard2] {
        let AcquireOutcome::Acquired { etag, record } =
            claim::try_acquire(&store, shard, "host-B").await.unwrap()
        else {
            panic!("peer acquire of {shard} must succeed");
        };
        let CompleteOutcome::Completed { .. } =
            claim::complete(&store, shard, &etag, "host-B", record.epoch)
                .await
                .unwrap()
        else {
            panic!("peer complete of {shard} must succeed");
        };
    }

    // Worker A still carries shard 1 in its skip set — and must still
    // see all_terminal and exit.
    let scan = scan_shards(&store, &manifest, lease, HB_SEC, &mut cache, &skip)
        .await
        .unwrap();
    assert!(
        scan.all_terminal,
        "skip set must not block the all-terminal exit once peers finish every shard",
    );
    assert!(scan.next_target.is_none());
}

// ---------------------------------------------------------------------------
// Test 5: the release path reuses the contention backoff (jittered
// sleep) so a fleet-wide transient doesn't become a claim/release
// storm. Asserted over paused tokio time plus mock-store op counts.
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn repeated_worker_local_errors_backoff() {
    const N: usize = 5;
    // The contention backoff sleeps base + jitter, base = hb/4.
    let base = Duration::from_millis(HB_SEC * 1000 / 4);

    let store = FakeStore::new();
    let current: Mutex<Option<HeldClaim>> = Mutex::new(None);
    let mut skip: HashSet<String> = HashSet::new();

    let start = tokio::time::Instant::now();
    for i in 1..=N {
        let shard = shard_name(i);
        let AcquireOutcome::Acquired { etag, record } =
            claim::try_acquire(&store, &shard, "host-A").await.unwrap()
        else {
            panic!("acquire of {shard} must succeed");
        };
        *current.lock().await = Some(HeldClaim {
            shard: shard.clone(),
            etag: etag.clone(),
            epoch: record.epoch,
        });
        // Fleet-wide transient: every shard yields a worker-local
        // error (e.g. an S3/scratch blip).
        let err = worker_local_io_error();
        let class = handle_process_error(
            ProcessErrorContext {
                store: &store,
                host_id: "host-A",
                shard_filename: &shard,
                current: &current,
                fallback_etag: &etag,
                fallback_epoch: record.epoch,
                heartbeat_sec: HB_SEC,
            },
            &err,
            &mut skip,
        )
        .await;
        assert_eq!(class, ShardErrorClass::WorkerLocal);
    }
    let elapsed = start.elapsed();

    // Each release must have slept at least the backoff base (the
    // jitter only adds on top), so N releases cost >= N × base of
    // paused time. Without the backoff this loop completes in ~0
    // paused time — the storm the backoff exists to prevent.
    assert!(
        elapsed >= base * N as u32,
        "release path did not back off: {N} releases took {elapsed:?} paused time \
         (expected >= {:?})",
        base * N as u32,
    );

    // Op-count bound: exactly one DELETE per release — no per-shard
    // retry hammering — and every shard locally skipped.
    let deletes = store
        .op_log()
        .iter()
        .filter(|op| matches!(&op.kind, OpKind::DeleteIfMatch { .. }))
        .count();
    assert_eq!(deletes, N, "expected exactly one release DELETE per shard");
    assert_eq!(skip.len(), N);
}
