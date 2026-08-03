//! Per-file async copy pipeline backing the bucketed async mover.
//!
//! See `docs/work-items/MULTI_PASS_MOVER.md` "Per-file async copy
//! pipeline" and `docs/work-items/MULTI_PASS_MOVER_PHASE2_NOTES.md`
//! §1–§4 for the design. This module ships the body that lives
//! *between* `dst.create()` and the fence-gated `dst.rename()`:
//!
//! 1. `fstat` the source for the pre-bracket.
//! 2. Stream the file through a read pipeline (out-of-order
//!    completions, short-read-safe two-cursor model) and a write
//!    pipeline (bounded backpressure via `FuturesUnordered`).
//! 3. Hash inline with `xxh3_128` so we don't need a second pass.
//! 4. `fsync` the destination — the whole-file NFS COMMIT
//!    durabilizes the bytes (the linked libnfs has no per-range
//!    `nfs_commit_async`; cutover-mode per-range COMMIT is deferred
//!    per `LIBNFS_ASYNC_FORK_AUDIT.md` follow-up #2).
//! 5. `fstat` the source for the post-bracket.
//!
//! ## What this module deliberately does NOT do
//!
//! - **Open / close fhs.** The caller owns both handles and closes them after
//!   this copy returns, before attribute application and the fence-gated
//!   rename. Stability is an
//!   open-time choice on the linked libnfs (`pwrite` has no per-call
//!   stability flag — see `LIBNFS_ASYNC_FORK.md` closing note).
//! - **Rename / commit.** The atomic `.partial → final` rename is the
//!   sole commit point and stays in the caller, gated on
//!   the caller's fence check. Attribute work and handle cleanup may follow
//!   `pipelined_copy`, but the final fence check and `rename` must remain
//!   adjacent. R8 from `CORRECTNESS_RULES.md`.
//! - **Torn-read remediation.** Torn reads are recorded in
//!   `FileCopyResult::torn`; the caller (`file_mover::classify_copy`,
//!   consumed by `copy_regular`) still commits the file and emits a
//!   `DowngradeKind::TornCopy` downgrade record carrying the pre/post
//!   stat brackets, so the tear is operator-visible. There is no
//!   re-copy today: the multi-pass converging mover that would re-copy
//!   torn rows is future work (`MULTI_PASS_MOVER.md`, phases 3–8
//!   unbuilt). This function never fails because of a torn read.

use crate::bucketed_pool::BucketConfig;
use crate::error::MoveError;
use crate::libnfs::asyncio::{AsyncNfsContext, AsyncNfsFh, NfsError, NfsStat64};
use crate::reorder::ReorderState;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use futures::FutureExt;
use migration_core::records::FailurePhase;
use xxhash_rust::xxh3::Xxh3;

/// Outcome of one `pipelined_copy` invocation. `AsyncBucketedFileMover` folds
/// this into the row's `MoveOutcome` and downgrade records.
#[derive(Debug, Clone)]
pub struct FileCopyResult {
    /// `xxh3_128` of the bytes that actually transited the pipeline.
    /// Little-endian byte order — matches what `Xxh3::digest128`
    /// produces on this platform. Zero-byte files hash to
    /// `xxh3_128([])`.
    pub file_hash: [u8; 16],
    /// Bytes actually written to the destination. Equals
    /// `min(size, source EOF)` — short source (source EOF before
    /// `size`) returns the partial byte count without failing; the
    /// caller decides whether to record this as a downgrade
    /// (`DowngradeKind::EarlyEof`) or a hard failure
    /// (`require_unchanged_size`).
    pub bytes_copied: u64,
    /// True iff the source's `(size, mtime, ctime)` differs between
    /// the pre- and post-stat brackets. Indicates the source was
    /// modified during the copy; the destination still has *some*
    /// interleaving of pre- and post-versions and the rename still
    /// publishes it. The caller (`file_mover::classify_copy` →
    /// `copy_regular`) commits the file and emits a
    /// `DowngradeKind::TornCopy` downgrade record — at-least-once
    /// semantics with the source intact. Re-copying torn rows is the
    /// future multi-pass driver's job (`MULTI_PASS_MOVER.md`).
    pub torn: bool,
    /// `fstat(src_fh)` taken *before* the first read. Paired with
    /// `post_stat` to populate `DowngradeKind::TornCopy`'s pre/post
    /// `(size, mtime, ctime)` triples when `torn` is true.
    pub pre_stat: NfsStat64,
    /// `fstat(src_fh)` taken *after* the last read + fsync. Caller
    /// uses this for the manifest row's authoritative
    /// `size`/`mtime_ns`/`ctime_ns`/`mode` (post-stat wins over the
    /// pre-stat because the post is what the *bytes on dest* are
    /// actually a snapshot of).
    pub post_stat: NfsStat64,
}

// `FuturesUnordered<F>` fixes `F` to the first push's type. Two
// anonymous `async move` blocks have distinct types even if their
// bodies are identical (rustc E0308), so coerce reads + writes to
// `BoxFuture`. The Box pin is one heap alloc per in-flight RPC —
// dwarfed by the per-RPC syscall path.
// A completed read resolves to (issue_offset, wanted_len, bytes).
type ReadFuture<'a> = BoxFuture<'a, Result<(u64, u64, Vec<u8>), NfsError>>;
type WriteFuture<'a> = BoxFuture<'a, Result<(), NfsError>>;

/// Per-file async copy. See module docs for the call contract.
///
/// Both `src_fh` and `dst_fh` are caller-owned; this function neither
/// opens nor closes them. `dst_fh` MUST already have been opened with
/// the correct stability flag — `Flags::wronly_sync()` for cutover
/// passes, plain `Flags::wronly().with_create()` for bulk. The body
/// is identical between the two.
///
/// ## Error path: drain before return (F11)
///
/// On ANY error the in-flight read/write RPCs are awaited to
/// completion (results discarded) BEFORE the error is returned —
/// dropping the futures would not cancel the RPCs (libnfs has no
/// NFSv3 cancel; see `asyncio/mod.rs` "Cancellation"), and the
/// caller's unconditional closes (`file_mover.rs::copy_regular`)
/// must act on quiescent fhs: in the pinned libnfs, close on a
/// non-dirty fh frees the `nfsfh` struct immediately, and close on a
/// dirty fh can free it while a WRITE reply is still outstanding —
/// a use-after-free window (audited in the F11 commit message).
/// This is enforced by construction: the whole copy body lives in
/// `copy_pipeline_body`, and this wrapper is the only caller — its
/// single `Err` arm is the one exit that can carry an error out,
/// and it drains both pipelines first. With F12's per-RPC timeout,
/// the drain wait is bounded.
pub async fn pipelined_copy(
    src: &AsyncNfsContext,
    src_fh: &AsyncNfsFh,
    dst: &AsyncNfsContext,
    dst_fh: &AsyncNfsFh,
    size: u64,
    cfg: BucketConfig,
) -> Result<FileCopyResult, MoveError> {
    let mut reads_inflight: FuturesUnordered<ReadFuture<'_>> = FuturesUnordered::new();
    let mut writes_inflight: FuturesUnordered<WriteFuture<'_>> = FuturesUnordered::new();

    match copy_pipeline_body(
        src,
        src_fh,
        dst,
        dst_fh,
        size,
        cfg,
        &mut reads_inflight,
        &mut writes_inflight,
    )
    .await
    {
        Ok(result) => Ok(result),
        Err(e) => {
            // F11: quiesce both fhs before the caller's closes.
            let drained_reads = drain_all(&mut reads_inflight).await;
            let drained_writes = drain_all(&mut writes_inflight).await;
            tracing::debug!(
                drained_reads,
                drained_writes,
                error = %e,
                "pipelined_copy error path: awaited in-flight RPCs before returning",
            );
            Err(e)
        }
    }
}

/// Await every future in `inflight`, discarding results. Returns how
/// many futures were consumed. The pure seam behind the F11 drain:
/// generic over any unpinned stream so unit tests can pin the
/// awaits-everything contract with plain tokio futures (no FFI).
async fn drain_all<S>(inflight: &mut S) -> usize
where
    S: futures::Stream + Unpin,
{
    let mut drained = 0usize;
    while inflight.next().await.is_some() {
        drained += 1;
    }
    drained
}

/// The copy body proper. MUST only be called by [`pipelined_copy`]:
/// every `?`/`return Err` in here relies on the wrapper's Err arm to
/// drain `reads_inflight` / `writes_inflight` before the error
/// escapes to the caller (F11 — reviewable by construction: the
/// queues outlive the body because the wrapper owns them).
#[allow(clippy::too_many_arguments)]
async fn copy_pipeline_body<'a>(
    src: &'a AsyncNfsContext,
    src_fh: &'a AsyncNfsFh,
    dst: &'a AsyncNfsContext,
    dst_fh: &'a AsyncNfsFh,
    size: u64,
    cfg: BucketConfig,
    reads_inflight: &mut FuturesUnordered<ReadFuture<'a>>,
    writes_inflight: &mut FuturesUnordered<WriteFuture<'a>>,
) -> Result<FileCopyResult, MoveError> {
    // Pre-stat for torn-read detection. Doing it via `fstat(fh)`
    // rather than `stat(path)` so a concurrent unlink-and-recreate
    // on the source path can't substitute a different inode under us.
    let pre = src.fstat(src_fh).await.map_err(read_err)?;

    let rsize: u64 = cfg.rsize as u64;
    let read_depth: usize = cfg.read_pipeline_depth.max(1) as usize;
    let write_depth: usize = cfg.write_pipeline_depth.max(1) as usize;

    // Two-cursor short-read-safe read pipeline, owned by the pure
    // `ReorderState` (see `reorder.rs`): the issue cursor walks
    // forward as we send pread RPCs; the deliver cursor walks forward
    // as we hand chunks to the writer. NFSv3 does not guarantee a
    // `pread(off, rsize)` returns `rsize` bytes pre-EOF, so we may
    // get short reads in the middle of a file (rare on VAST but the
    // spec permits it); the reorder buffer holds completions until
    // their offset is the next-to-deliver, at which point the
    // short-read tail is re-issued via `CompletionOutcome::reissue`.
    //
    // The read/write queues are caller-owned (`pipelined_copy`) so
    // the F11 drain can run after any error return from this body.
    // Writes don't have to return in order — `pwrite` is per-offset
    // and the resulting bytes are positionally addressed on the dst
    // fh. The fsync at the end is the durability barrier.
    let mut state = ReorderState::new(size);

    // Inline hasher. xxh3_128 runs at ~10 GB/s; well below the wire
    // rate even on a fleet of workers.
    let mut hasher = Xxh3::new();

    // Reorder-buffer bound (F07): if the read at the deliver cursor
    // stalls, completed reads pile up in the reorder buffer; without
    // a cap the pump would buffer the whole remaining file in RAM.
    // Derived from the bucket config (no new user knob): twice the
    // pipeline's natural window of `read_depth` chunks of `rsize`
    // (the largest chunk a read or gap-fill can carry).
    let max_buffered_bytes = (read_depth as u64).saturating_mul(rsize).saturating_mul(2);

    loop {
        // 1) Pump fresh reads while the state machine allows it
        // (below depth, below the current effective size — which
        // only shrinks if we observe EOF earlier than `size`
        // predicted — and within the buffered-bytes bound).
        while state.may_issue(reads_inflight.len(), read_depth, max_buffered_bytes) {
            let (off, want) = state.take_read(rsize);
            let fut = async move {
                let bytes = src.pread(src_fh, off, want as usize).await?;
                Ok::<_, NfsError>((off, want, bytes))
            }
            .boxed();
            reads_inflight.push(fut);
        }

        // 2) Exit when nothing is in flight and the state machine is
        // drained (reorder buffer empty, and delivery reached the
        // effective size or EOF was observed). The write pipeline is
        // drained after the loop.
        if reads_inflight.is_empty() {
            if state.drained() {
                break;
            }
            // Nothing in flight, not drained, and the pump above
            // declined to issue: no event can ever make progress.
            // Structurally unreachable — whenever the reorder buffer
            // is non-empty the chunk at the deliver cursor is in
            // flight (issuance is in offset order and the EOF clamp
            // purges unreachable entries) — so this is a state-machine
            // accounting bug. Fail the file copy rather than spin on
            // a core forever holding the inflight-limiter permit
            // (the F06 livelock shape).
            return Err(MoveError::new(
                FailurePhase::Read,
                "pipelined_copy wedged: no reads in flight but reorder \
                 state not drained (reorder/EOF accounting bug)",
            ));
        }

        // 3) Wait for at least one more read to complete and route it
        // through the state machine: EOF clamps the effective size,
        // data completions land in the reorder buffer and release
        // whatever is now contiguous with the deliver cursor.
        match reads_inflight.next().await {
            Some(Ok((off, expected, bytes))) => {
                if bytes.is_empty() {
                    // EOF at `off`. The file is shorter than `size`
                    // predicted. Clamp so we stop issuing past the
                    // real end; the reorder buffer may still have
                    // in-order chunks before this offset to deliver.
                    state.on_eof_clamp(off);
                    continue;
                }
                let outcome = state.on_completion(off, expected, bytes);
                if let Some((gap_off, gap_len)) = outcome.reissue {
                    // Short read mid-file (very rare on VAST but
                    // NFSv3 permits it). Issue the gap-fill; the
                    // reorder buffer holds both chunks and delivery
                    // walks forward through them.
                    let fut = async move {
                        let bytes = src.pread(src_fh, gap_off, gap_len as usize).await?;
                        Ok::<_, NfsError>((gap_off, gap_len, bytes))
                    }
                    .boxed();
                    reads_inflight.push(fut);
                }
                // Deliver released in-order chunks: hash, then submit
                // to the write pipeline (with backpressure).
                for chunk in outcome.deliver {
                    hasher.update(&chunk.bytes);

                    // Write-pipeline backpressure. Drain completed
                    // writes until there's a slot.
                    while writes_inflight.len() >= write_depth {
                        let r: Result<(), NfsError> = writes_inflight
                            .next()
                            .await
                            .expect("writes_inflight non-empty here");
                        r.map_err(write_err)?;
                    }

                    let fut = pwrite_all(dst, dst_fh, chunk.offset, chunk.bytes).boxed();
                    writes_inflight.push(fut);
                }
            }
            Some(Err(e)) => return Err(read_err(e)),
            None => unreachable!("reads_inflight checked non-empty above"),
        }
    }

    // Drain the write pipeline. The reorder buffer is empty by here
    // (delivery happens inline), so all bytes-to-write have been
    // submitted; we just wait for outstanding pwrites to ack.
    while let Some(r) = writes_inflight.next().await {
        r.map_err(write_err)?;
    }

    // Whole-file NFS COMMIT. Durabilizes everything written above.
    // The linked libnfs's `nfs_fsync_async` is whole-file
    // (LIBNFS_ASYNC_FORK_AUDIT #2); per-range COMMIT does not exist
    // on this surface. For cutover passes the dst_fh was opened with
    // FILE_SYNC (`Flags::wronly_sync()`), so the per-write acks
    // already include stability — fsync becomes a no-op COMMIT but
    // we issue it unconditionally so the call shape is identical to
    // the bulk path. (Cost: one COMMIT round-trip per file on
    // cutover — negligible.)
    dst.fsync(dst_fh).await.map_err(write_err)?;

    // Post-stat. Last thing before return; nothing else races.
    let post = src.fstat(src_fh).await.map_err(read_err)?;

    let torn = (pre.size, pre.mtime, pre.ctime) != (post.size, post.mtime, post.ctime);

    let file_hash = hasher.digest128().to_le_bytes();

    Ok(FileCopyResult {
        file_hash,
        bytes_copied: state.bytes_delivered(),
        torn,
        pre_stat: pre,
        post_stat: post,
    })
}

fn read_err(e: NfsError) -> MoveError {
    MoveError::new(FailurePhase::Read, format!("{e}"))
}

fn write_err(e: NfsError) -> MoveError {
    MoveError::new(FailurePhase::Write, format!("{e}"))
}

/// Loop `pwrite` until every byte in `chunk` is on the wire.
///
/// The linked libnfs's `nfs_pwrite_async` caps each call at
/// `nfs_get_writemax(ctx)`, which post-FSINFO is
/// `min(client wsize, server wtmax)` — see `lib/nfs_v3.c
/// :nfs3_pwrite_async_internal`. VAST var204 negotiates
/// `wtmax = 1 MiB`, so our medium (2 MiB) and large (4 MiB)
/// bucket configs would silently lose the tail of every chunk
/// without this retry loop. We clone `chunk` once up front so
/// the slow path (short write) can rebuild the unwritten tail
/// after libnfs consumes the original `Vec` — the fast path
/// (write completes in one RPC) keeps the clone alive only for
/// the duration of the single pwrite and drops it on success.
///
/// The 2× memory overhead is bounded by `write_pipeline_depth`
/// per file × `InflightLimiter` files-in-flight per bucket, so
/// the worst-case footprint is `2 × 32 × 4 MiB × 4 files` =
/// 1 GiB at the large bucket's defaults. Acceptable for a worker
/// process; revisit if profiling shows it dominating.
async fn pwrite_all(
    dst: &AsyncNfsContext,
    dst_fh: &AsyncNfsFh,
    off: u64,
    chunk: Vec<u8>,
) -> Result<(), NfsError> {
    let total = chunk.len();
    if total == 0 {
        return Ok(());
    }

    let backup = chunk.clone();
    let mut next_buf = chunk;
    let mut written = 0usize;
    loop {
        let attempt_len = next_buf.len();
        let n = dst.pwrite(dst_fh, off + written as u64, next_buf).await?;
        if n == 0 {
            return Err(NfsError::Protocol(format!(
                "pwrite returned 0 of {attempt_len} at off={} \
                 (already written {written}/{total}); likely \
                 server-side write rejection",
                off + written as u64,
            )));
        }
        written += n;
        if written >= total {
            return Ok(());
        }
        // Short write — libnfs returned fewer bytes than we sent.
        // Rebuild the unwritten tail from the backup and retry.
        next_buf = backup[written..].to_vec();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_hash_matches_xxh3_128_of_empty() {
        let h = Xxh3::new().digest128().to_le_bytes();
        // xxh3_128 of empty input is a well-known constant; lock it
        // in so a future xxhash-rust major bump doesn't silently
        // change manifest hashes for zero-byte files.
        // Captured from `xxhash_rust::xxh3::Xxh3::new().digest128()`
        // on this platform (little-endian byte order). Locks the
        // value in so a future xxhash-rust major bump cannot silently
        // change manifest hashes for zero-byte files.
        let expected_hex = "7f498d4624c30160d8984701d306aa99";
        let got_hex: String = h.iter().map(|b| format!("{:02x}", b)).collect();
        assert_eq!(got_hex, expected_hex);
    }

    // ---- F11: drain-before-close (PROTECTED_FFI_BATCH.md Item 2) --

    /// The drain helper must AWAIT every future it is handed — not
    /// drop them — because dropping a libnfs future does not cancel
    /// the underlying RPC (`asyncio/mod.rs` "Cancellation"): the RPC
    /// would still complete against an fh the caller is about to
    /// close. Plain tokio futures stand in for RPCs: half complete
    /// only after a deferred signal, so a drain that merely drops
    /// pending futures (or returns before the stream is exhausted)
    /// fails the completion count.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn drain_all_awaits_every_future_it_is_handed() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        const N: usize = 8;
        let completed = Arc::new(AtomicUsize::new(0));
        let mut inflight: FuturesUnordered<BoxFuture<'static, Result<(), NfsError>>> =
            FuturesUnordered::new();

        let mut senders = Vec::new();
        for i in 0..N {
            let completed = Arc::clone(&completed);
            if i % 2 == 0 {
                // Immediately ready (a mix of Ok and Err results —
                // drain must discard both without short-circuiting).
                inflight.push(
                    async move {
                        completed.fetch_add(1, Ordering::SeqCst);
                        if i % 4 == 0 {
                            Ok(())
                        } else {
                            Err(NfsError::Closed)
                        }
                    }
                    .boxed(),
                );
            } else {
                // Genuinely pending until its oneshot fires.
                let (tx, rx) = tokio::sync::oneshot::channel::<()>();
                senders.push(tx);
                inflight.push(
                    async move {
                        let _ = rx.await;
                        completed.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                    .boxed(),
                );
            }
        }
        // Fire the pending halves' signals shortly after drain_all
        // starts polling, so the drain demonstrably *waits*.
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            for tx in senders {
                let _ = tx.send(());
            }
        });

        let drained = drain_all(&mut inflight).await;
        assert_eq!(drained, N, "drain_all must consume every future");
        assert_eq!(
            completed.load(Ordering::SeqCst),
            N,
            "every future must have run to completion (awaited, not dropped)"
        );
        assert!(inflight.is_empty());
    }

    #[tokio::test]
    async fn drain_all_on_empty_stream_returns_zero() {
        let mut inflight: FuturesUnordered<BoxFuture<'static, Result<(), NfsError>>> =
            FuturesUnordered::new();
        assert_eq!(drain_all(&mut inflight).await, 0);
    }

    #[test]
    fn read_err_uses_read_phase() {
        let e = read_err(NfsError::Init("test".into()));
        assert_eq!(e.phase, FailurePhase::Read);
        assert!(e.error.contains("test"));
    }

    #[test]
    fn write_err_uses_write_phase() {
        let e = write_err(NfsError::Init("test".into()));
        assert_eq!(e.phase, FailurePhase::Write);
        assert!(e.error.contains("test"));
    }
}
