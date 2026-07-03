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
//! - **Open / close fhs.** Caller opens with the appropriate stability
//!   (`Flags::wronly_sync()` for cutover, plain `Flags::wronly()` for
//!   bulk) and closes after the fence check / rename. Stability is an
//!   open-time choice on the linked libnfs (`pwrite` has no per-call
//!   stability flag — see `LIBNFS_ASYNC_FORK.md` closing note).
//! - **Rename / commit.** The atomic `.partial → final` rename is the
//!   sole commit point and stays in the caller, gated on
//!   `Fence::check_pre_rename()`. The fence check must sit between
//!   `pipelined_copy().await?` and `rename().await` with no other
//!   work in between. R8 from `CORRECTNESS_RULES.md`.
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
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use futures::FutureExt;
use migration_core::records::FailurePhase;
use std::collections::BTreeMap;
use xxhash_rust::xxh3::Xxh3;

/// Outcome of one `pipelined_copy` invocation. Caller folds this into
/// its per-file record (per-host partial manifest writer in Phase 3,
/// `MoveOutcome` once the `FileMover` trait lands in Phase 2 T2).
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

/// Per-file async copy. See module docs for the call contract.
///
/// Both `src_fh` and `dst_fh` are caller-owned; this function neither
/// opens nor closes them. `dst_fh` MUST already have been opened with
/// the correct stability flag — `Flags::wronly_sync()` for cutover
/// passes, plain `Flags::wronly().with_create()` for bulk. The body
/// is identical between the two.
pub async fn pipelined_copy(
    src: &AsyncNfsContext,
    src_fh: &AsyncNfsFh,
    dst: &AsyncNfsContext,
    dst_fh: &AsyncNfsFh,
    size: u64,
    cfg: BucketConfig,
) -> Result<FileCopyResult, MoveError> {
    // Pre-stat for torn-read detection. Doing it via `fstat(fh)`
    // rather than `stat(path)` so a concurrent unlink-and-recreate
    // on the source path can't substitute a different inode under us.
    let pre = src.fstat(src_fh).await.map_err(read_err)?;

    let rsize: u64 = cfg.rsize as u64;
    let read_depth: usize = cfg.read_pipeline_depth.max(1) as usize;
    let write_depth: usize = cfg.write_pipeline_depth.max(1) as usize;

    // Two-cursor short-read-safe read pipeline. `next_issue_off`
    // walks forward as we send pread RPCs; `next_deliver_off` walks
    // forward as we hand chunks to the writer. NFSv3 does not
    // guarantee a `pread(off, rsize)` returns `rsize` bytes pre-EOF,
    // so we may get short reads in the middle of a file (rare on
    // VAST but the spec permits it); the reorder buffer holds
    // completions until their offset is the next-to-deliver, at
    // which point the short-read tail is re-issued automatically
    // by the offset arithmetic in the read-completion arm.
    let mut next_issue_off: u64 = 0;
    let mut next_deliver_off: u64 = 0;
    // `FuturesUnordered<F>` fixes `F` to the first push's type. Two
    // anonymous `async move` blocks have distinct types even if their
    // bodies are identical (rustc E0308), so coerce reads + writes to
    // `BoxFuture`. The Box pin is one heap alloc per in-flight RPC —
    // dwarfed by the per-RPC syscall path.
    // A completed read resolves to (issue_offset, wanted_len, bytes).
    type ReadFuture<'a> = BoxFuture<'a, Result<(u64, u64, Vec<u8>), NfsError>>;
    let mut reads_inflight: FuturesUnordered<ReadFuture<'_>> = FuturesUnordered::new();
    let mut reorder_buf: BTreeMap<u64, Vec<u8>> = BTreeMap::new();

    // Bounded write pipeline. `FuturesUnordered` of in-flight pwrites
    // is the capacity gate; `submit_write` waits for a free slot.
    // Writes don't have to return in order — `pwrite` is per-offset
    // and the resulting bytes are positionally addressed on the dst
    // fh. The fsync at the end is the durability barrier.
    let mut writes_inflight: FuturesUnordered<BoxFuture<'_, Result<(), NfsError>>> =
        FuturesUnordered::new();

    // Inline hasher. xxh3_128 runs at ~10 GB/s; well below the wire
    // rate even on a fleet of workers.
    let mut hasher = Xxh3::new();

    // Stop reading once we've seen EOF (short read that didn't fill
    // its requested range AND was the file's actual tail). After EOF
    // we still need to drain whatever's already in flight and in the
    // reorder buffer, then writes_inflight, then fsync.
    let mut hit_eof = false;
    let mut effective_size: u64 = size;

    loop {
        // 1) Pump fresh reads up to depth, capped at the current
        // effective size (which only shrinks if we observe EOF
        // earlier than `size` predicted).
        while !hit_eof && reads_inflight.len() < read_depth && next_issue_off < effective_size {
            let want = rsize.min(effective_size - next_issue_off);
            let off = next_issue_off;
            let fut = async move {
                let bytes = src.pread(src_fh, off, want as usize).await?;
                Ok::<_, NfsError>((off, want, bytes))
            }
            .boxed();
            reads_inflight.push(fut);
            next_issue_off += want;
        }

        // 2) Deliver as many in-order chunks as the reorder buffer
        // currently has at `next_deliver_off`. Each delivery: hash,
        // then submit to the write pipeline (with backpressure).
        while let Some(chunk) = reorder_buf.remove(&next_deliver_off) {
            let chunk_len = chunk.len() as u64;
            if chunk_len == 0 {
                // Defensive: empty delivery means we hit EOF at
                // exactly the boundary. Nothing to hash or write.
                continue;
            }
            hasher.update(&chunk);

            // Write-pipeline backpressure. Drain completed writes
            // until there's a slot.
            while writes_inflight.len() >= write_depth {
                let r: Result<(), NfsError> = writes_inflight
                    .next()
                    .await
                    .expect("writes_inflight non-empty here");
                r.map_err(write_err)?;
            }

            let off = next_deliver_off;
            let fut = pwrite_all(dst, dst_fh, off, chunk).boxed();
            writes_inflight.push(fut);
            next_deliver_off += chunk_len;
        }

        // 3) Exit when: no reads in flight AND nothing left in the
        // reorder buffer AND either we've delivered everything we
        // expected, or we've hit EOF early. The write pipeline is
        // drained after the loop.
        if reads_inflight.is_empty() && reorder_buf.is_empty() {
            if hit_eof || next_deliver_off >= effective_size {
                break;
            }
            // Reads queue is empty but we haven't issued enough to
            // cover `effective_size`. This is the loop-step-zero
            // boundary (e.g., the very first iteration on a zero-
            // byte file). The next iteration's "pump fresh reads"
            // step covers it; if `effective_size == 0`, the next
            // iteration's exit check trips.
            if next_issue_off >= effective_size {
                break;
            }
            continue;
        }

        // 4) Wait for at least one more read to complete, route it
        // into the reorder buffer, re-issue any short-read tail.
        match reads_inflight.next().await {
            Some(Ok((off, expected, bytes))) => {
                let actual = bytes.len() as u64;
                if actual == 0 {
                    // EOF at `off`. The file is shorter than `size`
                    // predicted. Clamp `effective_size` so we stop
                    // issuing past the real end; the reorder buffer
                    // may still have in-order chunks before this
                    // offset to deliver.
                    if off < effective_size {
                        effective_size = off;
                    }
                    hit_eof = true;
                    continue;
                }
                reorder_buf.insert(off, bytes);
                if actual < expected {
                    // Short read mid-file (very rare on VAST but
                    // NFSv3 permits it). Issue the gap-fill at
                    // `off + actual` for the remaining bytes. The
                    // reorder buffer will hold both chunks; delivery
                    // walks `next_deliver_off` forward through them.
                    let gap_off = off + actual;
                    let gap_len = expected - actual;
                    let fut = async move {
                        let bytes = src.pread(src_fh, gap_off, gap_len as usize).await?;
                        Ok::<_, NfsError>((gap_off, gap_len, bytes))
                    }
                    .boxed();
                    reads_inflight.push(fut);
                }
            }
            Some(Err(e)) => return Err(read_err(e)),
            None => {
                // reads_inflight was empty when we polled it; the
                // top-of-loop pump should have kept it non-empty if
                // there were more reads to issue. Defensive.
                if reorder_buf.is_empty() && (hit_eof || next_deliver_off >= effective_size) {
                    break;
                }
            }
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
        bytes_copied: next_deliver_off,
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
