//! Per-shard processing.
//!
//! Once a shard has been claimed and downloaded, this module walks its
//! rows in `row_id` order, builds byte-budgeted micro-batches, and
//! drives the mover. M3 dispatches rows concurrently.
//!
//! ## Concurrency model (M3)
//!
//! - Within a batch, rows are partitioned into **hardlink groups** and
//!   **singletons**:
//!   - Singletons: each gets its own task; up to `inflight_*` (per
//!     size class) tasks run in parallel.
//!   - Hardlink groups (same `(fsid, inode)` and `nlink > 1`): each
//!     group becomes one task, processing its rows **sequentially**
//!     so the first row is fully renamed before subsequent rows
//!     `nfs_link` against it.
//! - Each task acquires a size-class permit from `InflightLimiter`
//!   before each mover call, then a libnfs context pair from the
//!   pool, then runs the libnfs work on the blocking pool. Concurrent
//!   tasks therefore use distinct libnfs contexts.
//! - Outcomes are collected on the dispatcher thread; `record(...)`
//!   updates counters and counts failures. No shared map needed.
//!
//! ## Hardlink fidelity (R5 + SCHEMA_CONTRACT.md)
//!
//! The per-group sequential dispatch keeps hardlink fidelity without
//! a shared mutable map: the group leader copies; subsequent rows
//! `nfs_link` against the leader's path. The processor still emits
//! the one-shot `FsidUngrouped` downgrade per shard for any row whose
//! `inode` is set but `fsid` is not.
//!
//! Cross-shard hardlinks remain out of scope for v1.
//!
//! ## Operator pause (coord `Pause`)
//!
//! Checked between batches only: a paused worker finishes the batch in
//! flight, then blocks until the mode changes. The claim stays ours
//! throughout — the heartbeat task keeps writing the per-host progress
//! record, which is the liveness signal reclaimers trust (see
//! `check_progress_liveness`), so a long pause never turns into a
//! peer stealing the shard. Drain/Cancel are NOT honored mid-shard:
//! the shard runs to completion and the orchestrator's claim loop
//! stops claiming, so no partial shard is ever left for replay.
//!
//! ## Process stop (SIGTERM / SIGINT)
//!
//! Also checked between batches only. A stop request finishes the
//! batch in flight and returns with `ProcessOutcome::interrupted`
//! set; the orchestrator then releases the claim so a peer can pick
//! the shard up at once. Rows already committed are durable and are
//! recognized on replay, so the batch boundary is the cheapest safe
//! point to stop — no row is ever interrupted mid-copy.
//!
//! ## Fence checks (R3)
//!
//! - Between batches (in `process`).
//! - Between rows within a sequential group, and at task entry for
//!   singletons. **Never inside a copy.** Once a row enters the mover,
//!   it runs to commit (rename) or per-file failure.

use crate::throughput::ThroughputCounter;
use migration_core::fence::Fence;
use migration_core::records::{DowngradeKind, FailurePhase};
use migration_core::schema::FileTypeTag;
use migration_core::shard::{RowView, ShardReader};
use migration_mover::batch::{Batch, BatchBudget, InflightLimiter};
use migration_mover::{FailureSink, FileMover, MoveOutcome};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

/// Hardlink-map key. `None` for fsid means the source row didn't carry
/// one; the worker falls back to inode-only grouping (with a one-time
/// WARN + downgrade per shard, emitted on first occurrence).
type HardlinkKey = (Option<u64>, u64);

pub struct ShardProcessor {
    pub mover: Arc<dyn FileMover>,
    /// Live per-row counters published by the heartbeat between batch
    /// commits (see `heartbeat::LivePending`). Reset by the
    /// orchestrator when it merges shard-end totals into
    /// `ProgressState`.
    pub live: Arc<crate::heartbeat::LivePending>,
    pub fence: Fence,
    pub budget: BatchBudget,
    pub inflight: InflightLimiter,
    pub failures: FailureSink,
    pub throughput: ThroughputCounter,
    /// Directory rows seen in this shard, kept for the shard-end
    /// attr re-stamp: every file CREATE bumps its parent dir's mtime,
    /// so per-batch dir stamping is undone by later batches. One
    /// idempotent DirAttrs replay after the last batch makes dir
    /// mtimes correct at shard scope (cross-shard children remain the
    /// documented caveat).
    pub dir_restamp: Vec<RowView>,
    /// Set true the first time we see an `inode`-bearing row with no
    /// `fsid`; gates the one-shot WARN + `FsidUngrouped` downgrade.
    pub fsid_fallback_warned: bool,
    /// Coord event emitter. Disabled in legacy S3-only mode (every
    /// call is a no-op); when enabled, `record_outcome` pushes a
    /// `WorkerEventDraft` per file outcome so the coord_driver can
    /// coalesce a `ProgressDelta` for `/workers/{id}/events`.
    pub emitter: crate::coord_driver::EventEmitter,
    /// Coord-driven run control, consulted between batches for
    /// `Pause`. `None` in S3-only mode (no `[coord]`).
    pub run_control: Option<crate::run_control::RunControlReader>,
    /// Process stop request (SIGTERM / SIGINT), consulted between
    /// batches. Cancelled → finish the batch in hand and return with
    /// `interrupted` set. See the module docs.
    pub stop: CancellationToken,
}

impl ShardProcessor {
    pub async fn process(&mut self, parquet_path: &Path) -> anyhow::Result<ProcessOutcome> {
        let reader = ShardReader::open(parquet_path)?;
        let total = reader.rows();
        tracing::info!(parquet = %parquet_path.display(), total_rows = total, "shard opened");

        // Per-shard state reset.
        self.fsid_fallback_warned = false;
        self.dir_restamp.clear();

        let mut current = Batch::default();
        let mut outcome = ProcessOutcome {
            rows_total: total,
            ..Default::default()
        };

        // A stop that arrived before the first batch: nothing has been
        // copied from this shard, hand it straight back.
        if self.stop.is_cancelled() {
            return Ok(outcome.with_interrupted(true));
        }

        for row_result in reader.into_rows()? {
            if !self.fence.is_valid() {
                return Ok(outcome.with_fenced(true));
            }
            let row = row_result?;

            if current.would_overflow(&row, &self.budget) {
                let to_run = current.take();
                self.run_batch(to_run, &mut outcome).await?;
                if !self.fence.is_valid() {
                    return Ok(outcome.with_fenced(true));
                }
                self.hold_while_paused().await;
                // The fence may have tripped during a long hold.
                if !self.fence.is_valid() {
                    return Ok(outcome.with_fenced(true));
                }
                // Batch boundary: the only point a process stop is
                // honored. Everything before it is committed.
                if self.stop.is_cancelled() {
                    tracing::info!(
                        rows_done = outcome.files_ok + outcome.files_failed,
                        rows_total = total,
                        "stop requested; leaving shard at batch boundary",
                    );
                    return Ok(outcome.with_interrupted(true));
                }
            }
            current.push(row);
        }

        if !current.is_empty() {
            self.run_batch(current, &mut outcome).await?;
        }

        if !self.fence.is_valid() {
            return Ok(outcome.with_fenced(true));
        }

        // ---- Shard-end dir attr re-stamp -----------------------------
        // Replays DirAttrs for every dir row now that no more file
        // CREATEs in this shard can bump parent mtimes. Idempotent; no
        // ordering requirement (SETATTR on a child dir does not touch
        // its parent). Failures count normally.
        if !self.dir_restamp.is_empty() {
            tracing::info!(
                dirs = self.dir_restamp.len(),
                "shard-end directory attr re-stamp",
            );
            let rows = std::mem::take(&mut self.dir_restamp);
            let mut joins: JoinSet<Vec<(RowView, MoveOutcome)>> = JoinSet::new();
            for row in rows {
                let mover = Arc::clone(&self.mover);
                let inflight = self.inflight.clone();
                let fence = self.fence.clone();
                joins.spawn(async move {
                    if !fence.is_valid() {
                        return Vec::new();
                    }
                    let _permit = inflight.acquire(row.size).await;
                    if !fence.is_valid() {
                        return Vec::new();
                    }
                    let mo = mover.move_one(&row).await;
                    vec![(row, mo)]
                });
            }
            while let Some(joined) = joins.join_next().await {
                match joined {
                    Ok(results) => {
                        for (row, mo) in results {
                            // Only surface restamp *failures*; successes were
                            // already counted when the row ran in its batch.
                            if mo.result.is_err() {
                                self.record(&row, mo, &mut outcome);
                            }
                        }
                    }
                    Err(e) => {
                        outcome.files_failed += 1;
                        tracing::error!(error = ?e, "dir restamp task join failed");
                    }
                }
            }
            if !self.fence.is_valid() {
                return Ok(outcome.with_fenced(true));
            }
        }
        Ok(outcome)
    }

    /// Block between batches while the coord has the job paused. The
    /// batch boundary is the only safe point: no row is mid-copy, and
    /// the heartbeat keeps the claim alive for as long as the hold
    /// lasts.
    async fn hold_while_paused(&mut self) {
        let Some(rc) = self.run_control.as_mut() else {
            return;
        };
        if !rc.is_paused() {
            return;
        }
        tracing::info!("coord requested pause; holding at batch boundary");
        // A process stop ends the hold early; the caller's stop check
        // right after this returns then hands the shard back.
        tokio::select! {
            after = rc.wait_while_paused() => {
                tracing::info!(?after, "pause released; resuming shard");
            }
            _ = self.stop.cancelled() => {
                tracing::info!("stop requested while paused; leaving hold");
            }
        }
    }

    async fn run_batch(
        &mut self,
        batch: Batch,
        outcome: &mut ProcessOutcome,
    ) -> anyhow::Result<()> {
        if !self.fence.is_valid() {
            return Ok(());
        }

        // ---- Partition: hardlink groups, non-dir singletons, dirs ----
        //
        // Dirs are split out and run *after* everything else so file
        // commits inside them don't restamp their mtime. POSIX disallows
        // hardlinks on dirs, so dir rows can't appear in hardlink groups.
        let mut groups: HashMap<HardlinkKey, Vec<RowView>> = HashMap::new();
        let mut singletons: Vec<RowView> = Vec::new();
        let mut dirs: Vec<RowView> = Vec::new();
        for row in batch.rows {
            if row.file_type == FileTypeTag::Dir {
                self.dir_restamp.push(row.clone());
                dirs.push(row);
                continue;
            }
            match hardlink_key(&row) {
                Some(key) if row.nlink.unwrap_or(1) > 1 => {
                    if matches!(key, (None, _)) {
                        self.maybe_emit_fsid_fallback_warning(&row);
                    }
                    groups.entry(key).or_default().push(row);
                }
                _ => singletons.push(row),
            }
        }

        // Interleave singletons round-robin across parent directories.
        // Row order is directory-clustered (walker DFS), so the
        // in-flight window otherwise targets only a handful of parent
        // dirs at a time — and the NFS server serializes creates
        // within one parent (measured ~185 creates/s/dir on VAST).
        // Spreading the window across parents turns the per-dir
        // ceiling into a non-factor. Deterministic: first-seen parent
        // order, original row order within each parent.
        let singletons = interleave_by_parent(singletons);

        // ---- Phase 1: dispatch non-dir work concurrently -------------
        let mut joins: JoinSet<Vec<(RowView, MoveOutcome)>> = JoinSet::new();

        for (_key, rows) in groups {
            let mover = Arc::clone(&self.mover);
            let inflight = self.inflight.clone();
            let fence = self.fence.clone();
            joins.spawn(async move { run_group(mover, inflight, fence, rows).await });
        }

        for row in singletons {
            let mover = Arc::clone(&self.mover);
            let inflight = self.inflight.clone();
            let fence = self.fence.clone();
            let live = Arc::clone(&self.live);
            joins.spawn(async move {
                if !fence.is_valid() {
                    return Vec::new();
                }
                let _permit = inflight.acquire(row.size).await;
                // Fence may have tripped while waiting for the inflight
                // permit. Without rechecking here, all queued tasks
                // would run to completion even after fence trip, since
                // the pre-acquire check fired before they reached the
                // semaphore.
                if !fence.is_valid() {
                    return Vec::new();
                }
                live.rows_started
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let outcome = mover.move_one(&row).await;
                vec![(row, outcome)]
            });
        }

        while let Some(joined) = joins.join_next().await {
            match joined {
                Ok(results) => {
                    for (row, mo) in results {
                        self.record(&row, mo, outcome);
                    }
                }
                Err(e) => {
                    // A spawned task panicked or was cancelled. Don't
                    // panic the worker — log and keep collecting the
                    // rest. Treat as a failure so the operator sees it.
                    outcome.files_failed += 1;
                    tracing::error!(error = ?e, "dispatched task join failed");
                }
            }
        }

        // ---- Phase 2: dirs, deepest-first, level-parallel ------------
        //
        // Deepest-first is the invariant that keeps dir mtimes honest: a
        // parent's setattr must come after every deeper mkdir that could
        // restamp it. Fully sequential processing preserved that but
        // serializes ~5 ms metadata RPCs per dir, which dominates
        // dir-dense batches. Instead: group dirs into depth levels and
        // run each level concurrently with a barrier between levels.
        // Within one level no dir is an ancestor of another, and all
        // deeper levels (the only possible restampers) are fully
        // committed before a shallower level's setattr runs — the same
        // guarantee the sequential loop gave.
        if !dirs.is_empty() {
            sort_deepest_first(&mut dirs);
            let mut levels: Vec<Vec<RowView>> = Vec::new();
            let mut level_depth: Option<usize> = None;
            for row in dirs {
                let d = row.path.iter().filter(|&&b| b == b'/').count();
                if level_depth != Some(d) {
                    levels.push(Vec::new());
                    level_depth = Some(d);
                }
                levels.last_mut().expect("just pushed").push(row);
            }

            for level in levels {
                if !self.fence.is_valid() {
                    break;
                }
                let mut joins: JoinSet<Vec<(RowView, MoveOutcome)>> = JoinSet::new();
                for row in level {
                    let mover = Arc::clone(&self.mover);
                    let inflight = self.inflight.clone();
                    let fence = self.fence.clone();
                    joins.spawn(async move {
                        if !fence.is_valid() {
                            return Vec::new();
                        }
                        let _permit = inflight.acquire(row.size).await;
                        // Fence may have tripped while waiting for the
                        // inflight permit; recheck before the mover op.
                        if !fence.is_valid() {
                            return Vec::new();
                        }
                        let mo = mover.move_one(&row).await;
                        vec![(row, mo)]
                    });
                }
                while let Some(joined) = joins.join_next().await {
                    match joined {
                        Ok(results) => {
                            for (row, mo) in results {
                                self.record(&row, mo, outcome);
                            }
                        }
                        Err(e) => {
                            outcome.files_failed += 1;
                            tracing::error!(error = ?e, "dir task join failed");
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn maybe_emit_fsid_fallback_warning(&mut self, row: &RowView) {
        if self.fsid_fallback_warned {
            return;
        }
        self.fsid_fallback_warned = true;
        tracing::warn!(
            row_id = row.row_id,
            "row has inode without fsid; falling back to inode-only hardlink \
             grouping for this shard (see SCHEMA_CONTRACT.md \"Null attribute semantics\")",
        );
        self.mover
            .downgrade_sink()
            .record(row.row_id, &row.path, DowngradeKind::FsidUngrouped);
    }

    fn record(&mut self, row: &RowView, mo: MoveOutcome, outcome: &mut ProcessOutcome) {
        // Live counters first — same classification as record_outcome.
        {
            use std::sync::atomic::Ordering::Relaxed;
            self.live.rows_done.fetch_add(1, Relaxed);
            match (&mo.result, mo.strategy) {
                (Ok(()), _) => {
                    self.live.files_ok.fetch_add(1, Relaxed);
                    self.live.bytes_moved.fetch_add(mo.bytes_moved, Relaxed);
                }
                (Err(e), _) if matches!(e.phase, migration_core::records::FailurePhase::Fenced) => {
                    self.live.files_fenced.fetch_add(1, Relaxed);
                }
                (Err(_), _) => {
                    self.live.files_failed.fetch_add(1, Relaxed);
                }
            }
        }
        record_outcome(
            &row.path,
            mo,
            outcome,
            &self.failures,
            &self.throughput,
            &self.emitter,
        );
    }
}

/// Classify a single mover outcome into one of three sinks: success
/// counter, fenced counter, or the per-file failures sink. Factored
/// out of `ShardProcessor::record` so the Fenced special-case can be
/// unit-tested without standing up a Mover + libnfs pool.
fn record_outcome(
    row_path: &[u8],
    mo: MoveOutcome,
    outcome: &mut ProcessOutcome,
    failures: &FailureSink,
    throughput: &ThroughputCounter,
    emitter: &crate::coord_driver::EventEmitter,
) {
    match mo.result {
        Ok(()) => {
            outcome.files_ok += 1;
            // F05: a torn copy still commits (at-least-once; source
            // intact) — it is a success with a TornCopy downgrade
            // record already in the sink, not a failure. Count it so
            // the per-shard summary surfaces the tear.
            if mo.torn {
                outcome.files_torn += 1;
            }
            outcome.bytes_moved = outcome.bytes_moved.saturating_add(mo.bytes_moved);
            throughput.add(mo.bytes_moved);
            // Best-effort: push a per-file event draft. Disabled in
            // legacy mode; never blocks the processor.
            emitter.progress_ok(mo.bytes_moved);
        }
        // R8: a Fenced row is not a per-file failure. The mover saw
        // the fence trip immediately before its commit-point op
        // (rename / link / symlink) and bailed out without writing.
        // The shard's claim will terminate; the next reclaimer
        // copies this row. Recording it as a per-file failure would
        // (a) trip M5 assertion E (failures sink must be empty), and
        // (b) mislead operators into chasing a non-bug.
        Err(e) if e.phase == FailurePhase::Fenced => {
            outcome.files_fenced += 1;
            tracing::warn!(
                row_id = mo.row_id,
                strategy = ?mo.strategy,
                error = %e.error,
                "row fenced before commit; will be picked up after reclaim",
            );
            emitter.progress_fenced();
        }
        Err(e) => {
            outcome.files_failed += 1;
            tracing::warn!(
                row_id = mo.row_id,
                strategy = ?mo.strategy,
                phase = ?e.phase,
                error = %e.error,
                "file failed",
            );
            failures.record(mo.row_id, row_path, e.phase, e.error);
            emitter.progress_failed();
        }
    }
}

#[derive(Debug, Default)]
pub struct ProcessOutcome {
    pub rows_total: u64,
    pub files_ok: u64,
    pub files_failed: u64,
    /// R8: rows that bailed out at the mover's pre-commit fence check.
    /// Not a failure (no failures-sink record, no operator alert) — the
    /// row will be copied by whichever worker reclaims the shard next.
    /// Surfaced for observability so a spike is visible.
    pub files_fenced: u64,
    /// F05: rows that committed but were modified on the source while
    /// being copied (`MoveOutcome::torn`). Each also produced a
    /// `DowngradeKind::TornCopy` downgrade record. Counted inside
    /// `files_ok` — a torn copy is a success with a caveat, not a
    /// failure.
    pub files_torn: u64,
    pub bytes_moved: u64,
    pub fenced: bool,
    /// The shard was left at a batch boundary because the process was
    /// asked to stop (SIGTERM / SIGINT). Counters above cover only the
    /// batches that ran; the orchestrator releases the claim so a
    /// peer finishes the rest.
    pub interrupted: bool,
}

impl ProcessOutcome {
    pub fn with_fenced(mut self, v: bool) -> Self {
        self.fenced = v;
        self
    }

    pub fn with_interrupted(mut self, v: bool) -> Self {
        self.interrupted = v;
        self
    }
}

// =============================================================================
// Per-group sequential runner. Lives outside the impl so it can be
// `spawn`ed without borrowing `&mut self` across an await.
// =============================================================================

async fn run_group(
    mover: Arc<dyn FileMover>,
    inflight: InflightLimiter,
    fence: Fence,
    rows: Vec<RowView>,
) -> Vec<(RowView, MoveOutcome)> {
    let mut results = Vec::with_capacity(rows.len());
    let mut group_target: Option<Vec<u8>> = None;

    for row in rows {
        if !fence.is_valid() {
            break;
        }
        let _permit = inflight.acquire(row.size).await;
        // Fence may have tripped while waiting for the inflight permit;
        // recheck before launching the mover op so a tripped fence stops
        // the group at the next boundary instead of draining all queued
        // hardlink members.
        if !fence.is_valid() {
            break;
        }
        let outcome = match group_target.as_deref() {
            Some(target) => mover.move_hardlink(&row, target).await,
            None => mover.move_one(&row).await,
        };
        if outcome.result.is_ok() && group_target.is_none() && row.nlink.unwrap_or(1) > 1 {
            // Per R5: record the *final* path post-rename, not the
            // partial. `move_one` succeeded, so the rename happened.
            group_target = Some(row.path.clone());
        }
        results.push((row, outcome));
    }

    results
}

// =============================================================================
// Pure helpers — extracted so they can be unit-tested without a real
// mover or libnfs.
// =============================================================================

/// Build the hardlink-map key from a row. Returns `None` if the row
/// has no inode (it's not a hardlink target candidate). When the row
/// has an inode but no fsid, returns `Some((None, inode))` — the
/// caller is responsible for recording the FsidUngrouped downgrade.
pub(crate) fn hardlink_key(row: &RowView) -> Option<HardlinkKey> {
    row.inode.map(|inode| (row.fsid, inode))
}

/// Sort dir rows so deeper paths come first. Required by Phase 2 of
/// `run_batch`: applying attrs to a child dir doesn't bump the
/// parent's mtime, but `mkdir` of an empty child *does*. Doing the
/// deepest dirs first means any mkdir-of-empty-child happens before
/// the parent's setattr, so the parent's mtime is the one we set.
///
/// "Depth" here is just the count of `/` separators. Stable so
/// equal-depth paths keep their input order.
/// Round-robin rows across their parent directories: one row from each
/// parent in first-seen order, repeating until all queues drain. See
/// the call site in `run_batch` for why (per-directory create
/// serialization on the NFS server).
pub(crate) fn interleave_by_parent(rows: Vec<RowView>) -> Vec<RowView> {
    let total = rows.len();
    let mut queues: Vec<std::collections::VecDeque<RowView>> = Vec::new();
    let mut index: HashMap<Vec<u8>, usize> = HashMap::new();
    for row in rows {
        let parent = match row.path.iter().rposition(|&b| b == b'/') {
            Some(i) => row.path[..i].to_vec(),
            None => Vec::new(),
        };
        let qi = *index.entry(parent).or_insert_with(|| {
            queues.push(std::collections::VecDeque::new());
            queues.len() - 1
        });
        queues[qi].push_back(row);
    }
    let mut out = Vec::with_capacity(total);
    while out.len() < total {
        for q in queues.iter_mut() {
            if let Some(row) = q.pop_front() {
                out.push(row);
            }
        }
    }
    out
}

pub(crate) fn sort_deepest_first(dirs: &mut [RowView]) {
    dirs.sort_by(|a, b| {
        let da = a.path.iter().filter(|&&b| b == b'/').count();
        let db = b.path.iter().filter(|&&b| b == b'/').count();
        db.cmp(&da)
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use migration_core::schema::FileTypeTag;

    fn row(inode: Option<u64>, fsid: Option<u64>, nlink: Option<u32>) -> RowView {
        RowView {
            row_id: 1,
            path: b"/data/file".to_vec(),
            size: 1024,
            mtime_sec: None,
            mtime_nsec: None,
            atime_sec: None,
            atime_nsec: None,
            mode: 0o100644,
            uid: None,
            gid: None,
            nlink,
            inode,
            fsid,
            xattr_blob: None,
            symlink_target: None,
            file_type: FileTypeTag::Regular,
        }
    }

    #[test]
    fn hardlink_key_is_none_without_inode() {
        assert_eq!(hardlink_key(&row(None, Some(1), Some(2))), None);
    }

    #[test]
    fn hardlink_key_includes_fsid_when_present() {
        assert_eq!(
            hardlink_key(&row(Some(42), Some(7), Some(2))),
            Some((Some(7), 42)),
        );
    }

    #[test]
    fn hardlink_key_falls_back_to_none_fsid() {
        assert_eq!(
            hardlink_key(&row(Some(42), None, Some(2))),
            Some((None, 42)),
        );
    }

    #[test]
    fn rows_on_different_filesystems_do_not_collide() {
        let a = hardlink_key(&row(Some(42), Some(1), Some(2))).unwrap();
        let b = hardlink_key(&row(Some(42), Some(2), Some(2))).unwrap();
        assert_ne!(a, b);
    }

    fn dir_row(path: &[u8]) -> RowView {
        let mut r = row(None, None, None);
        r.path = path.to_vec();
        r.file_type = FileTypeTag::Dir;
        r
    }

    #[test]
    fn sort_deepest_first_orders_children_before_parents() {
        let mut dirs = vec![dir_row(b"/a"), dir_row(b"/a/b/c"), dir_row(b"/a/b")];
        sort_deepest_first(&mut dirs);
        let paths: Vec<&[u8]> = dirs.iter().map(|r| r.path.as_slice()).collect();
        assert_eq!(paths, vec![&b"/a/b/c"[..], &b"/a/b"[..], &b"/a"[..]]);
    }

    #[test]
    fn sort_deepest_first_is_stable_at_equal_depth() {
        let mut dirs = vec![dir_row(b"/a/x"), dir_row(b"/a/y"), dir_row(b"/a/z")];
        sort_deepest_first(&mut dirs);
        let paths: Vec<&[u8]> = dirs.iter().map(|r| r.path.as_slice()).collect();
        assert_eq!(paths, vec![&b"/a/x"[..], &b"/a/y"[..], &b"/a/z"[..]]);
    }

    // ---- R8: record_outcome Fenced special-case ------------------
    //
    // The classification rules are the load-bearing piece of R8 from
    // the operator's POV. M5 assertion E ("the failures/host-<id>/
    // prefix is empty or absent") becomes trivially false if a Fenced
    // row is mis-routed into the failures sink, so these tests pin
    // the routing for each of the three buckets.

    use migration_mover::strategy::Strategy;
    use migration_mover::MoveError;

    fn fenced_err() -> MoveError {
        MoveError::new(FailurePhase::Fenced, "FENCE_TRIPPED")
    }

    #[test]
    fn record_outcome_routes_fenced_to_counter_not_sink() {
        let sink = FailureSink::new();
        let throughput = ThroughputCounter::new();
        let mut outcome = ProcessOutcome::default();
        let mo = MoveOutcome {
            row_id: 42,
            strategy: Strategy::LibnfsIoUring,
            bytes_moved: 0,
            torn: false,
            result: Err(fenced_err()),
        };

        record_outcome(
            b"/data/file",
            mo,
            &mut outcome,
            &sink,
            &throughput,
            &crate::coord_driver::EventEmitter::disabled(),
        );

        assert_eq!(outcome.files_fenced, 1, "Fenced must bump files_fenced");
        assert_eq!(outcome.files_failed, 0, "Fenced must NOT bump files_failed");
        assert_eq!(outcome.files_ok, 0);
        assert_eq!(outcome.bytes_moved, 0);
        assert!(
            sink.is_empty(),
            "Fenced must NOT land in the failures sink (M5 assertion E)",
        );
    }

    #[test]
    fn record_outcome_routes_non_fenced_err_to_failures_sink() {
        // Sanity: pre-R8 routing for a genuine per-file failure must
        // still record to the sink and bump files_failed.
        let sink = FailureSink::new();
        let throughput = ThroughputCounter::new();
        let mut outcome = ProcessOutcome::default();
        let mo = MoveOutcome {
            row_id: 7,
            strategy: Strategy::LibnfsIoUring,
            bytes_moved: 0,
            torn: false,
            result: Err(MoveError::new(FailurePhase::Write, "ENOSPC")),
        };

        record_outcome(
            b"/data/file",
            mo,
            &mut outcome,
            &sink,
            &throughput,
            &crate::coord_driver::EventEmitter::disabled(),
        );

        assert_eq!(outcome.files_failed, 1);
        assert_eq!(outcome.files_fenced, 0);
        assert_eq!(sink.len(), 1, "non-Fenced Err must land in failures sink");
    }

    #[test]
    fn record_outcome_success_path_unchanged_by_r8() {
        // Regression guard: the Ok branch must still bump files_ok
        // and bytes_moved, untouched by the new Fenced arm.
        let sink = FailureSink::new();
        let throughput = ThroughputCounter::new();
        let mut outcome = ProcessOutcome::default();
        let mo = MoveOutcome {
            row_id: 1,
            strategy: Strategy::LibnfsIoUring,
            bytes_moved: 4096,
            torn: false,
            result: Ok(()),
        };

        record_outcome(
            b"/data/file",
            mo,
            &mut outcome,
            &sink,
            &throughput,
            &crate::coord_driver::EventEmitter::disabled(),
        );

        assert_eq!(outcome.files_ok, 1);
        assert_eq!(outcome.files_failed, 0);
        assert_eq!(outcome.files_fenced, 0);
        assert_eq!(outcome.bytes_moved, 4096);
        assert!(sink.is_empty());
    }

    // ---- F05: torn-copy wire-up -----------------------------------
    //
    // `pipelined_copy` detects a source modified mid-copy; the
    // classifier (`file_mover::classify_copy`) turns that into a
    // commit-and-record disposition; `copy_regular` surfaces it on
    // `MoveOutcome::torn`; and the per-shard summary counts it in
    // `files_torn`. Drive the real classifier end-to-end here (no
    // NFS needed) — see docs/work-items/MOVER_TORN_COPY_SURFACE.md.

    #[test]
    fn files_torn_increments_when_classifier_flags_torn() {
        use migration_mover::file_mover::classify_copy;
        use migration_mover::libnfs::asyncio::NfsStat64;
        use migration_mover::FileCopyResult;

        fn stat(size: u64, mtime: u64, ctime: u64) -> NfsStat64 {
            NfsStat64 {
                dev: 0,
                ino: 1,
                mode: 0o100644,
                nlink: 1,
                uid: 0,
                gid: 0,
                rdev: 0,
                size,
                blksize: 4096,
                blocks: 0,
                atime: 0,
                mtime,
                ctime,
                atime_nsec: 0,
                mtime_nsec: 0,
                ctime_nsec: 0,
                used: 0,
            }
        }

        let r = row(None, None, None);
        let copy = FileCopyResult {
            file_hash: [0; 16],
            bytes_copied: 1024,
            torn: true,
            pre_stat: stat(1024, 100, 100),
            post_stat: stat(1024, 200, 200),
        };
        let disposition = classify_copy(&copy, &r);
        assert!(disposition.commits(), "torn must still commit");

        let sink = FailureSink::new();
        let throughput = ThroughputCounter::new();
        let mut outcome = ProcessOutcome::default();
        let mo = MoveOutcome {
            row_id: r.row_id,
            strategy: Strategy::LibnfsIoUring,
            bytes_moved: copy.bytes_copied,
            torn: disposition.downgrade().is_some(),
            result: Ok(()),
        };

        record_outcome(
            b"/data/file",
            mo,
            &mut outcome,
            &sink,
            &throughput,
            &crate::coord_driver::EventEmitter::disabled(),
        );

        assert_eq!(outcome.files_torn, 1, "torn commit must bump files_torn");
        assert_eq!(outcome.files_ok, 1, "torn still counts as copied");
        assert_eq!(outcome.files_failed, 0, "torn is NOT a failure");
        assert!(sink.is_empty(), "torn must not land in the failures sink");
    }
}
