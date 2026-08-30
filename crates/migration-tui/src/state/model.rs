use super::{
    CommandStatus, CommandStatusKind, ConnectionStatus, ProgressDeltaHistory, RecentError,
    RecentErrors, RecentVerifyMismatch, RecentVerifyMismatches, UiState, VerifyStatus,
    COMMAND_STATUS_TTL_SECS,
};
use crate::theme::Theme;
use chrono::{DateTime, Utc};
use migration_control_protocol::schema::{
    ErrorBucket, EventEnvelope, EventKind, Job, JobId, OpLatency, Snapshot, Worker, WorkerId,
    WorkerState,
};
use std::collections::HashMap;

/// Build a [`Snapshot`] from the coord's REST views (F26 bootstrap,
/// COORD_PLAN §3.4): the `/jobs` pages plus each job's `/workers`
/// and `/errors`, stamped with the `/healthz` cursor. Pure — the
/// driver does the fetching, this only shapes the result for
/// [`AppState::replace_snapshot`].
///
/// Not reconstructable from REST (deliberately deferred): the
/// client-side chronological rings — recent errors, verify
/// mismatches, throughput samples. The coord only keeps aggregates,
/// so those tails resume from the live stream; the authoritative
/// counters (progress, error buckets, phases) are restored exactly.
pub fn snapshot_from_rest(
    jobs: Vec<Job>,
    workers: Vec<Worker>,
    error_buckets: Vec<(JobId, Vec<ErrorBucket>)>,
    last_seq: u64,
    now: DateTime<Utc>,
) -> Snapshot {
    let mut snap = Snapshot::empty(now);
    snap.last_seq = last_seq;
    snap.jobs = jobs.into_iter().map(|j| (j.id.clone(), j)).collect();
    snap.workers = workers.into_iter().map(|w| (w.id, w)).collect();
    snap.error_buckets = error_buckets.into_iter().collect();
    snap
}

/// Whole client-side state. `Snapshot` is the derived data model
/// (jobs, workers, errors, last_seq); the rest is TUI-only.
#[derive(Debug, Clone)]
pub struct AppState {
    pub snapshot: Snapshot,
    /// What `vamoose prepare` last reported, via the coord's
    /// `GET /prepare`; shown by the list view while there is no job.
    pub prepare: Option<migration_control_protocol::schema::PrepareResponse>,
    pub connection: ConnectionStatus,
    pub ui: UiState,
    /// Highest seq observed from the SSE stream. Always >= the
    /// snapshot's `last_seq` (the reducer also updates that field
    /// in-place). The resume cursor on reconnect is this value.
    pub last_seen_seq: u64,
    /// Count of SSE frames skipped because their `kind` is unknown
    /// to this build (F38 — a newer coord streaming to an older
    /// TUI). Surfaced in the banner so the operator knows the view
    /// may be missing event kinds this binary predates.
    pub unknown_events: u64,
    /// Per-job rolling throughput history derived from
    /// `ProgressDelta` events. Pruned to 5 min on every push.
    pub progress_windows: HashMap<JobId, ProgressDeltaHistory>,
    /// Per-(job, worker) rolling windows from the same
    /// `ProgressDelta` stream — the workers tab derives Files/s and
    /// MB/s from these client-side rather than trusting the
    /// worker-reported heartbeat rates (which are stubbed 0.0 for
    /// files on current workers).
    pub worker_windows: HashMap<(JobId, WorkerId), ProgressDeltaHistory>,
    /// Wall-clock of the last event seen from each worker on the
    /// stream. `LastHB` renders the fresher of this and the
    /// snapshot's `last_heartbeat` (heartbeat POSTs are not
    /// broadcast as events, so the snapshot value goes stale on a
    /// long-lived connection while the worker is demonstrably alive).
    pub worker_activity: HashMap<WorkerId, chrono::DateTime<chrono::Utc>>,
    /// Absolute-counter samples from `ProgressSync` frames — the
    /// preferred rate source (see `AbsoluteWindow`).
    pub job_syncs: HashMap<JobId, super::activity::AbsoluteWindow>,
    pub worker_syncs: HashMap<(JobId, WorkerId), super::activity::AbsoluteWindow>,
    /// Per-job recent-errors tail. The coord aggregates totals into
    /// `ErrorBucket`s on the snapshot; the TUI keeps a chronological
    /// ring so the Errors tab can show recent activity.
    pub recent_errors: HashMap<JobId, RecentErrors>,
    /// Per-job tail of `VerifyFileMismatch` events. The coord's
    /// Phase 1 reducer just streams these; the TUI captures them so
    /// the Verify tab has a recent-activity table.
    pub recent_verify_mismatches: HashMap<JobId, RecentVerifyMismatches>,
    /// Per-job verify lifecycle status (last start / complete / count).
    pub verify_status: HashMap<JobId, VerifyStatus>,
    /// Most recent palette-command result. Shown as a banner toast
    /// for a few seconds, then auto-cleared by the render layer
    /// (the renderer checks `now - at >= COMMAND_STATUS_TTL` and
    /// drops the field on render).
    pub command_status: Option<CommandStatus>,
    /// Color palette consumed by the render layer. Resolved at
    /// startup from `NO_COLOR` / `VAMOOSE_THEME`; preserved across
    /// reconnects so a `NO_COLOR=1` session never accidentally
    /// flashes color when the SSE link reconnects.
    pub theme: Theme,
}

impl AppState {
    /// Construct an empty state, before any traffic has arrived.
    /// `now` is captured as the snapshot's `written_at` so a fresh
    /// TUI doesn't look like it was loaded from an ancient
    /// persisted file.
    pub fn empty(now: DateTime<Utc>) -> Self {
        Self {
            snapshot: Snapshot::empty(now),
            prepare: None,
            connection: ConnectionStatus::Reconnecting {
                since: now,
                last_error: "not yet connected".into(),
            },
            ui: UiState::default(),
            last_seen_seq: 0,
            unknown_events: 0,
            progress_windows: HashMap::new(),
            worker_windows: HashMap::new(),
            worker_activity: HashMap::new(),
            job_syncs: HashMap::new(),
            worker_syncs: HashMap::new(),
            recent_errors: HashMap::new(),
            recent_verify_mismatches: HashMap::new(),
            verify_status: HashMap::new(),
            command_status: None,
            theme: Theme::default(),
        }
    }

    /// Replace the active theme. `vamoose tui` calls this with
    /// [`Theme::from_env`] right after constructing the initial
    /// state.
    pub fn with_theme(mut self, theme: Theme) -> Self {
        self.theme = theme;
        self
    }

    /// Record a successful command-status toast. The renderer
    /// auto-clears after [`COMMAND_STATUS_TTL_SECS`] seconds.
    pub fn set_command_ok(&mut self, message: impl Into<String>, now: DateTime<Utc>) {
        self.command_status = Some(CommandStatus {
            kind: CommandStatusKind::Ok,
            message: message.into(),
            at: now,
        });
    }

    /// Record an error command-status toast.
    pub fn set_command_error(&mut self, message: impl Into<String>, now: DateTime<Utc>) {
        self.command_status = Some(CommandStatus {
            kind: CommandStatusKind::Error,
            message: message.into(),
            at: now,
        });
    }

    /// Called by the render layer before drawing the banner. Drops
    /// the toast when it has been on screen longer than the TTL.
    pub fn tick_command_status(&mut self, now: DateTime<Utc>) {
        if let Some(s) = &self.command_status {
            let elapsed = now.signed_duration_since(s.at).num_seconds();
            if elapsed >= COMMAND_STATUS_TTL_SECS {
                self.command_status = None;
            }
        }
    }

    /// Apply one event envelope received from the SSE stream.
    ///
    /// Events arriving with `seq <= last_seen_seq` are silently
    /// discarded — they have already been folded into state on a
    /// prior pass (this happens when a reconnect overlaps with a
    /// late frame still in flight from the prior connection, or
    /// when the resume cursor is set slightly too low).
    ///
    /// Returns `true` if the event advanced state, `false` if it
    /// was discarded as a duplicate / stale.
    pub fn apply_envelope(&mut self, envelope: &EventEnvelope) -> bool {
        if envelope.seq <= self.last_seen_seq {
            return false;
        }
        self.snapshot.apply(envelope);
        self.last_seen_seq = envelope.seq;
        // Side-effect: feed the per-job throughput window if this
        // was a ProgressDelta. Pruning happens inside `push` so the
        // sample VecDeque stays bounded by the 5-min window.
        if let EventKind::ProgressDelta {
            job_id,
            worker_id,
            files_delta,
            bytes_delta,
            ..
        } = &envelope.kind
        {
            self.progress_windows
                .entry(job_id.clone())
                .or_default()
                .push(envelope.at, *bytes_delta, *files_delta);
            self.worker_windows
                .entry((job_id.clone(), *worker_id))
                .or_default()
                .push(envelope.at, *bytes_delta, *files_delta);
        }
        if let Some(w) = envelope
            .from_worker
            .or_else(|| envelope.kind.attributed_worker())
        {
            self.worker_activity.insert(w, envelope.at);
        }
        if let EventKind::ProgressSync {
            job_id,
            files_done,
            bytes_done,
            workers,
        } = &envelope.kind
        {
            self.job_syncs.entry(job_id.clone()).or_default().push(
                envelope.at,
                *files_done,
                *bytes_done,
            );
            for wc in workers {
                self.worker_syncs
                    .entry((job_id.clone(), wc.worker_id))
                    .or_default()
                    .push(envelope.at, wc.files_done, wc.bytes_done);
                self.worker_activity.insert(wc.worker_id, envelope.at);
            }
        }
        // Side-effect: append to the per-job recent-errors ring when
        // this was an `ErrorEmitted`. The dedup is implicit — we
        // only reach this branch after the early-return seq guard,
        // so a replayed event will never double-push.
        if let EventKind::ErrorEmitted {
            job_id,
            worker_id,
            class,
            path,
            retryable,
            message,
        } = &envelope.kind
        {
            self.recent_errors
                .entry(job_id.clone())
                .or_default()
                .push(RecentError {
                    at: envelope.at,
                    worker_id: *worker_id,
                    class: class.clone(),
                    path: path.clone(),
                    retryable: *retryable,
                    message: message.clone(),
                });
        }
        // Verify lifecycle bookkeeping. The coord drives phase
        // transitions but doesn't keep timestamps / counts on the
        // snapshot — those are captured here so the Verify tab can
        // narrate "started Xs ago, completed with N mismatches".
        match &envelope.kind {
            EventKind::VerifyStarted { job_id } => {
                self.verify_status
                    .entry(job_id.clone())
                    .or_default()
                    .last_started = Some(envelope.at);
            }
            EventKind::VerifyCompleted { job_id, mismatches } => {
                let s = self.verify_status.entry(job_id.clone()).or_default();
                s.last_completed = Some(envelope.at);
                s.last_mismatches = Some(*mismatches);
            }
            EventKind::VerifyFileMismatch {
                job_id,
                path,
                expected,
                got,
            } => {
                self.recent_verify_mismatches
                    .entry(job_id.clone())
                    .or_default()
                    .push(RecentVerifyMismatch {
                        at: envelope.at,
                        path: path.clone(),
                        expected: expected.clone(),
                        got: got.clone(),
                    });
            }
            _ => {}
        }
        true
    }

    /// Record one skipped unknown-kind frame (F38). Advances
    /// `last_seen_seq` through the same drop rule
    /// [`AppState::apply_envelope`] uses — `seq <= last_seen_seq` is
    /// a duplicate/stale replay and is discarded — so a reconnect
    /// overlap never double-counts, and the reconnect resume cursor
    /// moves past the frame instead of replaying it forever.
    ///
    /// Returns `true` when the frame advanced state.
    pub fn note_unknown_event(&mut self, seq: u64) -> bool {
        if seq <= self.last_seen_seq {
            return false;
        }
        self.last_seen_seq = seq;
        self.unknown_events += 1;
        true
    }

    /// Per-job recent-errors ring. Returns `None` for a job that has
    /// never emitted an error.
    pub fn recent_errors_for_job(&self, id: &JobId) -> Option<&RecentErrors> {
        self.recent_errors.get(id)
    }

    /// Per-job recent verify mismatches ring. `None` when no
    /// VerifyFileMismatch has streamed for the job yet.
    pub fn recent_verify_mismatches_for_job(&self, id: &JobId) -> Option<&RecentVerifyMismatches> {
        self.recent_verify_mismatches.get(id)
    }

    /// Per-job verify lifecycle status (last start, last complete,
    /// mismatch count from the last completion). Returns a zero-
    /// initialized `VerifyStatus` when no verify events have
    /// touched the job — the renderer treats absence and
    /// all-None the same.
    pub fn verify_status_for_job(&self, id: &JobId) -> VerifyStatus {
        self.verify_status.get(id).cloned().unwrap_or_default()
    }

    /// Total bytes-per-second across all jobs over the given
    /// rolling `window_secs`. Used by the banner's aggregate line.
    /// Aggregate files/s across every job, over `window_secs`.
    pub fn total_files_per_sec(&self, window_secs: i64, now: DateTime<Utc>) -> f64 {
        self.snapshot
            .jobs
            .keys()
            .map(|id| self.job_files_per_sec(id, window_secs, now))
            .sum()
    }

    /// Files/s for one job over `window_secs`. Prefers absolute
    /// `ProgressSync` samples (lossless); falls back to delta sums.
    pub fn job_files_per_sec(&self, job_id: &JobId, window_secs: i64, now: DateTime<Utc>) -> f64 {
        if let Some((f, _)) = self
            .job_syncs
            .get(job_id)
            .and_then(|w| w.rates(window_secs, now))
        {
            return f;
        }
        self.progress_windows
            .get(job_id)
            .map(|h| h.files_per_sec(window_secs, now))
            .unwrap_or(0.0)
    }

    /// Client-side (files/s, bytes/s) for one worker over `window_secs`.
    pub fn worker_rates(
        &self,
        job_id: &JobId,
        worker_id: WorkerId,
        window_secs: i64,
        now: DateTime<Utc>,
    ) -> (f64, f64) {
        if let Some(r) = self
            .worker_syncs
            .get(&(job_id.clone(), worker_id))
            .and_then(|w| w.rates(window_secs, now))
        {
            return r;
        }
        self.worker_windows
            .get(&(job_id.clone(), worker_id))
            .map(|h| {
                (
                    h.files_per_sec(window_secs, now),
                    h.bytes_per_sec(window_secs, now),
                )
            })
            .unwrap_or((0.0, 0.0))
    }

    /// Freshest liveness signal for a worker: the later of the
    /// snapshot's heartbeat stamp and the last streamed event.
    pub fn worker_freshness(
        &self,
        worker_id: WorkerId,
        snapshot_heartbeat: DateTime<Utc>,
    ) -> DateTime<Utc> {
        match self.worker_activity.get(&worker_id) {
            Some(t) if *t > snapshot_heartbeat => *t,
            _ => snapshot_heartbeat,
        }
    }

    pub fn total_bytes_per_sec(&self, window_secs: i64, now: DateTime<Utc>) -> f64 {
        self.progress_windows
            .values()
            .map(|h| h.bytes_per_sec(window_secs, now))
            .sum()
    }

    /// Per-job bytes-per-second over `window_secs`. None means no
    /// samples yet (most likely a job that hasn't started
    /// transferring; the render layer shows ` -- ` instead of 0
    /// for visual clarity).
    pub fn job_bytes_per_sec(
        &self,
        id: &JobId,
        window_secs: i64,
        now: DateTime<Utc>,
    ) -> Option<f64> {
        self.progress_windows
            .get(id)
            .filter(|h| !h.is_empty())
            .map(|h| h.bytes_per_sec(window_secs, now))
    }

    /// Replace the entire derived state with a freshly-fetched
    /// snapshot (e.g. after a forced full re-bootstrap via REST).
    /// The connection status and UI state are preserved.
    pub fn replace_snapshot(&mut self, snapshot: Snapshot) {
        self.last_seen_seq = self.last_seen_seq.max(snapshot.last_seq);
        self.snapshot = snapshot;
    }

    /// Mark the SSE link as connected (called by the connection
    /// driver on a successful response). Refreshes `last_traffic`
    /// to `now`.
    pub fn mark_connected(&mut self, now: DateTime<Utc>) {
        self.connection = ConnectionStatus::connected_now(now);
    }

    /// Refresh the connected-state `last_traffic` timestamp without
    /// transitioning to a different state — called on every SSE
    /// frame (event OR keepalive). No-op if not Connected (the
    /// connection driver hasn't recorded a successful connect yet).
    pub fn mark_traffic(&mut self, now: DateTime<Utc>) {
        if let ConnectionStatus::Connected { last_traffic } = &mut self.connection {
            *last_traffic = now;
        }
    }

    /// Mark the SSE link as reconnecting. Stamps `since` with `now`
    /// and surfaces `last_error` so the banner can show it.
    pub fn mark_reconnecting(&mut self, now: DateTime<Utc>, last_error: impl Into<String>) {
        self.connection = ConnectionStatus::Reconnecting {
            since: now,
            last_error: last_error.into(),
        };
    }

    /// Terminal disconnect. The banner goes red and the resume loop
    /// stops trying (used for non-retryable 4xx, e.g. bad token).
    pub fn mark_disconnected(&mut self, reason: impl Into<String>) {
        self.connection = ConnectionStatus::Disconnected {
            reason: reason.into(),
        };
    }

    // ----- Convenience accessors over the derived snapshot -----

    pub fn jobs_iter(&self) -> impl Iterator<Item = &Job> {
        self.snapshot.jobs.values()
    }

    pub fn job(&self, id: &JobId) -> Option<&Job> {
        self.snapshot.jobs.get(id)
    }

    pub fn workers_for_job(&self, id: &JobId) -> Vec<&Worker> {
        let job = match self.snapshot.jobs.get(id) {
            Some(j) => j,
            None => return Vec::new(),
        };
        job.assigned_workers
            .iter()
            .filter_map(|wid| self.snapshot.workers.get(wid))
            .collect()
    }

    /// Workers assigned to the job that are still talking to the
    /// coord. `assigned_workers` is a history — every worker that
    /// ever registered — so the headline count excludes the
    /// Disconnected ones.
    pub fn connected_worker_count(&self, id: &JobId) -> usize {
        self.workers_for_job(id)
            .into_iter()
            .filter(|w| w.state != WorkerState::Disconnected)
            .count()
    }

    pub fn worker(&self, id: &WorkerId) -> Option<&Worker> {
        self.snapshot.workers.get(id)
    }

    /// Roll the connected workers' latest latency windows up into one
    /// fleet picture. Per op: counts and wall time sum (so the mean is
    /// exact), and the *worst* p50/p95/p99/max across workers — the
    /// slow worker is the one the operator needs to see, and
    /// percentiles do not average. Busy shares are pair-weighted
    /// means. `None` until any connected worker has reported a
    /// window.
    pub fn fleet_latency(&self, id: &JobId) -> Option<FleetLatency> {
        let mut per_op: std::collections::BTreeMap<(u8, String), OpLatency> =
            std::collections::BTreeMap::new();
        let mut pairs_total = 0u64;
        let mut src_busy = 0.0f64;
        let mut dst_busy = 0.0f64;
        let mut s3_wait = 0.0f64;
        let mut reporting = 0usize;
        for w in self.workers_for_job(id) {
            if w.state == WorkerState::Disconnected {
                continue;
            }
            let Some(l) = &w.latency else { continue };
            reporting += 1;
            let pairs = l.pairs.max(1) as u64;
            pairs_total += pairs;
            src_busy += l.src_busy_pct * pairs as f64;
            dst_busy += l.dst_busy_pct * pairs as f64;
            s3_wait += l.s3_wait_pct;
            for o in &l.ops {
                let rank = match o.side.as_str() {
                    "src" => 0,
                    "dst" => 1,
                    _ => 2,
                };
                let e = per_op
                    .entry((rank, o.op.clone()))
                    .or_insert_with(|| OpLatency {
                        side: o.side.clone(),
                        op: o.op.clone(),
                        count: 0,
                        mean_us: 0,
                        p50_us: 0,
                        p95_us: 0,
                        p99_us: 0,
                        max_us: 0,
                        total_us: 0,
                    });
                e.count += o.count;
                e.total_us += o.total_us;
                e.p50_us = e.p50_us.max(o.p50_us);
                e.p95_us = e.p95_us.max(o.p95_us);
                e.p99_us = e.p99_us.max(o.p99_us);
                e.max_us = e.max_us.max(o.max_us);
            }
        }
        if reporting == 0 {
            return None;
        }
        let ops: Vec<OpLatency> = per_op
            .into_values()
            .map(|mut o| {
                o.mean_us = o.total_us.checked_div(o.count).unwrap_or(0);
                o
            })
            .collect();
        Some(FleetLatency {
            workers: reporting,
            src_busy_pct: src_busy / pairs_total.max(1) as f64,
            dst_busy_pct: dst_busy / pairs_total.max(1) as f64,
            s3_wait_pct: s3_wait / reporting as f64,
            ops,
        })
    }

    pub fn errors_for_job(&self, id: &JobId) -> &[ErrorBucket] {
        self.snapshot
            .error_buckets
            .get(id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    pub fn last_seq(&self) -> u64 {
        self.last_seen_seq
    }
}

/// Fleet roll-up of the workers' latency windows — see
/// [`AppState::fleet_latency`].
#[derive(Debug, Clone, PartialEq)]
pub struct FleetLatency {
    /// Connected workers that have reported a window.
    pub workers: usize,
    pub src_busy_pct: f64,
    pub dst_busy_pct: f64,
    pub s3_wait_pct: f64,
    /// Sorted src → dst → s3, then by op name.
    pub ops: Vec<OpLatency>,
}

impl FleetLatency {
    /// The one-line answer to "who is slow?". Thresholds: a side the
    /// connection pairs spend ≥ 85 % of their time waiting on is the
    /// bottleneck; when neither side reaches 60 % the servers have
    /// headroom and the client (CPU, scheduling, S3 gaps between
    /// shards) is what limits the rate.
    pub fn verdict(&self) -> String {
        let src = self.src_busy_pct;
        let dst = self.dst_busy_pct;
        let s3 = self.s3_wait_pct;
        if dst >= 85.0 && dst >= src {
            format!(
                "destination-bound (pairs wait on dest {dst:.0}% of the time, source {src:.0}%)"
            )
        } else if src >= 85.0 {
            format!("source-bound (pairs wait on source {src:.0}% of the time, dest {dst:.0}%)")
        } else if s3 >= 50.0 {
            format!(
                "S3-bound (in S3 calls {s3:.0}% of the window; source {src:.0}%, dest {dst:.0}%)"
            )
        } else if src.max(dst) < 60.0 {
            format!("client-bound (source {src:.0}%, dest {dst:.0}% — both servers have headroom)")
        } else {
            format!("mixed (source {src:.0}%, dest {dst:.0}%, S3 {s3:.0}%)")
        }
    }
}
