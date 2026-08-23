use super::activity::MAX_WINDOW_SECS;
use super::*;
use chrono::{DateTime, TimeZone, Utc};
use migration_control_protocol::schema::{ConfigHash, EventKind, SCHEMA_VERSION};
use migration_control_protocol::schema::{
    ErrorBucket, ErrorClass, EventEnvelope, Job, JobId, Snapshot, Worker, WorkerId,
};

fn at(secs: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(secs, 0).unwrap()
}

fn jid(s: &str) -> JobId {
    JobId::new(s).unwrap()
}

fn job_created(seq: u64, job: &str) -> EventEnvelope {
    EventEnvelope {
        seq,
        at: at(seq as i64),
        schema_version: SCHEMA_VERSION,
        worker_at: None,
        client_seq: None,
        from_worker: None,
        kind: EventKind::JobCreated {
            job_id: jid(job),
            name: format!("{job}-mig"),
            source: "nfs://src".into(),
            dest: "nfs://dst".into(),
            owner: "test".into(),
            config_hash: ConfigHash("ab".into()),
            total_files: 0,
            total_bytes: 0,
        },
    }
}

fn progress(seq: u64, job: &str, files: u64, bytes: u64) -> EventEnvelope {
    EventEnvelope {
        seq,
        at: at(seq as i64),
        schema_version: SCHEMA_VERSION,
        worker_at: None,
        client_seq: None,
        from_worker: None,
        kind: EventKind::ProgressDelta {
            job_id: jid(job),
            worker_id: WorkerId::new(),
            files_delta: files,
            bytes_delta: bytes,
            errors_delta: 0,
        },
    }
}

#[test]
fn empty_state_is_pre_connect() {
    let s = AppState::empty(at(0));
    assert_eq!(s.last_seen_seq, 0);
    assert!(s.snapshot.jobs.is_empty());
    assert!(matches!(
        s.connection,
        ConnectionStatus::Reconnecting { .. }
    ));
}

#[test]
fn apply_envelope_advances_state_and_seq() {
    let mut s = AppState::empty(at(0));
    assert!(s.apply_envelope(&job_created(1, "bobby")));
    assert_eq!(s.last_seen_seq, 1);
    assert!(s.snapshot.jobs.contains_key(&jid("bobby")));
    assert!(s.apply_envelope(&progress(2, "bobby", 5, 1024)));
    assert_eq!(s.last_seen_seq, 2);
    let j = s.job(&jid("bobby")).unwrap();
    assert_eq!(j.progress.files_done, 5);
    assert_eq!(j.progress.bytes_done, 1024);
}

#[test]
fn duplicate_or_stale_envelope_is_dropped() {
    let mut s = AppState::empty(at(0));
    s.apply_envelope(&job_created(1, "bobby"));
    s.apply_envelope(&progress(2, "bobby", 5, 1024));
    // Re-applying seq 2 must not double-count.
    assert!(!s.apply_envelope(&progress(2, "bobby", 5, 1024)));
    // Out-of-order older seq is also dropped.
    assert!(!s.apply_envelope(&progress(1, "bobby", 99, 99)));
    let j = s.job(&jid("bobby")).unwrap();
    assert_eq!(j.progress.files_done, 5);
    assert_eq!(j.progress.bytes_done, 1024);
    assert_eq!(s.last_seen_seq, 2);
}

#[test]
fn note_unknown_event_advances_cursor_and_dedups() {
    // F38: unknown-kind frames advance the resume cursor through
    // the same drop rule apply_envelope uses, so duplicates from
    // a reconnect overlap never double-count.
    let mut s = AppState::empty(at(0));
    s.apply_envelope(&job_created(1, "bobby"));
    assert!(s.note_unknown_event(5));
    assert_eq!(s.last_seen_seq, 5);
    assert_eq!(s.unknown_events, 1);
    // Duplicate / stale seqs are dropped.
    assert!(!s.note_unknown_event(5));
    assert!(!s.note_unknown_event(2));
    assert_eq!(s.unknown_events, 1);
    assert_eq!(s.last_seen_seq, 5);
    // A later unknown counts again.
    assert!(s.note_unknown_event(9));
    assert_eq!(s.unknown_events, 2);
    assert_eq!(s.last_seen_seq, 9);
}

#[test]
fn replace_snapshot_keeps_last_seen_when_snapshot_is_older() {
    let mut s = AppState::empty(at(0));
    s.apply_envelope(&job_created(1, "bobby"));
    s.apply_envelope(&progress(2, "bobby", 5, 1024));
    assert_eq!(s.last_seen_seq, 2);

    let mut older = Snapshot::empty(at(100));
    older.last_seq = 1;
    s.replace_snapshot(older);
    // We had already seen seq 2; do not regress.
    assert_eq!(s.last_seen_seq, 2);
    // But the data is now whatever the (older, empty) snapshot
    // says — replace is a hard replace.
    assert!(s.snapshot.jobs.is_empty());
}

#[test]
fn replace_snapshot_newer_wins_older_kept() {
    // F26 bootstrap path: a REST snapshot ahead of anything the
    // stream has shown must advance the resume cursor to the
    // snapshot's last_seq; a stale one must never regress it.
    let mut s = AppState::empty(at(0));
    s.apply_envelope(&job_created(1, "bobby"));
    assert_eq!(s.last_seen_seq, 1);

    let mut newer = Snapshot::empty(at(100));
    newer.last_seq = 10;
    s.replace_snapshot(newer);
    assert_eq!(s.last_seen_seq, 10, "newer snapshot wins");

    let mut older = Snapshot::empty(at(200));
    older.last_seq = 3;
    s.replace_snapshot(older);
    assert_eq!(s.last_seen_seq, 10, "older snapshot keeps the cursor");
}

#[test]
fn snapshot_from_rest_builds_full_snapshot() {
    // F26: pure conversion from the REST views (/jobs pages +
    // per-job /workers and /errors) into the Snapshot shape
    // replace_snapshot consumes. Donor state derives the same
    // Job/Worker/bucket values the coord would serve.
    let mut donor = AppState::empty(at(0));
    donor.apply_envelope(&job_created(1, "alpha"));
    let w = WorkerId::new();
    donor.apply_envelope(&EventEnvelope {
        seq: 2,
        at: at(2),
        schema_version: SCHEMA_VERSION,
        worker_at: None,
        client_seq: None,
        from_worker: None,
        kind: EventKind::WorkerJoined {
            worker_id: w,
            job_id: jid("alpha"),
            host: "h".into(),
            pid: 1,
            start_time: at(0),
            version: "0.6".into(),
        },
    });
    donor.apply_envelope(&EventEnvelope {
        seq: 3,
        at: at(3),
        schema_version: SCHEMA_VERSION,
        worker_at: None,
        client_seq: None,
        from_worker: None,
        kind: EventKind::ErrorEmitted {
            job_id: jid("alpha"),
            worker_id: w,
            class: ErrorClass::Permission,
            path: "/p/x".into(),
            retryable: false,
            message: "denied".into(),
        },
    });

    let jobs: Vec<Job> = donor.snapshot.jobs.values().cloned().collect();
    let workers: Vec<Worker> = donor.snapshot.workers.values().cloned().collect();
    let buckets: Vec<(JobId, Vec<ErrorBucket>)> = donor
        .snapshot
        .error_buckets
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    let snap = snapshot_from_rest(jobs, workers, buckets, 3, at(50));
    assert_eq!(snap.last_seq, 3);
    assert_eq!(snap.jobs, donor.snapshot.jobs);
    assert_eq!(snap.workers, donor.snapshot.workers);
    assert_eq!(snap.error_buckets, donor.snapshot.error_buckets);
    assert_eq!(snap.written_at, at(50));
}

#[test]
fn connection_status_transitions() {
    let mut s = AppState::empty(at(0));
    // Empty -> Reconnecting by construction.
    s.mark_connected(at(1));
    assert!(matches!(s.connection, ConnectionStatus::Connected { .. }));
    s.mark_traffic(at(5));
    if let ConnectionStatus::Connected { last_traffic } = &s.connection {
        assert_eq!(*last_traffic, at(5));
    } else {
        panic!("must be Connected");
    }
    s.mark_reconnecting(at(10), "connection reset");
    match &s.connection {
        ConnectionStatus::Reconnecting { last_error, .. } => {
            assert_eq!(last_error, "connection reset");
        }
        _ => panic!("must be Reconnecting"),
    }
    s.mark_disconnected("bad token");
    match &s.connection {
        ConnectionStatus::Disconnected { reason } => assert_eq!(reason, "bad token"),
        _ => panic!("must be Disconnected"),
    }
}

#[test]
fn mark_traffic_is_noop_when_not_connected() {
    let mut s = AppState::empty(at(0));
    // empty() leaves connection in Reconnecting; mark_traffic must
    // NOT promote that to Connected.
    s.mark_traffic(at(10));
    assert!(matches!(
        s.connection,
        ConnectionStatus::Reconnecting { .. }
    ));
}

#[test]
fn workers_for_job_filters_to_assigned() {
    let mut s = AppState::empty(at(0));
    s.apply_envelope(&job_created(1, "bobby"));
    let w1 = WorkerId::new();
    s.apply_envelope(&EventEnvelope {
        seq: 2,
        at: at(2),
        schema_version: SCHEMA_VERSION,
        worker_at: None,
        client_seq: None,
        from_worker: None,
        kind: EventKind::WorkerJoined {
            worker_id: w1,
            job_id: jid("bobby"),
            host: "h".into(),
            pid: 1,
            start_time: at(0),
            version: "0.6".into(),
        },
    });
    let workers = s.workers_for_job(&jid("bobby"));
    assert_eq!(workers.len(), 1);
    assert_eq!(workers[0].id, w1);
    assert_eq!(s.worker(&w1).map(|w| &w.host[..]), Some("h"));
}

// ----- JobSort cycle + label -----

#[test]
fn jobsort_cycle_visits_all_four_then_wraps() {
    let order = [
        JobSort::ById,
        JobSort::ByPhase,
        JobSort::ByProgressDesc,
        JobSort::ByErrorsDesc,
    ];
    let mut cur = order[0];
    for next in order.iter().skip(1) {
        cur = cur.cycle();
        assert_eq!(cur, *next);
    }
    cur = cur.cycle();
    assert_eq!(cur, JobSort::ById, "must wrap from last back to first");
}

#[test]
fn jobsort_labels_are_distinct_single_words() {
    let labels = [
        JobSort::ById.label(),
        JobSort::ByPhase.label(),
        JobSort::ByProgressDesc.label(),
        JobSort::ByErrorsDesc.label(),
    ];
    // No duplicates.
    let mut sorted: Vec<&str> = labels.to_vec();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), 4);
    // Each fits within ~10 characters for the banner.
    for l in labels {
        assert!(l.len() <= 10, "label too long: {l:?}");
        assert!(!l.contains(' '), "label has spaces: {l:?}");
    }
}

// ----- ProgressDeltaHistory + AppState wiring -----

fn prog_at(seq: u64, secs: i64, job: &str, bytes: u64) -> EventEnvelope {
    EventEnvelope {
        seq,
        at: at(secs),
        schema_version: migration_control_protocol::schema::SCHEMA_VERSION,
        worker_at: None,
        client_seq: None,
        from_worker: None,
        kind: EventKind::ProgressDelta {
            job_id: jid(job),
            worker_id: WorkerId::new(),
            files_delta: 1,
            bytes_delta: bytes,
            errors_delta: 0,
        },
    }
}

#[test]
fn history_push_then_bytes_per_sec_sums_within_window() {
    let mut h = ProgressDeltaHistory::default();
    h.push(at(0), 1000, 0);
    h.push(at(1), 2000, 0);
    h.push(at(2), 3000, 0);
    // 6000 bytes over the last 10 seconds → 600 B/s.
    assert!((h.bytes_per_sec(10, at(10)) - 600.0).abs() < 0.0001);
    // Inclusive window: at now=at(2), the 2-second window is
    // [at(0), at(2)] — all three samples fall in it. 6000 B
    // over 2 s = 3000 B/s.
    let v = h.bytes_per_sec(2, at(2));
    assert!((v - 3000.0).abs() < 0.0001, "got {v}");
    // A tighter 1-second window at now=at(2) includes only
    // samples at t ≥ at(1): 2000 + 3000 = 5000 over 1 s.
    let v = h.bytes_per_sec(1, at(2));
    assert!((v - 5000.0).abs() < 0.0001, "got {v}");
}

#[test]
fn history_prunes_entries_older_than_max_window() {
    let mut h = ProgressDeltaHistory::default();
    // First entry far in the past.
    h.push(at(0), 100, 0);
    // Push enough later that the first is dropped.
    h.push(at(MAX_WINDOW_SECS + 1), 200, 0);
    assert_eq!(h.len(), 1, "history pruned to 1 entry");
    // Only the surviving entry contributes to the window.
    assert!((h.bytes_per_sec(10, at(MAX_WINDOW_SECS + 1)) - 20.0).abs() < 0.0001);
}

#[test]
fn history_empty_window_yields_zero() {
    let h = ProgressDeltaHistory::default();
    assert_eq!(h.bytes_per_sec(10, at(0)), 0.0);
    assert_eq!(h.bytes_per_sec(60, at(0)), 0.0);
}

#[test]
fn history_window_secs_zero_or_negative_yields_zero() {
    let mut h = ProgressDeltaHistory::default();
    h.push(at(0), 1000, 0);
    assert_eq!(h.bytes_per_sec(0, at(0)), 0.0);
    assert_eq!(h.bytes_per_sec(-5, at(0)), 0.0);
}

#[test]
fn appstate_apply_envelope_feeds_progress_window() {
    let mut s = AppState::empty(at(0));
    s.apply_envelope(&job_created(1, "bobby"));
    // No progress yet — no window for this job.
    assert!(!s.progress_windows.contains_key(&jid("bobby")));
    s.apply_envelope(&prog_at(2, 1, "bobby", 1024));
    s.apply_envelope(&prog_at(3, 2, "bobby", 2048));
    // Window now exists with 2 samples, totaling 3072 bytes.
    let h = s.progress_windows.get(&jid("bobby")).expect("window");
    assert_eq!(h.len(), 2);
    let rate = s.job_bytes_per_sec(&jid("bobby"), 10, at(10));
    assert!(rate.is_some());
    assert!((rate.unwrap() - 307.2).abs() < 0.01);
}

#[test]
fn appstate_duplicate_envelope_does_not_double_count_window() {
    let mut s = AppState::empty(at(0));
    s.apply_envelope(&job_created(1, "bobby"));
    s.apply_envelope(&prog_at(2, 1, "bobby", 1024));
    // Re-applying same seq must be dropped at apply_envelope
    // level — window must not gain a second sample.
    s.apply_envelope(&prog_at(2, 1, "bobby", 1024));
    let h = s.progress_windows.get(&jid("bobby")).expect("window");
    assert_eq!(h.len(), 1);
}

#[test]
fn appstate_total_bytes_per_sec_sums_across_jobs() {
    let mut s = AppState::empty(at(0));
    s.apply_envelope(&job_created(1, "alpha"));
    s.apply_envelope(&job_created(2, "bravo"));
    s.apply_envelope(&prog_at(3, 0, "alpha", 1000));
    s.apply_envelope(&prog_at(4, 0, "bravo", 2000));
    // Over the last 10 seconds, alpha=100, bravo=200 → total=300.
    let total = s.total_bytes_per_sec(10, at(10));
    assert!((total - 300.0).abs() < 0.0001, "got {total}");
}

// ----- InputMode default -----

#[test]
fn default_input_mode_is_normal() {
    let ui = UiState::default();
    assert!(matches!(ui.input_mode, InputMode::Normal));
}

// ----- View / Tab (Phase 5a) -----

#[test]
fn default_view_is_list() {
    let ui = UiState::default();
    assert!(matches!(ui.view, View::List));
}

#[test]
fn tab_cycle_next_walks_all_five_then_wraps() {
    let order = [
        Tab::Overview,
        Tab::Workers,
        Tab::Errors,
        Tab::Plan,
        Tab::Verify,
    ];
    let mut cur = order[0];
    for next in order.iter().skip(1) {
        cur = cur.cycle_next();
        assert_eq!(cur, *next);
    }
    cur = cur.cycle_next();
    assert_eq!(cur, Tab::Overview, "Verify must wrap to Overview");
}

#[test]
fn tab_cycle_prev_walks_all_five_in_reverse_and_wraps() {
    let mut cur = Tab::Overview;
    let reverse = [
        Tab::Verify,
        Tab::Plan,
        Tab::Errors,
        Tab::Workers,
        Tab::Overview,
    ];
    for expected in reverse {
        cur = cur.cycle_prev();
        assert_eq!(cur, expected);
    }
}

#[test]
fn tab_labels_are_distinct_and_human_readable() {
    let labels: Vec<&'static str> = Tab::all().iter().map(|t| t.label()).collect();
    let mut sorted = labels.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), 5, "labels must be unique");
    for l in labels {
        assert!(!l.is_empty());
        // Tab bar real estate is tight — keep labels ≤ 10 chars.
        assert!(l.len() <= 10, "label too long: {l:?}");
    }
}

#[test]
fn tab_all_returns_canonical_order() {
    let all = Tab::all();
    assert_eq!(
        all,
        [
            Tab::Overview,
            Tab::Workers,
            Tab::Errors,
            Tab::Plan,
            Tab::Verify,
        ]
    );
}

// ----- WorkerSort (Phase 5b) -----

#[test]
fn worker_sort_default_is_mbps_desc() {
    // Operator-leaderboard default — degraded nodes sink so a
    // quick glance at the Workers tab surfaces the laggard.
    let s = WorkerSort::default();
    assert_eq!(s, WorkerSort::ByMbpsDesc);
}

#[test]
fn worker_sort_cycle_walks_all_four_then_wraps() {
    let order = [
        WorkerSort::ByMbpsDesc,
        WorkerSort::ByFilesDesc,
        WorkerSort::ByErrorsDesc,
        WorkerSort::ByHost,
    ];
    let mut cur = order[0];
    for next in order.iter().skip(1) {
        cur = cur.cycle();
        assert_eq!(cur, *next);
    }
    cur = cur.cycle();
    assert_eq!(cur, WorkerSort::ByMbpsDesc, "must wrap");
}

#[test]
fn worker_sort_labels_are_distinct_and_short() {
    let labels: Vec<&'static str> = [
        WorkerSort::ByMbpsDesc.label(),
        WorkerSort::ByFilesDesc.label(),
        WorkerSort::ByErrorsDesc.label(),
        WorkerSort::ByHost.label(),
    ]
    .to_vec();
    let mut sorted = labels.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), 4);
    for l in labels {
        assert!(l.len() <= 10);
        assert!(!l.is_empty());
    }
}

#[test]
fn modal_default_is_none() {
    let ui = UiState::default();
    assert!(ui.modal.is_none());
}

// ----- RecentErrors ring (Phase 5c) -----

fn err_evt(seq: u64, job: &str, path: &str, message: &str) -> EventEnvelope {
    EventEnvelope {
        seq,
        at: at(seq as i64),
        schema_version: migration_control_protocol::schema::SCHEMA_VERSION,
        worker_at: None,
        client_seq: None,
        from_worker: None,
        kind: EventKind::ErrorEmitted {
            job_id: jid(job),
            worker_id: WorkerId::new(),
            class: ErrorClass::Permission,
            path: path.into(),
            retryable: false,
            message: message.into(),
        },
    }
}

#[test]
fn recent_errors_push_caps_at_limit() {
    let mut r = RecentErrors::default();
    for i in 0..(RECENT_ERRORS_PER_JOB + 10) {
        r.push(RecentError {
            at: at(i as i64),
            worker_id: WorkerId::new(),
            class: ErrorClass::Other(format!("k{i}")),
            path: format!("/p/{i}"),
            retryable: false,
            message: format!("m{i}"),
        });
    }
    assert_eq!(r.len(), RECENT_ERRORS_PER_JOB);
    // Oldest (entries 0..9) should have dropped; the FIRST item
    // in the tail is now entry 10.
    let tail: Vec<_> = r.tail(usize::MAX).collect();
    assert_eq!(tail.first().unwrap().path, "/p/10");
    assert_eq!(
        tail.last().unwrap().path,
        format!("/p/{}", RECENT_ERRORS_PER_JOB + 9)
    );
}

#[test]
fn recent_errors_tail_n_returns_last_n_newest_last() {
    let mut r = RecentErrors::default();
    for i in 0..5 {
        r.push(RecentError {
            at: at(i),
            worker_id: WorkerId::new(),
            class: ErrorClass::Permission,
            path: format!("/p/{i}"),
            retryable: false,
            message: "m".into(),
        });
    }
    let last3: Vec<_> = r.tail(3).collect();
    assert_eq!(last3.len(), 3);
    assert_eq!(last3[0].path, "/p/2");
    assert_eq!(last3[2].path, "/p/4");
}

#[test]
fn appstate_apply_envelope_feeds_recent_errors() {
    let mut s = AppState::empty(at(0));
    s.apply_envelope(&job_created(1, "alpha"));
    s.apply_envelope(&err_evt(2, "alpha", "/path/a", "perm denied"));
    s.apply_envelope(&err_evt(3, "alpha", "/path/b", "another"));
    let r = s.recent_errors_for_job(&jid("alpha")).expect("ring");
    assert_eq!(r.len(), 2);
    let tail: Vec<_> = r.tail(usize::MAX).collect();
    assert_eq!(tail[0].path, "/path/a");
    assert_eq!(tail[0].message, "perm denied");
    assert_eq!(tail[1].path, "/path/b");
}

#[test]
fn appstate_duplicate_envelope_does_not_double_push_recent_errors() {
    let mut s = AppState::empty(at(0));
    s.apply_envelope(&job_created(1, "alpha"));
    s.apply_envelope(&err_evt(2, "alpha", "/p/x", "boom"));
    // Replay same seq — dedup at apply_envelope blocks it.
    s.apply_envelope(&err_evt(2, "alpha", "/p/x", "boom"));
    let r = s.recent_errors_for_job(&jid("alpha")).expect("ring");
    assert_eq!(r.len(), 1);
}

#[test]
fn appstate_recent_errors_for_job_returns_none_when_unseen() {
    let s = AppState::empty(at(0));
    assert!(s.recent_errors_for_job(&jid("alpha")).is_none());
}

// ----- Verify lifecycle + mismatches ring (Phase 5d) -----

fn verify_mismatch_evt(
    seq: u64,
    secs: i64,
    job: &str,
    path: &str,
    expected: &str,
    got: &str,
) -> EventEnvelope {
    EventEnvelope {
        seq,
        at: at(secs),
        schema_version: migration_control_protocol::schema::SCHEMA_VERSION,
        worker_at: None,
        client_seq: None,
        from_worker: None,
        kind: EventKind::VerifyFileMismatch {
            job_id: jid(job),
            path: path.into(),
            expected: expected.into(),
            got: got.into(),
        },
    }
}

#[test]
fn appstate_verify_started_then_completed_records_timestamps_and_count() {
    let mut s = AppState::empty(at(0));
    s.apply_envelope(&job_created(1, "alpha"));
    s.apply_envelope(&EventEnvelope {
        seq: 2,
        at: at(100),
        schema_version: migration_control_protocol::schema::SCHEMA_VERSION,
        worker_at: None,
        client_seq: None,
        from_worker: None,
        kind: EventKind::VerifyStarted {
            job_id: jid("alpha"),
        },
    });
    s.apply_envelope(&EventEnvelope {
        seq: 3,
        at: at(200),
        schema_version: migration_control_protocol::schema::SCHEMA_VERSION,
        worker_at: None,
        client_seq: None,
        from_worker: None,
        kind: EventKind::VerifyCompleted {
            job_id: jid("alpha"),
            mismatches: 7,
        },
    });
    let st = s.verify_status_for_job(&jid("alpha"));
    assert_eq!(st.last_started, Some(at(100)));
    assert_eq!(st.last_completed, Some(at(200)));
    assert_eq!(st.last_mismatches, Some(7));
}

#[test]
fn appstate_verify_mismatch_events_feed_ring() {
    let mut s = AppState::empty(at(0));
    s.apply_envelope(&job_created(1, "alpha"));
    s.apply_envelope(&verify_mismatch_evt(
        2, 10, "alpha", "/p/a", "size:100", "size:101",
    ));
    s.apply_envelope(&verify_mismatch_evt(
        3, 20, "alpha", "/p/b", "size:200", "size:0",
    ));
    let r = s
        .recent_verify_mismatches_for_job(&jid("alpha"))
        .expect("ring");
    assert_eq!(r.len(), 2);
    let tail: Vec<_> = r.tail(usize::MAX).collect();
    assert_eq!(tail[0].path, "/p/a");
    assert_eq!(tail[0].expected, "size:100");
    assert_eq!(tail[1].path, "/p/b");
    assert_eq!(tail[1].got, "size:0");
}

#[test]
fn recent_verify_mismatches_ring_caps_at_limit() {
    let mut r = RecentVerifyMismatches::default();
    for i in 0..(RECENT_VERIFY_MISMATCHES_PER_JOB + 5) {
        r.push(RecentVerifyMismatch {
            at: at(i as i64),
            path: format!("/p/{i}"),
            expected: "x".into(),
            got: "y".into(),
        });
    }
    assert_eq!(r.len(), RECENT_VERIFY_MISMATCHES_PER_JOB);
    // Oldest 5 dropped → first surviving entry is index 5.
    let tail: Vec<_> = r.tail(usize::MAX).collect();
    assert_eq!(tail.first().unwrap().path, "/p/5");
}

#[test]
fn verify_status_for_unseen_job_is_all_none() {
    let s = AppState::empty(at(0));
    let st = s.verify_status_for_job(&jid("alpha"));
    assert_eq!(st, VerifyStatus::default());
}

#[test]
fn duplicate_verify_envelope_does_not_double_push_ring() {
    let mut s = AppState::empty(at(0));
    s.apply_envelope(&job_created(1, "alpha"));
    s.apply_envelope(&verify_mismatch_evt(2, 10, "alpha", "/p/x", "e", "g"));
    // Replay same seq.
    s.apply_envelope(&verify_mismatch_evt(2, 10, "alpha", "/p/x", "e", "g"));
    let r = s
        .recent_verify_mismatches_for_job(&jid("alpha"))
        .expect("ring");
    assert_eq!(r.len(), 1);
}

// ----- Command status toast (Phase 6a) -----

#[test]
fn set_command_ok_records_a_green_toast() {
    let mut s = AppState::empty(at(0));
    s.set_command_ok("pause 'alpha' ok", at(100));
    let cs = s.command_status.expect("set");
    assert_eq!(cs.kind, CommandStatusKind::Ok);
    assert_eq!(cs.message, "pause 'alpha' ok");
    assert_eq!(cs.at, at(100));
}

#[test]
fn set_command_error_records_a_red_toast() {
    let mut s = AppState::empty(at(0));
    s.set_command_error("pause failed: 401", at(100));
    let cs = s.command_status.expect("set");
    assert_eq!(cs.kind, CommandStatusKind::Error);
}

#[test]
fn tick_command_status_clears_after_ttl() {
    let mut s = AppState::empty(at(0));
    s.set_command_ok("ok", at(100));
    // 1s after — still present.
    s.tick_command_status(at(101));
    assert!(s.command_status.is_some());
    // 5s exactly — TTL hit, cleared.
    s.tick_command_status(at(100 + COMMAND_STATUS_TTL_SECS));
    assert!(s.command_status.is_none());
}

#[test]
fn tick_command_status_idempotent_when_unset() {
    let mut s = AppState::empty(at(0));
    s.tick_command_status(at(100));
    assert!(s.command_status.is_none());
}
