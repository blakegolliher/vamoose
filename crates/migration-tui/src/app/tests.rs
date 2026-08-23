use super::{handle_input, install_panic_hook, panic_after_ms, AppAction, Input};
use crate::client::SseFrame;
use crate::state::{AppState, InputMode, Modal, Tab, View};
use chrono::{DateTime, TimeZone, Utc};
use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use migration_control_protocol::schema::{
    ConfigHash, EventEnvelope, EventKind, JobId, SCHEMA_VERSION,
};

fn at(s: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(s, 0).unwrap()
}
fn jid(s: &str) -> JobId {
    JobId::new(s).unwrap()
}
// ------------------------------------------------------------------
// F27: terminal restore under panic = "abort"
// ------------------------------------------------------------------

/// `TerminalGuard` is Drop-based and Drop never runs under release
/// `panic = "abort"`, so restore must ALSO be wired through a panic
/// hook. The hook and the guard share one `restore_terminal()` —
/// single source of truth asserted by construction (both call
/// sites name that fn; there is no other restore code).
///
/// One test covers install + composition + idempotence because the
/// panic hook is process-global state: splitting these into
/// separate `#[test]`s would race under the parallel test harness.
#[test]
fn panic_hook_composes_with_previous_and_is_idempotent() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    // A probe "previous" hook so we can observe composition.
    let prev_calls = Arc::new(AtomicUsize::new(0));
    let probe = Arc::clone(&prev_calls);
    std::panic::set_hook(Box::new(move |_| {
        probe.fetch_add(1, Ordering::SeqCst);
    }));

    assert!(
        install_panic_hook(),
        "first install must take effect and report true",
    );
    assert!(
        !install_panic_hook(),
        "second install must be a no-op (idempotent) — otherwise \
         every install would re-wrap the hook chain",
    );

    // The hook runs on any panic, unwinding or not; catch_unwind
    // keeps the test alive. restore_terminal() is a no-op-ish
    // best-effort on a non-tty, so this is safe under the harness.
    let caught = std::panic::catch_unwind(|| panic!("F27 probe panic"));
    assert!(caught.is_err());
    assert_eq!(
        prev_calls.load(Ordering::SeqCst),
        1,
        "previous hook must still run exactly once after restore",
    );
}

/// Pure parser for the hidden `VAMOOSE_TUI_PANIC_AFTER_MS` env
/// hook (manual F27 verification — see the ignored test below).
#[test]
fn panic_after_ms_parses_or_ignores() {
    assert_eq!(panic_after_ms(Some("2000")), Some(2000));
    assert_eq!(panic_after_ms(Some(" 250 ")), Some(250));
    assert_eq!(panic_after_ms(Some("garbage")), None);
    assert_eq!(panic_after_ms(Some("")), None);
    assert_eq!(panic_after_ms(None), None);
}

/// Manual verification recipe for F27 — `kill -SEGV` is not a
/// panic, so a real panic in a real terminal is needed:
///
/// ```text
/// cargo build --release --bin vamoose        # panic = "abort"
/// VAMOOSE_TUI_PANIC_AFTER_MS=2000 target/release/vamoose tui --url http://127.0.0.1:8443
/// ```
///
/// The TUI aborts ~2s after startup. PASS: the shell prompt comes
/// back on the main screen, echoing normally (raw mode off, alt
/// screen left). FAIL (pre-F27 behavior): terminal stuck raw in
/// the alternate screen, needing `reset`.
#[test]
#[ignore = "manual: needs a real terminal and a panic=abort build"]
fn manual_panic_abort_restore_via_env_hook() {}

fn key(code: KeyCode) -> KeyEvent {
    KeyEvent {
        code,
        modifiers: KeyModifiers::NONE,
        kind: KeyEventKind::Press,
        state: crossterm::event::KeyEventState::NONE,
    }
}
fn release(code: KeyCode) -> KeyEvent {
    KeyEvent {
        code,
        modifiers: KeyModifiers::NONE,
        kind: KeyEventKind::Release,
        state: crossterm::event::KeyEventState::NONE,
    }
}
fn job_created(seq: u64, j: &str) -> EventEnvelope {
    EventEnvelope {
        seq,
        at: at(seq as i64),
        schema_version: SCHEMA_VERSION,
        worker_at: None,
        client_seq: None,
        from_worker: None,
        kind: EventKind::JobCreated {
            job_id: jid(j),
            name: format!("{j}-mig"),
            source: "nfs://src".into(),
            dest: "nfs://dst".into(),
            owner: "test".into(),
            config_hash: ConfigHash("ab".into()),
            total_files: 0,
            total_bytes: 0,
        },
    }
}

// ----- handle_input branches -----

#[test]
fn quit_on_q_and_esc() {
    let mut s = AppState::empty(at(0));
    assert_eq!(
        handle_input(&mut s, Input::Key(key(KeyCode::Char('q'))), at(0)),
        AppAction::Quit
    );
    assert_eq!(
        handle_input(&mut s, Input::Key(key(KeyCode::Esc)), at(0)),
        AppAction::Quit
    );
    // Uppercase Q also quits (some terminals send shifted form).
    assert_eq!(
        handle_input(&mut s, Input::Key(key(KeyCode::Char('Q'))), at(0)),
        AppAction::Quit
    );
}

#[test]
fn key_release_does_not_quit() {
    let mut s = AppState::empty(at(0));
    assert_eq!(
        handle_input(&mut s, Input::Key(release(KeyCode::Char('q'))), at(0)),
        AppAction::Continue
    );
}

#[test]
fn sse_connected_marks_state() {
    let mut s = AppState::empty(at(0));
    assert_eq!(
        handle_input(&mut s, Input::SseConnected, at(10)),
        AppAction::Continue
    );
    assert!(matches!(
        s.connection,
        crate::state::ConnectionStatus::Connected { .. }
    ));
}

#[test]
fn sse_disconnected_flips_to_reconnecting() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    handle_input(&mut s, Input::SseDisconnected("reset".into()), at(5));
    match s.connection {
        crate::state::ConnectionStatus::Reconnecting { last_error, .. } => {
            assert_eq!(last_error, "reset");
        }
        _ => panic!("expected Reconnecting"),
    }
}

#[test]
fn sse_fatal_marks_disconnected_terminal() {
    let mut s = AppState::empty(at(0));
    handle_input(&mut s, Input::SseFatal("bad auth".into()), at(0));
    match s.connection {
        crate::state::ConnectionStatus::Disconnected { reason } => {
            assert_eq!(reason, "bad auth");
        }
        _ => panic!("expected Disconnected"),
    }
}

#[test]
fn sse_frame_event_applies_envelope_and_marks_traffic() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    let env = job_created(1, "alpha");
    handle_input(
        &mut s,
        Input::SseFrame(SseFrame::Event {
            seq: 1,
            envelope: Box::new(env),
        }),
        at(7),
    );
    assert!(s.job(&jid("alpha")).is_some());
    assert_eq!(s.last_seq(), 1);
    // mark_traffic refreshed last_traffic.
    if let crate::state::ConnectionStatus::Connected { last_traffic } = &s.connection {
        assert_eq!(*last_traffic, at(7));
    } else {
        panic!("expected Connected");
    }
}

#[test]
fn sse_frame_event_auto_selects_first_job() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    assert!(s.ui.selected_job.is_none());
    handle_input(
        &mut s,
        Input::SseFrame(SseFrame::Event {
            seq: 1,
            envelope: Box::new(job_created(1, "alpha")),
        }),
        at(0),
    );
    assert_eq!(s.ui.selected_job, Some(jid("alpha")));
    // A second job arriving does NOT change the selection.
    handle_input(
        &mut s,
        Input::SseFrame(SseFrame::Event {
            seq: 2,
            envelope: Box::new(job_created(2, "bravo")),
        }),
        at(0),
    );
    assert_eq!(s.ui.selected_job, Some(jid("alpha")));
}

#[test]
fn sse_frame_keepalive_only_refreshes_traffic() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    let before = s.last_seq();
    handle_input(&mut s, Input::SseFrame(SseFrame::Keepalive), at(3));
    assert_eq!(s.last_seq(), before);
    if let crate::state::ConnectionStatus::Connected { last_traffic } = &s.connection {
        assert_eq!(*last_traffic, at(3));
    } else {
        panic!("expected Connected");
    }
}

#[test]
fn driver_advances_cursor_past_unknown() {
    // F38: an unknown-kind frame (newer coord) must advance the
    // resume cursor past its seq — otherwise the reconnect
    // replays it forever — and bump the operator-visible
    // counter. It must NOT touch the connection state.
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    s.apply_envelope(&job_created(1, "alpha"));
    assert_eq!(s.last_seq(), 1);

    let action = handle_input(
        &mut s,
        Input::SseFrame(SseFrame::UnknownEvent {
            seq: 5,
            kind: "FutureThing".into(),
        }),
        at(3),
    );
    assert_eq!(action, AppAction::Continue, "no disconnect, no quit");
    assert_eq!(s.last_seq(), 5, "cursor advances past the unknown frame");
    assert_eq!(s.unknown_events, 1, "operator-visible counter bumps");
    assert!(
        matches!(
            s.connection,
            crate::state::ConnectionStatus::Connected { .. }
        ),
        "connection stays up"
    );

    // A replayed duplicate (reconnect overlap) is deduped by the
    // same drop rule apply_envelope uses.
    handle_input(
        &mut s,
        Input::SseFrame(SseFrame::UnknownEvent {
            seq: 5,
            kind: "FutureThing".into(),
        }),
        at(4),
    );
    assert_eq!(s.unknown_events, 1, "duplicate seq must not double-count");
    assert_eq!(s.last_seq(), 5);

    // Later valid events still apply on top.
    handle_input(
        &mut s,
        Input::SseFrame(SseFrame::Event {
            seq: 6,
            envelope: Box::new(job_created(6, "bravo")),
        }),
        at(5),
    );
    assert!(s.job(&jid("bravo")).is_some());
    assert_eq!(s.last_seq(), 6);
}

#[test]
fn sse_frame_resync_requests_driver_recovery() {
    // F26 — replaces `sse_frame_resync_marks_reconnecting`, which
    // pinned the old flip-banner-only behavior. The reducer does
    // no I/O: it marks the banner and returns AppAction::Resync;
    // the driver owns the actual recovery (drop stream → REST
    // re-bootstrap → resume from the new cursor). Without it the
    // events the coord dropped at the overflow are lost forever
    // and the counters desync permanently.
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    let action = handle_input(&mut s, Input::SseFrame(SseFrame::Resync), at(5));
    assert_eq!(
        action,
        AppAction::Resync,
        "reducer must demand a driver-side re-bootstrap"
    );
    assert!(matches!(
        s.connection,
        crate::state::ConnectionStatus::Reconnecting { .. }
    ));
}

#[test]
fn snapshot_input_replaces_state_and_advances_cursor() {
    // F26 — the bootstrap/Resync recovery path applies the REST
    // snapshot through the reducer (Input::Snapshot →
    // replace_snapshot), keeping all state mutation single-path.
    let mut s = AppState::empty(at(0));
    s.apply_envelope(&job_created(1, "alpha"));

    // Donor state builds a valid Snapshot the "REST fetch" would
    // have produced, further along than what we've seen.
    let mut donor = AppState::empty(at(0));
    donor.apply_envelope(&job_created(9, "bravo"));
    let snap = donor.snapshot.clone();
    assert_eq!(snap.last_seq, 9);

    let action = handle_input(&mut s, Input::Snapshot(Box::new(snap)), at(5));
    assert_eq!(action, AppAction::Continue);
    assert!(s.job(&jid("bravo")).is_some(), "snapshot data applied");
    assert!(
        s.job(&jid("alpha")).is_none(),
        "replace is a hard replace — stale derived state dropped"
    );
    assert_eq!(s.last_seq(), 9, "resume cursor advances to the snapshot's");
    assert_eq!(
        s.ui.selected_job,
        Some(jid("bravo")),
        "first job auto-selected after bootstrap, same as first SSE event"
    );
}

// ----- selection movement -----

#[test]
fn move_selection_with_no_jobs_is_a_noop() {
    let mut s = AppState::empty(at(0));
    handle_input(&mut s, Input::Key(key(KeyCode::Down)), at(0));
    assert!(s.ui.selected_job.is_none());
}

#[test]
fn move_selection_wraps_around_top_and_bottom() {
    let mut s = AppState::empty(at(0));
    s.apply_envelope(&job_created(1, "alpha"));
    s.apply_envelope(&job_created(2, "bravo"));
    s.apply_envelope(&job_created(3, "charlie"));
    s.ui.selected_job = Some(jid("alpha"));

    handle_input(&mut s, Input::Key(key(KeyCode::Down)), at(0));
    assert_eq!(s.ui.selected_job, Some(jid("bravo")));
    handle_input(&mut s, Input::Key(key(KeyCode::Down)), at(0));
    assert_eq!(s.ui.selected_job, Some(jid("charlie")));
    handle_input(&mut s, Input::Key(key(KeyCode::Down)), at(0));
    assert_eq!(
        s.ui.selected_job,
        Some(jid("alpha")),
        "wraps from last back to first"
    );
    handle_input(&mut s, Input::Key(key(KeyCode::Up)), at(0));
    assert_eq!(
        s.ui.selected_job,
        Some(jid("charlie")),
        "wraps from first back to last"
    );
}

#[test]
fn home_and_end_jump_to_extremes() {
    let mut s = AppState::empty(at(0));
    s.apply_envelope(&job_created(1, "alpha"));
    s.apply_envelope(&job_created(2, "bravo"));
    s.apply_envelope(&job_created(3, "charlie"));
    s.ui.selected_job = Some(jid("bravo"));

    handle_input(&mut s, Input::Key(key(KeyCode::End)), at(0));
    assert_eq!(s.ui.selected_job, Some(jid("charlie")));
    handle_input(&mut s, Input::Key(key(KeyCode::Home)), at(0));
    assert_eq!(s.ui.selected_job, Some(jid("alpha")));
}

#[test]
fn tick_does_not_quit() {
    let mut s = AppState::empty(at(0));
    assert_eq!(
        handle_input(&mut s, Input::Tick, at(0)),
        AppAction::Continue
    );
}

// ----- sort cycling -----

#[test]
fn s_key_cycles_sort_in_normal_mode() {
    let mut s = AppState::empty(at(0));
    assert_eq!(s.ui.sort, crate::state::JobSort::ById);
    handle_input(&mut s, Input::Key(key(KeyCode::Char('s'))), at(0));
    assert_eq!(s.ui.sort, crate::state::JobSort::ByPhase);
    handle_input(&mut s, Input::Key(key(KeyCode::Char('s'))), at(0));
    assert_eq!(s.ui.sort, crate::state::JobSort::ByProgressDesc);
    handle_input(&mut s, Input::Key(key(KeyCode::Char('s'))), at(0));
    assert_eq!(s.ui.sort, crate::state::JobSort::ByErrorsDesc);
    handle_input(&mut s, Input::Key(key(KeyCode::Char('s'))), at(0));
    assert_eq!(s.ui.sort, crate::state::JobSort::ById, "wraps");
}

// ----- filter mode UX -----

fn assert_normal(s: &AppState) {
    assert!(matches!(s.ui.input_mode, InputMode::Normal));
}

fn assert_filter_buffer(s: &AppState, expected: &str) {
    match &s.ui.input_mode {
        InputMode::Filter { buffer, .. } => assert_eq!(buffer, expected),
        _ => panic!("expected Filter mode, got {:?}", s.ui.input_mode),
    }
}

#[test]
fn slash_enters_filter_mode_and_seeds_buffer_from_current_filter() {
    let mut s = AppState::empty(at(0));
    s.ui.filter = "prod".into();
    handle_input(&mut s, Input::Key(key(KeyCode::Char('/'))), at(0));
    assert_filter_buffer(&s, "prod");
}

#[test]
fn filter_mode_appends_chars_and_updates_live_filter() {
    let mut s = AppState::empty(at(0));
    handle_input(&mut s, Input::Key(key(KeyCode::Char('/'))), at(0));
    handle_input(&mut s, Input::Key(key(KeyCode::Char('a'))), at(0));
    handle_input(&mut s, Input::Key(key(KeyCode::Char('b'))), at(0));
    assert_filter_buffer(&s, "ab");
    // Live update — visible jobs apply this NOW, no Enter needed.
    assert_eq!(s.ui.filter, "ab");
}

#[test]
fn filter_mode_backspace_pops_last_char() {
    let mut s = AppState::empty(at(0));
    handle_input(&mut s, Input::Key(key(KeyCode::Char('/'))), at(0));
    for c in ['a', 'b', 'c'] {
        handle_input(&mut s, Input::Key(key(KeyCode::Char(c))), at(0));
    }
    handle_input(&mut s, Input::Key(key(KeyCode::Backspace)), at(0));
    assert_filter_buffer(&s, "ab");
    assert_eq!(s.ui.filter, "ab");
    // Backspace on empty is a no-op.
    for _ in 0..5 {
        handle_input(&mut s, Input::Key(key(KeyCode::Backspace)), at(0));
    }
    assert_filter_buffer(&s, "");
    assert_eq!(s.ui.filter, "");
}

#[test]
fn filter_mode_enter_commits_and_returns_to_normal() {
    let mut s = AppState::empty(at(0));
    handle_input(&mut s, Input::Key(key(KeyCode::Char('/'))), at(0));
    for c in ['p', 'r', 'o', 'd'] {
        handle_input(&mut s, Input::Key(key(KeyCode::Char(c))), at(0));
    }
    handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
    assert_normal(&s);
    assert_eq!(s.ui.filter, "prod");
}

#[test]
fn filter_mode_esc_reverts_filter_to_prior() {
    let mut s = AppState::empty(at(0));
    s.ui.filter = "alpha".into();
    handle_input(&mut s, Input::Key(key(KeyCode::Char('/'))), at(0));
    // Live edit — filter changes as we type.
    for c in ['b', 'r', 'a', 'v', 'o'] {
        handle_input(&mut s, Input::Key(key(KeyCode::Char(c))), at(0));
    }
    assert_eq!(s.ui.filter, "alphabravo");
    handle_input(&mut s, Input::Key(key(KeyCode::Esc)), at(0));
    // Esc must NOT quit in filter mode — it cancels.
    assert_normal(&s);
    assert_eq!(s.ui.filter, "alpha", "Esc restores prior filter");
}

#[test]
fn q_in_filter_mode_is_a_literal_q_not_quit() {
    let mut s = AppState::empty(at(0));
    handle_input(&mut s, Input::Key(key(KeyCode::Char('/'))), at(0));
    // 'q' should append to the buffer, not return Quit.
    let action = handle_input(&mut s, Input::Key(key(KeyCode::Char('q'))), at(0));
    assert_eq!(action, AppAction::Continue);
    assert_filter_buffer(&s, "q");
}

#[test]
fn filter_commit_resets_selection_when_selected_falls_out_of_view() {
    let mut s = AppState::empty(at(0));
    s.apply_envelope(&job_created(1, "alpha"));
    s.apply_envelope(&job_created(2, "bravo"));
    s.apply_envelope(&job_created(3, "charlie"));
    s.ui.selected_job = Some(jid("charlie"));
    // Filter to "br" — only bravo matches; charlie does NOT
    // contain that substring (charlie does contain 'a' so a
    // single-letter filter like "a" would still match it).
    handle_input(&mut s, Input::Key(key(KeyCode::Char('/'))), at(0));
    handle_input(&mut s, Input::Key(key(KeyCode::Char('b'))), at(0));
    handle_input(&mut s, Input::Key(key(KeyCode::Char('r'))), at(0));
    handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
    // selected_job must have moved off charlie onto the now-
    // visible bravo.
    assert_eq!(s.ui.selected_job, Some(jid("bravo")));
}

// ----- Detail view navigation (Phase 5a) -----

fn assert_list(s: &AppState) {
    assert!(matches!(s.ui.view, View::List), "expected View::List");
}

fn assert_detail(s: &AppState, want_job: &str, want_tab: Tab) {
    match &s.ui.view {
        View::Detail { job_id, tab } => {
            assert_eq!(job_id.as_str(), want_job);
            assert_eq!(*tab, want_tab);
        }
        View::List => panic!("expected View::Detail, got List"),
    }
}

fn seed_two_jobs() -> AppState {
    let mut s = AppState::empty(at(0));
    s.apply_envelope(&job_created(1, "alpha"));
    s.apply_envelope(&job_created(2, "bravo"));
    s
}

#[test]
fn enter_on_selected_job_opens_detail_with_overview_tab() {
    let mut s = seed_two_jobs();
    s.ui.selected_job = Some(jid("bravo"));
    handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
    assert_detail(&s, "bravo", Tab::Overview);
}

#[test]
fn enter_with_no_selection_does_not_open_detail() {
    let mut s = seed_two_jobs();
    s.ui.selected_job = None;
    handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
    assert_list(&s);
}

#[test]
fn esc_in_detail_returns_to_list_not_quit() {
    let mut s = seed_two_jobs();
    s.ui.selected_job = Some(jid("alpha"));
    handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
    assert_detail(&s, "alpha", Tab::Overview);
    let action = handle_input(&mut s, Input::Key(key(KeyCode::Esc)), at(0));
    assert_eq!(action, AppAction::Continue, "Esc in Detail must NOT quit");
    assert_list(&s);
}

#[test]
fn backspace_in_detail_also_returns_to_list() {
    let mut s = seed_two_jobs();
    s.ui.selected_job = Some(jid("alpha"));
    handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
    handle_input(&mut s, Input::Key(key(KeyCode::Backspace)), at(0));
    assert_list(&s);
}

#[test]
fn q_in_detail_still_quits() {
    let mut s = seed_two_jobs();
    s.ui.selected_job = Some(jid("alpha"));
    handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
    let action = handle_input(&mut s, Input::Key(key(KeyCode::Char('q'))), at(0));
    assert_eq!(action, AppAction::Quit);
}

#[test]
fn tab_key_cycles_tabs_forward_and_wraps() {
    let mut s = seed_two_jobs();
    s.ui.selected_job = Some(jid("alpha"));
    handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
    // Overview → Workers → Errors → Plan → Verify → Overview
    let order = [
        Tab::Workers,
        Tab::Errors,
        Tab::Plan,
        Tab::Verify,
        Tab::Overview,
    ];
    for expected in order {
        handle_input(&mut s, Input::Key(key(KeyCode::Tab)), at(0));
        assert_detail(&s, "alpha", expected);
    }
}

#[test]
fn backtab_key_cycles_tabs_backward_and_wraps() {
    let mut s = seed_two_jobs();
    s.ui.selected_job = Some(jid("alpha"));
    handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
    // Overview ← Verify ← Plan ← Errors ← Workers ← Overview
    let order = [
        Tab::Verify,
        Tab::Plan,
        Tab::Errors,
        Tab::Workers,
        Tab::Overview,
    ];
    for expected in order {
        handle_input(&mut s, Input::Key(key(KeyCode::BackTab)), at(0));
        assert_detail(&s, "alpha", expected);
    }
}

#[test]
fn list_view_keys_inert_after_entering_detail() {
    // / and s do navigation in List, but in Detail they're
    // operator typos — must NOT open filter mode or cycle sort.
    let mut s = seed_two_jobs();
    s.ui.selected_job = Some(jid("alpha"));
    handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
    let prior_sort = s.ui.sort;
    handle_input(&mut s, Input::Key(key(KeyCode::Char('/'))), at(0));
    // input_mode must stay Normal — filter mode is List-only.
    assert!(matches!(s.ui.input_mode, InputMode::Normal));
    handle_input(&mut s, Input::Key(key(KeyCode::Char('s'))), at(0));
    // sort criterion unchanged.
    assert_eq!(s.ui.sort, prior_sort);
}

// ----- Workers tab navigation + modal (Phase 5b) -----

use crate::state::WorkerSort;
use migration_control_protocol::schema::WorkerId as TestWorkerId;

fn worker_joined(seq: u64, job: &str, wid: TestWorkerId, host: &str) -> EventEnvelope {
    EventEnvelope {
        seq,
        at: at(seq as i64),
        schema_version: SCHEMA_VERSION,
        worker_at: None,
        client_seq: None,
        from_worker: None,
        kind: EventKind::WorkerJoined {
            worker_id: wid,
            job_id: jid(job),
            host: host.into(),
            pid: 1000 + (seq as u32),
            start_time: at(0),
            version: "0.6.0".into(),
        },
    }
}

fn seed_job_with_workers() -> (AppState, [TestWorkerId; 3]) {
    let mut s = AppState::empty(at(0));
    s.apply_envelope(&job_created(1, "alpha"));
    let w1 = TestWorkerId::new();
    let w2 = TestWorkerId::new();
    let w3 = TestWorkerId::new();
    s.apply_envelope(&worker_joined(2, "alpha", w1, "host-1"));
    s.apply_envelope(&worker_joined(3, "alpha", w2, "host-2"));
    s.apply_envelope(&worker_joined(4, "alpha", w3, "host-3"));
    s.ui.selected_job = Some(jid("alpha"));
    // Enter detail and switch to Workers tab.
    handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
    handle_input(&mut s, Input::Key(key(KeyCode::Tab)), at(0));
    (s, [w1, w2, w3])
}

fn assert_workers_tab(s: &AppState) {
    match &s.ui.view {
        View::Detail { tab, .. } => assert_eq!(*tab, Tab::Workers),
        View::List => panic!("expected Detail view"),
    }
}

#[test]
fn down_arrow_in_workers_tab_selects_from_none_then_moves() {
    let (mut s, _wids) = seed_job_with_workers();
    assert_workers_tab(&s);
    assert!(s.ui.selected_worker.is_none());
    handle_input(&mut s, Input::Key(key(KeyCode::Down)), at(0));
    assert!(s.ui.selected_worker.is_some(), "Down must select a worker");
}

#[test]
fn worker_selection_wraps_around_top_and_bottom() {
    let (mut s, _) = seed_job_with_workers();
    handle_input(&mut s, Input::Key(key(KeyCode::Home)), at(0));
    let first = s.ui.selected_worker.unwrap();
    handle_input(&mut s, Input::Key(key(KeyCode::End)), at(0));
    let last = s.ui.selected_worker.unwrap();
    assert_ne!(first, last);
    // Down from last → wraps to first.
    handle_input(&mut s, Input::Key(key(KeyCode::Down)), at(0));
    assert_eq!(s.ui.selected_worker, Some(first));
    // Up from first → wraps to last.
    handle_input(&mut s, Input::Key(key(KeyCode::Up)), at(0));
    assert_eq!(s.ui.selected_worker, Some(last));
}

#[test]
fn s_in_workers_tab_cycles_worker_sort_not_job_sort() {
    let (mut s, _) = seed_job_with_workers();
    let prior_job_sort = s.ui.sort;
    assert_eq!(s.ui.worker_sort, WorkerSort::ByMbpsDesc);
    handle_input(&mut s, Input::Key(key(KeyCode::Char('s'))), at(0));
    assert_eq!(s.ui.worker_sort, WorkerSort::ByFilesDesc);
    assert_eq!(s.ui.sort, prior_job_sort, "job sort must NOT change");
}

#[test]
fn enter_in_workers_tab_opens_modal_on_selected_worker() {
    let (mut s, _) = seed_job_with_workers();
    // Auto-select first via Home.
    handle_input(&mut s, Input::Key(key(KeyCode::Home)), at(0));
    let sel = s.ui.selected_worker.unwrap();
    handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
    match &s.ui.modal {
        Some(Modal::WorkerDetail { worker_id }) => assert_eq!(*worker_id, sel),
        Some(other) => panic!("unexpected modal variant: {other:?}"),
        None => panic!("modal not opened"),
    }
}

#[test]
fn enter_with_no_selection_auto_selects_first_then_opens_modal() {
    let (mut s, _) = seed_job_with_workers();
    assert!(s.ui.selected_worker.is_none());
    handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
    // Both modal AND selection should now be set.
    assert!(s.ui.selected_worker.is_some());
    assert!(s.ui.modal.is_some());
}

#[test]
fn esc_with_modal_open_closes_modal_not_view() {
    let (mut s, _) = seed_job_with_workers();
    handle_input(&mut s, Input::Key(key(KeyCode::Home)), at(0));
    handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
    assert!(s.ui.modal.is_some());
    let action = handle_input(&mut s, Input::Key(key(KeyCode::Esc)), at(0));
    assert_eq!(action, AppAction::Continue);
    assert!(s.ui.modal.is_none(), "Esc must close modal");
    assert_workers_tab(&s);
}

#[test]
fn navigation_keys_inert_when_modal_open() {
    // Up/Down/Tab/'s' must not affect anything while a modal is
    // up — the modal owns the input.
    let (mut s, _) = seed_job_with_workers();
    handle_input(&mut s, Input::Key(key(KeyCode::Home)), at(0));
    let pre_selection = s.ui.selected_worker;
    let pre_sort = s.ui.worker_sort;
    handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
    assert!(s.ui.modal.is_some());

    for code in [
        KeyCode::Up,
        KeyCode::Down,
        KeyCode::Tab,
        KeyCode::BackTab,
        KeyCode::Char('s'),
        KeyCode::Char('/'),
    ] {
        handle_input(&mut s, Input::Key(key(code)), at(0));
    }
    // Modal still open, no state change.
    assert!(s.ui.modal.is_some(), "modal must stay open across navs");
    assert_eq!(s.ui.selected_worker, pre_selection);
    assert_eq!(s.ui.worker_sort, pre_sort);
    assert_workers_tab(&s);
}

#[test]
fn q_with_modal_open_still_quits() {
    let (mut s, _) = seed_job_with_workers();
    handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
    let action = handle_input(&mut s, Input::Key(key(KeyCode::Char('q'))), at(0));
    assert_eq!(action, AppAction::Quit);
}

// ----- Palette + confirm modal (Phase 6a) -----

use crate::palette::PaletteCommand;

fn assert_palette(s: &AppState) {
    assert!(
        matches!(s.ui.input_mode, InputMode::Palette { .. }),
        "expected Palette mode, got {:?}",
        s.ui.input_mode
    );
}

fn palette_buffer(s: &AppState) -> String {
    match &s.ui.input_mode {
        InputMode::Palette { buffer, .. } => buffer.clone(),
        _ => panic!("not in palette mode"),
    }
}

#[test]
fn colon_opens_palette_from_list() {
    let mut s = seed_two_jobs();
    handle_input(&mut s, Input::Key(key(KeyCode::Char(':'))), at(0));
    assert_palette(&s);
    assert_eq!(palette_buffer(&s), "");
}

#[test]
fn colon_opens_palette_from_detail() {
    let mut s = seed_two_jobs();
    s.ui.selected_job = Some(jid("alpha"));
    handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
    // Now in Detail.
    handle_input(&mut s, Input::Key(key(KeyCode::Char(':'))), at(0));
    assert_palette(&s);
}

#[test]
fn palette_chars_build_buffer() {
    let mut s = seed_two_jobs();
    handle_input(&mut s, Input::Key(key(KeyCode::Char(':'))), at(0));
    for c in ['p', 'a', 'u', 's', 'e'] {
        handle_input(&mut s, Input::Key(key(KeyCode::Char(c))), at(0));
    }
    assert_eq!(palette_buffer(&s), "pause");
}

#[test]
fn palette_backspace_pops_chars() {
    let mut s = seed_two_jobs();
    handle_input(&mut s, Input::Key(key(KeyCode::Char(':'))), at(0));
    for c in ['p', 'a', 'u'] {
        handle_input(&mut s, Input::Key(key(KeyCode::Char(c))), at(0));
    }
    handle_input(&mut s, Input::Key(key(KeyCode::Backspace)), at(0));
    assert_eq!(palette_buffer(&s), "pa");
}

#[test]
fn palette_tab_cycles_completion_from_empty_buffer() {
    let mut s = seed_two_jobs();
    handle_input(&mut s, Input::Key(key(KeyCode::Char(':'))), at(0));
    // First Tab → first verb in canonical order = "pause".
    handle_input(&mut s, Input::Key(key(KeyCode::Tab)), at(0));
    assert_eq!(palette_buffer(&s), "pause");
    // Next Tab → "resume".
    handle_input(&mut s, Input::Key(key(KeyCode::Tab)), at(0));
    assert_eq!(palette_buffer(&s), "resume");
}

#[test]
fn palette_esc_cancels_back_to_normal() {
    let mut s = seed_two_jobs();
    handle_input(&mut s, Input::Key(key(KeyCode::Char(':'))), at(0));
    let action = handle_input(&mut s, Input::Key(key(KeyCode::Esc)), at(0));
    assert_eq!(action, AppAction::Continue);
    assert!(matches!(s.ui.input_mode, InputMode::Normal));
}

#[test]
fn palette_enter_on_pause_with_default_dispatches_execute() {
    let mut s = seed_two_jobs();
    s.ui.selected_job = Some(jid("alpha"));
    handle_input(&mut s, Input::Key(key(KeyCode::Char(':'))), at(0));
    for c in ['p', 'a', 'u', 's', 'e'] {
        handle_input(&mut s, Input::Key(key(KeyCode::Char(c))), at(0));
    }
    let action = handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
    match action {
        AppAction::Execute(PaletteCommand::Pause { job_id }) => {
            assert_eq!(job_id, jid("alpha"));
        }
        other => panic!("expected Execute(Pause), got {other:?}"),
    }
}

#[test]
fn palette_enter_on_cancel_opens_confirm_modal() {
    let mut s = seed_two_jobs();
    s.ui.selected_job = Some(jid("alpha"));
    handle_input(&mut s, Input::Key(key(KeyCode::Char(':'))), at(0));
    for c in ['c', 'a', 'n', 'c', 'e', 'l'] {
        handle_input(&mut s, Input::Key(key(KeyCode::Char(c))), at(0));
    }
    let action = handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
    assert_eq!(action, AppAction::Continue);
    match &s.ui.modal {
        Some(Modal::ConfirmCommand { command, .. }) => {
            assert!(matches!(command, PaletteCommand::Cancel { .. }));
        }
        other => panic!("expected ConfirmCommand modal, got {other:?}"),
    }
}

#[test]
fn palette_enter_on_quit_returns_quit() {
    let mut s = seed_two_jobs();
    handle_input(&mut s, Input::Key(key(KeyCode::Char(':'))), at(0));
    for c in ['q', 'u', 'i', 't'] {
        handle_input(&mut s, Input::Key(key(KeyCode::Char(c))), at(0));
    }
    let action = handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
    assert_eq!(action, AppAction::Quit);
}

#[test]
fn palette_parse_error_surfaces_as_command_status() {
    let mut s = seed_two_jobs();
    handle_input(&mut s, Input::Key(key(KeyCode::Char(':'))), at(0));
    for c in ['n', 'u', 'k', 'e'] {
        handle_input(&mut s, Input::Key(key(KeyCode::Char(c))), at(0));
    }
    let action = handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
    assert_eq!(action, AppAction::Continue);
    let cs = s.command_status.as_ref().expect("status set");
    assert_eq!(cs.kind, crate::state::CommandStatusKind::Error);
    assert!(cs.message.contains("unknown command"));
}

#[test]
fn confirm_modal_y_executes_command() {
    let mut s = seed_two_jobs();
    s.ui.modal = Some(Modal::ConfirmCommand {
        command: PaletteCommand::Cancel {
            job_id: jid("alpha"),
        },
        summary: "cancel job 'alpha'?".into(),
    });
    let action = handle_input(&mut s, Input::Key(key(KeyCode::Char('y'))), at(0));
    match action {
        AppAction::Execute(PaletteCommand::Cancel { job_id }) => {
            assert_eq!(job_id, jid("alpha"));
        }
        other => panic!("expected Execute, got {other:?}"),
    }
    assert!(s.ui.modal.is_none(), "modal must close on confirm");
}

#[test]
fn confirm_modal_n_cancels_without_execute() {
    let mut s = seed_two_jobs();
    s.ui.modal = Some(Modal::ConfirmCommand {
        command: PaletteCommand::Cancel {
            job_id: jid("alpha"),
        },
        summary: "x".into(),
    });
    let action = handle_input(&mut s, Input::Key(key(KeyCode::Char('n'))), at(0));
    assert_eq!(action, AppAction::Continue);
    assert!(s.ui.modal.is_none());
}

#[test]
fn confirm_modal_esc_also_cancels() {
    let mut s = seed_two_jobs();
    s.ui.modal = Some(Modal::ConfirmCommand {
        command: PaletteCommand::Drain {
            job_id: jid("alpha"),
        },
        summary: "x".into(),
    });
    handle_input(&mut s, Input::Key(key(KeyCode::Esc)), at(0));
    assert!(s.ui.modal.is_none());
}

// ----- Help overlay (Phase 6b) -----

#[test]
fn question_mark_opens_help_modal_from_list() {
    let mut s = seed_two_jobs();
    handle_input(&mut s, Input::Key(key(KeyCode::Char('?'))), at(0));
    assert!(matches!(s.ui.modal, Some(Modal::Help)));
}

#[test]
fn question_mark_opens_help_modal_from_detail() {
    let mut s = seed_two_jobs();
    s.ui.selected_job = Some(jid("alpha"));
    handle_input(&mut s, Input::Key(key(KeyCode::Enter)), at(0));
    handle_input(&mut s, Input::Key(key(KeyCode::Char('?'))), at(0));
    assert!(matches!(s.ui.modal, Some(Modal::Help)));
}

#[test]
fn esc_closes_help_modal_without_quitting() {
    let mut s = seed_two_jobs();
    s.ui.modal = Some(Modal::Help);
    let action = handle_input(&mut s, Input::Key(key(KeyCode::Esc)), at(0));
    assert_eq!(action, AppAction::Continue);
    assert!(s.ui.modal.is_none());
}

#[test]
fn question_mark_in_help_also_closes_it() {
    let mut s = seed_two_jobs();
    s.ui.modal = Some(Modal::Help);
    handle_input(&mut s, Input::Key(key(KeyCode::Char('?'))), at(0));
    assert!(s.ui.modal.is_none());
}

#[test]
fn q_with_help_modal_still_quits() {
    let mut s = seed_two_jobs();
    s.ui.modal = Some(Modal::Help);
    let action = handle_input(&mut s, Input::Key(key(KeyCode::Char('q'))), at(0));
    assert_eq!(action, AppAction::Quit);
}

#[test]
fn nav_keys_inert_when_help_modal_open() {
    let mut s = seed_two_jobs();
    s.ui.modal = Some(Modal::Help);
    s.ui.selected_job = Some(jid("alpha"));
    let before = s.ui.selected_job.clone();
    handle_input(&mut s, Input::Key(key(KeyCode::Down)), at(0));
    handle_input(&mut s, Input::Key(key(KeyCode::Tab)), at(0));
    handle_input(&mut s, Input::Key(key(KeyCode::Char('s'))), at(0));
    // Selection / sort / view untouched.
    assert_eq!(s.ui.selected_job, before);
    assert!(matches!(s.ui.view, View::List));
    assert!(s.ui.modal.is_some(), "help modal must stay open");
}

#[test]
fn command_result_input_updates_banner_toast() {
    let mut s = seed_two_jobs();
    handle_input(
        &mut s,
        Input::CommandResult {
            ok: true,
            message: "pause 'alpha' ok".into(),
        },
        at(0),
    );
    let cs = s.command_status.as_ref().expect("status");
    assert_eq!(cs.kind, crate::state::CommandStatusKind::Ok);
    assert_eq!(cs.message, "pause 'alpha' ok");
}
