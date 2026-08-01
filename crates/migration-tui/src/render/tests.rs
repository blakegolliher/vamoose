use super::*;
use crate::state::{AppState, Tab};
use chrono::{DateTime, TimeZone, Utc};
use migration_control_protocol::schema::{
    ConfigHash, ErrorClass, EventEnvelope, EventKind, JobId, WorkerId, SCHEMA_VERSION,
};
use ratatui::backend::TestBackend;
use ratatui::buffer::Buffer;
use ratatui::Terminal;

fn at(s: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(s, 0).unwrap()
}
fn jid(s: &str) -> JobId {
    JobId::new(s).unwrap()
}

fn env(seq: u64, secs: i64, kind: EventKind) -> EventEnvelope {
    EventEnvelope {
        seq,
        at: at(secs),
        schema_version: SCHEMA_VERSION,
        worker_at: None,
        client_seq: None,
        from_worker: None,
        kind,
    }
}

fn job_created_evt(seq: u64, secs: i64, j: &str) -> EventEnvelope {
    env(
        seq,
        secs,
        EventKind::JobCreated {
            job_id: jid(j),
            name: format!("{j}-mig"),
            source: "nfs://src".into(),
            dest: "nfs://dst".into(),
            owner: "test".into(),
            config_hash: ConfigHash("ab".into()),
        },
    )
}

/// Set explicit progress totals/counts on an already-created job
/// by directly mutating the snapshot — the coord schema has no
/// "set totals" event today (the worker doesn't compute totals
/// either; coord progress.files_total defaults to 0). For render
/// tests we want a non-zero denominator so the percentage path
/// exercises.
fn set_progress(s: &mut AppState, job: &str, files_total: u64, files_done: u64, bytes_done: u64) {
    let j = s.snapshot.jobs.get_mut(&jid(job)).expect("job exists");
    j.progress.files_total = files_total;
    j.progress.files_done = files_done;
    j.progress.bytes_done = bytes_done;
}

fn render_to_buffer(state: &AppState, now: DateTime<Utc>, w: u16, h: u16) -> Buffer {
    let backend = TestBackend::new(w, h);
    let mut terminal = Terminal::new(backend).unwrap();
    terminal.draw(|f| render(f, state, now)).unwrap();
    terminal.backend().buffer().clone()
}

fn buffer_text(buf: &Buffer) -> String {
    let mut out = String::new();
    let area = buf.area;
    for y in 0..area.height {
        for x in 0..area.width {
            out.push_str(buf.cell((x, y)).unwrap().symbol());
        }
        out.push('\n');
    }
    out
}

fn buffer_row(buf: &Buffer, row: u16) -> String {
    let mut out = String::new();
    for x in 0..buf.area.width {
        out.push_str(buf.cell((x, row)).unwrap().symbol());
    }
    out.trim_end().to_string()
}

// ----- format helper layered tests -----

#[test]
fn progress_bar_renders_filled_and_empty_at_width() {
    assert_eq!(progress_bar(0, 0, 4), "░░░░");
    assert_eq!(progress_bar(0, 10, 4), "░░░░");
    assert_eq!(progress_bar(5, 10, 4), "▓▓░░");
    assert_eq!(progress_bar(10, 10, 4), "▓▓▓▓");
}

#[test]
fn aggregate_counts_classifies_phases() {
    let mut s = AppState::empty(at(0));
    s.apply_envelope(&job_created_evt(1, 0, "alpha"));
    s.apply_envelope(&job_created_evt(2, 1, "bravo"));
    s.apply_envelope(&env(
        3,
        2,
        EventKind::JobPaused {
            job_id: jid("alpha"),
            reason: "op".into(),
        },
    ));
    s.apply_envelope(&job_created_evt(4, 3, "charlie"));
    s.apply_envelope(&env(
        5,
        4,
        EventKind::JobCompleted {
            job_id: jid("charlie"),
        },
    ));
    let agg = aggregate_counts(&s);
    assert_eq!(agg.total, 3);
    assert_eq!(agg.paused, 1);
    assert_eq!(agg.running, 1);
    assert_eq!(agg.terminal, 1);
}

#[test]
fn visible_jobs_applies_filter_and_sort() {
    let mut s = AppState::empty(at(0));
    s.apply_envelope(&job_created_evt(1, 0, "alpha-prod"));
    s.apply_envelope(&job_created_evt(2, 1, "bravo-dev"));
    s.apply_envelope(&job_created_evt(3, 2, "carol-prod"));
    // No filter, default sort = ById.
    let v = visible_jobs(&s);
    assert_eq!(v.len(), 3);
    assert_eq!(v[0].id.as_str(), "alpha-prod");
    assert_eq!(v[1].id.as_str(), "bravo-dev");
    assert_eq!(v[2].id.as_str(), "carol-prod");
    // Filter to substring "prod".
    s.ui.filter = "prod".into();
    let v = visible_jobs(&s);
    assert_eq!(v.len(), 2);
    assert!(v.iter().all(|j| j.id.as_str().contains("prod")));
}

// ----- render snapshot tests -----

#[test]
fn top_banner_shows_vamoose_label_and_connection() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(95));
    let buf = render_to_buffer(&s, at(100), 80, 8);
    let banner = buffer_row(&buf, 0);
    assert!(banner.contains("vamoose"));
    assert!(banner.contains("connected"));
    // 5 seconds elapsed → "5s".
    assert!(banner.contains("5s"));
}

#[test]
fn top_banner_surfaces_unknown_event_counter() {
    // F38: skipped unknown-kind frames must be operator-visible.
    // Zero → no clutter; nonzero → a counter in the banner.
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(95));
    let buf = render_to_buffer(&s, at(100), 140, 8);
    assert!(
        !buffer_row(&buf, 0).contains("unknown"),
        "no unknown-events span when the counter is zero"
    );
    s.note_unknown_event(5);
    s.note_unknown_event(9);
    let buf = render_to_buffer(&s, at(100), 140, 8);
    let banner = buffer_row(&buf, 0);
    assert!(
        banner.contains("2 unknown"),
        "expected '2 unknown' in banner:\n{banner}"
    );
}

#[test]
fn top_banner_shows_reconnecting_when_link_down() {
    let mut s = AppState::empty(at(0));
    s.mark_reconnecting(at(90), "connection reset");
    let buf = render_to_buffer(&s, at(100), 80, 8);
    let banner = buffer_row(&buf, 0);
    assert!(banner.contains("reconnecting"));
    assert!(banner.contains("connection reset"));
}

#[test]
fn jobs_table_renders_header_and_rows() {
    let mut s = AppState::empty(at(0));
    s.apply_envelope(&job_created_evt(1, 0, "alpha"));
    s.apply_envelope(&job_created_evt(2, 0, "bravo"));
    let buf = render_to_buffer(&s, at(100), 80, 8);
    let text = buffer_text(&buf);
    assert!(text.contains("Job"));
    assert!(text.contains("Phase"));
    assert!(text.contains("Workers"));
    // Job ids appear in alphabetical order under header.
    assert!(text.contains("alpha"));
    assert!(text.contains("bravo"));
    // alpha row above bravo row.
    let alpha_pos = text.find("alpha").unwrap();
    let bravo_pos = text.find("bravo").unwrap();
    assert!(alpha_pos < bravo_pos);
}

#[test]
fn job_row_shows_progress_bar_and_percentage() {
    let mut s = AppState::empty(at(0));
    s.apply_envelope(&job_created_evt(1, 0, "alpha"));
    set_progress(&mut s, "alpha", 100, 50, 5000);
    let buf = render_to_buffer(&s, at(100), 100, 8);
    let text = buffer_text(&buf);
    // 50/100 = 50%.
    assert!(text.contains(" 50%"), "expected ' 50%' in:\n{text}");
    // ASCII bar should have both filled and empty cells.
    assert!(text.contains('▓'));
    assert!(text.contains('░'));
}

#[test]
fn job_row_shows_running_count_when_total_unknown() {
    // No event in the coord schema today sets `files_total`,
    // so a job that's actively streaming ProgressDelta will
    // have files_done > 0 and files_total == 0. The render
    // surfaces the running count instead of stranding the
    // operator on a flat " -- " placeholder.
    let mut s = AppState::empty(at(0));
    s.apply_envelope(&job_created_evt(1, 0, "alpha"));
    set_progress(&mut s, "alpha", 0, 1234, 5000);
    let buf = render_to_buffer(&s, at(100), 100, 8);
    let text = buffer_text(&buf);
    // format_count(1234) = "1.23k".
    assert!(text.contains("1.23k"), "expected count, got:\n{text}");
    // No percentage when the denominator is missing.
    assert!(!text.contains("%"));
}

#[test]
fn job_row_keeps_placeholder_when_truly_empty() {
    // Job exists but nothing has progressed yet — column still
    // renders the " -- " placeholder so width stays stable
    // across rows.
    let mut s = AppState::empty(at(0));
    s.apply_envelope(&job_created_evt(1, 0, "alpha"));
    let buf = render_to_buffer(&s, at(100), 100, 8);
    let text = buffer_text(&buf);
    assert!(text.contains(" -- "), "expected placeholder, got:\n{text}");
}

#[test]
fn bottom_hints_visible() {
    let s = AppState::empty(at(0));
    let buf = render_to_buffer(&s, at(0), 80, 8);
    let last = buffer_row(&buf, buf.area.height - 1);
    assert!(last.contains("q quit"));
    assert!(last.contains("/ filter"));
}

#[test]
fn empty_state_still_renders_a_clean_frame() {
    // No jobs, no events, no connection — render must not panic.
    let s = AppState::empty(at(0));
    let buf = render_to_buffer(&s, at(0), 80, 24);
    let banner = buffer_row(&buf, 0);
    assert!(banner.contains("vamoose"));
    // Middle area should not have any non-space character beyond
    // the header (which is the first row of the middle).
}

#[test]
fn filter_visible_in_banner_when_set() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    s.apply_envelope(&job_created_evt(1, 0, "alpha"));
    s.ui.filter = "prod".into();
    let buf = render_to_buffer(&s, at(0), 100, 8);
    let banner = buffer_row(&buf, 0);
    assert!(
        banner.contains("filter:"),
        "banner without 'filter:' was:\n>>>{banner}<<<"
    );
    assert!(banner.contains("prod"));
}

#[test]
fn sort_label_always_visible_in_banner() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    let buf = render_to_buffer(&s, at(0), 100, 8);
    let banner = buffer_row(&buf, 0);
    // Default sort = id.
    assert!(
        banner.contains("sort:id"),
        "banner missing 'sort:id': >>>{banner}<<<"
    );
    // Cycle to phase and re-render.
    s.ui.sort = crate::state::JobSort::ByPhase;
    let buf = render_to_buffer(&s, at(0), 100, 8);
    let banner = buffer_row(&buf, 0);
    assert!(banner.contains("sort:phase"));
}

#[test]
fn banner_shows_live_filter_buffer_with_cursor_in_filter_mode() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    s.ui.input_mode = crate::state::InputMode::Filter {
        buffer: "prod".into(),
        prior: String::new(),
    };
    s.ui.filter = "prod".into();
    let buf = render_to_buffer(&s, at(0), 100, 8);
    let banner = buffer_row(&buf, 0);
    assert!(banner.contains("filter:"));
    assert!(banner.contains("prod"));
    // Cursor glyph rendered after the buffer.
    assert!(
        banner.contains("prod_"),
        "expected cursor glyph after buffer; got >>>{banner}<<<"
    );
}

#[test]
fn banner_shows_throughput_when_progress_recorded() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    s.apply_envelope(&job_created_evt(1, 0, "alpha"));
    // Push a ProgressDelta of 60 MiB right at "now" so the
    // 60-second window yields exactly 1 MiB/s.
    s.apply_envelope(&EventEnvelope {
        seq: 2,
        at: at(0),
        schema_version: SCHEMA_VERSION,
        worker_at: None,
        client_seq: None,
        from_worker: None,
        kind: EventKind::ProgressDelta {
            job_id: jid("alpha"),
            worker_id: WorkerId::new(),
            files_delta: 1,
            bytes_delta: 60 * 1024 * 1024,
            errors_delta: 0,
        },
    });
    let buf = render_to_buffer(&s, at(0), 120, 8);
    let banner = buffer_row(&buf, 0);
    // 60 MiB over 60 s = 1 MiB/s → format_bytes(1 MiB) = "1.00MiB" + "/s".
    assert!(
        banner.contains("/s"),
        "throughput unit missing from banner: >>>{banner}<<<"
    );
}

#[test]
fn bottom_hints_switch_in_filter_mode() {
    let mut s = AppState::empty(at(0));
    s.ui.input_mode = crate::state::InputMode::Filter {
        buffer: String::new(),
        prior: String::new(),
    };
    let buf = render_to_buffer(&s, at(0), 80, 8);
    let last = buffer_row(&buf, buf.area.height - 1);
    assert!(last.contains("Enter apply"));
    assert!(last.contains("Esc cancel"));
    // Normal-mode hints should NOT be present.
    assert!(!last.contains("q quit"));
}

// ----- Detail view rendering (Phase 5a) -----

fn enter_detail(s: &mut AppState, job: &str, tab: Tab) {
    s.ui.view = crate::state::View::Detail {
        job_id: jid(job),
        tab,
    };
}

#[test]
fn detail_view_renders_tab_bar_with_all_five_tabs() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    s.apply_envelope(&job_created_evt(1, 0, "alpha"));
    enter_detail(&mut s, "alpha", Tab::Overview);
    let buf = render_to_buffer(&s, at(100), 100, 16);
    let text = buffer_text(&buf);
    for label in ["Overview", "Workers", "Errors", "Plan", "Verify"] {
        assert!(text.contains(label), "missing tab '{label}' in:\n{text}");
    }
}

#[test]
fn detail_overview_tab_shows_job_identity_and_status() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    s.apply_envelope(&job_created_evt(1, 0, "alpha"));
    enter_detail(&mut s, "alpha", Tab::Overview);
    let buf = render_to_buffer(&s, at(100), 100, 24);
    let text = buffer_text(&buf);
    // Section headers and key fields are present.
    assert!(text.contains("Identity"), "missing 'Identity' section");
    assert!(text.contains("Source / Dest"), "missing 'Source / Dest'");
    assert!(text.contains("Status"), "missing 'Status'");
    assert!(text.contains("Throughput"), "missing 'Throughput'");
    // Job id + name from the fixture must surface.
    assert!(text.contains("alpha"));
    assert!(text.contains("alpha-mig"));
    assert!(text.contains("nfs://src"));
    assert!(text.contains("nfs://dst"));
}

#[test]
fn detail_bottom_hints_swap_to_back_tab_quit() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    s.apply_envelope(&job_created_evt(1, 0, "alpha"));
    enter_detail(&mut s, "alpha", Tab::Overview);
    let buf = render_to_buffer(&s, at(0), 100, 16);
    let last = buffer_row(&buf, buf.area.height - 1);
    assert!(last.contains("Esc back"), "got: >>>{last}<<<");
    assert!(last.contains("Tab next"));
    assert!(last.contains("Shift-Tab prev"));
    assert!(last.contains("q quit"));
    // List-mode bindings must NOT be present in Detail hints.
    assert!(!last.contains("/ filter"));
    assert!(!last.contains("s sort"));
}

#[test]
fn detail_unknown_job_shows_not_found_message() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    // Detail view points at a job that doesn't exist in the
    // snapshot (was archived between Enter and render).
    enter_detail(&mut s, "ghost", Tab::Overview);
    let buf = render_to_buffer(&s, at(0), 80, 16);
    let text = buffer_text(&buf);
    assert!(
        text.contains("not found"),
        "expected error line in:\n{text}"
    );
    assert!(text.contains("Press Esc"));
}

// ----- Workers tab + modal (Phase 5b) -----

fn worker_joined_evt(seq: u64, secs: i64, job: &str, wid: WorkerId, host: &str) -> EventEnvelope {
    env(
        seq,
        secs,
        EventKind::WorkerJoined {
            worker_id: wid,
            job_id: jid(job),
            host: host.into(),
            pid: 1000 + (seq as u32),
            start_time: at(0),
            version: "0.6.0".into(),
        },
    )
}

#[test]
fn workers_tab_renders_header_and_rows() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    s.apply_envelope(&job_created_evt(1, 0, "alpha"));
    let w1 = WorkerId::new();
    let w2 = WorkerId::new();
    s.apply_envelope(&worker_joined_evt(2, 0, "alpha", w1, "host-1"));
    s.apply_envelope(&worker_joined_evt(3, 0, "alpha", w2, "host-2"));
    enter_detail(&mut s, "alpha", Tab::Workers);
    let buf = render_to_buffer(&s, at(0), 120, 16);
    let text = buffer_text(&buf);
    // Header columns appear.
    for col in [
        "Host", "State", "MB/s", "Files/s", "Errs/min", "Inflight", "Queue",
    ] {
        assert!(text.contains(col), "missing column '{col}'");
    }
    // Both worker hosts appear.
    assert!(text.contains("host-1"));
    assert!(text.contains("host-2"));
    // Header legend exposes the sort criterion.
    assert!(text.contains("sort:"));
    assert!(text.contains("mb/s"));
}

#[test]
fn workers_tab_with_zero_workers_shows_empty_message() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    s.apply_envelope(&job_created_evt(1, 0, "alpha"));
    enter_detail(&mut s, "alpha", Tab::Workers);
    let buf = render_to_buffer(&s, at(0), 100, 16);
    let text = buffer_text(&buf);
    assert!(
        text.contains("No workers assigned"),
        "expected empty-state hint, got:\n{text}"
    );
}

#[test]
fn workers_table_sorted_by_mbps_desc_by_default() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    s.apply_envelope(&job_created_evt(1, 0, "alpha"));
    let w_slow = WorkerId::new();
    let w_fast = WorkerId::new();
    s.apply_envelope(&worker_joined_evt(2, 0, "alpha", w_slow, "slow"));
    s.apply_envelope(&worker_joined_evt(3, 0, "alpha", w_fast, "fast"));
    // Drive heartbeats — coord assigns the counters; here we
    // poke the snapshot directly because we're testing the
    // render layer in isolation.
    s.snapshot
        .workers
        .get_mut(&w_slow)
        .unwrap()
        .counters
        .bytes_per_sec = 1_000.0;
    s.snapshot
        .workers
        .get_mut(&w_fast)
        .unwrap()
        .counters
        .bytes_per_sec = 50_000_000.0;
    enter_detail(&mut s, "alpha", Tab::Workers);

    let workers = visible_workers(&s, &jid("alpha"));
    assert_eq!(workers.len(), 2);
    assert_eq!(workers[0].host, "fast", "highest mbps must lead");
    assert_eq!(workers[1].host, "slow");
}

#[test]
fn workers_table_sorted_by_host_when_set() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    s.apply_envelope(&job_created_evt(1, 0, "alpha"));
    for (i, h) in ["zoo", "ant", "moo"].iter().enumerate() {
        let w = WorkerId::new();
        s.apply_envelope(&worker_joined_evt(2 + i as u64, 0, "alpha", w, h));
    }
    s.ui.worker_sort = crate::state::WorkerSort::ByHost;
    let hosts: Vec<&str> = visible_workers(&s, &jid("alpha"))
        .iter()
        .map(|w| w.host.as_str())
        .collect();
    assert_eq!(hosts, ["ant", "moo", "zoo"]);
}

#[test]
fn modal_renders_over_workers_tab_with_worker_fields() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    s.apply_envelope(&job_created_evt(1, 0, "alpha"));
    let wid = WorkerId::new();
    s.apply_envelope(&worker_joined_evt(2, 0, "alpha", wid, "host-A"));
    enter_detail(&mut s, "alpha", Tab::Workers);
    s.ui.selected_worker = Some(wid);
    s.ui.modal = Some(crate::state::Modal::WorkerDetail { worker_id: wid });

    // Use a taller buffer so the 70%-height modal can fit all
    // three sections (Identity / State / Counters) without
    // clipping the bottom one.
    let buf = render_to_buffer(&s, at(0), 120, 36);
    let text = buffer_text(&buf);
    // Modal title + Esc hint.
    assert!(text.contains("Worker detail"));
    assert!(text.contains("Esc to close"));
    // Worker identity exposed.
    assert!(text.contains("host-A"));
    // Section headers appear.
    assert!(text.contains("Identity"));
    assert!(text.contains("State"));
    assert!(text.contains("Counters"));
}

#[test]
fn modal_with_unknown_worker_shows_not_found() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    s.apply_envelope(&job_created_evt(1, 0, "alpha"));
    enter_detail(&mut s, "alpha", Tab::Workers);
    let ghost = WorkerId::new();
    s.ui.modal = Some(crate::state::Modal::WorkerDetail { worker_id: ghost });

    let buf = render_to_buffer(&s, at(0), 120, 24);
    let text = buffer_text(&buf);
    assert!(text.contains("not found"));
    assert!(text.contains("Esc to close"));
}

// ----- Errors tab (Phase 5c) -----

fn error_emitted_evt(
    seq: u64,
    secs: i64,
    job: &str,
    class: ErrorClass,
    path: &str,
    message: &str,
    retryable: bool,
) -> EventEnvelope {
    env(
        seq,
        secs,
        EventKind::ErrorEmitted {
            job_id: jid(job),
            worker_id: WorkerId::new(),
            class,
            path: path.into(),
            retryable,
            message: message.into(),
        },
    )
}

#[test]
fn errors_tab_empty_state_when_no_errors() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    s.apply_envelope(&job_created_evt(1, 0, "alpha"));
    enter_detail(&mut s, "alpha", Tab::Errors);
    let buf = render_to_buffer(&s, at(0), 100, 20);
    let text = buffer_text(&buf);
    assert!(
        text.contains("No errors recorded"),
        "expected empty state, got:\n{text}"
    );
}

#[test]
fn errors_tab_renders_buckets_and_recent_tail() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    s.apply_envelope(&job_created_evt(1, 0, "alpha"));
    s.apply_envelope(&error_emitted_evt(
        2,
        10,
        "alpha",
        ErrorClass::Permission,
        "/data/file1",
        "EACCES",
        false,
    ));
    s.apply_envelope(&error_emitted_evt(
        3,
        20,
        "alpha",
        ErrorClass::Nfs3Err(13),
        "/data/file2",
        "NFSERR_ACCES",
        true,
    ));
    enter_detail(&mut s, "alpha", Tab::Errors);

    let buf = render_to_buffer(&s, at(100), 120, 20);
    let text = buffer_text(&buf);
    // Legend
    assert!(text.contains("error classes"));
    assert!(text.contains("total errors"));
    // Bucket table headers
    for col in ["Class", "Count", "First", "Last", "Retry", "Sample path"] {
        assert!(text.contains(col), "missing bucket col '{col}'");
    }
    // Class formatting surfaces
    assert!(text.contains("permission"));
    assert!(text.contains("nfs3:13"));
    // Recent tail header
    assert!(text.contains("Recent"));
    // Recent tail column headers
    for col in ["When", "Class", "Path", "Message"] {
        assert!(text.contains(col), "missing tail col '{col}'");
    }
    // Specific error messages from the events appear in the tail
    assert!(text.contains("EACCES"));
    assert!(text.contains("NFSERR_ACCES"));
    assert!(text.contains("/data/file1"));
    assert!(text.contains("/data/file2"));
}

#[test]
fn errors_tab_buckets_sorted_by_count_desc() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    s.apply_envelope(&job_created_evt(1, 0, "alpha"));
    // 1x Permission, 3x Timeout — Timeout should lead the table.
    s.apply_envelope(&error_emitted_evt(
        2,
        10,
        "alpha",
        ErrorClass::Permission,
        "/p/a",
        "perm",
        false,
    ));
    for i in 0..3 {
        s.apply_envelope(&error_emitted_evt(
            3 + i,
            15 + i as i64,
            "alpha",
            ErrorClass::Timeout,
            &format!("/p/t{i}"),
            "timeout",
            true,
        ));
    }
    enter_detail(&mut s, "alpha", Tab::Errors);

    let buf = render_to_buffer(&s, at(100), 120, 20);
    let text = buffer_text(&buf);
    // "timeout" must appear before "permission" in the bucket
    // table (count 3 > count 1 → sort desc puts it on top).
    let timeout_pos = text.find("timeout").unwrap();
    let perm_pos = text.find("permission").unwrap();
    assert!(
        timeout_pos < perm_pos,
        "expected timeout to appear before permission; got positions {timeout_pos} vs {perm_pos}",
    );
}

#[test]
fn error_class_formatter_covers_all_variants() {
    assert_eq!(format_error_class(&ErrorClass::Nfs3Err(13)), "nfs3:13");
    assert_eq!(
        format_error_class(&ErrorClass::ClaimConflict),
        "claim-conflict"
    );
    assert_eq!(format_error_class(&ErrorClass::Permission), "permission");
    assert_eq!(format_error_class(&ErrorClass::Timeout), "timeout");
    assert_eq!(
        format_error_class(&ErrorClass::ChecksumMismatch),
        "checksum"
    );
    assert_eq!(
        format_error_class(&ErrorClass::Other("foo".into())),
        "other:foo"
    );
}

// ----- Plan tab (Phase 5d) -----

#[test]
fn plan_tab_shows_config_hash_and_jobconfig_fields() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    s.apply_envelope(&job_created_evt(1, 0, "alpha"));
    enter_detail(&mut s, "alpha", Tab::Plan);
    let buf = render_to_buffer(&s, at(0), 120, 30);
    let text = buffer_text(&buf);
    // Section headers + the config hash itself.
    assert!(text.contains("Config hash"));
    assert!(text.contains("JobConfig"));
    // job_created_evt seeds config_hash = "ab" via ConfigHash.
    assert!(
        text.contains("ab"),
        "expected config hash in output:\n{text}"
    );
    // JSON pretty-print exposes JobConfig fields.
    for field in ["source", "dest", "claim_version", "verify_mode"] {
        assert!(
            text.contains(field),
            "expected JobConfig field '{field}' in:\n{text}"
        );
    }
}

// ----- Verify tab (Phase 5d) -----

#[test]
fn verify_tab_empty_state_when_no_verify_events() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    s.apply_envelope(&job_created_evt(1, 0, "alpha"));
    enter_detail(&mut s, "alpha", Tab::Verify);
    let buf = render_to_buffer(&s, at(0), 100, 20);
    let text = buffer_text(&buf);
    assert!(
        text.contains("Verify phase has not run"),
        "expected empty state, got:\n{text}"
    );
    // Current phase still surfaces.
    assert!(text.contains("Planned"));
}

#[test]
fn verify_tab_shows_status_after_started_and_completed() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    s.apply_envelope(&job_created_evt(1, 0, "alpha"));
    s.apply_envelope(&env(
        2,
        10,
        EventKind::VerifyStarted {
            job_id: jid("alpha"),
        },
    ));
    s.apply_envelope(&env(
        3,
        20,
        EventKind::VerifyCompleted {
            job_id: jid("alpha"),
            mismatches: 3,
        },
    ));
    enter_detail(&mut s, "alpha", Tab::Verify);
    let buf = render_to_buffer(&s, at(100), 120, 24);
    let text = buffer_text(&buf);
    assert!(text.contains("Status"));
    assert!(text.contains("Started"));
    assert!(text.contains("Completed"));
    // 3 mismatches → red "3 mismatches" line.
    assert!(text.contains("3 mismatches"));
    // Recent mismatches section header still rendered even
    // though no VerifyFileMismatch events have streamed.
    assert!(text.contains("Recent mismatches"));
}

#[test]
fn verify_tab_shows_verify_ok_when_zero_mismatches() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    s.apply_envelope(&job_created_evt(1, 0, "alpha"));
    s.apply_envelope(&env(
        2,
        10,
        EventKind::VerifyStarted {
            job_id: jid("alpha"),
        },
    ));
    s.apply_envelope(&env(
        3,
        20,
        EventKind::VerifyCompleted {
            job_id: jid("alpha"),
            mismatches: 0,
        },
    ));
    enter_detail(&mut s, "alpha", Tab::Verify);
    let buf = render_to_buffer(&s, at(100), 120, 24);
    let text = buffer_text(&buf);
    assert!(text.contains("0 mismatches"));
    assert!(text.contains("verify ok"));
}

#[test]
fn verify_tab_renders_mismatches_tail_with_paths() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    s.apply_envelope(&job_created_evt(1, 0, "alpha"));
    s.apply_envelope(&env(
        2,
        10,
        EventKind::VerifyStarted {
            job_id: jid("alpha"),
        },
    ));
    // Stream two mismatches.
    for (seq, p, e, g) in [
        (3u64, "/data/file1", "size:100", "size:101"),
        (4u64, "/data/file2", "checksum:abc", "checksum:xyz"),
    ] {
        s.apply_envelope(&env(
            seq,
            seq as i64,
            EventKind::VerifyFileMismatch {
                job_id: jid("alpha"),
                path: p.into(),
                expected: e.into(),
                got: g.into(),
            },
        ));
    }
    enter_detail(&mut s, "alpha", Tab::Verify);
    let buf = render_to_buffer(&s, at(100), 120, 30);
    let text = buffer_text(&buf);
    // Tail headers + per-row contents.
    for col in ["When", "Path", "Expected", "Got"] {
        assert!(text.contains(col), "missing tail col '{col}'");
    }
    assert!(text.contains("/data/file1"));
    assert!(text.contains("/data/file2"));
    assert!(text.contains("size:100"));
    assert!(text.contains("checksum:xyz"));
}

// ----- Live tab-label counters (Phase 5e) -----

/// Lift just the tab-bar row out of a rendered Detail view so
/// label-counter assertions don't false-match against the tab
/// body content underneath. The Detail layout puts the tab bar
/// at row 1 (banner is row 0).
fn tab_bar_row(buf: &ratatui::buffer::Buffer) -> String {
    buffer_row(buf, 1)
}

#[test]
fn tab_bar_omits_counters_when_everything_is_zero() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    s.apply_envelope(&job_created_evt(1, 0, "alpha"));
    enter_detail(&mut s, "alpha", Tab::Overview);
    let buf = render_to_buffer(&s, at(0), 120, 16);
    let bar = tab_bar_row(&buf);
    // Workers / Errors / Verify all read as plain labels — no
    // "(N)" since the counters are zero.
    for label in ["Overview", "Workers", "Errors", "Plan", "Verify"] {
        assert!(bar.contains(label), "missing '{label}' in:\n{bar}");
    }
    assert!(
        !bar.contains("("),
        "expected no parenthetical counters, got:\n{bar}"
    );
}

#[test]
fn tab_bar_shows_workers_counter_when_workers_join() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    s.apply_envelope(&job_created_evt(1, 0, "alpha"));
    let w1 = WorkerId::new();
    let w2 = WorkerId::new();
    s.apply_envelope(&worker_joined_evt(2, 0, "alpha", w1, "host-1"));
    s.apply_envelope(&worker_joined_evt(3, 0, "alpha", w2, "host-2"));
    enter_detail(&mut s, "alpha", Tab::Overview);
    let buf = render_to_buffer(&s, at(0), 120, 16);
    let bar = tab_bar_row(&buf);
    assert!(
        bar.contains("Workers (2)"),
        "expected 'Workers (2)' in:\n{bar}"
    );
    // Errors / Verify counters still absent.
    assert!(!bar.contains("Errors ("));
    assert!(!bar.contains("Verify ("));
}

#[test]
fn tab_bar_shows_errors_counter_when_classes_appear() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    s.apply_envelope(&job_created_evt(1, 0, "alpha"));
    // Two distinct error classes → bucket count = 2.
    s.apply_envelope(&error_emitted_evt(
        2,
        0,
        "alpha",
        ErrorClass::Permission,
        "/p/a",
        "perm",
        false,
    ));
    s.apply_envelope(&error_emitted_evt(
        3,
        0,
        "alpha",
        ErrorClass::Timeout,
        "/p/b",
        "to",
        true,
    ));
    enter_detail(&mut s, "alpha", Tab::Overview);
    let buf = render_to_buffer(&s, at(0), 120, 16);
    let bar = tab_bar_row(&buf);
    assert!(
        bar.contains("Errors (2)"),
        "expected 'Errors (2)' in:\n{bar}"
    );
}

#[test]
fn tab_bar_shows_verify_counter_when_mismatches_arrive() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    s.apply_envelope(&job_created_evt(1, 0, "alpha"));
    for (seq, p) in [(2u64, "/m/a"), (3u64, "/m/b"), (4u64, "/m/c")] {
        s.apply_envelope(&env(
            seq,
            seq as i64,
            EventKind::VerifyFileMismatch {
                job_id: jid("alpha"),
                path: p.into(),
                expected: "e".into(),
                got: "g".into(),
            },
        ));
    }
    enter_detail(&mut s, "alpha", Tab::Overview);
    let buf = render_to_buffer(&s, at(0), 120, 16);
    let bar = tab_bar_row(&buf);
    assert!(
        bar.contains("Verify (3)"),
        "expected 'Verify (3)' in:\n{bar}"
    );
}

#[test]
fn tab_bar_counters_update_after_new_events() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    s.apply_envelope(&job_created_evt(1, 0, "alpha"));
    let w1 = WorkerId::new();
    s.apply_envelope(&worker_joined_evt(2, 0, "alpha", w1, "host-1"));
    enter_detail(&mut s, "alpha", Tab::Overview);

    let buf = render_to_buffer(&s, at(0), 120, 16);
    assert!(tab_bar_row(&buf).contains("Workers (1)"));

    // Second worker joins.
    let w2 = WorkerId::new();
    s.apply_envelope(&worker_joined_evt(3, 0, "alpha", w2, "host-2"));
    let buf = render_to_buffer(&s, at(0), 120, 16);
    assert!(tab_bar_row(&buf).contains("Workers (2)"));
}

// ----- Palette + confirm modal + toast (Phase 6a) -----

#[test]
fn bottom_row_shows_palette_buffer_when_in_palette_mode() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    s.ui.input_mode = crate::state::InputMode::Palette {
        buffer: "pause alpha".into(),
        completion_idx: 0,
    };
    let buf = render_to_buffer(&s, at(0), 100, 8);
    let last = buffer_row(&buf, buf.area.height - 1);
    assert!(
        last.starts_with(':'),
        "expected leading colon, got: >>>{last}<<<"
    );
    assert!(last.contains("pause alpha"));
    // Hints visible.
    assert!(last.contains("Tab complete"));
    assert!(last.contains("Enter run"));
}

#[test]
fn normal_bottom_hints_advertise_colon_for_command() {
    let s = AppState::empty(at(0));
    let buf = render_to_buffer(&s, at(0), 100, 8);
    let last = buffer_row(&buf, buf.area.height - 1);
    assert!(
        last.contains(": command"),
        "expected ': command' hint in: >>>{last}<<<"
    );
}

#[test]
fn banner_shows_ok_toast_when_command_status_set() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    s.set_command_ok("pause 'alpha' ok", at(0));
    let buf = render_to_buffer(&s, at(0), 120, 8);
    let banner = buffer_row(&buf, 0);
    assert!(banner.contains("✓"));
    assert!(banner.contains("pause 'alpha' ok"));
}

#[test]
fn banner_shows_error_toast_when_command_status_failed() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    s.set_command_error("pause: 401 Unauthorized", at(0));
    let buf = render_to_buffer(&s, at(0), 120, 8);
    let banner = buffer_row(&buf, 0);
    assert!(banner.contains("✗"));
    assert!(banner.contains("401"));
}

#[test]
fn help_modal_renders_with_section_headers() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    s.ui.modal = Some(crate::state::Modal::Help);
    // Tall buffer so the bottom Command palette + Other
    // sections fit without clipping. Real terminals are
    // routinely 24+ rows; on those the modal scrolls cleanly
    // because the body is a Paragraph that wraps to the
    // available area.
    let buf = render_to_buffer(&s, at(0), 100, 40);
    let text = buffer_text(&buf);
    assert!(text.contains("Help: keybindings"));
    // All section headers present.
    for section in [
        "Navigation (Jobs list)",
        "Filter & sort",
        "Detail view",
        "Workers tab",
        "Command palette",
    ] {
        assert!(
            text.contains(section),
            "missing help section '{section}' in:\n{text}"
        );
    }
    // Spot-check a few bindings.
    assert!(text.contains("open job detail"));
    assert!(text.contains("open palette"));
    assert!(text.contains("pause / resume / cancel"));
}

#[test]
fn normal_bottom_hints_advertise_question_mark_for_help() {
    let s = AppState::empty(at(0));
    let buf = render_to_buffer(&s, at(0), 100, 8);
    let last = buffer_row(&buf, buf.area.height - 1);
    assert!(
        last.contains("? help"),
        "expected '? help' hint in: >>>{last}<<<"
    );
}

#[test]
fn confirm_command_modal_renders_centered_with_summary() {
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    s.apply_envelope(&job_created_evt(1, 0, "alpha"));
    enter_detail(&mut s, "alpha", Tab::Overview);
    s.ui.modal = Some(crate::state::Modal::ConfirmCommand {
        command: crate::palette::PaletteCommand::Cancel {
            job_id: jid("alpha"),
        },
        summary: "cancel job 'alpha'?".into(),
    });
    let buf = render_to_buffer(&s, at(0), 120, 24);
    let text = buffer_text(&buf);
    // Title carries the verb; body shows the summary line + y/n prompt.
    assert!(text.contains("Confirm: cancel"));
    assert!(text.contains("cancel job 'alpha'?"));
    assert!(text.contains("y"));
    assert!(text.contains("n"));
    assert!(text.contains("Esc also cancels"));
}

// ----- Theme (Phase 6c) -----

/// Scan every cell in the buffer for any non-Reset foreground
/// or background color. Used to assert that NO_COLOR truly
/// neutralized the output.
fn any_color_set(buf: &ratatui::buffer::Buffer) -> bool {
    for y in 0..buf.area.height {
        for x in 0..buf.area.width {
            let cell = buf.cell((x, y)).unwrap();
            if cell.style().fg.unwrap_or(ratatui::style::Color::Reset)
                != ratatui::style::Color::Reset
            {
                return true;
            }
            if cell.style().bg.unwrap_or(ratatui::style::Color::Reset)
                != ratatui::style::Color::Reset
            {
                return true;
            }
        }
    }
    false
}

#[test]
fn no_color_theme_renders_with_zero_colored_cells() {
    // Force the NO_COLOR palette on state and verify every cell
    // of the rendered buffer comes out with Color::Reset for
    // both foreground and background.
    let mut s = AppState::empty(at(0)).with_theme(crate::theme::Theme::no_color());
    s.mark_connected(at(0));
    s.apply_envelope(&job_created_evt(1, 0, "alpha"));
    // Throw some color-bearing state into the mix: a Paused
    // job (yellow under the dark theme), some errors (red),
    // a selected row (DarkGray bg), and a non-empty filter
    // (yellow). All of these MUST come out neutral.
    s.apply_envelope(&env(
        2,
        10,
        EventKind::JobPaused {
            job_id: jid("alpha"),
            reason: "x".into(),
        },
    ));
    s.ui.selected_job = Some(jid("alpha"));
    s.ui.filter = "alpha".into();
    s.set_command_ok("pause ok", at(0));

    let buf = render_to_buffer(&s, at(0), 120, 16);
    assert!(
        !any_color_set(&buf),
        "NO_COLOR palette must render zero colored cells"
    );
}

#[test]
fn default_dark_theme_renders_at_least_one_colored_cell() {
    // Companion sanity check — if the dark theme ALSO ended up
    // neutral, no_color_theme_renders_with_zero_colored_cells
    // would trivially pass even after a regression that broke
    // theme threading. The dark theme must put SOME color
    // somewhere on a populated state.
    let mut s = AppState::empty(at(0));
    s.mark_connected(at(0));
    s.apply_envelope(&job_created_evt(1, 0, "alpha"));
    let buf = render_to_buffer(&s, at(0), 120, 16);
    assert!(
        any_color_set(&buf),
        "dark theme must render at least one colored cell"
    );
}
