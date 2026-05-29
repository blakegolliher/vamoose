//! Render layer for the TUI.
//!
//! All public entry points are pure functions: `(frame, area,
//! state, now) -> ()`. The TUI's event loop (Phase 4c) calls
//! [`render`] inside `Terminal::draw`; tests call it against a
//! [`ratatui::backend::TestBackend`] to snapshot specific cells
//! without spinning up the full event loop.
//!
//! Three-row layout:
//!
//! ```text
//!  ┌────────────────────────────────────────────────────────────┐
//!  │ vamoose · ● connected · 5 jobs, 1.2M files, 8.4TiB         │  top
//!  ├────────────────────────────────────────────────────────────┤
//!  │ Job           Phase    Progress              Workers  Errs │
//!  │ alpha-mig     Copying  ▓▓▓▓▓░░░░░  45%        3      0     │  middle
//!  │ bravo-mig     Paused   ▓▓▓░░░░░░░  18%        1      4     │  (table)
//!  │ …                                                          │
//!  ├────────────────────────────────────────────────────────────┤
//!  │ q quit  f filter  s sort                          last 1s  │  bottom
//!  └────────────────────────────────────────────────────────────┘
//! ```

use crate::format::{format_bytes, format_count, format_elapsed, format_pct};
use crate::state::{AppState, ConnectionStatus, JobSort, UiState};
use chrono::{DateTime, Utc};
use migration_coord::schema::{Job, Phase};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Cell, Paragraph, Row, Table};
use ratatui::Frame;

// =============================================================================
// Top-level entry point
// =============================================================================

/// Render the full TUI into `frame` using `state` and the current
/// wall-clock `now` (passed in so tests are deterministic).
pub fn render(frame: &mut Frame, state: &AppState, now: DateTime<Utc>) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // top banner
            Constraint::Min(0),    // jobs table
            Constraint::Length(1), // bottom hints
        ])
        .split(frame.area());

    render_top_banner(frame, chunks[0], state, now);
    render_jobs_table(frame, chunks[1], state, now);
    render_bottom_hints(frame, chunks[2], state, now);
}

// =============================================================================
// Top banner
// =============================================================================

fn render_top_banner(frame: &mut Frame, area: Rect, state: &AppState, now: DateTime<Utc>) {
    let agg = aggregate_counts(state);
    let conn = connection_label(&state.connection, now);
    // 1-min total throughput across all jobs. Append "/s" so the
    // unit is unambiguous even when the value rounds to 0.
    let total_bps = state.total_bytes_per_sec(60, now);
    let throughput_label = if total_bps > 0.0 {
        format!(" · {}/s", format_bytes(total_bps as u64))
    } else {
        String::new()
    };

    let mut spans = vec![
        Span::styled("vamoose", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(" · "),
        conn,
        Span::raw(" · "),
        Span::raw(format!(
            "{} jobs ({} run, {} pause, {} done) · {} files · {}{}",
            agg.total,
            agg.running,
            agg.paused,
            agg.terminal,
            format_count(agg.files_done),
            format_bytes(agg.bytes_done),
            throughput_label,
        )),
    ];

    // Filter span — show the live edit buffer with a cursor glyph
    // while in filter mode, or the committed filter otherwise.
    match &state.ui.input_mode {
        crate::state::InputMode::Filter { buffer, .. } => {
            spans.push(Span::raw(" · filter:"));
            // Cursor glyph after the buffer so the operator can see
            // where their next character will land.
            spans.push(Span::styled(
                format!("{buffer}_"),
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ));
        }
        crate::state::InputMode::Normal => {
            if !state.ui.filter.is_empty() {
                spans.push(Span::raw(" · filter:"));
                spans.push(Span::styled(
                    state.ui.filter.clone(),
                    Style::default().fg(Color::Yellow),
                ));
            }
        }
    }

    // Sort label is always shown so the operator knows what `s`
    // will cycle to next.
    spans.push(Span::raw(" · sort:"));
    spans.push(Span::styled(
        state.ui.sort.label(),
        Style::default().fg(Color::Cyan),
    ));

    let para = Paragraph::new(Line::from(spans));
    frame.render_widget(para, area);
}

fn connection_label(status: &ConnectionStatus, now: DateTime<Utc>) -> Span<'static> {
    match status {
        ConnectionStatus::Connected { last_traffic } => Span::styled(
            format!("● connected ({})", format_elapsed(*last_traffic, now)),
            Style::default().fg(Color::Green),
        ),
        ConnectionStatus::Reconnecting { since, last_error } => Span::styled(
            format!(
                "● reconnecting {} — {}",
                format_elapsed(*since, now),
                last_error
            ),
            Style::default().fg(Color::Yellow),
        ),
        ConnectionStatus::Disconnected { reason } => Span::styled(
            format!("● offline — {reason}"),
            Style::default().fg(Color::Red),
        ),
    }
}

// =============================================================================
// Jobs table
// =============================================================================

fn render_jobs_table(frame: &mut Frame, area: Rect, state: &AppState, now: DateTime<Utc>) {
    let jobs = visible_jobs(state);

    // Header row, then one row per job. Column widths sum to 51 of
    // fixed budget; the leading Job column takes whatever remains
    // (Min(15) reserves a sensible minimum for the smallest sane
    // terminal).
    let header = Row::new(vec![
        Cell::from(Span::styled("Job", header_style())),
        Cell::from(Span::styled("Phase", header_style())),
        Cell::from(Span::styled("Progress", header_style())),
        Cell::from(Span::styled("Workers", header_style())),
        Cell::from(Span::styled("Errs", header_style())),
        Cell::from(Span::styled("Updated", header_style())),
    ])
    .style(Style::default().add_modifier(Modifier::BOLD));

    let rows = jobs
        .iter()
        .map(|j| job_row(j, state, now))
        .collect::<Vec<_>>();

    let widths = [
        Constraint::Min(15),
        Constraint::Length(9),
        Constraint::Length(24),
        Constraint::Length(7),
        Constraint::Length(5),
        Constraint::Length(7),
    ];
    let table = Table::new(rows, widths).header(header).column_spacing(1);
    frame.render_widget(table, area);
}

fn job_row<'a>(job: &'a Job, state: &AppState, now: DateTime<Utc>) -> Row<'a> {
    let selected = state.ui.selected_job.as_ref() == Some(&job.id);
    let base_style = if selected {
        Style::default()
            .bg(Color::DarkGray)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default()
    };

    let progress_cell = progress_cell(job);
    let phase_span = phase_span(job.phase);
    let errs = job.progress.errors_total;
    let errs_style = if errs > 0 {
        Style::default().fg(Color::Red)
    } else {
        Style::default()
    };
    let workers = job.assigned_workers.len();
    // Derive a "last update" time: most recent phase transition if
    // any, otherwise creation. The coord doesn't currently maintain
    // an explicit `updated_at` field on Job; if/when it does, this
    // can switch to reading it directly.
    let updated_at = job
        .phase_history
        .last()
        .map(|t| t.at)
        .unwrap_or(job.created_at);
    let updated = format_elapsed(updated_at, now);

    Row::new(vec![
        Cell::from(job.id.as_str().to_string()),
        Cell::from(phase_span),
        progress_cell,
        Cell::from(format!("{workers}")),
        Cell::from(Span::styled(format!("{errs}"), errs_style)),
        Cell::from(updated),
    ])
    .style(base_style)
}

fn progress_cell(job: &Job) -> Cell<'static> {
    let p = &job.progress;
    let pct_text = format_pct(p.files_done, p.files_total);
    let bar = progress_bar(p.files_done, p.files_total, 10);
    Cell::from(format!("{bar} {pct_text}"))
}

/// ASCII progress bar of `width` cells (full ▓, empty ░). Caller is
/// expected to render the percentage to the right.
fn progress_bar(done: u64, total: u64, width: usize) -> String {
    if total == 0 {
        return "░".repeat(width);
    }
    let pct = (done as f64 / total as f64).clamp(0.0, 1.0);
    let filled = (pct * width as f64).round() as usize;
    let filled = filled.min(width);
    let empty = width - filled;
    let mut s = String::with_capacity(width * 3);
    for _ in 0..filled {
        s.push('▓');
    }
    for _ in 0..empty {
        s.push('░');
    }
    s
}

fn phase_span(phase: Phase) -> Span<'static> {
    let (label, color) = match phase {
        Phase::Planned => ("Planned", Color::Gray),
        Phase::Scanning => ("Scanning", Color::Cyan),
        Phase::Copying => ("Copying", Color::Green),
        Phase::Verifying => ("Verifying", Color::Cyan),
        Phase::Cutover => ("Cutover", Color::Cyan),
        Phase::Paused => ("Paused", Color::Yellow),
        Phase::Completed => ("Completed", Color::Green),
        Phase::Failed => ("Failed", Color::Red),
        Phase::Cancelled => ("Cancelled", Color::Red),
    };
    Span::styled(label, Style::default().fg(color))
}

fn header_style() -> Style {
    Style::default()
        .fg(Color::White)
        .add_modifier(Modifier::BOLD)
}

// =============================================================================
// Bottom hints
// =============================================================================

fn render_bottom_hints(frame: &mut Frame, area: Rect, state: &AppState, _now: DateTime<Utc>) {
    // Context-aware hints: filter mode swaps to "Enter apply / Esc
    // cancel / Backspace delete" since the normal-mode bindings
    // would mislead the operator (q would append, not quit).
    let hints: Vec<Span<'static>> = match &state.ui.input_mode {
        crate::state::InputMode::Filter { .. } => vec![
            key_hint("Enter", "apply"),
            Span::raw("  "),
            key_hint("Esc", "cancel"),
            Span::raw("  "),
            key_hint("Backspace", "delete"),
            Span::raw("  "),
            Span::styled(
                "(typing builds filter)",
                Style::default().fg(Color::DarkGray),
            ),
        ],
        crate::state::InputMode::Normal => vec![
            key_hint("q", "quit"),
            Span::raw("  "),
            key_hint("/", "filter"),
            Span::raw("  "),
            key_hint("s", "sort"),
            Span::raw("  "),
            key_hint("↑↓", "select"),
            Span::raw("  "),
            key_hint("Enter", "details"),
        ],
    };
    let para = Paragraph::new(Line::from(hints));
    frame.render_widget(para, area);
}

fn key_hint(key: &str, label: &str) -> Span<'static> {
    Span::styled(
        format!("{key} {label}"),
        Style::default().fg(Color::DarkGray),
    )
}

// =============================================================================
// Aggregates + filter/sort
// =============================================================================

#[derive(Debug, Clone, Copy, Default)]
pub struct AggregateCounts {
    pub total: usize,
    pub running: usize,
    pub paused: usize,
    /// Terminal = Completed + Failed + Cancelled.
    pub terminal: usize,
    pub files_done: u64,
    pub bytes_done: u64,
    pub errors_total: u64,
}

pub fn aggregate_counts(state: &AppState) -> AggregateCounts {
    let mut agg = AggregateCounts::default();
    for j in state.snapshot.jobs.values() {
        agg.total += 1;
        match j.phase {
            Phase::Paused => agg.paused += 1,
            Phase::Completed | Phase::Failed | Phase::Cancelled => agg.terminal += 1,
            _ => agg.running += 1,
        }
        agg.files_done = agg.files_done.saturating_add(j.progress.files_done);
        agg.bytes_done = agg.bytes_done.saturating_add(j.progress.bytes_done);
        agg.errors_total = agg.errors_total.saturating_add(j.progress.errors_total);
    }
    agg
}

/// Apply [`UiState::filter`] and [`UiState::sort`] to the snapshot's
/// jobs. Returns owned references — cheap because Job is in a
/// HashMap and we just hand out borrows.
pub fn visible_jobs(state: &AppState) -> Vec<&Job> {
    let UiState { filter, sort, .. } = &state.ui;
    let filter_lower = filter.to_lowercase();
    let mut out: Vec<&Job> = state
        .snapshot
        .jobs
        .values()
        .filter(|j| {
            if filter.is_empty() {
                return true;
            }
            j.id.as_str().to_lowercase().contains(&filter_lower)
                || j.name.to_lowercase().contains(&filter_lower)
        })
        .collect();
    match sort {
        JobSort::ById => out.sort_by(|a, b| a.id.as_str().cmp(b.id.as_str())),
        JobSort::ByPhase => out.sort_by(|a, b| {
            phase_rank(a.phase)
                .cmp(&phase_rank(b.phase))
                .then_with(|| a.id.as_str().cmp(b.id.as_str()))
        }),
        JobSort::ByProgressDesc => out.sort_by(|a, b| {
            let af = progress_fraction(a);
            let bf = progress_fraction(b);
            // Higher fraction comes first.
            bf.partial_cmp(&af)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.id.as_str().cmp(b.id.as_str()))
        }),
        JobSort::ByErrorsDesc => out.sort_by(|a, b| {
            b.progress
                .errors_total
                .cmp(&a.progress.errors_total)
                .then_with(|| a.id.as_str().cmp(b.id.as_str()))
        }),
    }
    out
}

fn phase_rank(phase: Phase) -> u8 {
    match phase {
        Phase::Copying => 0,
        Phase::Scanning => 1,
        Phase::Verifying => 2,
        Phase::Cutover => 3,
        Phase::Planned => 4,
        Phase::Paused => 5,
        Phase::Failed => 6,
        Phase::Cancelled => 7,
        Phase::Completed => 8,
    }
}

fn progress_fraction(j: &Job) -> f64 {
    if j.progress.files_total == 0 {
        0.0
    } else {
        j.progress.files_done as f64 / j.progress.files_total as f64
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::AppState;
    use chrono::TimeZone;
    use migration_coord::schema::{
        ConfigHash, EventEnvelope, EventKind, JobId, WorkerId, SCHEMA_VERSION,
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
    fn set_progress(
        s: &mut AppState,
        job: &str,
        files_total: u64,
        files_done: u64,
        bytes_done: u64,
    ) {
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
}
