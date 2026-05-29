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
use crate::state::{AppState, ConnectionStatus, JobSort, Modal, Tab, UiState, View, WorkerSort};
use chrono::{DateTime, Utc};
use migration_coord::schema::{Job, JobId, Phase, Worker, WorkerId, WorkerState};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, Tabs};
use ratatui::Frame;

// =============================================================================
// Top-level entry point
// =============================================================================

/// Render the full TUI into `frame` using `state` and the current
/// wall-clock `now` (passed in so tests are deterministic).
///
/// Dispatches by [`View`]: List view is the Phase 4 jobs list;
/// Detail view (Phase 5) opens after the operator presses Enter on
/// a job row.
pub fn render(frame: &mut Frame, state: &AppState, now: DateTime<Utc>) {
    match &state.ui.view {
        View::List => render_list(frame, state, now),
        View::Detail { job_id, tab } => render_detail(frame, state, now, job_id, *tab),
    }
}

fn render_list(frame: &mut Frame, state: &AppState, now: DateTime<Utc>) {
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

fn render_detail(
    frame: &mut Frame,
    state: &AppState,
    now: DateTime<Utc>,
    job_id: &JobId,
    tab: Tab,
) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // top banner (shared with List)
            Constraint::Length(1), // tab bar
            Constraint::Min(0),    // tab body
            Constraint::Length(1), // bottom hints (mode-aware)
        ])
        .split(frame.area());

    render_top_banner(frame, chunks[0], state, now);
    render_tab_bar(frame, chunks[1], tab);
    render_tab_body(frame, chunks[2], state, job_id, tab, now);
    render_bottom_hints(frame, chunks[3], state, now);

    // Modal overlays the tab body — render AFTER the body so the
    // modal frame paints on top. The body content underneath is
    // dimmed only by the modal's opaque Clear region; the operator
    // can still see the surrounding banner / tab bar / hints.
    if let Some(modal) = &state.ui.modal {
        render_modal(frame, chunks[2], state, modal, now);
    }
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
    let bar = progress_bar(p.files_done, p.files_total, 10);
    // Pick a right-hand label by what we actually know:
    //
    // - `files_total > 0` → percentage (the normal steady-state once
    //   a totals event has landed; bar fills proportionally).
    // - `files_total == 0 && files_done > 0` → running file count.
    //   The coord schema has no "scan completed" event today, so
    //   pre-scan progress would otherwise read a flat ` -- ` next to
    //   a flat bar even while events stream in. The compact count
    //   surfaces motion until a denominator arrives.
    // - both zero → ` -- ` placeholder so the column stays width-
    //   stable when nothing's happened.
    let rhs = if p.files_total > 0 {
        format_pct(p.files_done, p.files_total)
    } else if p.files_done > 0 {
        format_count(p.files_done)
    } else {
        format_pct(0, 0)
    };
    Cell::from(format!("{bar} {rhs}"))
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
    // Three-way switch: Filter mode wins (the only mode that takes
    // typed characters as literal input). Otherwise dispatch by view
    // — List shows the navigation bindings; Detail shows "back +
    // tab + quit". The shape is the same Vec<Span> so the renderer
    // doesn't care which arm produced it.
    use crate::state::InputMode;
    let hints: Vec<Span<'static>> = match (&state.ui.input_mode, &state.ui.view) {
        (InputMode::Filter { .. }, _) => vec![
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
        (InputMode::Normal, View::List) => vec![
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
        (InputMode::Normal, View::Detail { .. }) => vec![
            key_hint("Esc", "back"),
            Span::raw("  "),
            key_hint("Tab", "next"),
            Span::raw("  "),
            key_hint("Shift-Tab", "prev"),
            Span::raw("  "),
            key_hint("q", "quit"),
        ],
    };
    let para = Paragraph::new(Line::from(hints));
    frame.render_widget(para, area);
}

// =============================================================================
// Detail view: tab bar + per-tab bodies
// =============================================================================

fn render_tab_bar(frame: &mut Frame, area: Rect, current: Tab) {
    let titles: Vec<Line<'static>> = Tab::all()
        .into_iter()
        .map(|t| Line::from(Span::raw(t.label())))
        .collect();
    let selected = Tab::all()
        .into_iter()
        .position(|t| t == current)
        .unwrap_or(0);
    let tabs = Tabs::new(titles)
        .select(selected)
        .divider("│")
        .highlight_style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD | Modifier::UNDERLINED),
        );
    frame.render_widget(tabs, area);
}

fn render_tab_body(
    frame: &mut Frame,
    area: Rect,
    state: &AppState,
    job_id: &JobId,
    tab: Tab,
    now: DateTime<Utc>,
) {
    let job = match state.job(job_id) {
        Some(j) => j,
        None => {
            // Defensive: the operator might have entered Detail
            // just as the job was archived. Show a single line so
            // they know to press Esc.
            let text = Text::from(vec![
                Line::from(Span::styled(
                    format!("Job '{}' not found.", job_id.as_str()),
                    Style::default().fg(Color::Red),
                )),
                Line::raw(""),
                Line::from(Span::raw("Press Esc to return to the jobs list.")),
            ]);
            frame.render_widget(Paragraph::new(text), area);
            return;
        }
    };
    match tab {
        Tab::Overview => render_overview_tab(frame, area, state, job, now),
        Tab::Workers => render_workers_tab(frame, area, state, job, now),
        Tab::Errors => render_placeholder_tab(frame, area, "Errors", "step 5c"),
        Tab::Plan => render_placeholder_tab(frame, area, "Plan", "step 5d"),
        Tab::Verify => render_placeholder_tab(frame, area, "Verify", "step 5d"),
    }
}

fn render_overview_tab(
    frame: &mut Frame,
    area: Rect,
    state: &AppState,
    job: &Job,
    now: DateTime<Utc>,
) {
    let mut lines: Vec<Line<'static>> = Vec::new();

    // ---- Identity --------------------------------------------------
    lines.push(section_header("Identity"));
    lines.push(kv_line("ID", job.id.as_str().to_string()));
    lines.push(kv_line("Name", job.name.clone()));
    lines.push(kv_line(
        "Owner",
        if job.owner.is_empty() {
            "(unknown)".to_string()
        } else {
            job.owner.clone()
        },
    ));
    lines.push(kv_line("Created", format_elapsed(job.created_at, now)));
    lines.push(Line::raw(""));

    // ---- Source / Dest --------------------------------------------
    lines.push(section_header("Source / Dest"));
    lines.push(kv_line("Source", job.source.clone()));
    lines.push(kv_line("Dest", job.dest.clone()));
    lines.push(Line::raw(""));

    // ---- Status ---------------------------------------------------
    lines.push(section_header("Status"));
    lines.push(Line::from(vec![kv_key("Phase"), phase_span(job.phase)]));
    lines.push(kv_line("Files", files_summary(job)));
    lines.push(kv_line("Bytes", bytes_summary(job)));
    lines.push(kv_line("Errors", format!("{}", job.progress.errors_total)));

    // ---- Throughput (1s / 1m / 5m) --------------------------------
    let bps1 = state.job_bytes_per_sec(&job.id, 1, now).unwrap_or(0.0);
    let bps60 = state.job_bytes_per_sec(&job.id, 60, now).unwrap_or(0.0);
    let bps300 = state.job_bytes_per_sec(&job.id, 300, now).unwrap_or(0.0);
    lines.push(kv_line(
        "Throughput",
        format!(
            "1s {}/s   1m {}/s   5m {}/s",
            format_bytes(bps1 as u64),
            format_bytes(bps60 as u64),
            format_bytes(bps300 as u64),
        ),
    ));
    lines.push(Line::raw(""));

    // ---- Workers + Errors counts (full breakdowns in their tabs) ----
    lines.push(section_header("Activity"));
    lines.push(kv_line(
        "Workers",
        format!("{} assigned (see Workers tab)", job.assigned_workers.len()),
    ));
    let err_buckets = state.errors_for_job(&job.id);
    lines.push(kv_line(
        "Error classes",
        format!("{} (see Errors tab)", err_buckets.len()),
    ));

    let para = Paragraph::new(Text::from(lines));
    frame.render_widget(para, area);
}

// =============================================================================
// Workers tab
// =============================================================================

fn render_workers_tab(
    frame: &mut Frame,
    area: Rect,
    state: &AppState,
    job: &Job,
    now: DateTime<Utc>,
) {
    let workers = visible_workers(state, &job.id);
    if workers.is_empty() {
        let text = Text::from(vec![
            Line::from(Span::styled(
                "No workers assigned to this job.",
                Style::default().fg(Color::DarkGray),
            )),
            Line::raw(""),
            Line::from(Span::raw("When a worker registers it will appear here.")),
        ]);
        frame.render_widget(Paragraph::new(text), area);
        return;
    }

    // Carve a one-line legend off the top so the operator knows
    // which sort is active. The rest is the workers table.
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(0)])
        .split(area);

    let legend = Line::from(vec![
        Span::styled(
            format!("{} workers", workers.len()),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw("  ·  sort:"),
        Span::styled(
            state.ui.worker_sort.label(),
            Style::default().fg(Color::Cyan),
        ),
        Span::raw("  ·  press 's' to cycle, Enter to drill in"),
    ]);
    frame.render_widget(Paragraph::new(legend), chunks[0]);

    let header = Row::new(vec![
        Cell::from(Span::styled("Host", header_style())),
        Cell::from(Span::styled("State", header_style())),
        Cell::from(Span::styled("MB/s", header_style())),
        Cell::from(Span::styled("Files/s", header_style())),
        Cell::from(Span::styled("Errs/min", header_style())),
        Cell::from(Span::styled("Inflight", header_style())),
        Cell::from(Span::styled("Queue", header_style())),
        Cell::from(Span::styled("LastHB", header_style())),
    ])
    .style(Style::default().add_modifier(Modifier::BOLD));

    let rows: Vec<Row> = workers.iter().map(|w| worker_row(w, state, now)).collect();

    let widths = [
        Constraint::Min(12),    // Host (flex)
        Constraint::Length(12), // State
        Constraint::Length(8),  // MB/s
        Constraint::Length(8),  // Files/s
        Constraint::Length(8),  // Errs/min
        Constraint::Length(8),  // Inflight
        Constraint::Length(6),  // Queue
        Constraint::Length(7),  // LastHB
    ];
    let table = Table::new(rows, widths).header(header).column_spacing(1);
    frame.render_widget(table, chunks[1]);
}

fn worker_row<'a>(w: &'a Worker, state: &AppState, now: DateTime<Utc>) -> Row<'a> {
    let selected = state.ui.selected_worker.as_ref() == Some(&w.id);
    let base_style = if selected {
        Style::default()
            .bg(Color::DarkGray)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default()
    };
    let host = if w.host.is_empty() {
        "(unknown)".to_string()
    } else {
        w.host.clone()
    };
    Row::new(vec![
        Cell::from(host),
        Cell::from(worker_state_span(w.state)),
        Cell::from(format_bytes(w.counters.bytes_per_sec as u64)),
        Cell::from(format!("{:.1}", w.counters.files_per_sec)),
        Cell::from(format!("{:.1}", w.counters.errors_per_min)),
        Cell::from(format!("{}", w.inflight_ops)),
        Cell::from(format!("{}", w.queue_depth)),
        Cell::from(format_elapsed(w.last_heartbeat, now)),
    ])
    .style(base_style)
}

fn worker_state_span(s: WorkerState) -> Span<'static> {
    let (label, color) = match s {
        WorkerState::Idle => ("Idle", Color::Gray),
        WorkerState::Scanning => ("Scanning", Color::Cyan),
        WorkerState::Copying => ("Copying", Color::Green),
        WorkerState::Verifying => ("Verifying", Color::Cyan),
        WorkerState::Draining => ("Draining", Color::Yellow),
        WorkerState::Fenced => ("Fenced", Color::Red),
        WorkerState::Failed => ("Failed", Color::Red),
        WorkerState::Disconnected => ("Discon.", Color::DarkGray),
    };
    Span::styled(label, Style::default().fg(color))
}

/// Apply the Workers-tab sort criterion to the job's assigned
/// workers and return the resulting borrow slice. Each call walks
/// the snapshot once; the caller's loop reads stable references.
pub fn visible_workers<'a>(state: &'a AppState, job_id: &JobId) -> Vec<&'a Worker> {
    let mut workers = state.workers_for_job(job_id);
    sort_workers(&mut workers, state.ui.worker_sort);
    workers
}

fn sort_workers(workers: &mut Vec<&Worker>, sort: WorkerSort) {
    use std::cmp::Ordering;
    workers.sort_by(|a, b| {
        let primary = match sort {
            WorkerSort::ByMbpsDesc => b
                .counters
                .bytes_per_sec
                .partial_cmp(&a.counters.bytes_per_sec)
                .unwrap_or(Ordering::Equal),
            WorkerSort::ByFilesDesc => b
                .counters
                .files_per_sec
                .partial_cmp(&a.counters.files_per_sec)
                .unwrap_or(Ordering::Equal),
            WorkerSort::ByErrorsDesc => b
                .counters
                .errors_per_min
                .partial_cmp(&a.counters.errors_per_min)
                .unwrap_or(Ordering::Equal),
            WorkerSort::ByHost => a.host.cmp(&b.host),
        };
        // Stable tiebreaker on host so the table doesn't shuffle
        // identical-throughput workers on every render.
        primary.then_with(|| a.host.cmp(&b.host))
    });
}

// =============================================================================
// Worker-detail modal
// =============================================================================

fn render_modal(
    frame: &mut Frame,
    body_area: Rect,
    state: &AppState,
    modal: &Modal,
    now: DateTime<Utc>,
) {
    match modal {
        Modal::WorkerDetail { worker_id } => {
            render_worker_modal(frame, body_area, state, worker_id, now)
        }
    }
}

fn render_worker_modal(
    frame: &mut Frame,
    body_area: Rect,
    state: &AppState,
    worker_id: &WorkerId,
    now: DateTime<Utc>,
) {
    let area = centered_rect(70, 70, body_area);
    // Clear under the modal so the workers table beneath doesn't
    // bleed through.
    frame.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Worker detail (Esc to close) ")
        .border_style(Style::default().fg(Color::Cyan));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let Some(w) = state.worker(worker_id) else {
        let text = Text::from(vec![
            Line::from(Span::styled(
                format!("Worker '{worker_id}' not found."),
                Style::default().fg(Color::Red),
            )),
            Line::raw(""),
            Line::from(Span::raw("Press Esc to close.")),
        ]);
        frame.render_widget(Paragraph::new(text), inner);
        return;
    };

    let mut lines: Vec<Line<'static>> = Vec::new();
    lines.push(section_header("Identity"));
    lines.push(kv_line("ID", w.id.to_string()));
    lines.push(kv_line("Host", w.host.clone()));
    lines.push(kv_line("PID", format!("{}", w.pid)));
    lines.push(kv_line("Version", w.version.clone()));
    lines.push(kv_line("Started", format_elapsed(w.start_time, now)));
    lines.push(kv_line("Joined", format_elapsed(w.joined_at, now)));
    lines.push(Line::raw(""));

    lines.push(section_header("State"));
    lines.push(Line::from(vec![
        kv_key("State"),
        worker_state_span(w.state),
    ]));
    lines.push(kv_line("Last HB", format_elapsed(w.last_heartbeat, now)));
    lines.push(kv_line("Inflight", format!("{} ops", w.inflight_ops)));
    lines.push(kv_line("Queue depth", format!("{}", w.queue_depth)));
    if let Some(shard) = &w.assigned_shard {
        lines.push(kv_line("Shard", shard.0.clone()));
    }
    lines.push(Line::raw(""));

    lines.push(section_header("Counters"));
    lines.push(kv_line(
        "Throughput",
        format!("{}/s", format_bytes(w.counters.bytes_per_sec as u64)),
    ));
    lines.push(kv_line(
        "Files/s",
        format!("{:.2}", w.counters.files_per_sec),
    ));
    lines.push(kv_line(
        "Errs/min",
        format!("{:.2}", w.counters.errors_per_min),
    ));

    if w.last_error.is_some() || w.fence_reason.is_some() {
        lines.push(Line::raw(""));
        lines.push(section_header("Diagnostics"));
        if let Some(err) = &w.last_error {
            lines.push(kv_line("Last error", err.clone()));
        }
        if let Some(reason) = &w.fence_reason {
            lines.push(Line::from(vec![
                kv_key("Fenced"),
                Span::styled(reason.clone(), Style::default().fg(Color::Red)),
            ]));
        }
    }
    frame.render_widget(Paragraph::new(Text::from(lines)), inner);
}

/// Centered rectangle helper. Standard ratatui pattern for modal
/// overlays. `percent_x` / `percent_y` are 0-100.
fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let vchunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(vchunks[1])[1]
}

fn render_placeholder_tab(frame: &mut Frame, area: Rect, name: &str, milestone: &str) {
    let lines: Vec<Line<'static>> = vec![
        Line::from(Span::styled(
            format!("{name} tab"),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )),
        Line::raw(""),
        Line::from(Span::styled(
            format!("Coming in P5 {milestone}."),
            Style::default().fg(Color::DarkGray),
        )),
    ];
    frame.render_widget(Paragraph::new(Text::from(lines)), area);
}

// ----- small helpers for the Overview layout -----

fn section_header(label: &str) -> Line<'static> {
    Line::from(Span::styled(
        label.to_string(),
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    ))
}

fn kv_key(label: &str) -> Span<'static> {
    // 13-char column for the key so values align across lines.
    Span::styled(
        format!("  {label:<11}"),
        Style::default().fg(Color::DarkGray),
    )
}

fn kv_line(label: &str, value: impl Into<String>) -> Line<'static> {
    Line::from(vec![kv_key(label), Span::raw(value.into())])
}

fn files_summary(job: &Job) -> String {
    let done = format_count(job.progress.files_done);
    if job.progress.files_total > 0 {
        format!(
            "{} / {}  ({})",
            done,
            format_count(job.progress.files_total),
            format_pct(job.progress.files_done, job.progress.files_total).trim(),
        )
    } else {
        format!("{done} (total unknown)")
    }
}

fn bytes_summary(job: &Job) -> String {
    let done = format_bytes(job.progress.bytes_done);
    if job.progress.bytes_total > 0 {
        format!(
            "{} / {}  ({})",
            done,
            format_bytes(job.progress.bytes_total),
            format_pct(job.progress.bytes_done, job.progress.bytes_total).trim(),
        )
    } else {
        format!("{done} (total unknown)")
    }
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

    #[test]
    fn detail_placeholder_tabs_render_without_panicking() {
        // Workers tab landed in step 5b and is no longer a
        // placeholder. Remaining placeholders fill in across
        // steps 5c (Errors) and 5d (Plan / Verify).
        let mut s = AppState::empty(at(0));
        s.mark_connected(at(0));
        s.apply_envelope(&job_created_evt(1, 0, "alpha"));
        for tab in [Tab::Errors, Tab::Plan, Tab::Verify] {
            enter_detail(&mut s, "alpha", tab);
            let buf = render_to_buffer(&s, at(0), 100, 16);
            let text = buffer_text(&buf);
            assert!(
                text.contains(tab.label()),
                "tab body missing label for {:?}: \n{text}",
                tab
            );
            assert!(text.contains("Coming in P5"));
        }
    }

    // ----- Workers tab + modal (Phase 5b) -----

    fn worker_joined_evt(
        seq: u64,
        secs: i64,
        job: &str,
        wid: WorkerId,
        host: &str,
    ) -> EventEnvelope {
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
}
