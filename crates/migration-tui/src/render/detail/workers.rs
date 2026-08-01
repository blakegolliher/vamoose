use super::super::common::{header_style, worker_state_span};
use crate::format::{format_bytes, format_elapsed};
use crate::state::{AppState, WorkerSort};
use chrono::{DateTime, Utc};
use migration_control_protocol::schema::{Job, JobId, Worker};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Cell, Paragraph, Row, Table};
use ratatui::Frame;

pub(super) fn render_workers_tab(
    frame: &mut Frame,
    area: Rect,
    state: &AppState,
    job: &Job,
    now: DateTime<Utc>,
) {
    let theme = &state.theme;
    let workers = visible_workers(state, &job.id);
    if workers.is_empty() {
        let text = Text::from(vec![
            Line::from(Span::styled(
                "No workers assigned to this job.",
                Style::default().fg(theme.muted),
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
            Style::default().fg(theme.accent),
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
    let theme = &state.theme;
    let selected = state.ui.selected_worker.as_ref() == Some(&w.id);
    let base_style = if selected {
        Style::default()
            .bg(theme.selection_bg)
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
        Cell::from(worker_state_span(w.state, theme)),
        Cell::from(format_bytes(w.counters.bytes_per_sec as u64)),
        Cell::from(format!("{:.1}", w.counters.files_per_sec)),
        Cell::from(format!("{:.1}", w.counters.errors_per_min)),
        Cell::from(format!("{}", w.inflight_ops)),
        Cell::from(format!("{}", w.queue_depth)),
        Cell::from(format_elapsed(w.last_heartbeat, now)),
    ])
    .style(base_style)
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
