use super::common::{kv_key, kv_line, section_header, worker_state_span};
use crate::format::{format_bytes, format_elapsed};
use crate::state::{AppState, Modal};
use crate::theme::Theme;
use chrono::{DateTime, Utc};
use migration_control_protocol::schema::WorkerId;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};
use ratatui::Frame;

pub(super) fn render_modal(
    frame: &mut Frame,
    body_area: Rect,
    state: &AppState,
    modal: &Modal,
    now: DateTime<Utc>,
) {
    let theme = &state.theme;
    match modal {
        Modal::WorkerDetail { worker_id } => {
            render_worker_modal(frame, body_area, state, worker_id, now)
        }
        Modal::ConfirmCommand { command, summary } => {
            render_confirm_command_modal(frame, body_area, command, summary, theme)
        }
        Modal::Help => render_help_modal(frame, body_area, theme),
    }
}

fn render_help_modal(frame: &mut Frame, body_area: Rect, theme: &Theme) {
    // 90% tall on purpose — five binding sections + section
    // separators run ~28 lines; clipping is the worst possible UX
    // for a reference card.
    let area = centered_rect(70, 90, body_area);
    frame.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Help: keybindings (Esc to close) ")
        .border_style(Style::default().fg(theme.accent));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let mut lines: Vec<Line<'static>> = Vec::new();
    let push_section = |lines: &mut Vec<Line<'static>>, title: &str| {
        lines.push(Line::raw(""));
        lines.push(Line::from(Span::styled(
            title.to_string(),
            Style::default()
                .fg(theme.accent)
                .add_modifier(Modifier::BOLD),
        )));
    };
    let push_kv = |lines: &mut Vec<Line<'static>>, key: &str, desc: &str| {
        lines.push(Line::from(vec![
            Span::styled(format!("  {key:<14}"), Style::default().fg(theme.warn)),
            Span::raw(desc.to_string()),
        ]));
    };

    push_section(&mut lines, "Navigation (Jobs list)");
    push_kv(&mut lines, "↑ / ↓", "move cursor");
    push_kv(&mut lines, "Home / End", "jump to first / last job");
    push_kv(&mut lines, "Enter", "open job detail view");
    push_kv(&mut lines, "q / Q / Esc", "quit");

    push_section(&mut lines, "Filter & sort (Jobs list)");
    push_kv(&mut lines, "/", "enter filter mode (live)");
    push_kv(
        &mut lines,
        "s",
        "cycle sort: id → phase → progress → errors",
    );
    push_kv(&mut lines, "Enter (filter)", "commit filter");
    push_kv(
        &mut lines,
        "Esc (filter)",
        "cancel — revert to prior filter",
    );

    push_section(&mut lines, "Detail view");
    push_kv(&mut lines, "Esc / Backspace", "back to jobs list");
    push_kv(&mut lines, "Tab", "next tab");
    push_kv(&mut lines, "Shift-Tab", "previous tab");

    push_section(&mut lines, "Workers tab");
    push_kv(&mut lines, "↑ / ↓", "move worker cursor (wraps)");
    push_kv(
        &mut lines,
        "s",
        "cycle sort: mb/s → files/s → errors → host",
    );
    push_kv(&mut lines, "Enter", "open worker drill-down modal");

    push_section(&mut lines, "Command palette");
    push_kv(&mut lines, ":", "open palette");
    push_kv(&mut lines, "Tab", "cycle completions");
    push_kv(&mut lines, "Enter", "run (destructive verbs prompt y/n)");
    push_kv(&mut lines, "Esc", "cancel");
    push_kv(
        &mut lines,
        "verbs",
        "pause / resume / cancel / drain / retry-failed",
    );
    push_kv(
        &mut lines,
        "aliases",
        "stop = pause (holds at next batch); abort = cancel (final)",
    );
    push_kv(&mut lines, "local verbs", "help, quit");

    push_section(&mut lines, "Other");
    push_kv(&mut lines, "?", "this help overlay");

    frame.render_widget(Paragraph::new(Text::from(lines)), inner);
}

fn render_confirm_command_modal(
    frame: &mut Frame,
    body_area: Rect,
    command: &crate::palette::PaletteCommand,
    summary: &str,
    theme: &Theme,
) {
    // 60% wide is enough for one-line summaries; 60% tall fits the
    // borders + 6 body lines (summary + "this will change…" hint +
    // y/n prompt) without clipping on a 24-row terminal.
    let area = centered_rect(60, 60, body_area);
    frame.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .title(format!(" Confirm: {} ", command.verb()))
        .border_style(Style::default().fg(theme.warn));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let text = Text::from(vec![
        Line::raw(""),
        Line::from(Span::styled(
            summary.to_string(),
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::raw(""),
        Line::from(Span::styled(
            "This action will change job state on the coord.",
            Style::default().fg(theme.muted),
        )),
        Line::raw(""),
        Line::from(vec![
            Span::raw("Proceed? "),
            Span::styled(
                "y",
                Style::default().fg(theme.ok).add_modifier(Modifier::BOLD),
            ),
            Span::raw(" / "),
            Span::styled(
                "n",
                Style::default().fg(theme.err).add_modifier(Modifier::BOLD),
            ),
            Span::raw(" (Esc also cancels)"),
        ]),
    ]);
    frame.render_widget(Paragraph::new(text), inner);
}

fn render_worker_modal(
    frame: &mut Frame,
    body_area: Rect,
    state: &AppState,
    worker_id: &WorkerId,
    now: DateTime<Utc>,
) {
    let theme = &state.theme;
    let area = centered_rect(70, 70, body_area);
    // Clear under the modal so the workers table beneath doesn't
    // bleed through.
    frame.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Worker detail (Esc to close) ")
        .border_style(Style::default().fg(theme.accent));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let Some(w) = state.worker(worker_id) else {
        let text = Text::from(vec![
            Line::from(Span::styled(
                format!("Worker '{worker_id}' not found."),
                Style::default().fg(theme.err),
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
        worker_state_span(w.state, theme),
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
                Span::styled(reason.clone(), Style::default().fg(theme.err)),
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
