//! Shared top banner and bottom key-hint chrome.
//!
//! Jobs-list layout (the bottom row changes with view and input mode):
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────┐
//! │ vamoose · ● connected · 5 jobs, 1.2M files, 8.4TiB          │
//! ├──────────────────────────────────────────────────────────────┤
//! │ Job           Phase    Progress              Workers  Errs  │
//! │ alpha-mig     Copying  ▓▓▓▓▓░░░░░  45%        3      0      │
//! │ bravo-mig     Paused   ▓▓▓░░░░░░░  18%        1      4      │
//! │ …                                                            │
//! ├──────────────────────────────────────────────────────────────┤
//! │ q quit  / filter  : command  ? help  s sort  ↑↓ select       │
//! └──────────────────────────────────────────────────────────────┘
//! ```

use super::common::key_hint;
use super::jobs::aggregate_counts;
use crate::format::{format_bytes, format_count, format_elapsed};
use crate::state::{AppState, ConnectionStatus, View};
use crate::theme::Theme;
use chrono::{DateTime, Utc};
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::Frame;

// =============================================================================
// Top banner
// =============================================================================

pub(super) fn render_top_banner(
    frame: &mut Frame,
    area: Rect,
    state: &AppState,
    now: DateTime<Utc>,
) {
    let theme = &state.theme;
    let agg = aggregate_counts(state);
    let conn = connection_label(&state.connection, now, theme);
    // 1-min total throughput across all jobs. Append "/s" so the
    // unit is unambiguous even when the value rounds to 0.
    let total_bps = state.total_bytes_per_sec(60, now);
    let total_fps = state.total_files_per_sec(60, now);
    let throughput_label = if total_fps >= 1.0 {
        format!(
            " · {total_fps:.0} files/s · {}/s",
            format_bytes(total_bps as u64)
        )
    } else if total_bps > 0.0 {
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

    // Unknown-events counter (F38) — nonzero means a newer coord
    // streamed kinds this build predates and the view may be
    // missing information the operator would want. Warn-colored,
    // omitted entirely at zero to keep the banner quiet.
    if state.unknown_events > 0 {
        spans.push(Span::raw(" · "));
        spans.push(Span::styled(
            format!("{} unknown ev", state.unknown_events),
            Style::default().fg(theme.warn),
        ));
    }

    // Filter span — show the live edit buffer with a cursor glyph
    // while in filter mode, or the committed filter otherwise.
    // Palette mode doesn't touch the filter (it uses the bottom
    // line for its own input echo) — fall through to the Normal
    // arm so the previously-committed filter still surfaces.
    match &state.ui.input_mode {
        crate::state::InputMode::Filter { buffer, .. } => {
            spans.push(Span::raw(" · filter:"));
            // Cursor glyph after the buffer so the operator can see
            // where their next character will land.
            spans.push(Span::styled(
                format!("{buffer}_"),
                Style::default()
                    .fg(theme.cursor_fg)
                    .bg(theme.cursor_bg)
                    .add_modifier(Modifier::BOLD),
            ));
        }
        crate::state::InputMode::Normal | crate::state::InputMode::Palette { .. } => {
            if !state.ui.filter.is_empty() {
                spans.push(Span::raw(" · filter:"));
                spans.push(Span::styled(
                    state.ui.filter.clone(),
                    Style::default().fg(theme.warn),
                ));
            }
        }
    }

    // Sort label is always shown so the operator knows what `s`
    // will cycle to next.
    spans.push(Span::raw(" · sort:"));
    spans.push(Span::styled(
        state.ui.sort.label(),
        Style::default().fg(theme.accent),
    ));

    // Command-result toast — appended to the right of the banner
    // so it's visible whichever view the operator is on. The TTL
    // tick that drops the toast lives in the event loop (called
    // before each render); here we just surface whatever is set.
    if let Some(s) = &state.command_status {
        let (glyph, color) = match s.kind {
            crate::state::CommandStatusKind::Ok => ("✓", theme.ok),
            crate::state::CommandStatusKind::Error => ("✗", theme.err),
        };
        spans.push(Span::raw("  "));
        spans.push(Span::styled(
            format!("{glyph} {}", s.message),
            Style::default().fg(color).add_modifier(Modifier::BOLD),
        ));
    }

    let para = Paragraph::new(Line::from(spans));
    frame.render_widget(para, area);
}

fn connection_label(status: &ConnectionStatus, now: DateTime<Utc>, theme: &Theme) -> Span<'static> {
    match status {
        ConnectionStatus::Connected { last_traffic } => Span::styled(
            format!("● connected ({})", format_elapsed(*last_traffic, now)),
            Style::default().fg(theme.ok),
        ),
        ConnectionStatus::Reconnecting { since, last_error } => Span::styled(
            format!(
                "● reconnecting {} — {}",
                format_elapsed(*since, now),
                last_error
            ),
            Style::default().fg(theme.warn),
        ),
        ConnectionStatus::Disconnected { reason } => Span::styled(
            format!("● offline — {reason}"),
            Style::default().fg(theme.err),
        ),
    }
}

// =============================================================================
// Bottom hints
// =============================================================================

pub(super) fn render_bottom_hints(
    frame: &mut Frame,
    area: Rect,
    state: &AppState,
    _now: DateTime<Utc>,
) {
    let theme = &state.theme;
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
            Span::styled("(typing builds filter)", Style::default().fg(theme.muted)),
        ],
        (InputMode::Palette { buffer, .. }, _) => {
            // The palette owns the bottom row while it's active:
            // we render the leading colon + the operator's buffer
            // + a cursor glyph, then a short key-binding hint set.
            vec![
                Span::styled(
                    ":",
                    Style::default()
                        .fg(theme.accent)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    format!("{buffer}_"),
                    Style::default()
                        .fg(theme.cursor_fg)
                        .bg(theme.cursor_bg)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw("  "),
                key_hint("Tab", "complete"),
                Span::raw("  "),
                key_hint("Enter", "run"),
                Span::raw("  "),
                key_hint("Esc", "cancel"),
            ]
        }
        (InputMode::Normal, View::List) => vec![
            key_hint("q", "quit"),
            Span::raw("  "),
            key_hint("/", "filter"),
            Span::raw("  "),
            key_hint(":", "command"),
            Span::raw("  "),
            key_hint("?", "help"),
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
            key_hint(":", "command"),
            Span::raw("  "),
            key_hint("q", "quit"),
        ],
    };
    let para = Paragraph::new(Line::from(hints));
    frame.render_widget(para, area);
}
