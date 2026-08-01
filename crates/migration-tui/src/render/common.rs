use crate::format::{format_bytes, format_count, format_pct};
use crate::theme::Theme;
use migration_control_protocol::schema::{Job, Phase, WorkerState};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};

pub(super) fn phase_span(phase: Phase, theme: &Theme) -> Span<'static> {
    let (label, color) = match phase {
        Phase::Planned => ("Planned", theme.phase_planned),
        Phase::Scanning => ("Scanning", theme.phase_scanning),
        Phase::Copying => ("Copying", theme.phase_copying),
        Phase::Verifying => ("Verifying", theme.phase_verifying),
        Phase::Cutover => ("Cutover", theme.phase_cutover),
        Phase::Paused => ("Paused", theme.phase_paused),
        Phase::Completed => ("Completed", theme.phase_completed),
        Phase::Failed => ("Failed", theme.phase_failed),
        Phase::Cancelled => ("Cancelled", theme.phase_cancelled),
    };
    Span::styled(label, Style::default().fg(color))
}

pub(super) fn worker_state_span(s: WorkerState, theme: &Theme) -> Span<'static> {
    let (label, color) = match s {
        WorkerState::Idle => ("Idle", theme.worker_idle),
        WorkerState::Scanning => ("Scanning", theme.worker_scanning),
        WorkerState::Copying => ("Copying", theme.worker_copying),
        WorkerState::Verifying => ("Verifying", theme.worker_verifying),
        WorkerState::Draining => ("Draining", theme.worker_draining),
        WorkerState::Fenced => ("Fenced", theme.worker_fenced),
        WorkerState::Failed => ("Failed", theme.worker_failed),
        WorkerState::Disconnected => ("Discon.", theme.worker_disconnected),
    };
    Span::styled(label, Style::default().fg(color))
}

pub(super) fn header_style() -> Style {
    // Column headers stay bold-default; bold + terminal foreground
    // reads well on both dark and light terminals without needing
    // a theme-specific override.
    Style::default().add_modifier(Modifier::BOLD)
}

// ----- small helpers for shared detail layouts -----

pub(super) fn section_header(label: &str) -> Line<'static> {
    // Section headers are styled bold only — the theme accent
    // varies between dark / light / NO_COLOR and we want the
    // header to stand out without relying on a specific color.
    Line::from(Span::styled(
        label.to_string(),
        Style::default().add_modifier(Modifier::BOLD),
    ))
}

pub(super) fn kv_key(label: &str) -> Span<'static> {
    // 13-char column for the key so values align across lines.
    // Color-less: the dimmer terminal-default already separates
    // key from value visually.
    Span::raw(format!("  {label:<11}"))
}

pub(super) fn kv_line(label: &str, value: impl Into<String>) -> Line<'static> {
    Line::from(vec![kv_key(label), Span::raw(value.into())])
}

pub(super) fn files_summary(job: &Job) -> String {
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

pub(super) fn bytes_summary(job: &Job) -> String {
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

pub(super) fn key_hint(key: &str, label: &str) -> Span<'static> {
    // Bottom-row hints use terminal-default rather than a muted
    // theme color so the readability is consistent across NO_COLOR
    // sessions (where a dim DarkGray would otherwise render
    // invisibly on dark terminals).
    Span::raw(format!("{key} {label}"))
}
