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

/// `0.4ms`, `14ms`, `1.2s` — a latency the way an operator reads it.
pub(crate) fn fmt_us(us: u64) -> String {
    if us >= 1_000_000 {
        format!("{:.1}s", us as f64 / 1e6)
    } else if us >= 10_000 {
        format!("{}ms", us / 1000)
    } else if us >= 1000 {
        format!("{:.1}ms", us as f64 / 1000.0)
    } else {
        format!("{us}µs")
    }
}

/// One line per side of a latency window: `LOOKUP p50 0.4ms p95
/// 2.1ms · READ p50 …`, prefixed with the side's busy share. Shared
/// by the Overview (fleet roll-up) and the worker modal.
pub(crate) fn latency_side_lines(
    ops: &[migration_control_protocol::schema::OpLatency],
    src_busy_pct: f64,
    dst_busy_pct: f64,
    s3_wait_pct: f64,
) -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    for (side, label, share, share_label) in [
        ("src", "Source NFS", src_busy_pct, "busy"),
        ("dst", "Dest NFS", dst_busy_pct, "busy"),
        ("s3", "S3", s3_wait_pct, "wait"),
    ] {
        let detail: Vec<String> = ops
            .iter()
            .filter(|o| o.side == side)
            .map(|o| {
                format!(
                    "{} p50 {} p95 {} max {} ×{}",
                    o.op,
                    fmt_us(o.p50_us),
                    fmt_us(o.p95_us),
                    fmt_us(o.max_us),
                    format_count(o.count)
                )
            })
            .collect();
        if detail.is_empty() {
            continue;
        }
        lines.push(kv_line(
            label,
            format!("{share_label} {share:.0}%  ·  {}", detail.join("  ·  ")),
        ));
    }
    lines
}
