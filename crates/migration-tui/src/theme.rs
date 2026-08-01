//! Color theme for the render layer.
//!
//! All non-trivial color usage in [`crate::render`] goes through a
//! [`Theme`] instance so that:
//!
//! - `NO_COLOR=1` (per <https://no-color.org>) collapses every
//!   field to [`Color::Reset`], leaving the terminal's default
//!   foreground / background everywhere. The render layer doesn't
//!   need to special-case anything — same code path, neutral
//!   palette.
//!
//! - `VAMOOSE_THEME=light` swaps to a palette tuned for light
//!   terminals. The default (dark) palette continues to use the
//!   same colors the Phase 4 render layer hard-coded.
//!
//! - VAST teal lands as the accent color in both palettes — same
//!   spirit as `bg-teal-500` in the web dashboard.

use ratatui::style::Color;

/// Resolved color palette. The render layer takes a `&Theme` and
/// reads named fields — no raw `Color::Cyan` literals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Theme {
    // ----- Status / signal colors -----
    /// Brand accent. Section headers, modal borders, sort labels.
    pub accent: Color,
    /// Success — connection green, "verify ok".
    pub ok: Color,
    /// Caution — Paused phase, yellow filter cursor, reconnecting.
    pub warn: Color,
    /// Failure — error counters, Cancelled/Failed phases.
    pub err: Color,
    /// Muted — placeholder text, hints, dimmed disconnected
    /// workers.
    pub muted: Color,

    // ----- Phase column colors -----
    pub phase_planned: Color,
    pub phase_scanning: Color,
    pub phase_copying: Color,
    pub phase_verifying: Color,
    pub phase_cutover: Color,
    pub phase_paused: Color,
    pub phase_completed: Color,
    pub phase_failed: Color,
    pub phase_cancelled: Color,

    // ----- Worker state colors -----
    pub worker_idle: Color,
    pub worker_scanning: Color,
    pub worker_copying: Color,
    pub worker_verifying: Color,
    pub worker_draining: Color,
    pub worker_fenced: Color,
    pub worker_failed: Color,
    pub worker_disconnected: Color,

    // ----- Selection / cursor -----
    /// Background color used to highlight the selected row.
    pub selection_bg: Color,
    /// Inverted text color for the filter / palette buffer cursor.
    pub cursor_fg: Color,
    /// Background color for the filter / palette buffer cursor.
    pub cursor_bg: Color,
}

impl Theme {
    /// Dark palette — Phase 4's hard-coded look, slightly tightened
    /// so the accent threads VAST teal.
    pub const fn dark() -> Self {
        // VAST teal — picked to match the brand reference (close to
        // bg-teal-500 in the web dashboard).
        const TEAL: Color = Color::Rgb(20, 184, 166);
        Self {
            accent: TEAL,
            ok: Color::Green,
            warn: Color::Yellow,
            err: Color::Red,
            muted: Color::DarkGray,

            phase_planned: Color::Gray,
            phase_scanning: Color::Cyan,
            phase_copying: Color::Green,
            phase_verifying: Color::Cyan,
            phase_cutover: Color::Cyan,
            phase_paused: Color::Yellow,
            phase_completed: Color::Green,
            phase_failed: Color::Red,
            phase_cancelled: Color::Red,

            worker_idle: Color::Gray,
            worker_scanning: Color::Cyan,
            worker_copying: Color::Green,
            worker_verifying: Color::Cyan,
            worker_draining: Color::Yellow,
            worker_fenced: Color::Red,
            worker_failed: Color::Red,
            worker_disconnected: Color::DarkGray,

            selection_bg: Color::DarkGray,
            cursor_fg: Color::Black,
            cursor_bg: Color::Yellow,
        }
    }

    /// Light palette — same color semantics, darker shades for
    /// readability on light terminals.
    pub const fn light() -> Self {
        const TEAL: Color = Color::Rgb(15, 118, 110);
        Self {
            accent: TEAL,
            ok: Color::Rgb(21, 128, 61),  // green-700
            warn: Color::Rgb(180, 83, 9), // amber-700 — darker for contrast on white
            err: Color::Rgb(185, 28, 28), // red-700
            muted: Color::Gray,

            phase_planned: Color::DarkGray,
            phase_scanning: Color::Rgb(8, 145, 178), // cyan-600
            phase_copying: Color::Rgb(22, 163, 74),
            phase_verifying: Color::Rgb(8, 145, 178),
            phase_cutover: Color::Rgb(8, 145, 178),
            phase_paused: Color::Rgb(180, 83, 9),
            phase_completed: Color::Rgb(22, 163, 74),
            phase_failed: Color::Rgb(185, 28, 28),
            phase_cancelled: Color::Rgb(185, 28, 28),

            worker_idle: Color::DarkGray,
            worker_scanning: Color::Rgb(8, 145, 178),
            worker_copying: Color::Rgb(22, 163, 74),
            worker_verifying: Color::Rgb(8, 145, 178),
            worker_draining: Color::Rgb(180, 83, 9),
            worker_fenced: Color::Rgb(185, 28, 28),
            worker_failed: Color::Rgb(185, 28, 28),
            worker_disconnected: Color::Gray,

            selection_bg: Color::Gray,
            cursor_fg: Color::White,
            cursor_bg: Color::Rgb(180, 83, 9),
        }
    }

    /// Neutral palette: every field is [`Color::Reset`]. Activated
    /// by `NO_COLOR=1`. The render layer still applies the SAME
    /// style spans; ratatui's `Color::Reset` means "fall back to
    /// the terminal default", so bold / underline modifiers still
    /// work (per the NO_COLOR FAQ — only colors are stripped).
    pub const fn no_color() -> Self {
        Self {
            accent: Color::Reset,
            ok: Color::Reset,
            warn: Color::Reset,
            err: Color::Reset,
            muted: Color::Reset,

            phase_planned: Color::Reset,
            phase_scanning: Color::Reset,
            phase_copying: Color::Reset,
            phase_verifying: Color::Reset,
            phase_cutover: Color::Reset,
            phase_paused: Color::Reset,
            phase_completed: Color::Reset,
            phase_failed: Color::Reset,
            phase_cancelled: Color::Reset,

            worker_idle: Color::Reset,
            worker_scanning: Color::Reset,
            worker_copying: Color::Reset,
            worker_verifying: Color::Reset,
            worker_draining: Color::Reset,
            worker_fenced: Color::Reset,
            worker_failed: Color::Reset,
            worker_disconnected: Color::Reset,

            selection_bg: Color::Reset,
            cursor_fg: Color::Reset,
            cursor_bg: Color::Reset,
        }
    }

    /// Pick a theme from the environment.
    ///
    /// - `NO_COLOR=1` (or any non-empty value) → [`Self::no_color`]. Per
    ///   the standard at <https://no-color.org>, presence alone is
    ///   enough; we don't parse the value.
    /// - `VAMOOSE_THEME=light` → [`Self::light`].
    /// - anything else → [`Self::dark`].
    ///
    /// Reads via `std::env::var` so tests can drive it directly
    /// without poisoning their own env.
    pub fn from_env() -> Self {
        if env_is_set("NO_COLOR") {
            return Self::no_color();
        }
        match std::env::var("VAMOOSE_THEME").as_deref() {
            Ok("light") => Self::light(),
            _ => Self::dark(),
        }
    }
}

impl Default for Theme {
    fn default() -> Self {
        Self::dark()
    }
}

fn env_is_set(name: &str) -> bool {
    match std::env::var(name) {
        Ok(v) => !v.is_empty(),
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dark_default_palette_has_distinct_signal_colors() {
        let t = Theme::dark();
        assert_ne!(t.ok, t.warn);
        assert_ne!(t.warn, t.err);
        assert_ne!(t.ok, t.err);
        assert_ne!(t.accent, t.muted);
    }

    #[test]
    fn light_palette_differs_from_dark() {
        let d = Theme::dark();
        let l = Theme::light();
        // At least the muted color differs (DarkGray vs Gray) —
        // that's a sentinel that we didn't accidentally return the
        // same palette.
        assert_ne!(d.muted, l.muted);
    }

    #[test]
    fn no_color_palette_is_all_reset() {
        let t = Theme::no_color();
        assert_eq!(t.accent, Color::Reset);
        assert_eq!(t.ok, Color::Reset);
        assert_eq!(t.warn, Color::Reset);
        assert_eq!(t.err, Color::Reset);
        assert_eq!(t.phase_copying, Color::Reset);
        assert_eq!(t.worker_fenced, Color::Reset);
        assert_eq!(t.selection_bg, Color::Reset);
    }

    #[test]
    fn dark_and_light_share_accent_family() {
        // Both palettes anchor on VAST teal — the literal Rgb may
        // differ (light goes deeper) but neither uses a default
        // named color for the accent.
        for (label, theme) in [("dark", Theme::dark()), ("light", Theme::light())] {
            assert!(
                matches!(theme.accent, Color::Rgb(..)),
                "{label} accent must be a brand Rgb, got {:?}",
                theme.accent,
            );
        }
    }

    // env-driven from_env tests use std::env::set_var, which
    // races between tests. The serial_test crate would solve it
    // but it's not in the workspace; for now exercise the
    // selection logic directly via the public ctors. The CLI
    // path is covered by smoke.

    #[test]
    fn theme_is_copy() {
        // Compile-time guard — the render layer threads this by
        // value everywhere it reads it, so theme must be Copy.
        fn assert_copy<T: Copy>() {}
        assert_copy::<Theme>();
    }

    #[test]
    fn default_is_dark() {
        assert_eq!(Theme::default(), Theme::dark());
    }
}
