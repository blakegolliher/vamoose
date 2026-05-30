//! Command palette — parser, completion, and command surface.
//!
//! The palette opens on `:`. The operator types a command name
//! ("pause", "resume", "cancel", "drain", "retry-failed", "help",
//! or "quit"), optionally followed by a job id. Tab cycles through
//! suggestions: command names from an empty buffer, then visible
//! job ids once a verb is locked in.
//!
//! The parser is intentionally lenient on whitespace and case so a
//! tired operator's ":Pause  alpha-prod " parses the same as
//! ":pause alpha-prod". An empty job id is the signal to fall back
//! to the caller's "default" job (selected on the list, or the
//! job in the active Detail view).

use migration_coord::schema::JobId;

/// One command the operator can invoke through the palette.
///
/// `Help` and `Quit` are local-only — they don't hit the coord;
/// the event-loop handler intercepts them. The remaining five map
/// 1:1 to the coord's command endpoints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PaletteCommand {
    Pause {
        job_id: JobId,
    },
    Resume {
        job_id: JobId,
    },
    Cancel {
        job_id: JobId,
    },
    Drain {
        job_id: JobId,
    },
    RetryFailed {
        job_id: JobId,
    },
    /// Open the help overlay. v6b lands the overlay itself; the
    /// palette command exists here so `:help` works as soon as
    /// that step is wired.
    Help,
    /// Quit the app — same as pressing `q` from List view.
    Quit,
}

impl PaletteCommand {
    /// Whether the command requires an explicit y/n confirmation
    /// modal before it's sent. Cancel and Drain are terminal
    /// state changes (Cancel → Cancelled phase, Drain → operators
    /// usually intend "stop after in-flight"); a fat-fingered `Enter`
    /// shouldn't kill a running migration. Pause / Resume are
    /// trivially reversible — no prompt.
    pub fn is_destructive(&self) -> bool {
        matches!(
            self,
            Self::Cancel { .. } | Self::Drain { .. } | Self::RetryFailed { .. }
        )
    }

    /// Display label for the command, sans job id. Used by the
    /// confirm modal's title.
    pub fn verb(&self) -> &'static str {
        match self {
            Self::Pause { .. } => "pause",
            Self::Resume { .. } => "resume",
            Self::Cancel { .. } => "cancel",
            Self::Drain { .. } => "drain",
            Self::RetryFailed { .. } => "retry-failed",
            Self::Help => "help",
            Self::Quit => "quit",
        }
    }

    /// Job id the command targets, if any. Help / Quit return None.
    pub fn job_id(&self) -> Option<&JobId> {
        match self {
            Self::Pause { job_id }
            | Self::Resume { job_id }
            | Self::Cancel { job_id }
            | Self::Drain { job_id }
            | Self::RetryFailed { job_id } => Some(job_id),
            Self::Help | Self::Quit => None,
        }
    }
}

/// Parse failure with an operator-readable message — surfaced to
/// the banner toast on Enter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    Empty,
    UnknownVerb(String),
    MissingJobId(String),
    TrailingGarbage(String),
    InvalidJobId(String),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "empty command"),
            Self::UnknownVerb(v) => write!(
                f,
                "unknown command '{v}' (try pause/resume/cancel/drain/retry-failed/help/quit)"
            ),
            Self::MissingJobId(v) => write!(
                f,
                "'{v}' needs a job id and no default is available — try ':{v} <job-id>'"
            ),
            Self::TrailingGarbage(s) => write!(f, "unexpected extra args: {s:?}"),
            Self::InvalidJobId(s) => write!(f, "invalid job id {s:?}"),
        }
    }
}

impl std::error::Error for ParseError {}

/// All verbs the palette accepts, in canonical order. Used for tab
/// completion when the buffer hasn't yet typed a verb.
pub const VERBS: [&str; 7] = [
    "pause",
    "resume",
    "cancel",
    "drain",
    "retry-failed",
    "help",
    "quit",
];

/// Parse a palette buffer into a [`PaletteCommand`].
///
/// `default_job_id` fills in a missing job id; if `None` and the
/// verb needs one, returns [`ParseError::MissingJobId`].
pub fn parse(buffer: &str, default_job_id: Option<&JobId>) -> Result<PaletteCommand, ParseError> {
    let trimmed = buffer.trim();
    if trimmed.is_empty() {
        return Err(ParseError::Empty);
    }
    let mut parts = trimmed.split_whitespace();
    let verb_raw = parts.next().ok_or(ParseError::Empty)?;
    let verb = verb_raw.to_lowercase();
    let job_arg = parts.next();
    if let Some(extra) = parts.next() {
        return Err(ParseError::TrailingGarbage(extra.to_string()));
    }

    // Local-only verbs (no job_id).
    match verb.as_str() {
        "help" | "?" => return Ok(PaletteCommand::Help),
        "quit" | "q" | "exit" => return Ok(PaletteCommand::Quit),
        _ => {}
    }

    // Validate the verb is one we know BEFORE trying to resolve a
    // job id — otherwise a typo like ":nuke" surfaces as
    // "missing job id for nuke" instead of "unknown command nuke",
    // which is more confusing.
    let is_known_job_verb = matches!(
        verb.as_str(),
        "pause" | "resume" | "cancel" | "drain" | "retry-failed" | "retry"
    );
    if !is_known_job_verb {
        return Err(ParseError::UnknownVerb(verb));
    }

    // Job-id verbs: parse the arg if given, otherwise fall back to
    // the default.
    let job_id = match job_arg {
        Some(raw) => JobId::new(raw).map_err(|_| ParseError::InvalidJobId(raw.to_string()))?,
        None => default_job_id
            .cloned()
            .ok_or_else(|| ParseError::MissingJobId(verb.clone()))?,
    };

    Ok(match verb.as_str() {
        "pause" => PaletteCommand::Pause { job_id },
        "resume" => PaletteCommand::Resume { job_id },
        "cancel" => PaletteCommand::Cancel { job_id },
        "drain" => PaletteCommand::Drain { job_id },
        "retry-failed" | "retry" => PaletteCommand::RetryFailed { job_id },
        // Unreachable: is_known_job_verb gated above.
        _ => unreachable!(),
    })
}

/// Tab-completion suggestions for the current buffer. Two regimes:
///
/// - No space yet → suggest verbs that prefix-match the buffer.
///   On an empty buffer, suggests every verb in canonical order.
/// - Verb-then-space → suggest job ids that prefix-match the
///   typed fragment.
///
/// Returns suggestions in stable order so cycling with Tab is
/// predictable. Returns an empty Vec when nothing matches.
pub fn complete(buffer: &str, visible_job_ids: &[String]) -> Vec<String> {
    // The buffer up to (and not including) any whitespace is the
    // verb fragment; everything after the first whitespace is the
    // job-id fragment.
    let space = buffer.find(char::is_whitespace);
    match space {
        None => {
            let frag = buffer.to_lowercase();
            VERBS
                .iter()
                .filter(|v| v.starts_with(&frag))
                .map(|v| v.to_string())
                .collect()
        }
        Some(idx) => {
            let verb = &buffer[..idx];
            let after = buffer[idx..].trim_start();
            let frag = after.to_lowercase();
            visible_job_ids
                .iter()
                .filter(|j| j.to_lowercase().starts_with(&frag))
                .map(|j| format!("{verb} {j}"))
                .collect()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn jid(s: &str) -> JobId {
        JobId::new(s).unwrap()
    }

    // ----- parse() -----

    #[test]
    fn parse_pause_with_explicit_job() {
        let c = parse("pause alpha", None).unwrap();
        assert_eq!(
            c,
            PaletteCommand::Pause {
                job_id: jid("alpha")
            }
        );
    }

    #[test]
    fn parse_uses_default_job_when_missing() {
        let dj = jid("bravo");
        let c = parse("resume", Some(&dj)).unwrap();
        assert_eq!(
            c,
            PaletteCommand::Resume {
                job_id: jid("bravo")
            }
        );
    }

    #[test]
    fn parse_errors_when_verb_needs_job_and_no_default() {
        let err = parse("pause", None).unwrap_err();
        match err {
            ParseError::MissingJobId(v) => assert_eq!(v, "pause"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_is_case_insensitive_and_whitespace_tolerant() {
        let c = parse("  PaUsE   alpha   ", None).unwrap();
        assert_eq!(
            c,
            PaletteCommand::Pause {
                job_id: jid("alpha")
            }
        );
    }

    #[test]
    fn parse_help_and_quit_have_no_job() {
        assert_eq!(parse("help", None).unwrap(), PaletteCommand::Help);
        assert_eq!(parse("?", None).unwrap(), PaletteCommand::Help);
        assert_eq!(parse("quit", None).unwrap(), PaletteCommand::Quit);
        assert_eq!(parse("q", None).unwrap(), PaletteCommand::Quit);
        assert_eq!(parse("exit", None).unwrap(), PaletteCommand::Quit);
    }

    #[test]
    fn parse_retry_failed_aliases() {
        let c1 = parse("retry-failed alpha", None).unwrap();
        let c2 = parse("retry alpha", None).unwrap();
        assert_eq!(c1, c2);
        assert!(matches!(c1, PaletteCommand::RetryFailed { .. }));
    }

    #[test]
    fn parse_unknown_verb_errors() {
        let err = parse("nuke alpha", None).unwrap_err();
        match err {
            ParseError::UnknownVerb(v) => assert_eq!(v, "nuke"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_empty_buffer_errors() {
        assert!(matches!(parse("", None), Err(ParseError::Empty)));
        assert!(matches!(parse("    ", None), Err(ParseError::Empty)));
    }

    #[test]
    fn parse_rejects_trailing_args() {
        let err = parse("pause alpha extra", None).unwrap_err();
        assert!(matches!(err, ParseError::TrailingGarbage(_)));
    }

    #[test]
    fn parse_rejects_invalid_job_id_with_slash() {
        let err = parse("pause a/b", None).unwrap_err();
        assert!(matches!(err, ParseError::InvalidJobId(_)));
    }

    // ----- is_destructive -----

    #[test]
    fn destructive_classification() {
        let j = jid("alpha");
        assert!(!PaletteCommand::Pause { job_id: j.clone() }.is_destructive());
        assert!(!PaletteCommand::Resume { job_id: j.clone() }.is_destructive());
        assert!(PaletteCommand::Cancel { job_id: j.clone() }.is_destructive());
        assert!(PaletteCommand::Drain { job_id: j.clone() }.is_destructive());
        assert!(PaletteCommand::RetryFailed { job_id: j }.is_destructive());
        assert!(!PaletteCommand::Help.is_destructive());
        assert!(!PaletteCommand::Quit.is_destructive());
    }

    // ----- complete() -----

    #[test]
    fn complete_empty_buffer_suggests_all_verbs() {
        let s = complete("", &[]);
        assert_eq!(s, VERBS.iter().map(|v| v.to_string()).collect::<Vec<_>>());
    }

    #[test]
    fn complete_prefix_filters_verbs() {
        let s = complete("re", &[]);
        // "resume", "retry-failed"
        assert!(s.contains(&"resume".to_string()));
        assert!(s.contains(&"retry-failed".to_string()));
        assert!(!s.contains(&"pause".to_string()));
    }

    #[test]
    fn complete_after_verb_space_suggests_job_ids() {
        let jobs = vec![
            "alpha-prod".to_string(),
            "bravo-dev".to_string(),
            "alpha-stage".to_string(),
        ];
        let s = complete("pause ", &jobs);
        // All three jobs match an empty fragment after the space.
        assert_eq!(s.len(), 3);
        assert!(s.iter().any(|x| x == "pause alpha-prod"));
        assert!(s.iter().any(|x| x == "pause bravo-dev"));
    }

    #[test]
    fn complete_after_verb_with_prefix_filters_jobs() {
        let jobs = vec![
            "alpha-prod".to_string(),
            "bravo-dev".to_string(),
            "alpha-stage".to_string(),
        ];
        let s = complete("pause al", &jobs);
        // Two "alpha-*" matches, bravo excluded.
        assert_eq!(s.len(), 2);
        for x in &s {
            assert!(x.starts_with("pause alpha"), "got {x:?}");
        }
    }

    #[test]
    fn complete_no_matches_returns_empty() {
        let s = complete("xyz", &[]);
        assert!(s.is_empty());
    }
}
