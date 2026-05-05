//! Startup overlap guard for source vs destination endpoints.
//!
//! Refuses to start the worker when a misconfigured manifest could
//! cause writes to the destination to land on the source. The check
//! is conservative — it fires whenever:
//!
//! - `source.url == dest.url` AND `source.root == dest.root`, or
//! - `source.url == dest.url` AND one root is a path-prefix of the
//!   other.
//!
//! Different URLs cannot overlap regardless of paths.
//!
//! See `BUGFIX_PLAN.md` "Fix 3" — this exists because a manifest with
//! `source.url == dest.url` and `source.root="/"` `dest.root="/dst-test"`
//! caused the mover to truncate source files. The startup guard makes
//! that class of bug impossible to trigger by misconfiguration.
//!
//! The per-file self-target check inside the mover is the second line
//! of defense. Both must exist; either alone is insufficient.

use crate::errors::Error;
use crate::records::Endpoint;

/// Verify that `source` and `dest` cannot overlap. Returns
/// `Err(Error::SourceDestOverlap)` with a formatted message ready for
/// the operator if they do.
pub fn check(source: &Endpoint, dest: &Endpoint) -> Result<(), Error> {
    if source.url != dest.url {
        return Ok(());
    }

    let s_root = source.root.as_str();
    let d_root = dest.root.as_str();

    if normalize_root(s_root) == normalize_root(d_root) {
        return Err(Error::SourceDestOverlap {
            detail: format_message(source, dest, "source.root and dest.root are identical"),
        });
    }

    if is_path_prefix(s_root, d_root) {
        let problem = format!(
            "source.url == dest.url, and source.root={s:?} is a \
             path-prefix of dest.root={d:?}. Any file written to dest \
             could overlap with source.",
            s = s_root,
            d = d_root,
        );
        return Err(Error::SourceDestOverlap {
            detail: format_message(source, dest, &problem),
        });
    }
    if is_path_prefix(d_root, s_root) {
        let problem = format!(
            "source.url == dest.url, and dest.root={d:?} is a \
             path-prefix of source.root={s:?}. Any file written to dest \
             could overlap with source.",
            s = s_root,
            d = d_root,
        );
        return Err(Error::SourceDestOverlap {
            detail: format_message(source, dest, &problem),
        });
    }

    Ok(())
}

/// Build the multi-line operator-facing detail body. The error's
/// `Display` impl prefixes "source and destination overlap; refusing
/// to start" so this body starts straight at the diagnostic block.
fn format_message(source: &Endpoint, dest: &Endpoint, problem: &str) -> String {
    format!(
        "  source.url:  {s_url}\n  \
         source.root: {s_root}\n  \
         dest.url:    {d_url}\n  \
         dest.root:   {d_root}\n\n  \
         Problem: {problem}\n\n  \
         Fix: use a separate dest export, or move the source root so\n  \
         neither root is a prefix of the other.",
        s_url = source.url,
        s_root = source.root,
        d_url = dest.url,
        d_root = dest.root,
        problem = problem,
    )
}

/// Strip a single trailing slash for normalized comparison; map empty
/// to "/" so `""` and `"/"` and `"/" -> "/"` are equivalent.
fn normalize_root(r: &str) -> &str {
    if r.is_empty() {
        return "/";
    }
    match r.strip_suffix('/') {
        Some("") => "/",
        Some(stripped) => stripped,
        None => r,
    }
}

/// True iff `parent` is a path-prefix of `child`, splitting on `/`.
/// `/` is a path-prefix of every absolute path. `/foo` is a path-prefix
/// of `/foo/bar` but NOT of `/foobar`. Trailing slashes on either input
/// are ignored.
fn is_path_prefix(parent: &str, child: &str) -> bool {
    let parent = normalize_root(parent);
    let child = normalize_root(child);

    // Same string is not a strict prefix; the equality case is
    // detected by the caller separately.
    if parent == child {
        return false;
    }

    // "/" is a path-prefix of every absolute path that isn't itself.
    if parent == "/" {
        return child.starts_with('/');
    }

    // Otherwise compare component-wise.
    let mut p_iter = parent.split('/');
    let mut c_iter = child.split('/');
    loop {
        match (p_iter.next(), c_iter.next()) {
            (Some(a), Some(b)) if a == b => continue,
            (Some(_), Some(_)) => return false,
            (None, Some(_)) => return true,
            (Some(_), None) => return false,
            (None, None) => return false, // exact equality already handled
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::records::EndpointKind;

    fn ep(url: &str, root: &str) -> Endpoint {
        Endpoint {
            kind: EndpointKind::Nfs,
            url: url.into(),
            root: root.into(),
        }
    }

    fn assert_overlap(source: &Endpoint, dest: &Endpoint) {
        match check(source, dest) {
            Err(Error::SourceDestOverlap { detail }) => {
                assert!(
                    detail.contains(&source.url),
                    "detail should reference source.url: {detail}",
                );
            }
            other => panic!("expected SourceDestOverlap, got {other:?}"),
        }
    }

    #[test]
    fn overlap_guard_rejects_identical_endpoints() {
        let s = ep("nfs://host/exp", "/data");
        let d = ep("nfs://host/exp", "/data");
        assert_overlap(&s, &d);
    }

    #[test]
    fn overlap_guard_rejects_root_prefix_in_either_direction() {
        // source.root is a prefix of dest.root.
        let s = ep("nfs://host/exp", "/");
        let d = ep("nfs://host/exp", "/dst-test");
        assert_overlap(&s, &d);

        // dest.root is a prefix of source.root.
        let s = ep("nfs://host/exp", "/data/sub");
        let d = ep("nfs://host/exp", "/data");
        assert_overlap(&s, &d);
    }

    #[test]
    fn overlap_guard_allows_different_urls() {
        let s = ep("nfs://srcA/exp", "/");
        let d = ep("nfs://srcB/exp", "/");
        assert!(check(&s, &d).is_ok());
    }

    #[test]
    fn overlap_guard_allows_disjoint_roots() {
        let s = ep("nfs://host/exp", "/foo");
        let d = ep("nfs://host/exp", "/bar");
        assert!(check(&s, &d).is_ok());
    }

    #[test]
    fn overlap_guard_handles_trailing_slashes() {
        // /foo/ vs /foo — same root.
        let s = ep("nfs://host/exp", "/foo/");
        let d = ep("nfs://host/exp", "/foo");
        assert_overlap(&s, &d);

        // /foo/ vs /foo/bar — prefix match still detected.
        let s = ep("nfs://host/exp", "/foo/");
        let d = ep("nfs://host/exp", "/foo/bar");
        assert_overlap(&s, &d);
    }

    #[test]
    fn overlap_guard_does_not_treat_substring_as_prefix() {
        // /foo is NOT a path-prefix of /foobar — they're disjoint
        // siblings under root.
        let s = ep("nfs://host/exp", "/foo");
        let d = ep("nfs://host/exp", "/foobar");
        assert!(check(&s, &d).is_ok());
    }

    #[test]
    fn overlap_guard_root_slash_is_prefix_of_anything() {
        let s = ep("nfs://host/exp", "/");
        let d = ep("nfs://host/exp", "/anything");
        assert_overlap(&s, &d);
    }

    #[test]
    fn error_message_names_both_endpoints_and_a_fix() {
        let s = ep("nfs://h/e", "/");
        let d = ep("nfs://h/e", "/dst-test");
        match check(&s, &d) {
            Err(e @ Error::SourceDestOverlap { .. }) => {
                let msg = e.to_string();
                assert!(msg.contains("source and destination overlap"), "msg: {msg}");
                assert!(msg.contains("source.url"), "msg: {msg}");
                assert!(msg.contains("dest.url"), "msg: {msg}");
                assert!(msg.contains("Fix:"), "msg: {msg}");
                assert!(msg.contains("/dst-test"), "msg: {msg}");
            }
            other => panic!("expected SourceDestOverlap, got {other:?}"),
        }
    }
}
