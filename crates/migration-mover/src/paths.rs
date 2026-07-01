//! Path helpers shared by the data path.
//!
//! Three small but correctness-critical helpers:
//!
//! - [`join_root`] — byte-aware concatenation of `endpoint.root` and the
//!   parquet `path` column. Per SCHEMA_CONTRACT.md "Path encoding",
//!   `path` is relative to the export root with a leading slash, and
//!   `endpoint.root` documents the prefix to prepend. Misuse here is a
//!   data-loss vector — see BUGFIX_PLAN.md.
//! - [`partial_path`] — construct the `.partial` file name in the
//!   *same directory* as the final name. Cross-directory `nfs_rename`
//!   is not atomic on NFS, so the partial must be a sibling of the
//!   final name (R2 in the M2 design).
//! - [`cstr_from_bytes`] — wrap a path byte string for FFI, mapping
//!   interior NULs to a per-file failure (R1) instead of a panic.

use crate::error::MoveError;
use migration_core::records::FailurePhase;
use std::ffi::CString;

/// Byte-aware path-join of `endpoint.root` and a parquet `path` column.
///
/// The output has exactly one leading slash and no double-slashes at
/// the join point. Both inputs are treated as raw byte sequences —
/// non-UTF-8 names round-trip unchanged. Callers pass the *raw*
/// `endpoint.root` string from the manifest and the raw `row.path`
/// bytes from the parquet column; this helper handles trailing
/// slashes, missing leading slashes on `root`, and the root-is-`/`
/// case.
///
/// Examples (see unit tests for the full table):
/// - `join_root(b"/", b"/foo/bar")` → `b"/foo/bar"`
/// - `join_root(b"/dst", b"/foo/bar")` → `b"/dst/foo/bar"`
/// - `join_root(b"/dst/", b"/foo/bar")` → `b"/dst/foo/bar"`
/// - `join_root(b"dst", b"/foo")` → `b"/dst/foo"` (defensive leading slash)
pub fn join_root(root: &[u8], path: &[u8]) -> Vec<u8> {
    // Strip a single trailing slash off root so we never produce `//`.
    let root = match root.strip_suffix(b"/") {
        Some(stripped) => stripped,
        None => root,
    };

    // Path must have a leading slash per SCHEMA_CONTRACT.md "Path
    // encoding". If the caller hands us one without, defensively
    // prepend one and ignore root entirely (we have nothing safe to
    // splice into).
    if !path.starts_with(b"/") {
        let mut out = Vec::with_capacity(path.len() + 1);
        out.push(b'/');
        out.extend_from_slice(path);
        return out;
    }

    // Root empty or "/" → just the path. Even if root was "" we want
    // the canonical form to start with "/", which path already does.
    if root.is_empty() {
        return path.to_vec();
    }

    let mut out = Vec::with_capacity(root.len() + path.len() + 1);
    if !root.starts_with(b"/") {
        out.push(b'/');
    }
    out.extend_from_slice(root);
    out.extend_from_slice(path);
    out
}

/// Build a sibling `.partial` file name for `path`.
///
/// Output shape: `<dirname>/.<basename>.<host>.<pid>.partial`. For a
/// root-level file like `/foo.txt`, the dirname is `/` and the result
/// is `/.foo.txt.<host>.<pid>.partial`.
///
/// Returns `EINVAL` for empty paths, paths with interior NULs, or
/// paths that look like directories (trailing `/`). These are real
/// per-file failures, not panics — a corrupt index row should not
/// crash the worker.
pub fn partial_path(path: &[u8], host: &str, pid: u32) -> Result<Vec<u8>, MoveError> {
    if path.is_empty() {
        return Err(MoveError::new(FailurePhase::Open, "EINVAL"));
    }
    if path.contains(&0u8) {
        return Err(MoveError::new(FailurePhase::Open, "EINVAL"));
    }
    if path.last() == Some(&b'/') {
        // Trailing slash means directory; mover never sees one for files.
        return Err(MoveError::new(FailurePhase::Open, "EISDIR"));
    }

    let last_slash = path.iter().rposition(|&b| b == b'/');
    let (dir, base) = match last_slash {
        // Keep the trailing slash on dir so we can concat without
        // re-adding it: `/foo/` + `.bar` = `/foo/.bar`.
        Some(i) => (&path[..=i], &path[i + 1..]),
        None => (&[][..], path),
    };
    if base.is_empty() {
        return Err(MoveError::new(FailurePhase::Open, "EINVAL"));
    }

    let suffix = format!(".{host}.{pid}.partial");
    let mut out = Vec::with_capacity(path.len() + 1 + suffix.len());
    out.extend_from_slice(dir);
    out.push(b'.');
    out.extend_from_slice(base);
    out.extend_from_slice(suffix.as_bytes());
    Ok(out)
}

/// Wrap path bytes as a `CString` for FFI. Maps interior NULs to a
/// per-file `EINVAL` failure rather than panicking — the index could
/// in principle contain a corrupt row.
pub fn cstr_from_bytes(p: &[u8]) -> Result<CString, MoveError> {
    CString::new(p).map_err(|_| MoveError::new(FailurePhase::Open, "EINVAL"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_path() {
        let p = partial_path(b"/foo/bar.txt", "host-A", 123).unwrap();
        assert_eq!(p, b"/foo/.bar.txt.host-A.123.partial");
    }

    #[test]
    fn root_level_file() {
        let p = partial_path(b"/foo.txt", "host-A", 123).unwrap();
        assert_eq!(p, b"/.foo.txt.host-A.123.partial");
    }

    #[test]
    fn deep_path() {
        let p = partial_path(b"/a/b/c/d/file", "h", 1).unwrap();
        assert_eq!(p, b"/a/b/c/d/.file.h.1.partial");
    }

    #[test]
    fn rejects_trailing_slash() {
        assert_eq!(partial_path(b"/foo/", "h", 1).unwrap_err().error, "EISDIR",);
    }

    #[test]
    fn rejects_empty() {
        assert_eq!(partial_path(b"", "h", 1).unwrap_err().error, "EINVAL");
    }

    #[test]
    fn rejects_nul_byte() {
        assert_eq!(
            partial_path(b"/foo\0bar", "h", 1).unwrap_err().error,
            "EINVAL",
        );
    }

    #[test]
    fn non_utf8_basename_round_trips() {
        // 0xff 0xfe are not valid UTF-8 — the partial path must
        // preserve them byte-for-byte.
        let mut p = Vec::from(&b"/foo/"[..]);
        p.extend_from_slice(&[0xff, 0xfe, b'.', b'b', b'i', b'n']);
        let out = partial_path(&p, "h", 1).unwrap();

        let mut want = Vec::from(&b"/foo/."[..]);
        want.extend_from_slice(&[0xff, 0xfe, b'.', b'b', b'i', b'n']);
        want.extend_from_slice(b".h.1.partial");
        assert_eq!(out, want);
    }

    #[test]
    fn cstr_from_bytes_ok() {
        let c = cstr_from_bytes(b"/foo/bar").unwrap();
        assert_eq!(c.as_bytes(), b"/foo/bar");
    }

    #[test]
    fn cstr_from_bytes_rejects_nul() {
        assert_eq!(cstr_from_bytes(b"/foo\0bar").unwrap_err().error, "EINVAL",);
    }

    // -------------------------------------------------------------
    // join_root tests
    //
    // The shape of these is part of the bugfix contract — they
    // exhaust the cases that matter for the data-loss bug recovered
    // in M2 verification. See BUGFIX_PLAN.md.
    // -------------------------------------------------------------

    #[test]
    fn join_root_root_is_slash() {
        assert_eq!(join_root(b"/", b"/foo/bar"), b"/foo/bar");
    }

    #[test]
    fn join_root_root_is_empty() {
        assert_eq!(join_root(b"", b"/foo/bar"), b"/foo/bar");
    }

    #[test]
    fn join_root_simple_prefix() {
        assert_eq!(join_root(b"/dst", b"/foo/bar"), b"/dst/foo/bar");
    }

    #[test]
    fn join_root_trailing_slash_on_root() {
        assert_eq!(join_root(b"/dst/", b"/foo/bar"), b"/dst/foo/bar");
    }

    #[test]
    fn join_root_path_is_just_slash() {
        // path == "/" means "the export root itself". With a non-root
        // prefix it joins to `<root>/`. This is the directory-itself
        // case; the mover never actually invokes ops against it, but
        // it's useful that the helper rounds it correctly.
        assert_eq!(join_root(b"/dst", b"/"), b"/dst/");
    }

    #[test]
    fn join_root_preserves_non_utf8_bytes() {
        // 0xff 0xfe are not valid UTF-8 — must round-trip byte-for-byte.
        let path = [b'/', 0xff, 0xfe, b'.', b'b', b'i', b'n'];
        let out = join_root(b"/dst", &path);
        let mut want = Vec::from(&b"/dst/"[..]);
        want.extend_from_slice(&[0xff, 0xfe, b'.', b'b', b'i', b'n']);
        assert_eq!(out, want);
    }

    #[test]
    fn join_root_defensive_leading_slash_on_root() {
        // If the manifest gives us a root without a leading slash, we
        // still produce a canonical `/<root>/<path>`.
        assert_eq!(join_root(b"dst", b"/foo"), b"/dst/foo");
    }

    #[test]
    fn join_root_path_without_leading_slash_normalized() {
        // If somehow path lacks a leading slash, defensively add one
        // and drop root (we have no safe way to splice).
        assert_eq!(join_root(b"/dst", b"foo/bar"), b"/foo/bar");
    }

    #[test]
    fn join_root_no_double_slash_when_both_have_slashes() {
        assert_eq!(join_root(b"/dst/", b"/foo"), b"/dst/foo");
    }
}
