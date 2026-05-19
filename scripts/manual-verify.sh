#!/usr/bin/env bash
# Steps 4–7 of the M2 manual verification recipe.
#
# Usage: scripts/manual-verify.sh <src-root> <dst-root>
#
# Verifies, for the small test tree from MANUAL_VERIFY.md:
#   4. SHA-256 over file contents matches src vs dst.
#   5. POSIX metadata (mode, owner, mtime) matches modulo path prefix.
#   6. Symlink targets match.
#   7. Hardlink groupings (by inode) match — inode numbers differ
#      between filesystems, but the *partitioning of paths into
#      groups* must be identical.
#
# Exits non-zero on any mismatch. Intended to be run by hand against a
# real VAST source/dest pair after a single-worker M2 run.

set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "usage: $0 <src-root> <dst-root>" >&2
    exit 2
fi

SRC=$1
DST=$2

for d in "$SRC" "$DST"; do
    if [[ ! -d $d ]]; then
        echo "not a directory: $d" >&2
        exit 2
    fi
done

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

echo "==> [4] sha256 of regular files"
( cd "$SRC" && find . -type f -print0 | LC_ALL=C sort -z \
    | xargs -0 sha256sum ) > "$WORK/src.sha"
( cd "$DST" && find . -type f -print0 | LC_ALL=C sort -z \
    | xargs -0 sha256sum ) > "$WORK/dst.sha"
if ! diff -u "$WORK/src.sha" "$WORK/dst.sha"; then
    echo "FAIL: file content mismatch" >&2
    exit 1
fi

echo "==> [5] mode + owner + mtime"
# Use printf format that's stable: relative path, mode (octal), uid, gid,
# mtime (seconds). We deliberately omit atime — preserve_atime is a
# best-effort and can drift from access during the test.
#
# mtime is truncated to 6 fractional digits (µs) before comparison.
# The libnfs FFI surface (nfs_utimes / nfs_lutimes — neither this
# build nor upstream master export utimens/lutimens) goes through
# `struct timeval` which is µs-precision; sub-µs digits zero out on
# the wire. Comparing at full ns resolution would flag every regular
# file even when the wire was actually faithful to its precision
# ceiling. See docs/work-items/MTIME_PARITY_FIX.md for the gap.
meta_of() {
    find . -printf '%P\t%m\t%U\t%G\t%T@\n' \
        | awk -F'\t' 'BEGIN { OFS = "\t" } {
            p = index($5, ".");
            if (p > 0 && length($5) > p + 6) {
                $5 = substr($5, 1, p + 6);
            }
            print;
        }' \
        | LC_ALL=C sort
}
( cd "$SRC" && meta_of ) > "$WORK/src.meta"
( cd "$DST" && meta_of ) > "$WORK/dst.meta"
if ! diff -u "$WORK/src.meta" "$WORK/dst.meta"; then
    echo "FAIL: metadata mismatch" >&2
    exit 1
fi

echo "==> [6] symlink targets"
( cd "$SRC" && find . -type l -printf '%P -> %l\n' | LC_ALL=C sort ) \
    > "$WORK/src.sym"
( cd "$DST" && find . -type l -printf '%P -> %l\n' | LC_ALL=C sort ) \
    > "$WORK/dst.sym"
if ! diff -u "$WORK/src.sym" "$WORK/dst.sym"; then
    echo "FAIL: symlink target mismatch" >&2
    exit 1
fi

echo "==> [7] hardlink groupings"
# Inode numbers will differ between filesystems. Group paths by inode,
# strip the inode column, sort within each group, then compare the
# *sets* of groups.
group() {
    find "$1" -type f -links +1 -printf '%i\t%P\n' \
        | LC_ALL=C sort \
        | awk -F'\t' '{
            if ($1 != prev) {
                if (NR>1) print line;
                line = $2; prev = $1;
            } else {
                line = line "\t" $2;
            }
        } END { if (NR>0) print line; }' \
        | LC_ALL=C sort
}
group "$SRC" > "$WORK/src.hl"
group "$DST" > "$WORK/dst.hl"
if ! diff -u "$WORK/src.hl" "$WORK/dst.hl"; then
    echo "FAIL: hardlink grouping mismatch" >&2
    exit 1
fi

echo "OK: src and dst match across content, metadata, symlinks, hardlinks"
