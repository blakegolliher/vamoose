#!/usr/bin/env bash
# Manual verification recipes against a real NFS source/destination pair.
#
# Two modes:
#
#   scripts/manual-verify.sh <src-root> <dst-root>
#       Steps 4–7 of the M2 manual verification recipe (MANUAL_VERIFY.md):
#       compare SHA-256, POSIX metadata, symlink targets, and hardlink
#       groupings of two kernel-mounted trees. Exits non-zero on mismatch.
#
#   scripts/manual-verify.sh inject <src-mount> <dst-mount> \
#                                    <src-nfs-url> <dst-nfs-url> [work-dir]
#       Injected `vamoose verify --mode sample` scenarios. `<src-mount>` and
#       `<dst-mount>` are kernel mounts of the export roots that
#       `<src-nfs-url>` / `<dst-nfs-url>` name (`nfs://server/export`); the
#       script creates a throwaway tree under each mount, perturbs the
#       destination per scenario, runs the verifier through libnfs with a
#       local manifest, and asserts the exact exit code, terminal status,
#       and mismatch-kind counts from the JSON report. Run as root (the
#       verifier's libnfs contexts use the calling UID). Requires `vamoose`
#       on PATH (or VAMOOSE_BIN) and python3 for JSON assertions. Set
#       KEEP=1 to leave the injected trees and work directory in place.
#
# Neither mode is a substitute for CI: they exist because libnfs FFI and
# NFS server behaviour cannot be mocked.

set -euo pipefail

usage() {
    echo "usage: $0 <src-root> <dst-root>" >&2
    echo "       $0 inject <src-mount> <dst-mount> <src-nfs-url> <dst-nfs-url> [work-dir]" >&2
    exit 2
}

# ---------------------------------------------------------------------------
# Mode 1: tree comparison (steps 4–7).
# ---------------------------------------------------------------------------
compare_trees() {
    local SRC=$1 DST=$2
    for d in "$SRC" "$DST"; do
        if [[ ! -d $d ]]; then
            echo "not a directory: $d" >&2
            exit 2
        fi
    done

    local WORK
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
}

# ---------------------------------------------------------------------------
# Mode 2: injected sampled-content scenarios.
# ---------------------------------------------------------------------------
VAMOOSE=${VAMOOSE_BIN:-vamoose}
FAILURES=0

# Assert one scenario's report. Arguments: name, expected exit code,
# expected status, expected mismatches_by_kind as "kind=count,...",
# expected content_matches, expected content_mismatches, expected
# unreadable_entries, expected unstable_entries, actual exit code,
# report path.
assert_report() {
    local name=$1 want_exit=$2 want_status=$3 want_kinds=$4 want_matches=$5 \
          want_content_mismatches=$6 want_unreadable=$7 want_unstable=$8 \
          got_exit=$9 report=${10}
    if [[ $got_exit -ne $want_exit ]]; then
        echo "FAIL [$name]: exit $got_exit, expected $want_exit" >&2
        FAILURES=$((FAILURES + 1))
    fi
    if [[ ! -f $report ]]; then
        echo "FAIL [$name]: no report at $report" >&2
        FAILURES=$((FAILURES + 1))
        return
    fi
    local problems
    problems=$(python3 - "$report" "$want_status" "$want_kinds" "$want_matches" \
        "$want_content_mismatches" "$want_unreadable" "$want_unstable" <<'PY'
import json, sys
report = json.load(open(sys.argv[1]))
want_status, want_kinds, want_matches, want_cm, want_unreadable, want_unstable = sys.argv[2:8]
expected_kinds = {}
if want_kinds:
    for item in want_kinds.split(","):
        kind, count = item.split("=")
        expected_kinds[kind] = int(count)
problems = []
if report["status"] != want_status:
    problems.append(f"status {report['status']!r} != {want_status!r}")
if report["mismatches_by_kind"] != expected_kinds:
    problems.append(f"mismatches_by_kind {report['mismatches_by_kind']} != {expected_kinds}")
for field, want in (
    ("content_matches", int(want_matches)),
    ("content_mismatches", int(want_cm)),
    ("unreadable_entries", int(want_unreadable)),
    ("unstable_entries", int(want_unstable)),
):
    if report[field] != want:
        problems.append(f"{field} {report[field]} != {want}")
if report["mode"] != "sample" or not report["comparison_policy"]["content"]:
    problems.append("report does not claim sample content verification")
if not report["content_complete"]:
    problems.append("content_complete is false")
if report["sample_policy"]["selected_files"] != report["sample_policy"]["eligible_files"]:
    problems.append("expected every eligible file to be selected (raise --sample-files)")
print("\n".join(problems))
PY
)
    if [[ -n $problems ]]; then
        echo "FAIL [$name]:" >&2
        echo "$problems" | sed 's/^/    /' >&2
        FAILURES=$((FAILURES + 1))
    else
        echo "ok   [$name]"
    fi
}

write_manifest() {
    local path=$1 src_url=$2 dst_url=$3 root=$4
    cat > "$path" <<JSON
{
  "format_version": 1,
  "run_id": "manual-inject",
  "created_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "shards": [],
  "total_rows": 0,
  "source": {"kind": "nfs", "url": "$src_url", "root": "$root"},
  "dest": {"kind": "nfs", "url": "$dst_url", "root": "$root"},
  "exclusions": [],
  "options": {
    "preserve_owner": true,
    "preserve_mode": true,
    "preserve_times": true,
    "preserve_xattr": false,
    "server_side_copy": "off"
  }
}
JSON
}

# Populate the baseline tree: small, multi-chunk, zero-length, a
# non-UTF-8 name, a symlink, and a hardlink pair.
build_tree() {
    local root=$1
    mkdir -p "$root/dir"
    printf 'hello world\n' > "$root/dir/small.txt"
    head -c $((5 * 1024 * 1024 + 7)) /dev/urandom > "$root/dir/multi-chunk.bin"
    : > "$root/dir/empty"
    printf 'raw name\n' > "$root/dir/$(printf 'bad-\xff-name')"
    ln -s small.txt "$root/dir/link"
    printf 'shared\n' > "$root/dir/hl-a"
    ln "$root/dir/hl-a" "$root/dir/hl-b"
}

run_verify() {
    local name=$1 manifest=$2 work=$3
    shift 3
    local exit_code=0
    "$VAMOOSE" verify --mode sample --manifest "$manifest" --writers-stopped \
        --assume-no-risk-history --work-dir "$work" --verification-id "$name" \
        --sample-files 1000 --content-workers 2 --json "$@" \
        > "$work/$name.stdout" 2> "$work/$name.stderr" || exit_code=$?
    echo "$exit_code"
}

inject_scenarios() {
    local SRC_MOUNT=$1 DST_MOUNT=$2 SRC_URL=$3 DST_URL=$4 WORK=${5:-}
    for d in "$SRC_MOUNT" "$DST_MOUNT"; do
        if [[ ! -d $d ]]; then
            echo "not a directory: $d" >&2
            exit 2
        fi
    done
    command -v python3 >/dev/null || { echo "python3 is required" >&2; exit 2; }
    command -v "$VAMOOSE" >/dev/null || { echo "vamoose not found (set VAMOOSE_BIN)" >&2; exit 2; }

    local stamp
    stamp=$(date +%Y%m%dT%H%M%S)
    local rel="verify-inject-$stamp"
    local SRC="$SRC_MOUNT/$rel" DST="$DST_MOUNT/$rel"
    if [[ -z $WORK ]]; then
        WORK=$(mktemp -d)
    fi
    mkdir -p "$WORK"
    if [[ ${KEEP:-0} != 1 ]]; then
        trap 'rm -rf "$SRC" "$DST" "$WORK"' EXIT
    else
        echo "KEEP=1: leaving $SRC, $DST, and $WORK in place"
    fi

    echo "==> building baseline tree under $rel"
    build_tree "$SRC"
    cp -a "$SRC" "$DST"
    local MANIFEST="$WORK/manifest.json"
    write_manifest "$MANIFEST" "$SRC_URL" "$DST_URL" "/$rel"
    local report exit_code

    echo "==> [equal] identical trees (includes zero-length and non-UTF-8 names)"
    exit_code=$(run_verify equal "$MANIFEST" "$WORK")
    report="$WORK/equal/report.json"
    # Six regular files: small, multi-chunk, empty, the non-UTF-8 name, and
    # the hardlink pair; the symlink is metadata only.
    assert_report equal 0 passed "" 6 0 0 0 "$exit_code" "$report"

    echo "==> [same-size-corrupt] one flipped byte in a destination file"
    printf 'jello world\n' > "$DST/dir/small.txt"
    touch -r "$SRC/dir/small.txt" "$DST/dir/small.txt"
    exit_code=$(run_verify same-size-corrupt "$MANIFEST" "$WORK")
    report="$WORK/same-size-corrupt/report.json"
    assert_report same-size-corrupt 2 mismatched "content=1" 5 1 0 0 "$exit_code" "$report"
    cp -p "$SRC/dir/small.txt" "$DST/dir/small.txt"

    echo "==> [truncated] destination file shorter than the source"
    head -c 4096 "$SRC/dir/multi-chunk.bin" > "$DST/dir/multi-chunk.bin"
    touch -r "$SRC/dir/multi-chunk.bin" "$DST/dir/multi-chunk.bin"
    exit_code=$(run_verify truncated "$MANIFEST" "$WORK")
    report="$WORK/truncated/report.json"
    # A size mismatch is metadata evidence and makes the pair mandatory risk;
    # both sides still hash completely at their own sizes, so the digests
    # differ and a content record follows the size record.
    assert_report truncated 2 mismatched "size=1,content=1" 5 1 0 0 "$exit_code" "$report"
    cp -p "$SRC/dir/multi-chunk.bin" "$DST/dir/multi-chunk.bin"

    echo "==> [non-utf8-corrupt] corrupted content behind a non-UTF-8 name"
    printf 'raw nope\n' > "$DST/dir/$(printf 'bad-\xff-name')"
    touch -r "$SRC/dir/$(printf 'bad-\xff-name')" "$DST/dir/$(printf 'bad-\xff-name')"
    exit_code=$(run_verify non-utf8-corrupt "$MANIFEST" "$WORK")
    report="$WORK/non-utf8-corrupt/report.json"
    assert_report non-utf8-corrupt 2 mismatched "content=1" 5 1 0 0 "$exit_code" "$report"
    cp -p "$SRC/dir/$(printf 'bad-\xff-name')" "$DST/dir/$(printf 'bad-\xff-name')"

    echo "==> [unreadable] destination file with mode 000"
    # Not a hardlink member: chmod on one link would change both.
    chmod 000 "$DST/dir/empty"
    if cat "$DST/dir/empty" >/dev/null 2>&1; then
        echo "skip [unreadable]: the export does not enforce mode bits for this UID" \
             "(root is not squashed); the unreadable scenario cannot be injected here"
        chmod --reference="$SRC/dir/empty" "$DST/dir/empty"
    else
        exit_code=$(run_verify unreadable "$MANIFEST" "$WORK")
        report="$WORK/unreadable/report.json"
        # mode drift is metadata evidence; the open then fails on the
        # destination, which is a failed (not mismatched) verification.
        assert_report unreadable 1 failed "mode=1,unreadable_destination=1" 5 0 1 0 \
            "$exit_code" "$report"
        chmod --reference="$SRC/dir/empty" "$DST/dir/empty"
    fi

    echo "==> [mid-read-mutated] destination file touched while it is verified"
    (
        while [[ ! -f "$WORK/mutate.stop" ]]; do
            touch "$DST/dir/multi-chunk.bin"
            sleep 0.05
        done
    ) &
    local mutator=$!
    exit_code=$(run_verify mid-read-mutated "$MANIFEST" "$WORK")
    : > "$WORK/mutate.stop"
    wait "$mutator" 2>/dev/null || true
    report="$WORK/mid-read-mutated/report.json"
    # The touch also drifts mtime relative to the source, so the pair is
    # mandatory risk with an mtime record; the content bracket then sees the
    # attributes move and the run is inconclusive.
    assert_report mid-read-mutated 3 inconclusive "mtime=1,unstable_destination=1" 5 0 0 1 \
        "$exit_code" "$report"
    touch -r "$SRC/dir/multi-chunk.bin" "$DST/dir/multi-chunk.bin"

    if [[ $FAILURES -ne 0 ]]; then
        echo "FAIL: $FAILURES injected scenario(s) did not match; reports under $WORK" >&2
        exit 1
    fi
    echo "OK: every injected sampled-content scenario produced the expected exit code, status, and counts"
}

if [[ ${1:-} == inject ]]; then
    shift
    if [[ $# -lt 4 || $# -gt 5 ]]; then
        usage
    fi
    inject_scenarios "$@"
elif [[ $# -eq 2 ]]; then
    compare_trees "$1" "$2"
else
    usage
fi
