#!/usr/bin/env bash
# Verify and atomically activate a Vamoose release on one host.
set -euo pipefail

fail() {
    printf 'install-release: %s\n' "$*" >&2
    exit 1
}

usage() {
    cat >&2 <<'EOF'
usage: install-release.sh BUNDLE.tar.gz [--prefix /opt/vamoose] [--allow-dirty]

The archive's adjacent .sha256 file is mandatory. Releases are installed at
PREFIX/releases/RELEASE_ID and PREFIX/current is switched with one rename.
Existing release directories are immutable and are never overwritten.
EOF
    exit 2
}

[[ $# -gt 0 ]] || usage
archive=$1
shift
prefix=/opt/vamoose
allow_dirty=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --prefix) prefix=${2-}; shift 2 ;;
        --allow-dirty) allow_dirty=1; shift ;;
        -h|--help) usage ;;
        *) fail "unknown argument: $1" ;;
    esac
done

for command_name in python3 tar sha256sum realpath; do
    command -v "$command_name" >/dev/null || fail "required command not found: $command_name"
done
[[ "$prefix" == /* && "$prefix" != / ]] || fail "--prefix must be an absolute non-root path"
archive=$(realpath "$archive")
digest_file="$archive.sha256"
[[ -f "$archive" ]] || fail "bundle not found: $archive"
[[ -f "$digest_file" ]] || fail "archive digest not found: $digest_file"

(
    cd "$(dirname "$archive")"
    sha256sum --check --strict "$(basename "$digest_file")"
) >/dev/null || fail "archive digest verification failed"

release_name=$(python3 - "$archive" <<'PY'
import pathlib, sys, tarfile
archive = pathlib.Path(sys.argv[1])
with tarfile.open(archive, "r:gz") as tf:
    members = tf.getmembers()
    if not members:
        raise SystemExit("empty release archive")
    tops = set()
    for member in members:
        path = pathlib.PurePosixPath(member.name)
        if path.is_absolute() or ".." in path.parts or not path.parts:
            raise SystemExit(f"unsafe archive path: {member.name}")
        if member.isdev() or member.issym() or member.islnk():
            raise SystemExit(f"unsupported archive entry: {member.name}")
        tops.add(path.parts[0])
    if len(tops) != 1:
        raise SystemExit("archive must contain exactly one release directory")
    print(tops.pop())
PY
) || fail "archive layout validation failed"
[[ "$release_name" == vamoose-* ]] || fail "unexpected release directory: $release_name"

extract_tmp=$(mktemp -d "${TMPDIR:-/tmp}/vamoose-install.XXXXXX")
activation_tmp=
cleanup() {
    rm -rf -- "$extract_tmp"
    [[ -z "$activation_tmp" ]] || rm -f -- "$activation_tmp"
}
trap cleanup EXIT
tar -xzf "$archive" -C "$extract_tmp" --no-same-owner --no-same-permissions
extracted="$extract_tmp/$release_name"

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ALLOW_DIRTY_BUNDLE=$allow_dirty "$script_dir/verify-release.sh" "$extracted"

release_id=$(python3 -c \
    'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["release_id"])' \
    "$extracted/build-info.json")
[[ "$release_id" == "$release_name" ]] || fail "archive directory and release_id differ"

releases_dir="$prefix/releases"
release_dir="$releases_dir/$release_id"
install -d -m 0755 "$releases_dir"
if [[ -e "$release_dir" ]]; then
    [[ -d "$release_dir" ]] || fail "release path exists and is not a directory: $release_dir"
    ALLOW_DIRTY_BUNDLE=$allow_dirty "$script_dir/verify-release.sh" "$release_dir"
    cmp -s "$extracted/SHA256SUMS" "$release_dir/SHA256SUMS" || fail \
        "immutable release ID already exists with different contents: $release_id"
else
    incoming="$releases_dir/.${release_id}.incoming.$$"
    [[ ! -e "$incoming" ]] || fail "temporary install path already exists: $incoming"
    cp -a -- "$extracted" "$incoming"
    mv -- "$incoming" "$release_dir"
fi

# Everything above, including dynamic linkage and an executable smoke test,
# completes before this single rename changes what `current` references.
previous_release=$(readlink "$prefix/current" 2>/dev/null || true)
activation_tmp="$prefix/.current.${release_id}.$$"
ln -s "releases/$release_id" "$activation_tmp"
mv -Tf -- "$activation_tmp" "$prefix/current"
activation_tmp=

printf 'activated release: %s/current -> releases/%s\n' "$prefix" "$release_id"
if [[ -n "$previous_release" ]]; then
    printf 'previous release: %s\n' "$previous_release"
fi
