#!/usr/bin/env bash
# Validate and stage the exact release libnfs for the Rust linker.
set -euo pipefail

fail() {
    printf 'stage-pinned-libnfs: %s\n' "$*" >&2
    exit 1
}

[[ $# -eq 2 ]] || fail "usage: stage-pinned-libnfs.sh LIBNFS_SO OUTPUT_DIR"
libnfs=$(realpath "$1")
output_dir=$2
[[ -f "$libnfs" ]] || fail "libnfs not found: $libnfs"

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(git -C "$script_dir" rev-parse --show-toplevel)
lock_file="$repo_root/packaging/libnfs.lock.json"
[[ -f "$lock_file" ]] || fail "missing libnfs lock: $lock_file"

mapfile -t expected < <(python3 - "$lock_file" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as f:
    value = json.load(f)
if value.get("schema_version") != 1:
    raise SystemExit("unsupported libnfs lock schema")
for key in ("soname", "artifact_sha256"):
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise SystemExit(f"invalid libnfs lock field: {key}")
    print(item)
PY
)
[[ ${#expected[@]} -eq 2 ]] || fail "could not parse $lock_file"
soname=${expected[0]}
pinned_sha=${expected[1]}

actual_sha=$(sha256sum "$libnfs" | awk '{print $1}')
[[ "$actual_sha" == "$pinned_sha" ]] || fail \
    "libnfs SHA-256 is $actual_sha, expected pinned $pinned_sha"
actual_soname=$(readelf -d "$libnfs" | sed -n 's/.*(SONAME).*\[\(.*\)\].*/\1/p')
[[ "$actual_soname" == "$soname" ]] || fail \
    "libnfs SONAME is $actual_soname, expected $soname"

mkdir -p "$output_dir"
install -m 0755 "$libnfs" "$output_dir/$soname"
ln -sfn "$soname" "$output_dir/libnfs.so"
printf 'staged pinned libnfs: %s (%s)\n' "$output_dir/$soname" "$actual_sha"
