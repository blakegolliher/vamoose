#!/usr/bin/env bash
# Validate the compiler tools used for glibc-targeted release builds.
set -euo pipefail

fail() {
    printf 'check-release-toolchain: %s\n' "$*" >&2
    exit 1
}

zig_bin=
while [[ $# -gt 0 ]]; do
    case "$1" in
        --zig) zig_bin=${2-}; shift 2 ;;
        -h|--help) fail "usage: check-release-toolchain.sh --zig /path/to/zig" ;;
        *) fail "unknown argument: $1" ;;
    esac
done

[[ -n "$zig_bin" ]] || fail \
    "Zig not found (set ZIG=/absolute/path/to/zig; Snap installs use /snap/zig/current/zig)"
[[ -x "$zig_bin" ]] || fail "Zig is not executable: $zig_bin"
for command_name in rustc cargo cargo-zigbuild python3; do
    command -v "$command_name" >/dev/null || fail "required command not found: $command_name"
done

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(git -C "$script_dir" rev-parse --show-toplevel)
lock_file="$repo_root/packaging/release-toolchain.lock.json"
[[ -f "$lock_file" ]] || fail "missing release toolchain lock: $lock_file"

mapfile -t expected < <(python3 - "$lock_file" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as f:
    value = json.load(f)
if value.get("schema_version") != 1:
    raise SystemExit("unsupported release toolchain lock schema")
for key in ("rustc", "cargo", "cargo_zigbuild", "zig"):
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise SystemExit(f"invalid release toolchain field: {key}")
    print(item)
PY
)
[[ ${#expected[@]} -eq 4 ]] || fail "could not parse $lock_file"

actual_rustc=$(rustc --version | awk '{print $2}')
actual_cargo=$(cargo --version | awk '{print $2}')
actual_zigbuild=$(cargo-zigbuild --version | awk '{print $2}')
actual_zig=$("$zig_bin" version)

names=(rustc cargo cargo-zigbuild zig)
actual=("$actual_rustc" "$actual_cargo" "$actual_zigbuild" "$actual_zig")
for index in "${!names[@]}"; do
    [[ "${actual[$index]}" == "${expected[$index]}" ]] || fail \
        "${names[$index]} version ${actual[$index]} does not match pinned ${expected[$index]}"
done

printf 'release toolchain: rustc %s, cargo %s, cargo-zigbuild %s, zig %s\n' \
    "$actual_rustc" "$actual_cargo" "$actual_zigbuild" "$actual_zig"
