#!/usr/bin/env bash
# Verify an extracted Vamoose release before it is activated.
set -euo pipefail

fail() {
    printf 'verify-release: %s\n' "$*" >&2
    exit 1
}

[[ $# -eq 1 ]] || fail "usage: verify-release.sh EXTRACTED_RELEASE_DIR"
for command_name in python3 readelf sha256sum ldd realpath; do
    command -v "$command_name" >/dev/null || fail "required command not found: $command_name"
done

release_root=$(realpath "$1")
[[ -d "$release_root" ]] || fail "not a directory: $release_root"
[[ -f "$release_root/SHA256SUMS" ]] || fail "SHA256SUMS is missing"
[[ -f "$release_root/build-info.json" ]] || fail "build-info.json is missing"
[[ -f "$release_root/provenance/libnfs.lock.json" ]] || fail "libnfs lock is missing"
[[ -f "$release_root/provenance/release-toolchain.lock.json" ]] || fail \
    "release toolchain lock is missing"

(
    cd "$release_root"
    sha256sum --check --strict SHA256SUMS
) >/dev/null || fail "bundle checksum verification failed"

mapfile -t metadata < <(python3 - "$release_root/build-info.json" \
    "$release_root/provenance/libnfs.lock.json" \
    "$release_root/provenance/release-toolchain.lock.json" <<'PY'
import json, pathlib, sys
info = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
lock = json.loads(pathlib.Path(sys.argv[2]).read_text(encoding="utf-8"))
toolchain_lock = json.loads(pathlib.Path(sys.argv[3]).read_text(encoding="utf-8"))
if (info.get("schema_version") != 1 or lock.get("schema_version") != 1
        or toolchain_lock.get("schema_version") != 1):
    raise SystemExit("unsupported provenance schema")
lib = info.get("libnfs", {})
for info_key, lock_key in (
    ("source_url", "source_url"),
    ("source_git_sha", "source_git_sha"),
    ("soname", "soname"),
    ("sha256", "artifact_sha256"),
):
    if lib.get(info_key) != lock.get(lock_key):
        raise SystemExit(f"libnfs metadata disagrees with lock: {info_key}")
toolchain = info.get("build", {}).get("toolchain", {})
if toolchain.get("cargo_zigbuild") != "not-used":
    for key in ("rustc", "cargo", "cargo_zigbuild", "zig"):
        if toolchain.get(key) != toolchain_lock.get(key):
            raise SystemExit(f"build toolchain disagrees with lock: {key}")
values = [
    info.get("release_id"),
    str(info.get("git", {}).get("dirty", "")).lower(),
    info.get("build", {}).get("target"),
    info.get("build", {}).get("glibc_target"),
    info.get("build", {}).get("glibc_required_max"),
    lib.get("soname"),
    lib.get("sha256"),
]
if any(not isinstance(v, str) or not v for v in values):
    raise SystemExit("build-info.json has missing or invalid fields")
print(*values, sep="\n")
PY
) || fail "invalid provenance metadata"
[[ ${#metadata[@]} -eq 7 ]] || fail "could not parse provenance metadata"
release_id=${metadata[0]}
git_dirty=${metadata[1]}
glibc_target=${metadata[3]}
glibc_required=${metadata[4]}
libnfs_soname=${metadata[5]}
libnfs_sha=${metadata[6]}

[[ "$(basename "$release_root")" == "$release_id" ]] || fail \
    "directory name does not match release_id ($release_id)"
if [[ "$git_dirty" != false && "${ALLOW_DIRTY_BUNDLE:-0}" != 1 ]]; then
    fail "bundle was built from a dirty Git tree"
fi

libnfs_path="$release_root/lib/$libnfs_soname"
[[ -f "$libnfs_path" ]] || fail "bundled libnfs is missing: $libnfs_soname"
[[ "$(sha256sum "$libnfs_path" | awk '{print $1}')" == "$libnfs_sha" ]] || fail \
    "bundled libnfs does not match provenance digest"
actual_soname=$(readelf -d "$libnfs_path" | sed -n 's/.*(SONAME).*\[\(.*\)\].*/\1/p')
[[ "$actual_soname" == "$libnfs_soname" ]] || fail \
    "bundled libnfs SONAME is $actual_soname, expected $libnfs_soname"

binaries=(vamoose mig-worker mig-aggr mig-walker-rewrite)
for binary_name in "${binaries[@]}"; do
    binary_path="$release_root/bin/$binary_name"
    [[ -x "$binary_path" ]] || fail "missing executable: bin/$binary_name"
    readelf -h "$binary_path" >/dev/null 2>&1 || fail "not an ELF executable: bin/$binary_name"
    ldd_output=$(LD_LIBRARY_PATH="$release_root/lib" ldd "$binary_path" 2>&1) || fail \
        "dynamic-link check failed for bin/$binary_name: $ldd_output"
    ! grep -q 'not found' <<<"$ldd_output" || fail \
        "unresolved shared library for bin/$binary_name: $ldd_output"
done

for binary_name in vamoose mig-worker; do
    binary_path="$release_root/bin/$binary_name"
    dynamic=$(readelf -d "$binary_path")
    grep -Fq "Shared library: [$libnfs_soname]" <<<"$dynamic" || fail \
        "bin/$binary_name is not linked to pinned $libnfs_soname"
    # Literal dynamic-loader token, intentionally not a shell expansion.
    # shellcheck disable=SC2016
    origin_runpath='$ORIGIN/../lib'
    grep -Fq "$origin_runpath" <<<"$dynamic" || fail \
        "bin/$binary_name lacks the bundle-relative RUNPATH"
    resolved=$(LD_LIBRARY_PATH="$release_root/lib" ldd "$binary_path" \
        | awk -v wanted="$libnfs_soname" '$1 == wanted && $2 == "=>" {print $3}')
    [[ -n "$resolved" ]] || fail "could not resolve $libnfs_soname for bin/$binary_name"
    [[ "$(realpath "$resolved")" == "$(realpath "$libnfs_path")" ]] || fail \
        "bin/$binary_name resolves libnfs outside its release: $resolved"
done

actual_glibc_required=$(for artifact in "$release_root"/bin/* "$release_root"/lib/*; do
    readelf --version-info "$artifact" 2>/dev/null \
        | sed -n 's/.*Name: GLIBC_\([0-9][0-9.]*\).*/\1/p'
done | sort -V | tail -1)
[[ "${actual_glibc_required:-unknown}" == "$glibc_required" ]] || fail \
    "glibc requirement differs from provenance: got ${actual_glibc_required:-unknown}, expected $glibc_required"
if [[ "$glibc_target" != unknown && "$glibc_required" != unknown ]]; then
    newest=$(printf '%s\n%s\n' "$glibc_target" "$glibc_required" | sort -V | tail -1)
    [[ "$newest" == "$glibc_target" ]] || fail \
        "bundle requires glibc $glibc_required, newer than target $glibc_target"
fi

LD_LIBRARY_PATH="$release_root/lib" "$release_root/bin/vamoose" --version >/dev/null
printf 'verified release: %s (target=%s, glibc<=%s)\n' \
    "$release_id" "${metadata[2]}" "$glibc_target"
