#!/usr/bin/env bash
# Build a provenance-bearing release directory and deterministic tar layout.
# The Rust binaries must already exist; `make bundle` is the supported entry.
set -euo pipefail

fail() {
    printf 'build-release: %s\n' "$*" >&2
    exit 1
}

usage() {
    cat >&2 <<'EOF'
usage: build-release.sh --binary-dir DIR --target TARGET --libnfs FILE [--nfs-walker FILE] [--zig-bin FILE] [--output-dir DIR]

Environment:
  ALLOW_DIRTY=1         allow a dirty Git tree (testing only)
  SOURCE_DATE_EPOCH=N   normalize archive mtimes to N
EOF
    exit 2
}

binary_dir=
target=
libnfs=
nfs_walker=
zig_bin=
output_dir=dist
while [[ $# -gt 0 ]]; do
    case "$1" in
        --binary-dir) binary_dir=${2-}; shift 2 ;;
        --target) target=${2-}; shift 2 ;;
        --libnfs) libnfs=${2-}; shift 2 ;;
        --nfs-walker) nfs_walker=${2-}; shift 2 ;;
        --zig-bin) zig_bin=${2-}; shift 2 ;;
        --output-dir) output_dir=${2-}; shift 2 ;;
        -h|--help) usage ;;
        *) fail "unknown argument: $1" ;;
    esac
done

[[ -n "$binary_dir" && -n "$target" && -n "$libnfs" ]] || usage
for command_name in git python3 readelf sha256sum tar; do
    command -v "$command_name" >/dev/null || fail "required command not found: $command_name"
done

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(git -C "$script_dir" rev-parse --show-toplevel)
lock_file="$repo_root/packaging/libnfs.lock.json"
toolchain_lock_file="$repo_root/packaging/release-toolchain.lock.json"
[[ -f "$lock_file" ]] || fail "missing libnfs lock: $lock_file"
[[ -f "$toolchain_lock_file" ]] || fail "missing toolchain lock: $toolchain_lock_file"

binary_dir=$(realpath "$binary_dir")
libnfs=$(realpath "$libnfs")
mkdir -p "$output_dir"
output_dir=$(realpath "$output_dir")

mapfile -t libnfs_lock < <(python3 - "$lock_file" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as f:
    value = json.load(f)
for key in ("source_url", "source_git_sha", "soname", "artifact_sha256"):
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise SystemExit(f"invalid libnfs lock field: {key}")
    print(item)
PY
)
[[ ${#libnfs_lock[@]} -eq 4 ]] || fail "could not parse $lock_file"
libnfs_source_url=${libnfs_lock[0]}
libnfs_source_sha=${libnfs_lock[1]}
libnfs_soname=${libnfs_lock[2]}
libnfs_pinned_sha=${libnfs_lock[3]}

actual_libnfs_sha=$(sha256sum "$libnfs" | awk '{print $1}')
[[ "$actual_libnfs_sha" == "$libnfs_pinned_sha" ]] || fail \
    "libnfs SHA-256 is not pinned: got $actual_libnfs_sha, expected $libnfs_pinned_sha"
actual_soname=$(readelf -d "$libnfs" | sed -n 's/.*(SONAME).*\[\(.*\)\].*/\1/p')
[[ "$actual_soname" == "$libnfs_soname" ]] || fail \
    "libnfs SONAME is $actual_soname, expected $libnfs_soname"

git_sha=$(git -C "$repo_root" rev-parse HEAD)
git_short=$(git -C "$repo_root" rev-parse --short=12 HEAD)
git_dirty=false
if [[ -n "$(git -C "$repo_root" status --porcelain=v1 --untracked-files=all)" ]]; then
    git_dirty=true
    [[ "${ALLOW_DIRTY:-0}" == 1 ]] || fail \
        "Git tree is dirty; commit the release inputs or set ALLOW_DIRTY=1 for a non-deployable test bundle"
fi

version=$(cargo metadata --manifest-path "$repo_root/Cargo.toml" \
    --no-deps --format-version 1 | python3 -c \
    'import json,sys; d=json.load(sys.stdin); print(next(p["version"] for p in d["packages"] if p["name"] == "vamoose-cli"))')
safe_target=$(printf '%s' "$target" | tr -c 'A-Za-z0-9._-' '-')
release_id="vamoose-${version}-${git_short}-${safe_target}"

if [[ "$target" =~ ^(.+-linux-gnu)\.([0-9]+\.[0-9]+)$ ]]; then
    target_triple=${BASH_REMATCH[1]}
    glibc_target=${BASH_REMATCH[2]}
else
    target_triple=$target
    glibc_target=$(getconf GNU_LIBC_VERSION 2>/dev/null | awk '{print $2}')
    [[ -n "$glibc_target" ]] || glibc_target=unknown
fi

release_tmp=$(mktemp -d "$output_dir/.release-build.XXXXXX")
archive_tmp=
cleanup() {
    rm -rf -- "$release_tmp"
    [[ -z "$archive_tmp" ]] || rm -f -- "$archive_tmp"
}
trap cleanup EXIT

release_root="$release_tmp/$release_id"
install -d "$release_root/bin" "$release_root/lib" "$release_root/etc" \
    "$release_root/systemd" "$release_root/share/doc/vamoose" "$release_root/provenance"

binaries=(vamoose mig-worker mig-aggr mig-walker-rewrite)
for binary_name in "${binaries[@]}"; do
    binary_path="$binary_dir/$binary_name"
    [[ -x "$binary_path" ]] || fail "missing release binary: $binary_path"
    install -m 0755 "$binary_path" "$release_root/bin/$binary_name"
done
install -m 0755 "$libnfs" "$release_root/lib/$libnfs_soname"
if [[ -n "$nfs_walker" ]]; then
    [[ -x "$nfs_walker" ]] || fail "nfs-walker is not executable: $nfs_walker"
    install -d "$release_root/libexec"
    install -m 0755 "$nfs_walker" "$release_root/libexec/nfs-walker"
    printf 'Bundled nfs-walker (MIT), run by vamoose prepare.\nSource: https://github.com/blakegolliher/nfs-walker\nSHA256: %s\n' \
        "$(sha256sum "$nfs_walker" | cut -d' ' -f1)" \
        > "$release_root/share/doc/vamoose/NFS_WALKER_SOURCE.txt"
fi
install -m 0644 "$repo_root/examples/vamoose.toml" "$release_root/etc/vamoose.toml.example"
install -m 0600 "$repo_root/examples/vamoose.env.example" \
    "$release_root/etc/vamoose.env.example"
install -m 0644 "$repo_root/examples/vamoose-worker@.service" \
    "$release_root/systemd/vamoose-worker@.service"
install -m 0644 "$repo_root/examples/vamoose-coord.service" \
    "$release_root/systemd/vamoose-coord.service"
install -m 0644 "$repo_root/README.md" "$release_root/share/doc/vamoose/README.md"
install -m 0644 "$repo_root/docs/QUICKSTART.md" "$release_root/share/doc/vamoose/QUICKSTART.md"
install -m 0644 "$repo_root/examples/worker.toml" "$release_root/share/doc/vamoose/vamoose.toml.full"
install -m 0644 "$repo_root/THIRD_PARTY_LICENSES.md" \
    "$release_root/share/doc/vamoose/THIRD_PARTY_LICENSES.md"
install -m 0644 "$lock_file" "$release_root/provenance/libnfs.lock.json"
install -m 0644 "$toolchain_lock_file" \
    "$release_root/provenance/release-toolchain.lock.json"

glibc_required=$(for artifact in "$release_root"/bin/* "$release_root"/lib/*; do
    readelf --version-info "$artifact" 2>/dev/null \
        | sed -n 's/.*Name: GLIBC_\([0-9][0-9.]*\).*/\1/p'
done | sort -V | tail -1)
[[ -n "$glibc_required" ]] || glibc_required=unknown
if [[ "$glibc_target" != unknown && "$glibc_required" != unknown ]]; then
    newest=$(printf '%s\n%s\n' "$glibc_target" "$glibc_required" | sort -V | tail -1)
    [[ "$newest" == "$glibc_target" ]] || fail \
        "artifacts require glibc $glibc_required, newer than target $glibc_target"
fi

build_epoch=${SOURCE_DATE_EPOCH:-$(date -u +%s)}
[[ "$build_epoch" =~ ^[0-9]+$ ]] || fail "SOURCE_DATE_EPOCH must be an integer"
build_time=$(date -u -d "@$build_epoch" '+%Y-%m-%dT%H:%M:%SZ')
rustc_version=$(rustc --version | awk '{print $2}')
cargo_version=$(cargo --version | awk '{print $2}')
if [[ -n "$zig_bin" ]]; then
    zig_version=$("$zig_bin" version)
    cargo_zigbuild_version=$(cargo-zigbuild --version | awk '{print $2}')
else
    zig_version=not-used
    cargo_zigbuild_version=not-used
fi

python3 - "$release_root" "$release_id" "$version" "$git_sha" "$git_dirty" \
    "$build_time" "$build_epoch" "$target" "$target_triple" "$glibc_target" \
    "$glibc_required" "$libnfs_source_url" "$libnfs_source_sha" "$libnfs_soname" \
    "$actual_libnfs_sha" "$rustc_version" "$cargo_version" \
    "$cargo_zigbuild_version" "$zig_version" <<'PY'
import hashlib, json, pathlib, sys

(root, release_id, version, git_sha, dirty, build_time, build_epoch,
 target, target_triple, glibc_target, glibc_required, source_url,
 source_sha, soname, libnfs_sha, rustc_version, cargo_version,
 cargo_zigbuild_version, zig_version) = sys.argv[1:]
root = pathlib.Path(root)
artifacts = []
for rel in [
    "bin/vamoose", "bin/mig-worker", "bin/mig-aggr",
    "bin/mig-walker-rewrite", f"lib/{soname}",
]:
    path = root / rel
    artifacts.append({
        "path": rel,
        "size": path.stat().st_size,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    })
info = {
    "schema_version": 1,
    "release_id": release_id,
    "version": version,
    "git": {"sha": git_sha, "dirty": dirty == "true"},
    "build": {
        "timestamp_utc": build_time,
        "source_date_epoch": int(build_epoch),
        "profile": "release",
        "target": target,
        "target_triple": target_triple,
        "glibc_target": glibc_target,
        "glibc_required_max": glibc_required,
        "toolchain": {
            "rustc": rustc_version,
            "cargo": cargo_version,
            "cargo_zigbuild": cargo_zigbuild_version,
            "zig": zig_version,
        },
    },
    "libnfs": {
        "source_url": source_url,
        "source_git_sha": source_sha,
        "soname": soname,
        "sha256": libnfs_sha,
    },
    "artifacts": artifacts,
}
with (root / "build-info.json").open("w", encoding="utf-8") as f:
    json.dump(info, f, indent=2, sort_keys=True)
    f.write("\n")
PY

sums_tmp="$release_tmp/SHA256SUMS"
(
    cd "$release_root"
    find . -type f ! -name SHA256SUMS -print0 \
        | LC_ALL=C sort -z \
        | xargs -0 sha256sum > "$sums_tmp"
)
mv -- "$sums_tmp" "$release_root/SHA256SUMS"

ALLOW_DIRTY_BUNDLE=${ALLOW_DIRTY:-0} "$script_dir/verify-release.sh" "$release_root"

archive="$output_dir/${release_id}.tar.gz"
archive_tmp="$archive.tmp.$$"
tar --sort=name --owner=0 --group=0 --numeric-owner --mtime="@$build_epoch" \
    -C "$release_tmp" -czf "$archive_tmp" "$release_id"
mv -f -- "$archive_tmp" "$archive"
archive_tmp=
(
    cd "$output_dir"
    sha256sum "$(basename "$archive")" > "$(basename "$archive").sha256"
)

printf 'release bundle: %s\n' "$archive"
printf 'archive digest: %s.sha256\n' "$archive"
