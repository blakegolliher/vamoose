#!/usr/bin/env bash
# Check a built nfs-walker against packaging/nfs-walker.lock.json and
# print the provenance block the packages ship as NFS_WALKER_SOURCE.txt.
#
#   scripts/check-pinned-walker.sh /path/to/nfs-walker > NFS_WALKER_SOURCE.txt
#
# A binary whose SHA-256 or `--version` differs from the lock is refused
# (exit 1) so a package can never ship a scanner `vamoose prepare` was
# not built against — the flag set drifts between nfs-walker branches.
# ALLOW_UNPINNED_WALKER=1 downgrades that to a warning and records the
# binary as unpinned; for local experiments only.
set -euo pipefail

fail() {
    printf 'check-pinned-walker: %s\n' "$*" >&2
    exit 1
}

[[ $# -eq 1 ]] || fail "usage: check-pinned-walker.sh NFS_WALKER_BIN"
walker=$(realpath "$1")
[[ -x "$walker" ]] || fail "nfs-walker is not executable: $walker"

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
lock_file="$script_dir/../packaging/nfs-walker.lock.json"
[[ -f "$lock_file" ]] || fail "missing nfs-walker lock: $lock_file"

mapfile -t expected < <(python3 - "$lock_file" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as f:
    value = json.load(f)
if value.get("schema_version") != 1:
    raise SystemExit("unsupported nfs-walker lock schema")
for key in ("source_url", "source_git_ref", "source_git_sha", "version", "artifact_sha256"):
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise SystemExit(f"invalid nfs-walker lock field: {key}")
    print(item)
PY
)
[[ ${#expected[@]} -eq 5 ]] || fail "could not parse $lock_file"
source_url=${expected[0]}
git_ref=${expected[1]}
git_sha=${expected[2]}
version=${expected[3]}
pinned_sha=${expected[4]}

actual_sha=$(sha256sum "$walker" | awk '{print $1}')
actual_version=$("$walker" --version 2>/dev/null | head -n1 || true)

problems=()
[[ "$actual_sha" == "$pinned_sha" ]] || \
    problems+=("SHA-256 is $actual_sha, lock pins $pinned_sha")
[[ "$actual_version" == "$version" ]] || \
    problems+=("--version prints '${actual_version:-nothing}', lock pins '$version'")

if [[ ${#problems[@]} -gt 0 ]]; then
    if [[ "${ALLOW_UNPINNED_WALKER:-}" == "1" ]]; then
        for p in "${problems[@]}"; do
            printf 'check-pinned-walker: WARNING: %s\n' "$p" >&2
        done
        printf 'check-pinned-walker: shipping an UNPINNED nfs-walker (ALLOW_UNPINNED_WALKER=1)\n' >&2
        printf '%s\n' \
            'Bundled nfs-walker (MIT), run by vamoose prepare.' \
            "Source: $source_url" \
            "SHA256: $actual_sha" \
            "Version: ${actual_version:-unknown}" \
            "UNPINNED: does not match packaging/nfs-walker.lock.json ($git_ref @ $git_sha)"
        exit 0
    fi
    for p in "${problems[@]}"; do
        printf 'check-pinned-walker: %s\n' "$p" >&2
    done
    fail "$walker is not the pinned build ($git_ref @ $git_sha); rebuild it from that commit or update packaging/nfs-walker.lock.json (ALLOW_UNPINNED_WALKER=1 overrides for local experiments)"
fi

printf '%s\n' \
    'Bundled nfs-walker (MIT), run by vamoose prepare.' \
    "Source: $source_url" \
    "Branch: $git_ref" \
    "Commit: $git_sha" \
    "Version: $version" \
    "SHA256: $actual_sha"
