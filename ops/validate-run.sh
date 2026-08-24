#!/usr/bin/env bash
set -euo pipefail

ops_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck disable=SC1091
source "$ops_dir/lib/run.sh"

require_artifacts=0
if [[ ${1:-} == --require-artifacts ]]; then
    require_artifacts=1
    shift
fi
[[ $# -eq 0 ]] || { echo "usage: validate-run.sh [--require-artifacts]" >&2; exit 2; }

ops_validate_run_spec
if (( require_artifacts )); then
    [[ -f "$RELEASE_BUNDLE" ]] || ops_fail "release bundle not found: $RELEASE_BUNDLE"
    [[ -f "$RELEASE_BUNDLE.sha256" ]] || ops_fail \
        "release digest not found: $RELEASE_BUNDLE.sha256"
    [[ -x "$NFS_WALKER_BIN" ]] || ops_fail \
        "nfs-walker executable not found: $NFS_WALKER_BIN"
    actual_walker_sha=$(sha256sum "$NFS_WALKER_BIN" | awk '{print $1}')
    [[ "$actual_walker_sha" == "$NFS_WALKER_SHA256" ]] || ops_fail \
        "nfs-walker digest mismatch: expected $NFS_WALKER_SHA256, got $actual_walker_sha"
fi

printf 'valid run: %s (%d worker instances)\n' "$RUN_ID" "${#WORKER_INSTANCES[@]}"
