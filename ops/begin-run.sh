#!/usr/bin/env bash
# Start a timed run, with automatic external-resource cleanup on failure.
set -euo pipefail

ops_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck disable=SC1091
source "$ops_dir/lib/run.sh"
ops_validate_run_spec
[[ $# -eq 0 ]] || { echo "usage: begin-run.sh" >&2; exit 2; }
ops_assert_run_identity

state_dir=$(ops_state_dir)
mkdir -p "$state_dir"
paused_marker="$state_dir/external-resources.paused"
paused_here=0
resume_on_failure() {
    status=$?
    if (( status != 0 && paused_here == 1 )); then
        VAMOOSE_RUN_ENV="$RUN_ENV" "$RESUME_HOOK" || true
        rm -f -- "$paused_marker"
    fi
    exit "$status"
}
trap resume_on_failure EXIT

if [[ -n ${PAUSE_HOOK:-} && ! -e "$paused_marker" ]]; then
    VAMOOSE_RUN_ENV="$RUN_ENV" "$PAUSE_HOOK"
    printf '%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$paused_marker"
    paused_here=1
fi

"$ops_dir/timing.sh" start migration
"$ops_dir/start-workers.sh"
trap - EXIT
printf 'run started: %s\n' "$RUN_ID"
