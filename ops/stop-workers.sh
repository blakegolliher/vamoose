#!/usr/bin/env bash
# Stop only the configured worker instances; never process-name kill.
set -euo pipefail

ops_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck disable=SC1091
source "$ops_dir/lib/run.sh"
ops_validate_run_spec

hard=0
if [[ ${1:-} == --hard ]]; then
    hard=1
    shift
fi
[[ $# -eq 0 ]] || { echo "usage: stop-workers.sh [--hard]" >&2; exit 2; }

ops_assert_run_identity
rendered_dir=$("$ops_dir/render-worker-configs.sh")
while IFS=$'\t' read -r host instance _host_id _config_name _scratch; do
    if (( hard )); then
        printf '[%s] SIGKILL vamoose-worker@%s\n' "$host" "$instance"
        ssh -n -o BatchMode=yes "$REMOTE_USER@$host" \
            "sudo systemctl kill --kill-who=all --signal=SIGKILL 'vamoose-worker@$instance.service'; \
             sudo systemctl reset-failed 'vamoose-worker@$instance.service' || true"
    else
        printf '[%s] stopping vamoose-worker@%s\n' "$host" "$instance"
        ssh -n -o BatchMode=yes "$REMOTE_USER@$host" \
            "sudo systemctl stop 'vamoose-worker@$instance.service'"
    fi
done < "$rendered_dir/workers.tsv"
