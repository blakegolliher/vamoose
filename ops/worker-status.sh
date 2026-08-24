#!/usr/bin/env bash
# Instance-aware service status and journald tails.
set -euo pipefail

ops_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck disable=SC1091
source "$ops_dir/lib/run.sh"
ops_validate_run_spec
[[ $# -eq 0 ]] || { echo "usage: worker-status.sh" >&2; exit 2; }

rendered_dir=$("$ops_dir/render-worker-configs.sh")
failed=0
while IFS=$'\t' read -r host instance _host_id _config_name _scratch; do
    printf '== %s / %s ==\n' "$host" "$instance"
    if ! ssh -n -o BatchMode=yes "$REMOTE_USER@$host" \
        "sudo systemctl show 'vamoose-worker@$instance.service' \
           --property=ActiveState,SubState,MainPID,Result,ExecMainStatus --no-pager; \
         sudo journalctl --no-pager -n 5 -u 'vamoose-worker@$instance.service'"; then
        failed=1
    fi
done < "$rendered_dir/workers.tsv"
exit "$failed"
