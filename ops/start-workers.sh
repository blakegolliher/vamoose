#!/usr/bin/env bash
# Start every configured systemd instance and require a fresh S3 heartbeat.
set -euo pipefail

ops_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck disable=SC1091
source "$ops_dir/lib/run.sh"
ops_validate_run_spec
[[ $# -eq 0 ]] || { echo "usage: start-workers.sh" >&2; exit 2; }
for command_name in ssh aws python3; do
    command -v "$command_name" >/dev/null || ops_fail "required command not found: $command_name"
done

rendered_dir=$("$ops_dir/render-worker-configs.sh")
ops_assert_run_identity
ops_aws_s3api head-object --bucket "$VAMOOSE_BUCKET" --key manifest.json >/dev/null \
    || ops_fail "manifest.json is not readable in s3://$VAMOOSE_BUCKET"

started_epoch=$(date -u +%s)
while IFS=$'\t' read -r host instance _host_id _config_name _scratch; do
    printf '[%s] starting vamoose-worker@%s\n' "$host" "$instance"
    ssh -n -o BatchMode=yes "$REMOTE_USER@$host" \
        "sudo systemctl start 'vamoose-worker@$instance.service'"
done < "$rendered_dir/workers.tsv"

# Type=simple reaches active as soon as exec succeeds. Keep the process under
# observation, then require a progress object newer than this launch so stale
# state from a reused host_id cannot masquerade as readiness.
readiness_timeout=${READINESS_TIMEOUT_SEC:-$(( HEARTBEAT_SEC * 3 ))}
(( readiness_timeout >= 30 )) || readiness_timeout=30
deadline=$(( $(date -u +%s) + readiness_timeout ))
declare -A ready=()
while (( $(date -u +%s) <= deadline )); do
    all_ready=1
    while IFS=$'\t' read -r host instance host_id _config_name _scratch; do
        [[ ${ready[$instance]:-0} == 1 ]] && continue
        all_ready=0
        if ! ssh -n -o BatchMode=yes "$REMOTE_USER@$host" \
            "sudo systemctl is-active --quiet 'vamoose-worker@$instance.service'"; then
            continue
        fi
        progress=$(ops_aws_s3 cp \
            "s3://$VAMOOSE_BUCKET/progress/host-$host_id.json" - 2>/dev/null || true)
        if python3 -c '
import datetime, json, sys
host_id, minimum = sys.argv[1], int(sys.argv[2])
try:
    value = json.load(sys.stdin)
    stamp = value["heartbeat_utc"].replace("Z", "+00:00")
    epoch = int(datetime.datetime.fromisoformat(stamp).timestamp())
except (KeyError, TypeError, ValueError, json.JSONDecodeError):
    raise SystemExit(1)
raise SystemExit(0 if value.get("host") == host_id and epoch >= minimum - 2 else 1)
' "$host_id" "$started_epoch" <<<"$progress"; then
            ready[$instance]=1
            printf '[%s] ready: %s published a fresh heartbeat\n' "$host" "$instance"
        fi
    done < "$rendered_dir/workers.tsv"
    (( all_ready )) && break
    sleep 2
done

failed=0
while IFS=$'\t' read -r host instance _host_id _config_name _scratch; do
    if [[ ${ready[$instance]:-0} != 1 ]]; then
        failed=1
        printf '[%s] NOT READY: %s\n' "$host" "$instance" >&2
        ssh -n -o BatchMode=yes "$REMOTE_USER@$host" \
            "sudo systemctl --no-pager --full status 'vamoose-worker@$instance.service'; \
             sudo journalctl --no-pager -n 40 -u 'vamoose-worker@$instance.service'" >&2 || true
    fi
done < "$rendered_dir/workers.tsv"
(( failed == 0 )) || ops_fail "one or more workers failed readiness"
printf 'all %d workers ready\n' "${#WORKER_INSTANCES[@]}"
