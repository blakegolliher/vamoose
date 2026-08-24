#!/usr/bin/env bash
# Reset only this run's destination contents and mutable coordination keys.
set -euo pipefail

ops_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck disable=SC1091
source "$ops_dir/lib/run.sh"
ops_validate_run_spec

confirm=
while [[ $# -gt 0 ]]; do
    case "$1" in
        --confirm-run-id) confirm=${2-}; shift 2 ;;
        -h|--help)
            echo "usage: reset-run.sh --confirm-run-id RUN_ID" >&2
            exit 2 ;;
        *) ops_fail "unknown argument: $1" ;;
    esac
done
[[ "$confirm" == "$RUN_ID" ]] || ops_fail \
    "refusing reset: --confirm-run-id must exactly equal $RUN_ID"
ops_assert_run_identity

printf 'stopping configured workers before reset\n'
"$ops_dir/stop-workers.sh"

destination_path="${DST_MOUNT%/}${DST_ROOT}"
[[ "$destination_path" != / && "$destination_path" != "$DST_MOUNT" ]] \
    || ops_fail "computed destination reset path is unsafe: $destination_path"
printf '[%s] clearing destination contents under %s\n' "$RESET_HOST" "$destination_path"
ssh -n -o BatchMode=yes "$REMOTE_USER@$RESET_HOST" "
set -euo pipefail
mount_real=\$(sudo realpath -e '$DST_MOUNT')
target_real=\$(sudo realpath -e '$destination_path')
test \"\$target_real\" != / && test \"\$target_real\" != \"\$mount_real\"
case \"\$target_real\" in \"\$mount_real\"/*) ;; *) echo 'target escaped destination mount' >&2; exit 1;; esac
findmnt -n -M \"\$mount_real\" >/dev/null
sudo find \"\$target_real\" -xdev -mindepth 1 -delete
test -z \"\$(sudo find \"\$target_real\" -xdev -mindepth 1 -print -quit)\"
"

printf 'clearing mutable coordination prefixes for %s\n' "$RUN_ID"
for prefix in shards/ progress/ batches/ failures/ downgrades/; do
    ops_aws_s3 rm "s3://$VAMOOSE_BUCKET/$prefix" --recursive --only-show-errors
    remaining=$(ops_aws_s3api list-objects-v2 --bucket "$VAMOOSE_BUCKET" \
        --prefix "$prefix" --max-keys 1 --query KeyCount --output text)
    [[ "$remaining" == 0 ]] || ops_fail "objects remain under $prefix after reset"
done

control_vamoose=$(ops_control_vamoose)
rendered_dir=$("$ops_dir/render-worker-configs.sh")
"$control_vamoose" init --config "$rendered_dir/local.toml" --force
printf '%s\treset\t%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$RUN_ID" \
    >> "$(ops_state_dir)/lifecycle.tsv"
printf 'run reset complete: %s\n' "$RUN_ID"
