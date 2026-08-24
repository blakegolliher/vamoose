#!/usr/bin/env bash
# Bind a fresh coordination bucket to this immutable run specification.
set -euo pipefail

ops_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck disable=SC1091
source "$ops_dir/lib/run.sh"
ops_validate_run_spec
[[ $# -eq 0 ]] || { echo "usage: init-run.sh" >&2; exit 2; }
for command_name in aws python3 sha256sum; do
    command -v "$command_name" >/dev/null || ops_fail "required command not found: $command_name"
done
[[ -f "$RELEASE_BUNDLE" && -f "$RELEASE_BUNDLE.sha256" ]] \
    || ops_fail "release bundle and adjacent digest are required"
ops_assert_harness_provenance

if ! ops_aws_s3api head-bucket --bucket "$VAMOOSE_BUCKET" >/dev/null 2>&1; then
    printf 'creating fresh run bucket: %s\n' "$VAMOOSE_BUCKET"
    ops_aws_s3 mb "s3://$VAMOOSE_BUCKET"
fi

state_dir=$(ops_state_dir)
mkdir -p "$state_dir"
marker_file="$state_dir/run-marker.json"
spec_sha=$(ops_spec_sha256)
archive_sha=$(awk '{print $1}' "$RELEASE_BUNDLE.sha256")
python3 - "$marker_file" "$RUN_ID" "$spec_sha" "$archive_sha" \
    "$SRC_NFS_URL" "$SRC_ROOT" "$DST_NFS_URL" "$DST_ROOT" <<'PY'
import datetime, json, pathlib, sys
path, run_id, spec_sha, archive_sha, src_url, src_root, dst_url, dst_root = sys.argv[1:]
value = {
    "schema_version": 1,
    "run_id": run_id,
    "initialized_utc": datetime.datetime.now(datetime.UTC).isoformat().replace("+00:00", "Z"),
    "run_env_sha256": spec_sha,
    "release_archive_sha256": archive_sha,
    "source": {"url": src_url, "root": src_root},
    "destination": {"url": dst_url, "root": dst_root},
}
pathlib.Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY

if ops_aws_s3api head-object --bucket "$VAMOOSE_BUCKET" \
    --key .vamoose-run.json >/dev/null 2>&1; then
    ops_assert_run_identity
    printf 'run identity already initialized and matches: %s\n' "$RUN_ID"
else
    object_count=$(ops_aws_s3api list-objects-v2 --bucket "$VAMOOSE_BUCKET" \
        --max-keys 1 --query KeyCount --output text)
    [[ "$object_count" == 0 ]] || ops_fail \
        "bucket is not empty and has no run identity marker; use a fresh bucket"
    ops_aws_s3api put-object --bucket "$VAMOOSE_BUCKET" --key .vamoose-run.json \
        --body "$marker_file" --if-none-match '*' >/dev/null
    ops_assert_run_identity
    printf 'bound bucket %s to run %s\n' "$VAMOOSE_BUCKET" "$RUN_ID"
fi

control_vamoose=$(ops_control_vamoose)
rendered_dir=$("$ops_dir/render-worker-configs.sh")
"$control_vamoose" init --config "$rendered_dir/local.toml"
