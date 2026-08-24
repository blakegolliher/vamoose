#!/usr/bin/env bash
# Generate a deterministic canonical sample and verify it on one fleet host.
set -euo pipefail

ops_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck disable=SC1091
source "$ops_dir/lib/run.sh"
ops_validate_run_spec
[[ $# -eq 0 ]] || { echo "usage: verify-sample.sh" >&2; exit 2; }
for command_name in python3 ssh scp; do
    command -v "$command_name" >/dev/null \
        || ops_fail "required command not found: $command_name"
done
ops_assert_run_identity

prepare_dir=$(ops_prepare_dir)
rewrite_report="$prepare_dir/checkpoints/rewrite.json"
upload_report="$prepare_dir/checkpoints/upload.json"
[[ -f "$rewrite_report" && -f "$upload_report" ]] \
    || ops_fail "completed rewrite/upload checkpoints are required"
python3 - "$upload_report" <<'PY'
import json, pathlib, sys
value = json.loads(pathlib.Path(sys.argv[1]).read_text())
if value.get("complete") is not True:
    raise SystemExit("upload checkpoint is not complete")
PY

state_dir=$(ops_state_dir)
evidence_dir="$state_dir/evidence"
mkdir -p "$evidence_dir"
sample_file="$evidence_dir/verification-sample.json"
result_file="$evidence_dir/verification-result.json"
python3 "$ops_dir/verify_sample.py" sample \
    --rewrite-report "$rewrite_report" \
    --output "$sample_file" \
    --run-id "$RUN_ID" \
    --count "$VERIFY_SAMPLE_COUNT" \
    --seed "$VERIFY_SAMPLE_SEED"

remote_verify_dir="$REMOTE_STAGING_DIR/verification-$RUN_ID"
ssh -o BatchMode=yes "$REMOTE_USER@$VERIFY_HOST" \
    "mkdir -p '$remote_verify_dir'"
scp -q "$ops_dir/verify_sample.py" "$sample_file" \
    "$REMOTE_USER@$VERIFY_HOST:$remote_verify_dir/"

source_base="${SRC_MOUNT%/}${SRC_ROOT}"
dest_base="${DST_MOUNT%/}${DST_ROOT}"
verification_status=0
if result=$(ssh -o BatchMode=yes "$REMOTE_USER@$VERIFY_HOST" \
    "sudo python3 '$remote_verify_dir/verify_sample.py' check \
        --sample '$remote_verify_dir/verification-sample.json' \
        --run-id '$RUN_ID' \
        --source-base '$source_base' \
        --dest-base '$dest_base' \
        --max-content-bytes '$VERIFY_MAX_CONTENT_BYTES'"); then
    verification_status=0
else
    verification_status=$?
fi
printf '%s\n' "$result" > "$result_file"
python3 -m json.tool "$result_file" >/dev/null \
    || ops_fail "verification host did not return valid result JSON"
cat "$result_file"
(( verification_status == 0 )) \
    || ops_fail "sampled migration verification found mismatches"
