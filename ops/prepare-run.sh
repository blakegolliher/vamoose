#!/usr/bin/env bash
# Resumable scan -> canonical rewrite -> verified upload/manifest pipeline.
set -euo pipefail

ops_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck disable=SC1091
source "$ops_dir/lib/run.sh"
ops_validate_run_spec
[[ $# -eq 0 ]] || { echo "usage: prepare-run.sh" >&2; exit 2; }
for command_name in aws python3 sha256sum tee; do
    command -v "$command_name" >/dev/null \
        || ops_fail "required command not found: $command_name"
done
[[ -x "$NFS_WALKER_BIN" ]] \
    || ops_fail "nfs-walker executable not found: $NFS_WALKER_BIN"
actual_walker_sha=$(sha256sum "$NFS_WALKER_BIN" | awk '{print $1}')
[[ "$actual_walker_sha" == "$NFS_WALKER_SHA256" ]] \
    || ops_fail "nfs-walker digest mismatch: expected $NFS_WALKER_SHA256, got $actual_walker_sha"

ops_assert_run_identity
prepare_dir=$(ops_prepare_dir)
checkpoint_dir="$prepare_dir/checkpoints"
scan_report="$checkpoint_dir/scan.json"
rewrite_report="$checkpoint_dir/rewrite.json"
upload_report="$checkpoint_dir/upload.json"
manifest_path="$prepare_dir/manifest.json"
mkdir -p "$checkpoint_dir" "$prepare_dir/scans"
spec_sha=$(ops_spec_sha256)
walker_version=$($NFS_WALKER_BIN --version)
scan_url=$(ops_source_scan_url)

if [[ -f "$scan_report" ]]; then
    scan_dir=$(python3 "$ops_dir/prepare.py" verify-scan \
        --report "$scan_report" \
        --run-id "$RUN_ID" \
        --run-env-sha256 "$spec_sha" \
        --walker-sha256 "$NFS_WALKER_SHA256")
    printf 'scan checkpoint valid: %s\n' "$scan_dir"
else
    attempt_number=1
    while :; do
        printf -v attempt_name 'attempt-%04d' "$attempt_number"
        attempt_dir="$prepare_dir/scans/$attempt_name"
        [[ -e "$attempt_dir" ]] || break
        attempt_number=$(( attempt_number + 1 ))
    done
    mkdir -p "$attempt_dir"
    walk_root="$attempt_dir/walk.parquet"
    printf 'starting immutable scan attempt: %s\n' "$attempt_name"
    printf 'source scan URL: %s\n' "$scan_url"

    walker_command=(
        "$NFS_WALKER_BIN" "$scan_url"
        --output "$walk_root"
        --workers "$WALKER_WORKERS"
        --queue-size "$WALKER_QUEUE_SIZE"
        --batch-size "$WALKER_BATCH_SIZE"
        --writer-shards "$WALKER_WRITER_SHARDS"
        --pipeline-depth "$WALKER_PIPELINE_DEPTH"
        --parquet-compression "$WALKER_PARQUET_COMPRESSION"
        --parquet-row-group-size "$WALKER_ROW_GROUP_SIZE"
        --parquet-file-size-mb "$WALKER_FILE_SIZE_MB"
        --log "$attempt_dir/walker-progress.jsonl"
        --log-fmt json
    )
    for exclude in "${WALKER_EXCLUDES[@]-}"; do
        [[ -n "$exclude" ]] && walker_command+=(--exclude "$exclude")
    done
    if [[ "$NFS_WALKER_SUDO" == true ]]; then
        sudo "${walker_command[@]}" 2>&1 | tee "$attempt_dir/walker-console.log"
        sudo chown -R "$(id -u):$(id -g)" "$attempt_dir"
    else
        "${walker_command[@]}" 2>&1 | tee "$attempt_dir/walker-console.log"
    fi

    scan_dir=$(python3 "$ops_dir/prepare.py" scan-report \
        --report "$scan_report" \
        --walk-root "$walk_root" \
        --run-id "$RUN_ID" \
        --run-env-sha256 "$spec_sha" \
        --source-url "$SRC_NFS_URL" \
        --source-root "$SRC_ROOT" \
        --scan-url "$scan_url" \
        --walker "$NFS_WALKER_BIN" \
        --walker-sha256 "$NFS_WALKER_SHA256" \
        --walker-version "$walker_version")
    printf 'scan checkpoint committed: %s\n' "$scan_report"
fi

rewrite_bin=$(ops_control_binary mig-walker-rewrite)
canonical_dir="$prepare_dir/canonical"
"$rewrite_bin" \
    --input "$scan_dir" \
    --output "$canonical_dir" \
    --source-root "$SRC_ROOT" \
    --walker-version "$walker_version" \
    --resume \
    --report "$rewrite_report"

python3 "$ops_dir/prepare.py" upload \
    --rewrite-report "$rewrite_report" \
    --report "$upload_report" \
    --manifest "$manifest_path" \
    --run-id "$RUN_ID" \
    --run-env-sha256 "$spec_sha" \
    --endpoint "$VAMOOSE_ENDPOINT" \
    --bucket "$VAMOOSE_BUCKET" \
    --region "$AWS_REGION" \
    --profile "$AWS_PROFILE" \
    --verify-tls "$VERIFY_TLS" \
    --source-url "$SRC_NFS_URL" \
    --source-root "$SRC_ROOT" \
    --dest-url "$DST_NFS_URL" \
    --dest-root "$DST_ROOT" \
    --preserve-owner "$PRESERVE_OWNER" \
    --preserve-mode "$PRESERVE_MODE" \
    --preserve-times "$PRESERVE_TIMES" \
    --preserve-xattr "$PRESERVE_XATTR"

printf '%s\tprepared\t%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$RUN_ID" \
    >> "$(ops_state_dir)/lifecycle.tsv"
printf 'run preparation complete: %s\n' "$RUN_ID"
