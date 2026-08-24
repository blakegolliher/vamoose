#!/usr/bin/env bash
# Verify a terminal run, capture evidence, sample data, and resume resources.
set -euo pipefail

ops_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck disable=SC1091
source "$ops_dir/lib/run.sh"
ops_validate_run_spec
[[ $# -eq 0 ]] || { echo "usage: finalize-run.sh" >&2; exit 2; }
for command_name in aws python3 ssh scp sha256sum; do
    command -v "$command_name" >/dev/null \
        || ops_fail "required command not found: $command_name"
done
ops_assert_run_identity

state_dir=$(ops_state_dir)
evidence_dir="$state_dir/evidence"
paused_marker="$state_dir/external-resources.paused"
mkdir -p "$evidence_dir"
terminal_reached=0
verification_timing_open=0

resume_resources() {
    if [[ -e "$paused_marker" ]]; then
        [[ -n ${RESUME_HOOK:-} ]] \
            || { printf 'finalizer: paused marker exists without RESUME_HOOK\n' >&2; return 1; }
        VAMOOSE_RUN_ENV="$RUN_ENV" "$RESUME_HOOK"
        rm -f -- "$paused_marker"
        printf 'external resources resumed\n'
    fi
}

finalizer_exit() {
    exit_status=$?
    trap - EXIT
    if (( verification_timing_open == 1 )); then
        "$ops_dir/timing.sh" end verification >/dev/null 2>&1 || true
    fi
    # A non-terminal invocation leaves intentionally paused resources alone;
    # after terminal state is proven, every later failure must resume them.
    if (( terminal_reached == 1 )); then
        if ! resume_resources; then
            printf 'finalizer: failed to resume external resources\n' >&2
            exit_status=1
        fi
    fi
    exit "$exit_status"
}
trap finalizer_exit EXIT

control_vamoose=$(ops_control_vamoose)
rendered_dir=$("$ops_dir/render-worker-configs.sh")
status_json="$evidence_dir/status-final.json"
"$control_vamoose" status --config "$rendered_dir/local.toml" --json > "$status_json"

python3 - "$status_json" <<'PY'
import json, pathlib, sys
status = json.loads(pathlib.Path(sys.argv[1]).read_text())
if status.get("terminal") is not True:
    raise SystemExit(
        "run is not terminal: completed={} failed={} active={} unclaimed={}".format(
            status.get("completed"), status.get("failed"),
            status.get("in_progress"), status.get("unclaimed")
        )
    )
PY
terminal_reached=1

# Workers should already have exited after observing terminal claims. Stop the
# exact configured units so no idle process remains during final verification.
"$ops_dir/stop-workers.sh"

timing_ledger="$state_dir/timings.tsv"
[[ -f "$timing_ledger" ]] || ops_fail "migration timing ledger is missing"
if awk -F'\t' '$1=="migration" && $3=="-"{found=1} END{exit !found}' "$timing_ledger"; then
    "$ops_dir/timing.sh" end migration
elif ! awk -F'\t' '$1=="migration" && $3!="-"{found=1} END{exit !found}' "$timing_ledger"; then
    ops_fail "migration timing ledger has neither an open nor completed migration stage"
fi

python3 - "$status_json" <<'PY'
import json, pathlib, sys
status = json.loads(pathlib.Path(sys.argv[1]).read_text())
problems = []
if status.get("failed") != 0:
    problems.append(f"{status.get('failed')} failed shard claims")
if status.get("completed") != status.get("total_shards"):
    problems.append("not every manifest shard completed successfully")
if status.get("files_failed") != 0:
    problems.append(f"progress records contain {status.get('files_failed')} file failures")
if problems:
    raise SystemExit("terminal run is unhealthy: " + "; ".join(problems))
PY

failure_object_count=$(ops_aws_s3api list-objects-v2 \
    --bucket "$VAMOOSE_BUCKET" --prefix failures/ --max-keys 1 \
    --query KeyCount --output text)
[[ "$failure_object_count" == 0 ]] \
    || ops_fail "failure records exist under s3://$VAMOOSE_BUCKET/failures/"

"$ops_dir/timing.sh" start verification
verification_timing_open=1
"$ops_dir/verify-sample.sh"
"$ops_dir/timing.sh" end verification
verification_timing_open=0

verification_json="$evidence_dir/verification-result.json"
python3 - "$verification_json" <<'PY'
import json, pathlib, sys
value = json.loads(pathlib.Path(sys.argv[1]).read_text())
if value.get("passed") is not True:
    raise SystemExit("sample verification did not pass")
PY

prepare_dir=$(ops_prepare_dir)
for checkpoint_name in scan rewrite upload; do
    checkpoint="$prepare_dir/checkpoints/$checkpoint_name.json"
    [[ -f "$checkpoint" ]] || ops_fail "missing preparation checkpoint: $checkpoint"
    install -m 0644 "$checkpoint" "$evidence_dir/$checkpoint_name.json"
done
install -m 0644 "$timing_ledger" "$evidence_dir/timings.tsv"
build_info=$(cd -- "$(dirname -- "$control_vamoose")/.." && pwd)/build-info.json
[[ -f "$build_info" ]] || ops_fail "control release build-info.json is missing"
install -m 0644 "$build_info" "$evidence_dir/build-info.json"

final_summary="$evidence_dir/final-summary.json"
python3 - "$final_summary" "$RUN_ID" "$(ops_spec_sha256)" \
    "$status_json" "$verification_json" "$timing_ledger" \
    "$evidence_dir/build-info.json" \
    "$evidence_dir/scan.json" "$evidence_dir/rewrite.json" "$evidence_dir/upload.json" <<'PY'
import datetime, hashlib, json, pathlib, sys
(
    output, run_id, spec_sha, status_path, verification_path, timing_path,
    build_info_path, scan_path, rewrite_path, upload_path,
) = sys.argv[1:]

def load(path):
    return json.loads(pathlib.Path(path).read_text())

def digest(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()

timings = []
for line in pathlib.Path(timing_path).read_text().splitlines():
    stage, started, ended, duration = line.split("\t")
    timings.append({
        "stage": stage,
        "started_utc": started,
        "ended_utc": None if ended == "-" else ended,
        "duration_seconds": None if duration == "-" else int(duration),
    })
value = {
    "schema_version": 1,
    "run_id": run_id,
    "finalized_utc": datetime.datetime.now(datetime.UTC).isoformat().replace("+00:00", "Z"),
    "run_env_sha256": spec_sha,
    "status": load(status_path),
    "verification": load(verification_path),
    "timings": timings,
    "release": load(build_info_path),
    "checkpoint_sha256": {
        "scan": digest(scan_path),
        "rewrite": digest(rewrite_path),
        "upload": digest(upload_path),
    },
}
path = pathlib.Path(output)
partial = path.with_name(path.name + ".partial")
partial.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
partial.replace(path)
PY

resume_resources
printf '%s\tfinalized\t%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$RUN_ID" \
    >> "$state_dir/lifecycle.tsv"
trap - EXIT
printf 'run finalized successfully: %s\n' "$RUN_ID"
printf 'evidence: %s\n' "$final_summary"
