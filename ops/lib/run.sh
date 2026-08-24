#!/usr/bin/env bash
# Shared run-spec loading and validation for the tracked operations harness.

ops_fail() {
    printf 'ops: %s\n' "$*" >&2
    return 1
}

ops_load_run_env() {
    if [[ ${OPS_RUN_ENV_LOADED:-0} == 1 ]]; then
        return 0
    fi
    OPS_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
    RUN_ENV=${VAMOOSE_RUN_ENV:-$OPS_DIR/run.env}
    [[ -r "$RUN_ENV" ]] || ops_fail \
        "run specification not found: $RUN_ENV (copy ops/run.env.example to ops/run.env)" || return
    # The specification is intentionally Bash so arrays remain readable.
    # shellcheck source=/dev/null
    source "$RUN_ENV"
    OPS_RUN_ENV_LOADED=1
    export OPS_DIR RUN_ENV OPS_RUN_ENV_LOADED
}

ops_require_scalar() {
    local name=$1
    [[ -n ${!name+x} && -n ${!name} ]] || ops_fail "required setting is empty: $name"
}

ops_validate_toml_string() {
    local name=$1 value=${!1-}
    [[ "$value" != *$'\n'* && "$value" != *$'\r'* && "$value" != *'"'* && "$value" != *\\* ]] \
        || ops_fail "$name contains a character that cannot be rendered safely"
}

ops_validate_bool() {
    local name=$1 value=${!1-}
    [[ "$value" == true || "$value" == false ]] || ops_fail "$name must be true or false"
}

ops_validate_uint() {
    local name=$1 min=$2 max=$3 value=${!1-}
    [[ "$value" =~ ^[0-9]+$ ]] || ops_fail "$name must be an integer"
    (( value >= min && value <= max )) || ops_fail "$name must be between $min and $max"
}

ops_parse_worker() {
    local record=$1 extra
    IFS='|' read -r OPS_WORKER_HOST OPS_WORKER_INSTANCE OPS_WORKER_HOST_ID \
        OPS_WORKER_CONNECTIONS OPS_WORKER_INFLIGHT_SMALL OPS_WORKER_SCRATCH extra <<<"$record"
    [[ -z ${extra:-} ]] || ops_fail "worker record has too many fields: $record" || return
    for value in OPS_WORKER_HOST OPS_WORKER_INSTANCE OPS_WORKER_HOST_ID \
        OPS_WORKER_CONNECTIONS OPS_WORKER_INFLIGHT_SMALL OPS_WORKER_SCRATCH; do
        [[ -n ${!value} ]] || ops_fail "worker record has an empty field: $record" || return
    done
}

ops_aws_s3() {
    local options=(s3 --endpoint-url "$VAMOOSE_ENDPOINT" --region "$AWS_REGION" --profile "$AWS_PROFILE")
    [[ "$VERIFY_TLS" == true ]] || options+=(--no-verify-ssl)
    aws "${options[@]}" "$@"
}

ops_aws_s3api() {
    local options=(s3api --endpoint-url "$VAMOOSE_ENDPOINT" --region "$AWS_REGION" --profile "$AWS_PROFILE")
    [[ "$VERIFY_TLS" == true ]] || options+=(--no-verify-ssl)
    aws "${options[@]}" "$@"
}

ops_spec_sha256() {
    sha256sum "$RUN_ENV" | awk '{print $1}'
}

ops_state_dir() {
    printf '%s/%s\n' "${VAMOOSE_STATE_ROOT:-$OPS_DIR/state}" "$RUN_ID"
}

ops_prepare_dir() {
    printf '%s/%s\n' "$PREPARE_ROOT" "$RUN_ID"
}

ops_control_binary() {
    local binary_name=$1 repo_root state_dir control_prefix
    [[ "$binary_name" =~ ^[A-Za-z0-9._-]+$ ]] \
        || ops_fail "invalid control binary name: $binary_name" || return
    repo_root=$(git -C "$OPS_DIR" rev-parse --show-toplevel)
    state_dir=$(ops_state_dir)
    control_prefix="$state_dir/control-release"
    mkdir -p "$state_dir"
    if [[ ! -x "$control_prefix/current/bin/vamoose" ]]; then
        "$repo_root/scripts/install-release.sh" "$RELEASE_BUNDLE" \
            --prefix "$control_prefix" >&2
    fi
    [[ -x "$control_prefix/current/bin/$binary_name" ]] \
        || ops_fail "release does not contain executable: $binary_name" || return
    printf '%s\n' "$control_prefix/current/bin/$binary_name"
}

ops_control_vamoose() {
    ops_control_binary vamoose
}

ops_source_scan_url() {
    if [[ "$SRC_ROOT" == / ]]; then
        printf '%s\n' "${SRC_NFS_URL%/}"
    else
        printf '%s%s\n' "${SRC_NFS_URL%/}" "$SRC_ROOT"
    fi
}

ops_assert_harness_provenance() {
    local repo_root release_name build_info expected_git actual_git dirty
    for command_name in git tar sha256sum python3; do
        command -v "$command_name" >/dev/null \
            || ops_fail "required provenance command not found: $command_name" || return
    done
    [[ -f "$RELEASE_BUNDLE" && -f "$RELEASE_BUNDLE.sha256" ]] \
        || ops_fail "release bundle and adjacent digest are required" || return
    (
        cd -- "$(dirname -- "$RELEASE_BUNDLE")"
        sha256sum --check --strict "$(basename -- "$RELEASE_BUNDLE.sha256")" >/dev/null
    ) || ops_fail "release archive digest verification failed" || return
    release_name=$(tar -tzf "$RELEASE_BUNDLE" | sed -n '1s|/.*||p')
    [[ "$release_name" =~ ^vamoose-[A-Za-z0-9._-]+$ ]] \
        || ops_fail "unexpected release archive layout" || return
    build_info=$(tar -xOf "$RELEASE_BUNDLE" "$release_name/build-info.json") \
        || ops_fail "cannot read release build-info.json" || return
    read -r expected_git dirty < <(python3 -c '
import json, sys
value = json.load(sys.stdin)
print(value["git"]["sha"], str(value["git"]["dirty"]).lower())
' <<<"$build_info")
    [[ "$dirty" == false ]] \
        || ops_fail "state-changing operations require a clean release bundle" || return
    repo_root=$(git -C "$OPS_DIR" rev-parse --show-toplevel)
    actual_git=$(git -C "$repo_root" rev-parse HEAD)
    [[ "$actual_git" == "$expected_git" ]] || ops_fail \
        "operations checkout $actual_git does not match release source $expected_git" || return
    [[ -z $(git -C "$repo_root" status --porcelain=v1 --untracked-files=all) ]] \
        || ops_fail "operations checkout is dirty; commit it and build a matching release" || return
}

ops_assert_run_identity() {
    local marker expected_sha
    ops_assert_harness_provenance || return
    expected_sha=$(ops_spec_sha256)
    marker=$(ops_aws_s3 cp "s3://$VAMOOSE_BUCKET/.vamoose-run.json" - 2>/dev/null) \
        || ops_fail "run identity marker is missing from s3://$VAMOOSE_BUCKET" || return
    python3 -c '
import json, sys
run_id, spec_sha = sys.argv[1:]
try:
    value = json.load(sys.stdin)
except json.JSONDecodeError as error:
    raise SystemExit(f"invalid run identity marker: {error}")
if value.get("run_id") != run_id:
    raise SystemExit("bucket belongs to run {!r}, not {!r}".format(value.get("run_id"), run_id))
if value.get("run_env_sha256") != spec_sha:
    raise SystemExit("run.env changed after this run was initialized; choose a new RUN_ID")
' "$RUN_ID" "$expected_sha" <<<"$marker" || ops_fail \
        "bucket run identity does not match the loaded specification"
}

ops_validate_run_spec() {
    ops_load_run_env || return
    local required=(
        RUN_ID RELEASE_BUNDLE PREPARE_ROOT NFS_WALKER_BIN NFS_WALKER_SHA256
        VAMOOSE_ENDPOINT VAMOOSE_BUCKET AWS_REGION
        AWS_PROFILE SRC_NFS_URL DST_NFS_URL SRC_ROOT DST_ROOT SRC_MOUNT
        DST_MOUNT RESET_HOST VERIFY_HOST REMOTE_USER
        INSTALL_PREFIX REMOTE_CONFIG_DIR REMOTE_STAGING_DIR HEARTBEAT_SEC
        LEASE_TIMEOUT_SEC MAX_SHARDS_IN_FLIGHT RPC_TIMEOUT_MS BYTES_BUDGET
        FILES_BUDGET INFLIGHT_MEDIUM INFLIGHT_LARGE LARGE_STRIPE_SIZE
        LARGE_STRIPE_DEPTH FAILURE_PCT_WINDOW_SEC FAILURE_PCT_THRESHOLD
        THROUGHPUT_FLOOR_MB_S WALKER_WORKERS WALKER_QUEUE_SIZE
        WALKER_BATCH_SIZE WALKER_WRITER_SHARDS WALKER_PIPELINE_DEPTH
        WALKER_PARQUET_COMPRESSION WALKER_ROW_GROUP_SIZE WALKER_FILE_SIZE_MB
        VERIFY_SAMPLE_COUNT VERIFY_SAMPLE_SEED VERIFY_MAX_CONTENT_BYTES
    )
    local name
    for name in "${required[@]}"; do
        ops_require_scalar "$name" || return
    done
    [[ "$RUN_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{2,127}$ ]] \
        || ops_fail "RUN_ID must be 3-128 portable characters" || return
    [[ "$VAMOOSE_BUCKET" =~ ^[A-Za-z0-9][A-Za-z0-9.-]{1,61}[A-Za-z0-9]$ ]] \
        || ops_fail "VAMOOSE_BUCKET is not a portable bucket name" || return
    [[ "$VAMOOSE_ENDPOINT" =~ ^https?://[^/]+/?$ ]] \
        || ops_fail "VAMOOSE_ENDPOINT must be an http(s) origin" || return
    [[ "$SRC_NFS_URL" =~ ^nfs://[^/]+/.+ && "$DST_NFS_URL" =~ ^nfs://[^/]+/.+ ]] \
        || ops_fail "SRC_NFS_URL and DST_NFS_URL must be complete nfs:// URLs" || return
    [[ "$SRC_ROOT" == /* && "$DST_ROOT" == /* ]] \
        || ops_fail "SRC_ROOT and DST_ROOT must be absolute export-relative paths" || return
    for name in SRC_ROOT DST_ROOT SRC_MOUNT DST_MOUNT; do
        [[ "${!name}" =~ ^/([A-Za-z0-9._-]+/)*[A-Za-z0-9._-]*$ ]] \
            || ops_fail "$name must be a normalized portable absolute path" || return
    done
    [[ "$DST_ROOT" != / ]] \
        || ops_fail "DST_ROOT cannot be an export root; guarded reset would be unsafe" || return
    [[ "$SRC_MOUNT" != / && "$DST_MOUNT" != / ]] \
        || ops_fail "kernel mount paths cannot be /" || return
    if [[ "$SRC_NFS_URL" == "$DST_NFS_URL" ]]; then
        src_prefix="${SRC_ROOT%/}/"
        dst_prefix="${DST_ROOT%/}/"
        [[ "$src_prefix" != "$dst_prefix" && "$src_prefix" != "$dst_prefix"* \
            && "$dst_prefix" != "$src_prefix"* ]] \
            || ops_fail "source and destination roots overlap on the same export" || return
    fi
    [[ "$INSTALL_PREFIX" == /* && "$INSTALL_PREFIX" != / ]] \
        || ops_fail "INSTALL_PREFIX must be an absolute non-root path" || return
    [[ "$PREPARE_ROOT" == /* && "$PREPARE_ROOT" != / ]] \
        || ops_fail "PREPARE_ROOT must be an absolute non-root path" || return
    [[ "$NFS_WALKER_BIN" == /* ]] \
        || ops_fail "NFS_WALKER_BIN must be an absolute path" || return
    [[ "$NFS_WALKER_SHA256" =~ ^[0-9a-fA-F]{64}$ ]] \
        || ops_fail "NFS_WALKER_SHA256 must be exactly 64 hexadecimal characters" || return
    [[ "$REMOTE_CONFIG_DIR" == /* && "$REMOTE_STAGING_DIR" == /* ]] \
        || ops_fail "remote directories must be absolute" || return
    for name in INSTALL_PREFIX PREPARE_ROOT NFS_WALKER_BIN REMOTE_CONFIG_DIR REMOTE_STAGING_DIR; do
        [[ "${!name}" =~ ^/[A-Za-z0-9._/-]+$ ]] \
            || ops_fail "$name contains non-portable remote-path characters" || return
    done
    [[ "$REMOTE_CONFIG_DIR" == /etc/* ]] \
        || ops_fail "REMOTE_CONFIG_DIR must be below /etc for systemd management" || return
    [[ "$REMOTE_USER" =~ ^[A-Za-z_][A-Za-z0-9_-]*$ ]] \
        || ops_fail "REMOTE_USER is not a portable account name" || return

    for name in VERIFY_TLS NFS_WALKER_SUDO USE_RAW_FH DIRECT_COMMIT USE_BUCKETED_POOL \
        PRESERVE_OWNER PRESERVE_MODE PRESERVE_TIMES PRESERVE_XATTR \
        REQUIRE_CHOWN_CAPABILITY REQUIRE_UNCHANGED_SIZE; do
        ops_validate_bool "$name" || return
    done
    ops_validate_uint HEARTBEAT_SEC 1 3600 || return
    ops_validate_uint LEASE_TIMEOUT_SEC 2 86400 || return
    (( LEASE_TIMEOUT_SEC > HEARTBEAT_SEC )) \
        || ops_fail "LEASE_TIMEOUT_SEC must exceed HEARTBEAT_SEC" || return
    ops_validate_uint MAX_SHARDS_IN_FLIGHT 1 64 || return
    ops_validate_uint RPC_TIMEOUT_MS 0 3600000 || return
    ops_validate_uint FILES_BUDGET 1 10000000 || return
    ops_validate_uint INFLIGHT_MEDIUM 1 100000 || return
    ops_validate_uint INFLIGHT_LARGE 1 100000 || return
    ops_validate_uint LARGE_STRIPE_DEPTH 1 100000 || return
    ops_validate_uint WALKER_WORKERS 1 100000 || return
    ops_validate_uint WALKER_QUEUE_SIZE 1 100000000 || return
    ops_validate_uint WALKER_BATCH_SIZE 1 10000000 || return
    ops_validate_uint WALKER_WRITER_SHARDS 1 32 || return
    ops_validate_uint WALKER_PIPELINE_DEPTH 0 100000 || return
    ops_validate_uint WALKER_ROW_GROUP_SIZE 1 100000000 || return
    ops_validate_uint WALKER_FILE_SIZE_MB 1 1048576 || return
    ops_validate_uint VERIFY_SAMPLE_COUNT 1 10000000 || return
    ops_validate_uint VERIFY_SAMPLE_SEED 0 4294967295 || return
    ops_validate_uint VERIFY_MAX_CONTENT_BYTES 0 1099511627776 || return
    case "$WALKER_PARQUET_COMPRESSION" in
        zstd1|zstd3|zstd6|snappy|lz4-raw|none) ;;
        *) ops_fail "invalid WALKER_PARQUET_COMPRESSION" || return ;;
    esac
    ops_validate_uint FAILURE_PCT_WINDOW_SEC 1 86400 || return
    [[ "$FAILURE_PCT_THRESHOLD" =~ ^[0-9]+([.][0-9]+)?$ ]] \
        || ops_fail "FAILURE_PCT_THRESHOLD must be numeric" || return
    [[ "$THROUGHPUT_FLOOR_MB_S" =~ ^[0-9]+([.][0-9]+)?$ ]] \
        || ops_fail "THROUGHPUT_FLOOR_MB_S must be numeric" || return

    [[ ${WORKER_INSTANCES+x} && ${#WORKER_INSTANCES[@]} -gt 0 ]] \
        || ops_fail "WORKER_INSTANCES must contain at least one record" || return
    declare -A seen_instances=() seen_host_ids=()
    declare -A seen_hosts=()
    local record
    for record in "${WORKER_INSTANCES[@]}"; do
        ops_parse_worker "$record" || return
        [[ "$OPS_WORKER_HOST" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] \
            || ops_fail "invalid worker SSH host: $OPS_WORKER_HOST" || return
        [[ "$OPS_WORKER_INSTANCE" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] \
            || ops_fail "invalid worker instance: $OPS_WORKER_INSTANCE" || return
        [[ "$OPS_WORKER_HOST_ID" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] \
            || ops_fail "invalid worker host_id: $OPS_WORKER_HOST_ID" || return
        [[ -z ${seen_instances[$OPS_WORKER_INSTANCE]+x} ]] \
            || ops_fail "duplicate worker instance: $OPS_WORKER_INSTANCE" || return
        [[ -z ${seen_host_ids[$OPS_WORKER_HOST_ID]+x} ]] \
            || ops_fail "duplicate worker host_id: $OPS_WORKER_HOST_ID" || return
        seen_instances[$OPS_WORKER_INSTANCE]=1
        seen_host_ids[$OPS_WORKER_HOST_ID]=1
        seen_hosts[$OPS_WORKER_HOST]=1
        [[ "$OPS_WORKER_CONNECTIONS" =~ ^[0-9]+$ ]] \
            && (( OPS_WORKER_CONNECTIONS >= 1 && OPS_WORKER_CONNECTIONS <= 10000 )) \
            || ops_fail "invalid nfs_connections in $OPS_WORKER_INSTANCE" || return
        [[ "$OPS_WORKER_INFLIGHT_SMALL" =~ ^[0-9]+$ ]] \
            && (( OPS_WORKER_INFLIGHT_SMALL >= 1 && OPS_WORKER_INFLIGHT_SMALL <= 100000 )) \
            || ops_fail "invalid inflight_small in $OPS_WORKER_INSTANCE" || return
        [[ "$OPS_WORKER_SCRATCH" == /* && "$OPS_WORKER_SCRATCH" != / ]] \
            || ops_fail "scratch must be an absolute non-root path in $OPS_WORKER_INSTANCE" || return
        [[ "$OPS_WORKER_SCRATCH" =~ ^/[A-Za-z0-9._/-]+$ ]] \
            || ops_fail "scratch contains non-portable path characters in $OPS_WORKER_INSTANCE" || return
    done
    [[ -n ${seen_hosts[$RESET_HOST]+x} ]] \
        || ops_fail "RESET_HOST is not present in WORKER_INSTANCES" || return
    [[ -n ${seen_hosts[$VERIFY_HOST]+x} ]] \
        || ops_fail "VERIFY_HOST is not present in WORKER_INSTANCES" || return

    local toml_names=(
        VAMOOSE_ENDPOINT VAMOOSE_BUCKET AWS_REGION AWS_PROFILE SRC_NFS_URL
        DST_NFS_URL SRC_ROOT DST_ROOT BYTES_BUDGET LARGE_STRIPE_SIZE
    )
    for name in "${toml_names[@]}"; do
        ops_validate_toml_string "$name" || return
    done
    if [[ -n ${COORD_URL:-} || -n ${JOB_ID:-} ]]; then
        [[ -n ${COORD_URL:-} && -n ${JOB_ID:-} ]] \
            || ops_fail "COORD_URL and JOB_ID must either both be set or both be empty" || return
        [[ "$COORD_URL" =~ ^https?://[^/]+(:[0-9]+)?/?$ ]] \
            || ops_fail "COORD_URL must be an http(s) origin" || return
        ops_validate_toml_string COORD_URL || return
        ops_validate_toml_string JOB_ID || return
    fi
    if [[ -n ${PAUSE_HOOK:-} || -n ${RESUME_HOOK:-} ]]; then
        [[ -n ${PAUSE_HOOK:-} && -n ${RESUME_HOOK:-} ]] \
            || ops_fail "PAUSE_HOOK and RESUME_HOOK must both be set or both be empty" || return
        [[ -x "$PAUSE_HOOK" && -x "$RESUME_HOOK" ]] \
            || ops_fail "pause/resume hooks must be executable files" || return
    fi
}
