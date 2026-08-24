#!/usr/bin/env bash
# Render local and per-instance TOML from one validated run specification.
set -euo pipefail

ops_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck disable=SC1091
source "$ops_dir/lib/run.sh"
ops_validate_run_spec

renderer_hash=$(sha256sum "$RUN_ENV" "$ops_dir/render-worker-configs.sh" \
    "$ops_dir/lib/run.sh" | sha256sum | cut -c1-12)
generated_root=${VAMOOSE_GENERATED_ROOT:-$ops_dir/generated}
rendered_dir="$generated_root/${RUN_ID}-${renderer_hash}"
if [[ -d "$rendered_dir" ]]; then
    printf '%s\n' "$rendered_dir"
    exit 0
fi

mkdir -p "$generated_root"
render_tmp=$(mktemp -d "$generated_root/.${RUN_ID}.render.XXXXXX")
cleanup() { rm -rf -- "$render_tmp"; }
trap cleanup EXIT

render_run_block() {
    cat <<EOF
[run]
bucket     = "$VAMOOSE_BUCKET"
endpoint   = "$VAMOOSE_ENDPOINT"
region     = "$AWS_REGION"
profile    = "$AWS_PROFILE"
verify_tls = $VERIFY_TLS
EOF
}

render_run_block > "$render_tmp/local.toml"
: > "$render_tmp/workers.tsv"
for record in "${WORKER_INSTANCES[@]}"; do
    ops_parse_worker "$record"
    config_name="$OPS_WORKER_INSTANCE.toml"
    {
        render_run_block
        cat <<EOF

[worker]
host_id           = "$OPS_WORKER_HOST_ID"
heartbeat_sec     = $HEARTBEAT_SEC
lease_timeout_sec = $LEASE_TIMEOUT_SEC

[shard]
local_scratch = "$OPS_WORKER_SCRATCH"
max_in_flight = $MAX_SHARDS_IN_FLIGHT

[mover]
strategy_default     = "libnfs_io_uring"
src_url              = "$SRC_NFS_URL"
dst_url              = "$DST_NFS_URL"
nfs_connections      = $OPS_WORKER_CONNECTIONS
rpc_timeout_ms       = $RPC_TIMEOUT_MS
pipeline_depth       = 8
io_uring_queue_depth = 256
fixed_buffer_count   = 256
fixed_buffer_size    = "1 MiB"
use_raw_fh           = $USE_RAW_FH
direct_commit        = $DIRECT_COMMIT
use_bucketed_pool    = $USE_BUCKETED_POOL

[batch]
bytes_budget       = "$BYTES_BUDGET"
files_budget       = $FILES_BUDGET
inflight_small     = $OPS_WORKER_INFLIGHT_SMALL
inflight_medium    = $INFLIGHT_MEDIUM
inflight_large     = $INFLIGHT_LARGE
large_stripe_size  = "$LARGE_STRIPE_SIZE"
large_stripe_depth = $LARGE_STRIPE_DEPTH

[copy]
preserve_owner           = $PRESERVE_OWNER
preserve_mode            = $PRESERVE_MODE
preserve_times           = $PRESERVE_TIMES
preserve_xattr           = $PRESERVE_XATTR
server_side_copy         = "off"
require_chown_capability = $REQUIRE_CHOWN_CAPABILITY
require_unchanged_size   = $REQUIRE_UNCHANGED_SIZE

[backpressure]
failure_pct_window_sec = $FAILURE_PCT_WINDOW_SEC
failure_pct_threshold  = $FAILURE_PCT_THRESHOLD
throughput_floor_mb_s  = $THROUGHPUT_FLOOR_MB_S
EOF
        if [[ -n ${COORD_URL:-} ]]; then
            cat <<EOF

[coord]
url    = "$COORD_URL"
job_id = "$JOB_ID"
EOF
        fi
    } > "$render_tmp/$config_name"
    printf '%s\t%s\t%s\t%s\t%s\n' "$OPS_WORKER_HOST" \
        "$OPS_WORKER_INSTANCE" "$OPS_WORKER_HOST_ID" "$config_name" \
        "$OPS_WORKER_SCRATCH" >> "$render_tmp/workers.tsv"
done

# The package template points at /usr/bin. Release deployments use this
# rendered override so ExecStart follows the atomically switched release.
cat > "$render_tmp/vamoose-worker@.service" <<EOF
[Unit]
Description=vamoose migration worker (%i)
Documentation=https://github.com/blakegolliher/vamoose
After=network-online.target
Wants=network-online.target
StartLimitIntervalSec=300
StartLimitBurst=3

[Service]
Type=simple
User=root
ExecStartPre=/usr/bin/test -x $INSTALL_PREFIX/current/bin/vamoose
ExecStartPre=/usr/bin/test -r $REMOTE_CONFIG_DIR/%i.toml
ExecStart=$INSTALL_PREFIX/current/bin/vamoose worker --config $REMOTE_CONFIG_DIR/%i.toml
EnvironmentFile=-/etc/vamoose/vamoose.env
EnvironmentFile=-$REMOTE_CONFIG_DIR/%i.env
Restart=on-failure
RestartPreventExitStatus=3
RestartSec=10
SuccessExitStatus=0
KillSignal=SIGTERM
TimeoutStopSec=300
LimitNOFILE=65536
UMask=0027
StandardOutput=journal
StandardError=journal
SyslogIdentifier=vamoose-worker-%i

[Install]
WantedBy=multi-user.target
EOF

{
    printf 'run_id=%s\n' "$RUN_ID"
    printf 'run_env_sha256=%s\n' "$(sha256sum "$RUN_ENV" | awk '{print $1}')"
    printf 'renderer_sha256=%s\n' "$(sha256sum "$ops_dir/render-worker-configs.sh" | awk '{print $1}')"
    printf 'systemd_unit_sha256=%s\n' "$(sha256sum "$render_tmp/vamoose-worker@.service" | awk '{print $1}')"
    printf 'worker_count=%s\n' "${#WORKER_INSTANCES[@]}"
} > "$render_tmp/render-info"

mv -- "$render_tmp" "$rendered_dir"
trap - EXIT
printf 'rendered %d worker configs for %s\n' "${#WORKER_INSTANCES[@]}" "$RUN_ID" >&2
printf '%s\n' "$rendered_dir"
