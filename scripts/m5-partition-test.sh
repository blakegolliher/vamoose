#!/usr/bin/env bash
# M5 partition test — R8 stress harness.
#
# Companion to scripts/m5-self-fence-test.sh. Same protocol, same seven
# A-G assertions, plus one new assertion (H) that proves R8's
# commit-point gate actually executed.
#
# Mechanism (Phase 3, what's different from the SIGSTOP harness):
#   1. Launch worker A normally; let it commit N rows.
#   2. Block A's S3 traffic with iptables REJECT --tcp-reset on outbound
#      port 443 to the resolved VAST endpoint IP. NFS (port 2049) stays
#      open, so A keeps copying files.
#   3. A's heartbeat HEAD calls fail repeatedly. After R6's retry budget
#      (ceil(lease_timeout / heartbeat_sec)) consecutive failures, the
#      heartbeat task trips the fence WHILE A IS MID-SHARD WITH ROWS IN
#      FLIGHT. R8's check_fence() catches those in-flight rows at their
#      commit op (rename/link/symlink) and short-circuits with
#      FailurePhase::Fenced.
#   4. Remove the iptables block, wait for A to fully exit, launch B,
#      B reclaims and finishes the shard.
#
# Assertion H: files_fenced > 0 — proves R8 actually fired. The SIGSTOP
# harness PASSES with R8 in but never triggers it; this harness does.
#
# See docs/work-items/M5_PARTITION_TEST.md for the full design.
#
# Real-VAST run only. Do not run in CI.

set -euo pipefail

# -----------------------------------------------------------------------------
# Args + env
# -----------------------------------------------------------------------------

FILES=5000
FILE_SIZE=4096
PRE_PARTITION_COMMITS=30
TS="$(date -u +%Y%m%dT%H%M%SZ)"
BUCKET_PREFIX="m5p-${TS}"
KEEP_ARTIFACTS=0

usage() {
    cat <<EOF
usage: $0 [--files N] [--file-size BYTES] [--pre-partition-commits N]
          [--bucket-prefix S] [--keep-artifacts]

Required env (same as m5-self-fence-test.sh):
  AWS_PROFILE          VAST credentials profile.
  VAMOOSE_BUCKET       S3 bucket to use. Will be cleared of m5p artifacts.
  VAMOOSE_ENDPOINT     VAST S3 endpoint URL.
  VAMOOSE_SRC_NFS_URL  Source NFS URL (libnfs, e.g. nfs://host/export).
  VAMOOSE_DST_NFS_URL  Destination NFS URL. Must NOT overlap source.
  VAMOOSE_SRC_MOUNT    Kernel-mounted path to the source export.
  VAMOOSE_DST_MOUNT    Kernel-mounted path to the destination export.
  VAMOOSE_SRC_ROOT     Path under the source export root for the test tree.
  VAMOOSE_DST_ROOT     Path under the dest export root. Must differ from SRC.

Optional env:
  NFS_WALKER           Path to nfs-walker (post-RocksDB-removal walker
                       with single-step direct parquet output).
  VAMOOSE_BIN          Path to unified vamoose binary.
  MIG_WALKER_REWRITE   Path to mig-walker-rewrite (default: cargo run).
  AWS_S3_FLAGS         Extra args for aws s3 / aws s3api.

Host requirements:
  - Run as root (libnfs UID 0 requirement; same as the SIGSTOP harness).
  - Passwordless sudo for the in-script 'sudo -n' signal-delivery path.
  - sudo -n iptables -L OUTPUT must work. The partition window installs
    and removes an OUTPUT REJECT rule on TCP port 443 to the resolved
    VAST endpoint IP. The harness fails fast in Phase 0 if it can't.
  - During the partition window the entire host loses S3 connectivity
    to that endpoint (the iptables match is per-destination, not
    per-process). Don't run on a host doing other VAST work.

Invocation:
  sudo -E bash $0 [options]

  HOME and PATH are auto-recovered from \$SUDO_USER so AWS credentials
  and pipx-installed 'aws' stay discoverable.

This harness is a COMPANION to m5-self-fence-test.sh. It does NOT
replace it — the SIGSTOP test exercises clean-recovery from a paused
worker; this one exercises the R8 commit-point fence under live-but-
partitioned operation.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --files)                  FILES="$2"; shift 2;;
        --file-size)              FILE_SIZE="$2"; shift 2;;
        --pre-partition-commits)  PRE_PARTITION_COMMITS="$2"; shift 2;;
        --bucket-prefix)          BUCKET_PREFIX="$2"; shift 2;;
        --keep-artifacts)         KEEP_ARTIFACTS=1; shift;;
        -h|--help)                usage; exit 0;;
        *)                        echo "unknown arg: $1" >&2; usage; exit 2;;
    esac
done

# -----------------------------------------------------------------------------
# Bootstrap — tolerate sudo's HOME/PATH stripping.
#
# libnfs requires UID 0, but sudo's defaults clobber HOME (→ /root) and PATH
# (→ secure_path) even with -E, which breaks AWS credential lookup and tool
# discovery respectively. Rebuild both from $SUDO_USER so a bare
# `sudo -E bash <script>` just works.
# -----------------------------------------------------------------------------
if [[ $EUID -ne 0 ]]; then
    echo "FAIL: this harness must run as root (libnfs needs UID 0)." >&2
    echo "Re-run with: sudo -E bash $0 $*" >&2
    exit 2
fi

if [[ -n "${SUDO_USER:-}" ]]; then
    invoker_home="$(getent passwd "${SUDO_USER}" 2>/dev/null | cut -d: -f6)"
    if [[ -n "${invoker_home}" && -d "${invoker_home}" ]]; then
        if [[ "${HOME}" != "${invoker_home}" ]]; then
            export HOME="${invoker_home}"
        fi
        if ! command -v aws >/dev/null 2>&1 && [[ -x "${invoker_home}/.local/bin/aws" ]]; then
            export PATH="${invoker_home}/.local/bin:${PATH}"
        fi
    fi
fi

if ! command -v aws >/dev/null 2>&1; then
    echo "FAIL: aws CLI not found on PATH (PATH=${PATH})." >&2
    echo "Install it under root's PATH or expose it via SUDO_USER's ~/.local/bin." >&2
    exit 2
fi

: "${AWS_PROFILE:?AWS_PROFILE is required}"
: "${VAMOOSE_BUCKET:?VAMOOSE_BUCKET is required}"
: "${VAMOOSE_ENDPOINT:?VAMOOSE_ENDPOINT is required}"
: "${VAMOOSE_SRC_NFS_URL:?VAMOOSE_SRC_NFS_URL is required}"
: "${VAMOOSE_DST_NFS_URL:?VAMOOSE_DST_NFS_URL is required}"
: "${VAMOOSE_SRC_MOUNT:?VAMOOSE_SRC_MOUNT is required}"
: "${VAMOOSE_DST_MOUNT:?VAMOOSE_DST_MOUNT is required}"
: "${VAMOOSE_SRC_ROOT:?VAMOOSE_SRC_ROOT is required (path under the source export)}"
: "${VAMOOSE_DST_ROOT:?VAMOOSE_DST_ROOT is required (path under the dest export; must differ from VAMOOSE_SRC_ROOT)}"
if [[ "${VAMOOSE_SRC_ROOT}" == "${VAMOOSE_DST_ROOT}" ]]; then
    echo "VAMOOSE_SRC_ROOT and VAMOOSE_DST_ROOT must differ" >&2
    exit 2
fi
export DST_ROOT="${VAMOOSE_DST_ROOT}"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUN_DIR="${REPO_ROOT}/m5/run/${TS}"
mkdir -p "${RUN_DIR}"

A_OUT="${RUN_DIR}/A.out"; A_ERR="${RUN_DIR}/A.err"; A_PID_FILE="${RUN_DIR}/A.pid"
B_OUT="${RUN_DIR}/B.out"; B_ERR="${RUN_DIR}/B.err"; B_PID_FILE="${RUN_DIR}/B.pid"
ASSERT_LOG="${RUN_DIR}/assertions.log"
: > "${ASSERT_LOG}"

A_HOST="m5-host-A"
B_HOST="m5-host-B"

# Walker default detection — same logic as m5-self-fence-test.sh.
if [[ -z "${NFS_WALKER:-}" ]]; then
    if [[ -x "${HOME}/projects/nfs-walker/target/release/nfs-walker" ]]; then
        NFS_WALKER="${HOME}/projects/nfs-walker/target/release/nfs-walker"
    elif [[ -x "${HOME}/projects/nfs-walker/build/nfs-walker" ]]; then
        NFS_WALKER="${HOME}/projects/nfs-walker/build/nfs-walker"
    else
        NFS_WALKER="nfs-walker"
    fi
fi

VAMOOSE_BIN="${VAMOOSE_BIN:-${REPO_ROOT}/target/release/vamoose}"
MIG_WALKER_REWRITE_BIN="${MIG_WALKER_REWRITE:-}"
AWS_S3_FLAGS="${AWS_S3_FLAGS:-}"

aws_s3() {
    aws ${AWS_S3_FLAGS} --endpoint-url "${VAMOOSE_ENDPOINT}" s3 "$@"
}
aws_s3api() {
    aws ${AWS_S3_FLAGS} --endpoint-url "${VAMOOSE_ENDPOINT}" s3api "$@"
}
# Concatenate every object under an S3 prefix. The failure sink writes
# one immutable object per shard flush
# (failures/host-<id>/<shard-stem>-e<epoch>.jsonl since F04), so
# "is the sink empty?" means listing the per-host prefix and cat'ing
# whatever is there. Prints nothing when the prefix is absent/empty.
s3_cat_prefix() {
    local prefix="$1" key
    aws_s3api list-objects-v2 --bucket "${VAMOOSE_BUCKET}" \
        --prefix "${prefix}" --query 'Contents[].Key' --output text 2>/dev/null |
        tr '\t' '\n' | grep -ve '^None$' -e '^$' |
        while read -r key; do
            aws_s3 cp "s3://${VAMOOSE_BUCKET}/${key}" - 2>/dev/null || true
        done
}

log() { printf '[%s] %s\n' "$(date -u +%H:%M:%SZ)" "$*"; }
fail() { printf 'FAIL: %s\n' "$*" | tee -a "${ASSERT_LOG}" >&2; exit 1; }
assert_pass() { printf 'PASS: %s\n' "$*" | tee -a "${ASSERT_LOG}"; }
assert_fail() { printf 'FAIL: %s\n' "$*" | tee -a "${ASSERT_LOG}"; FAILED=1; }

# Liveness check; same as m5-self-fence-test.sh. Zombies count as gone.
proc_alive() {
    local pid="$1"
    [[ -d "/proc/${pid}" ]] || return 1
    local state
    state=$(awk '/^State:/{print $2}' "/proc/${pid}/status" 2>/dev/null)
    [[ "${state}" != "Z" && -n "${state}" ]]
}

FAILED=0

# Iptables partition state. Tracked so the cleanup trap can remove the
# rule even if the harness dies mid-partition.
PARTITION_INSTALLED=0
VAST_IP=""

remove_partition() {
    if [[ "${PARTITION_INSTALLED}" -eq 1 && -n "${VAST_IP}" ]]; then
        log "partition: removing iptables REJECT for ${VAST_IP}:443"
        sudo -n iptables -D OUTPUT -d "${VAST_IP}" -p tcp --dport 443 \
            -j REJECT --reject-with tcp-reset 2>/dev/null || true
        PARTITION_INSTALLED=0
    fi
}

# -----------------------------------------------------------------------------
# Cleanup trap. iptables removal goes FIRST so subsequent S3 cleanup
# can actually reach the endpoint.
# -----------------------------------------------------------------------------
cleanup() {
    local status=$?
    set +e
    remove_partition
    log "cleanup: ensuring A and B are not running"
    if [[ -s "${A_PID_FILE}" ]]; then
        local apid; apid="$(cat "${A_PID_FILE}")"
        sudo -n kill -CONT "${apid}" 2>/dev/null || true
        sudo -n kill -TERM "${apid}" 2>/dev/null || true
        sleep 1
        sudo -n kill -KILL "${apid}" 2>/dev/null || true
    fi
    if [[ -s "${B_PID_FILE}" ]]; then
        local bpid; bpid="$(cat "${B_PID_FILE}")"
        sudo -n kill -TERM "${bpid}" 2>/dev/null || true
        sleep 1
        sudo -n kill -KILL "${bpid}" 2>/dev/null || true
    fi
    wait 2>/dev/null || true
    sudo -n pkill -KILL -P 1 -f "vamoose worker --config ${RUN_DIR}/" 2>/dev/null || true
    if [[ "${KEEP_ARTIFACTS}" -eq 0 && -n "${BUCKET_PREFIX:-}" ]]; then
        for p in manifest.json index/ shards/ progress/ failures/ downgrades/ batches/; do
            aws_s3 rm "s3://${VAMOOSE_BUCKET}/${p}" --recursive >/dev/null 2>&1 || true
            aws_s3 rm "s3://${VAMOOSE_BUCKET}/${p}" >/dev/null 2>&1 || true
        done
    fi
    exit "${status}"
}
trap cleanup EXIT INT TERM

# -----------------------------------------------------------------------------
# Phase 0 — preflight
# -----------------------------------------------------------------------------
log "Phase 0: preflight"

if ! sudo -n true 2>/dev/null; then
    fail "sudo -n true failed; this harness requires passwordless sudo"
fi
if ! sudo -n iptables -L OUTPUT >/dev/null 2>&1; then
    fail "sudo -n iptables -L OUTPUT failed; this harness needs iptables under passwordless sudo"
fi

if [[ ! -x "${VAMOOSE_BIN}" ]]; then
    fail "vamoose binary not found or not executable: ${VAMOOSE_BIN}.
Run: cargo build --release --workspace"
fi

worker_mtime=$(stat -c %Y "${VAMOOSE_BIN}")
newest_src=$(find "${REPO_ROOT}/crates/migration-worker/src" \
                  "${REPO_ROOT}/crates/migration-mover/src" \
                  "${REPO_ROOT}/crates/migration-core/src" \
                  "${REPO_ROOT}/crates/vamoose-cli/src" \
                  -type f -name '*.rs' -printf '%T@\n' | sort -n | tail -1 | cut -d. -f1)
if [[ -n "${newest_src}" && "${newest_src}" -gt "${worker_mtime}" ]]; then
    fail "vamoose binary is older than crate sources. Rebuild:
  cargo build --release --workspace"
fi

if ! command -v "${NFS_WALKER}" >/dev/null 2>&1 && [[ ! -x "${NFS_WALKER}" ]]; then
    fail "nfs-walker not found at ${NFS_WALKER}"
fi
# Reject pre-RocksDB-removal walkers: those still ship export-parquet
# and expect a two-step rocks → parquet workflow that this harness no
# longer drives.
if "${NFS_WALKER}" help export-parquet >/dev/null 2>&1; then
    fail "${NFS_WALKER} still ships the export-parquet subcommand; rebuild from a current nfs-walker checkout (single-step parquet output required)"
fi

if ! aws_s3 ls "s3://${VAMOOSE_BUCKET}/" >/dev/null 2>&1; then
    fail "cannot list s3://${VAMOOSE_BUCKET}"
fi
if [[ ! -d "${VAMOOSE_SRC_MOUNT}" ]]; then
    fail "source mount not found: ${VAMOOSE_SRC_MOUNT}"
fi
if [[ ! -d "${VAMOOSE_DST_MOUNT}" ]]; then
    fail "dest mount not found: ${VAMOOSE_DST_MOUNT}"
fi
if [[ "${VAMOOSE_SRC_NFS_URL}" == "${VAMOOSE_DST_NFS_URL}" ]]; then
    fail "src and dst NFS URLs must differ"
fi

# Resolve VAST endpoint to IP for the iptables rule. Use getent so we
# pick up the same resolver chain everything else does. If DNS returns
# multiple IPs (round-robin), block them all.
VAST_HOST=$(echo "${VAMOOSE_ENDPOINT}" | sed -E 's|https?://([^/:]+).*|\1|')
VAST_IPS=$(getent hosts "${VAST_HOST}" | awk '{print $1}' | LC_ALL=C sort -u | tr '\n' ' ')
VAST_IPS="${VAST_IPS% }"
if [[ -z "${VAST_IPS}" ]]; then
    fail "could not resolve VAST endpoint host: ${VAST_HOST}"
fi
VAST_IP="${VAST_IPS%% *}"   # first IP, used for logging
log "VAST endpoint: ${VAST_HOST} → ${VAST_IPS}"

log "preflight: wiping any stale m5p artifacts in s3://${VAMOOSE_BUCKET}/"
for p in manifest.json index/ shards/ progress/ failures/ downgrades/ batches/; do
    aws_s3 rm "s3://${VAMOOSE_BUCKET}/${p}" --recursive >/dev/null 2>&1 || true
    aws_s3 rm "s3://${VAMOOSE_BUCKET}/${p}" >/dev/null 2>&1 || true
done
log "preflight: wiping stale dest tree at ${VAMOOSE_DST_MOUNT}${VAMOOSE_DST_ROOT}"
sudo rm -rf "${VAMOOSE_DST_MOUNT}${VAMOOSE_DST_ROOT}"

# -----------------------------------------------------------------------------
# Phase 1 — source tree + canonical parquet upload
# (identical to m5-self-fence-test.sh)
# -----------------------------------------------------------------------------
log "Phase 1: building source tree (${FILES} files × ${FILE_SIZE} bytes)"

SRC_TREE_HOST="${VAMOOSE_SRC_MOUNT}${VAMOOSE_SRC_ROOT}"
DST_TREE_HOST="${VAMOOSE_DST_MOUNT}${VAMOOSE_DST_ROOT}"

sudo rm -rf "${SRC_TREE_HOST}"
sudo mkdir -p "${SRC_TREE_HOST}"
sudo chown "$(id -u):$(id -g)" "${SRC_TREE_HOST}"
sudo rm -rf "${DST_TREE_HOST}"
sudo mkdir -p "${DST_TREE_HOST}"
sudo chown "$(id -u):$(id -g)" "${DST_TREE_HOST}"

python3 - "${SRC_TREE_HOST}" "${FILES}" "${FILE_SIZE}" <<'PY'
import os, sys
root, n, size = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
for i in range(n):
    sub = f"dir-{i % 10:02d}"
    os.makedirs(os.path.join(root, sub), exist_ok=True)
    rel = f"{sub}/file-{i:08d}.bin"
    full = os.path.join(root, rel)
    seed = rel.encode() + b"\n"
    out = (seed * (size // len(seed) + 1))[:size]
    with open(full, "wb") as f:
        f.write(out)
PY

ACTUAL_FILES=$(find "${SRC_TREE_HOST}" -type f | wc -l)
if [[ "${ACTUAL_FILES}" -ne "${FILES}" ]]; then
    fail "expected ${FILES} source files, got ${ACTUAL_FILES}"
fi
log "source tree built: ${ACTUAL_FILES} files at ${SRC_TREE_HOST}"

LEGACY_PARQUET_DIR="${RUN_DIR}/legacy.parquet"
CANON_OUT="${RUN_DIR}/canonical"
WALKER_LOG="${RUN_DIR}/walker.log"
: > "${WALKER_LOG}"

# Single-step direct parquet output. --writer-shards=1 pins the test to
# a single part-rNN-SSSSS.parquet so the downstream single-shard
# invariant holds without extra plumbing; --no-log suppresses the
# walker's sidecar progress logfile.
log "scan: ${NFS_WALKER} ${VAMOOSE_SRC_NFS_URL}${VAMOOSE_SRC_ROOT} → ${LEGACY_PARQUET_DIR}"
{
    echo "===== scan ====="
    if ! sudo "${NFS_WALKER}" "${VAMOOSE_SRC_NFS_URL}${VAMOOSE_SRC_ROOT}" \
            -o "${LEGACY_PARQUET_DIR}" \
            -w 16 -v \
            --writer-shards 1 \
            --no-log 2>&1; then
        echo "===== scan failed ====="
        cat "${WALKER_LOG}" >&2 || true
        fail "nfs-walker scan failed; see ${WALKER_LOG}"
    fi
} >> "${WALKER_LOG}" 2>&1

# Walker writes scans/<scan_id>/part-rNN-SSSSS.parquet + metadata.json
# under the output dir. The recursive globstar glob locates the part
# file; the shim's .parquet extension filter skips metadata.json.
shopt -s globstar nullglob
legacy_files=( "${LEGACY_PARQUET_DIR}"/**/*.parquet )
shopt -u globstar nullglob
if [[ "${#legacy_files[@]}" -ne 1 ]]; then
    fail "partition test requires single-shard manifest; got ${#legacy_files[@]} files (reduce --files)"
fi
WALK_PARQUET_DIR="$(dirname "${legacy_files[0]}")"
log "walker parquet at ${WALK_PARQUET_DIR} (1 file)"

log "running mig-walker-rewrite shim"
if [[ -n "${MIG_WALKER_REWRITE_BIN}" ]]; then
    "${MIG_WALKER_REWRITE_BIN}" \
        --input "${WALK_PARQUET_DIR}" \
        --output "${CANON_OUT}" \
        --source-root "/" 2>&1 | tee "${RUN_DIR}/shim.log" \
        || fail "mig-walker-rewrite failed; see ${RUN_DIR}/shim.log"
else
    ( cd "${REPO_ROOT}" && \
      cargo run --release -p mig-walker-rewrite --quiet -- \
        --input "${WALK_PARQUET_DIR}" \
        --output "${CANON_OUT}" \
        --source-root "/" ) 2>&1 | tee "${RUN_DIR}/shim.log" \
        || fail "mig-walker-rewrite failed; see ${RUN_DIR}/shim.log"
fi

shopt -s globstar nullglob
canon_shards=( "${CANON_OUT}"/**/*.parquet )
shopt -u globstar nullglob
if [[ "${#canon_shards[@]}" -ne 1 ]]; then
    fail "partition test requires single-shard manifest; got ${#canon_shards[@]}"
fi
SHARD_PARQUET="${canon_shards[0]}"
SHARD_NAME="part-0000.parquet"
log "canonical single shard: ${SHARD_PARQUET} → s3://${VAMOOSE_BUCKET}/index/${SHARD_NAME}"

ROW_COUNT=$( ( cd "${REPO_ROOT}" && \
    cargo run --release -p mig-walker-rewrite --example verify_shard --quiet -- \
        "${CANON_OUT}" 2>&1 ) \
    | awk '/^OK[[:space:]]+[0-9]+/{print $2; exit} /total_rows[= ]/{for(i=1;i<=NF;i++)if($i~/^[0-9]+$/){print $i; exit}}')
if ! [[ "${ROW_COUNT}" =~ ^[0-9]+$ ]] || [[ "${ROW_COUNT}" -eq 0 ]]; then
    ROW_COUNT=$(python3 - "${SHARD_PARQUET}" 2>/dev/null <<'PY' || true
import sys
try:
    import pyarrow.parquet as pq
    print(pq.ParquetFile(sys.argv[1]).metadata.num_rows)
except Exception:
    pass
PY
)
fi
if ! [[ "${ROW_COUNT}" =~ ^[0-9]+$ ]] || [[ "${ROW_COUNT}" -eq 0 ]]; then
    fail "could not determine parquet row count"
fi
SHARD_BYTES=$(stat -c %s "${SHARD_PARQUET}")
log "shard rows=${ROW_COUNT} bytes=${SHARD_BYTES}"

log "uploading shard parquet"
aws_s3 cp "${SHARD_PARQUET}" "s3://${VAMOOSE_BUCKET}/index/${SHARD_NAME}" >/dev/null
SHARD_ETAG=$(aws_s3api head-object --bucket "${VAMOOSE_BUCKET}" --key "index/${SHARD_NAME}" \
             --query 'ETag' --output text | tr -d '"')

MANIFEST_PATH="${RUN_DIR}/manifest.json"
python3 - "${MANIFEST_PATH}" "${BUCKET_PREFIX}" "${SHARD_NAME}" "${ROW_COUNT}" "${SHARD_BYTES}" \
        "${SHARD_ETAG}" "${VAMOOSE_SRC_NFS_URL}" "${VAMOOSE_DST_NFS_URL}" "${VAMOOSE_SRC_ROOT}" \
        "${VAMOOSE_DST_ROOT}" <<'PY'
import json, sys, datetime
out, run_id, shard_name, rows, sz, etag, src_url, dst_url, src_root, dst_root = sys.argv[1:]
manifest = {
    "format_version": 1,
    "run_id": run_id,
    "created_utc": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
    "shards": [{
        "key": f"index/{shard_name}",
        "rows": int(rows),
        "bytes": int(sz),
        "etag": etag,
    }],
    "total_rows": int(rows),
    "source": {"kind": "nfs", "url": src_url, "root": src_root},
    "dest":   {"kind": "nfs", "url": dst_url, "root": dst_root},
    "options": {
        "preserve_owner": True,
        "preserve_mode":  True,
        "preserve_times": True,
        "preserve_xattr": True,
        "server_side_copy": "off",
    },
}
with open(out, "w") as f:
    json.dump(manifest, f, indent=2)
PY

aws_s3 cp "${MANIFEST_PATH}" "s3://${VAMOOSE_BUCKET}/manifest.json" >/dev/null
log "manifest uploaded"

# -----------------------------------------------------------------------------
# Phase 2 — config generation (same TOML as m5-self-fence-test.sh)
# -----------------------------------------------------------------------------
log "Phase 2: generating worker configs"

write_worker_toml() {
    local out="$1" host_id="$2" scratch="$3"
    cat > "${out}" <<EOF
[run]
bucket     = "${VAMOOSE_BUCKET}"
endpoint   = "${VAMOOSE_ENDPOINT}"
region     = "us-east-1"
profile    = "${AWS_PROFILE}"
verify_tls = false

[worker]
host_id           = "${host_id}"
heartbeat_sec     = 1
lease_timeout_sec = 10

[shard]
local_scratch  = "${scratch}"
max_in_flight  = 1

[mover]
strategy_default     = "libnfs_io_uring"
src_url              = "${VAMOOSE_SRC_NFS_URL}"
dst_url              = "${VAMOOSE_DST_NFS_URL}"
nfs_connections      = 1
pipeline_depth       = 1
io_uring_queue_depth = 32
fixed_buffer_count   = 32
fixed_buffer_size    = "1 MiB"

[batch]
bytes_budget       = "8 GiB"
files_budget       = 100000
inflight_small     = 1
inflight_medium    = 1
inflight_large     = 1
large_stripe_size  = "4 MiB"
large_stripe_depth = 32

[copy]
preserve_owner            = true
preserve_mode             = true
preserve_times            = true
preserve_xattr            = true
server_side_copy          = "off"
require_chown_capability  = false
require_unchanged_size    = false

[backpressure]
failure_pct_window_sec = 60
failure_pct_threshold  = 100.0
throughput_floor_mb_s  = 0
EOF
}

A_TOML="${RUN_DIR}/workerA.toml"
B_TOML="${RUN_DIR}/workerB.toml"
A_SCRATCH="${RUN_DIR}/scratchA"
B_SCRATCH="${RUN_DIR}/scratchB"
mkdir -p "${A_SCRATCH}" "${B_SCRATCH}"

write_worker_toml "${A_TOML}" "${A_HOST}" "${A_SCRATCH}"
write_worker_toml "${B_TOML}" "${B_HOST}" "${B_SCRATCH}"

HEARTBEAT_SEC=1
LEASE_TIMEOUT_SEC=10
# Partition window: long enough for R6 retry budget to exhaust
# (ceil(lease/heartbeat) = 10 ticks) plus 30s buffer for A to exit.
PARTITION_WINDOW=$((LEASE_TIMEOUT_SEC + 60))

# -----------------------------------------------------------------------------
# Phase 3 — orchestrate (partition variant)
# -----------------------------------------------------------------------------
log "Phase 3: launching worker A"

WORKER_RUST_LOG="info,migration_worker=info,migration_mover=debug"

read_progress() {
    local host="$1"
    aws_s3 cp "s3://${VAMOOSE_BUCKET}/progress/host-${host}.json" - 2>/dev/null
}
read_claim() {
    aws_s3 cp "s3://${VAMOOSE_BUCKET}/shards/${SHARD_NAME}.claim" - 2>/dev/null
}

# Launch A — same shape as m5-self-fence-test.sh: sudo wrapper, PID
# capture via pgrep+cmdline disambiguation.
( cd "${REPO_ROOT}" && \
  HOME="${HOME}" \
  RUST_LOG="${WORKER_RUST_LOG}" \
  AWS_PROFILE="${AWS_PROFILE}" \
  exec setsid sudo -n -E "${VAMOOSE_BIN}" worker --config "${A_TOML}" --use-bucketed-pool >"${A_OUT}" 2>"${A_ERR}" ) &
A_LAUNCHER_PID=$!
A_PID=""
for _ in $(seq 1 50); do
    sleep 0.2
    for cand in $(pgrep -x vamoose 2>/dev/null || true); do
        if grep -qa -- "$(basename "${A_TOML}")" "/proc/${cand}/cmdline" 2>/dev/null; then
            A_PID="${cand}"
            break 2
        fi
    done
done
if [[ -z "${A_PID}" ]]; then
    fail "worker A failed to launch; launcher pid=${A_LAUNCHER_PID}; see ${A_ERR}"
fi
if [[ "${A_PID}" == "${A_LAUNCHER_PID}" ]]; then
    fail "worker A pid capture matched the launcher (sudo); pgrep logic broken"
fi
echo "${A_PID}" > "${A_PID_FILE}"
log "worker A started, pid=${A_PID} (launcher=${A_LAUNCHER_PID})"

# Wait for A to commit at least PRE_PARTITION_COMMITS rows. Same
# commit-log counting strategy as the SIGSTOP harness.
A_EPOCH_AT_PART=0
deadline=$(( $(date +%s) + 180 ))
caught=0
while [[ $(date +%s) -lt ${deadline} ]]; do
    if ! proc_alive "${A_PID}"; then
        fail "worker A exited prematurely; see ${A_ERR}"
    fi
    commits=$(awk '/commit: rename .partial/{n++} END{print n+0}' "${A_OUT}" 2>/dev/null)
    commits=${commits:-0}
    if [[ "${commits}" -ge "${PRE_PARTITION_COMMITS}" ]]; then
        caught=1
        log "A reached ${commits} commits; ready to partition"
        break
    fi
    sleep 0.5
done
if [[ "${caught}" -ne 1 ]]; then
    fail "could not catch A at ${PRE_PARTITION_COMMITS} commits within budget"
fi

# Read A's current claim epoch (will compare against B's reclaim epoch
# in Phase 3 tail).
claim_body=$(read_claim || true)
if [[ -n "${claim_body}" ]]; then
    A_EPOCH_AT_PART=$(echo "${claim_body}" | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("epoch",0))' 2>/dev/null || echo 0)
fi

# Install the partition. REJECT --tcp-reset (not DROP) so existing
# pooled S3 connections inside A's AWS SDK get RSTed immediately
# rather than waiting on TCP keepalive. The for-loop covers DNS RR.
PART_TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)
log "partition: installing iptables REJECT for ${VAST_IPS}:443 at ${PART_TS}"
for ip in ${VAST_IPS}; do
    sudo -n iptables -I OUTPUT -d "${ip}" -p tcp --dport 443 \
        -j REJECT --reject-with tcp-reset
done
PARTITION_INSTALLED=1

# Sleep through the partition window. During this time:
#   - A keeps copying via libnfs (port 2049 unaffected).
#   - A's heartbeat HEAD on port 443 fails immediately (RST).
#   - consec_failures climbs each tick (heartbeat_sec=1).
#   - After ~10 ticks (R6 budget = ceil(10/1) = 10), fence trips.
#   - In-flight rows hit R8's check_fence() → MoveError::Fenced.
#   - Shard processor's between-row check exits the shard.
#   - A exits cleanly.
log "partition: sleeping ${PARTITION_WINDOW}s (lease=${LEASE_TIMEOUT_SEC}s + 30s buffer)"
sleep "${PARTITION_WINDOW}"

# Lift the partition so B and the harness can talk to S3 again.
remove_partition

# Wait for A to fully exit. By now the fence should have tripped
# during the partition window; A should be exited or near-exited.
deadline=$(( $(date +%s) + 30 ))
a_exited=0
while [[ $(date +%s) -lt ${deadline} ]]; do
    if ! proc_alive "${A_PID}"; then
        a_exited=1
        break
    fi
    sleep 1
done
if [[ "${a_exited}" -ne 1 ]]; then
    if grep -q "fence tripped" "${A_OUT}" 2>/dev/null; then
        fail "A fence tripped but did not exit within 30s post-partition (shutdown-hang regression?)"
    else
        fail "A did not exit after partition lifted; fence may not have tripped (see ${A_OUT}/${A_ERR})"
    fi
fi
wait "${A_LAUNCHER_PID}" 2>/dev/null && A_EXIT=0 || A_EXIT=$?
log "A exit code: ${A_EXIT}"

# Snapshot A's progress object NOW, before B comes online and changes
# anything. files_fenced lives here; we want it for assertion H even
# if --keep-artifacts is off (cleanup trap wipes S3 progress).
A_PROGRESS_SNAPSHOT="${RUN_DIR}/host-A-progress.json"
if read_progress "${A_HOST}" > "${A_PROGRESS_SNAPSHOT}" 2>/dev/null; then
    log "captured A's progress: ${A_PROGRESS_SNAPSHOT}"
else
    log "WARN: could not snapshot A's progress object (may not have been written)"
    : > "${A_PROGRESS_SNAPSHOT}"
fi

# Launch B. Same shape as A.
log "launching worker B"
( cd "${REPO_ROOT}" && \
  HOME="${HOME}" \
  RUST_LOG="${WORKER_RUST_LOG}" \
  AWS_PROFILE="${AWS_PROFILE}" \
  exec setsid sudo -n -E "${VAMOOSE_BIN}" worker --config "${B_TOML}" --use-bucketed-pool >"${B_OUT}" 2>"${B_ERR}" ) &
B_LAUNCHER_PID=$!
B_PID=""
for _ in $(seq 1 50); do
    sleep 0.2
    for cand in $(pgrep -x vamoose 2>/dev/null || true); do
        if grep -qa -- "$(basename "${B_TOML}")" "/proc/${cand}/cmdline" 2>/dev/null; then
            B_PID="${cand}"
            break 2
        fi
    done
done
if [[ -z "${B_PID}" ]]; then
    fail "worker B failed to launch; launcher pid=${B_LAUNCHER_PID}; see ${B_ERR}"
fi
if [[ "${B_PID}" == "${B_LAUNCHER_PID}" ]]; then
    fail "worker B pid capture matched the launcher (sudo); pgrep logic broken"
fi
echo "${B_PID}" > "${B_PID_FILE}"
log "worker B started, pid=${B_PID} (launcher=${B_LAUNCHER_PID})"

# Wait for B to reclaim. A's claim has been stale for > lease_timeout
# (we slept lease + 30s with no heartbeat from A), so B should reclaim
# on its first scan.
deadline=$(( $(date +%s) + 2 * LEASE_TIMEOUT_SEC ))
reclaimed=0
while [[ $(date +%s) -lt ${deadline} ]]; do
    if ! proc_alive "${B_PID}"; then
        fail "worker B exited before reclaim; see ${B_ERR}"
    fi
    body=$(read_claim || true)
    if [[ -n "${body}" ]]; then
        chost=$(echo "${body}" | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("host",""))' 2>/dev/null || echo "")
        cepoch=$(echo "${body}" | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("epoch",0))' 2>/dev/null || echo 0)
        if [[ "${chost}" == "${B_HOST}" && "${cepoch}" -gt "${A_EPOCH_AT_PART}" ]]; then
            reclaimed=1
            log "B reclaimed: epoch ${A_EPOCH_AT_PART} → ${cepoch}"
            break
        fi
    fi
    sleep 2
done
if [[ "${reclaimed}" -ne 1 ]]; then
    fail "B did not reclaim within 2 × lease_timeout_sec"
fi

# Wait for B to complete the shard. Budget scales with FILES because
# at inflight=1 B serial-copies all rows from scratch (~22ms each on
# this hardware). 4*lease + 60 covered M5's 1000 files; partition test
# uses 5000+ so add FILES/10 = 100ms/file (5x safety margin vs the
# ~22ms observed rate).
B_COMPLETE_BUDGET=$(( 4 * LEASE_TIMEOUT_SEC + 60 + FILES / 10 ))
log "B-completion budget: ${B_COMPLETE_BUDGET}s for ${FILES} files"
deadline=$(( $(date +%s) + B_COMPLETE_BUDGET ))
b_done=0
while [[ $(date +%s) -lt ${deadline} ]]; do
    body=$(read_progress "${B_HOST}" || true)
    if [[ -n "${body}" ]]; then
        rd=$(echo "${body}" | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("shard_rows_done",0))' 2>/dev/null || echo 0)
        rt=$(echo "${body}" | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("shard_rows_total",0))' 2>/dev/null || echo 0)
        if [[ "${rt}" -gt 0 && "${rd}" -ge "${rt}" ]]; then
            b_done=1
            break
        fi
    fi
    if ! proc_alive "${B_PID}"; then
        b_done=1
        break
    fi
    sleep 2
done
if [[ "${b_done}" -ne 1 ]]; then
    fail "B did not complete shard within budget"
fi
log "B completed shard"

# Be defensive: wait for B to exit cleanly.
if proc_alive "${B_PID}"; then
    deadline=$(( $(date +%s) + 30 ))
    while proc_alive "${B_PID}" && [[ $(date +%s) -lt ${deadline} ]]; do
        sleep 1
    done
    if proc_alive "${B_PID}"; then
        sudo -n kill -TERM "${B_PID}" 2>/dev/null || true
        sleep 2
        sudo -n kill -KILL "${B_PID}" 2>/dev/null || true
    fi
fi
wait "${B_LAUNCHER_PID}" 2>/dev/null && B_EXIT=0 || B_EXIT=$?
log "B exit code: ${B_EXIT}"

# -----------------------------------------------------------------------------
# Phase 4 — assertions
# -----------------------------------------------------------------------------
log "Phase 4: assertions"

# A. Final claim record.
final_claim=$(read_claim || true)
if [[ -z "${final_claim}" ]]; then
    assert_fail "A: claim object not present at run end"
else
    final_state=$(echo "${final_claim}" | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("state",""))')
    final_host=$(echo "${final_claim}" | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("host",""))')
    if [[ "${final_state}" == "completed" && "${final_host}" == "${B_HOST}" ]]; then
        assert_pass "A: final claim state=completed host=${B_HOST}"
    else
        assert_fail "A: final claim state=${final_state} host=${final_host} (expected completed/${B_HOST})"
    fi
fi

# B. Dest file count == source file count, excluding .partial.
src_count=$(find "${SRC_TREE_HOST}" -type f -not -name '.*.partial' | wc -l)
dst_count=$(find "${DST_TREE_HOST}" -type f -not -name '.*.partial' | wc -l)
if [[ "${src_count}" -eq "${dst_count}" ]]; then
    assert_pass "B: file count src=${src_count} dst=${dst_count}"
else
    assert_fail "B: file count mismatch src=${src_count} dst=${dst_count}"
fi

# C. Per-file SHA-256 match.
SRC_SHA="${RUN_DIR}/src.sha"; DST_SHA="${RUN_DIR}/dst.sha"
( cd "${SRC_TREE_HOST}" && find . -type f -not -name '.*.partial' -print0 | LC_ALL=C sort -z \
    | xargs -0 sha256sum ) > "${SRC_SHA}"
( cd "${DST_TREE_HOST}" && find . -type f -not -name '.*.partial' -print0 | LC_ALL=C sort -z \
    | xargs -0 sha256sum ) > "${DST_SHA}"
if diff -q "${SRC_SHA}" "${DST_SHA}" >/dev/null; then
    assert_pass "C: SHA-256 match across $(wc -l < "${SRC_SHA}") files"
else
    diff -u "${SRC_SHA}" "${DST_SHA}" > "${RUN_DIR}/sha.diff" || true
    assert_fail "C: SHA-256 mismatch; see ${RUN_DIR}/sha.diff"
fi

# D. Worker A clean self-fence. Reason may mention 'claim refresh: HEAD
# shows different etag' (the usual path) OR 'heartbeat HEAD failing
# for' (the R6 retry-budget path, which is exactly what this test
# should exercise).
fence_line=$(grep -m1 "fence tripped" "${A_OUT}" || true)
if [[ -z "${fence_line}" ]]; then
    assert_fail "D: no 'fence tripped' line in A.out"
else
    if echo "${fence_line}" | grep -qE 'claim refresh|HEAD shows different etag|heartbeat HEAD failing for'; then
        if [[ "${A_EXIT}" -eq 0 ]]; then
            reason_short=$(echo "${fence_line}" | grep -oE '(claim refresh[^"]*|heartbeat HEAD failing for [0-9]+)' | head -1)
            assert_pass "D: A self-fenced cleanly (exit=0, reason: ${reason_short:-unknown})"
        else
            assert_fail "D: A fence tripped but exit code was ${A_EXIT}, not 0"
        fi
    else
        assert_fail "D: A fence reason did not mention 'claim refresh' or 'heartbeat HEAD failing for': ${fence_line}"
    fi
fi

# E. Failures sinks empty/absent (one object per shard flush under
# the per-host prefix since F04).
fail_a=$(s3_cat_prefix "failures/host-${A_HOST}/" || true)
fail_b=$(s3_cat_prefix "failures/host-${B_HOST}/" || true)
if [[ -z "${fail_a}" && -z "${fail_b}" ]]; then
    assert_pass "E: failures/host-A/ + failures/host-B/ absent or empty"
else
    {
        echo "--- failures/host-A/ ---"
        echo "${fail_a}"
        echo "--- failures/host-B/ ---"
        echo "${fail_b}"
    } > "${RUN_DIR}/failures.txt"
    assert_fail "E: per-file failures present; see ${RUN_DIR}/failures.txt"
fi

# F. No concurrent renames within 1.0s across A.out + B.out. Same
# python harness as the SIGSTOP test. Note: in the partition variant,
# B runs AFTER A has exited, so concurrent renames are inherently
# impossible — but we still run F as a regression guard.
F_THRESHOLD_S=1.0
if F_RESULT=$(A_OUT_PATH="${A_OUT}" B_OUT_PATH="${B_OUT}" \
              F_THRESHOLD_S="${F_THRESHOLD_S}" \
              python3 - <<'PY'
import os, re, sys, datetime, collections
threshold = float(os.environ["F_THRESHOLD_S"])
ansi_re = re.compile(r"\x1b\[[0-9;]*m")
ts_re = re.compile(r"^(\S+Z)")
dest_re = re.compile(r"dest=(\S+)")
paths = collections.defaultdict(list)
for path in [os.environ["A_OUT_PATH"], os.environ["B_OUT_PATH"]]:
    try:
        with open(path) as f:
            for line in f:
                line = ansi_re.sub("", line)
                if "commit: rename" not in line:
                    continue
                tm = ts_re.match(line)
                dm = dest_re.search(line)
                if not (tm and dm):
                    continue
                ts = datetime.datetime.fromisoformat(tm.group(1).replace("Z", "+00:00"))
                paths[dm.group(1)].append(ts)
    except FileNotFoundError:
        pass
total = sum(len(v) for v in paths.values())
if total == 0:
    print("F: no commit-rename events found"); sys.exit(1)
concurrent = []
for dest, tss in paths.items():
    if len(tss) > 1:
        tss.sort()
        if (tss[-1] - tss[0]).total_seconds() < threshold:
            concurrent.append((dest, (tss[-1] - tss[0]).total_seconds()))
dup_paths = sum(1 for tss in paths.values() if len(tss) > 1)
seq_dups = dup_paths - len(concurrent)
if concurrent:
    detail = "; ".join(f"{d} spread={s*1000:.0f}ms" for d, s in concurrent[:5])
    print(f"F: {len(concurrent)} concurrent renames within {threshold}s: {detail}")
    sys.exit(1)
print(f"F: no concurrent renames within {threshold}s; {total} total commits, {seq_dups} sequential duplicates")
sys.exit(0)
PY
); then
    assert_pass "${F_RESULT}"
else
    assert_fail "${F_RESULT}"
fi

# G. Orphan partials. With R8 in, A's mover bails on the fence check
# BEFORE issuing rename, so those rows leave NO partial behind. The
# only A partial that can survive is one where the libnfs write was
# mid-flight when the partition started but completed before the
# fence trip (i.e., the read-write loop finished but rename hadn't
# happened yet). Expected: 0–1 A partials.
A_PID_VAL=$(cat "${A_PID_FILE}")
B_PID_VAL=$(cat "${B_PID_FILE}")
ALL_PARTIALS="${RUN_DIR}/dst-partials.txt"
find "${DST_TREE_HOST}" -type f -name '.*.partial' > "${ALL_PARTIALS}" || true
b_partials=$(grep -E "\.${B_HOST}\.${B_PID_VAL}\.partial$" "${ALL_PARTIALS}" || true)
if [[ -n "${b_partials}" ]]; then
    assert_fail "G: orphan partials from B (clean exit) found; see ${ALL_PARTIALS}"
else
    a_count=$(grep -cE "\.${A_HOST}\.${A_PID_VAL}\.partial$" "${ALL_PARTIALS}" || true)
    assert_pass "G: no B partials; ${a_count:-0} A partials (allowed — fence trip mid-shard)"
fi

# H. R8 actually fired. Two evidence sources, either is sufficient:
#   - files_fenced > 0 in A's progress snapshot, OR
#   - Fenced log lines > 0 in A.err / A.out.
#
# The progress-snapshot path can be stale: heartbeat shutdown writes
# during the partition window may fail to land on S3, leaving the
# object on a pre-shard state with files_fenced=0. The log-count
# fallback is the load-bearing assertion.
files_fenced=0
if [[ -s "${A_PROGRESS_SNAPSHOT}" ]]; then
    files_fenced=$(python3 -c 'import sys,json
d=json.load(open(sys.argv[1]))
print(d.get("files_fenced", 0))' "${A_PROGRESS_SNAPSHOT}" 2>/dev/null || echo 0)
fi

# Concatenate first, THEN grep -c. `grep -c FILE1 FILE2` prints
# "FILE1:N\nFILE2:M" which trips arithmetic test downstream;
# `cat | grep -c` prints a single integer.
fenced_log_count=$( (cat "${A_ERR}" "${A_OUT}" 2>/dev/null || true) \
    | grep -cE 'FENCE_TRIPPED|row fenced before commit' \
    || true)
fenced_log_count=${fenced_log_count:-0}

if [[ "${files_fenced}" -gt 0 || "${fenced_log_count}" -gt 0 ]]; then
    assert_pass "H: R8 fired — files_fenced=${files_fenced}, log_hits=${fenced_log_count}"
else
    assert_fail "H: R8 did NOT fire — files_fenced=0 AND no Fenced log lines. Partition window may be too long; check ${A_OUT}/${A_ERR}."
fi

if [[ "${FAILED}" -ne 0 ]]; then
    log "RESULT: FAIL — see ${ASSERT_LOG} and ${RUN_DIR}"
    exit 1
fi
log "RESULT: PASS — all assertions satisfied; see ${ASSERT_LOG}"
exit 0
