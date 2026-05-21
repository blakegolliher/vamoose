#!/usr/bin/env bash
# Fast-reclaim drill — multi-host verification of the progress-file
# liveness cross-check landed in claim hardening (see
# docs/work-items/PROGRESS_LIVENESS_CROSS_CHECK.md).
#
# Two workers on a single shard. Worker A is SIGKILL'd mid-shard
# (NOT SIGSTOP — we want A's progress object frozen at its last
# tick's heartbeat_utc, not held alive). B reclaims and finishes
# the shard. Pass criterion: B observes the reclaim (its host_id +
# bumped epoch in the claim body) within `lease_timeout_sec / 2`
# of the kill moment, which proves the progress-cross-check path
# fired rather than the lease-fallback path.
#
# The two paths are distinguishable because the worker configs
# set heartbeat_sec=5, lease_timeout_sec=60: fast-reclaim is
# eligible at 2 × 5 = 10s; lease-only would not fire until 60s.
# A reclaim observed in the 20–30s envelope is a clean fast-reclaim;
# anything ≥ 30s is the lease path and means the cross-check
# failed.
#
# Final state assertions mirror the M5 harness (SHA-256 parity,
# claim terminal-Completed under B's host_id, failures sink empty)
# plus a new D assertion that B's progress object on S3 carries
# the new held_etag + heartbeat_sec fields populated correctly.
#
# Real-VAST run only — see docs/CORRECTNESS_RULES.md "Verification
# gates". Do not run this in CI.

set -euo pipefail

# -----------------------------------------------------------------------------
# Args + env
# -----------------------------------------------------------------------------

FILES=1000
FILE_SIZE=4096
TS="$(date -u +%Y%m%dT%H%M%SZ)"
BUCKET_PREFIX="fastrec-${TS}"
KEEP_ARTIFACTS=0
# Reclaim-latency budget: configured heartbeat_sec is 5; cross-check
# eligibility fires at 2×=10s; we accept up to 30s end-to-end to
# absorb sudo PAM + AWS init + libnfs mount on B. ≥30s is treated
# as a lease-path reclaim and fails the drill.
PASS_RECLAIM_S=30

usage() {
    cat <<EOF
usage: $0 [--files N] [--file-size BYTES] [--bucket-prefix S] [--keep-artifacts]
          [--pass-reclaim-s N]

Required env: same as scripts/m5-self-fence-test.sh — see that file
for the canonical env contract. AWS_PROFILE, VAMOOSE_BUCKET,
VAMOOSE_ENDPOINT, VAMOOSE_SRC_NFS_URL, VAMOOSE_DST_NFS_URL,
VAMOOSE_SRC_MOUNT, VAMOOSE_DST_MOUNT, VAMOOSE_SRC_ROOT,
VAMOOSE_DST_ROOT.

Invocation: sudo -E bash $0 [options]
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --files)            FILES="$2"; shift 2;;
        --file-size)        FILE_SIZE="$2"; shift 2;;
        --bucket-prefix)    BUCKET_PREFIX="$2"; shift 2;;
        --keep-artifacts)   KEEP_ARTIFACTS=1; shift;;
        --pass-reclaim-s)   PASS_RECLAIM_S="$2"; shift 2;;
        -h|--help)          usage; exit 0;;
        *)                  echo "unknown arg: $1" >&2; usage; exit 2;;
    esac
done

# -----------------------------------------------------------------------------
# Bootstrap — same HOME/PATH recovery trick as M5 (see comments there).
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
    exit 2
fi

: "${AWS_PROFILE:?AWS_PROFILE is required}"
: "${VAMOOSE_BUCKET:?VAMOOSE_BUCKET is required}"
: "${VAMOOSE_ENDPOINT:?VAMOOSE_ENDPOINT is required}"
: "${VAMOOSE_SRC_NFS_URL:?VAMOOSE_SRC_NFS_URL is required}"
: "${VAMOOSE_DST_NFS_URL:?VAMOOSE_DST_NFS_URL is required}"
: "${VAMOOSE_SRC_MOUNT:?VAMOOSE_SRC_MOUNT is required}"
: "${VAMOOSE_DST_MOUNT:?VAMOOSE_DST_MOUNT is required}"
: "${VAMOOSE_SRC_ROOT:?VAMOOSE_SRC_ROOT is required}"
: "${VAMOOSE_DST_ROOT:?VAMOOSE_DST_ROOT is required}"
if [[ "${VAMOOSE_SRC_ROOT}" == "${VAMOOSE_DST_ROOT}" ]]; then
    echo "VAMOOSE_SRC_ROOT and VAMOOSE_DST_ROOT must differ" >&2
    exit 2
fi
export DST_ROOT="${VAMOOSE_DST_ROOT}"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUN_DIR="${REPO_ROOT}/m5/run/fastrec-${TS}"
mkdir -p "${RUN_DIR}"

A_OUT="${RUN_DIR}/A.out"; A_ERR="${RUN_DIR}/A.err"; A_PID_FILE="${RUN_DIR}/A.pid"
B_OUT="${RUN_DIR}/B.out"; B_ERR="${RUN_DIR}/B.err"; B_PID_FILE="${RUN_DIR}/B.pid"
ASSERT_LOG="${RUN_DIR}/assertions.log"
: > "${ASSERT_LOG}"

A_HOST="fastrec-host-A"
B_HOST="fastrec-host-B"

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

log() { printf '[%s] %s\n' "$(date -u +%H:%M:%SZ)" "$*"; }
fail() { printf 'FAIL: %s\n' "$*" | tee -a "${ASSERT_LOG}" >&2; exit 1; }
assert_pass() { printf 'PASS: %s\n' "$*" | tee -a "${ASSERT_LOG}"; }
assert_fail() { printf 'FAIL: %s\n' "$*" | tee -a "${ASSERT_LOG}"; FAILED=1; }

# proc_alive — same trick as M5. SIGKILL doesn't produce zombies as
# reliably as SIGSTOP+CONT, but we still want to treat state=Z as
# gone (kernel-reaper raceyness on some hosts).
proc_alive() {
    local pid="$1"
    [[ -d "/proc/${pid}" ]] || return 1
    local state
    state=$(awk '/^State:/{print $2}' "/proc/${pid}/status" 2>/dev/null)
    [[ "${state}" != "Z" && -n "${state}" ]]
}

FAILED=0

cleanup() {
    local status=$?
    set +e
    log "cleanup: ensuring A and B are not running"
    if [[ -s "${A_PID_FILE}" ]]; then
        local apid; apid="$(cat "${A_PID_FILE}")"
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
    fail "sudo -n true failed; this harness requires passwordless sudo on this host"
fi

if [[ ! -x "${VAMOOSE_BIN}" ]]; then
    fail "vamoose binary not found or not executable: ${VAMOOSE_BIN}.
Run: cargo build --release --workspace"
fi

# Stale-binary check (CORRECTNESS_RULES.md "Verify binaries are fresh").
worker_mtime=$(stat -c %Y "${VAMOOSE_BIN}")
newest_src=$(find "${REPO_ROOT}/crates/migration-worker/src" \
                  "${REPO_ROOT}/crates/migration-mover/src" \
                  "${REPO_ROOT}/crates/migration-core/src" \
                  "${REPO_ROOT}/crates/vamoose-cli/src" \
                  -type f -name '*.rs' -printf '%T@\n' | sort -n | tail -1 | cut -d. -f1)
if [[ -n "${newest_src}" && "${newest_src}" -gt "${worker_mtime}" ]]; then
    fail "vamoose binary is older than crate sources. Rebuild:
  cargo build --release --workspace
  worker_mtime=${worker_mtime}, newest_src_mtime=${newest_src}"
fi

if ! aws_s3 ls "s3://${VAMOOSE_BUCKET}/" >/dev/null 2>&1; then
    fail "cannot list s3://${VAMOOSE_BUCKET} — check VAMOOSE_ENDPOINT, AWS_PROFILE, AWS_S3_FLAGS"
fi
if [[ ! -d "${VAMOOSE_SRC_MOUNT}" ]]; then
    fail "source mount not found: ${VAMOOSE_SRC_MOUNT}"
fi
if [[ ! -d "${VAMOOSE_DST_MOUNT}" ]]; then
    fail "dest mount not found: ${VAMOOSE_DST_MOUNT}"
fi
if [[ "${VAMOOSE_SRC_NFS_URL}" == "${VAMOOSE_DST_NFS_URL}" ]]; then
    fail "src and dst NFS URLs must differ; the worker overlap guard would refuse to start"
fi

# Walker preflight identical to M5 (post-RocksDB-removal check).
if "${NFS_WALKER}" help export-parquet >/dev/null 2>&1; then
    fail "${NFS_WALKER} still ships the export-parquet subcommand; this harness now \
requires the post-RocksDB-removal walker (nfs-walker <url> -o <out>.parquet). \
Rebuild from a current nfs-walker checkout."
fi

log "preflight: wiping any stale fast-reclaim artifacts in s3://${VAMOOSE_BUCKET}/"
for p in manifest.json index/ shards/ progress/ failures/ downgrades/ batches/; do
    aws_s3 rm "s3://${VAMOOSE_BUCKET}/${p}" --recursive >/dev/null 2>&1 || true
    aws_s3 rm "s3://${VAMOOSE_BUCKET}/${p}" >/dev/null 2>&1 || true
done

log "preflight: wiping stale dest tree at ${VAMOOSE_DST_MOUNT}${VAMOOSE_DST_ROOT}"
sudo rm -rf "${VAMOOSE_DST_MOUNT}${VAMOOSE_DST_ROOT}"

# -----------------------------------------------------------------------------
# Phase 1 — source tree + canonical parquet upload
# (identical to M5; copy-pasted here so the script is self-contained
# and small drift between the two harnesses is operator-visible)
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
    fail "expected ${FILES} source files, got ${ACTUAL_FILES} under ${SRC_TREE_HOST}"
fi
log "source tree built: ${ACTUAL_FILES} files at ${SRC_TREE_HOST}"

LEGACY_PARQUET_DIR="${RUN_DIR}/legacy.parquet"
CANON_OUT="${RUN_DIR}/canonical"
WALKER_LOG="${RUN_DIR}/walker.log"
: > "${WALKER_LOG}"

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

shopt -s globstar nullglob
legacy_files=( "${LEGACY_PARQUET_DIR}"/**/*.parquet )
shopt -u globstar nullglob
if [[ "${#legacy_files[@]}" -ne 1 ]]; then
    fail "single-shard required; walker produced ${#legacy_files[@]} files in ${LEGACY_PARQUET_DIR}"
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
    fail "single-shard required; canon produced ${#canon_shards[@]} files"
fi
SHARD_PARQUET="${canon_shards[0]}"
SHARD_NAME="part-0000.parquet"
log "canonical single shard: ${SHARD_PARQUET}"

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
    fail "could not determine parquet row count for ${SHARD_PARQUET}"
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
    "created_utc": datetime.datetime.utcnow().isoformat() + "Z",
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
# Phase 2 — worker configs
#
# heartbeat_sec=5, lease_timeout_sec=60. Fast-reclaim is eligible
# at 2 × heartbeat_sec = 10s. Lease-only fallback fires at 60s.
# The 6× gap makes the two paths trivially distinguishable in the
# observed reclaim latency.
# -----------------------------------------------------------------------------
log "Phase 2: generating worker configs (heartbeat=5s, lease=60s)"

HEARTBEAT_SEC=5
LEASE_TIMEOUT_SEC=60

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
heartbeat_sec     = ${HEARTBEAT_SEC}
lease_timeout_sec = ${LEASE_TIMEOUT_SEC}

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

# -----------------------------------------------------------------------------
# Phase 3 — orchestrate
# -----------------------------------------------------------------------------
log "Phase 3: launching worker A"

WORKER_RUST_LOG="info,migration_worker=info,migration_mover=debug"

# Launch A under sudo -n; capture vamoose-child PID via cmdline grep
# (same trick as M5 — sudo's launcher → monitor → vamoose chain
# means pgrep -P launcher-pid finds the monitor, not vamoose).
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
        if grep -qa -- "$(basename "${A_TOML}")" \
               "/proc/${cand}/cmdline" 2>/dev/null; then
            A_PID="${cand}"
            break 2
        fi
    done
done
if [[ -z "${A_PID}" ]]; then
    fail "worker A failed to launch; launcher pid=${A_LAUNCHER_PID}; see ${A_ERR}"
fi
echo "${A_PID}" > "${A_PID_FILE}"
log "worker A started, pid=${A_PID} (launcher=${A_LAUNCHER_PID})"

read_progress() {
    local host="$1"
    aws_s3 cp "s3://${VAMOOSE_BUCKET}/progress/host-${host}.json" - 2>/dev/null
}
read_claim() {
    aws_s3 cp "s3://${VAMOOSE_BUCKET}/shards/${SHARD_NAME}.claim" - 2>/dev/null
}

# Wait for A to acquire and start committing rows. We need at least
# one progress write so the held_etag field is populated on S3
# before we kill A — otherwise B can't cross-check against an etag
# A never published.
A_EPOCH_AT_KILL=0
deadline=$(( $(date +%s) + 120 ))
caught=0
while [[ $(date +%s) -lt ${deadline} ]]; do
    if ! proc_alive "${A_PID}"; then
        fail "worker A exited prematurely; see ${A_ERR}"
    fi
    commits=$(awk '/commit: rename .partial/{n++} END{print n+0}' "${A_OUT}" 2>/dev/null)
    commits=${commits:-0}
    # Also require that A has published at least one progress object
    # with a non-null held_etag — without it, B's cross-check can't
    # match and would degrade to lease-only (which is what we're
    # trying to NOT measure).
    a_progress=$(read_progress "${A_HOST}" || true)
    a_held=""
    if [[ -n "${a_progress}" ]]; then
        a_held=$(echo "${a_progress}" | python3 -c \
            'import sys,json;d=json.load(sys.stdin);print(d.get("held_etag") or "")' 2>/dev/null || echo "")
    fi
    if [[ "${commits}" -ge 5 && -n "${a_held}" ]]; then
        caught=1
        break
    fi
    sleep 0.5
done
if [[ "${caught}" -ne 1 ]]; then
    fail "could not catch worker A mid-shard with a published held_etag; see ${A_OUT} / progress/host-${A_HOST}.json"
fi

claim_body=$(read_claim || true)
if [[ -n "${claim_body}" ]]; then
    A_EPOCH_AT_KILL=$(echo "${claim_body}" | python3 -c \
        'import sys,json;d=json.load(sys.stdin);print(d.get("epoch",0))' 2>/dev/null || echo 0)
fi

# Confirm A's progress object has heartbeat_sec populated correctly.
# This is the precondition for the cross-check we're testing — if
# heartbeat_sec=0, scan_shards defers to lease and we'd be measuring
# the wrong path.
A_HB_SEC=$(read_progress "${A_HOST}" | python3 -c \
    'import sys,json;d=json.load(sys.stdin);print(d.get("heartbeat_sec",0))' 2>/dev/null || echo 0)
if [[ "${A_HB_SEC}" -ne "${HEARTBEAT_SEC}" ]]; then
    fail "A's progress object reports heartbeat_sec=${A_HB_SEC}, expected ${HEARTBEAT_SEC}; \
the cross-check fields are not being written — verify the build is current"
fi

# Capture the exact kill moment with sub-second precision; we'll
# measure the reclaim latency from here.
KILL_TS=$(date +%s.%N)
KILL_TS_HUMAN=$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)
log "SIGKILL A pid=${A_PID} epoch_pre=${A_EPOCH_AT_KILL} ts=${KILL_TS_HUMAN}"
sudo -n kill -KILL "${A_PID}"

# Wait for the OS to reap A so we don't race a still-writing
# heartbeat against B's scan.
wait_deadline=$(( $(date +%s) + 5 ))
while proc_alive "${A_PID}" && [[ $(date +%s) -lt ${wait_deadline} ]]; do
    sleep 0.1
done
if proc_alive "${A_PID}"; then
    fail "A did not exit within 5s of SIGKILL — proc state may be stuck"
fi

# Launch B. Cross-check is gated on B's scan_shards seeing A's
# progress object as stale — at this moment A has just been killed,
# so progress.heartbeat_utc is at most HEARTBEAT_SEC seconds old.
# The 2×heartbeat_sec=10s window means B's first scan that runs ≥10s
# after A's last tick will fast-reclaim.
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
        if grep -qa -- "$(basename "${B_TOML}")" \
               "/proc/${cand}/cmdline" 2>/dev/null; then
            B_PID="${cand}"
            break 2
        fi
    done
done
if [[ -z "${B_PID}" ]]; then
    fail "worker B failed to launch; launcher pid=${B_LAUNCHER_PID}; see ${B_ERR}"
fi
echo "${B_PID}" > "${B_PID_FILE}"
log "worker B started, pid=${B_PID} (launcher=${B_LAUNCHER_PID})"

# Poll for reclaim. Budget = lease_timeout_sec + slack — if we
# haven't seen the reclaim by then, EITHER fast-reclaim didn't
# fire AND lease fallback also didn't (which is a bug) OR the
# reclaim happened but our poll missed it.
RECLAIM_TS=""
RECLAIM_EPOCH=""
RECLAIM_DEADLINE=$(( $(date +%s) + LEASE_TIMEOUT_SEC + 20 ))
while [[ $(date +%s) -lt ${RECLAIM_DEADLINE} ]]; do
    if ! proc_alive "${B_PID}"; then
        fail "worker B exited before reclaim; see ${B_ERR}"
    fi
    body=$(read_claim || true)
    if [[ -n "${body}" ]]; then
        chost=$(echo "${body}" | python3 -c \
            'import sys,json;d=json.load(sys.stdin);print(d.get("host",""))' 2>/dev/null || echo "")
        cepoch=$(echo "${body}" | python3 -c \
            'import sys,json;d=json.load(sys.stdin);print(d.get("epoch",0))' 2>/dev/null || echo 0)
        if [[ "${chost}" == "${B_HOST}" && "${cepoch}" -gt "${A_EPOCH_AT_KILL}" ]]; then
            RECLAIM_TS=$(date +%s.%N)
            RECLAIM_EPOCH="${cepoch}"
            log "B reclaimed: epoch ${A_EPOCH_AT_KILL} → ${cepoch}"
            break
        fi
    fi
    sleep 0.5
done
if [[ -z "${RECLAIM_TS}" ]]; then
    fail "B did not reclaim within ${LEASE_TIMEOUT_SEC}+20s of kill"
fi

# Compute reclaim latency from KILL_TS (sub-second). awk handles the
# floating-point subtraction safely.
RECLAIM_ELAPSED=$(awk -v k="${KILL_TS}" -v r="${RECLAIM_TS}" 'BEGIN{printf "%.2f", r - k}')
log "reclaim latency: ${RECLAIM_ELAPSED}s (kill=${KILL_TS_HUMAN}, threshold pass<${PASS_RECLAIM_S}s lease=${LEASE_TIMEOUT_SEC}s)"

# Wait for B to complete the shard.
deadline=$(( $(date +%s) + 4 * LEASE_TIMEOUT_SEC ))
b_done=0
while [[ $(date +%s) -lt ${deadline} ]]; do
    body=$(read_progress "${B_HOST}" || true)
    if [[ -n "${body}" ]]; then
        rd=$(echo "${body}" | python3 -c \
            'import sys,json;d=json.load(sys.stdin);print(d.get("shard_rows_done",0))' 2>/dev/null || echo 0)
        rt=$(echo "${body}" | python3 -c \
            'import sys,json;d=json.load(sys.stdin);print(d.get("shard_rows_total",0))' 2>/dev/null || echo 0)
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
    fail "B did not complete shard within 4 × lease_timeout_sec"
fi
log "B completed shard"

# Wait for B to exit cleanly (it should after marking Completed).
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

# A. Reclaim latency under the fast-reclaim threshold. This is the
# load-bearing assertion — anything else is supporting evidence.
elapsed_lt_pass=$(awk -v e="${RECLAIM_ELAPSED}" -v p="${PASS_RECLAIM_S}" \
                      'BEGIN{print (e < p) ? "1" : "0"}')
elapsed_lt_lease=$(awk -v e="${RECLAIM_ELAPSED}" -v l="${LEASE_TIMEOUT_SEC}" \
                       'BEGIN{print (e < l) ? "1" : "0"}')
if [[ "${elapsed_lt_pass}" -eq 1 ]]; then
    assert_pass "A: reclaim latency ${RECLAIM_ELAPSED}s < ${PASS_RECLAIM_S}s — fast-reclaim path fired (lease would have been ${LEASE_TIMEOUT_SEC}s)"
elif [[ "${elapsed_lt_lease}" -eq 1 ]]; then
    assert_fail "A: reclaim latency ${RECLAIM_ELAPSED}s ≥ ${PASS_RECLAIM_S}s pass threshold but < ${LEASE_TIMEOUT_SEC}s lease — fast-reclaim either didn't fire or was unusually slow"
else
    assert_fail "A: reclaim latency ${RECLAIM_ELAPSED}s ≥ ${LEASE_TIMEOUT_SEC}s — lease fallback path, not fast-reclaim"
fi

# B. Final claim record is Completed by B.
final_claim=$(read_claim || true)
if [[ -z "${final_claim}" ]]; then
    assert_fail "B: claim object not present at run end"
else
    final_state=$(echo "${final_claim}" | python3 -c \
        'import sys,json;d=json.load(sys.stdin);print(d.get("state",""))')
    final_host=$(echo "${final_claim}" | python3 -c \
        'import sys,json;d=json.load(sys.stdin);print(d.get("host",""))')
    if [[ "${final_state}" == "completed" && "${final_host}" == "${B_HOST}" ]]; then
        assert_pass "B: final claim state=completed host=${B_HOST}"
    else
        assert_fail "B: final claim state=${final_state} host=${final_host} (expected completed/${B_HOST})"
    fi
fi

# C. Dest file count == source file count, excluding .partial files.
src_count=$(find "${SRC_TREE_HOST}" -type f -not -name '.*.partial' | wc -l)
dst_count=$(find "${DST_TREE_HOST}" -type f -not -name '.*.partial' | wc -l)
if [[ "${src_count}" -eq "${dst_count}" ]]; then
    assert_pass "C: file count src=${src_count} dst=${dst_count}"
else
    assert_fail "C: file count mismatch src=${src_count} dst=${dst_count}"
fi

# D. Per-file SHA-256 match (content only).
SRC_SHA="${RUN_DIR}/src.sha"; DST_SHA="${RUN_DIR}/dst.sha"
( cd "${SRC_TREE_HOST}" && find . -type f -not -name '.*.partial' -print0 | LC_ALL=C sort -z \
    | xargs -0 sha256sum ) > "${SRC_SHA}"
( cd "${DST_TREE_HOST}" && find . -type f -not -name '.*.partial' -print0 | LC_ALL=C sort -z \
    | xargs -0 sha256sum ) > "${DST_SHA}"
if diff -q "${SRC_SHA}" "${DST_SHA}" >/dev/null; then
    assert_pass "D: SHA-256 match across $(wc -l < "${SRC_SHA}") files"
else
    diff -u "${SRC_SHA}" "${DST_SHA}" > "${RUN_DIR}/sha.diff" || true
    assert_fail "D: SHA-256 mismatch; see ${RUN_DIR}/sha.diff"
fi

# E. B's progress object carries the new cross-check fields populated.
# Sanity that the heartbeat task is actually writing held_etag and
# heartbeat_sec — without this the cross-check path can't work even
# when the assertion-A reclaim time happened to land fast for an
# unrelated reason (e.g. lease misconfigured).
b_progress=$(read_progress "${B_HOST}" || true)
if [[ -z "${b_progress}" ]]; then
    assert_fail "E: B progress object missing on S3"
else
    b_hb_sec=$(echo "${b_progress}" | python3 -c \
        'import sys,json;d=json.load(sys.stdin);print(d.get("heartbeat_sec",0))' 2>/dev/null || echo 0)
    # held_etag may be None if B has already marked Completed and
    # cleared its held-claim cell; the field MUST exist (presence is
    # what proves the schema bump went through), and heartbeat_sec
    # MUST equal the configured value.
    has_held_field=$(echo "${b_progress}" | python3 -c \
        'import sys,json;d=json.load(sys.stdin);print("1" if "held_etag" in d else "0")' 2>/dev/null || echo 0)
    if [[ "${b_hb_sec}" -eq "${HEARTBEAT_SEC}" && "${has_held_field}" -eq 1 ]]; then
        assert_pass "E: B progress carries cross-check fields (heartbeat_sec=${b_hb_sec}, held_etag present)"
    else
        assert_fail "E: B progress missing cross-check fields (heartbeat_sec=${b_hb_sec} expected ${HEARTBEAT_SEC}; held_etag present=${has_held_field})"
    fi
fi

# F. Failures sinks empty/absent. SIGKILL of A means any in-flight
# rows from A simply weren't committed; B re-runs the whole shard.
# Per-file failures should be empty.
fail_a=$(aws_s3 cp "s3://${VAMOOSE_BUCKET}/failures/host-${A_HOST}.jsonl" - 2>/dev/null || true)
fail_b=$(aws_s3 cp "s3://${VAMOOSE_BUCKET}/failures/host-${B_HOST}.jsonl" - 2>/dev/null || true)
if [[ -z "${fail_a}" && -z "${fail_b}" ]]; then
    assert_pass "F: failures/host-A.jsonl + failures/host-B.jsonl absent or empty"
else
    {
        echo "--- failures/host-A.jsonl ---"
        echo "${fail_a}"
        echo "--- failures/host-B.jsonl ---"
        echo "${fail_b}"
    } > "${RUN_DIR}/failures.txt"
    assert_fail "F: per-file failures present; see ${RUN_DIR}/failures.txt"
fi

# G. Orphan partials from B. A's partials are tolerated (A was
# SIGKILL'd mid-write); B's partials shouldn't exist after a clean
# completion.
B_PID_VAL=$(cat "${B_PID_FILE}")
ALL_PARTIALS="${RUN_DIR}/dst-partials.txt"
find "${DST_TREE_HOST}" -type f -name '.*.partial' > "${ALL_PARTIALS}" || true
b_partials=$(grep -E "\.${B_HOST}\.${B_PID_VAL}\.partial$" "${ALL_PARTIALS}" || true)
if [[ -n "${b_partials}" ]]; then
    assert_fail "G: orphan partials from B (clean exit) found; see ${ALL_PARTIALS}"
else
    A_PID_VAL=$(cat "${A_PID_FILE}")
    a_count=$(grep -cE "\.${A_HOST}\.${A_PID_VAL}\.partial$" "${ALL_PARTIALS}" || true)
    assert_pass "G: no B partials; ${a_count:-0} A partials (allowed — A was SIGKILL'd mid-write)"
fi

if [[ "${FAILED}" -ne 0 ]]; then
    log "RESULT: FAIL — see ${ASSERT_LOG} and ${RUN_DIR} for forensics"
    exit 1
fi
log "RESULT: PASS — fast-reclaim verified in ${RECLAIM_ELAPSED}s (threshold ${PASS_RECLAIM_S}s, lease ${LEASE_TIMEOUT_SEC}s)"
exit 0
