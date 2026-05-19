#!/usr/bin/env bash
# T4 — M2/M3 cookbook against real VAST hardware with --use-bucketed-pool.
#
# Drives a single worker through a mixed test tree that exercises all three
# bucketed-pool buckets (small <1 MiB, medium 1 MiB–1 GiB, large ≥1 GiB),
# then asserts byte-for-byte parity via scripts/manual-verify.sh.
#
# Companion to scripts/m5-self-fence-test.sh: same plumbing
# (nfs-walker → mig-walker-rewrite → S3 single-shard manifest → vamoose
# worker), but no SIGSTOP dance — this is the Phase 2 verification gate
# for the async file mover, not an R8 stress test.
#
# Real-VAST run only. Do not run in CI.
#
# Exits 0 on parity match. Emits a one-line summary the operator can paste
# into docs/work-items/MULTI_PASS_MOVER_PHASE2_NOTES.md.

set -euo pipefail

# -----------------------------------------------------------------------------
# Args + env
# -----------------------------------------------------------------------------

LARGE_BYTES=$((1280 * 1024 * 1024))     # ~1.25 GiB → large bucket
MEDIUM_BYTES=$((10 * 1024 * 1024))      # 10 MiB    → medium bucket
SMALL_BYTES=4096                         # 4 KiB     → small bucket
N_SMALL=50
N_MEDIUM=5
N_LARGE=1
TS="$(date -u +%Y%m%dT%H%M%SZ)"
BUCKET_PREFIX="t4-${TS}"
KEEP_ARTIFACTS=0
SKIP_LARGE=0
USE_BUCKETED_POOL=1   # default: --use-bucketed-pool; --sync flips this off

usage() {
    cat <<EOF
usage: $0 [--sync] [--skip-large] [--bucket-prefix S] [--keep-artifacts]

  --sync  Run the worker WITHOUT --use-bucketed-pool (sync libnfs mover).
          Useful for A/B comparison against the async path to determine
          whether a parity diff is a Phase 2 regression or pre-existing
          sync-mover behavior.

Required env (same shape as scripts/m5-self-fence-test.sh):
  AWS_PROFILE          VAST credentials profile.
  VAMOOSE_BUCKET       S3 bucket to use. Will be cleared of t4 artifacts.
  VAMOOSE_ENDPOINT     VAST S3 endpoint URL.
  VAMOOSE_SRC_NFS_URL  Source NFS URL (libnfs, e.g. nfs://host/export).
  VAMOOSE_DST_NFS_URL  Destination NFS URL. Must NOT overlap source.
  VAMOOSE_SRC_MOUNT    Kernel-mounted path to the source export.
  VAMOOSE_DST_MOUNT    Kernel-mounted path to the destination export.
  VAMOOSE_SRC_ROOT     Path under the source export root for the test tree
                       (e.g. /t4/<ts>). Must round-trip through the walker
                       rewrite shim.
  VAMOOSE_DST_ROOT     Path under the destination export root. Must NOT
                       equal VAMOOSE_SRC_ROOT.

Optional env:
  NFS_WALKER           Path to nfs-walker binary (default detection same
                       as the M5 harness).
  MIG_WALKER_REWRITE   Path to mig-walker-rewrite (default: cargo run --release).
  VAMOOSE_BIN          Path to unified vamoose binary (default:
                       target/release/vamoose).
  AWS_S3_FLAGS         Extra args for aws s3 / aws s3api (e.g. --no-verify-ssl).

Tree shape:
  Small bucket   : ${N_SMALL} files × ${SMALL_BYTES} bytes  (4 KiB)
  Medium bucket  : ${N_MEDIUM} files × ${MEDIUM_BYTES} bytes (10 MiB)
  Large bucket   : ${N_LARGE} file  × ${LARGE_BYTES} bytes  (~1.25 GiB)
  Symlinks       : 2 (relative + absolute)
  Hardlinks      : 3-way group
  Plus mode/owner variation and a non-ASCII path.

  --skip-large drops the large-bucket file (useful for fast iteration).

Host requirements:
  - Run as root (libnfs needs UID 0).
  - Passwordless sudo for in-script signal delivery and rm of the dest tree.

Invocation:
  sudo -E bash $0 [options]

  HOME and PATH are auto-recovered from \$SUDO_USER so AWS credentials
  and pipx-installed 'aws' stay discoverable.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --sync)             USE_BUCKETED_POOL=0; shift;;
        --skip-large)       SKIP_LARGE=1; N_LARGE=0; shift;;
        --bucket-prefix)    BUCKET_PREFIX="$2"; shift 2;;
        --keep-artifacts)   KEEP_ARTIFACTS=1; shift;;
        -h|--help)          usage; exit 0;;
        *)                  echo "unknown arg: $1" >&2; usage; exit 2;;
    esac
done

if [[ "${USE_BUCKETED_POOL}" -eq 1 ]]; then
    MOVER_TAG="async"
    POOL_FLAG="--use-bucketed-pool"
else
    MOVER_TAG="sync"
    POOL_FLAG=""
fi

# -----------------------------------------------------------------------------
# Bootstrap — tolerate sudo's HOME/PATH stripping (see M5 harness for prose).
# -----------------------------------------------------------------------------
if [[ $EUID -ne 0 ]]; then
    echo "FAIL: this cookbook must run as root (libnfs needs UID 0)." >&2
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
RUN_DIR="${REPO_ROOT}/t4/run/${TS}-${MOVER_TAG}"
mkdir -p "${RUN_DIR}"

WORKER_OUT="${RUN_DIR}/worker.out"
WORKER_ERR="${RUN_DIR}/worker.err"
SUMMARY="${RUN_DIR}/summary.txt"
: > "${SUMMARY}"

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

aws_s3()    { aws ${AWS_S3_FLAGS} --endpoint-url "${VAMOOSE_ENDPOINT}" s3    "$@"; }
aws_s3api() { aws ${AWS_S3_FLAGS} --endpoint-url "${VAMOOSE_ENDPOINT}" s3api "$@"; }
log()  { printf '[%s] %s\n' "$(date -u +%H:%M:%SZ)" "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

# -----------------------------------------------------------------------------
# Cleanup trap
# -----------------------------------------------------------------------------
cleanup() {
    local status=$?
    set +e
    if [[ "${KEEP_ARTIFACTS}" -eq 0 ]]; then
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

if ! aws_s3 ls "s3://${VAMOOSE_BUCKET}/" >/dev/null 2>&1; then
    fail "cannot list s3://${VAMOOSE_BUCKET} — check VAMOOSE_ENDPOINT, AWS_PROFILE, AWS_S3_FLAGS"
fi
[[ -d "${VAMOOSE_SRC_MOUNT}" ]] || fail "source mount not found: ${VAMOOSE_SRC_MOUNT}"
[[ -d "${VAMOOSE_DST_MOUNT}" ]] || fail "dest mount not found: ${VAMOOSE_DST_MOUNT}"
[[ "${VAMOOSE_SRC_NFS_URL}" != "${VAMOOSE_DST_NFS_URL}" ]] \
    || fail "src and dst NFS URLs must differ"

log "preflight: wiping any stale t4 artifacts in s3://${VAMOOSE_BUCKET}/"
for p in manifest.json index/ shards/ progress/ failures/ downgrades/ batches/; do
    aws_s3 rm "s3://${VAMOOSE_BUCKET}/${p}" --recursive >/dev/null 2>&1 || true
    aws_s3 rm "s3://${VAMOOSE_BUCKET}/${p}" >/dev/null 2>&1 || true
done

SRC_TREE="${VAMOOSE_SRC_MOUNT}${VAMOOSE_SRC_ROOT}"
DST_TREE="${VAMOOSE_DST_MOUNT}${VAMOOSE_DST_ROOT}"

log "preflight: wiping stale src/dst trees"
sudo rm -rf "${SRC_TREE}" "${DST_TREE}"

# -----------------------------------------------------------------------------
# Phase 1 — build mixed test tree
# -----------------------------------------------------------------------------
log "Phase 1: building source tree (small=${N_SMALL}, medium=${N_MEDIUM}, large=${N_LARGE})"

sudo mkdir -p "${SRC_TREE}"
sudo chown "$(id -u):$(id -g)" "${SRC_TREE}"
sudo mkdir -p "${DST_TREE}"
sudo chown "$(id -u):$(id -g)" "${DST_TREE}"

python3 - "${SRC_TREE}" "${N_SMALL}" "${SMALL_BYTES}" \
           "${N_MEDIUM}" "${MEDIUM_BYTES}" \
           "${N_LARGE}" "${LARGE_BYTES}" <<'PY'
import os, sys, stat
root = sys.argv[1]
n_small, sz_small   = int(sys.argv[2]), int(sys.argv[3])
n_medium, sz_medium = int(sys.argv[4]), int(sys.argv[5])
n_large, sz_large   = int(sys.argv[6]), int(sys.argv[7])

def write(rel, size):
    full = os.path.join(root, rel)
    os.makedirs(os.path.dirname(full), exist_ok=True)
    seed = (rel.encode() + b"\n")
    blob = (seed * (size // len(seed) + 1))[:size]
    with open(full, "wb") as f:
        f.write(blob)
    return full

# Bucket-coverage files.
for i in range(n_small):
    write(f"small/file-{i:04d}.bin", sz_small)
for i in range(n_medium):
    write(f"medium/file-{i:04d}.bin", sz_medium)
for i in range(n_large):
    write(f"large/file-{i:04d}.bin", sz_large)

# Mode variation.
m = write("modes/mode-0644.bin", 4096); os.chmod(m, 0o644)
m = write("modes/mode-0755.bin", 4096); os.chmod(m, 0o755)
m = write("modes/mode-0600.bin", 4096); os.chmod(m, 0o600)

# Symlinks (relative + absolute) — sync mover handles these.
target = write("links/target.bin", 4096)
os.symlink("target.bin",   os.path.join(root, "links/symlink-rel.bin"))
os.symlink("/etc/hostname", os.path.join(root, "links/symlink-abs.bin"))

# Hardlink group — 3 paths sharing one inode.
hl = write("links/hl-original.bin", 4096)
os.link(hl, os.path.join(root, "links/hl-1.bin"))
os.link(hl, os.path.join(root, "links/hl-2.bin"))

# Non-ASCII path.
write("unicode/日本語ファイル.bin", 4096)

# Empty + tiny edge cases.
open(os.path.join(root, "empty.bin"), "wb").close()
with open(os.path.join(root, "tiny.txt"), "wb") as f:
    f.write(b"x")
PY

ACTUAL=$(find "${SRC_TREE}" -type f -o -type l | wc -l)
log "source tree built: ${ACTUAL} entries at ${SRC_TREE}"

# -----------------------------------------------------------------------------
# Walker → shim → upload
# -----------------------------------------------------------------------------
LEGACY_PARQUET_DIR="${RUN_DIR}/legacy.parquet"
CANON_OUT="${RUN_DIR}/canonical"
WALKER_LOG="${RUN_DIR}/walker.log"
: > "${WALKER_LOG}"

log "scan: ${NFS_WALKER} ${VAMOOSE_SRC_NFS_URL}${VAMOOSE_SRC_ROOT} → ${LEGACY_PARQUET_DIR}"
{
    if ! sudo "${NFS_WALKER}" "${VAMOOSE_SRC_NFS_URL}${VAMOOSE_SRC_ROOT}" \
            -o "${LEGACY_PARQUET_DIR}" \
            -w 16 -v \
            --writer-shards 1 \
            --no-log 2>&1; then
        echo "===== scan failed ====="
        fail "nfs-walker scan failed; see ${WALKER_LOG}"
    fi
} >> "${WALKER_LOG}" 2>&1

shopt -s globstar nullglob
legacy_files=( "${LEGACY_PARQUET_DIR}"/**/*.parquet )
shopt -u globstar nullglob
[[ "${#legacy_files[@]}" -eq 1 ]] \
    || fail "T4 requires single-shard manifest; walker produced ${#legacy_files[@]} files"
WALK_PARQUET_DIR="$(dirname "${legacy_files[0]}")"
log "walker parquet at ${WALK_PARQUET_DIR}"

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
[[ "${#canon_shards[@]}" -eq 1 ]] \
    || fail "T4 requires single-shard manifest (got ${#canon_shards[@]})"
SHARD_PARQUET="${canon_shards[0]}"
SHARD_NAME="part-0000.parquet"

ROW_COUNT=$( ( cd "${REPO_ROOT}" && \
    cargo run --release -p mig-walker-rewrite --example verify_shard --quiet -- \
        "${CANON_OUT}" 2>&1 ) \
    | awk '/^OK[[:space:]]+[0-9]+/{print $2; exit} /total_rows[= ]/{for(i=1;i<=NF;i++)if($i~/^[0-9]+$/){print $i; exit}}')
if ! [[ "${ROW_COUNT}" =~ ^[0-9]+$ ]] || [[ "${ROW_COUNT}" -eq 0 ]]; then
    ROW_COUNT=$(python3 - "${SHARD_PARQUET}" <<'PY'
import sys
import pyarrow.parquet as pq
print(pq.ParquetFile(sys.argv[1]).metadata.num_rows)
PY
)
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
    "created_utc": datetime.datetime.now(datetime.UTC).isoformat().replace("+00:00", "Z"),
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
# Phase 2 — worker config
# -----------------------------------------------------------------------------
log "Phase 2: generating worker config"
WORKER_TOML="${RUN_DIR}/worker.toml"
cat > "${WORKER_TOML}" <<EOF
[run]
bucket     = "${VAMOOSE_BUCKET}"
endpoint   = "${VAMOOSE_ENDPOINT}"
region     = "us-east-1"
profile    = "${AWS_PROFILE}"
verify_tls = false

[worker]
host_id           = "t4-cookbook"
heartbeat_sec     = 30
lease_timeout_sec = 180

[shard]
local_scratch = "${RUN_DIR}/scratch"
max_in_flight = 1

[mover]
strategy_default     = "libnfs_io_uring"
src_url              = "${VAMOOSE_SRC_NFS_URL}"
dst_url              = "${VAMOOSE_DST_NFS_URL}"
nfs_connections      = 16
pipeline_depth       = 8
io_uring_queue_depth = 256
fixed_buffer_count   = 256
fixed_buffer_size    = "1 MiB"

[batch]
bytes_budget       = "8 GiB"
files_budget       = 100000
inflight_small     = 256
inflight_medium    = 16
inflight_large     = 4
large_stripe_size  = "4 MiB"
large_stripe_depth = 32

[copy]
preserve_owner            = true
preserve_mode             = true
preserve_times            = true
preserve_xattr            = true
server_side_copy          = "off"
require_chown_capability  = true
require_unchanged_size    = false

[backpressure]
failure_pct_window_sec = 60
failure_pct_threshold  = 5.0
throughput_floor_mb_s  = 100
EOF
mkdir -p "${RUN_DIR}/scratch"
log "worker config: ${WORKER_TOML}"

# -----------------------------------------------------------------------------
# Phase 3 — run worker with --use-bucketed-pool, capture wall-clock
# -----------------------------------------------------------------------------
log "Phase 3: launching worker (mover=${MOVER_TAG}${POOL_FLAG:+ ${POOL_FLAG}})"
START_NS=$(date +%s%N)
set +e
sudo -E HOME="${HOME}" RUST_LOG=info,migration_mover=info,aws_smithy_runtime=warn \
    "${VAMOOSE_BIN}" worker --config "${WORKER_TOML}" ${POOL_FLAG} \
    >"${WORKER_OUT}" 2>"${WORKER_ERR}"
WORKER_RC=$?
set -e
END_NS=$(date +%s%N)
ELAPSED_NS=$((END_NS - START_NS))
ELAPSED_S=$(awk -v ns="${ELAPSED_NS}" 'BEGIN{printf "%.2f", ns/1e9}')
log "worker exit code: ${WORKER_RC} (${ELAPSED_S}s)"
[[ "${WORKER_RC}" -eq 0 ]] || fail "worker exited non-zero (see ${WORKER_ERR})"

# -----------------------------------------------------------------------------
# Phase 4 — parity + residue + summary
# -----------------------------------------------------------------------------
log "Phase 4: parity verification"
PARITY_LOG="${RUN_DIR}/parity.log"
set +e
"${REPO_ROOT}/scripts/manual-verify.sh" "${SRC_TREE}" "${DST_TREE}" >"${PARITY_LOG}" 2>&1
PARITY_RC=$?
set -e
if [[ "${PARITY_RC}" -eq 0 ]]; then
    PARITY_STATUS="PASS"
else
    PARITY_STATUS="FAIL (see ${PARITY_LOG})"
fi
log "parity: ${PARITY_STATUS}"

PARTIALS=$(sudo find "${DST_TREE}" -name '.*.partial' -print 2>/dev/null | wc -l)

TOTAL_BYTES=$(du -sb "${SRC_TREE}" | awk '{print $1}')
TOTAL_FILES=$(find "${SRC_TREE}" -type f | wc -l)
RATE_MBPS=$(awk -v b="${TOTAL_BYTES}" -v s="${ELAPSED_S}" \
    'BEGIN{ if(s>0) printf "%.1f", (b/1048576.0)/s; else print "n/a" }')

{
    echo "T4 cookbook result"
    echo "  mover         ${MOVER_TAG}"
    echo "  run_id        ${BUCKET_PREFIX}"
    echo "  files         ${TOTAL_FILES}"
    echo "  bytes         ${TOTAL_BYTES}"
    echo "  wall_clock_s  ${ELAPSED_S}"
    echo "  throughput    ${RATE_MBPS} MiB/s"
    echo "  worker_rc     ${WORKER_RC}"
    echo "  parity        ${PARITY_STATUS}"
    echo "  partials      ${PARTIALS}"
    echo "  src_tree      ${SRC_TREE}"
    echo "  dst_tree      ${DST_TREE}"
    echo "  worker_log    ${WORKER_OUT}"
} | tee "${SUMMARY}"

if [[ "${PARITY_RC}" -ne 0 || "${PARTIALS}" -ne 0 ]]; then
    log "RESULT: FAIL — see ${SUMMARY}"
    exit 1
fi
log "RESULT: PASS — see ${SUMMARY}"
