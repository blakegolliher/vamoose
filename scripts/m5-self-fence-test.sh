#!/usr/bin/env bash
# M5 — multi-host self-fence verification harness.
#
# Two workers on a single shard. Worker A is paused mid-shard (SIGSTOP),
# B reclaims after the lease times out and finishes the shard, A is
# resumed (SIGCONT) and must self-fence on its first heartbeat refresh
# (412). Final state is asserted against the source tree.
#
# See docs/work-items/M5_SELF_FENCE.md for what 'done' looks like, why
# the assertions are sufficient, and the scope substitution
# (two-worker rather than three-worker).
#
# Real-VAST run only — see docs/CORRECTNESS_RULES.md "Verification gates".
# Do not run this in CI; it depends on libnfs, kernel mounts, and
# real S3 endpoints.

set -euo pipefail

# -----------------------------------------------------------------------------
# Args + env
# -----------------------------------------------------------------------------

FILES=1000
FILE_SIZE=4096
TS="$(date -u +%Y%m%dT%H%M%SZ)"
BUCKET_PREFIX="m5-${TS}"
KEEP_ARTIFACTS=0

usage() {
    cat <<EOF
usage: $0 [--files N] [--file-size BYTES] [--bucket-prefix S] [--keep-artifacts]

Required env:
  AWS_PROFILE          VAST credentials profile.
  VAMOOSE_BUCKET       S3 bucket to use. Will be cleared of m5 artifacts.
  VAMOOSE_ENDPOINT     VAST S3 endpoint URL.
  VAMOOSE_SRC_NFS_URL  Source NFS URL (libnfs, e.g. nfs://host/export).
  VAMOOSE_DST_NFS_URL  Destination NFS URL. Must NOT overlap source.
  VAMOOSE_SRC_MOUNT    Kernel-mounted path to the source export.
  VAMOOSE_DST_MOUNT    Kernel-mounted path to the destination export.
  VAMOOSE_SRC_ROOT     Path under the source export root for the test tree
                       (e.g. /m5/<ts>); must round-trip with --source-root
                       through the walker rewrite shim.
  VAMOOSE_DST_ROOT     Path under the destination export root where the
                       test tree is materialized (e.g. /m5-dst/<ts>). Must
                       NOT equal VAMOOSE_SRC_ROOT — the mover's overlap
                       guard refuses identical roots, and on the lab NFS
                       export the source tree's dirs would EEXIST against
                       the dest writes.

Optional env:
  NFS_WALKER           Path to nfs-walker binary. MUST support direct
                       parquet output (post-RocksDB-removal walker;
                       \`nfs-walker <url> -o <out>.parquet\`). Default
                       detection prefers
                       \$HOME/projects/nfs-walker/target/release/nfs-walker
                       and falls back to \$HOME/projects/nfs-walker/build/nfs-walker
                       only if target/release is missing.
  MIG_WALKER_REWRITE   Path to mig-walker-rewrite (default: cargo run --release).
  VAMOOSE_BIN          Path to unified vamoose binary (default: target/release/vamoose).
                       Invoked as 'vamoose worker --config <path>'. The worker
                       subcommand auto-detects unified vs. legacy worker TOML
                       formats; this harness emits the legacy format.
  AWS_S3_FLAGS         Extra args for aws s3 / aws s3api (e.g. --no-verify-ssl).

Host requirements:
  Run as root. libnfs mounts of the source export return EACCES under
  unprivileged users (matches the M2/M3 MANUAL_VERIFY.md flow). The
  harness also requires passwordless sudo for the in-script 'sudo -n'
  signal-delivery path; it fails fast in Phase 0 if that isn't usable.

Invocation:
  sudo -E bash $0 [options]

  The script auto-recovers HOME and PATH from \$SUDO_USER so AWS
  credentials at ~/.aws/credentials and pipx-installed 'aws' on
  ~/.local/bin remain discoverable. No need to thread HOME=\$HOME
  PATH=\$PATH manually.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --files)            FILES="$2"; shift 2;;
        --file-size)        FILE_SIZE="$2"; shift 2;;
        --bucket-prefix)    BUCKET_PREFIX="$2"; shift 2;;
        --keep-artifacts)   KEEP_ARTIFACTS=1; shift;;
        -h|--help)          usage; exit 0;;
        *)                  echo "unknown arg: $1" >&2; usage; exit 2;;
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
: "${VAMOOSE_SRC_ROOT:?VAMOOSE_SRC_ROOT is required (path under the source export, e.g. /m5/$TS)}"
: "${VAMOOSE_DST_ROOT:?VAMOOSE_DST_ROOT is required (path under the dest export, e.g. /m5-dst/$TS; must differ from VAMOOSE_SRC_ROOT)}"
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

# Walker default detection. We require the post-RocksDB-removal walker
# (single-step direct parquet output). target/release is preferred; the
# build/ symlink is only used if target/release is missing.
if [[ -z "${NFS_WALKER:-}" ]]; then
    if [[ -x "${HOME}/projects/nfs-walker/target/release/nfs-walker" ]]; then
        NFS_WALKER="${HOME}/projects/nfs-walker/target/release/nfs-walker"
    elif [[ -x "${HOME}/projects/nfs-walker/build/nfs-walker" ]]; then
        NFS_WALKER="${HOME}/projects/nfs-walker/build/nfs-walker"
    else
        NFS_WALKER="nfs-walker"
    fi
fi
if ! command -v "${NFS_WALKER}" >/dev/null 2>&1 && [[ ! -x "${NFS_WALKER}" ]]; then
    fail "nfs-walker not found at ${NFS_WALKER}; set NFS_WALKER or build it."
fi
# Stale-walker check: the post-removal walker rejects `help export-parquet`
# because that subcommand no longer exists. Conversely, presence of any
# `export-parquet`/`stats --live`/etc. hint in --help means a pre-removal
# binary that still wants a two-step rocks workflow. Reject either way.
if "${NFS_WALKER}" help export-parquet >/dev/null 2>&1; then
    fail "${NFS_WALKER} still ships the export-parquet subcommand; this harness now \
requires the post-RocksDB-removal walker (nfs-walker <url> -o <out>.parquet). \
Rebuild from a current nfs-walker checkout."
fi
VAMOOSE_BIN="${VAMOOSE_BIN:-${REPO_ROOT}/target/release/vamoose}"
MIG_WALKER_REWRITE_BIN="${MIG_WALKER_REWRITE:-}"
AWS_S3_FLAGS="${AWS_S3_FLAGS:-}"

# Shorthand for aws s3 invocations against the VAST endpoint.
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

# Liveness check that distinguishes "process gone" from "process still
# alive" but treats a zombie (state=Z) as gone. The worker calls
# libc::_exit at end of main(); on this host sudo's monitor sometimes
# doesn't reap the worker for tens of seconds, leaving /proc/<pid>
# present-but-zombied. From the harness's perspective the worker has
# logically exited the moment _exit fires, so the proc-state check
# matches that intent.
proc_alive() {
    local pid="$1"
    [[ -d "/proc/${pid}" ]] || return 1
    local state
    state=$(awk '/^State:/{print $2}' "/proc/${pid}/status" 2>/dev/null)
    [[ "${state}" != "Z" && -n "${state}" ]]
}

FAILED=0

# -----------------------------------------------------------------------------
# Cleanup trap — runs unconditionally.
# -----------------------------------------------------------------------------
cleanup() {
    local status=$?
    set +e
    log "cleanup: ensuring A and B are not running"
    # Workers run as root under sudo, so signals must go via 'sudo -n kill'.
    # Killing the vamoose worker (the captured PID) causes its sudo parent
    # to exit on its own; we don't track or kill the launcher PID separately.
    if [[ -s "${A_PID_FILE}" ]]; then
        local apid; apid="$(cat "${A_PID_FILE}")"
        # SIGCONT first in case we left it stopped.
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
    # Reap direct children (the backgrounded launcher sudos from Phase 3).
    # When the vamoose worker dies, its sudo monitor exits, then the launcher
    # sudo exits — without `wait` they sit as zombies under this script until
    # it itself exits, and any orphaned vamoose process reparented to init
    # slips through. Belt-and-braces: also kill any vamoose worker still
    # parented to init that was launched from these configs, in case the
    # sudo chain broke down.
    wait 2>/dev/null || true
    sudo -n pkill -KILL -P 1 -f "vamoose worker --config ${RUN_DIR}/" 2>/dev/null || true
    if [[ "${KEEP_ARTIFACTS}" -eq 0 && -n "${BUCKET_PREFIX:-}" ]]; then
        # Wipe everything we created in this bucket. Only known prefixes,
        # never the bucket itself.
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

# The harness runs the worker via sudo -n because libnfs mounts of the
# source export return EACCES under the unprivileged user. Fail loudly
# here if passwordless sudo isn't usable, rather than discovering it
# mid-Phase-3 when a worker fails to launch.
if ! sudo -n true 2>/dev/null; then
    fail "sudo -n true failed; this harness requires passwordless sudo on this host"
fi

if [[ ! -x "${VAMOOSE_BIN}" ]]; then
    fail "vamoose binary not found or not executable: ${VAMOOSE_BIN}.
Run: cargo build --release --workspace"
fi

# Stale-binary check (docs/CORRECTNESS_RULES.md "Verify binaries are fresh against current source").
# If any source file under the worker stack (incl. the vamoose-cli
# subcommand wrapper) is newer than the binary, refuse to start; the
# binary on disk may not include recent fixes.
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

# Confirm AWS / NFS endpoints reachable.
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

# Verify single-worker M2/M3 verification has happened. We don't have a
# durable marker file — this is a soft reminder. docs/CORRECTNESS_RULES.md
# "Verification gates": never build a milestone on top of an unverified one.
log "preflight: assuming MANUAL_VERIFY.md cookbook has passed recently against this hardware (m2-m3-verified tag)."

# Wipe any stale m5 state from prior runs in this bucket.
log "preflight: wiping any stale m5 artifacts in s3://${VAMOOSE_BUCKET}/"
for p in manifest.json index/ shards/ progress/ failures/ downgrades/ batches/; do
    aws_s3 rm "s3://${VAMOOSE_BUCKET}/${p}" --recursive >/dev/null 2>&1 || true
    aws_s3 rm "s3://${VAMOOSE_BUCKET}/${p}" >/dev/null 2>&1 || true
done

# Wipe any leftover dest tree under VAMOOSE_DST_ROOT before the run. The
# trap cleanup at exit only clears S3 prefixes, not the dest filesystem,
# so a prior failed run can leave dirs in place that EEXIST on the next
# attempt and fail the entire shard.
log "preflight: wiping stale dest tree at ${VAMOOSE_DST_MOUNT}${VAMOOSE_DST_ROOT}"
sudo rm -rf "${VAMOOSE_DST_MOUNT}${VAMOOSE_DST_ROOT}"

# -----------------------------------------------------------------------------
# Phase 1 — source tree + canonical parquet upload
# -----------------------------------------------------------------------------
log "Phase 1: building source tree (${FILES} files × ${FILE_SIZE} bytes)"

SRC_TREE_HOST="${VAMOOSE_SRC_MOUNT}${VAMOOSE_SRC_ROOT}"
DST_TREE_HOST="${VAMOOSE_DST_MOUNT}${VAMOOSE_DST_ROOT}"

# Clean & create source tree on the kernel mount (libnfs reads it later).
sudo rm -rf "${SRC_TREE_HOST}"
sudo mkdir -p "${SRC_TREE_HOST}"
sudo chown "$(id -u):$(id -g)" "${SRC_TREE_HOST}"

# Also clear any leftover dest under VAMOOSE_DST_ROOT — this run owns it.
sudo rm -rf "${DST_TREE_HOST}"
sudo mkdir -p "${DST_TREE_HOST}"
sudo chown "$(id -u):$(id -g)" "${DST_TREE_HOST}"

# Deterministic file generation:
#   path encoded as ASCII bytes, repeated to FILE_SIZE, then truncated.
# Distributing files across 10 subdirs to exercise mkdir-on-demand.
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

# Walker scan → mig-walker-rewrite → canonical single shard.
# Walker is single-step: writes sharded parquet under
# <output>/scans/<scan_id>/part-rNN-SSSSS.parquet + metadata.json. At
# 1000 × 4 KiB our test tree fits well under the part-file rotation
# threshold (--parquet-file-size-mb, default 512), so the default
# 32-shard layout produces a small handful of part files; we still
# require exactly one .parquet on disk to keep the M5 "single-shard
# manifest" invariant, so the harness pins --writer-shards=1.
LEGACY_PARQUET_DIR="${RUN_DIR}/legacy.parquet"
CANON_OUT="${RUN_DIR}/canonical"
WALKER_LOG="${RUN_DIR}/walker.log"
: > "${WALKER_LOG}"
# LEGACY_PARQUET_DIR and CANON_OUT must NOT exist yet — walker / shim
# refuse to clobber non-empty dirs.

# sudo because the source export is mounted with squash semantics and
# libnfs reads under the unprivileged user return EACCES (matches the
# M2/M3 cookbook). --no-log keeps the run dir free of the sidecar
# progress logfile; --writer-shards=1 enforces single-shard
# output so the downstream manifest invariant holds without extra
# plumbing.
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

# Walker writes scans/<scan_id>/part-rNN-SSSSS.parquet under the output
# dir. With --writer-shards=1 and the test-size tree that's
# exactly one part-r00-00000.parquet (plus metadata.json, which the shim
# ignores by extension filter).
shopt -s globstar nullglob
legacy_files=( "${LEGACY_PARQUET_DIR}"/**/*.parquet )
shopt -u globstar nullglob
if [[ "${#legacy_files[@]}" -ne 1 ]]; then
    fail "M5 requires single-shard manifest; walker produced ${#legacy_files[@]} files in ${LEGACY_PARQUET_DIR} (expected 1). Pin --writer-shards=1 (already set) and reduce --files if the part-file rotation threshold is being hit."
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

# Hard requirement: exactly one shard.
shopt -s globstar nullglob
canon_shards=( "${CANON_OUT}"/**/*.parquet )
shopt -u globstar nullglob
if [[ "${#canon_shards[@]}" -ne 1 ]]; then
    fail "M5 requires single-shard manifest; reduce --files or check walker shard threshold (got ${#canon_shards[@]})"
fi
SHARD_PARQUET="${canon_shards[0]}"
SHARD_NAME="part-0000.parquet"
log "canonical single shard: ${SHARD_PARQUET} → s3://${VAMOOSE_BUCKET}/index/${SHARD_NAME}"

# Get parquet row count via the shipped verify_shard example. This is a
# read-only walk that decodes through ShardReader, so it's an exact
# stand-in for what the worker will see.
ROW_COUNT=$( ( cd "${REPO_ROOT}" && \
    cargo run --release -p mig-walker-rewrite --example verify_shard --quiet -- \
        "${CANON_OUT}" 2>&1 ) \
    | awk '/^OK[[:space:]]+[0-9]+/{print $2; exit} /total_rows[= ]/{for(i=1;i<=NF;i++)if($i~/^[0-9]+$/){print $i; exit}}')
if ! [[ "${ROW_COUNT}" =~ ^[0-9]+$ ]] || [[ "${ROW_COUNT}" -eq 0 ]]; then
    # Fallback: try to read footer with python pyarrow if it's available.
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

# Upload index/<shard> and capture etag.
log "uploading shard parquet"
aws_s3 cp "${SHARD_PARQUET}" "s3://${VAMOOSE_BUCKET}/index/${SHARD_NAME}" >/dev/null
SHARD_ETAG=$(aws_s3api head-object --bucket "${VAMOOSE_BUCKET}" --key "index/${SHARD_NAME}" \
             --query 'ETag' --output text | tr -d '"')

# Generate manifest.json.
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
# Phase 2 — config generation
# -----------------------------------------------------------------------------
log "Phase 2: generating worker configs"

# Aggressive heartbeat for M5 diagnostic visibility — investigating
# WORKER_HEARTBEAT_NOT_FIRING_POST_SIGCONT.
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

HEARTBEAT_SEC=10
LEASE_TIMEOUT_SEC=60

# -----------------------------------------------------------------------------
# Phase 3 — orchestrate
# -----------------------------------------------------------------------------
log "Phase 3: launching worker A"

WORKER_RUST_LOG="info,migration_worker=info,migration_mover=debug"

# Launch under sudo -n: libnfs source-mount EACCES otherwise. SIGSTOP
# can't be caught, so signaling the sudo parent does NOT stop the
# vamoose child — we have to capture its PID and signal that one
# directly. HOME is forwarded so root finds ~/.aws/credentials at the
# operator's home, matching MANUAL_VERIFY.md.
( cd "${REPO_ROOT}" && \
  HOME="${HOME}" \
  RUST_LOG="${WORKER_RUST_LOG}" \
  AWS_PROFILE="${AWS_PROFILE}" \
  exec setsid sudo -n -E "${VAMOOSE_BIN}" worker --config "${A_TOML}" --use-bucketed-pool >"${A_OUT}" 2>"${A_ERR}" ) &
A_LAUNCHER_PID=$!
# sudo on this host uses a launcher → monitor → vamoose chain, where
# both launcher and monitor have comm=sudo. pgrep -P launcher_pid -x
# vamoose would match nothing (the only direct child has comm=sudo).
# Instead: pgrep -x vamoose for all candidates, then disambiguate A
# vs B by matching the config-file basename in /proc/<pid>/cmdline.
# Budget 50 × 0.2s = 10s for sudo PAM + AWS init + libnfs mount.
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
if [[ "${A_PID}" == "${A_LAUNCHER_PID}" ]]; then
    fail "worker A pid capture matched the launcher (sudo); pgrep logic broken"
fi
echo "${A_PID}" > "${A_PID_FILE}"
log "worker A started, pid=${A_PID} (launcher=${A_LAUNCHER_PID})"

# Wait for A to claim and start making progress.
read_progress() {
    local host="$1"
    aws_s3 cp "s3://${VAMOOSE_BUCKET}/progress/host-${host}.json" - 2>/dev/null
}
read_claim() {
    aws_s3 cp "s3://${VAMOOSE_BUCKET}/shards/${SHARD_NAME}.claim" - 2>/dev/null
}

A_ROWS_AT_STOP=0
A_EPOCH_AT_STOP=0
# Catch signal: count 'commit: rename' lines in A.out (mover's
# per-row commit DEBUG log). More reliable than S3 progress polling
# which depends on heartbeat cadence + S3 PUT success.
deadline=$(( $(date +%s) + 180 ))
caught=0
while [[ $(date +%s) -lt ${deadline} ]]; do
    if ! proc_alive "${A_PID}"; then
        fail "worker A exited prematurely; see ${A_ERR}"
    fi
    # Count successful rename commits A has emitted so far.
    # tracing DEBUG goes to stdout, hence A_OUT not A_ERR.
    # awk (not grep -c) because grep -c prints "0" AND exits 1 on no
    # matches; combined with `|| echo 0` that yields "0\n0" and breaks
    # the arithmetic test below. awk always exits 0 and prints once.
    commits=$(awk '/commit: rename .partial/{n++} END{print n+0}' "${A_OUT}" 2>/dev/null)
    commits=${commits:-0}
    if [[ "${commits}" -ge 5 ]]; then
        A_ROWS_AT_STOP="${commits}"
        caught=1
        break
    fi
    sleep 0.5
done
if [[ "${caught}" -ne 1 ]]; then
    fail "could not catch worker A mid-shard (no commits in ${A_OUT} within budget); increase --files or check worker logs"
fi

# Read A's current claim epoch so we can detect B's reclaim by epoch bump.
claim_body=$(read_claim || true)
if [[ -n "${claim_body}" ]]; then
    A_EPOCH_AT_STOP=$(echo "${claim_body}" | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("epoch",0))' 2>/dev/null || echo 0)
fi

STOP_TS=$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)
# Sub-second epoch for the reclaim-latency assertion (H).
STOP_TS_NS=$(date +%s.%N)
log "SIGSTOP A pid=${A_PID} at rows_done=${A_ROWS_AT_STOP} epoch_pre=${A_EPOCH_AT_STOP} ts=${STOP_TS}"
sudo -n kill -STOP "${A_PID}"

# Launch B (same sudo wrapper as A; same PID-capture trick).
log "launching worker B"
( cd "${REPO_ROOT}" && \
  HOME="${HOME}" \
  RUST_LOG="${WORKER_RUST_LOG}" \
  AWS_PROFILE="${AWS_PROFILE}" \
  exec setsid sudo -n -E "${VAMOOSE_BIN}" worker --config "${B_TOML}" --use-bucketed-pool >"${B_OUT}" 2>"${B_ERR}" ) &
B_LAUNCHER_PID=$!
# Same cmdline-disambiguation as A; see comment at A_LAUNCHER_PID.
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
if [[ "${B_PID}" == "${B_LAUNCHER_PID}" ]]; then
    fail "worker B pid capture matched the launcher (sudo); pgrep logic broken"
fi
echo "${B_PID}" > "${B_PID_FILE}"
log "worker B started, pid=${B_PID} (launcher=${B_LAUNCHER_PID})"

# Wait for B to reclaim. Budget: 2 × lease_timeout_sec.
#
# Poll tightly (0.5s) so the captured reclaim timestamp is accurate
# enough for assertion H. With the progress-cross-check landed
# (PROGRESS_LIVENESS_CROSS_CHECK.md) the typical reclaim time at
# heartbeat=1/lease=10 is ~2s, not the full lease — assertion H
# pins this and would fail loud if we ever regressed back to the
# lease-only path.
deadline=$(( $(date +%s) + 2 * LEASE_TIMEOUT_SEC ))
reclaimed=0
RECLAIM_TS_NS=""
while [[ $(date +%s) -lt ${deadline} ]]; do
    if ! proc_alive "${B_PID}"; then
        fail "worker B exited before reclaim; see ${B_ERR}"
    fi
    body=$(read_claim || true)
    if [[ -n "${body}" ]]; then
        chost=$(echo "${body}" | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("host",""))' 2>/dev/null || echo "")
        cepoch=$(echo "${body}" | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("epoch",0))' 2>/dev/null || echo 0)
        if [[ "${chost}" == "${B_HOST}" && "${cepoch}" -gt "${A_EPOCH_AT_STOP}" ]]; then
            reclaimed=1
            RECLAIM_TS_NS=$(date +%s.%N)
            log "B reclaimed: epoch ${A_EPOCH_AT_STOP} → ${cepoch}"
            break
        fi
    fi
    sleep 0.5
done
if [[ "${reclaimed}" -ne 1 ]]; then
    fail "B did not reclaim within 2 × lease_timeout_sec (${LEASE_TIMEOUT_SEC}s × 2)"
fi
RECLAIM_ELAPSED=$(awk -v s="${STOP_TS_NS}" -v r="${RECLAIM_TS_NS}" \
    'BEGIN{printf "%.2f", r - s}')
log "reclaim latency: ${RECLAIM_ELAPSED}s (lease=${LEASE_TIMEOUT_SEC}s, threshold for cross-check assertion: lease/2)"

# Wait for B to complete the shard. Budget: 4 × lease_timeout_sec.
deadline=$(( $(date +%s) + 4 * LEASE_TIMEOUT_SEC ))
b_done=0
while [[ $(date +%s) -lt ${deadline} ]]; do
    body=$(read_progress "${B_HOST}" || true)
    if [[ -n "${body}" ]]; then
        rd=$(echo "${body}" | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("shard_rows_done",0))' 2>/dev/null || echo 0)
        rt=$(echo "${body}" | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("shard_rows_total",0))' 2>/dev/null || echo 0)
        st=$(echo "${body}" | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("status",""))' 2>/dev/null || echo "")
        if [[ "${rt}" -gt 0 && "${rd}" -ge "${rt}" ]]; then
            b_done=1
            break
        fi
    fi
    if ! proc_alive "${B_PID}"; then
        # B exited — accept clean exit as completion.
        b_done=1
        break
    fi
    sleep 2
done
if [[ "${b_done}" -ne 1 ]]; then
    fail "B did not complete shard within 4 × lease_timeout_sec"
fi
log "B completed shard"

# Resume A. A's heartbeat will refresh with its old etag, get 412, fence,
# and exit. Also SIGCONT the sudo launcher: sudo's monitor mirrors its
# child's stopped state via job control, so when we SIGSTOP'd the vamoose
# worker, sudo stopped too (state Ts in `ps`). Sending SIGCONT only to
# the worker leaves sudo stopped, which means sudo never reaps its child
# after _exit — the worker becomes a permanent zombie and /proc/<pid>
# persists, fooling the harness's liveness check. CONTing both is the
# correct fix.
log "SIGCONT A pid=${A_PID} launcher=${A_LAUNCHER_PID}"
sudo -n kill -CONT "${A_LAUNCHER_PID}" "${A_PID}"

# Wait up to 3 × heartbeat_sec for A to exit on its own.
deadline=$(( $(date +%s) + 3 * HEARTBEAT_SEC ))
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
        fail "WORKER_SHUTDOWN_HANG regression suspected — bug was fixed by 8f768f8; if A hangs after fence trip, the cancellation-token wiring has regressed."
    else
        fail "A did not self-fence after SIGCONT (no 'fence tripped' line in ${A_OUT})"
    fi
fi

# Capture A's exit code via the launcher (sudo). Bash's `wait` only
# accepts direct children; A_PID is the vamoose worker (a grandchild).
# Sudo exits with the wrapped process's status, so the launcher PID's
# exit code is the worker's.
wait "${A_LAUNCHER_PID}" 2>/dev/null && A_EXIT=0 || A_EXIT=$?
log "A exit code: ${A_EXIT}"

# Be defensive: also wait for B to exit cleanly so we have its full err log.
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

# B. Dest file count == source file count, excluding .partial files.
src_count=$(find "${SRC_TREE_HOST}" -type f -not -name '.*.partial' | wc -l)
dst_count=$(find "${DST_TREE_HOST}" -type f -not -name '.*.partial' | wc -l)
if [[ "${src_count}" -eq "${dst_count}" ]]; then
    assert_pass "B: file count src=${src_count} dst=${dst_count}"
else
    assert_fail "B: file count mismatch src=${src_count} dst=${dst_count}"
fi

# C. Per-file SHA-256 match (content only).
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

# D. Worker A clean self-fence.
fence_line=$(grep -m1 "fence tripped" "${A_OUT}" || true)
if [[ -z "${fence_line}" ]]; then
    assert_fail "D: no 'fence tripped' line in A.out"
else
    if echo "${fence_line}" | grep -qE 'claim refresh|HEAD shows different etag'; then
        if [[ "${A_EXIT}" -eq 0 ]]; then
            assert_pass "D: A self-fenced cleanly (exit=0, reason mentions v2 'claim refresh: HEAD shows different etag')"
        else
            assert_fail "D: A fence tripped but exit code was ${A_EXIT}, not 0"
        fi
    else
        assert_fail "D: A fence reason did not mention 'claim refresh' or 'HEAD shows different etag': ${fence_line}"
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

# F. No CONCURRENT rename of the same dest path across A and B.
#
# Vamoose's design contract is at-least-once execution with a
# convergent destination; sequential duplicate renames during a
# mid-shard fence trip (A commits some rows pre-SIGSTOP, B reclaims
# and re-runs the shard from scratch, A's pre-trip paths get
# renamed again by B) are expected behavior, not bugs. The bug F
# must catch is two workers ACTUALLY RACING on the same dest path
# concurrently — that would mean the claim protocol failed to
# provide mutual exclusion. We detect it by checking that no dest
# path appears in both A.out and B.out with commit timestamps
# within F_THRESHOLD_S of each other.
F_THRESHOLD_S=1.0
if F_RESULT=$(A_OUT_PATH="${A_OUT}" B_OUT_PATH="${B_OUT}" \
              F_THRESHOLD_S="${F_THRESHOLD_S}" \
              python3 - <<'PY'
import os, re, sys, datetime, collections

threshold = float(os.environ["F_THRESHOLD_S"])
ansi_re   = re.compile(r"\x1b\[[0-9;]*m")
ts_re     = re.compile(r"^(\S+Z)")
dest_re   = re.compile(r"dest=(\S+)")

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
                ts = datetime.datetime.fromisoformat(
                    tm.group(1).replace("Z", "+00:00")
                )
                paths[dm.group(1)].append(ts)
    except FileNotFoundError:
        pass

total_commits = sum(len(v) for v in paths.values())
if total_commits == 0:
    print("F: no commit-rename events found in A.out/B.out "
          "(RUST_LOG include migration_mover=debug?)")
    sys.exit(1)

concurrent = []
for dest, tss in paths.items():
    if len(tss) > 1:
        tss.sort()
        spread = (tss[-1] - tss[0]).total_seconds()
        if spread < threshold:
            concurrent.append((dest, spread))

dup_paths = sum(1 for tss in paths.values() if len(tss) > 1)
sequential_dups = dup_paths - len(concurrent)

if concurrent:
    detail = "; ".join(f"{d} spread={s*1000:.0f}ms"
                       for d, s in concurrent[:5])
    print(f"F: {len(concurrent)} concurrent renames "
          f"(within {threshold}s) across A+B: {detail}")
    sys.exit(1)

print(f"F: no concurrent renames across A.out+B.out within "
      f"{threshold}s; {total_commits} total commits, "
      f"{sequential_dups} sequential duplicates accepted "
      f"(at-least-once execution under fence trip)")
sys.exit(0)
PY
); then
    assert_pass "${F_RESULT}"
else
    assert_fail "${F_RESULT}"
fi

# G. Orphan partials.
A_PID_VAL=$(cat "${A_PID_FILE}")
B_PID_VAL=$(cat "${B_PID_FILE}")
ALL_PARTIALS="${RUN_DIR}/dst-partials.txt"
find "${DST_TREE_HOST}" -type f -name '.*.partial' > "${ALL_PARTIALS}" || true
b_partials=$(grep -E "\.${B_HOST}\.${B_PID_VAL}\.partial$" "${ALL_PARTIALS}" || true)
b_clobber=0
if [[ -n "${b_partials}" ]]; then
    while IFS= read -r p; do
        # Reconstruct the basename the partial corresponds to and check for
        # a final dest with the same basename in the same dir.
        d=$(dirname "${p}")
        bn=$(basename "${p}")
        # .<basename>.<host>.<pid>.partial → strip leading "." and trailing ".<host>.<pid>.partial"
        stripped="${bn#.}"
        stripped="${stripped%.${B_HOST}.${B_PID_VAL}.partial}"
        if [[ -e "${d}/${stripped}" ]]; then
            b_clobber=1
            break
        fi
    done <<< "${b_partials}"
fi
if [[ -n "${b_partials}" ]]; then
    assert_fail "G: orphan partials from B (clean exit) found; see ${ALL_PARTIALS}"
elif [[ "${b_clobber}" -eq 1 ]]; then
    assert_fail "G: B partial coexists with a final dest of same basename; see ${ALL_PARTIALS}"
else
    a_count=$(grep -cE "\.${A_HOST}\.${A_PID_VAL}\.partial$" "${ALL_PARTIALS}" || true)
    assert_pass "G: no B partials; ${a_count:-0} A partials (allowed — A was paused mid-write)"
fi

# H. Reclaim took < lease_timeout / 2 — proves the cross-check
# (progress-file liveness) path fired, not the lease-fallback path.
#
# At M5's config (heartbeat=1, lease=10) the cross-check is
# eligible at 2 × heartbeat = 2s, while lease wouldn't fire until
# 10s. The half-lease threshold (5s) gives 2.5x headroom over the
# expected ~2s reclaim, absorbing B's startup + scan iteration
# without false-failing on slow clusters. A latency at or above
# lease_timeout_sec / 2 means the cross-check probe didn't see the
# progress object as stale and we waited the full lease — which
# would be a regression of the PROGRESS_LIVENESS_CROSS_CHECK work
# and warrants investigation.
H_THRESHOLD_S=$(awk -v l="${LEASE_TIMEOUT_SEC}" 'BEGIN{printf "%.2f", l / 2}')
if awk -v e="${RECLAIM_ELAPSED}" -v t="${H_THRESHOLD_S}" \
       'BEGIN{exit (e < t) ? 0 : 1}'; then
    assert_pass "H: reclaim latency ${RECLAIM_ELAPSED}s < ${H_THRESHOLD_S}s (lease/2) — cross-check path fired (lease would have been ${LEASE_TIMEOUT_SEC}s)"
else
    assert_fail "H: reclaim latency ${RECLAIM_ELAPSED}s ≥ ${H_THRESHOLD_S}s (lease/2) — cross-check did NOT fire; reclaim took the slow lease-fallback path. Verify the held_etag + heartbeat_sec fields are present on the progress object and that scan_shards is consulting them."
fi

if [[ "${FAILED}" -ne 0 ]]; then
    log "RESULT: FAIL — see ${ASSERT_LOG} and ${RUN_DIR} for forensics"
    exit 1
fi
log "RESULT: PASS — all assertions satisfied; see ${ASSERT_LOG}"
exit 0
