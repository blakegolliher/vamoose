#!/usr/bin/env bash
# Record immutable lifecycle stage boundaries beneath ignored ops/state/.
set -euo pipefail

ops_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck disable=SC1091
source "$ops_dir/lib/run.sh"
ops_validate_run_spec

state_dir=$(ops_state_dir)
mkdir -p "$state_dir"
ledger="$state_dir/timings.tsv"
action=${1:-}
stage=${2:-}
case "$action" in
    start)
        [[ -n "$stage" && $# -eq 2 ]] || ops_fail "usage: timing.sh start STAGE"
        if [[ -f "$ledger" ]] && awk -F'\t' -v s="$stage" '$1==s && $3=="-"{found=1} END{exit !found}' "$ledger"; then
            ops_fail "stage already has an open start: $stage"
        fi
        printf '%s\t%s\t-\t-\n' "$stage" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$ledger" ;;
    end)
        [[ -n "$stage" && $# -eq 2 ]] || ops_fail "usage: timing.sh end STAGE"
        [[ -f "$ledger" ]] || ops_fail "timing ledger does not exist"
        start_iso=$(awk -F'\t' -v s="$stage" '$1==s && $3=="-"{v=$2} END{print v}' "$ledger")
        [[ -n "$start_iso" ]] || ops_fail "stage has no open start: $stage"
        end_iso=$(date -u +%Y-%m-%dT%H:%M:%SZ)
        duration=$(( $(date -d "$end_iso" +%s) - $(date -d "$start_iso" +%s) ))
        awk -F'\t' -v OFS='\t' -v s="$stage" -v e="$end_iso" -v d="$duration" '
            $1==s && $3=="-" {line=NR} {rows[NR]=$0} END {
                for (i=1;i<=NR;i++) {
                    if (i==line) {split(rows[i],f,"\t"); f[3]=e; f[4]=d; print f[1],f[2],f[3],f[4]}
                    else print rows[i]
                }
            }' "$ledger" > "$ledger.tmp"
        mv -- "$ledger.tmp" "$ledger" ;;
    report)
        [[ $# -eq 1 ]] || ops_fail "usage: timing.sh report"
        [[ -f "$ledger" ]] || ops_fail "timing ledger does not exist"
        column -t -s $'\t' "$ledger" ;;
    *) ops_fail "usage: timing.sh start STAGE | end STAGE | report" ;;
esac
