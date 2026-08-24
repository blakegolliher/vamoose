#!/usr/bin/env bash
# Deploy one verified release and all rendered instance configs to the fleet.
set -euo pipefail

ops_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(git -C "$ops_dir" rev-parse --show-toplevel)
# shellcheck disable=SC1091
source "$ops_dir/lib/run.sh"
ops_validate_run_spec

[[ $# -eq 0 ]] || { echo "usage: deploy-release.sh" >&2; exit 2; }
for command_name in ssh scp sha256sum tar python3; do
    command -v "$command_name" >/dev/null || ops_fail "required command not found: $command_name"
done
[[ -f "$RELEASE_BUNDLE" ]] || ops_fail "release bundle not found: $RELEASE_BUNDLE"
[[ -f "$RELEASE_BUNDLE.sha256" ]] || ops_fail "release digest not found: $RELEASE_BUNDLE.sha256"
ops_assert_harness_provenance
(
    cd "$(dirname "$RELEASE_BUNDLE")"
    sha256sum --check --strict "$(basename "$RELEASE_BUNDLE.sha256")"
) >/dev/null || ops_fail "release archive digest verification failed"

release_name=$(tar -tzf "$RELEASE_BUNDLE" | sed -n '1s|/.*||p')
[[ "$release_name" =~ ^vamoose-[A-Za-z0-9._-]+$ ]] \
    || ops_fail "unexpected release archive layout"
release_dirty=$(tar -xOf "$RELEASE_BUNDLE" "$release_name/build-info.json" \
    | python3 -c 'import json,sys; print(str(json.load(sys.stdin)["git"]["dirty"]).lower())')
[[ "$release_dirty" == false ]] || ops_fail "refusing to deploy a dirty release bundle"

rendered_dir=$("$ops_dir/render-worker-configs.sh")
archive_name=$(basename "$RELEASE_BUNDLE")
remote_release_dir="$REMOTE_STAGING_DIR/$release_name"

mapfile -t worker_hosts < <(cut -f1 "$rendered_dir/workers.tsv" | LC_ALL=C sort -u)
printf 'deploying %s to %d hosts\n' "$release_name" "${#worker_hosts[@]}"
for host in "${worker_hosts[@]}"; do
    printf '[%s] staging release\n' "$host"
    ssh -o BatchMode=yes "$REMOTE_USER@$host" "mkdir -p '$remote_release_dir'"
    scp -q "$RELEASE_BUNDLE" "$RELEASE_BUNDLE.sha256" \
        "$repo_root/scripts/install-release.sh" "$repo_root/scripts/verify-release.sh" \
        "$REMOTE_USER@$host:$remote_release_dir/"
    ssh -o BatchMode=yes "$REMOTE_USER@$host" \
        "sudo '$remote_release_dir/install-release.sh' '$remote_release_dir/$archive_name' --prefix '$INSTALL_PREFIX'"

    printf '[%s] installing instance configs\n' "$host"
    scp -q "$rendered_dir/vamoose-worker@.service" \
        "$REMOTE_USER@$host:$remote_release_dir/vamoose-worker@.service"
    ssh -o BatchMode=yes "$REMOTE_USER@$host" \
        "sudo install -m 0644 '$remote_release_dir/vamoose-worker@.service' \
             '/etc/systemd/system/vamoose-worker@.service' && sudo systemctl daemon-reload"
    while IFS=$'\t' read -r row_host instance _host_id config_name scratch; do
        [[ "$row_host" == "$host" ]] || continue
        scp -q "$rendered_dir/$config_name" \
            "$REMOTE_USER@$host:$remote_release_dir/$config_name"
        ssh -n -o BatchMode=yes "$REMOTE_USER@$host" \
            "sudo install -d -m 0755 '$REMOTE_CONFIG_DIR' '$scratch' && \
             sudo install -m 0600 '$remote_release_dir/$config_name' '$REMOTE_CONFIG_DIR/$instance.toml'"
    done < "$rendered_dir/workers.tsv"

    ssh -o BatchMode=yes "$REMOTE_USER@$host" \
        "test \"\$(readlink '$INSTALL_PREFIX/current')\" = 'releases/$release_name' && \
         '$INSTALL_PREFIX/current/bin/vamoose' --version"
    printf '[%s] release and configs ready\n' "$host"
done
printf 'deployment complete: %s\n' "$release_name"
