#!/usr/bin/env bash
set -euo pipefail

OUTDIR="${1:-$HOME/quick_issue_logs_$(date +%Y%m%d_%H%M%S)}"
SINCE="${2:-90m}"

mkdir -p "$OUTDIR/mgmt" "$OUTDIR/nodes"

echo "[INFO] outdir=$OUTDIR since=$SINCE"

CLUSTER_ID="$(sudo /usr/local/bin/sbctl -d cluster list --json 2>/dev/null | python3 -c 'import json,sys; d=json.load(sys.stdin); print((d[0] or {}).get("UUID",""))' || true)"
if [ -n "$CLUSTER_ID" ]; then
  sudo /usr/local/bin/sbctl -d cluster get "$CLUSTER_ID" > "$OUTDIR/mgmt/cluster_get.txt" 2>&1 || true
  sudo /usr/local/bin/sbctl -d cluster get-subtasks "$CLUSTER_ID" > "$OUTDIR/mgmt/cluster_subtasks.txt" 2>&1 || true
fi

sudo /usr/local/bin/sbctl -d cluster list > "$OUTDIR/mgmt/cluster_list.txt" 2>&1 || true
sudo /usr/local/bin/sbctl -d sn list > "$OUTDIR/mgmt/sn_list.txt" 2>&1 || true
sudo /usr/local/bin/sbctl -d sn check 5c81bbc2-c739-4e90-863c-e3931419f8e6 > "$OUTDIR/mgmt/sn_check_5c81.txt" 2>&1 || true

sudo docker ps --format '{{.Names}} {{.Image}}' > "$OUTDIR/mgmt/docker_ps.txt" 2>&1 || true

for c in $(sudo docker ps --format '{{.Names}}' | grep -E '^app_(StorageNodeMonitor|TasksRunnerMigration|HealthCheck|MainDistrEventCollector|TasksRunnerFailedMigration|TasksRunnerRestart)\.' || true); do
  sudo docker logs --since "$SINCE" "$c" > "$OUTDIR/mgmt/${c}.log" 2>&1 || true
done

for host in 54.211.110.50 54.173.20.244 3.91.89.126; do
  ndir="$OUTDIR/nodes/$host"
  mkdir -p "$ndir"

  ssh -o StrictHostKeyChecking=no -i "$HOME/.ssh/mtes01.pem" "ec2-user@$host" \
    "hostname; date -u; sudo docker ps --format '{{.Names}} {{.Image}}'" > "$ndir/docker_ps.txt" 2>&1 || true

  ssh -o StrictHostKeyChecking=no -i "$HOME/.ssh/mtes01.pem" "ec2-user@$host" \
    "for c in \$(sudo docker ps --format '{{.Names}}' | grep -E 'spdk|proxy' || true); do echo \"=== \$c ===\"; sudo docker logs --since '$SINCE' \$c 2>&1; done" \
    > "$ndir/spdk_related_logs.txt" 2>&1 || true

  ssh -o StrictHostKeyChecking=no -i "$HOME/.ssh/mtes01.pem" "ec2-user@$host" \
    "sudo dmesg -T | tail -n 400" > "$ndir/dmesg_tail.txt" 2>&1 || true
done

TAR_PATH="${OUTDIR}.tar.gz"
tar -czf "$TAR_PATH" -C "$(dirname "$OUTDIR")" "$(basename "$OUTDIR")"
echo "$TAR_PATH"
