#!/usr/bin/env bash
set -euo pipefail

pkg_root="/usr/local/lib/python3.9/site-packages"
storage_node_ops="${pkg_root}/simplyblock_core/storage_node_ops.py"
migration_runner="${pkg_root}/simplyblock_core/services/tasks_runner_migration.py"

sudo cp /tmp/storage_node_ops.py "${storage_node_ops}"
sudo cp /tmp/tasks_runner_migration.py "${migration_runner}"
sudo python3 -m py_compile "${storage_node_ops}" "${migration_runner}"

mapfile -t app_containers < <(sudo docker ps --format '{{.Names}}' | grep '^app_' || true)
for c in "${app_containers[@]}"; do
  if ! sudo docker exec "${c}" test -d "${pkg_root}/simplyblock_core" 2>/dev/null; then
    echo "skip non-simplyblock container -> ${c}"
    continue
  fi
  echo "patch storage_node_ops.py -> ${c}"
  sudo docker cp /tmp/storage_node_ops.py "${c}:${storage_node_ops}"
  sudo docker exec "${c}" python3 -m py_compile "${storage_node_ops}"
done

mapfile -t migration_containers < <(sudo docker ps --format '{{.Names}}' | grep '^app_TasksRunnerMigration\.' || true)
if [ "${#migration_containers[@]}" -eq 0 ]; then
  echo "ERROR: app_TasksRunnerMigration container not found" >&2
  exit 1
fi

for c in "${migration_containers[@]}"; do
  echo "patch tasks_runner_migration.py -> ${c}"
  sudo docker cp /tmp/tasks_runner_migration.py "${c}:${migration_runner}"
  sudo docker exec "${c}" python3 -m py_compile "${migration_runner}"
done
