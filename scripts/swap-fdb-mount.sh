#!/usr/bin/env bash
# Repoint every swarm service that bind-mounts /etc/foundationdb:/etc/foundationdb
# to /var/fdb:/etc/foundationdb.
#
# Usage:  ./swap-fdb-mount.sh            # apply
#         DRY_RUN=1 ./swap-fdb-mount.sh  # only list what would change
set -euo pipefail

OLD_SRC=/etc/foundationdb
NEW_SRC=/var/fdb
TARGET=/etc/foundationdb

# A bind mount whose source is missing makes swarm reject the task on that node,
# so /var/fdb (with fdb.cluster in it) must exist on every manager first.
if [[ ! -f "$NEW_SRC/fdb.cluster" ]]; then
  echo "ERROR: $NEW_SRC/fdb.cluster not found on this node." >&2
  echo "Create $NEW_SRC and copy fdb.cluster into it on ALL manager nodes first." >&2
  exit 1
fi

mapfile -t services < <(docker service ls --format '{{.Name}}')

matched=()
for svc in "${services[@]}"; do
  mounts=$(docker service inspect "$svc" \
    --format '{{range .Spec.TaskTemplate.ContainerSpec.Mounts}}{{.Type}}|{{.Source}}|{{.Target}}{{"\n"}}{{end}}')
  if grep -qx "bind|${OLD_SRC}|${TARGET}" <<<"$mounts"; then
    matched+=("$svc")
  fi
done

if [[ ${#matched[@]} -eq 0 ]]; then
  echo "No services mount ${OLD_SRC}:${TARGET}. Nothing to do."
  exit 0
fi

echo "Services to update (${#matched[@]}):"
printf '  %s\n' "${matched[@]}"

if [[ -n "${DRY_RUN:-}" ]]; then
  echo "DRY_RUN set, not updating."
  exit 0
fi

failed=()
for svc in "${matched[@]}"; do
  echo ">>> Updating $svc"
  # --mount-add with an existing target replaces that mount in place.
  # Do NOT also pass --mount-rm $TARGET: docker applies the removal after the
  # add, so it would drop the new mount as well.
  if ! docker service update --quiet \
        --mount-add "type=bind,source=${NEW_SRC},target=${TARGET}" \
        "$svc"; then
    failed+=("$svc")
  fi
done

FDB_SERVER=app_fdb-server
if docker service inspect "$FDB_SERVER" >/dev/null 2>&1; then
  if [[ ! -d "$NEW_SRC/data" || ! -d "$NEW_SRC/logs" ]]; then
    echo "ERROR: $NEW_SRC/data or $NEW_SRC/logs missing, not updating $FDB_SERVER data/log mounts." >&2
    failed+=("$FDB_SERVER (data/logs)")
  else
    echo ">>> Updating $FDB_SERVER data/log mounts"
    if ! docker service update --quiet \
          --mount-add "type=bind,source=${NEW_SRC}/data,target=/var/fdb/data" \
          --mount-add "type=bind,source=${NEW_SRC}/logs,target=/var/fdb/logs" \
          "$FDB_SERVER"; then
      failed+=("$FDB_SERVER (data/logs)")
    fi
  fi
fi

if [[ ${#failed[@]} -gt 0 ]]; then
  echo "FAILED:" >&2
  printf '  %s\n' "${failed[@]}" >&2
  exit 1
fi
echo "All ${#matched[@]} services updated."
