#!/bin/bash

TD=$(dirname -- "$(readlink -f -- "$0")")

# Grafana Password
export grafanaPassword=FKA1QNCsqdLBrjea1ZYw

# Grafana username
GF_ADMIN_USER=admin

HOST=3.237.189.214:3000

DASHBOARDS="${TD}/dashboards"
for dashboard in "${DASHBOARDS}/cluster.json" "${DASHBOARDS}/devices.json" "${DASHBOARDS}/nodes.json" "${DASHBOARDS}/lvols.json" "${DASHBOARDS}/pools.json"; do
    echo -e "\nUploading dashboard: ${dashboard}"
    curl -X POST -H "Content-Type: application/json" \
        -d "@${dashboard}" \
        "http://${GF_ADMIN_USER}:${grafanaPassword}@${HOST}/api/dashboards/import"
    echo ""
done

echo "Cluster deployment complete."
