#!/bin/bash

TD=$(dirname -- "$(readlink -f -- "$0")")

# Grafana Password
export grafanaPassword=$1

# Grafana username
GF_ADMIN_USER=admin

HOST=0.0.0.0:3000

DASHBOARDS="${TD}/dashboards"
for dashboard in "${DASHBOARDS}/cluster.json" "${DASHBOARDS}/devices.json" "${DASHBOARDS}/nodes.json" "${DASHBOARDS}/lvols.json" "${DASHBOARDS}/pools.json" "${DASHBOARDS}/node-exporter.json"; do
    echo -e "\nUploading dashboard: ${dashboard}"
    curl -X POST -H "Content-Type: application/json" \
        -d "@${dashboard}" \
        "http://${GF_ADMIN_USER}:${grafanaPassword}@${HOST}/api/dashboards/import"
    echo ""
done

echo "Cluster deployment complete."
