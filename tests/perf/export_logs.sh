#!/bin/bash
# Export SPDK/proxy logs from OpenSearch on .211
# Usage: bash export_logs.sh [from_ts] [to_ts]
# Example: bash export_logs.sh "2026-03-31 07:00:00.000" "2026-03-31 08:10:00.000"

FROM_TS="${1:-2026-03-31 07:00:00.000}"
TO_TS="${2:-2026-03-31 08:10:00.000}"
OS_CONTAINER=$(docker ps --format '{{.ID}}' --filter name=opensearch)
OUTDIR="/tmp/graylog_export"
mkdir -p "$OUTDIR"

echo "[$(date '+%H:%M:%S')] OpenSearch container: $OS_CONTAINER"
echo "[$(date '+%H:%M:%S')] Range: $FROM_TS to $TO_TS"

for node in vm205 vm206 vm207 vm208; do
    rm -f /tmp/${node}_p*.json ${OUTDIR}/${node}_all.csv
    offset=0
    while true; do
        echo "[$(date '+%H:%M:%S')] $node offset=$offset"
        docker exec "$OS_CONTAINER" curl -s -X POST \
            "http://localhost:9200/graylog_*/_search" \
            -H "Content-Type: application/json" \
            -d "{\"size\":10000,\"from\":$offset,\"sort\":[{\"timestamp\":\"desc\"}],\"query\":{\"bool\":{\"filter\":[{\"range\":{\"timestamp\":{\"gte\":\"$FROM_TS\",\"lte\":\"$TO_TS\"}}},{\"wildcard\":{\"source\":\"${node}*\"}}]}}}" \
            > /tmp/${node}_p${offset}.json

        count=$(python3 -c "
import json
d = json.load(open('/tmp/${node}_p${offset}.json'))
if 'error' in d:
    print(0)
else:
    print(len(d.get('hits',{}).get('hits',[])))
")
        echo "  count=$count"
        if [ "$count" -lt 10000 ]; then break; fi
        offset=$((offset + 10000))
    done

    # Merge all pages in chronological order
    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
    python3 "${SCRIPT_DIR}/merge_logs.py" "$node" 2>/dev/null || python3 /tmp/merge_logs.py "$node"

    # Split into spdk and proxy
    grep '|\[2026-' "${OUTDIR}/${node}_all.csv" > "${OUTDIR}/${node}_spdk.csv" 2>/dev/null || true
    grep -v '|\[2026-' "${OUTDIR}/${node}_all.csv" > "${OUTDIR}/${node}_proxy.csv" 2>/dev/null || true
    echo "[$(date '+%H:%M:%S')] $node: spdk=$(wc -l < ${OUTDIR}/${node}_spdk.csv) proxy=$(wc -l < ${OUTDIR}/${node}_proxy.csv)"
done

echo "[$(date '+%H:%M:%S')] Done. Files in $OUTDIR:"
wc -l ${OUTDIR}/*.csv
