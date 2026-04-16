# Log Collection from OpenSearch (Graylog backend)

## Scripts

- `export_logs.sh` — Queries OpenSearch in paginated chunks, merges, and splits into spdk/proxy per node
- `merge_logs.py` — Merges paginated JSON exports into chronological CSV

## Quick Start

### 1. Copy scripts to .211

```bash
scp tests/perf/export_logs.sh tests/perf/merge_logs.py root@192.168.10.211:/tmp/
```

### 2. Increase OpenSearch result window (one-time)

```bash
OS=$(docker ps --format '{{.ID}}' --filter name=opensearch)
docker exec $OS curl -s -X PUT "http://localhost:9200/graylog_*/_settings" \
  -H "Content-Type: application/json" \
  -d '{"index.max_result_window": 200000}'
```

### 3. Run export

```bash
bash /tmp/export_logs.sh "2026-03-31 07:00:00.000" "2026-03-31 08:10:00.000"
```

Output files in `/tmp/graylog_export/`:
- `vm20X_all.csv` — all logs for node
- `vm20X_spdk.csv` — SPDK container logs only
- `vm20X_proxy.csv` — spdk_proxy logs only

### 4. Copy to jump host

```bash
for f in vm205_spdk vm205_proxy vm206_spdk vm206_proxy vm207_spdk vm207_proxy vm208_spdk vm208_proxy; do
  sshpass -p 3tango11 scp root@192.168.10.211:/tmp/graylog_export/${f}.csv /tmp/${f}.csv
done
```

## Manual OpenSearch Query

From inside the OpenSearch container or via `docker exec`:

```bash
OS=$(docker ps --format '{{.ID}}' --filter name=opensearch)
docker exec $OS curl -s -X POST "http://localhost:9200/graylog_*/_search" \
  -H "Content-Type: application/json" \
  -d '{
    "size": 10000,
    "from": 0,
    "sort": [{"timestamp": "desc"}],
    "query": {
      "bool": {
        "filter": [
          {"range": {"timestamp": {"gte": "2026-03-31 07:00:00.000", "lte": "2026-03-31 08:10:00.000"}}},
          {"wildcard": {"source": "vm205*"}}
        ]
      }
    }
  }' > /tmp/vm205_raw.json
```

**Notes:**
- Timestamp format must include milliseconds: `YYYY-MM-DD HH:MM:SS.SSS`
- Max 10000 results per query; use `"from": 10000` for next page
- Default `max_result_window` is 10000; increase with the PUT command above
- Sort `desc` to get most recent first; the merge script reverses to chronological order
