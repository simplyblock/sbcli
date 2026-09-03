#!/usr/bin/env python3
"""
Standalone test for the Graylog / OpenSearch per-container log export.

Reads configuration from environment variables and exercises the same
API calls that the e2e ``export_graylog_logs()`` method uses.

Discovery strategy (in order):
  1. Graylog /terms endpoint  (fast, but may not exist in Graylog 5.x)
  2. OpenSearch terms aggregation  (reliable, finds ALL containers)
  3. Graylog search sampling  (last resort)

Fetch strategy:
  - Graylog REST API when reachable; OpenSearch scroll API otherwise.

Chunking:
  - The total time window is split into 1-hour chunks.
  - Chunks are processed in REVERSE order (newest first) so you can
    verify collection is working while it runs.
  - Each chunk writes to a subfolder: OUTPUT_DIR/chunk_NN_HH-MM_to_HH-MM/

Environment variables:
    MGMT_IP            Management node IP (required unless both dedicated IPs are set)
    CLUSTER_SECRET     Graylog admin password / cluster secret (required)
    START_TIME         UTC start time, ISO-8601 (optional; defaults to now - DURATION_MINUTES)
    DURATION_MINUTES   Window length in minutes (default: 60)
    DEPLOY_MODE        "docker" (default), "kubernetes", or "k8s-native"
    OUTPUT_DIR         Where to write per-container .log files (default: ./graylog_test_output)
    OPENSEARCH_IP      Dedicated OpenSearch IP (optional; defaults to MGMT_IP)
    GRAYLOG_IP         Dedicated Graylog IP (optional; defaults to MGMT_IP)

Usage:
    export MGMT_IP=192.168.10.210
    export CLUSTER_SECRET="<your-cluster-secret>"
    export START_TIME="2026-04-25T16:38:00"
    export DURATION_MINUTES=420
    python3 e2e/utils/test_graylog_export.py

    # Or with separate IPs for OpenSearch and Graylog:
    export OPENSEARCH_IP=192.168.10.210
    export GRAYLOG_IP=192.168.10.211
    export CLUSTER_SECRET="<your-cluster-secret>"
    export START_TIME="2026-06-27T21:59:00"
    export DURATION_MINUTES=660
    python3 e2e/utils/test_graylog_export.py
"""

import os
import sys
import time
from datetime import datetime, timezone, timedelta

try:
    import requests
except ImportError:
    print("ERROR: 'requests' library is required.  pip3 install requests", file=sys.stderr)
    sys.exit(1)

# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------

MGMT_IP = os.environ.get("MGMT_IP", "")
CLUSTER_SECRET = os.environ.get("CLUSTER_SECRET", "")
DEPLOY_MODE = os.environ.get("DEPLOY_MODE", "docker")
DURATION_MINUTES = int(os.environ.get("DURATION_MINUTES", "60"))
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "./graylog_test_output")
OPENSEARCH_IP = os.environ.get("OPENSEARCH_IP", "") or MGMT_IP
GRAYLOG_IP = os.environ.get("GRAYLOG_IP", "") or MGMT_IP

START_TIME_STR = os.environ.get("START_TIME", "")

if not MGMT_IP and not (OPENSEARCH_IP and GRAYLOG_IP):
    print("ERROR: MGMT_IP not set (or set OPENSEARCH_IP and GRAYLOG_IP)", file=sys.stderr)
    sys.exit(1)
if not CLUSTER_SECRET:
    print("ERROR: CLUSTER_SECRET not set", file=sys.stderr)
    sys.exit(1)

# Compute time window
if START_TIME_STR:
    start_dt = datetime.fromisoformat(START_TIME_STR.replace(" ", "T"))
    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=timezone.utc)
    end_dt = start_dt + timedelta(minutes=DURATION_MINUTES)
elif DURATION_MINUTES:
    # No start time but duration given -> last N minutes
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(minutes=DURATION_MINUTES)
else:
    # No start time, no duration -> collect everything Graylog has
    start_dt = datetime(2020, 1, 1, tzinfo=timezone.utc)
    end_dt = datetime.now(timezone.utc)

FROM_ISO = start_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
TO_ISO = end_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
FROM_MS = int(start_dt.timestamp() * 1000)
TO_MS = int(end_dt.timestamp() * 1000)

# URLs — use dedicated IPs when set, otherwise fall back to MGMT_IP
_K8S_MODES = ("kubernetes", "k8s-native")

if DEPLOY_MODE in _K8S_MODES:
    GRAYLOG_BASE = f"http://{GRAYLOG_IP}:9000/api"
else:
    GRAYLOG_BASE = f"http://{GRAYLOG_IP}/graylog/api"

OPENSEARCH_BASE = f"http://{OPENSEARCH_IP}/opensearch"

CNAME_FIELD = "kubernetes_container_name" if DEPLOY_MODE in _K8S_MODES else "container_name"

PAGE_SIZE = 1000
MAX_RESULT_WINDOW = 100_000

# Chunk size in minutes for splitting the time window
CHUNK_MINUTES = 60

# ---------------------------------------------------------------------------
# HTTP sessions
# ---------------------------------------------------------------------------

gl_session = requests.Session()
gl_session.auth = ("admin", CLUSTER_SECRET)
gl_session.headers.update({
    "X-Requested-By": "sb-log-collector",
    "Accept": "application/json",
})

os_session = requests.Session()
os_session.headers.update({"Content-Type": "application/json"})

# ---------------------------------------------------------------------------
# OpenSearch helpers (adapted from scripts/collect_logs.py)
# ---------------------------------------------------------------------------


def os_get_index():
    """Discover graylog indices in OpenSearch."""
    try:
        r = os_session.get(
            f"{OPENSEARCH_BASE}/_cat/indices?h=index&format=json", timeout=10
        )
        r.raise_for_status()
        indices = sorted(
            i["index"]
            for i in r.json()
            if i["index"].startswith("graylog") and not i["index"].startswith(".")
        )
        if indices:
            return ",".join(indices)
    except Exception as exc:
        print(f"    WARN: could not discover OpenSearch indices: {exc}")
    return "_all"


def os_probe(index):
    """Probe OpenSearch to discover field names and doc count."""
    result = {
        "ts_field": "timestamp",
        "cname_field": "container_name",
        "window_count": 0,
    }

    # Sample document to detect field names
    try:
        r = os_session.post(
            f"{OPENSEARCH_BASE}/{index}/_search",
            json={"size": 1, "query": {"match_all": {}}},
            timeout=10,
        )
        if r.ok:
            hits = r.json().get("hits", {}).get("hits", [])
            if hits:
                src = hits[0].get("_source", {})
                if "@timestamp" in src:
                    result["ts_field"] = "@timestamp"
                for candidate in (
                    "container_name", "container_id", "containerName",
                    "_container_name", "docker_container_name",
                ):
                    if candidate in src:
                        result["cname_field"] = candidate
                        break
    except Exception as exc:
        print(f"    WARN: OpenSearch probe (sample) failed: {exc}")

    # Count in time window
    ts = result["ts_field"]
    try:
        r = os_session.post(
            f"{OPENSEARCH_BASE}/{index}/_count",
            json={
                "query": {
                    "range": {
                        ts: {"gte": FROM_MS, "lte": TO_MS, "format": "epoch_millis"}
                    }
                }
            },
            timeout=10,
        )
        if r.ok:
            result["window_count"] = r.json().get("count", 0)
    except Exception as exc:
        print(f"    WARN: OpenSearch probe (count) failed: {exc}")

    return result


def os_discover_containers():
    """Discover (container_name, source) pairs via OpenSearch aggregation.

    Uses a nested terms aggregation: container_name -> source.
    Tries field.keyword first, then raw field name.

    Returns list of (container_name, source) tuples.
    """
    print("    Trying OpenSearch terms aggregation ...")
    index = os_get_index()
    probe = os_probe(index)

    print(f"    OpenSearch: index={index}  ts_field={probe['ts_field']}  "
          f"cname_field={probe['cname_field']}  docs_in_window={probe['window_count']}")

    if probe["window_count"] == 0:
        print("    OpenSearch: no documents in time window")
        return []

    cname_f = probe["cname_field"]
    for suffix in [".keyword", ""]:
        agg_cname = f"{cname_f}{suffix}"
        agg_source = f"source{suffix}"
        body = {
            "size": 0,
            "query": {
                "range": {
                    probe["ts_field"]: {
                        "gte": FROM_MS, "lte": TO_MS, "format": "epoch_millis",
                    }
                }
            },
            "aggs": {
                "containers": {
                    "terms": {
                        "field": agg_cname,
                        "size": 500,
                    },
                    "aggs": {
                        "sources": {
                            "terms": {
                                "field": agg_source,
                                "size": 100,
                            }
                        }
                    },
                }
            },
        }
        try:
            r = os_session.post(
                f"{OPENSEARCH_BASE}/{index}/_search", json=body, timeout=30
            )
            if r.ok:
                ctr_buckets = (
                    r.json()
                    .get("aggregations", {})
                    .get("containers", {})
                    .get("buckets", [])
                )
                if ctr_buckets:
                    pairs = []
                    for cb in ctr_buckets:
                        cname = cb["key"]
                        src_buckets = cb.get("sources", {}).get("buckets", [])
                        if src_buckets:
                            for sb in src_buckets:
                                pairs.append((cname, sb["key"]))
                        else:
                            pairs.append((cname, ""))
                    print(f"    OpenSearch discovered {len(ctr_buckets)} containers, "
                          f"{len(pairs)} (container, source) pairs "
                          f"(field={agg_cname}):")
                    for cb in sorted(ctr_buckets, key=lambda x: -x["doc_count"]):
                        src_buckets = cb.get("sources", {}).get("buckets", [])
                        for sb in sorted(src_buckets, key=lambda x: -x["doc_count"]):
                            print(f"      {cb['key']:<35} @ {sb['key']:<25} {sb['doc_count']:>10} docs")
                    return pairs
                else:
                    print(f"    OpenSearch terms agg on '{agg_cname}' "
                          f"returned no buckets, trying next")
            else:
                print(f"    OpenSearch terms agg on '{agg_cname}' returned "
                      f"HTTP {r.status_code}, trying next")
        except Exception as exc:
            print(f"    OpenSearch terms agg on '{agg_cname}' failed: {exc}, "
                  f"trying next")

    print("    OpenSearch terms aggregation found no containers")
    return []


MAX_RETRIES = 3
RETRY_BACKOFF = [10, 30, 60]  # seconds between retries on 429


def _os_request_with_retry(session, method, url, retries=MAX_RETRIES, **kwargs):
    """Make an HTTP request with retry on 429 (circuit breaker) errors."""
    for attempt in range(retries + 1):
        try:
            r = session.request(method, url, **kwargs)
            if r.status_code == 429 and attempt < retries:
                wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                print(f"    429 circuit breaker hit, waiting {wait}s "
                      f"(attempt {attempt + 1}/{retries}) ...", file=sys.stderr)
                time.sleep(wait)
                continue
            return r
        except requests.RequestException:
            if attempt < retries:
                wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                time.sleep(wait)
                continue
            raise
    return r  # unreachable but keeps linters happy


def os_fetch_container_logs(container_name, source, out_path,
                            chunk_from_ms, chunk_to_ms,
                            probe_cache=None):
    """Fetch logs from OpenSearch using the scroll API. Returns line count."""
    if probe_cache is None:
        probe_cache = {}
    if "index" not in probe_cache:
        probe_cache["index"] = os_get_index()
        probe_cache["probe"] = os_probe(probe_cache["index"])

    index = probe_cache["index"]
    probe = probe_cache["probe"]
    ts_f = probe["ts_field"]
    cname_f = probe["cname_field"]

    esc = container_name.replace("/", "\\/").replace(":", "\\:")
    must_clauses = [
        {"range": {ts_f: {"gte": chunk_from_ms, "lte": chunk_to_ms,
                           "format": "epoch_millis"}}},
        {"query_string": {"default_field": cname_f,
                           "query": f"*{esc}*",
                           "analyze_wildcard": True}},
    ]
    if source:
        must_clauses.append({
            "query_string": {
                "default_field": "source",
                "query": f'"{source}"',
            }
        })
    body = {
        "query": {"bool": {"must": must_clauses}},
        "sort": [{ts_f: {"order": "asc"}}],
        "size": PAGE_SIZE,
        "_source": [ts_f, "source", cname_f, "level", "message"],
    }

    def _fmt(src):
        ts = src.get("timestamp", src.get(ts_f, ""))
        s = src.get("source", "")
        cname = src.get("container_name", src.get(cname_f, ""))
        lvl = src.get("level", "")
        text = str(src.get("message", "")).replace("\n", "\\n")
        return f"{ts}  src={s}  ctr={cname}  lvl={lvl}  {text}"

    init_url = f"{OPENSEARCH_BASE}/{index}/_search?scroll=2m"
    written = 0

    try:
        r = _os_request_with_retry(
            os_session, "POST", init_url, json=body, timeout=60)
        if not r.ok:
            print(f"    WARN: OpenSearch scroll failed for {container_name}: "
                  f"HTTP {r.status_code} {r.text[:300]}", file=sys.stderr)
            open(out_path, "w").close()
            return 0
    except requests.RequestException as exc:
        print(f"    WARN: OpenSearch scroll failed for {container_name}: {exc}",
              file=sys.stderr)
        open(out_path, "w").close()
        return 0

    data = r.json()
    scroll_id = data.get("_scroll_id")
    hits = data.get("hits", {}).get("hits", [])
    total = data.get("hits", {}).get("total", {})
    total = total.get("value", total) if isinstance(total, dict) else int(total or 0)

    with open(out_path, "w") as fh:
        while hits:
            for h in hits:
                src = h.get("_source", {})
                if ts_f != "timestamp":
                    src["timestamp"] = src.get(ts_f, "")
                if cname_f != "container_name":
                    src["container_name"] = src.get(cname_f, "")
                fh.write(_fmt(src) + "\n")
                written += 1
            if len(hits) < PAGE_SIZE or not scroll_id:
                break
            try:
                sc_r = _os_request_with_retry(
                    os_session, "POST",
                    f"{OPENSEARCH_BASE}/_search/scroll",
                    json={"scroll": "2m", "scroll_id": scroll_id},
                    timeout=60,
                )
                if not sc_r.ok:
                    print(f"    WARN: scroll continuation failed for "
                          f"{container_name}: HTTP {sc_r.status_code}",
                          file=sys.stderr)
                    break
                sc_data = sc_r.json()
                scroll_id = sc_data.get("_scroll_id", scroll_id)
                hits = sc_data.get("hits", {}).get("hits", [])
            except requests.RequestException as exc:
                print(f"    WARN: scroll continuation failed for {container_name}: {exc}",
                      file=sys.stderr)
                break

    # Release scroll context
    if scroll_id:
        try:
            os_session.delete(
                f"{OPENSEARCH_BASE}/_search/scroll",
                json={"scroll_id": scroll_id},
                timeout=10,
            )
        except Exception:
            pass

    return written


# ---------------------------------------------------------------------------
# Graylog helpers
# ---------------------------------------------------------------------------


def _gl_escape(value):
    """Escape Lucene special characters (dots) in Graylog field queries."""
    return value.replace(".", "\\.")


def gl_discover_containers():
    """Discover (container, source) pairs via Graylog time-slice sampling.

    Large offsets cause Graylog 500 errors, so we slice the time window
    into chunks and sample the first page of each chunk.

    Returns list of (container_name, source) tuples.
    """
    print("    Falling back to Graylog time-slice discovery ...")
    search_url = f"{GRAYLOG_BASE}/search/universal/absolute"
    pairs = set()

    t_start = datetime.fromisoformat(FROM_ISO.replace("Z", "+00:00"))
    t_end = datetime.fromisoformat(TO_ISO.replace("Z", "+00:00"))
    total_minutes = (t_end - t_start).total_seconds() / 60

    # Use ~10-20 slices, minimum 1 minute each
    num_slices = max(1, min(20, int(total_minutes / 5)))
    slice_delta = (t_end - t_start) / num_slices

    print(f"    Sampling {num_slices} time slices across {total_minutes:.0f} minutes")

    for i in range(num_slices):
        s_from = t_start + slice_delta * i
        s_to = t_start + slice_delta * (i + 1)
        s_from_iso = s_from.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        s_to_iso = s_to.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        params = {
            "query": "*",
            "from": s_from_iso,
            "to": s_to_iso,
            "limit": 500,
            "offset": 0,
            "sort": "timestamp:asc",
            "fields": f"timestamp,source,{CNAME_FIELD}",
        }
        try:
            resp = gl_session.get(search_url, params=params, timeout=60,
                                  headers={"Accept": "application/json"})
            if resp.ok:
                messages = resp.json().get("messages", [])
                for m in messages:
                    msg = m.get("message", {})
                    name = msg.get(CNAME_FIELD, "")
                    source = msg.get("source", "")
                    if name:
                        pairs.add((name, source))
                print(f"    Slice {i+1}/{num_slices}: "
                      f"{len(messages)} msgs, {len(pairs)} unique pairs so far")
            else:
                print(f"    Slice {i+1}/{num_slices} returned HTTP "
                      f"{resp.status_code}")
        except Exception as exc:
            print(f"    Slice {i+1}/{num_slices} failed: {exc}")

    if pairs:
        print(f"    Discovered {len(pairs)} (container, source) pairs "
              f"via time-slice sampling:")
        for cname, src in sorted(pairs):
            print(f"      {cname:<35} @ {src}")
        return list(pairs)

    print("    No container names found via any method")
    return []


def gl_fetch_container_logs(container_name, source, out_path,
                            chunk_from_iso, chunk_to_iso):
    """Fetch all logs for a container+source via Graylog. Returns line count."""
    search_url = f"{GRAYLOG_BASE}/search/universal/absolute"
    # Use wildcard so partial names work (e.g. "spdk_8080" matches
    # "/spdk_8080", "SNodeAPI" matches "simplyblock_SNodeAPI.1.xyz")
    esc_name = _gl_escape(container_name)
    query = f'{CNAME_FIELD}:*{esc_name}*'
    if source:
        query += f' AND source:"{source}"'

    def _fetch_page(q, f_iso, t_iso, limit, offset):
        params = {
            "query": q, "from": f_iso, "to": t_iso,
            "limit": limit, "offset": offset,
            "sort": "timestamp:asc",
            "fields": "timestamp,source,container_name,level,message",
        }
        try:
            resp = gl_session.get(
                search_url, params=params, timeout=90,
                headers={"Accept": "application/json"},
            )
            resp.raise_for_status()
        except requests.RequestException as exc:
            print(f"    WARN: page request failed (offset={offset}): {exc}",
                  file=sys.stderr)
            return None, 0

        if not resp.text.strip():
            print(f"    WARN: empty response (offset={offset}, "
                  f"status={resp.status_code})", file=sys.stderr)
            return None, 0
        try:
            data = resp.json()
        except ValueError as exc:
            print(f"    WARN: invalid JSON (offset={offset}): {exc}",
                  file=sys.stderr)
            return None, 0
        return data.get("messages", []), data.get("total_results", 0)

    def _fmt(msg):
        ts = msg.get("timestamp", "")
        src = msg.get("source", "")
        cname = msg.get("container_name", "")
        lvl = msg.get("level", "")
        text = str(msg.get("message", "")).replace("\n", "\\n")
        return f"{ts}  src={src}  ctr={cname}  lvl={lvl}  {text}"

    def _write_window(fh, q, f_iso, t_iso):
        written = 0
        offset = 0
        msgs, total = _fetch_page(q, f_iso, t_iso, 1, 0)
        if msgs is None:
            return 0
        while offset < total:
            msgs, _ = _fetch_page(q, f_iso, t_iso, PAGE_SIZE, offset)
            if not msgs:
                break
            for m in msgs:
                fh.write(_fmt(m.get("message", {})) + "\n")
                written += 1
            offset += len(msgs)
            if len(msgs) < PAGE_SIZE:
                break
        return written

    # Probe total
    msgs, total = _fetch_page(query, chunk_from_iso, chunk_to_iso, 1, 0)
    if msgs is None:
        open(out_path, "w").close()
        return 0

    written = 0
    with open(out_path, "w") as fh:
        if total <= MAX_RESULT_WINDOW:
            written = _write_window(fh, query, chunk_from_iso, chunk_to_iso)
        else:
            # Split into 10-minute sub-windows
            t = datetime.fromisoformat(chunk_from_iso.replace("Z", "+00:00"))
            t_end = datetime.fromisoformat(chunk_to_iso.replace("Z", "+00:00"))
            chunk = timedelta(minutes=10)
            while t < t_end:
                chunk_end = min(t + chunk, t_end)
                c_from = t.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                c_to = chunk_end.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                written += _write_window(fh, query, c_from, c_to)
                t = chunk_end

    return written


# ---------------------------------------------------------------------------
# Discovery orchestrator
# ---------------------------------------------------------------------------


def discover_containers(graylog_ok):
    """Try all discovery methods in order.

    Returns list of (container_name, source) tuples.
    """
    print(f"\n[2] Discovering containers ({FROM_ISO} -> {TO_ISO}) ...")

    # 1. OpenSearch nested terms aggregation (most reliable)
    pairs = os_discover_containers()
    if pairs:
        return pairs

    # 2. Graylog time-slice sampling (last resort)
    if graylog_ok:
        pairs = gl_discover_containers()
        if pairs:
            return pairs

    return []


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _safe(s):
    return (
        s.replace("/", "_").replace("\\", "_")
        .replace(":", "_").strip("_")
    ) or "unnamed"


def _build_chunks(start, end, chunk_minutes=CHUNK_MINUTES):
    """Build list of (chunk_start_dt, chunk_end_dt) in reverse order (newest first)."""
    chunks = []
    t = start
    while t < end:
        c_end = min(t + timedelta(minutes=chunk_minutes), end)
        chunks.append((t, c_end))
        t = c_end
    chunks.reverse()  # newest first
    return chunks


def _fetch_chunk(pairs, chunk_start, chunk_end, chunk_dir, os_ok,
                 graylog_ok, os_probe_cache, chunk_label):
    """Fetch all container logs for a single time chunk.

    Containers are fetched SEQUENTIALLY (one scroll context at a time)
    to avoid OpenSearch circuit breaker / heap pressure.
    On OpenSearch failure, falls back to Graylog for that container.
    Returns total lines.
    """
    c_from_ms = int(chunk_start.timestamp() * 1000)
    c_to_ms = int(chunk_end.timestamp() * 1000)
    c_from_iso = chunk_start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    c_to_iso = chunk_end.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    os.makedirs(chunk_dir, exist_ok=True)

    total_lines = 0

    for i, (container_name, source) in enumerate(sorted(pairs), 1):
        safe_cname = _safe(container_name)
        if source:
            safe_source = _safe(source)
            fname = f"{safe_cname}__{safe_source}.log"
        else:
            fname = f"{safe_cname}.log"
        out_path = os.path.join(chunk_dir, fname)
        label = f"{container_name}@{source}" if source else container_name

        n = 0
        try:
            if os_ok:
                n = os_fetch_container_logs(
                    container_name, source, out_path,
                    chunk_from_ms=c_from_ms, chunk_to_ms=c_to_ms,
                    probe_cache=os_probe_cache,
                )
                # Fallback to Graylog if OpenSearch returned 0 lines
                if n == 0 and graylog_ok:
                    n = gl_fetch_container_logs(
                        container_name, source, out_path,
                        chunk_from_iso=c_from_iso, chunk_to_iso=c_to_iso,
                    )
            else:
                n = gl_fetch_container_logs(
                    container_name, source, out_path,
                    chunk_from_iso=c_from_iso, chunk_to_iso=c_to_iso,
                )
        except Exception as exc:
            print(f"    [{i}/{len(pairs)}] {label:<50} FAILED: {exc}")
            continue

        total_lines += n
        if n > 0:
            print(f"    [{i}/{len(pairs)}] {label:<50} {n:>8,} lines")

    return total_lines


def main():
    print("=" * 64)
    print("  Graylog / OpenSearch Export (Chunked, Reverse Order)")
    print("=" * 64)
    print(f"  Window     : {FROM_ISO}  ->  {TO_ISO}  ({DURATION_MINUTES} min)")
    print(f"  Chunk size : {CHUNK_MINUTES} min")
    print(f"  Mode       : {DEPLOY_MODE}")
    print(f"  Field      : {CNAME_FIELD}")
    print(f"  Graylog    : {GRAYLOG_BASE}")
    print(f"  OpenSearch : {OPENSEARCH_BASE}")
    print(f"  Output     : {OUTPUT_DIR}")
    print()

    # Check Graylog
    print(f"[1] Checking Graylog at {GRAYLOG_BASE} ...")
    graylog_ok = False
    try:
        r = gl_session.get(f"{GRAYLOG_BASE}/system", timeout=10)
        if r.status_code == 200:
            ver = r.json().get("version", "?")
            print(f"    OK  (version {ver})")
            graylog_ok = True
        else:
            print(f"    WARN: HTTP {r.status_code}")
            graylog_ok = True  # try anyway
    except Exception as exc:
        print(f"    FAIL: {exc} -- will use OpenSearch")

    # Check OpenSearch
    print(f"\n    Checking OpenSearch at {OPENSEARCH_BASE} ...")
    os_ok = False
    try:
        r = os_session.get(f"{OPENSEARCH_BASE}/_cluster/health", timeout=10)
        if r.status_code == 200:
            status = r.json().get("status", "?")
            print(f"    OK  (cluster status: {status})")
            os_ok = True
        else:
            print(f"    WARN: HTTP {r.status_code}")
    except Exception as exc:
        print(f"    FAIL: {exc}")

    if not graylog_ok and not os_ok:
        print("\nNeither Graylog nor OpenSearch is reachable. Exiting.")
        sys.exit(1)

    # Discover (container, source) pairs across the full window
    pairs = discover_containers(graylog_ok)
    if not pairs:
        print("\nNo containers found. Check your time window and log setup.")
        sys.exit(1)

    # Pre-populate probe cache once (shared across all chunks)
    os_probe_cache = {}
    if os_ok:
        try:
            os_probe_cache["index"] = os_get_index()
            os_probe_cache["probe"] = os_probe(os_probe_cache["index"])
        except Exception as exc:
            print(f"  WARN: Failed to pre-populate probe cache: {exc}")

    # Build 1-hour chunks in reverse order (newest first)
    chunks = _build_chunks(start_dt, end_dt, CHUNK_MINUTES)
    num_chunks = len(chunks)

    fetch_via = "OpenSearch" if os_ok else "Graylog"
    print(f"\n[3] Fetching logs in {num_chunks} chunk(s) of {CHUNK_MINUTES} min "
          f"(reverse order, newest first)")
    print(f"    {len(pairs)} (container, source) pairs via {fetch_via}")
    print("=" * 64)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    grand_total = 0

    for idx, (c_start, c_end) in enumerate(chunks, 1):
        c_start_label = c_start.strftime("%Y%m%d_%H%M")
        c_end_label = c_end.strftime("%H%M")
        chunk_dir_name = f"chunk_{idx:02d}_of_{num_chunks:02d}_{c_start_label}_to_{c_end_label}"
        chunk_dir = os.path.join(OUTPUT_DIR, chunk_dir_name)

        c_start_pretty = c_start.strftime("%Y-%m-%d %H:%M")
        c_end_pretty = c_end.strftime("%H:%M")
        print(f"\n--- Chunk {idx}/{num_chunks}: {c_start_pretty} -> {c_end_pretty} "
              f"-> {chunk_dir_name}/ ---")

        chunk_lines = _fetch_chunk(
            pairs, c_start, c_end, chunk_dir, os_ok,
            graylog_ok, os_probe_cache, chunk_dir_name,
        )
        grand_total += chunk_lines

        print(f"    Chunk {idx}/{num_chunks} done: {chunk_lines:,} lines")

    print()
    print("=" * 64)
    print(f"  TOTAL: {grand_total:,} lines from {len(pairs)} pairs "
          f"across {num_chunks} chunk(s)")
    print(f"  Output: {os.path.abspath(OUTPUT_DIR)}")
    print("=" * 64)
    print()


if __name__ == "__main__":
    main()
