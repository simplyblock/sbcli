#!/usr/bin/env python3
"""
Simplyblock Log Collector
=========================
Collects container logs from Graylog (or directly from OpenSearch) for a
specified time window, organises them by storage node and control-plane
service, and packages everything into a compressed tarball.

The script must be run on a management node or inside an admin pod where
the `sbctl` CLI is available and has full admin access.

Usage
-----
  collect_logs.py <start_time> <duration_minutes> [options]

  start_time        ISO-8601 datetime, UTC assumed when no timezone given.
                    Accepted formats: "2024-01-15T10:00:00"
                                      "2024-01-15 10:00:00"
                                      "2024-01-15T10:00:00+00:00"

  duration_minutes  Number of minutes to collect from start_time.

Options
-------
  --output-dir DIR    Write the tarball here (default: current directory).
  --mode MODE         Deployment mode: "docker" (default) or "kubernetes".
                      Selects the set of control-plane service names and
                      adjusts which log sources are queried.
  --use-opensearch    Query OpenSearch scroll API directly instead of the
                      Graylog search REST API.  Useful when Graylog is
                      unavailable or when the result set is very large.
  --cluster-id UUID   Force a specific cluster UUID (default: first cluster).
  --mgmt-ip IP        Override management-node IP for Graylog / OpenSearch.

Examples
--------
  collect_logs.py "2024-01-15T10:00:00" 60
  collect_logs.py "2024-01-15 10:00:00" 30 --output-dir /tmp/logs
  collect_logs.py "2024-01-15T10:00:00" 120 --use-opensearch
  collect_logs.py "2024-01-15T10:00:00" 60 --mode kubernetes
"""

import argparse
import json
import subprocess
import sys
import tarfile
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

try:
    import requests
except ImportError:
    print(
        "ERROR: the 'requests' library is required.\n"
        "       Install it with:  pip3 install requests",
        file=sys.stderr,
    )
    sys.exit(1)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Maximum records per single Graylog search page.
PAGE_SIZE = 1000

# OpenSearch max_result_window is set to 100 000 during cluster initialisation
# (see simplyblock_core/cluster_ops.py :: _set_max_result_window).
# Requests that would exceed this threshold are split into time-based chunks.
MAX_RESULT_WINDOW = 100_000

# Docker Swarm service names that run on the management / control-plane node.
CONTROL_PLANE_SERVICES_DOCKER = [
    "WebAppAPI",
    "WebAppAPI2",
    "WebAppAPI3",
    "WebAppAPI4",
    "WebAppAPI5",
    "fdb-server",
    "fdb-backup-agent",
    "StorageNodeMonitor",
    "MgmtNodeMonitor",
    "LVolStatsCollector",
    "MainDistrEventCollector",
    "CapacityAndStatsCollector",
    "CapacityMonitor",
    "HealthCheck",
    "DeviceMonitor",
    "LVolMonitor",
    "SnapshotMonitor",
    "TasksRunnerRestart",
    "TasksRunnerMigration",
    "TasksRunnerLVolMigration",
    "TasksRunnerFailedMigration",
    "TasksRunnerClusterStatus",
    "TasksRunnerNewDeviceMigration",
    "TasksNodeAddRunner",
    "TasksRunnerClusterExpand",
    "TasksRunnerPortAllow",
    "TasksRunnerJCCompResume",
    "TasksRunnerLVolSyncDelete",
    "TasksRunnerBackup",
    "TasksRunnerBackupMerge",
    "HAProxy",
]

CONTROL_PLANE_SERVICES_KUBERNETES = [
    "simplyblock-control",
    "webappapi",
    "storage-node-monitor",
    "mgmt-node-monitor",
    "lvol-stats-collector",
    "main-distr-event-collector",
    "capacity-and-stats-collector",
    "capacity-monitor",
    "health-check",
    "device-monitor",
    "lvol-monitor",
    "snapshot-monitor",
    "tasks-node-add-runner",
    "tasks-runner-restart",
    "tasks-runner-migration",
    "tasks-runner-failed-migration",
    "tasks-runner-cluster-status",
    "tasks-runner-new-device-migration",
    "tasks-runner-port-allow",
    "tasks-runner-jc-comp-resume",
    "tasks-runner-sync-lvol-del",
    "tasks-runner-backup",
    "tasks-runner-backup-merge",
    "tasks-runner-snapshot-replication",
]

# ---------------------------------------------------------------------------
# sbctl helpers
# ---------------------------------------------------------------------------


def _run(cmd, timeout=30):
    """Run *cmd* list; return CompletedProcess or None on failure."""
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError:
        print(f"ERROR: command not found: {cmd[0]}", file=sys.stderr)
        sys.exit(1)
    except subprocess.TimeoutExpired:
        print(f"ERROR: command timed out: {' '.join(cmd)}", file=sys.stderr)
        return None


def sbctl_json(*args):
    """
    Run ``sbctl <args> --json`` and return the parsed JSON (list or dict).
    Returns None and prints an error on failure.
    """
    cmd = ["sbctl"] + list(args) + ["--json"]
    r = _run(cmd)
    if r is None or r.returncode != 0:
        if r:
            print(f"ERROR: {' '.join(cmd)}\n  stderr: {r.stderr.strip()}", file=sys.stderr)
        return None
    try:
        return json.loads(r.stdout)
    except json.JSONDecodeError:
        print(
            f"ERROR: could not parse JSON from: {' '.join(cmd)}\n"
            f"  output: {r.stdout[:400]}",
            file=sys.stderr,
        )
        return None


def sbctl_raw(*args):
    """
    Run ``sbctl <args>`` (no --json) and return stripped stdout text.
    Returns None on failure.
    """
    r = _run(["sbctl"] + list(args))
    if r is None or r.returncode != 0:
        if r:
            print(
                f"ERROR: sbctl {' '.join(args)}\n  stderr: {r.stderr.strip()}",
                file=sys.stderr,
            )
        return None
    return r.stdout.strip()


# ---------------------------------------------------------------------------
# Log-line formatter
# ---------------------------------------------------------------------------


def _fmt(msg: dict) -> str:
    """Render a Graylog / OpenSearch message dict as a single log line."""
    ts = msg.get("timestamp", "")
    src = msg.get("source", "")
    cname = msg.get("container_name", "")
    lvl = msg.get("level", "")
    text = str(msg.get("message", "")).replace("\n", "\\n")
    return f"{ts}  src={src}  ctr={cname}  lvl={lvl}  {text}"


# ---------------------------------------------------------------------------
# Graylog REST API helpers
# ---------------------------------------------------------------------------

def _gl_escape(value: str) -> str:
    """
    Escape Lucene special characters in a Graylog field query term.
    Hyphens are NOT escaped — they are only special in range expressions
    and cause HTTP 400 when escaped in the Graylog REST API.
    """
    return value.replace(".", "\\.")


def _gl_search_page(session, search_url, query, from_iso, to_iso, limit, offset):
    """
    Fetch one page of results from the Graylog absolute-search endpoint.
    Returns (messages_list, total_results) or (None, 0) on error.
    """
    params = {
        "query": query,
        "from": from_iso,
        "to": to_iso,
        "limit": limit,
        "offset": offset,
        "sort": "timestamp:asc",
        "fields": "timestamp,source,container_name,level,message",
    }
    try:
        resp = session.get(search_url, params=params, timeout=90,
                           headers={"Accept": "application/json"})
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"    WARN: Graylog page request failed (offset={offset}): {exc}", file=sys.stderr)
        return None, 0

    if not resp.text.strip():
        print(f"    WARN: Graylog returned empty response (offset={offset}, status={resp.status_code})", file=sys.stderr)
        return None, 0
    try:
        data = resp.json()
    except requests.exceptions.JSONDecodeError as exc:
        print(f"    WARN: Graylog response is not valid JSON (offset={offset}): {exc}", file=sys.stderr)
        return None, 0
    return data.get("messages", []), data.get("total_results", 0)


def _gl_write_window(session, search_url, query, from_iso, to_iso, fh):
    """
    Paginate through a single time window and write ALL lines to *fh*.

    The window MUST have total <= MAX_RESULT_WINDOW.  Caller is responsible
    for checking the count first (via ``_gl_probe_total``) and bisecting
    the window when it exceeds the limit.

    Returns number of lines written.
    """
    written = 0
    offset = 0

    while True:
        msgs, _ = _gl_search_page(
            session, search_url, query, from_iso, to_iso, PAGE_SIZE, offset
        )
        if msgs is None:
            break
        if not msgs:
            break
        for m in msgs:
            fh.write(_fmt(m.get("message", {})) + "\n")
            written += 1
        offset += len(msgs)
        if len(msgs) < PAGE_SIZE:
            break
        if offset >= MAX_RESULT_WINDOW:
            break

    return written


def _gl_probe_total(session, search_url, query, from_iso, to_iso):
    """Return total number of matching entries for a window, or -1 on error."""
    msgs, total = _gl_search_page(session, search_url, query, from_iso, to_iso, 1, 0)
    if msgs is None:
        return -1
    return total


# Minimum bisection window size in seconds.  Windows smaller than this are
# fetched best-effort (capped at MAX_RESULT_WINDOW).
MIN_BISECT_SEC = 1


def _gl_fetch_recursive(session, search_url, query, from_dt, to_dt, fh,
                         depth=0):
    """
    Recursively bisect the time window until entries fit within
    MAX_RESULT_WINDOW, then paginate and write.

    Returns ``(lines_written, truncated_count)`` where *truncated_count*
    is the number of leaf windows that exceeded the limit even at the
    minimum window size (data loss).
    """
    from_iso = from_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    to_iso = to_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    window_sec = (to_dt - from_dt).total_seconds()
    indent = "    " + "  " * min(depth, 6)

    total = _gl_probe_total(session, search_url, query, from_iso, to_iso)
    if total <= 0:
        return 0, 0

    # Window fits — fetch everything
    if total <= MAX_RESULT_WINDOW:
        written = _gl_write_window(session, search_url, query,
                                    from_iso, to_iso, fh)
        return written, 0

    # Window too big — can we bisect further?
    if window_sec <= MIN_BISECT_SEC:
        # Smallest window reached — best-effort fetch (capped at 100k)
        print(f"{indent}WARN: {window_sec:.0f}s window at {from_iso} has "
              f"{total} entries (>{MAX_RESULT_WINDOW}), capturing first "
              f"{MAX_RESULT_WINDOW} (best effort)", file=sys.stderr)
        written = _gl_write_window(session, search_url, query,
                                    from_iso, to_iso, fh)
        return written, 1

    # Bisect
    mid_dt = from_dt + (to_dt - from_dt) / 2
    print(f"{indent}NOTE: {from_iso}..{to_iso} ({window_sec:.0f}s) has "
          f"{total} entries, bisecting")

    w1, t1 = _gl_fetch_recursive(session, search_url, query,
                                  from_dt, mid_dt, fh, depth + 1)
    w2, t2 = _gl_fetch_recursive(session, search_url, query,
                                  mid_dt, to_dt, fh, depth + 1)
    return w1 + w2, t1 + t2


def graylog_fetch_all(session, base_url, query, from_iso, to_iso, out_path):
    """
    Download all log messages matching *query* within [from_iso, to_iso].

    Uses recursive binary bisection: probe the window count first, and if
    it exceeds MAX_RESULT_WINDOW, split the window in half and recurse.
    Keeps halving down to MIN_BISECT_SEC (1 second).  Only writes data at
    leaf windows that fit within the limit (no duplicate writes).

    Writes one text line per message to *out_path*.
    Returns ``(lines_written, truncated_count)`` where *truncated_count*
    is the number of leaf windows that still exceeded the limit at the
    minimum window size.
    """
    search_url = f"{base_url}/search/universal/absolute"

    # Quick probe
    total = _gl_probe_total(session, search_url, query, from_iso, to_iso)
    if total < 0:
        Path(out_path).touch()
        return 0, 0
    if total == 0:
        Path(out_path).touch()
        print(f"    total entries: 0")
        return 0, 0

    print(f"    total entries: {total}")

    from_dt = datetime.fromisoformat(from_iso.replace("Z", "+00:00"))
    to_dt = datetime.fromisoformat(to_iso.replace("Z", "+00:00"))

    with open(out_path, "w") as fh:
        written, truncated = _gl_fetch_recursive(
            session, search_url, query, from_dt, to_dt, fh,
        )

    return written, truncated


# ---------------------------------------------------------------------------
# OpenSearch scroll API helpers (--use-opensearch)
# ---------------------------------------------------------------------------


def _os_get_index(session, os_url):
    """
    Discover the graylog indices present in OpenSearch and return them as a
    comma-separated string suitable for use in a URL path segment.

    Using _cat/indices avoids embedding a '*' wildcard in the URL, which
    HAProxy may reject (400).  Falls back to '_all' if discovery fails.
    """
    try:
        r = session.get(f"{os_url}/_cat/indices?h=index&format=json", timeout=10)
        r.raise_for_status()
        indices = sorted(
            i["index"]
            for i in r.json()
            if i["index"].startswith("graylog") and not i["index"].startswith(".")
        )
        if indices:
            return ",".join(indices)
    except Exception as exc:
        print(f"    WARN: could not discover OpenSearch indices ({exc}); using _all", file=sys.stderr)
    return "_all"


def _os_probe(session, os_url, index, from_ms, to_ms):
    """
    Probe the index to discover:
      - The actual timestamp field name (e.g. 'timestamp' vs '@timestamp')
      - The actual container-name field name
      - How many documents exist in the requested time window (any container)
      - A sample document so we can see real field values

    Returns a dict with keys: ts_field, cname_field, window_count, sample_doc
    """
    result = {"ts_field": "timestamp", "cname_field": "container_name",
              "window_count": 0, "sample_doc": None}

    # --- sample document (no time filter) ---
    try:
        r = session.post(
            f"{os_url}/{index}/_search",
            json={"size": 1, "query": {"match_all": {}}},
            timeout=10,
        )
        if r.ok:
            hits = r.json().get("hits", {}).get("hits", [])
            if hits:
                src = hits[0].get("_source", {})
                result["sample_doc"] = src
                # Detect timestamp field
                if "@timestamp" in src:
                    result["ts_field"] = "@timestamp"
                # Detect container-name field (various naming conventions)
                for candidate in ("kubernetes_container_name", "container_name",
                                  "container_id", "containerName",
                                  "_container_name", "docker_container_name"):
                    if candidate in src:
                        result["cname_field"] = candidate
                        break
    except Exception as exc:
        print(f"    WARN: probe (sample doc) failed: {exc}", file=sys.stderr)

    # --- count within the requested time window ---
    ts = result["ts_field"]
    try:
        r = session.post(
            f"{os_url}/{index}/_count",
            json={"query": {"range": {ts: {"gte": from_ms, "lte": to_ms,
                                           "format": "epoch_millis"}}}},
            timeout=10,
        )
        if r.ok:
            result["window_count"] = r.json().get("count", 0)
    except Exception as exc:
        print(f"    WARN: probe (window count) failed: {exc}", file=sys.stderr)

    return result


def _os_sample_container_names(session, os_url, index, from_ms, to_ms, ts_field, cname_field, n=30):
    """
    Return up to *n* distinct container_name values within the time window
    using a terms aggregation.  Used by --diagnose.
    """
    body = {
        "size": 0,
        "query": {"range": {ts_field: {"gte": from_ms, "lte": to_ms,
                                        "format": "epoch_millis"}}},
        "aggs": {
            "names": {
                "terms": {
                    "field": f"{cname_field}.keyword",
                    "size": n,
                }
            }
        },
    }
    try:
        r = session.post(f"{os_url}/{index}/_search", json=body, timeout=15)
        if r.ok:
            buckets = r.json().get("aggregations", {}).get("names", {}).get("buckets", [])
            return [(b["key"], b["doc_count"]) for b in buckets]
    except Exception:
        pass
    return []


def opensearch_diagnose(session, os_url, from_iso, to_iso):
    """
    Print a detailed diagnostic report about what is in OpenSearch.
    Called when --diagnose is passed.
    """
    print("\n" + "=" * 64)
    print("  OpenSearch Diagnostic Report")
    print("=" * 64)

    from_ms = int(datetime.fromisoformat(from_iso.replace("Z", "+00:00")).timestamp() * 1000)
    to_ms   = int(datetime.fromisoformat(to_iso.replace("Z", "+00:00")).timestamp() * 1000)

    # 1. List all indices
    print("\n[D1] All indices:")
    try:
        r = session.get(f"{os_url}/_cat/indices?h=index,docs.count,store.size&format=json",
                        timeout=10)
        r.raise_for_status()
        for idx in sorted(r.json(), key=lambda x: x["index"]):
            print(f"     {idx['index']:<45} docs={idx.get('docs.count','?'):>10}  "
                  f"size={idx.get('store.size','?')}")
    except Exception as exc:
        print(f"     ERROR: {exc}")

    index = _os_get_index(session, os_url)
    print(f"\n     → Using index(es): {index}")

    # 2. Probe
    probe = _os_probe(session, os_url, index, from_ms, to_ms)
    print("\n[D2] Detected field names:")
    print(f"     timestamp field    : {probe['ts_field']}")
    print(f"     container_name field: {probe['cname_field']}")
    print(f"\n[D3] Documents in requested time window: {probe['window_count']}")

    # 3. Sample document
    if probe["sample_doc"]:
        print("\n[D4] Sample document fields and values:")
        for k, v in sorted(probe["sample_doc"].items()):
            v_str = str(v)[:120]
            print(f"     {k:<35} = {v_str}")
    else:
        print("\n[D4] No sample document found (index may be empty).")

    # 4. Container names in window
    print("\n[D5] Distinct container_name values in time window (up to 30):")
    names = _os_sample_container_names(session, os_url, index,
                                        from_ms, to_ms,
                                        probe["ts_field"], probe["cname_field"])
    if names:
        for name, count in names:
            print(f"     {name:<60}  {count:>8} docs")
    else:
        print("     (none found – aggregation on .keyword sub-field may have failed)")
        print("      Trying match_all sample …")
        try:
            r = session.post(
                f"{os_url}/{index}/_search",
                json={"size": 5, "query": {"match_all": {}},
                      "_source": [probe["cname_field"]]},
                timeout=10,
            )
            if r.ok:
                for h in r.json().get("hits", {}).get("hits", []):
                    print(f"     {h.get('_source', {}).get(probe['cname_field'], '???')}")
        except Exception:
            pass

    print("\n" + "=" * 64)


def opensearch_fetch_all(session, os_url, container_name, source, from_iso, to_iso, out_path,
                         probe_cache=None, pod_name=None):
    """
    Fetch logs directly from OpenSearch using the scroll API.

    Discovers the actual timestamp and container-name field names via a
    one-time probe (cached in *probe_cache* dict across calls).
    Uses query_string wildcards for container matching so Docker Swarm
    names like 'simplyblock_WebAppAPI.1.<hash>' are matched by just
    passing 'WebAppAPI'.
    Returns number of lines written.
    """
    # Graylog's OpenSearch index maps the timestamp field with format
    # "uuuu-MM-dd HH:mm:ss.SSS" (space separator, no timezone suffix).
    # epoch_millis is accepted regardless of the field's stored date format.
    from_ms = int(datetime.fromisoformat(from_iso.replace("Z", "+00:00")).timestamp() * 1000)
    to_ms   = int(datetime.fromisoformat(to_iso.replace("Z", "+00:00")).timestamp() * 1000)

    # One-time index discovery + probe (cached)
    if probe_cache is None:
        probe_cache = {}
    if "index" not in probe_cache:
        probe_cache["index"] = _os_get_index(session, os_url)
        probe_cache["probe"] = _os_probe(session, os_url, probe_cache["index"], from_ms, to_ms)
        p = probe_cache["probe"]
        print(f"    [OpenSearch] index={probe_cache['index']}  "
              f"ts_field={p['ts_field']}  cname_field={p['cname_field']}  "
              f"docs_in_window={p['window_count']}")
        if p["window_count"] == 0:
            print("    WARN: no documents in the requested time window – "
                  "check the start_time / duration, or run with --diagnose",
                  file=sys.stderr)

    index  = probe_cache["index"]
    probe  = probe_cache["probe"]
    ts_f   = probe["ts_field"]
    cname_f = probe["cname_field"]

    # Build query
    # Use query_string wildcards so partial names work:
    #   "WebAppAPI"  matches "simplyblock_WebAppAPI.1.abc123"
    #   "spdk_8080"  matches "/spdk_8080"
    must_clauses: list[Any] = [
        {"range": {ts_f: {"gte": from_ms, "lte": to_ms, "format": "epoch_millis"}}},
    ]
    if container_name:
        esc = container_name.replace("/", "\\/").replace(":", "\\:")
        must_clauses.append({
            "query_string": {
                "default_field": cname_f,
                "query": f"*{esc}*",
                "analyze_wildcard": True,
            }
        })
    if pod_name:
        esc_pod = pod_name.replace("/", "\\/").replace(":", "\\:")
        must_clauses.append({
            "query_string": {
                "default_field": "kubernetes_pod_name",
                "query": f"*{esc_pod}*",
                "analyze_wildcard": True,
            }
        })
    if source:
        # source may be a single string or a list of candidate values
        # (e.g. multiple hostname formats for the same node).
        # When it is a list we OR them so any matching format succeeds.
        candidates = source if isinstance(source, (list, tuple)) else [source]
        if len(candidates) == 1:
            must_clauses.append({
                "query_string": {
                    "default_field": "source",
                    "query": f'"{candidates[0]}"',
                }
            })
        else:
            must_clauses.append({
                "bool": {
                    "should": [
                        {"query_string": {"default_field": "source",
                                          "query": f'"{c}"'}}
                        for c in candidates
                    ],
                    "minimum_should_match": 1,
                }
            })

    body = {
        "query": {"bool": {"must": must_clauses}},
        "sort": [{ts_f: {"order": "asc"}}],
        "size": PAGE_SIZE,
        "_source": [ts_f, "source", cname_f, "level", "message"],
    }

    init_url = f"{os_url}/{index}/_search?scroll=2m"
    written = 0

    try:
        r = session.post(init_url, json=body, timeout=60)
        if not r.ok:
            print(
                f"    WARN: OpenSearch initial scroll failed: {r.status_code} {r.reason}"
                f"\n          body: {r.text[:400]}",
                file=sys.stderr,
            )
            Path(out_path).touch()
            return 0
    except requests.RequestException as exc:
        print(f"    WARN: OpenSearch initial scroll failed: {exc}", file=sys.stderr)
        Path(out_path).touch()
        return 0

    data = r.json()
    scroll_id = data.get("_scroll_id")
    hits = data.get("hits", {}).get("hits", [])
    total = data.get("hits", {}).get("total", {})
    total = total.get("value", total) if isinstance(total, dict) else int(total or 0)
    print(f"    total entries: {total}")

    with open(out_path, "w") as fh:
        while hits:
            for h in hits:
                src = h.get("_source", {})
                # normalise field names to what _fmt expects
                if ts_f != "timestamp":
                    src["timestamp"] = src.get(ts_f, "")
                if cname_f != "container_name":
                    src["container_name"] = src.get(cname_f, "")
                fh.write(_fmt(src) + "\n")
                written += 1
            if len(hits) < PAGE_SIZE or not scroll_id:
                break
            try:
                sc_r = session.post(
                    f"{os_url}/_search/scroll",
                    json={"scroll": "2m", "scroll_id": scroll_id},
                    timeout=60,
                )
                sc_r.raise_for_status()
                sc_data = sc_r.json()
                scroll_id = sc_data.get("_scroll_id", scroll_id)
                hits = sc_data.get("hits", {}).get("hits", [])
            except requests.RequestException as exc:
                print(f"    WARN: scroll continuation failed: {exc}", file=sys.stderr)
                break

    # Release scroll context
    if scroll_id:
        try:
            session.delete(
                f"{os_url}/_search/scroll",
                json={"scroll_id": scroll_id},
                timeout=10,
            )
        except Exception:
            pass

    return written


# ---------------------------------------------------------------------------
# Dispatch helper
# ---------------------------------------------------------------------------


def fetch(
    *,
    gl_session,
    os_session,
    graylog_base,
    opensearch_base,
    use_opensearch,
    gl_query,
    os_container,
    os_source,
    from_iso,
    to_iso,
    out_path,
    probe_cache,
    os_pod_name=None,
):
    """Route to Graylog or OpenSearch depending on *use_opensearch*.

    When using Graylog and the recursive bisection still has truncated
    leaf windows, automatically falls back to OpenSearch scroll API
    (which has no offset limit) for the entire service.
    """
    if use_opensearch:
        return opensearch_fetch_all(
            os_session, opensearch_base,
            os_container, os_source,
            from_iso, to_iso, str(out_path),
            probe_cache=probe_cache,
            pod_name=os_pod_name,
        )

    written, truncated = graylog_fetch_all(
        gl_session, graylog_base,
        gl_query, from_iso, to_iso, str(out_path),
    )

    if truncated and opensearch_base:
        print(f"    NOTE: Graylog had {truncated} truncated window(s), "
              f"retrying via OpenSearch scroll API for complete data")
        try:
            os_written = opensearch_fetch_all(
                os_session, opensearch_base,
                os_container, os_source,
                from_iso, to_iso, str(out_path),
                probe_cache=probe_cache,
                pod_name=os_pod_name,
            )
            if os_written > 0:
                return os_written
            print(f"    WARN: OpenSearch fallback returned 0 lines, "
                  f"keeping Graylog result ({written} lines)")
        except Exception as exc:
            print(f"    WARN: OpenSearch fallback failed ({exc}), "
                  f"keeping Graylog result ({written} lines)",
                  file=sys.stderr)

    return written


# ---------------------------------------------------------------------------
# kubectl pod-log helpers
# ---------------------------------------------------------------------------


def _kubectl(*args, timeout=60) -> str:
    """Run kubectl with the given args and return stdout. Returns '' on failure."""
    try:
        r = subprocess.run(
            ["kubectl"] + list(args),
            capture_output=True, text=True, timeout=timeout,
        )
        return r.stdout
    except Exception as exc:
        print(f"    WARN: kubectl {' '.join(args[:4])} … failed: {exc}", file=sys.stderr)
        return ""


def _kubectl_list_pods(namespace: str, prefix: str) -> list[str]:
    """Return pod names in *namespace* whose name starts with *prefix*."""
    out = _kubectl("get", "pods", "-n", namespace,
                   "--no-headers", "-o", "custom-columns=:metadata.name")
    return [p for p in out.splitlines() if p.startswith(prefix)]


def _kubectl_containers(namespace: str, pod: str) -> list[str]:
    """Return init + regular container names for *pod*."""
    out = _kubectl(
        "get", "pod", pod, "-n", namespace,
        "-o",
        "jsonpath={range .spec.initContainers[*]}{.name}{'\\n'}{end}"
        "{range .spec.containers[*]}{.name}{'\\n'}{end}",
    )
    return [c for c in out.splitlines() if c]


def collect_k8s_pod_logs(namespace: str, pod: str, out_dir: Path,
                          from_iso: str, to_iso: str) -> None:
    """
    Write current + previous logs for every container in *pod* to *out_dir*.
    Files are named  <pod>_<container>.log
    """
    containers = _kubectl_containers(namespace, pod)
    for container in containers:
        log_file = out_dir / f"{pod}_{container}.log"
        print(f"      {pod} / {container}")
        with open(log_file, "w") as fh:
            fh.write(f"=== Pod: {pod} | Container: {container} | Namespace: {namespace} ===\n")
            fh.write(f"=== From: {from_iso} | Until: {to_iso} ===\n\n")

            fh.write("--- current logs ---\n")
            out = _kubectl("logs", pod, "-c", container, "-n", namespace,
                           "--timestamps", f"--since-time={from_iso}", timeout=120)
            # Trim lines beyond to_iso
            for line in out.splitlines():
                if line[:26] > to_iso[:26]:
                    break
                fh.write(line + "\n")

            fh.write("\n--- previous (last crash) logs ---\n")
            prev = _kubectl("logs", pod, "-c", container, "-n", namespace,
                            "--timestamps", "--previous", timeout=60)
            fh.write(prev if prev.strip() else "(no previous logs)\n")


def collect_k8s_csi_dmesg(namespace: str, pod: str, out_dir: Path,
                            from_iso: str, to_iso: str) -> None:
    """
    Collect dmesg from the csi-node container of a CSI pod,
    filtered to the requested time window using the kernel boot epoch.
    """
    from_epoch = int(datetime.fromisoformat(from_iso.replace("Z", "+00:00")).timestamp())
    to_epoch   = int(datetime.fromisoformat(to_iso.replace("Z", "+00:00")).timestamp())

    log_file = out_dir / f"{pod}_csi-node_dmesg.log"
    print(f"      {pod} / csi-node (dmesg)")

    # Derive boot epoch from /proc/uptime inside the container
    uptime_out = _kubectl("exec", pod, "-c", "csi-node", "-n", namespace,
                          "--", "cat", "/proc/uptime", timeout=10)
    try:
        boot_epoch = int(datetime.now(timezone.utc).timestamp()) - int(float(uptime_out.split()[0]))
    except Exception:
        boot_epoch = 0

    # Prefer human-readable reltime; fall back to monotonic seconds
    dmesg_out = _kubectl("exec", pod, "-c", "csi-node", "-n", namespace,
                         "--", "dmesg", "--kernel", "--time-format=reltime",
                         "--nopager", timeout=30)
    if not dmesg_out.strip():
        dmesg_out = _kubectl("exec", pod, "-c", "csi-node", "-n", namespace,
                             "--", "dmesg", "--kernel", "--nopager", timeout=30)
        # Filter by monotonic timestamp
        filtered = []
        import re
        for line in dmesg_out.splitlines():
            m = re.match(r'^\[\s*([0-9]+\.[0-9]+)\]', line)
            if m:
                wall = boot_epoch + int(float(m.group(1)))
                if wall < from_epoch:
                    continue
                if wall > to_epoch:
                    break
            filtered.append(line)
        dmesg_out = "\n".join(filtered)

    with open(log_file, "w") as fh:
        fh.write(f"=== Pod: {pod} | Container: csi-node | dmesg ===\n")
        fh.write(f"=== From: {from_iso} | Until: {to_iso} ===\n\n")
        fh.write(dmesg_out or "(no dmesg output)\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        prog="collect_logs.py",
        description="Collect simplyblock container logs for a given time window.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            '  collect_logs.py "2024-01-15T10:00:00" 60\n'
            '  collect_logs.py "2024-01-15 10:00:00" 30 --output-dir /tmp/logs\n'
            '  collect_logs.py "2024-01-15T10:00:00" 120 --use-opensearch\n'
            '  collect_logs.py "2024-01-15T10:00:00" 60 --mode kubernetes\n'
        ),
    )
    parser.add_argument(
        "start_time",
        help=(
            "Start of the collection window (UTC assumed if no timezone given). "
            'Formats: "2024-01-15T10:00:00"  or  "2024-01-15 10:00:00"'
        ),
    )
    parser.add_argument(
        "duration_minutes",
        type=int,
        help="Duration in minutes.",
    )
    parser.add_argument(
        "--output-dir",
        default=".",
        metavar="DIR",
        help="Directory to write the output tarball (default: current directory).",
    )
    parser.add_argument(
        "--mode",
        choices=["docker", "kubernetes"],
        default="docker",
        help=(
            "Deployment mode: 'docker' (default) uses Docker Swarm service names "
            "for control-plane log collection; 'kubernetes' uses Kubernetes container "
            "names and skips Graylog-based SPDK log collection (kubectl is used instead)."
        ),
    )
    parser.add_argument(
        "--use-opensearch",
        action="store_true",
        help=(
            "Query OpenSearch directly via scroll API instead of the Graylog "
            "REST API.  Useful for very large result sets or when Graylog is "
            "unreachable."
        ),
    )
    parser.add_argument(
        "--cluster-id",
        metavar="UUID",
        help="Target a specific cluster UUID (default: first cluster returned by sbctl).",
    )
    parser.add_argument(
        "--mgmt-ip",
        metavar="IP",
        help="Override the management-node IP used to reach Graylog / OpenSearch.",
    )
    parser.add_argument(
        "--monitoring-secret",
        metavar="SECRET",
        help=(
            "Graylog / OpenSearch password to use instead of the cluster secret. "
            "When provided this takes precedence over the cluster secret."
        ),
    )
    parser.add_argument(
        "--namespace",
        default="simplyblock",
        metavar="NS",
        help=(
            "Kubernetes namespace to collect CSI / storage-node DS pod logs from "
            "(default: simplyblock).  Pass an empty string to skip kubectl collection."
        ),
    )
    parser.add_argument(
        "--diagnose",
        action="store_true",
        help=(
            "Print a diagnostic report from OpenSearch (indices, field names, "
            "sample documents, container names present in the time window) and "
            "exit without collecting logs.  Use this when collections return 0 "
            "to understand the actual data layout.  Implies --use-opensearch."
        ),
    )
    args = parser.parse_args()
    if args.diagnose:
        args.use_opensearch = True

    # ── 1. Parse time range ──────────────────────────────────────────────────

    try:
        start_dt = datetime.fromisoformat(args.start_time.replace(" ", "T"))
    except ValueError as exc:
        print(f"ERROR: invalid start_time – {exc}", file=sys.stderr)
        sys.exit(1)

    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=timezone.utc)

    end_dt = start_dt + timedelta(minutes=args.duration_minutes)
    from_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    to_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    print("=" * 64)
    print("  Simplyblock Log Collector")
    print("=" * 64)
    print(f"  Window : {from_iso}  →  {to_iso}  ({args.duration_minutes} min)")
    print(f"  Deploy : {args.mode}")
    print(f"  Mode   : {'OpenSearch (direct)' if args.use_opensearch else 'Graylog REST API'}")

    # ── 2. Cluster UUID + secret ─────────────────────────────────────────────

    print("\n[1] Retrieving cluster info …")
    cluster_uuid = args.cluster_id
    if not cluster_uuid:
        clusters = sbctl_json("cluster", "list")
        if not clusters:
            print("ERROR: 'sbctl cluster list' returned nothing.", file=sys.stderr)
            sys.exit(1)
        cluster_uuid = clusters[0]["UUID"]

    print(f"    Cluster UUID : {cluster_uuid}")

    cluster_secret = sbctl_raw("cluster", "get-secret", cluster_uuid)
    if not cluster_secret:
        print("ERROR: could not retrieve cluster secret.", file=sys.stderr)
        sys.exit(1)
    print(f"    Secret       : {'*' * min(len(cluster_secret), 8)}…  (len={len(cluster_secret)})")

    # ── 3. Management-node IP ────────────────────────────────────────────────

    print("\n[2] Resolving management node …")
    if args.mgmt_ip:
        mgmt_ip = args.mgmt_ip
        print(f"    Using provided IP : {mgmt_ip}")
    else:
        cp_nodes = sbctl_json("control-plane", "list")
        if not cp_nodes:
            print("ERROR: 'sbctl control-plane list' returned nothing.", file=sys.stderr)
            sys.exit(1)
        mgmt_ip = cp_nodes[0]["IP"]
        print(f"    Management IP : {mgmt_ip}  ({len(cp_nodes)} node(s) total)")

    if args.mode == "kubernetes":
        graylog_base = f"http://{mgmt_ip}:9000/api"
        opensearch_base = f"http://{mgmt_ip}:9200"
    else:
        graylog_base = f"http://{mgmt_ip}/graylog/api"
        opensearch_base = f"http://{mgmt_ip}/opensearch"

    # ── 4. Storage nodes ─────────────────────────────────────────────────────

    print("\n[3] Retrieving storage nodes …")
    sn_list = sbctl_json("storage-node", "list") or []
    if not sn_list:
        print("    WARN: no storage nodes found (continuing without them).")
    else:
        print(f"    Found {len(sn_list)} storage node(s).")

    # ── 5. HTTP sessions ─────────────────────────────────────────────────────

    graylog_password = args.monitoring_secret if args.monitoring_secret else cluster_secret
    if args.monitoring_secret:
        print("    Using provided --monitoring-secret for Graylog auth.")

    gl_session = requests.Session()
    gl_session.auth = ("admin", graylog_password)
    gl_session.headers.update({"X-Requested-By": "sb-log-collector"})

    os_session = requests.Session()

    # Verify Graylog reachability (informational only)
    if not args.use_opensearch:
        print(f"\n[4] Checking Graylog at {graylog_base} …")
        try:
            r = gl_session.get(f"{graylog_base}/system", timeout=10)
            if r.status_code == 200:
                ver = r.json().get("version", "?")
                print(f"    OK  (version {ver})")
            else:
                print(f"    WARN: HTTP {r.status_code} – will still attempt collection.")
        except requests.RequestException as exc:
            print(f"    WARN: {exc} – will still attempt collection.")
    else:
        print(f"\n[4] Checking OpenSearch at {opensearch_base} …")
        try:
            r = os_session.get(f"{opensearch_base}/_cluster/health", timeout=10)
            if r.status_code == 200:
                status = r.json().get("status", "?")
                print(f"    OK  (cluster status: {status})")
            else:
                print(f"    WARN: HTTP {r.status_code}.")
        except requests.RequestException as exc:
            print(f"    WARN: {exc}.")

        # --diagnose: print full report and exit
        if args.diagnose:
            opensearch_diagnose(os_session, opensearch_base, from_iso, to_iso)
            sys.exit(0)

    # ── 6. Prepare temp workspace ────────────────────────────────────────────

    ts_str = start_dt.strftime("%Y%m%d_%H%M%S")
    bundle_name = f"sb_logs_{ts_str}_{args.duration_minutes}m"
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    tarball_path = output_dir / f"{bundle_name}.tar.gz"

    probe_cache: dict = {}   # shared across all OpenSearch calls in this run

    fetch_kw = dict(
        gl_session=gl_session,
        os_session=os_session,
        graylog_base=graylog_base,
        opensearch_base=opensearch_base,
        use_opensearch=args.use_opensearch,
        from_iso=from_iso,
        to_iso=to_iso,
        probe_cache=probe_cache,
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        log_root = Path(tmpdir) / bundle_name
        log_root.mkdir()

        # ── 7. Control-plane logs ────────────────────────────────────────────

        cp_services = (
            CONTROL_PLANE_SERVICES_KUBERNETES
            if args.mode == "kubernetes"
            else CONTROL_PLANE_SERVICES_DOCKER
        )
        print(f"\n[5] Collecting control-plane logs ({len(cp_services)} services, mode={args.mode}) …")
        cp_dir = log_root / "control_plane"
        cp_dir.mkdir()

        gl_cname_field = "kubernetes_container_name" if args.mode == "kubernetes" else "container_name"

        total_cp_lines = 0
        for svc in cp_services:
            out_f = cp_dir / f"{svc}.log"
            gl_q = f'{gl_cname_field}:/.*{_gl_escape(svc)}.*/'
            n = fetch(
                gl_query=gl_q,
                os_container=svc,
                os_source=None,
                out_path=out_f,
                **fetch_kw,
            )
            total_cp_lines += n
            status = f"{n:>8,} lines"
            print(f"  {svc:<42} {status}")

        print(f"  {'Control-plane total':<42} {total_cp_lines:>8,} lines")

        # ── 8. Storage-node logs ─────────────────────────────────────────────
        # Docker mode: collect SPDK/SNodeAPI logs from Graylog/OpenSearch.
        # Kubernetes mode: SPDK logs are captured via kubectl in step 9.

        if args.mode == "docker":
            print("\n[6] Collecting storage-node logs (docker) …")
            sn_root = log_root / "storage_nodes"
            sn_root.mkdir()

            # SNodeAPI runs on every storage node under the same container name.
            # Its GELF 'source' field is the Docker host hostname whose exact
            # format varies by deployment and cannot be reliably derived from
            # the management IP alone.  Collect ALL SNodeAPI logs once (no
            # source filter) into a shared file; each line contains src=<host>
            # so per-node filtering can be done with grep afterwards.
            print("\n  SNodeAPI (all nodes combined) …")
            snode_api_log = sn_root / "SNodeAPI_all_nodes.log"
            snode_api_count = fetch(
                gl_query='container_name:"SNodeAPI"',
                os_container="SNodeAPI",
                os_source=None,
                out_path=snode_api_log,
                **fetch_kw,
            )
            print(f"  {'SNodeAPI (all nodes)':<42} {snode_api_count:>8,} lines")
            print("  (filter by src=<ip> to isolate per-node logs)")

            for node in sn_list:
                hostname = node.get("Hostname", "unknown")
                node_ip = node.get("Management IP", "")
                rpc_port = node.get("SPDK P", 8080)

                node_label = f"{hostname}_{node_ip}".strip("_") if node_ip else hostname
                node_dir = sn_root / node_label
                node_dir.mkdir()

                print(f"\n  Node: {hostname}  ip={node_ip}  rpc_port={rpc_port}")

                # spdk_N and spdk_proxy_N are globally unique by RPC port number;
                # no source filter needed.
                spdk_containers = [
                    (f"spdk_{rpc_port}",       f"spdk_{rpc_port}.log"),
                    (f"spdk_proxy_{rpc_port}", f"spdk_proxy_{rpc_port}.log"),
                ]

                for cname, fname in spdk_containers:
                    out_f = node_dir / fname
                    n = fetch(
                        gl_query=f'container_name:"{cname}"',
                        os_container=cname,
                        os_source=None,
                        out_path=out_f,
                        **fetch_kw,
                    )
                    print(f"    {cname:<42} {n:>8,} lines")
        else:
            print("\n[6] Collecting storage-node logs (kubernetes) …")
            sn_root = log_root / "storage_nodes"
            sn_root.mkdir()

            for node in sn_list:
                hostname = node.get("Hostname", "unknown")
                node_ip = node.get("Management IP", "")
                rpc_port = node.get("SPDK P", 8080)

                node_label = f"{hostname}_{node_ip}".strip("_") if node_ip else hostname
                node_dir = sn_root / node_label
                node_dir.mkdir()

                print(f"\n  Node: {hostname}  ip={node_ip}  rpc_port={rpc_port}")

                # Pod name pattern: snode-spdk-pod-<rpc_port>-<cluster_uuid>
                # Container names inside that pod: spdk-container, spdk-proxy-container
                pod_name = f"snode-spdk-pod-{rpc_port}-*"
                spdk_containers = [
                    ("spdk-container",       f"spdk-container_{rpc_port}.log"),
                    ("spdk-proxy-container", f"spdk-proxy-container_{rpc_port}.log"),
                ]

                for cname, fname in spdk_containers:
                    out_f = node_dir / fname
                    gl_q = (
                        f'kubernetes_pod_name:{_gl_escape(pod_name)} '
                        f'AND kubernetes_container_name:/.*{_gl_escape(cname)}.*/'
                    )
                    n = fetch(
                        gl_query=gl_q,
                        os_container=cname,
                        os_source=None,
                        os_pod_name=pod_name,
                        out_path=out_f,
                        **fetch_kw,
                    )
                    print(f"    {cname:<42} {n:>8,} lines")

        # ── 9. Kubernetes pod logs (CSI node + storage-node DS) ──────────────

        k8s_ns = args.namespace
        if k8s_ns:
            print(f"\n[7] Collecting Kubernetes pod logs (namespace: {k8s_ns}) …")
            k8s_dir = log_root / "k8s_pods"
            k8s_dir.mkdir()

            # 9a. simplyblock-csi-node* pods — all containers + dmesg
            csi_pods = _kubectl_list_pods(k8s_ns, "simplyblock-csi-node")
            if csi_pods:
                csi_dir = k8s_dir / "csi-node"
                csi_dir.mkdir()
                print(f"  CSI node pods ({len(csi_pods)}) …")
                for pod in csi_pods:
                    collect_k8s_pod_logs(k8s_ns, pod, csi_dir, from_iso, to_iso)
                    collect_k8s_csi_dmesg(k8s_ns, pod, csi_dir, from_iso, to_iso)
            else:
                print(f"  No simplyblock-csi-node pods found in namespace {k8s_ns}.")

            # 9b. simplyblock-csi-controller* pods — all containers
            csi_ctrl_pods = _kubectl_list_pods(k8s_ns, "simplyblock-csi-controller")
            if csi_ctrl_pods:
                csi_ctrl_dir = k8s_dir / "csi-controller"
                csi_ctrl_dir.mkdir()
                print(f"  CSI controller pods ({len(csi_ctrl_pods)}) …")
                for pod in csi_ctrl_pods:
                    collect_k8s_pod_logs(k8s_ns, pod, csi_ctrl_dir, from_iso, to_iso)
            else:
                print(f"  No simplyblock-csi-controller pods found in namespace {k8s_ns}.")

            # 9c. simplyblock-manager* pods — all containers
            mgr_pods = _kubectl_list_pods(k8s_ns, "simplyblock-manager")
            if mgr_pods:
                mgr_dir = k8s_dir / "simplyblock-manager"
                mgr_dir.mkdir()
                print(f"  Simplyblock manager pods ({len(mgr_pods)}) …")
                for pod in mgr_pods:
                    collect_k8s_pod_logs(k8s_ns, pod, mgr_dir, from_iso, to_iso)
            else:
                print(f"  No simplyblock-manager pods found in namespace {k8s_ns}.")

            # 9d. simplyblock-storage-node-ds* pods — all containers
            sn_ds_pods = _kubectl_list_pods(k8s_ns, "simplyblock-storage-node-ds")
            if sn_ds_pods:
                sn_ds_dir = k8s_dir / "storage-node-ds"
                sn_ds_dir.mkdir()
                print(f"  Storage-node DS pods ({len(sn_ds_pods)}) …")
                for pod in sn_ds_pods:
                    collect_k8s_pod_logs(k8s_ns, pod, sn_ds_dir, from_iso, to_iso)
            else:
                print(f"  No simplyblock-storage-node-ds pods found in namespace {k8s_ns}.")
        else:
            print("\n[7] Skipping Kubernetes pod logs (--namespace not set).")

        # ── 10. sbctl cluster / node snapshots ───────────────────────────────

        print("\n[8] Collecting sbctl cluster / node info …")
        info_dir = log_root / "sbctl_info"
        info_dir.mkdir()

        def save_sbctl(label, cmd_args, out_name, use_json=False):
            """Run sbctl, save output to out_name, print status."""
            if use_json:
                data = sbctl_json(*cmd_args)
                if data is not None:
                    out_path = info_dir / out_name
                    with open(out_path, "w") as f:
                        json.dump(data, f, indent=2)
                    print(f"  {label:<50} OK  ({out_name})")
                    return True
            else:
                text = sbctl_raw(*cmd_args)
                if text is not None:
                    out_path = info_dir / out_name
                    out_path.write_text(text)
                    print(f"  {label:<50} OK  ({out_name})")
                    return True
            print(f"  {label:<50} FAILED", file=sys.stderr)
            return False

        # 1. cluster show
        save_sbctl(
            "sbctl cluster show",
            ["cluster", "show", cluster_uuid],
            "cluster_show.txt",
        )

        # 2. lvol list
        save_sbctl(
            "sbctl lvol list",
            ["lvol", "list", "--cluster-id", cluster_uuid],
            "lvol_list.json",
            use_json=True,
        )

        # 3. sn list (already fetched; save the raw JSON for completeness)
        save_sbctl(
            "sbctl sn list",
            ["sn", "list"],
            "sn_list.json",
            use_json=True,
        )

        # 4. sn check <node_uuid>  – one file per storage node
        print("  sbctl sn check  (per node) …")
        sn_check_dir = info_dir / "sn_check"
        sn_check_dir.mkdir()
        for node in sn_list:
            node_uuid = node.get("UUID", "")
            node_hostname = node.get("Hostname", node_uuid)
            node_ip = node.get("Management IP", "")
            label = f"{node_hostname}_{node_ip}".strip("_") if node_ip else node_hostname
            text = sbctl_raw("sn", "check", node_uuid)
            if text is not None:
                (sn_check_dir / f"{label}.txt").write_text(text)
                print(f"    {label}")
            else:
                print(f"    {label}  FAILED", file=sys.stderr)

        # 5. cluster get-logs --limit 0  (all cluster-level events)
        save_sbctl(
            "sbctl cluster get-logs --limit 0",
            ["cluster", "get-logs", cluster_uuid, "--limit", "0"],
            "cluster_get_logs.txt",
        )

        # ── 11. Write a collection manifest ──────────────────────────────────

        manifest = {
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "window_from": from_iso,
            "window_to": to_iso,
            "duration_minutes": args.duration_minutes,
            "cluster_uuid": cluster_uuid,
            "mgmt_ip": mgmt_ip,
            "deploy_mode": args.mode,
            "log_source": "opensearch-direct" if args.use_opensearch else "graylog-api",
            "storage_nodes": [
                {
                    "hostname": n.get("Hostname"),
                    "ip": n.get("Management IP"),
                    "rpc_port": n.get("SPDK P"),
                    "uuid": n.get("UUID"),
                }
                for n in sn_list
            ],
        }
        with open(log_root / "manifest.json", "w") as mf:
            json.dump(manifest, mf, indent=2)

        # ── 12. Pack into tarball ─────────────────────────────────────────────

        print("\n[9] Creating tarball …")
        with tarfile.open(str(tarball_path), "w:gz") as tar:
            tar.add(str(log_root), arcname=bundle_name)

        size_mb = tarball_path.stat().st_size / 1_048_576
        print(f"\n{'=' * 64}")
        print("  Done!")
        print(f"  Tarball : {tarball_path}")
        print(f"  Size    : {size_mb:.2f} MB")
        print(f"{'=' * 64}\n")


if __name__ == "__main__":
    main()
