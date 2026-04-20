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
"""

import argparse
import json
import subprocess
import sys
import tarfile
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path

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
CONTROL_PLANE_SERVICES = [
    "WebAppAPI",
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
    "TasksRunnerPortAllow",
    "TasksRunnerJCCompResume",
    "TasksRunnerLVolSyncDelete",
    "TasksRunnerBackup",
    "TasksRunnerBackupMerge",
    "HAProxy",
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
# SSH + per-host helpers (for --include-node-docker-logs, --include-client-dmesg)
# ---------------------------------------------------------------------------


def ssh_exec(host: str, user: str, key: str, command: str, timeout: int = 120) -> tuple[int, str, str]:
    """Run *command* on *host* via ssh. Returns (rc, stdout, stderr)."""
    argv = [
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=15",
        "-o", "ServerAliveInterval=15",
        "-o", "ServerAliveCountMax=3",
        "-i", key,
        f"{user}@{host}",
        f"bash -lc {json.dumps(command)}",
    ]
    try:
        proc = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        return 124, exc.stdout or "", exc.stderr or "timeout"
    return proc.returncode, proc.stdout, proc.stderr


def docker_logs_for_window(host: str, user: str, key: str, container: str,
                           from_iso: str, to_iso: str, out_path: Path) -> int:
    """Capture `docker logs <container>` for the given UTC window. Returns line count."""
    # --since/--until accept RFC3339. Graylog ISOs already look like
    # 2024-01-15T10:00:00.000Z which docker accepts.
    cmd = (
        f"sudo docker logs --timestamps "
        f"--since {from_iso} --until {to_iso} {container} 2>&1 || true"
    )
    rc, out, err = ssh_exec(host, user, key, cmd, timeout=300)
    out_path.write_text(out, encoding="utf-8", errors="replace")
    # rc is best-effort; docker may return non-zero if the container died but
    # still dumped logs. We keep whatever it produced.
    return out.count("\n")


def files_overlapping_window(directory: Path, patterns: tuple,
                             from_dt: datetime, to_dt: datetime, slack_s: int = 7200) -> list[Path]:
    """Return files in *directory* matching *patterns* whose mtime falls inside
    [from_dt - slack, to_dt + slack].  A generous slack catches the run that
    started before the window and is still being appended to after it."""
    if not directory.is_dir():
        return []
    window_start = from_dt.timestamp() - slack_s
    window_end = to_dt.timestamp() + slack_s
    matched: list[Path] = []
    for pattern in patterns:
        for p in directory.glob(pattern):
            if not p.is_file():
                continue
            mtime = p.stat().st_mtime
            if window_start <= mtime <= window_end:
                matched.append(p)
    return sorted(set(matched))


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
        resp = session.get(search_url, params=params, timeout=90)
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"    WARN: Graylog page request failed (offset={offset}): {exc}", file=sys.stderr)
        return None, 0

    data = resp.json()
    return data.get("messages", []), data.get("total_results", 0)


def _gl_write_window(session, search_url, query, from_iso, to_iso, fh):
    """
    Paginate through a single time window and write lines to *fh*.
    Returns number of lines written.
    """
    written = 0
    offset = 0

    # Probe total size first
    msgs, total = _gl_search_page(session, search_url, query, from_iso, to_iso, 1, 0)
    if msgs is None:
        return 0

    while offset < total:
        msgs, _ = _gl_search_page(
            session, search_url, query, from_iso, to_iso, PAGE_SIZE, offset
        )
        if not msgs:
            break
        for m in msgs:
            fh.write(_fmt(m.get("message", {})) + "\n")
            written += 1
        offset += len(msgs)
        if len(msgs) < PAGE_SIZE:
            break

    return written


def graylog_fetch_all(session, base_url, query, from_iso, to_iso, out_path):
    """
    Download all log messages matching *query* within [from_iso, to_iso].

    Strategy:
      1. Probe total_results.
      2. If <= MAX_RESULT_WINDOW  → straightforward offset pagination.
      3. If >  MAX_RESULT_WINDOW  → split into 10-minute sub-windows and
                                    paginate each one independently.

    Writes one text line per message to *out_path*.
    Returns number of lines written.
    """
    search_url = f"{base_url}/search/universal/absolute"
    written = 0

    # Probe
    msgs, total = _gl_search_page(session, search_url, query, from_iso, to_iso, 1, 0)
    if msgs is None:
        Path(out_path).touch()
        return 0

    print(f"    total entries: {total}")

    with open(out_path, "w") as fh:
        if total <= MAX_RESULT_WINDOW:
            written = _gl_write_window(session, search_url, query, from_iso, to_iso, fh)
        else:
            # Split into 10-minute chunks to stay under max_result_window
            print("    NOTE: >100 k entries – collecting via 10-minute sub-windows")
            t = datetime.fromisoformat(from_iso.replace("Z", "+00:00"))
            t_end = datetime.fromisoformat(to_iso.replace("Z", "+00:00"))
            chunk = timedelta(minutes=10)
            while t < t_end:
                chunk_end = min(t + chunk, t_end)
                c_from = t.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                c_to = chunk_end.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                written += _gl_write_window(
                    session, search_url, query, c_from, c_to, fh
                )
                t = chunk_end

    return written


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
                for candidate in ("container_name", "container_id", "containerName",
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
                         probe_cache=None):
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
    must_clauses = [
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
):
    """Route to Graylog or OpenSearch depending on *use_opensearch*."""
    if use_opensearch:
        return opensearch_fetch_all(
            os_session, opensearch_base,
            os_container, os_source,
            from_iso, to_iso, str(out_path),
            probe_cache=probe_cache,
        )
    return graylog_fetch_all(
        gl_session, graylog_base,
        gl_query, from_iso, to_iso, str(out_path),
    )


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
        "--diagnose",
        action="store_true",
        help=(
            "Print a diagnostic report from OpenSearch (indices, field names, "
            "sample documents, container names present in the time window) and "
            "exit without collecting logs.  Use this when collections return 0 "
            "to understand the actual data layout.  Implies --use-opensearch."
        ),
    )
    parser.add_argument(
        "--include-node-docker-logs",
        action="store_true",
        help=(
            "SSH to each storage node and capture `docker logs SNodeAPI` for "
            "the requested time window (supplements the Graylog-based "
            "collection with raw container output)."
        ),
    )
    parser.add_argument(
        "--node-ssh-user",
        default="ec2-user",
        help="SSH user for storage-node docker-logs collection (default ec2-user).",
    )
    parser.add_argument(
        "--node-ssh-key",
        metavar="PATH",
        help="SSH private key for storage-node docker-logs collection.",
    )
    parser.add_argument(
        "--include-soak-logs",
        action="store_true",
        help=(
            "Include soak test stdout/log files (*.log, *.out) whose mtime "
            "overlaps the requested window, from --soak-logs-dir."
        ),
    )
    parser.add_argument(
        "--soak-logs-dir",
        metavar="DIR",
        default=str(Path.home() / "perf"),
        help="Directory to scan for soak *.log/*.out files (default: ~/perf).",
    )
    parser.add_argument(
        "--include-client-dmesg",
        action="store_true",
        help=(
            "SSH to each client and collect dmesg + a persistent dmesg log "
            "(/var/log/sb-dmesg.log if present) + journalctl -k for the "
            "window.  NOTE: full coverage requires the soak script to run "
            "`nohup sudo dmesg -Tw >> /var/log/sb-dmesg.log &` at start so "
            "the kernel ring buffer doesn't rotate the incident out."
        ),
    )
    parser.add_argument(
        "--metadata",
        metavar="PATH",
        help=(
            "Cluster metadata JSON (e.g. cluster_metadata_base.json) used to "
            "auto-fill client IPs and the SSH key path for client/node "
            "collections."
        ),
    )
    parser.add_argument(
        "--client-ssh-user",
        default="ec2-user",
        help="SSH user for client dmesg collection (default ec2-user).",
    )
    parser.add_argument(
        "--client-ssh-key",
        metavar="PATH",
        help="SSH private key for client dmesg collection.",
    )
    args = parser.parse_args()
    if args.diagnose:
        args.use_opensearch = True

    # ── 0. Metadata auto-fill ───────────────────────────────────────────────
    metadata_clients: list[dict] = []
    if args.metadata:
        with open(args.metadata, "r", encoding="utf-8") as fh:
            md = json.load(fh)
        metadata_clients = md.get("clients") or []
        if not args.node_ssh_key:
            args.node_ssh_key = md.get("key_path") or None
        if not args.client_ssh_key:
            args.client_ssh_key = md.get("key_path") or None
        if md.get("user") and args.client_ssh_user == "ec2-user":
            args.client_ssh_user = md["user"]
        if md.get("user") and args.node_ssh_user == "ec2-user":
            args.node_ssh_user = md["user"]

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

    gl_session = requests.Session()
    gl_session.auth = ("admin", cluster_secret)
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

        print(f"\n[5] Collecting control-plane logs ({len(CONTROL_PLANE_SERVICES)} services) …")
        cp_dir = log_root / "control_plane"
        cp_dir.mkdir()

        total_cp_lines = 0
        for svc in CONTROL_PLANE_SERVICES:
            out_f = cp_dir / f"{svc}.log"
            # Graylog Lucene query – no source filter (services are globally unique)
            gl_q = f'container_name:"{svc}"'
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

        print("\n[6] Collecting storage-node logs …")
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

        # ── 8b. SNodeAPI per-node docker logs (optional, via SSH) ────────────
        if args.include_node_docker_logs:
            print("\n[6b] Collecting SNodeAPI docker logs per storage node (ssh) …")
            if not args.node_ssh_key:
                print("  SKIP: --node-ssh-key not set (and no key_path in --metadata).")
            else:
                for node in sn_list:
                    hostname = node.get("Hostname", "unknown")
                    node_ip = node.get("Management IP", "")
                    if not node_ip:
                        print(f"  SKIP {hostname}: no Management IP")
                        continue
                    node_label = f"{hostname}_{node_ip}".strip("_") if node_ip else hostname
                    node_dir = sn_root / node_label
                    node_dir.mkdir(exist_ok=True)
                    out_f = node_dir / "SNodeAPI_docker.log"
                    print(f"  ssh {args.node_ssh_user}@{node_ip}: docker logs SNodeAPI --since {from_iso} --until {to_iso}")
                    try:
                        n = docker_logs_for_window(
                            node_ip, args.node_ssh_user, args.node_ssh_key,
                            "SNodeAPI", from_iso, to_iso, out_f,
                        )
                        print(f"    {'SNodeAPI (docker)':<42} {n:>8,} lines -> {out_f.name}")
                    except Exception as exc:
                        print(f"    WARN: {exc}", file=sys.stderr)

        # ── 8c. Client dmesg (optional, via SSH) ─────────────────────────────
        if args.include_client_dmesg:
            print("\n[6c] Collecting client dmesg / journalctl -k …")
            if not args.client_ssh_key:
                print("  SKIP: --client-ssh-key not set (and no key_path in --metadata).")
            elif not metadata_clients:
                print("  SKIP: no clients in --metadata JSON.")
            else:
                client_dir = log_root / "clients"
                client_dir.mkdir(exist_ok=True)
                for c in metadata_clients:
                    host = c.get("public_ip") or c.get("private_ip")
                    if not host:
                        print(f"  SKIP client without IP: {c}")
                        continue
                    per = client_dir / host.replace(".", "_")
                    per.mkdir(exist_ok=True)
                    # 1. Persistent dmesg log written by the soak script (if any)
                    rc, out, _ = ssh_exec(
                        host, args.client_ssh_user, args.client_ssh_key,
                        "sudo cat /var/log/sb-dmesg.log 2>/dev/null || true",
                        timeout=180,
                    )
                    (per / "sb-dmesg.log").write_text(out, encoding="utf-8", errors="replace")
                    if not out:
                        print(f"  {host}: /var/log/sb-dmesg.log missing or empty "
                              f"(soak script must run `nohup sudo dmesg -Tw >> /var/log/sb-dmesg.log &` at start)")
                    # 2. Current kernel ring buffer snapshot (may have rotated)
                    _, out, _ = ssh_exec(
                        host, args.client_ssh_user, args.client_ssh_key,
                        "sudo dmesg -T 2>&1 || true",
                    )
                    (per / "dmesg_current.log").write_text(out, encoding="utf-8", errors="replace")
                    # 3. journalctl -k for the window (often has longer retention)
                    cmd = (
                        f"sudo journalctl -k --no-pager --since {json.dumps(from_iso)} "
                        f"--until {json.dumps(to_iso)} 2>&1 || true"
                    )
                    _, out, _ = ssh_exec(host, args.client_ssh_user, args.client_ssh_key, cmd, timeout=180)
                    (per / "journalctl_k.log").write_text(out, encoding="utf-8", errors="replace")
                    print(f"  {host}: sb-dmesg / dmesg_current / journalctl_k saved under clients/{per.name}/")

        # ── 8d. Soak test stdout/log files (optional, local copy) ────────────
        if args.include_soak_logs:
            print(f"\n[6d] Collecting soak *.log/*.out from {args.soak_logs_dir} …")
            soak_src = Path(args.soak_logs_dir).expanduser()
            matched = files_overlapping_window(
                soak_src, ("*.log", "*.out"), start_dt, end_dt,
            )
            if not matched:
                print("  (no files overlap the time window)")
            else:
                soak_dst = log_root / "soak_scripts"
                soak_dst.mkdir(exist_ok=True)
                import shutil as _sh
                for p in matched:
                    _sh.copy2(p, soak_dst / p.name)
                    print(f"  copied {p.name} ({p.stat().st_size} bytes)")

        # ── 9. sbctl cluster / node snapshots ────────────────────────────────

        print("\n[7] Collecting sbctl cluster / node info …")
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
            "mode": "opensearch-direct" if args.use_opensearch else "graylog-api",
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

        print("\n[8] Creating tarball …")
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
