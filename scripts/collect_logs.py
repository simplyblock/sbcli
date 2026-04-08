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


def _os_term(field, value):
    """
    Build an exact-match clause that works regardless of whether the field is
    mapped as 'keyword' or 'text+keyword'.  Tries fieldname.keyword first;
    OpenSearch ignores unknown sub-fields gracefully via a should/filter combo.
    """
    return {
        "bool": {
            "should": [
                {"term": {f"{field}.keyword": value}},
                {"term": {field: value}},
            ],
            "minimum_should_match": 1,
        }
    }


def opensearch_fetch_all(session, os_url, container_name, source, from_iso, to_iso, out_path):
    """
    Fetch logs directly from OpenSearch using the scroll API.

    Builds a bool/must query filtering by container_name (and optionally
    source) within the requested time range.  Scrolls through all hits.
    Returns number of lines written.
    """
    must_clauses = [
        {"range": {"timestamp": {"gte": from_iso, "lte": to_iso}}},
    ]
    if container_name:
        # Docker may prepend a leading "/" to the container name; match both.
        must_clauses.append({
            "bool": {
                "should": [
                    _os_term("container_name", container_name),
                    _os_term("container_name", f"/{container_name}"),
                ],
                "minimum_should_match": 1,
            }
        })
    if source:
        must_clauses.append(_os_term("source", source))

    body = {
        "query": {"bool": {"must": must_clauses}},
        "sort": [{"timestamp": {"order": "asc"}}],
        "size": PAGE_SIZE,
        "_source": ["timestamp", "source", "container_name", "level", "message"],
    }

    # Discover actual index names – avoids wildcard in URL path (HAProxy rejects it)
    index = _os_get_index(session, os_url)
    init_url = f"{os_url}/{index}/_search?scroll=2m"
    written = 0

    try:
        r = session.post(init_url, json=body, timeout=60)
        if not r.ok:
            print(
                f"    WARN: OpenSearch initial scroll failed: {r.status_code} {r.reason}"
                f"\n          body: {r.text[:300]}",
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
                fh.write(_fmt(h.get("_source", {})) + "\n")
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
):
    """Route to Graylog or OpenSearch depending on *use_opensearch*."""
    if use_opensearch:
        return opensearch_fetch_all(
            os_session, opensearch_base,
            os_container, os_source,
            from_iso, to_iso, str(out_path),
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
    args = parser.parse_args()

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

    # ── 6. Prepare temp workspace ────────────────────────────────────────────

    ts_str = start_dt.strftime("%Y%m%d_%H%M%S")
    bundle_name = f"sb_logs_{ts_str}_{args.duration_minutes}m"
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    tarball_path = output_dir / f"{bundle_name}.tar.gz"

    fetch_kw = dict(
        gl_session=gl_session,
        os_session=os_session,
        graylog_base=graylog_base,
        opensearch_base=opensearch_base,
        use_opensearch=args.use_opensearch,
        from_iso=from_iso,
        to_iso=to_iso,
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

        print(f"\n[6] Collecting storage-node logs …")
        sn_root = log_root / "storage_nodes"
        sn_root.mkdir()

        for node in sn_list:
            hostname = node.get("Hostname", "unknown")
            node_ip = node.get("Management IP", "")
            rpc_port = node.get("SPDK P", 8080)

            # The GELF 'source' field is the Docker host's address.
            # We prefer the IP; fall back to the hostname.
            source_id = node_ip or hostname
            node_label = f"{hostname}_{node_ip}".strip("_") if node_ip else hostname
            node_dir = sn_root / node_label
            node_dir.mkdir()

            print(f"\n  Node: {hostname}  ip={node_ip}  rpc_port={rpc_port}")

            # Per-node containers:
            #   spdk_{rpc_port}        – SPDK process
            #   spdk_proxy_{rpc_port}  – SPDK HTTP proxy
            #   SNodeAPI               – storage node API (firewall / management)
            snode_containers = [
                (f"spdk_{rpc_port}",       f"spdk_{rpc_port}.log"),
                (f"spdk_proxy_{rpc_port}", f"spdk_proxy_{rpc_port}.log"),
                ("SNodeAPI",               "SNodeAPI.log"),
            ]

            for cname, fname in snode_containers:
                out_f = node_dir / fname
                # Include source filter so we don't mix logs from different
                # storage nodes that might share the same RPC port number.
                gl_q = f'container_name:"{cname}" AND source:"{source_id}"'
                n = fetch(
                    gl_query=gl_q,
                    os_container=cname,
                    os_source=source_id,
                    out_path=out_f,
                    **fetch_kw,
                )
                print(f"    {cname:<42} {n:>8,} lines")

        # ── 9. Write a collection manifest ───────────────────────────────────

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

        # ── 10. Pack into tarball ─────────────────────────────────────────────

        print(f"\n[7] Creating tarball …")
        with tarfile.open(str(tarball_path), "w:gz") as tar:
            tar.add(str(log_root), arcname=bundle_name)

        size_mb = tarball_path.stat().st_size / 1_048_576
        print(f"\n{'=' * 64}")
        print(f"  Done!")
        print(f"  Tarball : {tarball_path}")
        print(f"  Size    : {size_mb:.2f} MB")
        print(f"{'=' * 64}\n")


if __name__ == "__main__":
    main()
