#!/usr/bin/env python3
"""
Standalone test for the Graylog per-container log export.

Reads configuration from environment variables and exercises the same Graylog
REST API calls that the e2e ``export_graylog_logs()`` method uses.

Environment variables:
    MGMT_IP            Management node IP (required)
    CLUSTER_SECRET     Graylog admin password / cluster secret (required)
    START_TIME         UTC start time, ISO-8601 (optional; defaults to now - DURATION_MINUTES)
    DURATION_MINUTES   Window length in minutes (default: 60)
    DEPLOY_MODE        "docker" (default) or "kubernetes"
    OUTPUT_DIR         Where to write per-container .log files (default: ./graylog_test_output)

Usage:
    export MGMT_IP=192.168.10.210
    export CLUSTER_SECRET="<your-cluster-secret>"
    export START_TIME="2026-04-25T16:38:00"
    export DURATION_MINUTES=420
    python3 e2e/utils/test_graylog_export.py
"""

import os
import sys
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

START_TIME_STR = os.environ.get("START_TIME", "")

if not MGMT_IP:
    print("ERROR: MGMT_IP not set", file=sys.stderr)
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
    # No start time but duration given → last N minutes
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(minutes=DURATION_MINUTES)
else:
    # No start time, no duration → collect everything Graylog has
    start_dt = datetime(2020, 1, 1, tzinfo=timezone.utc)
    end_dt = datetime.now(timezone.utc)

FROM_ISO = start_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
TO_ISO = end_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

# Graylog base URL
if DEPLOY_MODE == "kubernetes":
    GRAYLOG_BASE = f"http://{MGMT_IP}:9000/api"
else:
    GRAYLOG_BASE = f"http://{MGMT_IP}/graylog/api"

CNAME_FIELD = "kubernetes_container_name" if DEPLOY_MODE == "kubernetes" else "container_name"

PAGE_SIZE = 1000
MAX_RESULT_WINDOW = 100_000

# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------

session = requests.Session()
session.auth = ("admin", CLUSTER_SECRET)
session.headers.update({
    "X-Requested-By": "sb-log-collector",
    "Accept": "application/json",
})

# ---------------------------------------------------------------------------
# Helpers (same logic as cluster_test_base.py export methods)
# ---------------------------------------------------------------------------


def check_graylog():
    """Verify Graylog is reachable."""
    print(f"[1] Checking Graylog at {GRAYLOG_BASE} ...")
    try:
        r = session.get(f"{GRAYLOG_BASE}/system", timeout=10)
        if r.status_code == 200:
            ver = r.json().get("version", "?")
            print(f"    OK  (version {ver})")
            return True
        else:
            print(f"    WARN: HTTP {r.status_code}")
            return True  # try anyway
    except Exception as exc:
        print(f"    FAIL: {exc}")
        return False


def discover_containers():
    """Discover all unique container names in the time window."""
    print(f"\n[2] Discovering containers ({FROM_ISO} -> {TO_ISO}) ...")

    # Primary: /terms endpoint
    terms_url = f"{GRAYLOG_BASE}/search/universal/absolute/terms"
    params = {
        "field": CNAME_FIELD,
        "query": "*",
        "from": FROM_ISO,
        "to": TO_ISO,
        "size": 200,
    }
    try:
        resp = session.get(terms_url, params=params, timeout=30)
        if resp.ok:
            terms = resp.json().get("terms", {})
            if terms:
                print(f"    Found {len(terms)} containers via /terms endpoint:")
                for name, count in sorted(terms.items(), key=lambda x: -x[1]):
                    print(f"      {name:<50} {count:>10} messages")
                return list(terms.keys())
    except Exception as exc:
        print(f"    Terms endpoint failed: {exc}")

    # Fallback: sample search page
    print("    Falling back to search-based discovery ...")
    search_url = f"{GRAYLOG_BASE}/search/universal/absolute"
    params = {
        "query": "*",
        "from": FROM_ISO,
        "to": TO_ISO,
        "limit": 1000,
        "offset": 0,
        "sort": "timestamp:asc",
        "fields": f"timestamp,{CNAME_FIELD}",
    }
    try:
        resp = session.get(search_url, params=params, timeout=60)
        if resp.ok:
            messages = resp.json().get("messages", [])
            names = {m.get("message", {}).get(CNAME_FIELD, "") for m in messages}
            names.discard("")
            print(f"    Found {len(names)} containers via search fallback:")
            for name in sorted(names):
                print(f"      {name}")
            return list(names)
    except Exception as exc:
        print(f"    Search fallback failed: {exc}")

    return []


def _gl_escape(value):
    """Escape Lucene special characters (dots) in Graylog field queries."""
    return value.replace(".", "\\.")


def fetch_container_logs(container_name, out_path):
    """Fetch all logs for a container. Returns line count."""
    search_url = f"{GRAYLOG_BASE}/search/universal/absolute"
    # Query uses the mode-specific field, but fields list always uses
    # "container_name" — matching collect_logs.py.
    query = f'{CNAME_FIELD}:"{_gl_escape(container_name)}"'

    def _fetch_page(q, f_iso, t_iso, limit, offset):
        params = {
            "query": q, "from": f_iso, "to": t_iso,
            "limit": limit, "offset": offset,
            "sort": "timestamp:asc",
            "fields": "timestamp,source,container_name,level,message",
        }
        try:
            resp = session.get(
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
    msgs, total = _fetch_page(query, FROM_ISO, TO_ISO, 1, 0)
    if msgs is None:
        with open(out_path, "w"):
            pass
        return 0

    written = 0
    with open(out_path, "w") as fh:
        if total <= MAX_RESULT_WINDOW:
            written = _write_window(fh, query, FROM_ISO, TO_ISO)
        else:
            # Split into 10-minute sub-windows
            t = datetime.fromisoformat(FROM_ISO.replace("Z", "+00:00"))
            t_end = datetime.fromisoformat(TO_ISO.replace("Z", "+00:00"))
            chunk = timedelta(minutes=10)
            while t < t_end:
                chunk_end = min(t + chunk, t_end)
                c_from = t.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                c_to = chunk_end.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                written += _write_window(fh, query, c_from, c_to)
                t = chunk_end

    return written


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 64)
    print("  Graylog Export Test")
    print("=" * 64)
    print(f"  Window : {FROM_ISO}  ->  {TO_ISO}  ({DURATION_MINUTES} min)")
    print(f"  Mode   : {DEPLOY_MODE}")
    print(f"  Field  : {CNAME_FIELD}")
    print()

    if not check_graylog():
        print("\nGraylog is not reachable. Exiting.")
        sys.exit(1)

    containers = discover_containers()
    if not containers:
        print("\nNo containers found. Check your time window and Graylog setup.")
        sys.exit(1)

    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"\n[3] Fetching logs for {len(containers)} containers -> {OUTPUT_DIR}")
    print("-" * 64)

    total_lines = 0
    for container in sorted(containers):
        safe_name = (
            container.replace("/", "_")
            .replace("\\", "_")
            .replace(":", "_")
            .strip("_")
        ) or "unnamed"
        out_path = os.path.join(OUTPUT_DIR, f"{safe_name}.log")

        try:
            n = fetch_container_logs(container, out_path)
            total_lines += n
            print(f"  {container:<50} {n:>8,} lines")
        except Exception as exc:
            print(f"  {container:<50} FAILED: {exc}")

    print("-" * 64)
    print(f"  TOTAL: {total_lines:,} lines from {len(containers)} containers")
    print(f"  Output: {os.path.abspath(OUTPUT_DIR)}")
    print()


if __name__ == "__main__":
    main()
