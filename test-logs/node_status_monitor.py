#!/usr/bin/env python3
"""
node_status_monitor.py — tight-interval node status/health change logger.

Polls `sbctl sn list --json` every POLL_SEC seconds and logs a line ONLY when
a node's status or health changes (not a full snapshot every tick), so a
transient down/online bounce lasting just a few seconds -- exactly the
port-block symptom seen in earlier runs -- gets caught even though the main
test's own driving loop only checks in every few minutes.

Runs until killed (SIGINT/SIGTERM) or --duration seconds elapse. Meant to be
launched detached (nohup ... &) alongside test_node_removal_lvol_migration.py,
not by it -- so it keeps recording even if the main test process is looked at
independently, and its own crash/exit doesn't affect the main test.

Usage:
  python3 node_status_monitor.py [--duration 3600] [--poll 2]
"""
import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone


def log(msg):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    line = f"[{ts}] {msg}"
    print(line, flush=True)


def get_nodes():
    """Return {node_id: (status, health, hostname)} or {} on any failure --
    never raises, so a transient sbctl/API hiccup doesn't kill the monitor."""
    try:
        proc = subprocess.run(["sbctl", "sn", "list", "--json"],
                              capture_output=True, text=True, timeout=20)
    except Exception as e:
        log(f"  [monitor] sbctl call failed: {e}")
        return None
    raw = proc.stdout or ""
    start = next((i for i, c in enumerate(raw) if c in "{["), None)
    if start is None:
        log(f"  [monitor] no JSON in sbctl output (rc={proc.returncode})")
        return None
    try:
        parsed = json.loads(raw[start:])
    except json.JSONDecodeError:
        log(f"  [monitor] JSON parse failed (rc={proc.returncode})")
        return None
    rows = parsed.get("results", parsed) if isinstance(parsed, dict) else parsed
    if not isinstance(rows, list):
        return None
    out = {}
    for n in rows:
        nid = n.get("UUID") or n.get("id") or n.get("uuid")
        if not nid:
            continue
        out[nid] = (
            (n.get("Status") or n.get("status") or "").lower(),
            n.get("Health", n.get("health")),
            n.get("Hostname") or n.get("hostname") or "",
        )
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--duration", type=int, default=3600,
                   help="Stop after this many seconds (default: 3600)")
    p.add_argument("--poll", type=float, default=2.0,
                   help="Poll interval in seconds (default: 2)")
    args = p.parse_args()

    log(f"[monitor] starting, poll={args.poll}s duration={args.duration}s")
    deadline = time.time() + args.duration
    last = {}
    misses = 0

    while time.time() < deadline:
        nodes = get_nodes()
        if nodes is None:
            misses += 1
            time.sleep(args.poll)
            continue
        if misses:
            log(f"  [monitor] sbctl recovered after {misses} failed poll(s)")
            misses = 0

        for nid, (status, health, hostname) in nodes.items():
            prev = last.get(nid)
            if prev is None:
                log(f"  {hostname or nid[:8]} ({nid}): initial status={status} health={health}")
            elif prev != (status, health):
                prev_status, prev_health = prev
                log(f"  {hostname or nid[:8]} ({nid}): status {prev_status}->{status}  "
                    f"health {prev_health}->{health}")
            last[nid] = (status, health)

        # Nodes that vanished from the listing entirely (fully removed).
        for nid in list(last.keys()):
            if nid not in nodes:
                log(f"  {nid}: no longer listed (removed)")
                del last[nid]

        time.sleep(args.poll)

    log("[monitor] duration elapsed, exiting")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("[monitor] interrupted, exiting")
        sys.exit(0)
