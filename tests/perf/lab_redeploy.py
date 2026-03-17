#!/usr/bin/env python3
"""Fresh redeployment of sbcli into the lab (192.168.10.111-.114).

Uses tests/perf/ssh_run.py to reach nodes through jump host.
MGMT: .111, Storage: .112, .113, .114
Branch: feature-lvol-migration

All command output is streamed directly to stdout.
"""
import json as _json
import os
import subprocess
import sys
import time
import re

BRANCH = "feature-lvol-migration"
MGMT = "192.168.10.111"
STORAGE_NODES = ["192.168.10.112", "192.168.10.113", "192.168.10.114"]
ALL_NODES = [MGMT] + STORAGE_NODES

BACKUP_CONFIG = {
    "secondary_target": 0,
    "with_compression": False,
    "snapshot_backups": True,
    "local_testing": True,
    "local_endpoint": "http://192.168.10.164:9000",
    "access_key_id": "minioadmin",
    "secret_access_key": "minioadmin",
}

SSH_RUN = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ssh_run.py")


def run(cmd, target, timeout=300):
    """Run a command on target node. Streams stdout to console AND captures it."""
    print(f"\n>>> [{target}] {cmd}", flush=True)
    result = subprocess.run(
        [sys.executable, SSH_RUN, cmd, target, str(timeout)],
        capture_output=True, text=True, timeout=timeout + 30
    )
    output = result.stdout.strip()
    # Print output directly so it's visible
    if output:
        print(output, flush=True)
    if result.stderr.strip():
        print(f"STDERR: {result.stderr.strip()}", flush=True)
    if result.returncode != 0:
        print(f"EXIT CODE: {result.returncode}", flush=True)
    return result.returncode, output


def section(title):
    print(f"\n{'='*60}", flush=True)
    print(f"{title}", flush=True)
    print(f"{'='*60}", flush=True)


def main():
    print(f"Deploying with branch={BRANCH}")
    print(f"MGMT: {MGMT}, Storage: {', '.join(STORAGE_NODES)}")

    # --- Step 1: pip install on ALL nodes ---
    section("Step 1: pip install on all nodes")
    for node in ALL_NODES:
        rc, _ = run(
            f"pip install git+https://github.com/simplyblock-io/sbcli@{BRANCH} --upgrade --force 2>&1 | tail -5",
            node, timeout=300
        )

    # --- Step 2: deploy-cleaner on ALL nodes ---
    section("Step 2: deploy-cleaner on all nodes")
    for node in ALL_NODES:
        run("sbctl sn deploy-cleaner 2>&1 | tail -5", node, timeout=120)

    # --- Step 3: Docker cleanup on MGMT ---
    section("Step 3: docker cleanup on mgmt")
    run("docker rm -f $(docker ps -aq) 2>/dev/null; docker system prune -af --volumes 2>&1 | tail -3", MGMT, timeout=120)

    # --- Step 4: Clean NVMe partitions on storage nodes ---
    section("Step 4: clean NVMe partitions on storage nodes")
    for node in STORAGE_NODES:
        run("echo YES | sbctl sn clean-devices 2>&1 | tail -5", node, timeout=60)

    # --- Step 4b: Upload backup config to MGMT ---
    section("Step 4b: upload backup config")
    backup_json = _json.dumps(BACKUP_CONFIG)
    run(f"echo '{backup_json}' > /tmp/backup_config.json && cat /tmp/backup_config.json", MGMT, timeout=10)

    # --- Step 5: cluster create ---
    section("Step 5: cluster create")
    rc, out = run("sbctl cluster create --use-backup /tmp/backup_config.json 2>&1", MGMT, timeout=300)

    # Extract cluster UUID
    all_uuids = re.findall(r'([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})', out)
    if not all_uuids:
        print("FATAL: Could not extract cluster UUID from output!")
        sys.exit(1)
    cluster_uuid = all_uuids[-1]
    print(f"\n  => Cluster UUID: {cluster_uuid}", flush=True)

    # --- Step 6: configure + deploy storage nodes ---
    section("Step 6: configure + deploy storage nodes")
    for node in STORAGE_NODES:
        run("sbctl sn configure --max-lvol 10 2>&1 | tail -3", node, timeout=120)

    for node in STORAGE_NODES:
        run("sbctl sn deploy --isolate-cores --ifname eth0 2>&1 | tail -3", node, timeout=120)

    print("\n  Waiting 20s for SNodeAPI startup...", flush=True)
    time.sleep(20)

    for node in STORAGE_NODES:
        run("curl -s -o /dev/null -w '%{http_code}' http://localhost:5000/", node, timeout=15)

    # --- Step 7: add storage nodes ---
    section("Step 7: add storage nodes")
    for node in STORAGE_NODES:
        run(
            f"sbctl sn add-node {cluster_uuid} {node}:5000 eth0 --journal-partition 0 --data-nics eth1 2>&1",
            MGMT, timeout=120
        )

    run("sbctl sn list 2>&1", MGMT, timeout=30)

    # --- Step 8: activate cluster ---
    section("Step 8: activate cluster")
    time.sleep(3)
    run(f"sbctl cluster activate {cluster_uuid} 2>&1", MGMT, timeout=120)

    # Show final state
    section("Final state")
    run("sbctl cluster list 2>&1", MGMT, timeout=30)
    run("sbctl sn list 2>&1", MGMT, timeout=30)

    section("DEPLOYMENT COMPLETE")
    print(f"Cluster: {cluster_uuid}")


if __name__ == "__main__":
    main()
