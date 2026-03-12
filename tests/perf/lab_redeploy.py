#!/usr/bin/env python3
"""Fresh redeployment of sbcli into the lab (192.168.10.111-.114).

Uses pexpect via /tmp/ssh_run.py to reach nodes through jump host.
MGMT: .111, Storage: .112, .113, .114
Branch: feature-lvol-migration
"""
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


def run(cmd, target, timeout=300):
    """Run a command on target node via ssh_run.py, return (rc, output)."""
    result = subprocess.run(
        [sys.executable, "/tmp/ssh_run.py", cmd, target, str(timeout)],
        capture_output=True, text=True, timeout=timeout + 30
    )
    output = result.stdout.strip()
    print(f"  [{target}] {output[-500:]}" if len(output) > 500 else f"  [{target}] {output}")
    if result.stderr.strip():
        print(f"  [{target}] stderr: {result.stderr.strip()[-200:]}")
    return result.returncode, output


def section(title):
    print(f"\n{'='*60}")
    print(f"{title}")
    print(f"{'='*60}")


def main():
    print(f"Deploying with branch={BRANCH}")
    print(f"MGMT: {MGMT}, Storage: {', '.join(STORAGE_NODES)}")

    # --- Step 1: pip install on ALL nodes ---
    section("Step 1: pip install on all nodes")
    for node in ALL_NODES:
        print(f"  Installing on {node}...")
        rc, out = run(
            f"pip install git+https://github.com/simplyblock-io/sbcli@{BRANCH} --upgrade --force 2>&1 | tail -5",
            node, timeout=300
        )
        if rc != 0:
            print(f"  WARNING: pip install on {node} returned rc={rc}")

    # --- Step 2: deploy-cleaner on ALL nodes ---
    section("Step 2: deploy-cleaner on all nodes")
    for node in ALL_NODES:
        print(f"  Cleaning {node}...")
        rc, out = run("sbctl sn deploy-cleaner 2>&1 | tail -5", node, timeout=120)

    # --- Step 3: Docker cleanup on MGMT ---
    section("Step 3: docker cleanup on mgmt")
    run("docker rm -f $(docker ps -aq) 2>/dev/null; docker system prune -af --volumes 2>&1 | tail -3", MGMT, timeout=120)

    # --- Step 4: Clean NVMe partitions on storage nodes ---
    section("Step 4: clean NVMe partitions on storage nodes")
    for node in STORAGE_NODES:
        run("echo YES | sbctl sn clean-devices 2>&1 | tail -5", node, timeout=60)

    # --- Step 4b: Upload backup config to MGMT ---
    section("Step 4b: upload backup config")
    import json as _json
    backup_json = _json.dumps(BACKUP_CONFIG)
    run(f"echo '{backup_json}' > /tmp/backup_config.json", MGMT, timeout=10)
    print("  Uploaded /tmp/backup_config.json")

    # --- Step 5: cluster create ---
    section("Step 5: cluster create")
    rc, out = run("sbctl cluster create --use-backup /tmp/backup_config.json 2>&1", MGMT, timeout=300)
    print(f"  rc={rc}")

    # Extract cluster UUID
    cluster_match = re.search(r'([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})', out)
    if not cluster_match:
        print("FATAL: Could not extract cluster UUID from output!")
        print(out)
        sys.exit(1)

    # The last UUID in the output is typically the cluster ID
    all_uuids = re.findall(r'([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})', out)
    cluster_uuid = all_uuids[-1]
    print(f"  Cluster UUID: {cluster_uuid}")

    # Verify cluster is in list
    rc, out = run("sbctl cluster list 2>&1", MGMT, timeout=30)
    if cluster_uuid in out:
        print("  Verified: cluster in list")
    else:
        print("  WARNING: cluster UUID not found in cluster list!")

    # --- Step 6: configure + deploy storage nodes ---
    section("Step 6: configure + deploy storage nodes")
    print("  Configuring...")
    for node in STORAGE_NODES:
        rc, out = run("sbctl sn configure --max-lvol 10 2>&1 | tail -3", node, timeout=120)
        print(f"  {node}: configured (rc={rc})")

    print("  Deploying...")
    for node in STORAGE_NODES:
        rc, out = run("sbctl sn deploy --isolate-cores --ifname eth0 2>&1 | tail -3", node, timeout=120)
        print(f"  {node}: deployed (rc={rc})")

    # Wait for SNodeAPI
    print("  Waiting 20s for SNodeAPI startup...")
    time.sleep(20)

    # Check SNodeAPI is up
    print("  Checking SNodeAPI...")
    for node in STORAGE_NODES:
        rc, out = run("curl -s -o /dev/null -w '%{http_code}' http://localhost:5000/ 2>&1", node, timeout=15)
        print(f"  {node}: HTTP {out}")

    # Check containers
    print("  Checking containers...")
    for node in STORAGE_NODES:
        rc, out = run("docker ps --format '{{.Names}}' | grep -c SNodeAPI 2>&1", node, timeout=15)
        print(f"  {node}: {out.strip()} containers (SNodeAPI)")

    # --- Step 7: add storage nodes ---
    section("Step 7: add storage nodes")
    print(f"  Cmd: sbctl sn add-node {{cluster}} {{ip}}:5000 eth0 --journal-partition 0 --data-nics eth1")
    for node in STORAGE_NODES:
        rc, out = run(
            f"sbctl sn add-node {cluster_uuid} {node}:5000 eth0 --journal-partition 0 --data-nics eth1 2>&1",
            MGMT, timeout=120
        )
        print(f"  {node}: rc={rc}")

    # Verify all nodes added
    rc, out = run("sbctl sn list 2>&1", MGMT, timeout=30)
    sn_count = out.count("online")
    print(f"  Verified: {sn_count if sn_count else 'check'} nodes in sn list")

    # --- Step 8: activate cluster ---
    section("Step 8: activate cluster")
    time.sleep(3)
    rc, out = run(f"sbctl cluster activate {cluster_uuid} 2>&1", MGMT, timeout=120)
    print(f"  rc={rc}")
    if "activated successfully" in out.lower() or "True" in out:
        print("  Activated")
    else:
        print(f"  Output: {out[-300:]}")

    # Show final state
    rc, out = run("sbctl cluster list 2>&1", MGMT, timeout=30)
    for line in out.splitlines():
        if cluster_uuid in line:
            print(f"  Cluster: {line.strip()}")
    rc, out = run("sbctl sn list 2>&1", MGMT, timeout=30)
    sn_lines = [l for l in out.splitlines() if "192.168.10" in l]
    print(f"  Storage nodes: {len(sn_lines)}/{len(STORAGE_NODES)}")
    for line in sn_lines:
        print(f"    {line.strip()}")

    section("DEPLOYMENT COMPLETE")
    print(f"Cluster: {cluster_uuid}")


if __name__ == "__main__":
    main()
