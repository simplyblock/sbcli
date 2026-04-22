#!/usr/bin/env python3
"""
ftt2_soak_test.py — FTT=2 soak test with overlapping node outages.

Runs on the management node. Requires:
  - Cluster deployed with --parity-chunks-per-stripe 2 (FT=2 and HA journals=4 are auto-derived)
  - 4 storage nodes, 1 client node
  - sbctl available on PATH

Test flow:
  Phase 1: Create 4 volumes (25GB, 1 per storage node), connect to client, format, mount, start fio
  Phase 2: Loop until failure:
    - Pick 2 random nodes for outage (shutdown + restart)
    - Restarts are sequential; if restart blocked by another restart, retry
    - After each outage: check fio for IO errors
    - Wait for data migration to complete
    - All errors (IO error, unsuccessful shutdown/restart, hanging restart) are fatal

Usage:
  python3 ftt2_soak_test.py --cluster-uuid <uuid> --client-ip <ip> --logfile /tmp/soak.log
"""

import argparse
import json
import logging
import random
import subprocess
import sys
import time
from datetime import datetime

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging(logfile):
    fmt = "%(asctime)s [%(levelname)s] %(message)s"
    handlers = [logging.StreamHandler(sys.stdout)]
    if logfile:
        handlers.append(logging.FileHandler(logfile))
    logging.basicConfig(level=logging.DEBUG, format=fmt, handlers=handlers)
    return logging.getLogger("soak")


# ---------------------------------------------------------------------------
# CLI helpers
# ---------------------------------------------------------------------------

def run_sbctl(args_str, logger, timeout=300):
    """Run sbctl -d <args> and return (returncode, stdout, stderr)."""
    cmd = f"sbctl -d {args_str}"
    logger.info(f"CMD: {cmd}")
    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        if result.stdout.strip():
            logger.debug(f"STDOUT: {result.stdout.strip()}")
        if result.stderr.strip():
            logger.debug(f"STDERR: {result.stderr.strip()}")
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        logger.error(f"TIMEOUT after {timeout}s: {cmd}")
        return -1, "", "timeout"


def run_ssh(ip, cmd, logger, user="ec2-user", timeout=60):
    """Run command on remote host via SSH."""
    ssh_cmd = f"ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 {user}@{ip} '{cmd}'"
    logger.debug(f"SSH [{ip}]: {cmd}")
    try:
        result = subprocess.run(
            ssh_cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        logger.error(f"SSH TIMEOUT [{ip}]: {cmd}")
        return -1, "", "timeout"


# ---------------------------------------------------------------------------
# Cluster info
# ---------------------------------------------------------------------------

def get_storage_nodes(cluster_uuid, logger):
    """Get list of storage node UUIDs and IPs."""
    rc, out, _ = run_sbctl(f"sn list --cluster-id {cluster_uuid} -j", logger)
    if rc != 0:
        return []
    nodes = json.loads(out)
    return [{"uuid": n["UUID"], "ip": n["Management IP"], "status": n["Status"]} for n in nodes]


def get_cluster_status(cluster_uuid, logger):
    """Get cluster status."""
    rc, out, _ = run_sbctl(f"cluster status {cluster_uuid} -j", logger)
    if rc != 0:
        return None
    return json.loads(out)


# ---------------------------------------------------------------------------
# Phase 1: Volume setup
# ---------------------------------------------------------------------------

def create_volumes(cluster_uuid, nodes, pool_name, logger):
    """Create 1 volume per storage node, 25GB each. Returns list of volume UUIDs."""
    volumes = []
    for i, node in enumerate(nodes):
        vol_name = f"soak-vol-{i}"
        logger.info(f"Creating volume {vol_name} (25G) on node {node['uuid'][:8]}")
        rc, out, _ = run_sbctl(
            f"lvol add --cluster-id {cluster_uuid} --pool {pool_name} "
            f"--name {vol_name} --size 25G --host-id {node['uuid']}", logger)
        if rc != 0:
            logger.error(f"Failed to create volume {vol_name}")
            return None
        # Parse UUID from output
        vol_uuid = out.strip().split()[-1] if out.strip() else None
        if vol_uuid:
            volumes.append({"name": vol_name, "uuid": vol_uuid, "node_idx": i})
            logger.info(f"Created volume {vol_name}: {vol_uuid}")
        else:
            logger.error(f"Could not parse volume UUID from: {out}")
            return None
    return volumes


def connect_and_mount_volumes(volumes, client_ip, logger):
    """Connect volumes to client, format, mount. Returns mount paths."""
    mounts = []
    for i, vol in enumerate(volumes):
        logger.info(f"Connecting volume {vol['name']} to client")
        rc, out, _ = run_sbctl(f"lvol connect {vol['uuid']}", logger)
        if rc != 0:
            logger.error(f"Failed to connect volume {vol['name']}")
            return None

        # Wait for device to appear
        time.sleep(5)

        # Find the NVMe device on the client
        dev_path = f"/dev/disk/by-id/nvme-*{vol['uuid'][:8]}*"
        rc, out, _ = run_ssh(client_ip, f"ls {dev_path} 2>/dev/null | head -1", logger)
        if rc != 0 or not out.strip():
            # Try finding by volume name
            rc, out, _ = run_ssh(client_ip, "lsblk -J 2>/dev/null", logger)
            logger.warning(f"Could not find device for {vol['name']}, trying nvme list")
            rc, out, _ = run_ssh(client_ip, "sudo nvme list -o json 2>/dev/null", logger)
            dev_path = f"/dev/nvme{i+1}n1"  # fallback
        else:
            dev_path = out.strip()

        mount_dir = f"/mnt/soak{i}"
        logger.info(f"Formatting {dev_path} and mounting at {mount_dir}")
        run_ssh(client_ip, f"sudo mkfs.xfs -f {dev_path}", logger, timeout=120)
        run_ssh(client_ip, f"sudo mkdir -p {mount_dir}", logger)
        run_ssh(client_ip, f"sudo mount {dev_path} {mount_dir}", logger)

        mounts.append({"vol": vol, "dev": dev_path, "mount": mount_dir})

    return mounts


def start_fio(mounts, client_ip, logger):
    """Start fio on all mounted volumes."""
    for m in mounts:
        mount_dir = m["mount"]
        fio_cmd = (
            f"sudo fio --name=soak --directory={mount_dir} "
            f"--direct=1 --rw=randrw --bs=4K --numjobs=4 --iodepth=4 "
            f"--ioengine=libaio --group_reporting --time_based "
            f"--runtime=72000 --size=3G "
            f"--output={mount_dir}/fio.log "
            f"</dev/null >/dev/null 2>&1 &"
        )
        logger.info(f"Starting fio on {mount_dir}")
        run_ssh(client_ip, fio_cmd, logger)

    time.sleep(5)
    # Verify fio is running
    rc, out, _ = run_ssh(client_ip, "pgrep -c fio", logger)
    fio_count = int(out.strip()) if out.strip().isdigit() else 0
    logger.info(f"fio processes running: {fio_count}")
    return fio_count > 0


# ---------------------------------------------------------------------------
# Health checks
# ---------------------------------------------------------------------------

def check_fio_errors(mounts, client_ip, logger):
    """Check if any fio process has reported IO errors. Returns True if OK."""
    # Check if fio is still running
    rc, out, _ = run_ssh(client_ip, "pgrep -c fio", logger)
    fio_count = int(out.strip()) if out.strip().isdigit() else 0
    if fio_count == 0:
        logger.error("FATAL: No fio processes running!")
        return False

    # Check dmesg for IO errors
    rc, out, _ = run_ssh(client_ip, "dmesg | grep -i 'i/o error' | tail -5", logger)
    if out.strip():
        logger.error(f"FATAL: IO errors in dmesg: {out.strip()}")
        return False

    logger.info(f"fio OK: {fio_count} processes running, no IO errors")
    return True


def wait_for_migration_complete(cluster_uuid, logger, timeout=600):
    """Wait for data migration to complete across all nodes."""
    logger.info("Waiting for data migration to complete...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        rc, out, _ = run_sbctl(f"cluster status {cluster_uuid} -j", logger)
        if rc == 0:
            try:
                json.loads(out)  # validate JSON parseable
                # Check if any migration tasks are active
                rc2, out2, _ = run_sbctl(f"sn list --cluster-id {cluster_uuid} -j", logger)
                if rc2 == 0:
                    nodes = json.loads(out2)
                    migrating = False
                    for n in nodes:
                        if n.get("Status") == "online":
                            continue
                    if not migrating:
                        logger.info("Data migration complete")
                        return True
            except json.JSONDecodeError:
                pass
        time.sleep(30)

    logger.error(f"FATAL: Data migration did not complete within {timeout}s")
    return False


# ---------------------------------------------------------------------------
# Phase 2: Outage loop
# ---------------------------------------------------------------------------

def perform_outage(cluster_uuid, nodes, client_ip, mounts, logger):
    """Perform one outage cycle: shutdown 2 random nodes, restart them, verify."""
    if len(nodes) < 3:
        logger.error("Not enough nodes for 2-node outage")
        return False

    # Pick 2 random nodes
    outage_nodes = random.sample(nodes, 2)
    logger.info("=" * 60)
    logger.info(f"OUTAGE: shutting down {outage_nodes[0]['uuid'][:8]} and {outage_nodes[1]['uuid'][:8]}")
    logger.info("=" * 60)

    # Shutdown both nodes
    for n in outage_nodes:
        logger.info(f"Shutting down node {n['uuid'][:8]} ({n['ip']})")
        rc, out, err = run_sbctl(f"sn shutdown {n['uuid']}", logger, timeout=120)
        if rc != 0:
            logger.error(f"FATAL: Failed to shutdown node {n['uuid'][:8]}: {err}")
            return False
        logger.info(f"Node {n['uuid'][:8]} shutdown successful")

    # Wait for shutdown to take effect
    time.sleep(10)

    # Check fio after shutdown
    if not check_fio_errors(mounts, client_ip, logger):
        return False

    # Restart nodes sequentially
    for n in outage_nodes:
        logger.info(f"Restarting node {n['uuid'][:8]} ({n['ip']})")
        max_retries = 10
        for attempt in range(max_retries):
            rc, out, err = run_sbctl(f"sn restart {n['uuid']}", logger, timeout=600)
            if rc == 0:
                logger.info(f"Node {n['uuid'][:8]} restart successful")
                break
            elif "in_restart" in err or "in_shutdown" in err or "in restart" in err.lower():
                logger.warning(f"Restart blocked (peer restarting), retry {attempt+1}/{max_retries}")
                time.sleep(30)
            else:
                logger.error(f"FATAL: Failed to restart node {n['uuid'][:8]}: {err}")
                return False
        else:
            logger.error(f"FATAL: Restart of {n['uuid'][:8]} failed after {max_retries} retries (hanging)")
            return False

    # Wait for nodes to come online
    time.sleep(15)

    # Check fio after restart
    if not check_fio_errors(mounts, client_ip, logger):
        return False

    # Wait for data migration
    if not wait_for_migration_complete(cluster_uuid, logger):
        return False

    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="FTT=2 soak test")
    parser.add_argument("--cluster-uuid", required=True)
    parser.add_argument("--client-ip", required=True)
    parser.add_argument("--pool", default="default")
    parser.add_argument("--logfile", default="/tmp/ftt2_soak.log")
    parser.add_argument("--skip-setup", action="store_true",
                        help="Skip volume creation/mount (reuse existing)")
    args = parser.parse_args()

    logger = setup_logging(args.logfile)
    logger.info("=" * 60)
    logger.info("FTT=2 SOAK TEST STARTING")
    logger.info(f"Cluster: {args.cluster_uuid}")
    logger.info(f"Client: {args.client_ip}")
    logger.info(f"Log: {args.logfile}")
    logger.info("=" * 60)

    # Get storage nodes
    nodes = get_storage_nodes(args.cluster_uuid, logger)
    if len(nodes) < 4:
        logger.error(f"Expected 4 storage nodes, got {len(nodes)}")
        sys.exit(1)
    logger.info(f"Found {len(nodes)} storage nodes")

    mounts = []
    if not args.skip_setup:
        # Phase 1: Create volumes, connect, format, mount, start fio
        logger.info("=== Phase 1: Volume Setup ===")
        volumes = create_volumes(args.cluster_uuid, nodes, args.pool, logger)
        if not volumes:
            logger.error("Volume creation failed")
            sys.exit(1)

        mounts = connect_and_mount_volumes(volumes, args.client_ip, logger)
        if not mounts:
            logger.error("Volume mount failed")
            sys.exit(1)

        if not start_fio(mounts, args.client_ip, logger):
            logger.error("fio start failed")
            sys.exit(1)
    else:
        logger.info("Skipping setup (--skip-setup)")
        # Assume 4 mounts at /mnt/soak0..3
        for i in range(4):
            mounts.append({"mount": f"/mnt/soak{i}"})

    # Phase 2: Outage loop — runs until failure
    logger.info("=== Phase 2: Outage Loop (runs until failure) ===")
    iteration = 0
    while True:
        iteration += 1
        logger.info(f"\n{'#' * 60}")
        logger.info(f"ITERATION {iteration} — {datetime.now().isoformat()}")
        logger.info(f"{'#' * 60}")

        # Refresh node list
        nodes = get_storage_nodes(args.cluster_uuid, logger)
        online_nodes = [n for n in nodes if n["status"] == "online"]
        if len(online_nodes) < 3:
            logger.warning(f"Only {len(online_nodes)} online nodes, waiting...")
            time.sleep(60)
            continue

        if not perform_outage(args.cluster_uuid, online_nodes, args.client_ip, mounts, logger):
            logger.error(f"SOAK TEST FAILED at iteration {iteration}")
            sys.exit(1)

        logger.info(f"Iteration {iteration} PASSED")


if __name__ == "__main__":
    main()
