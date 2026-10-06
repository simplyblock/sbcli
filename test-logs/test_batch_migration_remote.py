#!/usr/bin/env python3
"""
test_batch_migration_remote.py

Shared-namespace (batch) lvol migration test, runnable from your own machine
against an already-running docker-deployed cluster, via the bridge tunnel in
bridge_utils.py. All orchestration is migration_test_lib.py's own
batch-migration API (start_batch_migration / continue_batch_migration /
wait_for_batch_migration, ...) — see remote_bridge_lib.py for how that
library's execution primitives get routed over SSH instead of assuming local
execution on the mgmt node.

This script does not set up the cluster — point --cluster at one that's
already running (see bridge_utils.CLUSTERS).

Usage (copy a private key into this folder named "simplyblock" first, or
pass --key to point at a different one):
  python3 test_batch_migration_remote.py
  python3 test_batch_migration_remote.py --namespaces 4 --client 192.168.10.148
  python3 test_batch_migration_remote.py --cluster new
"""

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import migration_test_lib as lib
import remote_bridge_lib as bridge

POOL       = "batch-mig-remote-pool"
LVOL_SIZE  = "10G"
FIO_SIZE   = "1G"
MOUNT      = "/mnt/batch_mig_remote"

LOG_DIR  = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"batch_remote_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"


def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--key", default="./simplyblock",
                   help="Path to the SSH private key for the bridge host "
                        "(default: ./simplyblock)")
    p.add_argument("--cluster", default="default",
                   help="Cluster profile from bridge_utils.CLUSTERS (default: default)")
    p.add_argument("--client", default=None, metavar="IP",
                   help="Cluster node to use as the NVMe-oF client "
                        "(default: the cluster profile's first sn_ip)")
    p.add_argument("--namespaces", type=int, default=4,
                   help="Total namespace count, master + members (default: 4)")
    p.add_argument("--size", default=LVOL_SIZE, help=f"Lvol size (default: {LVOL_SIZE})")
    p.add_argument("--target", default=None,
                   help="Target node ID (default: any other online node)")
    return p.parse_args()


def main():
    args = parse_args()
    lib.init_logging(LOG_FILE)
    lib.log.info(f"Log file: {LOG_FILE}")
    n_ns = max(2, args.namespaces)
    lvol_names = [f"ns_{i}" for i in range(n_ns)]
    fio_file = f"{MOUNT}/data.bin"
    fio_log = "/tmp/fio_batch_mig_remote.log"

    bridge.enable(key_path=args.key, cluster=args.cluster, client_ip=args.client)
    chk = lib.Checklist()
    try:
        cluster_id = bridge.discover_cluster_id()
        lib.log.info(f"Cluster ID: {cluster_id}")

        chk.step("Pre-flight: ensure nvme-cli/fio + nvme-tcp module on client")
        lib.local_run("command -v nvme || sudo dnf install -y nvme-cli || sudo apt-get install -y nvme-cli")
        lib.local_run("command -v fio || sudo dnf install -y fio || sudo apt-get install -y fio")
        lib.local_run("sudo modprobe nvme-tcp 2>/dev/null || true")

        chk.step("0. Cleanup previous run")
        lib.cleanup_pool_and_lvols(POOL, cluster_id, lvol_names=lvol_names, mount=MOUNT)

        chk.step("1. Pick source / target nodes")
        nodes = lib.get_online_nodes()
        if len(nodes) < 2:
            raise RuntimeError(f"Need >= 2 online nodes, got {len(nodes)}")
        node_ids = [lib.get(n, "id") for n in nodes if lib.get(n, "id")]
        src_id = lib.pick_source_node(nodes)
        tgt_id = args.target or lib.pick_random_target_node(src_id, node_ids)
        lib.log.info(f"Source: {src_id}")
        lib.log.info(f"Target: {tgt_id}")

        chk.step(f"2. Create pool + {n_ns} shared-namespace lvols")
        pool_id = lib.ensure_pool(POOL, cluster_id)
        master_id = lib.create_lvol(lvol_names[0], args.size, pool_id, src_id,
                                    namespaced=True, max_ns=n_ns)
        for name in lvol_names[1:]:
            lib.create_lvol(name, args.size, pool_id, src_id, namespaced=True, max_ns=n_ns)
        chk.check(bool(master_id), f"Created {n_ns} shared-namespace lvols")

        chk.step("3. Connect/mount master, start background fio")
        lib.connect_and_mount_lvol(master_id, MOUNT)
        lib.fio_prefill(fio_file, fio_log + ".pre", size=FIO_SIZE)
        fio_bg = lib.start_fio_bg(fio_file, fio_log, size=FIO_SIZE)
        time.sleep(15)

        try:
            chk.step(f"4. Batch-migrate {src_id} -> {tgt_id}")
            group_id = lib.start_batch_migration(master_id, tgt_id)
            chk.check(bool(group_id), "Got batch Migration Group ID")
            lib.continue_batch_migration(group_id)

            chk.step("5. Wait for batch migration to complete")
            status, phase = lib.wait_for_batch_migration(group_id, cluster_id)
            chk.check(status in ("done", "completed"),
                     f"Batch migration completed (status={status}, phase={phase})")

            chk.step("6. Verify every member landed on target")
            for name in lvol_names:
                lv = lib.get_lvol_by_name(pool_id, name)
                if lv:
                    node = lib.get_lvol_node(lib.get(lv, "id"))
                    chk.check(node == tgt_id, f"{name} on target (got {str(node)[:8]})")

            chk.step("7. Stop fio, check for data corruption")
            ok, verify_errors = lib.check_fio_output(fio_log, fault_injected=False)
            chk.check(ok, f"No fio data corruption (verify_errors={verify_errors})")
        finally:
            lib.stop_fio(fio_bg, post_wait=0)
            lib.local_run(f"sudo umount {MOUNT} 2>&1 || true")
            lib.local_run("sudo nvme disconnect-all 2>&1 || true")

        ok = chk.summary()
        sys.exit(0 if ok else 1)
    finally:
        bridge.disable()


if __name__ == "__main__":
    main()
