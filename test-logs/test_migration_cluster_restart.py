#!/usr/bin/env python3
"""
test_migration_cluster_restart.py

Verifies that migration still works after a full cluster shutdown/restart
cycle, using the same source -> target node pair both times:

  1. Create lvol #1 on --source, connect/mount it, start continuous
     background fio.
  2. Migrate lvol #1: --source -> --target, to completion (baseline:
     migration works before the cluster is touched).
  3. Shut down every online storage node, one at a time:
       sbctl sn shutdown <node1-id>
       sbctl sn shutdown <node2-id>
       sbctl sn shutdown <node3-id>
     ... waiting for the cluster to report SUSPENDED.
  4. Restart every node:
       sbctl sn restart <node1-id>
       sbctl sn restart <node2-id>
       sbctl sn restart <node3-id>
     ... waiting for all of them to come back ONLINE, then:
       sbctl cluster activate <cluster-id>
     ... waiting for the cluster to finish rebalancing and report ACTIVE.
  5. Create a SECOND lvol on --source, connect/mount/fio it too, then
     migrate it: --source -> --target (same pair, fresh lvol), and verify
     it completes.
  6. Stop fio on both lvols and check for data corruption.

Usage:
  python3 test_migration_cluster_restart.py
  python3 test_migration_cluster_restart.py --snapshots 3
  python3 test_migration_cluster_restart.py --source <node-id> --target <node-id>
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import migration_test_lib as lib

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
POOL_NAME = "cluster-restart-pool"

LVOL_NAME_1  = "cluster_restart_lvol_1"
LVOL_NAME_2  = "cluster_restart_lvol_2"
MOUNT_POINT_1 = f"/mnt/{LVOL_NAME_1}"
MOUNT_POINT_2 = f"/mnt/{LVOL_NAME_2}"
SNAP_PREFIX = f"{LVOL_NAME_1}_snap"

LOG_DIR  = Path("/tmp/cluster_restart_logs") / datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "cluster_restart.log"


def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--lvol-size", default="5G", help="Lvol size (default: 5G)")
    p.add_argument("--fio-size", default="1G", help="fio data size (default: 1G)")
    p.add_argument("--snapshots", type=int, default=0,
                   help="Snapshots to create on lvol #1 before the first migration "
                        "(default: 0)")
    p.add_argument("--pool", default=POOL_NAME,
                   help=f"Pool name to create/reuse (default: {POOL_NAME})")
    p.add_argument("--source", default=None,
                   help="Node both lvols are created on (default: first online node)")
    p.add_argument("--target", default=None,
                   help="Node both migrations move the lvol to (default: a random "
                        "other online node)")
    p.add_argument("--migration-timeout", type=int, default=1800,
                   help="Seconds to wait for each migration to complete (default: 1800)")
    p.add_argument("--node-shutdown-timeout", type=int, default=180,
                   help="Seconds to wait for each node to report shut down (default: 180)")
    p.add_argument("--cluster-suspend-timeout", type=int, default=300,
                   help="Seconds to wait for the cluster to report SUSPENDED (default: 300)")
    p.add_argument("--node-restart-timeout", type=int, default=600,
                   help="Seconds to wait for all nodes to be back ONLINE after "
                        "restart, before activating the cluster (default: 600)")
    p.add_argument("--cluster-active-timeout", type=int, default=1800,
                   help="Seconds to wait for the cluster to finish rebalancing "
                        "and report ACTIVE (default: 1800)")
    p.add_argument("--fio-post-wait", type=int, default=30,
                   help="Seconds to keep fio running after the second migration "
                        "completes (default: 30)")
    return p.parse_args()


def cleanup_previous_run(pool_name, cluster_id, snap_count):
    lib.log.info("--- Cleaning up previous run ---")
    lib.local_run("sudo killall fio 2>/dev/null || true")
    lib.local_run(f"sudo umount {MOUNT_POINT_1} 2>/dev/null || true")
    lib.local_run(f"sudo umount {MOUNT_POINT_2} 2>/dev/null || true")
    lib.local_run("sudo nvme disconnect-all 2>/dev/null || true")
    snap_names = [f"{SNAP_PREFIX}{i}" for i in range(1, snap_count + 1)]
    lib.cleanup_pool_and_lvols(pool_name, cluster_id, lvol_names=[LVOL_NAME_1, LVOL_NAME_2],
                               snap_names=snap_names, mount=MOUNT_POINT_1)
    lib.local_run(f"sudo rm -rf {MOUNT_POINT_2} 2>/dev/null || true")
    lib.log.info("--- Cleanup complete ---")


def shutdown_all_nodes(chk, node_ids, timeout):
    for node_id in node_ids:
        lib.log.info(f"Shutting down node {node_id} ...")
        lib.shutdown_node(node_id)
        down = lib.wait_node_status(
            node_id, expected_statuses=("in_shutdown", "offline", "down", "suspended"),
            timeout=timeout)
        chk.check(down, f"Node {node_id} reported shut down within {timeout}s")


def restart_all_nodes(chk, node_ids, timeout):
    for node_id in node_ids:
        lib.log.info(f"Restarting node {node_id} ...")
        lib.restart_node(node_id)
    back_online = lib.wait_all_nodes_status(node_ids, expected_statuses=("online",),
                                            timeout=timeout)
    chk.check(back_online, f"All {len(node_ids)} node(s) back ONLINE within {timeout}s")
    return back_online


def create_connect_mount_fio(chk, step_label, lvol_name, mount_point, source_node,
                             pool_id, args, snap_count=0):
    lvol_id = lib.create_lvol(lvol_name, args.lvol_size, pool_id, source_node)
    lib.log.info(f"LVOL: {lvol_name} -> {lvol_id} on {source_node}")
    if snap_count:
        lib.create_snapshots(lvol_id, f"{lvol_name}_snap", snap_count, interval=5)

    lib.connect_and_mount_lvol(lvol_id, mount_point, already_formatted=False)
    fio_file    = f"{mount_point}/fio_data.0.0"
    prefill_log = str(LOG_DIR / f"fio_{lvol_name}_prefill.log")
    fio_log     = str(LOG_DIR / f"fio_{lvol_name}.log")
    lib.fio_prefill(fio_file, prefill_log, size=args.fio_size)
    fio_proc = lib.start_fio_bg(fio_file, fio_log, size=args.fio_size, runtime=7200)
    chk.check(fio_proc is not None, f"{step_label}: fio started in background")
    return lvol_id, fio_proc, fio_log


def run_migration(chk, step_label, lvol_id, target_node, cluster_id, timeout):
    lib.log.info(f"Migrating {lvol_id} -> {target_node} ...")
    status = lib.migrate_lvol(lvol_id, target_node, cluster_id,
                              terminal_only=True, timeout=timeout)
    lib.log.info(f"Migration terminal status: {status}")
    ok = chk.check(status in ("done", "completed"),
                   f"{step_label}: migration completed (got: {status})")
    if ok:
        chk.check(lib.get_lvol_node(lvol_id) == target_node,
                  f"{step_label}: lvol now on target node {target_node}")
    return ok


def main():
    args = parse_args()
    log = lib.init_logging(LOG_FILE)
    chk = lib.Checklist()

    log.info(f"Log dir : {LOG_DIR}")

    cluster_id = lib.discover_cluster_id()
    log.info(f"Cluster : {cluster_id}")

    nodes = lib.get_online_nodes()
    if len(nodes) < 2:
        raise RuntimeError(f"Need >= 2 online nodes, got {len(nodes)}")
    node_ids = [lib.get(n, "id") for n in nodes if lib.get(n, "id")]
    log.info(f"Nodes   : {node_ids}")

    chk.step("0. Cleanup previous run")
    cleanup_previous_run(args.pool, cluster_id, args.snapshots)
    pool_id = lib.ensure_pool(args.pool, cluster_id)

    source_node = args.source or node_ids[0]
    target_node = args.target or lib.pick_random_target_node(source_node, node_ids)
    log.info(f"Source  : {source_node}")
    log.info(f"Target  : {target_node}")

    chk.step("1. Create lvol #1, connect, mount, start continuous fio")
    lvol1_id, fio1_proc, fio1_log = create_connect_mount_fio(
        chk, "lvol #1", LVOL_NAME_1, MOUNT_POINT_1, source_node, pool_id, args,
        snap_count=args.snapshots)

    chk.step(f"2. First migration {source_node} -> {target_node} "
             f"(baseline, before touching the cluster)")
    run_migration(chk, "First migration", lvol1_id, target_node, cluster_id,
                 args.migration_timeout)

    chk.step(f"3. Shut down all {len(node_ids)} node(s)")
    shutdown_all_nodes(chk, node_ids, args.node_shutdown_timeout)

    chk.step("4. Wait for cluster to report SUSPENDED")
    suspended = lib.wait_cluster_status(cluster_id, "suspended",
                                        timeout=args.cluster_suspend_timeout)
    chk.check(suspended, f"Cluster {cluster_id} reported SUSPENDED within "
                        f"{args.cluster_suspend_timeout}s")

    chk.step(f"5. Restart all {len(node_ids)} node(s)")
    nodes_back = restart_all_nodes(chk, node_ids, args.node_restart_timeout)
    if not nodes_back:
        log.warning("Not all nodes came back online — activating anyway to "
                    "observe cluster behavior")

    chk.step("6. Activate cluster and wait for it to finish rebalancing")
    lib.activate_cluster(cluster_id)
    active = lib.wait_cluster_status(cluster_id, "active",
                                     timeout=args.cluster_active_timeout)
    chk.check(active, f"Cluster {cluster_id} reported ACTIVE within "
                      f"{args.cluster_active_timeout}s")

    chk.step("7. Create lvol #2 on source, connect, mount, start continuous fio")
    lvol2_id, fio2_proc, fio2_log = create_connect_mount_fio(
        chk, "lvol #2", LVOL_NAME_2, MOUNT_POINT_2, source_node, pool_id, args)

    chk.step(f"8. Second migration {source_node} -> {target_node} "
             f"(same pair, fresh lvol, after cluster restart)")
    run_migration(chk, "Second migration", lvol2_id, target_node, cluster_id,
                 args.migration_timeout)

    chk.step(f"9. Stop fio on both lvols ({args.fio_post_wait}s after second "
             f"migration), check for corruption")
    lib.stop_fio(fio1_proc, post_wait=args.fio_post_wait)
    lib.stop_fio(fio2_proc, post_wait=args.fio_post_wait)
    no_corrupt1, errs1 = lib.check_fio_output(fio1_log, fault_injected=True)
    no_corrupt2, errs2 = lib.check_fio_output(fio2_log, fault_injected=True)
    chk.check(no_corrupt1, f"lvol #1: no data corruption (verify_errors={errs1})")
    chk.check(no_corrupt2, f"lvol #2: no data corruption (verify_errors={errs2})")

    ok = chk.summary()
    log.info(f"Log dir: {LOG_DIR}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
