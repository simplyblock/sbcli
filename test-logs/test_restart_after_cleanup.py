#!/usr/bin/env python3
"""
test_restart_after_cleanup.py

Verifies that a migration can be cleanly retried after a prior attempt was
rolled back via CLEANUP_TARGET *and* the target node rebooted in between.

  1. Create an lvol, connect/mount it, and start continuous background fio
     (verify=md5) so it's actively writing data.
  2. Create --snapshots snapshots (default 10) while fio keeps writing, so
     each snapshot actually captures new data instead of an empty volume.
  3. Start migrating it to a target node; once --cancel-after-snaps of the
     snapshots have been copied (default 5), cancel the migration.
     `migrate-cancel` forces the migration straight into PHASE_CLEANUP_TARGET
     — the same rollback path a real failure takes — ending in
     STATUS_CANCELLED.
  4. Reboot the target node and wait for it to come back online.
  5. Re-issue the SAME migration (lvol -> same target) with no cancellation
     this time, and verify it now runs to completion.
  6. Stop fio and check for data corruption across the whole sequence.

Usage:
  python3 test_restart_after_cleanup.py
  python3 test_restart_after_cleanup.py --snapshots 10 --cancel-after-snaps 5
  python3 test_restart_after_cleanup.py --target <node-id>
"""

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import migration_test_lib as lib

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
POOL_NAME  = "restart-after-cleanup-pool"
LVOL_NAME  = "restart_cleanup_lvol"
MOUNT_POINT = f"/mnt/{LVOL_NAME}"
SNAP_PREFIX = f"{LVOL_NAME}_snap"

LOG_DIR  = Path("/tmp/restart_after_cleanup_logs") / datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "restart_after_cleanup.log"


def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--lvol-size", default="5G", help="Lvol size (default: 5G)")
    p.add_argument("--fio-size", default="1G", help="fio data size (default: 1G)")
    p.add_argument("--snapshots", type=int, default=10,
                   help="Total snapshots to create before migrating (default: 10)")
    p.add_argument("--cancel-after-snaps", type=int, default=5,
                   help="Cancel the first migration once this many snapshots "
                        "have been copied (default: 5)")
    p.add_argument("--target", default=None,
                   help="Target node ID (default: a random other online node)")
    p.add_argument("--pool", default=POOL_NAME,
                   help=f"Pool name to create/reuse (default: {POOL_NAME})")
    p.add_argument("--snap-progress-timeout", type=int, default=300,
                   help="Seconds to wait for --cancel-after-snaps to copy (default: 300)")
    p.add_argument("--cancel-timeout", type=int, default=300,
                   help="Seconds to wait for the cancelled migration to go terminal (default: 300)")
    p.add_argument("--post-reboot-wait", type=int, default=300,
                   help="Fixed seconds to wait after sending reboot, regardless of node "
                        "status, before polling for it to come back online (default: 300)")
    p.add_argument("--node-restart-timeout", type=int, default=300,
                   help="Seconds to wait (after --post-reboot-wait) for the target node "
                        "to come back online (default: 300)")
    p.add_argument("--retry-migration-timeout", type=int, default=1800,
                   help="Seconds to wait for the retried migration to complete (default: 1800)")
    p.add_argument("--fio-post-wait", type=int, default=30,
                   help="Seconds to keep fio running after the retried migration completes (default: 30)")
    p.add_argument("--pre-snapshot-fio-wait", type=int, default=60,
                   help="Seconds to let fio write before the first snapshot (default: 60)")
    p.add_argument("--snapshot-interval", type=int, default=15,
                   help="Seconds between each snapshot, so fio writes new data "
                        "in between instead of back-to-back empty snapshots (default: 15)")
    # TEMPORARY DEBUG: hold CLEANUP_TARGET open for fault injection.
    # Remove once debugging is complete.
    p.add_argument("--cleanup-delay", type=int, default=0, dest="cleanup_delay_seconds",
                   help="[DEBUG] Seconds to sleep before CLEANUP_TARGET begins (default: 0)")
    return p.parse_args()


def cleanup_previous_run(pool_name, cluster_id, snap_count):
    lib.log.info("--- Cleaning up previous run ---")
    lib.local_run("sudo killall fio 2>/dev/null || true")
    lib.local_run(f"sudo umount {MOUNT_POINT} 2>/dev/null || true")
    lib.local_run("sudo nvme disconnect-all 2>/dev/null || true")
    snap_names = [f"{SNAP_PREFIX}{i}" for i in range(1, snap_count + 1)]
    lib.cleanup_pool_and_lvols(pool_name, cluster_id, lvol_names=[LVOL_NAME],
                               snap_names=snap_names, mount=MOUNT_POINT)
    lib.log.info("--- Cleanup complete ---")


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
    node_ip_map = lib.build_node_ip_map(nodes)

    chk.step("0. Cleanup previous run")
    cleanup_previous_run(args.pool, cluster_id, args.snapshots)
    pool_id = lib.ensure_pool(args.pool, cluster_id)

    chk.step("1. Pick source/target nodes")
    source_node = node_ids[0]
    target_node = args.target or lib.pick_random_target_node(source_node, node_ids)
    log.info(f"Source: {source_node}")
    log.info(f"Target: {target_node}")
    if target_node not in node_ip_map:
        raise RuntimeError(f"No management IP known for target node {target_node}")

    chk.step("2. Create lvol")
    lvol_id = lib.create_lvol(LVOL_NAME, args.lvol_size, pool_id, source_node)
    log.info(f"LVOL: {LVOL_NAME} -> {lvol_id}")

    chk.step("3. Connect, mount, start continuous fio")
    lib.connect_and_mount_lvol(lvol_id, MOUNT_POINT, already_formatted=False)
    fio_file    = f"{MOUNT_POINT}/fio_data.0.0"
    prefill_log = str(LOG_DIR / "fio_prefill.log")
    fio_log     = str(LOG_DIR / "fio_output.log")
    lib.fio_prefill(fio_file, prefill_log, size=args.fio_size)
    fio_proc = lib.start_fio_bg(fio_file, fio_log, size=args.fio_size, runtime=7200)
    chk.check(fio_proc is not None, "fio started in background")

    chk.step(f"4. Create {args.snapshots} snapshot(s) while fio writes data")
    log.info(f"Letting fio write for {args.pre_snapshot_fio_wait}s before the first snapshot...")
    time.sleep(args.pre_snapshot_fio_wait)
    lib.create_snapshots(lvol_id, SNAP_PREFIX, args.snapshots, interval=args.snapshot_interval)
    chk.check(True, f"Created {args.snapshots} snapshot(s) with live fio data")

    chk.step(f"5. Start migration {source_node} -> {target_node}, "
             f"cancel after {args.cancel_after_snaps}/{args.snapshots} snapshots")
    migration_id = lib.start_migration(lvol_id, target_node)
    log.info(f"Migration ID: {migration_id}")
    lib.continue_migration(migration_id, deadline=3600,
                           cleanup_delay_seconds=args.cleanup_delay_seconds)

    reached = lib.wait_for_snap_count(lvol_id, cluster_id, args.cancel_after_snaps,
                                      timeout=args.snap_progress_timeout)
    chk.check(reached, f"Migration reached {args.cancel_after_snaps} copied snapshot(s)")

    if not reached:
        log.warning("Snapshot progress threshold not observed — "
                    "migration may have already moved past snap_copy; cancelling anyway")
    cancel_out = lib.cancel_migration(migration_id)
    chk.check("cancelled" in (cancel_out or "").lower(),
              f"migrate-cancel reported success (got: {cancel_out!r})")

    chk.step("6. Wait for cancelled migration to reach CLEANUP_TARGET -> terminal")
    status = lib.wait_for_migration(lvol_id, cluster_id, terminal_only=True,
                                    timeout=args.cancel_timeout)
    log.info(f"First migration terminal status: {status}")
    chk.check(status == "cancelled", f"First migration ended CANCELLED (got: {status})")

    chk.check(lib.get_lvol_node(lvol_id) == source_node,
              "lvol still on source node after cancelled migration")

    chk.step(f"7. Reboot target node {target_node}")
    target_ip = node_ip_map[target_node]
    try:
        lib.node_ssh(target_ip, "reboot", timeout=15)
    except Exception as e:
        log.info(f"reboot SSH dropped (node rebooting): {e}")
    log.info(f"reboot sent — waiting {args.post_reboot_wait}s unconditionally before checking status")
    time.sleep(args.post_reboot_wait)

    back_online = lib.wait_node_status(target_node, timeout=args.node_restart_timeout)
    chk.check(back_online, f"Target node {target_node} back online within "
                           f"{args.node_restart_timeout}s after the fixed wait")

    chk.step(f"8. Retry migration {source_node} -> {target_node} (no cancel this time)")
    retry_status = lib.migrate_lvol(lvol_id, target_node, cluster_id, deadline=3600,
                                    terminal_only=True, timeout=args.retry_migration_timeout)
    log.info(f"Retried migration terminal status: {retry_status}")
    chk.check(retry_status in ("done", "completed"),
              f"Retried migration completed successfully (got: {retry_status})")

    if retry_status in ("done", "completed"):
        chk.check(lib.get_lvol_node(lvol_id) == target_node,
                  f"lvol now on target node {target_node}")

    chk.step(f"9. Stop fio ({args.fio_post_wait}s after retry), check for corruption")
    lib.stop_fio(fio_proc, post_wait=args.fio_post_wait)
    no_corruption, verify_errs = lib.check_fio_output(fio_log, fault_injected=True)
    chk.check(no_corruption, f"fio: no data corruption (verify_errors={verify_errs})")

    ok = chk.summary()
    log.info(f"Log dir: {LOG_DIR}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
