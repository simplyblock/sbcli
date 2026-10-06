#!/usr/bin/env python3
"""
test_snapshot_clone_delete_order.py

Unrelated to migration — builds an lvol plus a chain of 6 snapshots so a
specific out-of-order delete sequence can be fired manually afterward (this
script does NOT fire any deletes itself).

Setup:
  1. Create an lvol, connect/mount it, and run fio (verify=md5) against it.
  2. Take 6 snapshots while fio writes: snap1 (oldest) .. snap6 (newest).
  3. Stop fio, unmount, and disconnect the NVMe-oF connection.

The lvol and snapshot IDs are printed at the end for manual deletion.

Usage:
  python3 test_snapshot_clone_delete_order.py
  python3 test_snapshot_clone_delete_order.py --lvol-size 5G --fio-size 1G
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
POOL_NAME   = "snap-clone-delete-order-pool"
LVOL_NAME   = "lvol"
MOUNT_POINT = f"/mnt/{LVOL_NAME}"
SNAP_PREFIX = "snap"
SNAP_COUNT  = 6

LOG_DIR  = Path("/tmp/snap_clone_delete_order_logs") / datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "snap_clone_delete_order.log"


def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--lvol-size", default="5G", help="Lvol size (default: 5G)")
    p.add_argument("--fio-size", default="1G", help="fio data size (default: 1G)")
    p.add_argument("--pool", default=POOL_NAME,
                   help=f"Pool name to create/reuse (default: {POOL_NAME})")
    p.add_argument("--pre-snapshot-fio-wait", type=int, default=60,
                   help="Seconds to let fio write before the first snapshot (default: 60)")
    p.add_argument("--snapshot-interval", type=int, default=15,
                   help="Seconds between each snapshot, so fio writes new data "
                        "in between instead of back-to-back empty snapshots (default: 15)")
    return p.parse_args()


def cleanup_previous_run(pool_name, cluster_id):
    lib.log.info("--- Cleaning up previous run ---")
    lib.local_run("sudo killall fio 2>/dev/null || true")
    lib.local_run(f"sudo umount {MOUNT_POINT} 2>/dev/null || true")
    lib.local_run("sudo nvme disconnect-all 2>/dev/null || true")
    snap_names = [f"{SNAP_PREFIX}{i}" for i in range(1, SNAP_COUNT + 1)]
    lib.cleanup_pool_and_lvols(pool_name, cluster_id,
                               lvol_names=[LVOL_NAME],
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
    if not nodes:
        raise RuntimeError("Need >= 1 online node, got 0")
    node_ids = [lib.get(n, "id") for n in nodes if lib.get(n, "id")]
    source_node = node_ids[0]

    chk.step("0. Cleanup previous run")
    cleanup_previous_run(args.pool, cluster_id)
    pool_id = lib.ensure_pool(args.pool, cluster_id)

    chk.step("1. Create lvol")
    lvol_id = lib.create_lvol(LVOL_NAME, args.lvol_size, pool_id, source_node)
    log.info(f"LVOL: {LVOL_NAME} -> {lvol_id}")

    chk.step("2. Connect, mount, start fio")
    lib.connect_and_mount_lvol(lvol_id, MOUNT_POINT, already_formatted=False)
    fio_file    = f"{MOUNT_POINT}/fio_data.0.0"
    prefill_log = str(LOG_DIR / "fio_prefill.log")
    fio_log     = str(LOG_DIR / "fio_output.log")
    lib.fio_prefill(fio_file, prefill_log, size=args.fio_size)
    fio_proc = lib.start_fio_bg(fio_file, fio_log, size=args.fio_size, runtime=7200)
    chk.check(fio_proc is not None, "fio started in background")

    chk.step(f"3. Create {SNAP_COUNT} snapshots while fio writes data")
    log.info(f"Letting fio write for {args.pre_snapshot_fio_wait}s before the first snapshot...")
    time.sleep(args.pre_snapshot_fio_wait)
    lib.create_snapshots(lvol_id, SNAP_PREFIX, SNAP_COUNT, interval=args.snapshot_interval)
    snap_ids = {}
    for i in range(1, SNAP_COUNT + 1):
        name = f"{SNAP_PREFIX}{i}"
        snap = lib.get_snapshot(cluster_id, name)
        chk.check(snap is not None, f"snapshot {name} resolved to an id")
        snap_ids[i] = lib.get(snap, "id") if snap else None

    chk.step("4. Stop fio, unmount, disconnect from the client")
    lib.stop_fio(fio_proc, post_wait=0)
    lib.local_run(f"sudo umount {MOUNT_POINT} 2>/dev/null || true")
    lib.local_run("sudo nvme disconnect-all 2>/dev/null || true")

    ok = chk.summary()

    log.info("--- Setup complete. IDs for manual deletion: ---")
    log.info(f"lvol : {lvol_id}")
    for i in range(1, SNAP_COUNT + 1):
        log.info(f"snap{i}: {snap_ids[i]}")
    log.info(f"Log dir: {LOG_DIR}")

    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
