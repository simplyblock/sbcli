"""One-shot reconcile for snapshots stuck IN_DELETION with an incomplete
delete protocol (phase-1 async issued, phase-2 sync deletes never sent).

Run 20260725 left 18k snapshots in this state: the controller fired phase-1
async deletes while the LVS was leaderless / leadership flapped, and the
monitor's phase-2 hard-required an RPC-confirmed leader so no sync delete was
ever issued. The monitor now completes phase-2 without a leader (it polls the
recorded phase-1 node), so this script simply drives one monitor pass per
stuck snapshot, immediately, with progress reporting — for clusters that ran
the pre-fix build. Safe to re-run; snapshots whose delete cannot be completed
yet are reported and left for the monitor.

Usage (on a management node, inside the app container environment):
    python3 reconcile_stuck_snapshot_deletes.py [--dry-run]
"""
import argparse
import sys

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.services import snapshot_monitor


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="only list stuck snapshots, change nothing")
    args = parser.parse_args()

    db = DBController()
    totals = {"stuck": 0, "completed": 0, "pending": 0, "failed": 0}

    for cluster in db.get_clusters():
        snodes = {n.get_id(): n
                  for n in db.get_storage_nodes_by_cluster_id(cluster.get_id())}
        in_deletion = [m for m in db.get_mini_snapshots()
                       if m.status == SnapShot.STATUS_IN_DELETION]
        for mini in in_deletion:
            try:
                snap = db.get_snapshot_by_id(mini.get_id())
            except KeyError:
                continue
            if snap.status != SnapShot.STATUS_IN_DELETION:
                continue
            if snap.cluster_id and snap.cluster_id != cluster.get_id():
                continue
            snode = snodes.get(snap.lvol.node_id)
            if snode is None:
                continue
            totals["stuck"] += 1
            if args.dry_run:
                print(f"STUCK snap={snap.get_id()} bdev={snap.snap_bdev} "
                      f"phase1_node={snap.deletion_status or '-'}")
                continue
            try:
                ok = snapshot_monitor.process_snap_delete(snap, snode)
            except Exception as exc:
                totals["failed"] += 1
                print(f"FAIL snap={snap.get_id()} bdev={snap.snap_bdev}: {exc}")
                continue
            # process_snap_delete removes the record on full completion.
            try:
                db.get_snapshot_by_id(snap.get_id())
                still_there = True
            except KeyError:
                still_there = False
            if not still_there:
                totals["completed"] += 1
                print(f"DONE snap={snap.get_id()} bdev={snap.snap_bdev}")
            else:
                totals["pending"] += 1
                print(f"PENDING snap={snap.get_id()} bdev={snap.snap_bdev} "
                      f"(ok={ok}) — monitor will retry")

    print(f"stuck={totals['stuck']} completed={totals['completed']} "
          f"pending={totals['pending']} failed={totals['failed']}")
    return 1 if totals["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
