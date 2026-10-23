#!/usr/bin/env python3
"""Management-node side of the replication service-class benchmark.

Runs ON the management node, where sbcli is installed and the control plane's
database is reachable, and is driven over SSH by bench.py. It reads records
straight from the database rather than through the API, so it needs no
credentials and sees fields the API does not serve (snapshot timestamps,
replication task parameters).

    sudo python3 agent.py qos-set --cluster C --share 15 [--default-weight 100]
    sudo python3 agent.py qos-get --cluster C
    sudo python3 agent.py volume-info --volumes V1,V2
    sudo python3 agent.py snapshots --volumes V1,V2
    sudo python3 agent.py collect --volumes V1,V2 --out F --stop-file S [--interval 10]
    sudo python3 agent.py leftovers --volumes V1,V2

Every subcommand prints one JSON document on stdout (collect writes JSON lines
to --out instead and runs until --stop-file exists).
"""
import argparse
import json
import os
import sys
import time

from simplyblock_core import constants
from simplyblock_core.controllers import qos_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.snapshot import SnapShot

db = DBController()


def _out(doc):
    json.dump(doc, sys.stdout, default=str)
    sys.stdout.write("\n")


def qos_set(args):
    failed = qos_controller.set_config(args.cluster, args.default_weight, [], args.share)
    _out({"push_failed": failed, "config": qos_controller.get_config(args.cluster)})
    return 1 if failed else 0


def qos_get(args):
    _out(qos_controller.get_config(args.cluster))


def volume_info(args):
    out = {}
    for vid in args.volumes:
        lv = db.get_lvol_by_id(vid)
        out[vid] = {"name": lv.lvol_name, "nqn": lv.nqn, "node_id": lv.node_id,
                    "top_bdev": lv.top_bdev, "size": lv.size,
                    "priority_class": lv.lvol_priority_class}
    _out(out)


def _snapshot_records(volumes):
    """One record per internal snapshot of each volume, as it stands now."""
    recs = []
    for vid in volumes:
        for s in db.get_snapshots_by_lvol_id(vid):
            if s.snap_type != SnapShot.TYPE_INTERNAL:
                continue
            rec = {"volume": vid, "snapshot": s.get_id(), "created_at": s.created_at,
                   "target": s.target_replicated_snap_uuid,
                   "status": s.status, "replicated_at": 0, "size": s.size,
                   "used_size": s.used_size}
            if s.target_replicated_snap_uuid:
                try:
                    rec["replicated_at"] = db.get_snapshot_by_id(s.target_replicated_snap_uuid).created_at
                except KeyError:
                    # pruned on the target already; the source still says it
                    # replicated, but not when
                    rec["replicated_at"] = -1
            recs.append(rec)
    return recs


def snapshots(args):
    _out(_snapshot_records(args.volumes))


def _landing_classes(cluster_id, seen):
    """Service class of every new landing volume, read from its SPDK node.

    The control plane creates the landing volume in the replication class; this
    is the check that the class actually reached the data plane.
    """
    found = []
    for t in db.get_job_tasks_by_function(cluster_id, JobSchedule.FN_SNAPSHOT_REPLICATION):
        if t.status == JobSchedule.STATUS_DONE:
            continue
        rid = (t.function_params or {}).get("remote_lvol_id")
        if not rid or rid in seen:
            continue
        try:
            lv = db.get_lvol_by_id(rid)
            node = db.get_storage_node_by_id(lv.node_id)
            bdev = node.rpc_client(timeout=5).bdev_get(lv.top_bdev)
            if bdev is None:
                continue                  # not created on the node yet; next poll
            cls = bdev["driver_specific"]["lvol"].get("lvol_priority_class")
        except Exception as e:                               # noqa: BLE001
            cls = f"unreadable: {e}"
        seen.add(rid)
        found.append({"landing_volume": rid, "priority_class": cls,
                      "expected": constants.QOS_REPLICATION_WIRE_CLASS})
    return found


def collect(args):
    """Append changed snapshot records to --out until --stop-file appears."""
    last = {}
    seen_landing = set()
    cluster_id = db.get_storage_node_by_id(db.get_lvol_by_id(args.volumes[0]).node_id).cluster_id
    with open(args.out, "a", encoding="utf-8") as f:
        while not os.path.exists(args.stop_file):
            now = time.time()
            try:
                for rec in _snapshot_records(args.volumes):
                    key = rec["snapshot"]
                    state = (rec["status"], rec["replicated_at"])
                    if last.get(key) != state:
                        last[key] = state
                        f.write(json.dumps({"t": now, "kind": "snapshot", **rec}) + "\n")
                for rec in _landing_classes(cluster_id, seen_landing):
                    f.write(json.dumps({"t": now, "kind": "landing_class", **rec}) + "\n")
            except Exception as e:                           # noqa: BLE001
                f.write(json.dumps({"t": now, "kind": "error", "error": str(e)}) + "\n")
            f.flush()
            time.sleep(args.interval)


def leftovers(args):
    """Internal snapshots still present for the volumes, on either side."""
    recs = _snapshot_records(args.volumes)
    _out({"internal_snapshots": len(recs),
          "replicated_copies": sum(1 for r in recs if r["replicated_at"])})


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("qos-set")
    s.add_argument("--cluster", required=True)
    s.add_argument("--share", type=int, required=True)
    s.add_argument("--default-weight", type=int, default=100)
    s = sub.add_parser("qos-get")
    s.add_argument("--cluster", required=True)
    for name in ("volume-info", "snapshots", "leftovers", "collect"):
        s = sub.add_parser(name)
        s.add_argument("--volumes", required=True, type=lambda v: v.split(","))
        if name == "collect":
            s.add_argument("--out", required=True)
            s.add_argument("--stop-file", required=True)
            s.add_argument("--interval", type=float, default=10.0)
    args = p.parse_args()
    fn = {"qos-set": qos_set, "qos-get": qos_get, "volume-info": volume_info,
          "snapshots": snapshots, "collect": collect, "leftovers": leftovers}[args.cmd]
    sys.exit(fn(args) or 0)


if __name__ == "__main__":
    main()
