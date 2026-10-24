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
    sudo python3 agent.py repl-stop --volumes V1,V2

Every subcommand prints one JSON document on stdout (collect writes JSON lines
to --out instead and runs until --stop-file exists).
"""
import argparse
import json
import os
import sys
import time

from simplyblock_core import constants
from simplyblock_core.controllers import lvol_controller, qos_controller
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


_INFO_FIELDS = ("state", "lag_seconds", "outstanding_count", "outstanding_bytes",
                "oldest_outstanding_seconds", "last_replicated_at", "last_cycle_seconds",
                "last_cycle_bytes", "replicated_count", "failing_count")


def _repl_info(vid):
    """The control plane's own view of a volume's replication: how far the
    target is behind, what is still queued, and the newest cycle's duration
    and bytes."""
    info = lvol_controller.get_replication_info(vid) or {}
    return {k: info.get(k) for k in _INFO_FIELDS}


def repl_stop(args):
    """Stop replicating the volumes but keep their policy and snapshots.

    This is the first step of detaching a policy (no new snapshots, queued
    transfers cancelled) without its purge, whose snapshot deletions would
    load the cluster in the phase that is meant to show the recovery after
    replication. The benchmark detaches the policy, purge included, after
    the client load has stopped."""
    _out({vid: lvol_controller.replication_stop(vid, from_policy=True) for vid in args.volumes})


def _rpc(node, method, **params):
    return node.rpc_client(timeout=10, retry=1)._request(method, **params)


def _node_kind(nqn):
    """Traffic type of an NVMe-oF subsystem, by its NQN."""
    if ":lvol:" in nqn:
        return "client"
    if ":dev:" in nqn:
        return "internal-device"
    if ":transferhub:" in nqn:
        return "transferhub"    # replication transfers (shares the hublvol port)
    if "hublvol" in nqn:
        return "hublvol"        # internal redirect, or replication: told apart by the peer
    if ":jm" in nqn or "_jm" in nqn:
        return "internal-journal"
    return "other"


def _telemetry(f, now):
    """Per storage node: every subsystem's listen ports and queue pairs (with
    their peers), and every bdev's IO statistics. What the qpairs and bytes per
    traffic type are computed from (telemetry_report.py)."""
    for node in db.get_storage_nodes():
        if node.status != "online":
            continue
        try:
            subs = _rpc(node, "nvmf_get_subsystems") or []
            for s in subs:
                nqn = s.get("nqn", "")
                if "discovery" in nqn:
                    continue
                try:
                    qps = _rpc(node, "nvmf_subsystem_get_qpairs", nqn=nqn) or []
                except Exception:                            # noqa: BLE001
                    qps = []
                peers = {}
                for q in qps:
                    pa = q.get("peer_address") or {}
                    key = f"{pa.get('traddr')}:{pa.get('trsvcid')}"
                    peers[key] = peers.get(key, 0) + 1
                f.write(json.dumps({"t": now, "kind": "qpairs", "node": node.get_id(),
                                    "cluster": node.cluster_id,
                                    "node_ip": node.data_nics[0].ip4_address if node.data_nics else node.mgmt_ip,
                                    "nqn": nqn, "type": _node_kind(nqn),
                                    "listen": [a.get("trsvcid") for a in s.get("listen_addresses") or []],
                                    "qpairs": len(qps), "peers": peers}) + "\n")
            st = _rpc(node, "bdev_get_iostat") or {}
            keep = ("name", "bytes_read", "num_read_ops", "bytes_written", "num_write_ops",
                    "read_latency_ticks", "write_latency_ticks")
            f.write(json.dumps({"t": now, "kind": "iostat", "node": node.get_id(),
                                "tick_rate": st.get("tick_rate"),
                                "bdevs": [{k: b.get(k) for k in keep} for b in st.get("bdevs") or []]}) + "\n")
        except Exception as e:                               # noqa: BLE001
            f.write(json.dumps({"t": now, "kind": "error", "error": f"telemetry {node.get_id()}: {e}"}) + "\n")


def _transfers(f, now, cluster_id):
    """For every replication transfer in flight: the source's transfer statistics
    (request timing) and the landing volume's write statistics on the target
    (its share of the write time)."""
    for t in db.get_job_tasks(cluster_id):
        if t.function_name != JobSchedule.FN_SNAPSHOT_REPLICATION or t.status == JobSchedule.STATUS_DONE:
            continue
        p = t.function_params or {}
        try:
            snap = db.get_snapshot_by_id(p.get("snapshot_id"))
            src = db.get_storage_node_by_id(snap.lvol.node_id)
            st = src.rpc_client(timeout=10, retry=1).bdev_lvol_transfer_stat(snap.snap_bdev) or {}
            rec = {"t": now, "kind": "xfer_stat", "snapshot": snap.get_id(), "volume": snap.lvol.get_id(),
                   "allow_partial": p.get("allow_partial"), "transfer_done": bool(p.get("transfer_done")),
                   "deferred_to": p.get("chain_deferred_to"), "task_result": str(t.function_result)[:80],
                   **st}
            if p.get("remote_lvol_id"):
                lv = db.get_lvol_by_id(p["remote_lvol_id"])
                tgt = db.get_storage_node_by_id(lv.node_id)
                io = _rpc(tgt, "bdev_get_iostat", name=lv.top_bdev) or {}
                b = (io.get("bdevs") or [{}])[0]
                rec["landing"] = {"lvol": lv.get_id(), "bdev": lv.top_bdev, "tick_rate": io.get("tick_rate"),
                                  **{k: b.get(k) for k in ("bytes_written", "num_write_ops",
                                                           "write_latency_ticks")}}
                if p.get("transfer_done"):
                    # the merge on the target, while it runs next to the next transfer
                    rec["merge"] = _rpc(tgt, "bdev_lvol_merge_base_stat", lvol_name=lv.top_bdev)
            f.write(json.dumps(rec) + "\n")
        except Exception as e:                               # noqa: BLE001
            f.write(json.dumps({"t": now, "kind": "error", "error": f"xfer {t.get_id()}: {e}"}) + "\n")


def collect(args):
    """Append changed snapshot records to --out until --stop-file appears;
    every poll the transfers in flight, every --telemetry-every seconds the
    qpairs and bdev statistics of every storage node."""
    last = {}
    seen_landing = set()
    last_telemetry = 0.0
    cluster_id = db.get_storage_node_by_id(db.get_lvol_by_id(args.volumes[0]).node_id).cluster_id
    with open(args.out, "a", encoding="utf-8") as f:
        while not os.path.exists(args.stop_file):
            now = time.time()
            _transfers(f, now, cluster_id)
            if now - last_telemetry >= args.telemetry_every:
                last_telemetry = now
                _telemetry(f, now)
            try:
                for rec in _snapshot_records(args.volumes):
                    key = rec["snapshot"]
                    state = (rec["status"], rec["replicated_at"])
                    if last.get(key) != state:
                        last[key] = state
                        f.write(json.dumps({"t": now, "kind": "snapshot", **rec}) + "\n")
                f.writelines(json.dumps({"t": now, "kind": "repl_info", "volume": vid,
                                        **_repl_info(vid)}) + "\n" for vid in args.volumes)
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
    for name in ("volume-info", "snapshots", "leftovers", "collect", "repl-stop"):
        s = sub.add_parser(name)
        s.add_argument("--volumes", required=True, type=lambda v: v.split(","))
        if name == "collect":
            s.add_argument("--out", required=True)
            s.add_argument("--stop-file", required=True)
            s.add_argument("--interval", type=float, default=10.0)
            s.add_argument("--telemetry-every", type=float, default=60.0)
    args = p.parse_args()
    fn = {"qos-set": qos_set, "qos-get": qos_get, "volume-info": volume_info,
          "snapshots": snapshots, "collect": collect, "leftovers": leftovers,
          "repl-stop": repl_stop}[args.cmd]
    sys.exit(fn(args) or 0)


if __name__ == "__main__":
    main()
