#!/usr/bin/env python3
# coding=utf-8
"""
test_ctl.py – CLI tool to inspect and manipulate control-plane DB objects
during a live migration test run.

This is intentionally kept separate from the normal sbcli so that it can be
run in a second terminal while a test is executing.  It does NOT start any
background services.

Usage examples
--------------
# List all migrations in the test cluster
python -m tests.migration.test_ctl migration list --cluster-id <id>

# Force a specific node offline (simulate failure)
python -m tests.migration.test_ctl node set-status <node-id> offline

# Force cluster into degraded state
python -m tests.migration.test_ctl cluster set-status <cluster-id> degraded

# Show current migration state
python -m tests.migration.test_ctl migration show <migration-id>

# Cancel a running migration
python -m tests.migration.test_ctl migration cancel <migration-id>

# Set mock-server failure rate (connects via RPC)
python -m tests.migration.test_ctl mock set-failure-rate --host 127.0.0.1 --port 9901 --rate 0.2
"""

import argparse
import json
import sys
import requests

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_migration import LVolMigration
from simplyblock_core.models.storage_node import StorageNode

from tests.migration.topology_loader import (
    set_cluster_status, set_node_status, set_lvol_status, set_snap_status,
)

db = DBController()


# ---------------------------------------------------------------------------
# Cluster commands
# ---------------------------------------------------------------------------

def cmd_cluster_status(args):
    cluster = db.get_cluster_by_id(args.cluster_id)
    print(json.dumps({
        "cluster_id": cluster.uuid,
        "status": cluster.status,
        "ha_type": cluster.ha_type,
    }, indent=2))


def cmd_cluster_set_status(args):
    set_cluster_status(args.cluster_id, args.status)
    print(f"Cluster {args.cluster_id} status → {args.status}")


# ---------------------------------------------------------------------------
# Node commands
# ---------------------------------------------------------------------------

def cmd_node_list(args):
    cluster_id = getattr(args, 'cluster_id', None)
    if cluster_id:
        nodes = db.get_storage_nodes_by_cluster_id(cluster_id)
    else:
        nodes = db.get_storage_nodes()
    for n in nodes:
        print(json.dumps({
            "node_id": n.uuid,
            "cluster_id": n.cluster_id,
            "status": n.status,
            "mgmt_ip": n.mgmt_ip,
            "rpc_port": n.rpc_port,
            "lvstore": n.lvstore,
            "secondary_node_id": n.secondary_node_id,
        }, indent=2))


def cmd_node_status(args):
    node = db.get_storage_node_by_id(args.node_id)
    print(json.dumps({
        "node_id": node.uuid,
        "status": node.status,
        "mgmt_ip": node.mgmt_ip,
        "rpc_port": node.rpc_port,
        "lvstore": node.lvstore,
        "secondary_node_id": node.secondary_node_id,
    }, indent=2))


def cmd_node_set_status(args):
    valid = [StorageNode.STATUS_ONLINE, StorageNode.STATUS_OFFLINE,
             StorageNode.STATUS_DOWN, StorageNode.STATUS_SUSPENDED,
             "in_shutdown", "in_restart", "unreachable"]
    if args.status not in valid:
        print(f"Unknown status '{args.status}'. Valid: {valid}", file=sys.stderr)
        sys.exit(1)
    set_node_status(args.node_id, args.status)
    print(f"Node {args.node_id} status → {args.status}")


# ---------------------------------------------------------------------------
# LVol commands
# ---------------------------------------------------------------------------

def cmd_lvol_list(args):
    cluster_id = getattr(args, 'cluster_id', None)
    node_id = getattr(args, 'node_id', None)
    if node_id:
        lvols = db.get_lvols_by_node_id(node_id)
    else:
        lvols = db.get_lvols()
    for l in lvols:
        if cluster_id and l.cluster_id != cluster_id:
            continue
        print(json.dumps({
            "uuid": l.uuid,
            "name": l.lvol_name,
            "status": l.status,
            "node_id": l.node_id,
            "nqn": l.nqn,
            "cloned_from_snap": l.cloned_from_snap,
        }, indent=2))


def cmd_lvol_set_status(args):
    set_lvol_status(args.lvol_id, args.status)
    print(f"LVol {args.lvol_id} status → {args.status}")


# ---------------------------------------------------------------------------
# Snapshot commands
# ---------------------------------------------------------------------------

def cmd_snap_list(args):
    node_id = getattr(args, 'node_id', None)
    cluster_id = getattr(args, 'cluster_id', None)
    if node_id:
        snaps = db.get_snapshots_by_node_id(node_id)
    else:
        snaps = db.get_snapshots()
    for s in snaps:
        if cluster_id and s.cluster_id != cluster_id:
            continue
        print(json.dumps({
            "uuid": s.uuid,
            "name": s.snap_name,
            "status": s.status,
            "lvol_uuid": s.lvol.uuid if s.lvol else None,
            "node_id": s.lvol.node_id if s.lvol else None,
            "snap_ref_id": s.snap_ref_id,
        }, indent=2))


def cmd_snap_set_status(args):
    set_snap_status(args.snap_id, args.status)
    print(f"Snapshot {args.snap_id} status → {args.status}")


# ---------------------------------------------------------------------------
# Migration commands
# ---------------------------------------------------------------------------

def cmd_migration_list(args):
    cluster_id = getattr(args, 'cluster_id', None)
    migrations = db.get_migrations(cluster_id)
    for m in reversed(migrations):
        print(json.dumps({
            "migration_id": m.uuid,
            "lvol_id": m.lvol_id,
            "source_node": m.source_node_id,
            "target_node": m.target_node_id,
            "phase": m.phase,
            "status": m.status,
            "snaps_migrated": len(m.snaps_migrated),
            "snaps_total": len(m.snap_migration_plan),
            "retry_count": m.retry_count,
            "error": m.error_message or "",
        }, indent=2))


def cmd_migration_show(args):
    migration = db.get_migration_by_id(args.migration_id)
    print(json.dumps(migration.get_clean_dict(), indent=2))


def cmd_migration_cancel(args):
    migration = db.get_migration_by_id(args.migration_id)
    if not migration.is_active():
        print(f"Migration {args.migration_id} is not active (status={migration.status})",
              file=sys.stderr)
        sys.exit(1)
    migration.canceled = True
    migration.write_to_db(db.kv_store)
    print(f"Migration {args.migration_id} flagged for cancellation")


# ---------------------------------------------------------------------------
# Mock-server commands (talk to a running mock via RPC)
# ---------------------------------------------------------------------------

def cmd_mock_set_failure_rate(args):
    url = f"http://{args.host}:{args.port}/"
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "mock_set_failure_rate",
        "params": {
            "failure_rate": float(args.rate),
            "timeout_seconds": float(args.timeout),
        },
    }
    try:
        resp = requests.post(url, json=payload, timeout=5)
        resp.raise_for_status()
        print(f"Failure rate on {args.host}:{args.port} set to {args.rate}")
    except Exception as e:
        print(f"Failed to contact mock server: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_mock_state(args):
    """Dump in-memory state of a mock server (lists lvols, snapshots, subsystems)."""
    # The mock server doesn't have a dedicated state-dump RPC by default, so we
    # query bdev_get_bdevs and nvmf_get_subsystems individually.
    url = f"http://{args.host}:{args.port}/"

    def rpc(method, params=None):
        payload = {"jsonrpc": "2.0", "id": 1, "method": method}
        if params:
            payload["params"] = params
        r = requests.post(url, json=payload, timeout=5)
        r.raise_for_status()
        return r.json().get("result")

    try:
        bdevs = rpc("bdev_get_bdevs") or []
        subsystems = rpc("nvmf_get_subsystems") or []
        print(json.dumps({"bdevs": bdevs, "subsystems": subsystems}, indent=2))
    except Exception as e:
        print(f"Failed to contact mock server: {e}", file=sys.stderr)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def build_parser():
    parser = argparse.ArgumentParser(
        prog="test_ctl",
        description="Control-plane inspector / manipulator for migration tests")
    sub = parser.add_subparsers(dest="domain", required=True)

    # ---- cluster ----
    p_cluster = sub.add_parser("cluster")
    cs = p_cluster.add_subparsers(dest="action", required=True)

    p_cstatus = cs.add_parser("status")
    p_cstatus.add_argument("cluster_id")
    p_cstatus.set_defaults(func=cmd_cluster_status)

    p_cset = cs.add_parser("set-status")
    p_cset.add_argument("cluster_id")
    p_cset.add_argument("status", choices=[
        Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED,
        Cluster.STATUS_INACTIVE, Cluster.STATUS_SUSPENDED,
        Cluster.STATUS_READONLY])
    p_cset.set_defaults(func=cmd_cluster_set_status)

    # ---- node ----
    p_node = sub.add_parser("node")
    ns = p_node.add_subparsers(dest="action", required=True)

    p_nlist = ns.add_parser("list")
    p_nlist.add_argument("--cluster-id", dest="cluster_id", default=None)
    p_nlist.set_defaults(func=cmd_node_list)

    p_nstatus = ns.add_parser("status")
    p_nstatus.add_argument("node_id")
    p_nstatus.set_defaults(func=cmd_node_status)

    p_nset = ns.add_parser("set-status")
    p_nset.add_argument("node_id")
    p_nset.add_argument("status")
    p_nset.set_defaults(func=cmd_node_set_status)

    # ---- lvol ----
    p_lvol = sub.add_parser("lvol")
    ls = p_lvol.add_subparsers(dest="action", required=True)

    p_llist = ls.add_parser("list")
    p_llist.add_argument("--cluster-id", dest="cluster_id", default=None)
    p_llist.add_argument("--node-id", dest="node_id", default=None)
    p_llist.set_defaults(func=cmd_lvol_list)

    p_lset = ls.add_parser("set-status")
    p_lset.add_argument("lvol_id")
    p_lset.add_argument("status")
    p_lset.set_defaults(func=cmd_lvol_set_status)

    # ---- snapshot ----
    p_snap = sub.add_parser("snapshot")
    ss = p_snap.add_subparsers(dest="action", required=True)

    p_slist = ss.add_parser("list")
    p_slist.add_argument("--cluster-id", dest="cluster_id", default=None)
    p_slist.add_argument("--node-id", dest="node_id", default=None)
    p_slist.set_defaults(func=cmd_snap_list)

    p_sset = ss.add_parser("set-status")
    p_sset.add_argument("snap_id")
    p_sset.add_argument("status")
    p_sset.set_defaults(func=cmd_snap_set_status)

    # ---- migration ----
    p_mig = sub.add_parser("migration")
    ms = p_mig.add_subparsers(dest="action", required=True)

    p_mlist = ms.add_parser("list")
    p_mlist.add_argument("--cluster-id", dest="cluster_id", default=None)
    p_mlist.set_defaults(func=cmd_migration_list)

    p_mshow = ms.add_parser("show")
    p_mshow.add_argument("migration_id")
    p_mshow.set_defaults(func=cmd_migration_show)

    p_mcancel = ms.add_parser("cancel")
    p_mcancel.add_argument("migration_id")
    p_mcancel.set_defaults(func=cmd_migration_cancel)

    # ---- mock ----
    p_mock = sub.add_parser("mock")
    mks = p_mock.add_subparsers(dest="action", required=True)

    p_mkfail = mks.add_parser("set-failure-rate")
    p_mkfail.add_argument("--host", default="127.0.0.1")
    p_mkfail.add_argument("--port", type=int, required=True)
    p_mkfail.add_argument("--rate", type=float, default=0.0,
                           help="Fraction of calls that fail (0.0–1.0)")
    p_mkfail.add_argument("--timeout", type=float, default=6.0,
                           help="Seconds to sleep for timeout-type failures")
    p_mkfail.set_defaults(func=cmd_mock_set_failure_rate)

    p_mkstate = mks.add_parser("state")
    p_mkstate.add_argument("--host", default="127.0.0.1")
    p_mkstate.add_argument("--port", type=int, required=True)
    p_mkstate.set_defaults(func=cmd_mock_state)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
