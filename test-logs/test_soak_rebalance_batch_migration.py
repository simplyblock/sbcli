#!/usr/bin/env python3
"""
test_soak_rebalance_batch_migration.py

Long-duration batch-migration soak test, shaped after the fiomig-1787384533
k8s-operator run analyzed this session (Archive (9)): 5 shared-namespace
groups (4 x 6-member + 1 x 1-member) continuously cycled between nodes for
several hours.

The one deliberate difference from that run and from every other script in
this directory: a background thread periodically triggers a REAL cluster
rebalance, mirroring realignment.go's production cadence ("I just kick it
off about every 10 minutes, if at least one volume was moved") --
simplyblock_web/api/v2/cluster/__init__.py:267-269 (POST /rebalance) calls
cluster_ops.rebalance(cluster_id) directly; there is no sbctl subcommand for
it and this framework has no HTTP client, so the trigger runs the same
underlying call directly on the mgmt node over SSH instead of reproducing
the REST endpoint's auth plumbing.

This reproduces, on demand, the admission-blocking window Archive (9)
independently hit ("Cluster ... is rebalancing; wait for it to finish
before migrating") instead of waiting for a natural rebalance to happen to
collide with a migration attempt.

Not meant to be run interactively for its full duration from a laptop that
might sleep or lose network -- see deploy_and_watch.py, which uploads this
script (plus its dependencies) to a persistent host and runs it inside a
tmux session there, watching with reconnect-on-drop instead of driving it
live over a single SSH connection.

Usage (copy a private key into this folder named "simplyblock" first, or
pass --key to point at a different one):
  python3 test_soak_rebalance_batch_migration.py
  python3 test_soak_rebalance_batch_migration.py --duration-hours 6 --rebalance-interval-min 10
"""

import argparse
import random
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bridge_utils as bu
import migration_test_lib as lib
import remote_bridge_lib as bridge

POOL       = "soak-rebalance-pool"
LVOL_SIZE  = "5G"
FIO_SIZE   = "1G"
MOUNT_BASE = "/mnt/soak_rebalance"

# Mirrors Archive (9)'s observed topology: groups A-D each 6 members
# (shared NVMe-oF namespace), group E a single (non-batch) lvol.
GROUP_SPECS = [("A", 6), ("B", 6), ("C", 6), ("D", 6), ("E", 1)]

LOG_DIR = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"soak_rebalance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

_stop = threading.Event()


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
    p.add_argument("--duration-hours", type=float, default=6.0,
                   help="Total soak duration in hours (default: 6, matching Archive (9))")
    p.add_argument("--rebalance-interval-min", type=float, default=10.0,
                   help="Minutes between rebalance triggers (default: 10, matching "
                        "realignment.go's production cadence)")
    p.add_argument("--rebalancing-timeout", type=int, default=660,
                   help="Migration admission retry budget in seconds when the cluster "
                        "reports 'rebalancing' (default: 660s -- comfortably past one "
                        "full 10-minute rebalance window so a migration that starts "
                        "right as one begins still gets admitted once it ends)")
    return p.parse_args()


def pick_random_target(current_node_id, nodes):
    candidates = [lib.get(n, "id") for n in nodes if lib.get(n, "id") != current_node_id]
    if not candidates:
        raise RuntimeError("No candidate target nodes available")
    return random.choice(candidates)


def rebalance_loop(mgmt_ip, cluster_id, interval_s, key_path):
    """Background thread: trigger a cluster rebalance on a fixed cadence.

    Runs `cluster_ops.rebalance(cluster_id)` directly on the mgmt node over
    SSH -- the same function the REST endpoint calls -- rather than issuing
    the actual HTTP POST, since this framework has no HTTP/auth client and
    the two are functionally identical for this test's purpose.
    """
    cmd = ("python3 -c \"from simplyblock_core import cluster_ops; "
          f"cluster_ops.rebalance('{cluster_id}')\"")
    while not _stop.wait(interval_s):
        try:
            bridge_ssh = bu.connect_bridge(key_path)
            node = bu.connect_node(bridge_ssh, mgmt_ip)
            bu.run(node, cmd, label="rebalance")
            node.close()
            bridge_ssh.close()
            bu.log("rebalance", "triggered")
        except Exception as e:
            bu.log("rebalance", f"trigger failed (non-fatal, retrying next interval): {e}")


def setup_group(group_label, member_count, pool_id, src_id):
    lvol_names = [f"soak_{group_label}_{i}" for i in range(member_count)]
    is_batch = member_count > 1
    master_id = lib.create_lvol(lvol_names[0], LVOL_SIZE, pool_id, src_id,
                                namespaced=is_batch, max_ns=member_count)
    members = [{"id": master_id, "name": lvol_names[0]}]
    for name in lvol_names[1:]:
        lv_id = lib.create_lvol(name, LVOL_SIZE, pool_id, src_id,
                                namespaced=True, max_ns=member_count)
        members.append({"id": lv_id, "name": name})

    for i, m in enumerate(members):
        m["mount_point"] = f"{MOUNT_BASE}_{group_label}_{i}"

    if is_batch:
        lib.connect_and_mount_batch_members(members)
    else:
        lib.connect_and_mount_lvol(members[0]["id"], members[0]["mount_point"])

    fio_handles = []
    for m in members:
        fio_file = f"{m['mount_point']}/data.bin"
        fio_log = f"/tmp/fio_soak_{group_label}_{m['name']}.log"
        lib.fio_prefill(fio_file, fio_log + ".pre", size=FIO_SIZE)
        # iodepth=1: earlier soak runs at higher iodepth saturated the client
        # VMs badly enough that starting the *next* fio job took minutes.
        handle = lib.start_fio_bg(fio_file, fio_log, size=FIO_SIZE,
                                  runtime=int(24 * 3600),
                                  iodepth=1, numjobs=1)
        fio_handles.append({"fio_log": fio_log, "handle": handle,
                            "name": m["name"], "lvol_id": m["id"]})

    return {"label": group_label, "members": members, "fio_handles": fio_handles,
            "current_node": src_id, "is_batch": is_batch, "cycles": 0, "failures": 0}


def cycle_group(group, nodes, cluster_id, rebalancing_timeout):
    """One migrate-and-wait cycle for a single group."""
    leader_id = group["members"][0]["id"]
    target_id = pick_random_target(group["current_node"], nodes)
    bu.log(f"group-{group['label']}",
          f"migrating {group['current_node'][:8]} -> {target_id[:8]} "
          f"(cycle {group['cycles'] + 1})")
    try:
        if group["is_batch"]:
            group_id = lib.start_batch_migration(leader_id, target_id,
                                                 rebalancing_timeout=rebalancing_timeout)
            lib.continue_batch_migration(group_id)
            status, phase = lib.wait_for_batch_migration(group_id, cluster_id)
        else:
            migration_id = lib.start_migration(leader_id, target_id,
                                               rebalancing_timeout=rebalancing_timeout)
            lib.continue_migration(migration_id)
            status = lib.wait_for_migration(leader_id, cluster_id, terminal_only=True)
            phase = ""

        group["cycles"] += 1
        if status in ("done", "completed"):
            group["current_node"] = target_id
            bu.log(f"group-{group['label']}",
                  f"cycle {group['cycles']} done -> now on {target_id[:8]}")
        else:
            group["failures"] += 1
            bu.log(f"group-{group['label']}",
                  f"cycle {group['cycles']} ended status={status} phase={phase}")
    except Exception as e:
        group["failures"] += 1
        bu.log(f"group-{group['label']}", f"cycle failed (non-fatal, retrying next tick): {e}")


def main():
    args = parse_args()
    lib.init_logging(LOG_FILE)
    lib.log.info(f"Log file: {LOG_FILE}")
    lib.log.info(f"Soak start (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")

    mgmt_ip, sn_ips = bu.get_cluster(args.cluster)
    client_ip = args.client or sn_ips[0]
    bridge.enable(key_path=args.key, cluster=args.cluster, client_ip=client_ip)

    cluster_id = bridge.discover_cluster_id()
    lib.log.info(f"Cluster ID: {cluster_id}")

    lib.local_run("command -v nvme || sudo dnf install -y nvme-cli || sudo apt-get install -y nvme-cli")
    lib.local_run("command -v fio || sudo dnf install -y fio || sudo apt-get install -y fio")
    lib.local_run("sudo modprobe nvme-tcp 2>/dev/null || true")
    lib.local_run("sudo killall fio 2>/dev/null || true")
    lib.local_run("sudo nvme disconnect-all 2>/dev/null || true")
    lib.local_run(f"sudo umount {MOUNT_BASE}_* 2>/dev/null || true")

    all_names = [f"soak_{label}_{i}" for label, n in GROUP_SPECS for i in range(n)]
    lib.cleanup_pool_and_lvols(POOL, cluster_id, lvol_names=all_names)
    pool_id = lib.ensure_pool(POOL, cluster_id)

    nodes = lib.get_online_nodes()
    if len(nodes) < 2:
        raise RuntimeError(f"Need >= 2 online nodes, got {len(nodes)}")

    groups = []
    for i, (label, member_count) in enumerate(GROUP_SPECS):
        src_id = lib.get(nodes[i % len(nodes)], "id")
        lib.log.info(f"Setting up group {label}: {member_count} member(s), source={src_id}")
        groups.append(setup_group(label, member_count, pool_id, src_id))

    rebalance_thread = threading.Thread(
        target=rebalance_loop,
        args=(mgmt_ip, cluster_id, args.rebalance_interval_min * 60, args.key),
        daemon=True)
    rebalance_thread.start()
    bu.log("soak", f"rebalance thread started, interval={args.rebalance_interval_min}min")

    deadline = time.time() + args.duration_hours * 3600
    try:
        while time.time() < deadline:
            for group in groups:
                if time.time() >= deadline:
                    break
                cycle_group(group, nodes, cluster_id, args.rebalancing_timeout)
    finally:
        _stop.set()
        bu.log("soak", "stopping fio on all members...")
        for group in groups:
            for h in group["fio_handles"]:
                try:
                    lib.stop_fio(h["handle"])
                except Exception as e:
                    bu.log("soak", f"stop_fio failed for {h['name']} (non-fatal): {e}")

        bu.log("soak", "=== SUMMARY ===")
        for group in groups:
            bu.log("soak", f"group {group['label']}: {group['cycles']} cycles, "
                          f"{group['failures']} failures, ended on {group['current_node'][:8]}")
        bu.log("soak", f"Soak end (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
