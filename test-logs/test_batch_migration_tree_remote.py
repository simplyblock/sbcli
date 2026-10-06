#!/usr/bin/env python3
"""
test_batch_migration_tree_remote.py

Batch/shared-namespace lvol migration test against one specific, fixed
snapshot/clone tree (built to verify the exact order of operations, not a
generic parametrized shape):

    1. Create Lvol (namespaced, shared subsystem).
    2. Snapshot Lvol three times in sequence: Snap1, Snap2, Snap3
       (simplyblock's normal repeated-snapshot behavior chains these
       automatically: Snap1 <- Snap2 <- Snap3 <- Lvol).
    3. Clone Snap2 -> CloneB (joins Lvol's shared subsystem).
    4. Snapshot CloneB once -> Snap1B.
    5. Clone Snap3 -> CloneC (joins Lvol's shared subsystem).
    6. Snapshot CloneC twice in sequence -> Snap1C, then Snap2C.

Resulting ancestry:
    Snap1 <- Snap2 <- Snap3 <- Lvol
              ^         ^
              |         |
           Snap1B     Snap1C <- Snap2C <- CloneC
              ^
              |
           CloneB

Only Lvol, CloneB, and CloneC are real (mountable) lvols -- they are the
batch group's members, sharing one NVMe-oF subsystem. Snap1/2/3/1B/1C/2C are
backing snapshots only, never connected/mounted directly.

Single group, single migration, no-overlap only (source and target have no
HA relationship) -- see test_batch_migration_groups_remote.py /
test_batch_migration_tree_remote.py's prior generic-tree version for the
multi-scenario batch tests. This script is deliberately narrow: one fixed
tree, one migration, so the order of operations is easy to verify against
the control-plane/storage-node logs afterward.

Same multi-client connect/mount/fio infrastructure as
test_batch_migration_groups_remote.py (see remote_bridge_lib.py). No
ANA-state verification here by design -- this only checks: the migration
reaches done/completed, every member ends up on the target, and fio
verify=md5 reports no corruption anywhere.

This does not set up the cluster -- point --cluster at one that's already
running (see bridge_utils.CLUSTERS).

Node placement uses stable vm hostnames (e.g. "vm07"), not uuids -- those are
re-generated on every redeploy, but hostname/IP position is fixed. The actual
HA relationship between --src/--tgt is read live from `sn list`, not
assumed, and logged so a mislabeled pair shows up as a loud mismatch instead
of silently testing the wrong scenario.

Usage (copy a private key into this folder named "simplyblock" first, or
pass --key to point at a different one):
  python3 test_batch_migration_tree_remote.py
  python3 test_batch_migration_tree_remote.py --src vm07 --tgt vm12
"""

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bridge_utils as bu
import migration_test_lib as lib
import remote_bridge_lib as bridge

POOL       = "batch-mig-tree-pool"
LVOL_SIZE  = "10G"
FIO_SIZE   = "1G"
MOUNT_BASE = "/mnt/batch_mig_tree"

LVOL_NAME    = "tree_lvol"
SNAP_NAMES   = ["tree_snap1", "tree_snap2", "tree_snap3"]   # sequential, on Lvol
CLONEB_NAME  = "tree_cloneB"                                # clone of tree_snap2
SNAP1B_NAME  = "tree_snap1B"                                # snapshot of CloneB
CLONEC_NAME  = "tree_cloneC"                                # clone of tree_snap3
SNAPC_NAMES  = ["tree_snap1C", "tree_snap2C"]                # sequential, on CloneC

ALL_LVOL_NAMES = [LVOL_NAME, CLONEB_NAME, CLONEC_NAME]
ALL_SNAP_NAMES = SNAP_NAMES + [SNAP1B_NAME] + SNAPC_NAMES

LOG_DIR  = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"batch_tree_remote_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"


def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--key", default="./simplyblock",
                   help="Path to the SSH private key for the bridge host "
                        "(default: ./simplyblock)")
    p.add_argument("--cluster", default="default",
                   help=f"Cluster profile from bridge_utils.CLUSTERS. Choices: {list(bu.CLUSTERS)}")
    p.add_argument("--clients", default=None, metavar="IP,IP,...",
                   help="Comma-separated cluster nodes to use as NVMe-oF clients "
                        "(default: the cluster profile's sn_ips)")
    p.add_argument("--src", default="vm07", metavar="VM", help="Source vm hostname (default: vm07)")
    p.add_argument("--tgt", default="vm12", metavar="VM",
                   help="Target vm hostname (default: vm12 -- no HA relationship to vm07)")
    p.add_argument("--size", default=LVOL_SIZE, help=f"Lvol size (default: {LVOL_SIZE})")
    p.add_argument("--fio-runtime", type=int, default=7200)
    return p.parse_args()


def start_batch_migration_multi_client(master_id, target_node_id, client_ips,
                                       retries=5, retry_interval=10):
    """Same precreate-and-retry loop as migration_test_lib.start_batch_migration(),
    but the group's members can be connected from several different client
    nodes, so the TGT connect strings this returns must be run on every one
    of them -- not just whichever client happens to be "current", which is
    all the library version does.
    """
    last_err = None
    for attempt in range(1, retries + 1):
        out = lib.sbctl("--dev", "lvol", "migrate", master_id, target_node_id, "--batch")
        group_id = lib.parse_group_id(out)
        if group_id:
            cmds = lib.parse_nvme_connect_cmds(out)
            for ip in client_ips:
                with bridge.as_client(ip):
                    for cmd in cmds:
                        lib.local_run(f"sudo {cmd} 2>&1 || true")
            time.sleep(2)
            return group_id
        last_err = out
        lib.log.warning(f"start_batch_migration_multi_client: attempt {attempt}/{retries} "
                        f"returned no Migration Group ID; retrying in {retry_interval}s")
        time.sleep(retry_interval)
    raise RuntimeError(f"No Migration Group ID after {retries} attempts; "
                       f"last output: {(last_err or '')[:300]!r}")


def _snapshot_id(cluster_id, name):
    snap = lib.get_snapshot(cluster_id, name)
    snap_id = lib.get(snap, "id") if snap else None
    if not snap_id:
        raise RuntimeError(f"snapshot {name} not found after creation")
    return snap_id


def _lvol_id(pool_id, name):
    lv = lib.get_lvol_by_name(pool_id, name)
    lvol_id = lib.get(lv, "id") if lv else None
    if not lvol_id:
        raise RuntimeError(f"lvol {name} not found after creation")
    return lvol_id


def main():
    args = parse_args()
    lib.init_logging(LOG_FILE)
    lib.log.info(f"Log file: {LOG_FILE}")
    _, sn_ips = bu.get_cluster(args.cluster)
    client_ips = args.clients.split(",") if args.clients else sn_ips

    bridge.enable(key_path=args.key, cluster=args.cluster, client_ip=client_ips[0])
    chk = lib.Checklist()
    try:
        cluster_id = bridge.discover_cluster_id()
        lib.log.info(f"Cluster ID: {cluster_id}")
        lib.log.info(f"Clients   : {client_ips}")

        chk.step("Pre-flight: ensure nvme-cli/fio + nvme-tcp module on every client")
        for ip in set(client_ips):
            with bridge.as_client(ip):
                lib.local_run("command -v nvme || sudo dnf install -y nvme-cli "
                             "|| sudo apt-get install -y nvme-cli")
                lib.local_run("command -v fio || sudo dnf install -y fio "
                             "|| sudo apt-get install -y fio")
                lib.local_run("sudo modprobe nvme-tcp; echo modprobe_rc=$?")

        chk.step("0. Cleanup previous run")
        for ip in set(client_ips):
            with bridge.as_client(ip):
                lib.local_run("sudo killall fio 2>/dev/null || true")
                lib.local_run("sudo nvme disconnect-all 2>/dev/null || true")
                lib.local_run(f"sudo umount {MOUNT_BASE}_* 2>/dev/null || true")
        lib.cleanup_pool_and_lvols(POOL, cluster_id, lvol_names=ALL_LVOL_NAMES,
                                   snap_names=ALL_SNAP_NAMES)

        chk.step("1. Resolve source/target nodes")
        nodes = lib.get_online_nodes()
        if len(nodes) < 2:
            raise RuntimeError(f"Need >= 2 online nodes, got {len(nodes)}")
        src_id = bridge.resolve_by_hostname(nodes, args.src)
        tgt_id = bridge.resolve_by_hostname(nodes, args.tgt)
        relation = bridge.classify_pair(nodes, src_id, tgt_id)
        lib.log.info(f"  {args.src}({src_id[:8]}) -> {args.tgt}({tgt_id[:8]}): {relation}")
        chk.check(relation == "no_overlap",
                  f"{args.src} -> {args.tgt} is no_overlap (got {relation})")

        chk.step("2. Create pool")
        pool_id = lib.ensure_pool(POOL, cluster_id)

        chk.step("3. Build the fixed snapshot/clone tree")
        lvol_id = lib.create_lvol(LVOL_NAME, args.size, pool_id, src_id,
                                  namespaced=True, max_ns=len(ALL_LVOL_NAMES))

        # Snap1, Snap2, Snap3: three sequential snapshots of Lvol. simplyblock
        # chains repeated snapshots of the same lvol automatically
        # (Snap1 <- Snap2 <- Snap3 <- Lvol) -- no explicit clone needed here.
        for snap_name in SNAP_NAMES:
            lib.sbctl("volume", "create-snapshot", lvol_id, snap_name)
            time.sleep(3)
            lib.log.info(f"    {snap_name}: snapshot of {LVOL_NAME} created")

        # CloneB: clone of Snap2, then one snapshot of CloneB (Snap1B).
        snap2_id = _snapshot_id(cluster_id, SNAP_NAMES[1])
        lib.sbctl("snapshot", "clone", snap2_id, CLONEB_NAME)
        time.sleep(3)
        cloneB_id = _lvol_id(pool_id, CLONEB_NAME)
        lib.log.info(f"    {CLONEB_NAME}: cloned from {SNAP_NAMES[1]} ({cloneB_id})")
        lib.sbctl("volume", "create-snapshot", cloneB_id, SNAP1B_NAME)
        time.sleep(3)
        lib.log.info(f"    {SNAP1B_NAME}: snapshot of {CLONEB_NAME} created")

        # CloneC: clone of Snap3, then two sequential snapshots of CloneC
        # (Snap1C, then Snap2C).
        snap3_id = _snapshot_id(cluster_id, SNAP_NAMES[2])
        lib.sbctl("snapshot", "clone", snap3_id, CLONEC_NAME)
        time.sleep(3)
        cloneC_id = _lvol_id(pool_id, CLONEC_NAME)
        lib.log.info(f"    {CLONEC_NAME}: cloned from {SNAP_NAMES[2]} ({cloneC_id})")
        for snap_name in SNAPC_NAMES:
            lib.sbctl("volume", "create-snapshot", cloneC_id, snap_name)
            time.sleep(3)
            lib.log.info(f"    {snap_name}: snapshot of {CLONEC_NAME} created")

        members = [
            {"id": lvol_id, "name": LVOL_NAME},
            {"id": cloneB_id, "name": CLONEB_NAME},
            {"id": cloneC_id, "name": CLONEC_NAME},
        ]
        master_id = lvol_id

        chk.step("4. Connect/mount/fio the 3 real members across clients")
        client_cycle_idx = 0
        by_client = {}
        for i, node in enumerate(members):
            ip = client_ips[client_cycle_idx % len(client_ips)]
            client_cycle_idx += 1
            by_client.setdefault(ip, []).append({
                "id": node["id"], "name": node["name"],
                "mount_point": f"{MOUNT_BASE}_{i}",
            })

        fio_handles = []
        for ip, members_subset in by_client.items():
            with bridge.as_client(ip):
                lib.connect_and_mount_batch_members(members_subset)
                for m in members_subset:
                    fio_file = f"{m['mount_point']}/data.bin"
                    fio_log = f"/tmp/fio_{m['name']}.log"
                    lib.fio_prefill(fio_file, fio_log + ".pre", size=FIO_SIZE)
                    handle = lib.start_fio_bg(fio_file, fio_log, size=FIO_SIZE,
                                              runtime=args.fio_runtime)
                    fio_handles.append({"client_ip": ip, "fio_log": fio_log,
                                        "handle": handle, "name": m["name"],
                                        "lvol_id": m["id"]})
            lib.log.info(f"  {len(members_subset)} member(s) on {ip}")
        chk.check(len(fio_handles) == len(members),
                 f"fio started on all {len(members)} member(s)")

        chk.step("5. Batch-migrate the tree")
        clients_in_group = sorted({h["client_ip"] for h in fio_handles})
        lib.log.info(f"  {args.src} -> {args.tgt}  clients={clients_in_group}")
        group_id = start_batch_migration_multi_client(master_id, tgt_id, clients_in_group)
        lib.continue_batch_migration(group_id)
        lib.log.info(f"  Migration Group ID {group_id}")

        chk.step("6. Wait for migration to complete")
        status, phase = lib.wait_for_batch_migration(group_id, cluster_id)
        chk.check(status in ("done", "completed"),
                 f"migration completed (status={status}, phase={phase})")

        chk.step("7. Verify every member landed on target")
        for m in members:
            node = lib.get_lvol_node(m["id"])
            chk.check(node == tgt_id, f"{m['name']} on target (got {str(node)[:8]})")

        chk.step("8. Stop fio, verify no corruption")
        for h in fio_handles:
            with bridge.as_client(h["client_ip"]):
                lib.stop_fio(h["handle"], post_wait=0)
        for h in fio_handles:
            try:
                with bridge.as_client(h["client_ip"]):
                    ok, verify_errors = lib.check_fio_output(h["fio_log"])
                chk.check(ok, f"{h['name']} ({h['client_ip']}): "
                             f"no fio corruption (verify_errors={verify_errors})")
            except Exception as e:  # noqa: BLE001 -- one member's check failing
                # (e.g. an SSH read timing out) must not abort the whole run.
                lib.log.error(f"  {h['name']} ({h['client_ip']}): fio check raised {e!r}")
                chk.check(False, f"{h['name']} ({h['client_ip']}): fio check failed to run ({e})")

        ok = chk.summary()
        sys.exit(0 if ok else 1)
    finally:
        bridge.disable()


if __name__ == "__main__":
    main()
