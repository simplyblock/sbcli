#!/usr/bin/env python3
"""
test_batch_migration_snapshots_remote.py

Batch/shared-namespace lvol migration test with 6 independent lvols sharing
one NVMe-oF subsystem, each carrying 3 sequential snapshots of its own (no
clones this time -- see test_batch_migration_tree_remote.py for the
clone/snapshot ancestry variant):

    lvol1 .. lvol6   (namespaced, shared subsystem, max_ns=6)
    each gets 3 sequential snapshots: <name>_snap1, <name>_snap2, <name>_snap3
    (simplyblock chains these automatically: snap1 <- snap2 <- snap3 <- lvol)

All 6 lvols are real (mountable) members of the batch group; the 18
snapshots are backing objects only, never connected/mounted directly.

Same setting as test_batch_migration_tree_remote.py: single group, single
migration, no-overlap only (source and target have no HA relationship), same
multi-client connect/mount/fio infrastructure (see remote_bridge_lib.py). No
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
  python3 test_batch_migration_snapshots_remote.py
  python3 test_batch_migration_snapshots_remote.py --src vm07 --tgt vm12
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

POOL       = "batch-mig-snaps-pool"
LVOL_SIZE  = "10G"
FIO_SIZE   = "1G"
MOUNT_BASE = "/mnt/batch_mig_snaps"

NUM_LVOLS      = 6
SNAPS_PER_LVOL = 3

LVOL_NAMES = [f"snaps_lvol{i}" for i in range(1, NUM_LVOLS + 1)]
SNAP_NAMES_OF = {
    name: [f"{name}_snap{j}" for j in range(1, SNAPS_PER_LVOL + 1)]
    for name in LVOL_NAMES
}
ALL_SNAP_NAMES = [n for names in SNAP_NAMES_OF.values() for n in names]

LOG_DIR  = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"batch_snaps_remote_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"


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
        lib.cleanup_pool_and_lvols(POOL, cluster_id, lvol_names=LVOL_NAMES,
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

        chk.step(f"3. Create {NUM_LVOLS} lvols (shared subsystem), "
                 f"{SNAPS_PER_LVOL} sequential snapshots each")
        members = []
        master_id = None
        for name in LVOL_NAMES:
            lvol_id = lib.create_lvol(name, args.size, pool_id, src_id,
                                      namespaced=True, max_ns=NUM_LVOLS)
            if master_id is None:
                master_id = lvol_id
            members.append({"id": lvol_id, "name": name})
            for snap_name in SNAP_NAMES_OF[name]:
                lib.sbctl("volume", "create-snapshot", lvol_id, snap_name)
                time.sleep(3)
                lib.log.info(f"    {snap_name}: snapshot of {name} created")

        chk.step(f"4. Connect/mount/fio the {NUM_LVOLS} members across clients")
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

        chk.step("5. Batch-migrate the group")
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
