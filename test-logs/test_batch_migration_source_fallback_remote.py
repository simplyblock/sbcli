#!/usr/bin/env python3
"""
test_batch_migration_source_fallback_remote.py

Batch-migration counterpart to test_migration_source_fallback_remote.py:
manual test for the "migrate from secondary/tertiary when the primary source
is offline" feature applied to a shared-namespace (batch) migration group,
runnable from your own machine against an already-running docker-deployed
cluster, via the bridge tunnel in bridge_utils.py.

IMPORTANT: this exercises the same TEMP DEBUG override in
migration_controller._resolve_active_source_node() as the single-lvol script.
create_batch_migration() calls create_migration() once per member, and every
one of those calls goes through the same resolver -- so the override applies
automatically here too, no extra wiring needed. That override forces every
migration to source from the primary's secondary (whenever one is online)
regardless of the primary's real status, and must be removed from
migration_controller.py before this feature ships.

What this checks
-----------------
  1. A shared-namespace batch group (master + N members) created on a source
     node that has an online secondary (see pick_source_with_online_secondary()
     below -- same relaxed pick as the single-lvol script; tertiary optional).
  2. fio runs continuously (randrw + verify=md5) on the master member from
     connect through migration completion.
  3. Batch migration is started normally (`lvol migrate --batch` /
     `migrate-continue --batch`) -- the backend silently sources every
     member from the secondary because of the debug override above.
  4. Group reaches done/completed; every member ends up on the target node.
  5. fio reports zero verify errors.
  6. The raw group record (source_node_id / active_source_node_id / phase /
     status) is dumped to the log so you can visually confirm
     active_source_node_id came back as the secondary, not the primary.

This does not set up the cluster -- point --cluster at one that's already
running (see bridge_utils.CLUSTERS).

Usage (copy a private key into this folder named "simplyblock" first, or
pass --key to point at a different one):
  python3 test_batch_migration_source_fallback_remote.py
  python3 test_batch_migration_source_fallback_remote.py --namespaces 4
  python3 test_batch_migration_source_fallback_remote.py --target no-overlap --client 192.168.10.148
"""

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import migration_test_lib as lib
import remote_bridge_lib as bridge

POOL       = "batch-mig-src-fallback-pool"
LVOL_SIZE  = "5G"
FIO_SIZE   = "1G"
MOUNT      = "/mnt/batch_mig_src_fallback"

LOG_DIR  = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"batch_src_fallback_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"


def pick_source_with_online_secondary(nodes):
    """Same relaxed pick as test_migration_source_fallback_remote.py --
    only requires an ONLINE secondary; tertiary is optional. This feature
    (and its debug override) only ever needs a secondary, so a 1+1
    (primary+secondary, no tertiary) cluster must still be usable here.
    """
    for n in nodes:
        nid = lib.get(n, "id")
        if not nid:
            continue
        sec_id = lib.get_node_secondary_id(nid, nodes)
        if not sec_id:
            continue
        sec_node = next((x for x in nodes if lib.get(x, "id") == sec_id), None)
        sec_status = (lib.get(sec_node, "status") or "").lower() if sec_node else ""
        if sec_status in ("online", "active", "online_healthy"):
            ter_id = lib.get_node_tertiary_id(nid, nodes)
            lib.log.info(f"Source: {nid}  sec={sec_id} (online)  ter={ter_id or 'none'}")
            return nid
    raise RuntimeError("No online node with an online secondary configured")


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
    p.add_argument("--target", default="no-overlap",
                   help="Target node: no-overlap | a | b | c | d | UUID-prefix "
                        "[default: no-overlap]")
    p.add_argument("--namespaces", type=int, default=3,
                   help="Total namespace count, master + members (default: 3)")
    p.add_argument("--migration-timeout", type=int, default=600,
                   help="Seconds to wait for the group to reach a terminal "
                        "state [default: 600]")
    return p.parse_args()


def main():
    args = parse_args()
    lib.init_logging(LOG_FILE)
    lib.log.info(f"Log file: {LOG_FILE}")
    n_ns = max(2, args.namespaces)
    lvol_names = [f"src_fb_ns_{i}" for i in range(n_ns)]
    fio_file = f"{MOUNT}/data.bin"
    fio_log  = "/tmp/fio_batch_mig_src_fallback.log"

    bridge.enable(key_path=args.key, cluster=args.cluster, client_ip=args.client)
    chk = lib.Checklist()
    fio_proc = None
    group_id = None
    try:
        cluster_id = bridge.discover_cluster_id()
        lib.log.info(f"Cluster ID: {cluster_id}")

        chk.step("Pre-flight: ensure nvme-cli/fio + nvme-tcp module on client")
        lib.local_run("command -v nvme || sudo dnf install -y nvme-cli || sudo apt-get install -y nvme-cli")
        lib.local_run("command -v fio || sudo dnf install -y fio || sudo apt-get install -y fio")
        lib.local_run("sudo modprobe nvme-tcp 2>/dev/null || true")

        chk.step("0. Cleanup previous run")
        lib.cleanup_pool_and_lvols(POOL, cluster_id, lvol_names=lvol_names, mount=MOUNT)

        chk.step("1. Pick source (needs an online secondary) / target nodes")
        nodes = lib.get_online_nodes()
        if len(nodes) < 2:
            raise RuntimeError(f"Need >= 2 online nodes, got {len(nodes)}")
        src_id = pick_source_with_online_secondary(nodes)
        tgt_id = lib.pick_target_node(src_id, nodes, args.target)
        sec_id = lib.get_node_secondary_id(src_id, nodes)
        ter_id = lib.get_node_tertiary_id(src_id, nodes)
        lib.log.info(f"Source primary : {src_id}")
        lib.log.info(f"Source secondary (expected group active_source_node_id): {sec_id}")
        lib.log.info(f"Source tertiary : {ter_id}")
        lib.log.info(f"Target          : {tgt_id}")
        chk.check(src_id != tgt_id, "Source and target nodes are distinct")
        chk.check(bool(sec_id), "Source has an online secondary (debug override needs this)")

        chk.step(f"2. Create pool + {n_ns} shared-namespace lvols")
        pool_id = lib.ensure_pool(POOL, cluster_id)
        master_id = lib.create_lvol(lvol_names[0], LVOL_SIZE, pool_id, src_id,
                                    namespaced=True, max_ns=n_ns)
        for name in lvol_names[1:]:
            lib.create_lvol(name, LVOL_SIZE, pool_id, src_id, namespaced=True, max_ns=n_ns)
        chk.check(bool(master_id), f"Created {n_ns} shared-namespace lvols")

        chk.step("3. Connect/mount master, start background fio")
        lib.connect_and_mount_lvol(master_id, MOUNT)
        lib.fio_prefill(fio_file, fio_log + ".pre", size=FIO_SIZE)
        fio_proc = lib.start_fio_bg(fio_file, fio_log, size=FIO_SIZE)
        chk.check(fio_proc is not None, "fio started")
        time.sleep(15)

        chk.step(f"4. Batch-migrate {src_id} -> {tgt_id} "
                 f"(backend will silently source every member from secondary {sec_id})")
        group_id = lib.start_batch_migration(master_id, tgt_id)
        chk.check(bool(group_id), "Got batch Migration Group ID")
        lib.continue_batch_migration(group_id)
        lib.log.info(f"Group: {group_id}")

        chk.step("5. Wait for batch migration to complete")
        status, phase = lib.wait_for_batch_migration(group_id, cluster_id,
                                                      timeout=args.migration_timeout)
        chk.check(status in ("done", "completed"),
                 f"Batch migration completed (status={status}, phase={phase})")

        chk.step("6. Dump raw group record (visually confirm active_source_node_id)")
        record = lib.get_batch_record(group_id, cluster_id)
        lib.log.info("Raw group record:\n" + json.dumps(record, indent=2, default=str))
        recorded_source = lib.get(record or {}, "source_node_id") if record else None
        recorded_active = lib.get(record or {}, "active_source_node_id") if record else None
        lib.log.info(f"source_node_id (should stay primary)      : {recorded_source}")
        lib.log.info(f"active_source_node_id (should be secondary): {recorded_active}")
        if recorded_active is not None:
            chk.check(recorded_active == sec_id,
                      f"active_source_node_id == source secondary "
                      f"(got {recorded_active}, expected {sec_id})")
        else:
            lib.log.warning("  active_source_node_id not present in migrate-group-list output "
                            "(CLI/DTO may not surface it yet) -- inspect the record above "
                            "manually, or query the API/DB directly.")

        chk.step("7. Verify every member landed on target")
        for name in lvol_names:
            lv = lib.get_lvol_by_name(pool_id, name)
            if lv:
                node = lib.get_lvol_node(lib.get(lv, "id"))
                chk.check(node == tgt_id, f"{name} on target (got {str(node)[:8]})")

        chk.step("8. Stop fio, check for data corruption")
        lib.stop_fio(fio_proc, post_wait=10)
        fio_proc = None
        ok, verify_errors = lib.check_fio_output(fio_log, fault_injected=False)
        chk.check(ok, f"No fio data corruption (verify_errors={verify_errors})")

        ok = chk.summary()
        sys.exit(0 if ok else 1)
    finally:
        lib.stop_fio(fio_proc, post_wait=0)
        lib.local_run(f"sudo umount {MOUNT} 2>&1 || true")
        lib.local_run("sudo nvme disconnect-all 2>&1 || true")
        try:
            cluster_id = bridge.discover_cluster_id()
            lib.cleanup_pool_and_lvols(POOL, cluster_id, lvol_names=lvol_names, mount=MOUNT)
        except Exception as e:
            lib.log.warning(f"final cleanup skipped: {e}")
        bridge.disable()


if __name__ == "__main__":
    main()
