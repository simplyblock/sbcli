#!/usr/bin/env python3
"""
test_migration_source_fallback_remote.py

Manual test for the "migrate from secondary/tertiary when the primary source
is offline" feature, runnable from your own machine against an already-running
docker-deployed cluster, via the bridge tunnel in bridge_utils.py.

IMPORTANT: this exercises the TEMP DEBUG override in
migration_controller._resolve_active_source_node(), which currently forces
every migration to source from the primary's secondary (whenever one is
online) regardless of the primary's real status. That override must be
removed from migration_controller.py before this feature ships — this script
is only meant to validate the migrate-from-replica code path while that
override is in place. Once the override is removed, this script would need a
real "take the primary offline" step (e.g. bridge.node_ssh + shutdown_node)
to exercise the same path.

What this checks
-----------------
  1. lvol + pool created on the source node (which must have both a
     secondary and tertiary configured — see lib.pick_source_node()).
  2. fio runs continuously (randrw + verify=md5) from connect through
     migration completion.
  3. Migration is started normally (`lvol migrate` / `migrate-continue`) —
     the backend silently sources from the secondary because of the debug
     override above.
  4. Migration reaches done/completed; lvol ends up on the target node.
  5. fio reports zero verify errors (no silent corruption from the
     secondary-sourced transfer).
  6. The raw migration record (source_node_id / active_source_node_id /
     phase / status) is dumped to the log so you can visually confirm
     active_source_node_id came back as the secondary, not the primary.

This does not set up the cluster — point --cluster at one that's already
running (see bridge_utils.CLUSTERS).

Usage (copy a private key into this folder named "simplyblock" first, or
pass --key to point at a different one):
  python3 test_migration_source_fallback_remote.py
  python3 test_migration_source_fallback_remote.py --target no-overlap
  python3 test_migration_source_fallback_remote.py --snapshots 4 --client 192.168.10.148
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

POOL       = "mig-src-fallback-pool"
LVOL_NAME  = "mig_src_fallback_lvol"
LVOL_SIZE  = "5G"
FIO_SIZE   = "1G"
MOUNT      = "/mnt/mig_src_fallback"

LOG_DIR  = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"src_fallback_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

SNAP_PREFIX = "mig_src_fallback_snap"


def pick_source_with_online_secondary(nodes):
    """Like lib.pick_source_node(), but only requires an ONLINE secondary --
    tertiary is optional. lib.pick_source_node() requires both, which only
    exists on 3+-node HA clusters; this feature (and its debug override) only
    ever needs a secondary, so a 1+1 (primary+secondary, no tertiary) cluster
    must still be usable here.
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
    p.add_argument("--snapshots", type=int, default=3,
                   help="Number of snapshots to create before migration [default: 3]")
    p.add_argument("--migration-timeout", type=int, default=600,
                   help="Seconds to wait for the migration to reach a terminal "
                        "state [default: 600]")
    return p.parse_args()


def main():
    args = parse_args()
    lib.init_logging(LOG_FILE)
    lib.log.info(f"Log file: {LOG_FILE}")

    fio_file = f"{MOUNT}/data.bin"
    fio_log  = "/tmp/fio_mig_src_fallback.log"
    fio_prefill_log = "/tmp/fio_mig_src_fallback_prefill.log"

    bridge.enable(key_path=args.key, cluster=args.cluster, client_ip=args.client)
    chk = lib.Checklist()
    fio_proc = None
    migration_id = None
    lvol_id = None
    try:
        cluster_id = bridge.discover_cluster_id()
        lib.log.info(f"Cluster ID: {cluster_id}")

        chk.step("Pre-flight: ensure nvme-cli/fio + nvme-tcp module on client")
        lib.local_run("command -v nvme || sudo dnf install -y nvme-cli || sudo apt-get install -y nvme-cli")
        lib.local_run("command -v fio || sudo dnf install -y fio || sudo apt-get install -y fio")
        lib.local_run("sudo modprobe nvme-tcp 2>/dev/null || true")

        chk.step("0. Cleanup previous run")
        lib.cleanup_pool_and_lvols(
            POOL, cluster_id, lvol_names=[LVOL_NAME],
            snap_names=[f"{SNAP_PREFIX}{i}" for i in range(1, 21)],
            mount=MOUNT)

        chk.step("1. Pick source (needs an online secondary) / target nodes")
        nodes = lib.get_online_nodes()
        if len(nodes) < 2:
            raise RuntimeError(f"Need >= 2 online nodes, got {len(nodes)}")
        src_id = pick_source_with_online_secondary(nodes)
        tgt_id = lib.pick_target_node(src_id, nodes, args.target)
        sec_id = lib.get_node_secondary_id(src_id, nodes)
        ter_id = lib.get_node_tertiary_id(src_id, nodes)
        lib.log.info(f"Source primary : {src_id}")
        lib.log.info(f"Source secondary (expected active_source_node_id): {sec_id}")
        lib.log.info(f"Source tertiary : {ter_id}")
        lib.log.info(f"Target          : {tgt_id}")
        chk.check(src_id != tgt_id, "Source and target nodes are distinct")
        chk.check(bool(sec_id), "Source has an online secondary (debug override needs this)")

        chk.step("2. Create pool + lvol on source")
        pool_id = lib.ensure_pool(POOL, cluster_id)
        lvol_id = lib.create_lvol(LVOL_NAME, LVOL_SIZE, pool_id, src_id, snapshot=True)
        chk.check(bool(lvol_id), "lvol created")

        chk.step("3. Connect/mount, pre-fill + start background fio")
        lib.connect_and_mount_lvol(lvol_id, MOUNT)
        lib.fio_prefill(fio_file, fio_prefill_log, size=FIO_SIZE)
        fio_proc = lib.start_fio_bg(fio_file, fio_log, size=FIO_SIZE)
        chk.check(fio_proc is not None, "fio started")
        time.sleep(15)

        if args.snapshots > 0:
            chk.step(f"4. Create {args.snapshots} snapshot(s) before migration")
            lib.create_snapshots(lvol_id, SNAP_PREFIX, args.snapshots, interval=10)
            chk.check(True, f"{args.snapshots} snapshots created")

        chk.step(f"5. Migrate {src_id} -> {tgt_id} "
                 f"(backend will silently source from secondary {sec_id})")
        migration_id = lib.start_migration(lvol_id, tgt_id)
        chk.check(bool(migration_id), "Got migration ID")
        lib.continue_migration(migration_id, deadline=3600)
        lib.log.info(f"Migration: {migration_id}")

        chk.step("6. Wait for migration to complete")
        status = lib.wait_for_migration(lvol_id, cluster_id, terminal_only=True,
                                        timeout=args.migration_timeout)
        chk.check(status in ("done", "completed"),
                  f"Migration completed (status={status})")

        chk.step("7. Dump raw migration record (visually confirm active_source_node_id)")
        record = lib.get_migration_record(lvol_id, cluster_id)
        lib.log.info("Raw migration record:\n" + json.dumps(record, indent=2, default=str))
        recorded_source = lib.get(record or {}, "source_node_id") if record else None
        recorded_active = lib.get(record or {}, "active_source_node_id") if record else None
        lib.log.info(f"source_node_id (should stay primary)      : {recorded_source}")
        lib.log.info(f"active_source_node_id (should be secondary): {recorded_active}")
        if recorded_active is not None:
            chk.check(recorded_active == sec_id,
                      f"active_source_node_id == source secondary "
                      f"(got {recorded_active}, expected {sec_id})")
        else:
            lib.log.warning("  active_source_node_id not present in migrate-list output "
                            "(CLI/DTO may not surface it yet) -- inspect the record above "
                            "manually, or query the API/DB directly.")

        chk.step("8. Verify lvol landed on target")
        lvol_node = lib.get_lvol_node(lvol_id)
        chk.check(lvol_node == tgt_id, f"lvol on target node {tgt_id} (got {lvol_node})")

        chk.step("9. Stop fio, check for data corruption")
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
            lib.cleanup_pool_and_lvols(
                POOL, cluster_id, lvol_names=[LVOL_NAME],
                snap_names=[f"{SNAP_PREFIX}{i}" for i in range(1, 21)],
                mount=MOUNT)
        except Exception as e:
            lib.log.warning(f"final cleanup skipped: {e}")
        bridge.disable()


if __name__ == "__main__":
    main()
