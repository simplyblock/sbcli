#!/usr/bin/env python3
"""
test_batch_migration_ns_gaps_remote.py

Single batch-migration group, single client, flat 10-member shared-namespace
group (no snapshots/tree) -- but two members are deleted from the MIDDLE of
the group before migration starts, so the remaining members' ns_ids are no
longer sequential (e.g. 1,2,3,5,6,8,9,10 instead of 1..8). The point of this
test is narrow: does the target end up with the EXACT SAME (gapped) ns_id
map as the source, member for member, or does something along the way
renumber/compact them?

This directly exercises the create_migration() fix that now explicitly pins
each target namespace to the source's own lvol.ns_id (migration_controller.py)
instead of letting SPDK auto-assign on the target subsystem -- auto-assign
would have silently compacted the gaps away (1,2,3,4,5,6,7,8), which is
exactly the divergence this test is designed to catch.

Meant to run against the "new" cluster profile (bridge_utils.CLUSTERS["new"]),
which only has 3 storage nodes -- fewer nodes to juggle than the "default"
6-node cluster used by the other batch scripts.

This does not set up the cluster -- point --cluster at one that's already
running.

Usage (copy a private key into this folder named "simplyblock" first, or
pass --key to point at a different one):
  python3 test_batch_migration_ns_gaps_remote.py --cluster new
"""

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bridge_utils as bu
import migration_test_lib as lib
import remote_bridge_lib as bridge

POOL       = "batch-mig-ns-gaps-pool"
LVOL_SIZE  = "5G"
FIO_SIZE   = "512M"
MOUNT_BASE = "/mnt/batch_mig_ns_gap"
NAME_PREFIX = "ns_gap"

LOG_DIR  = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"batch_ns_gaps_remote_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"


def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--key", default="./simplyblock",
                   help="Path to the SSH private key for the bridge host "
                        "(default: ./simplyblock)")
    p.add_argument("--cluster", default="new",
                   help=f"Cluster profile from bridge_utils.CLUSTERS. Choices: {list(bu.CLUSTERS)} "
                        "(default: new -- the smaller 3-node cluster)")
    p.add_argument("--client", default=None, metavar="IP",
                   help="Single NVMe-oF client node (default: the cluster profile's first sn_ip)")
    p.add_argument("--members", type=int, default=10,
                   help="Total members created before deletion (default: 10)")
    p.add_argument("--delete-indices", default=None, metavar="I,I,...",
                   help="0-based indices (into the member list, name order) to delete "
                        "before migration, opening ns_id gaps (default: two indices "
                        "picked from the middle of --members, never index 0 since that's "
                        "the group's migrate handle)")
    p.add_argument("--size", default=LVOL_SIZE, help=f"Lvol size (default: {LVOL_SIZE})")
    p.add_argument("--target", default=None, help="Target node ID (default: any other online node)")
    p.add_argument("--fio-runtime", type=int, default=7200)
    return p.parse_args()


def fetch_ns_ids(pool_id, member_ids):
    """{lvol_id: ns_id} for every id in member_ids currently in the pool."""
    by_id = {}
    for lv in lib.sbctl_list("lvol", "list", "--pool", pool_id):
        lvol_id = lib.get(lv, "id")
        if lvol_id in member_ids:
            by_id[lvol_id] = lib.get(lv, "NS ID")
    return by_id


def main():
    args = parse_args()
    test_start_utc = datetime.now(timezone.utc)
    lib.init_logging(LOG_FILE)
    lib.log.info(f"Log file: {LOG_FILE}")
    lib.log.info(f"Test start (UTC): {test_start_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    _, sn_ips = bu.get_cluster(args.cluster)
    client_ip = args.client or sn_ips[0]

    n = args.members
    if args.delete_indices:
        delete_indices = sorted(int(x) for x in args.delete_indices.split(","))
    else:
        mid = n // 2
        delete_indices = sorted({max(1, mid - 1), min(n - 2, mid + 1)})
    if 0 in delete_indices:
        raise SystemExit("refusing to delete index 0 -- it's the group's migrate handle")

    names = [f"{NAME_PREFIX}_{i}" for i in range(n)]
    keep_indices = [i for i in range(n) if i not in delete_indices]

    bridge.enable(key_path=args.key, cluster=args.cluster, client_ip=client_ip)
    chk = lib.Checklist()
    try:
        cluster_id = bridge.discover_cluster_id()
        lib.log.info(f"Cluster ID: {cluster_id}")
        lib.log.info(f"Client    : {client_ip}")
        lib.log.info(f"Members   : {n} created, deleting indices {delete_indices} "
                    f"(names: {[names[i] for i in delete_indices]})")

        chk.step("Pre-flight: ensure nvme-cli/fio + nvme-tcp module on client")
        with bridge.as_client(client_ip):
            lib.local_run("command -v nvme || sudo dnf install -y nvme-cli "
                         "|| sudo apt-get install -y nvme-cli")
            lib.local_run("command -v fio || sudo dnf install -y fio "
                         "|| sudo apt-get install -y fio")
            lib.local_run("sudo modprobe nvme-tcp; echo modprobe_rc=$?")

        chk.step("0. Cleanup previous run")
        with bridge.as_client(client_ip):
            lib.local_run("sudo killall fio 2>/dev/null || true")
            lib.local_run("sudo nvme disconnect-all 2>/dev/null || true")
            lib.local_run(f"sudo umount {MOUNT_BASE}_* 2>/dev/null || true")
        lib.cleanup_pool_and_lvols(POOL, cluster_id, lvol_names=names)

        chk.step("1. Resolve source/target nodes")
        nodes = lib.get_online_nodes()
        if len(nodes) < 2:
            raise RuntimeError(f"Need >= 2 online nodes, got {len(nodes)}")
        node_ids = [lib.get(n_, "id") for n_ in nodes if lib.get(n_, "id")]
        # lib.pick_source_node() requires a node with both secondary AND
        # tertiary configured (full 3x HA) -- the "new" cluster only has 3
        # storage nodes total and isn't set up for that, so just take the
        # first online node; this test cares about ns_id gaps, not HA overlap.
        src_id = node_ids[0]
        tgt_id = args.target or lib.pick_random_target_node(src_id, node_ids)
        lib.log.info(f"  Source: {src_id[:8]}   Target: {tgt_id[:8]}")

        chk.step("2. Create pool")
        pool_id = lib.ensure_pool(POOL, cluster_id)

        chk.step(f"3. Create {n} shared-namespace members on {src_id[:8]}")
        member_ids = [lib.create_lvol(names[0], args.size, pool_id, src_id,
                                      namespaced=True, max_ns=n)]
        for name in names[1:]:
            member_ids.append(lib.create_lvol(name, args.size, pool_id, src_id,
                                              namespaced=True, max_ns=n))
        time.sleep(3)

        ns_ids_before_delete = fetch_ns_ids(pool_id, member_ids)
        lib.log.info("  ns_ids right after creation:")
        for i, (name, mid) in enumerate(zip(names, member_ids)):
            lib.log.info(f"    [{i}] {name} ({mid[:8]}): ns_id={ns_ids_before_delete.get(mid)}")

        chk.step(f"4. Delete members at indices {delete_indices} (opening ns_id gaps)")
        for i in delete_indices:
            lvol_id = member_ids[i]
            lib.log.info(f"  deleting [{i}] {names[i]} ({lvol_id[:8]})")
            lib.sbctl("volume", "delete", lvol_id, "--force")
            lib.wait_lvol_deleted(lvol_id, cluster_id)
        time.sleep(3)

        keep_ids = [member_ids[i] for i in keep_indices]
        keep_names = [names[i] for i in keep_indices]
        master_id = keep_ids[0]  # index 0 was never a deletion candidate

        ns_ids_source = fetch_ns_ids(pool_id, keep_ids)
        lib.log.info("  ns_ids after deletion (source side, pre-migration):")
        gap_seen = False
        prev_ns = None
        for i, mid in zip(keep_indices, keep_ids):
            ns_id = ns_ids_source.get(mid)
            lib.log.info(f"    [{i}] {names[i]} ({mid[:8]}): ns_id={ns_id}")
            if prev_ns is not None and ns_id is not None and ns_id != prev_ns + 1:
                gap_seen = True
            prev_ns = ns_id
        chk.check(gap_seen, "remaining members show a non-sequential ns_id gap "
                            "after deletion (source side)")

        chk.step(f"5. Connect/mount/fio the {len(keep_ids)} remaining member(s) from {client_ip}")
        members_subset = [{"id": mid, "name": names[i], "mount_point": f"{MOUNT_BASE}_{i}"}
                          for i, mid in zip(keep_indices, keep_ids)]
        fio_handles = []
        with bridge.as_client(client_ip):
            lib.connect_and_mount_batch_members(members_subset)
            for m in members_subset:
                fio_file = f"{m['mount_point']}/data.bin"
                fio_log = f"/tmp/fio_{m['name']}.log"
                lib.fio_prefill(fio_file, fio_log + ".pre", size=FIO_SIZE)
                handle = lib.start_fio_bg(fio_file, fio_log, size=FIO_SIZE,
                                          runtime=args.fio_runtime)
                fio_handles.append({"client_ip": client_ip, "fio_log": fio_log,
                                    "handle": handle, "name": m["name"], "lvol_id": m["id"]})
        chk.check(len(fio_handles) == len(keep_ids),
                 f"fio started on all {len(keep_ids)} remaining member(s)")
        time.sleep(10)

        chk.step("6. Batch-migrate the group")
        with bridge.as_client(client_ip):
            group_id = lib.start_batch_migration(master_id, tgt_id)
        lib.continue_batch_migration(group_id)
        lib.log.info(f"  Migration Group ID {group_id}")

        chk.step("7. Wait for migration to complete")
        status, phase = lib.wait_for_batch_migration(group_id, cluster_id)
        chk.check(status in ("done", "completed"),
                 f"migration completed (status={status}, phase={phase})")

        chk.step("8. Verify every remaining member landed on target")
        for i, mid in zip(keep_indices, keep_ids):
            node = lib.get_lvol_node(mid)
            chk.check(node == tgt_id, f"[{i}] {names[i]} on target (got {str(node)[:8]})")

        chk.step("9. Compare target ns_ids against the original source ns_ids")
        ns_ids_target = fetch_ns_ids(pool_id, keep_ids)
        for i, mid in zip(keep_indices, keep_ids):
            src_ns = ns_ids_source.get(mid)
            tgt_ns = ns_ids_target.get(mid)
            lib.log.info(f"    [{i}] {names[i]} ({mid[:8]}): source ns_id={src_ns}  "
                        f"target ns_id={tgt_ns}")
            chk.check(src_ns is not None and src_ns == tgt_ns,
                     f"[{i}] {names[i]}: target ns_id matches source "
                     f"(source={src_ns}, target={tgt_ns})")

        chk.step("9.5 Collect dmesg, sb_logs, and fio logs for the test period")
        artifacts_dir = LOG_DIR / f"artifacts_{test_start_utc.strftime('%Y%m%d_%H%M%S')}"
        artifacts_dir.mkdir(parents=True, exist_ok=True)

        lib.log.info(f"  Collecting dmesg from {client_ip}")
        out = bridge.collect_dmesg(client_ip)
        if out is not None:
            (artifacts_dir / f"dmesg_{client_ip}.log").write_text(
                out, encoding="utf-8", errors="replace")
            lib.log.info(f"    dmesg saved for {client_ip}")

        lib.log.info("  Checking fio output for corruption before downloading logs")
        for h in fio_handles:
            try:
                with bridge.as_client(h["client_ip"]):
                    ok, verify_errors = lib.check_fio_output(h["fio_log"])
            except Exception as e:  # noqa: BLE001 -- one node's check failing must not
                # abort artifact collection for the rest.
                lib.log.error(f"    {h['name']} ({h['client_ip']}): fio check raised {e!r}")
                ok, verify_errors = True, 0
            if not ok:
                local_path = artifacts_dir / f"fio_{h['name']}.json"
                bridge.download_remote_file(h["fio_log"], local_path, client_ip=h["client_ip"])
                lib.log.error(f"    {h['name']} ({h['client_ip']}): CORRUPTED "
                             f"(verify_errors={verify_errors}) -- fio log downloaded")
            else:
                lib.log.info(f"    {h['name']} ({h['client_ip']}): clean -- fio log not downloaded")

        import math
        elapsed_minutes = math.ceil((datetime.now(timezone.utc) - test_start_utc).total_seconds() / 60)
        duration_minutes = max(1, elapsed_minutes) + 5  # trailing buffer for
        # async cleanup/COMPLETED events that land after this collection call.
        start_dt_str = test_start_utc.strftime("%Y-%m-%d %H:%M:%S")
        lib.log.info(f"  Collecting sb_logs for window start={start_dt_str} UTC, "
                    f"duration={duration_minutes}m")
        bridge.collect_sb_logs(start_dt_str, duration_minutes, artifacts_dir)
        lib.log.info(f"  Artifacts saved under {artifacts_dir}")

        chk.step("10. Stop fio, verify no corruption")
        for h in fio_handles:
            with bridge.as_client(h["client_ip"]):
                lib.stop_fio(h["handle"], post_wait=0)
        for h in fio_handles:
            try:
                with bridge.as_client(h["client_ip"]):
                    ok, verify_errors = lib.check_fio_output(h["fio_log"])
                chk.check(ok, f"{h['name']}: no fio corruption (verify_errors={verify_errors})")
            except Exception as e:  # noqa: BLE001 -- one member's check failing must
                # not abort the whole run and lose every other member's result.
                lib.log.error(f"  {h['name']}: fio check raised {e!r}")
                chk.check(False, f"{h['name']}: fio check failed to run ({e})")

        ok = chk.summary()
        sys.exit(0 if ok else 1)
    finally:
        bridge.disable()


if __name__ == "__main__":
    main()
