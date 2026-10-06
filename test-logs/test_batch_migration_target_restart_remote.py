#!/usr/bin/env python3
"""
test_batch_migration_target_restart_remote.py

Fault-injection counterpart to test_batch_migration_source_fallback_remote.py:
starts a batch (shared-namespace) migration and, a few seconds in, restarts
the TARGET node so it goes offline mid-transfer -- the migration should
detect this, roll back cleanly (group phase -> cleanup_target -> failed),
and every member should stay on the source with no data loss/corruption.

What this checks
-----------------
  1. A shared-namespace batch group of 4 namespaces (master + 3 members) is
     created on --source, each namespace is connected/mounted and pre-filled
     with 1 GiB of data.
  2. Continuous background fio (randrw + verify=md5) keeps running on every
     member from connect through the whole test, so any data loss from the
     failure/rollback path shows up as a checksum mismatch.
  3. Batch migration --source -> --target is started (`lvol migrate --batch`
     then `migrate-continue --batch`).
  4. --restart-delay seconds after the migrate-continue call (default 2, so
     the fault lands squarely mid-flight rather than before anything is
     actually moving), a fault is injected against the TARGET node --
     --fault-type shutdown (default): `sbctl sn shutdown`, an administrative
     signal the cluster's own node.status checks react to immediately; or
     --fault-type nic_down: a real `ip link set <nic> down` on the target
     for --nic-down-seconds, auto-restoring itself -- closer to a genuine
     network partition, but detection depends on the cluster's health
     monitor noticing the heartbeat loss first, so it may land later/less
     deterministically than shutdown.
  5. The group must reach a terminal FAILED/CANCELLED status within
     --migration-timeout -- not TIMEOUT (stuck forever) and not
     done/completed (a cutover shouldn't succeed against a dead target).
  6. The target node is waited back to online.
  7. Every member must still be on the SOURCE node (rollback kept them
     there, nothing left half-migrated).
  8. fio is stopped and checked for verify errors on every member.
  9. dmesg, sb_logs, and any corrupted member's fio log are collected into
     an artifacts directory, same as test_batch_migration_tree_overlapb_remote.py.

This does not set up the cluster -- point --cluster at one that's already
running (see bridge_utils.CLUSTERS).

Usage (copy a private key into this folder named "simplyblock" first, or
pass --key to point at a different one):
  python3 test_batch_migration_target_restart_remote.py
  python3 test_batch_migration_target_restart_remote.py --namespaces 4 --restart-delay 2
  python3 test_batch_migration_target_restart_remote.py --target no-overlap --client 192.168.10.148
"""

import argparse
import math
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bridge_utils as bu
import migration_test_lib as lib
import remote_bridge_lib as bridge

POOL      = "batch-mig-tgt-restart-pool"
LVOL_SIZE = "5G"
FIO_SIZE  = "1G"
MOUNT_BASE = "/mnt/batch_mig_tgt_restart"

LOG_DIR  = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"batch_tgt_restart_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"


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
    p.add_argument("--namespaces", type=int, default=4,
                   help="Total namespace count, master + members (default: 4)")
    p.add_argument("--restart-delay", type=int, default=2, metavar="SEC",
                   help="Seconds after the migrate-continue call before "
                        "firing the fault, so it lands mid-flight (default: 2)")
    p.add_argument("--fault-type", choices=("shutdown", "nic_down"), default="shutdown",
                   help="How to take the target offline: 'shutdown' (sbctl sn "
                        "shutdown -- administrative, immediate) or 'nic_down' "
                        "(real ip link down on the data NIC, auto-restores) "
                        "[default: shutdown]")
    p.add_argument("--nic-down-seconds", type=int, default=45, metavar="SEC",
                   help="With --fault-type nic_down, how long the target's "
                        "data NIC stays down before auto-restoring [default: 45]")
    p.add_argument("--migration-timeout", type=int, default=300,
                   help="Seconds to wait for the group to reach a terminal "
                        "state [default: 300]")
    p.add_argument("--node-restart-timeout", type=int, default=300,
                   help="Seconds to wait for the target node to come back "
                        "online after the restart [default: 300]")
    return p.parse_args()


def main():
    args = parse_args()
    test_start_utc = datetime.utcnow()
    lib.init_logging(LOG_FILE)
    lib.log.info(f"Log file: {LOG_FILE}")
    lib.log.info(f"Test start (UTC): {test_start_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    n_ns = max(2, args.namespaces)
    lvol_names = [f"tgt_restart_ns_{i}" for i in range(n_ns)]
    _, sn_ips = bu.get_cluster(args.cluster)
    client_ip = args.client or sn_ips[0]

    bridge.enable(key_path=args.key, cluster=args.cluster, client_ip=client_ip)
    chk = lib.Checklist()
    fio_handles = []
    group_id = None
    try:
        cluster_id = bridge.discover_cluster_id()
        lib.log.info(f"Cluster ID: {cluster_id}")

        chk.step("Pre-flight: ensure nvme-cli/fio + nvme-tcp module on client")
        lib.local_run("command -v nvme || sudo dnf install -y nvme-cli || sudo apt-get install -y nvme-cli")
        lib.local_run("command -v fio || sudo dnf install -y fio || sudo apt-get install -y fio")
        lib.local_run("sudo modprobe nvme-tcp 2>/dev/null || true")

        chk.step("0. Cleanup previous run")
        lib.local_run("sudo killall fio 2>/dev/null || true")
        lib.local_run("sudo nvme disconnect-all 2>/dev/null || true")
        lib.local_run(f"sudo umount {MOUNT_BASE}_* 2>/dev/null || true")
        lib.cleanup_pool_and_lvols(POOL, cluster_id, lvol_names=lvol_names)

        chk.step("1. Pick source/target nodes")
        nodes = lib.get_online_nodes()
        if len(nodes) < 2:
            raise RuntimeError(f"Need >= 2 online nodes, got {len(nodes)}")
        src_id = lib.get(nodes[0], "id")
        tgt_id = lib.pick_target_node(src_id, nodes, args.target)
        lib.log.info(f"Source: {src_id}")
        lib.log.info(f"Target: {tgt_id}")
        chk.check(src_id != tgt_id, "Source and target nodes are distinct")

        chk.step(f"2. Create pool + {n_ns} shared-namespace lvols on source")
        pool_id = lib.ensure_pool(POOL, cluster_id)
        master_id = lib.create_lvol(lvol_names[0], LVOL_SIZE, pool_id, src_id,
                                    namespaced=True, max_ns=n_ns)
        members = [{"id": master_id, "name": lvol_names[0]}]
        for name in lvol_names[1:]:
            lv_id = lib.create_lvol(name, LVOL_SIZE, pool_id, src_id,
                                    namespaced=True, max_ns=n_ns)
            members.append({"id": lv_id, "name": name})
        chk.check(len(members) == n_ns, f"Created {n_ns} shared-namespace lvols")

        chk.step(f"3. Connect/mount all {n_ns} members, pre-fill each with "
                 f"{FIO_SIZE}, start background fio")
        for i, m in enumerate(members):
            m["mount_point"] = f"{MOUNT_BASE}_{i}"
        lib.connect_and_mount_batch_members(members)
        for m in members:
            fio_file = f"{m['mount_point']}/data.bin"
            fio_log = f"/tmp/fio_{m['name']}.log"
            lib.fio_prefill(fio_file, fio_log + ".pre", size=FIO_SIZE)
            handle = lib.start_fio_bg(fio_file, fio_log, size=FIO_SIZE)
            fio_handles.append({"fio_log": fio_log, "handle": handle,
                                "name": m["name"], "lvol_id": m["id"]})
        chk.check(len(fio_handles) == n_ns, f"fio started on all {n_ns} member(s)")

        chk.step("4. sbctl lvol list (pre-migration snapshot for the log)")
        lib.log.info("  sbctl lvol list:\n" + (lib.sbctl("lvol", "list") or "<empty>"))

        chk.step(f"5. Start batch migration {src_id} -> {tgt_id}")
        group_id = lib.start_batch_migration(master_id, tgt_id)
        chk.check(bool(group_id), "Got batch Migration Group ID")
        lib.continue_batch_migration(group_id)
        lib.log.info(f"Group: {group_id}")

        chk.step(f"6. Wait {args.restart_delay}s after migrate-continue, then "
                 f"fire fault-type={args.fault_type} against the TARGET node "
                 f"({tgt_id}) mid-flight")
        time.sleep(args.restart_delay)
        if args.fault_type == "shutdown":
            lib.log.info(f"  Shutting down target node {tgt_id} ...")
            lib.shutdown_node(tgt_id)
        else:
            node_ip_map = lib.build_node_ip_map(nodes)
            tgt_ip = node_ip_map.get(tgt_id)
            if not tgt_ip:
                raise RuntimeError(f"No management IP found for target {tgt_id}")
            nic_cmd = (
                f"nohup sh -c 'ip link set {lib.DATA_NIC} down"
                f" && sleep {args.nic_down_seconds}"
                f" && ip link set {lib.DATA_NIC} up' &"
            )
            lib.log.info(f"  Bringing {lib.DATA_NIC} down on target {tgt_id} "
                        f"({tgt_ip}) for {args.nic_down_seconds}s ...")
            try:
                lib.node_ssh(tgt_ip, nic_cmd, timeout=15)
            except Exception as e:
                lib.log.info(f"  nic_down SSH dropped (NIC is down, expected): {e}")

        chk.step("7. Wait for the group to reach a terminal state")
        status, phase = lib.wait_for_batch_migration(
            group_id, cluster_id, timeout=args.migration_timeout)
        lib.log.info(f"  Final status={status}  phase={phase}")
        chk.check(status in ("failed", "cancelled", "error"),
                 f"Migration correctly failed/rolled back after target restart "
                 f"(got status={status}, phase={phase})")

        chk.step(f"8. Wait for target node to come back online "
                 f"(timeout={args.node_restart_timeout}s)")
        if args.fault_type == "shutdown":
            # nic_down auto-restores itself after --nic-down-seconds; a
            # shutdown node stays down until explicitly restarted.
            lib.log.info(f"  Restarting target node {tgt_id} ...")
            lib.restart_node(tgt_id)
        back_online = lib.wait_node_status(tgt_id, timeout=args.node_restart_timeout)
        chk.check(back_online, f"Target node {tgt_id} back online")

        chk.step("9. Verify every member is still on the SOURCE node "
                 "(rollback should not have left anything stranded)")
        for m in members:
            node = lib.get_lvol_node(m["id"])
            chk.check(node == src_id, f"{m['name']} still on source (got {str(node)[:8]})")

        chk.step("10. Stop fio, check for data corruption on every member")
        artifacts_dir = LOG_DIR / f"artifacts_{test_start_utc.strftime('%Y%m%d_%H%M%S')}"
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        for h in fio_handles:
            lib.stop_fio(h["handle"], post_wait=10)
            ok, verify_errors = lib.check_fio_output(h["fio_log"])
            chk.check(ok, f"{h['name']}: no fio data corruption (verify_errors={verify_errors})")
            if not ok:
                local_path = artifacts_dir / f"fio_{h['name']}.json"
                bridge.download_remote_file(h["fio_log"], local_path, client_ip=client_ip)
                lib.log.error(f"    {h['name']}: fio log downloaded to {local_path}")
        fio_handles = []

        chk.step("11. Collect dmesg and sb_logs for the test period")
        lib.log.info(f"  Collecting dmesg from client {client_ip}")
        out = bridge.collect_dmesg(client_ip)
        if out is not None:
            (artifacts_dir / f"dmesg_{client_ip}.log").write_text(
                out, encoding="utf-8", errors="replace")
            lib.log.info(f"    dmesg saved for {client_ip}")

        elapsed_minutes = math.ceil((datetime.utcnow() - test_start_utc).total_seconds() / 60)
        duration_minutes = max(1, elapsed_minutes) + 5  # trailing buffer for
        # async cleanup/rollback events that land after this collection call.
        start_dt_str = test_start_utc.strftime("%Y-%m-%d %H:%M:%S")
        lib.log.info(f"  Collecting sb_logs for window start={start_dt_str} UTC, "
                    f"duration={duration_minutes}m")
        bridge.collect_sb_logs(start_dt_str, duration_minutes, artifacts_dir)
        lib.log.info(f"  Artifacts saved under {artifacts_dir}")

        ok = chk.summary()
        sys.exit(0 if ok else 1)
    finally:
        # Deliberately NOT deleting the pool/lvols here -- left in place after
        # the run (pass or fail) for manual inspection. The next run's own
        # "0. Cleanup previous run" step still cleans up from a PRIOR run
        # before creating fresh ones, so re-running is still safe.
        for h in fio_handles:
            try:
                lib.stop_fio(h["handle"], post_wait=0)
            except Exception as e:
                lib.log.warning(f"stop_fio failed for {h.get('name')}: {e}")
        lib.local_run(f"sudo umount {MOUNT_BASE}_* 2>&1 || true")
        lib.local_run("sudo nvme disconnect-all 2>&1 || true")
        bridge.disable()


if __name__ == "__main__":
    main()
