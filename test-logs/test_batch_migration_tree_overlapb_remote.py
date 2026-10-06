#!/usr/bin/env python3
"""
test_batch_migration_tree_overlapb_remote.py

Reproduces, alone and with no flags required, the "overlap-b" tree scenario
from the earlier multi-scenario generic-tree run (branches=2, depth=3 -> 15
nodes: 1 root + 2 + 4 + 8): a root lvol is snapshotted, a clone is taken
from that snapshot (joining the root's shared subsystem), that clone is
itself snapshotted, a clone taken from THAT snapshot joins too, and so on --
so every clone below the root gets its own snapshot(s) too, not just the
root.

overlap-b: source is target's HA secondary (source=vm08, target=vm07 by
default) -- this was the scenario that showed the heaviest real data
corruption (13/15 members) once the fio corruption-detection JSON-parsing
bug was fixed, so it's the one worth re-running in isolation.

Single group, single migration -- see test_batch_migration_tree_remote.py
for the separate fixed-tree (Lvol/Snap1/Snap2/Snap3/CloneB/CloneC)
order-of-operations script, and the git-less predecessor of this file (the
generic multi-scenario --pairs/--labels/--branches/--depth version) for the
full no-overlap/overlap-a/overlap-b sweep.

Same multi-client connect/mount/fio infrastructure as
test_batch_migration_groups_remote.py (see remote_bridge_lib.py). No
ANA-state verification here by design -- this only checks: the migration
reaches done/completed, every node ends up on the target, and fio
verify=md5 reports no corruption anywhere.

This does not set up the cluster -- point --cluster at one that's already
running (see bridge_utils.CLUSTERS).

Node placement uses stable vm hostnames (e.g. "vm07"), not uuids -- those are
re-generated on every redeploy, but hostname/IP position is fixed. The
actual HA relationship between --src/--tgt is read live from `sn list`, not
assumed, and logged so a mislabeled pair shows up as a loud mismatch instead
of silently testing the wrong scenario.

Usage (copy a private key into this folder named "simplyblock" first, or
pass --key to point at a different one):
  python3 test_batch_migration_tree_overlapb_remote.py
  python3 test_batch_migration_tree_overlapb_remote.py --branches 3 --depth 2
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
MOUNT_BASE = "/mnt/batch_mig_tree_ob"
ROOT_NAME  = "tree_overlap-b_root"

LOG_DIR  = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"batch_tree_overlapb_remote_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"


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
    p.add_argument("--single-client", default=None, metavar="IP",
                   help="Connect/mount/fio ALL tree nodes from just this one "
                        "client instead of round-robining across --clients "
                        "(default: the cluster profile's first sn_ip). "
                        "Overrides --clients.")
    p.add_argument("--src", default="vm08", metavar="VM",
                   help="Source vm hostname (default: vm08)")
    p.add_argument("--tgt", default="vm07", metavar="VM",
                   help="Target vm hostname (default: vm07 -- source is "
                        "target's HA secondary, i.e. overlap-b)")
    p.add_argument("--branches", type=int, default=2,
                   help="Snapshots (and clones taken from them) per node (default: 2)")
    p.add_argument("--depth", type=int, default=3,
                   help="Tree depth below the root (default: 3 -> 1+2+4+8=15 nodes, "
                        "matching the original overlap-b run)")
    p.add_argument("--size", default=LVOL_SIZE, help=f"Lvol size (default: {LVOL_SIZE})")
    p.add_argument("--fio-runtime", type=int, default=7200)
    p.add_argument("--check-interval", type=int, default=180, metavar="SEC",
                   help="Seconds between corruption checks in step 8 (default: 180)")
    p.add_argument("--monitor-duration", type=int, default=3600, metavar="SEC",
                   help="Max seconds to keep periodically checking in step 8, "
                        "unless interrupted first with Ctrl+C (default: 3600)")
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


def tree_plan(root_name, branching, depth):
    """Static (lvol_names, snap_names) for a tree of this shape, breadth-first,
    root first. Computable purely from the shape -- no cluster access needed --
    so cleanup can run against every name before anything is created.
    """
    lvol_names = [root_name]
    snap_names = []
    frontier = [root_name]
    for _ in range(depth):
        next_frontier = []
        for parent in frontier:
            for b in range(branching):
                snap_names.append(f"{parent}_snap{b}")
                child = f"{parent}_c{b}"
                lvol_names.append(child)
                next_frontier.append(child)
        frontier = next_frontier
    return lvol_names, snap_names


def build_snapshot_clone_tree(pool_id, host_id, root_name, size, max_ns, cluster_id,
                              branching, depth):
    """Create the root (namespaced) lvol, then walk the tree breadth-first:
    snapshot each node, clone that snapshot -- the clone joins the root's
    shared subsystem as a new namespace -- and recurse into the clone for the
    next level, so every clone below the root gets its own snapshot(s) too.

    Returns members in breadth-first, root-first order: [{"id", "name"}, ...].
    """
    lvol_names, _ = tree_plan(root_name, branching, depth)
    ids = {root_name: lib.create_lvol(root_name, size, pool_id, host_id,
                                      namespaced=True, max_ns=max_ns)}
    frontier = [root_name]
    for _ in range(depth):
        next_frontier = []
        for parent in frontier:
            parent_id = ids[parent]
            for b in range(branching):
                child = f"{parent}_c{b}"
                snap_name = f"{parent}_snap{b}"
                lib.sbctl("volume", "create-snapshot", parent_id, snap_name)
                time.sleep(3)
                snap = lib.get_snapshot(cluster_id, snap_name)
                snap_id = lib.get(snap, "id") if snap else None
                if not snap_id:
                    raise RuntimeError(f"snapshot {snap_name} (on {parent}) not "
                                       f"found after creation")
                lib.sbctl("snapshot", "clone", snap_id, child)
                time.sleep(3)
                clone_lv = lib.get_lvol_by_name(pool_id, child)
                clone_id = lib.get(clone_lv, "id") if clone_lv else None
                if not clone_id:
                    raise RuntimeError(f"clone {child} (from {snap_name}) not "
                                       f"found after creation")
                ids[child] = clone_id
                lib.log.info(f"    {child}: cloned from {snap_name} ({clone_id})")
                next_frontier.append(child)
        frontier = next_frontier
    return [{"id": ids[n], "name": n} for n in lvol_names]


def main():
    args = parse_args()
    CHECK_INTERVAL_S = args.check_interval
    MAX_MONITOR_S = args.monitor_duration
    test_start_utc = datetime.utcnow()
    lib.init_logging(LOG_FILE)
    lib.log.info(f"Log file: {LOG_FILE}")
    lib.log.info(f"Test start (UTC): {test_start_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    _, sn_ips = bu.get_cluster(args.cluster)
    client_ips = args.clients.split(",") if args.clients else sn_ips
    if args.single_client:
        client_ips = [args.single_client]

    lvol_names, snap_names = tree_plan(ROOT_NAME, args.branches, args.depth)
    tree_size = len(lvol_names)

    bridge.enable(key_path=args.key, cluster=args.cluster, client_ip=client_ips[0])
    chk = lib.Checklist()
    try:
        cluster_id = bridge.discover_cluster_id()
        lib.log.info(f"Cluster ID: {cluster_id}")
        lib.log.info(f"Clients   : {client_ips}")
        lib.log.info(f"Tree size : {tree_size} (1 root + branches={args.branches} "
                    f"to depth={args.depth})")

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
        lib.cleanup_pool_and_lvols(POOL, cluster_id, lvol_names=lvol_names,
                                   snap_names=snap_names)

        chk.step("1. Resolve source/target nodes")
        nodes = lib.get_online_nodes()
        if len(nodes) < 2:
            raise RuntimeError(f"Need >= 2 online nodes, got {len(nodes)}")
        src_id = bridge.resolve_by_hostname(nodes, args.src)
        tgt_id = bridge.resolve_by_hostname(nodes, args.tgt)
        relation = bridge.classify_pair(nodes, src_id, tgt_id)
        lib.log.info(f"  {args.src}({src_id[:8]}) -> {args.tgt}({tgt_id[:8]}): {relation}")
        chk.check(relation == "source_is_target_secondary",
                  f"{args.src} -> {args.tgt} is source_is_target_secondary (got {relation})")

        chk.step("2. Create pool")
        pool_id = lib.ensure_pool(POOL, cluster_id)

        chk.step(f"3. Build snapshot/clone tree ({tree_size} nodes)")
        members = build_snapshot_clone_tree(pool_id, src_id, ROOT_NAME, args.size,
                                            tree_size, cluster_id,
                                            args.branches, args.depth)
        master_id = members[0]["id"]

        chk.step(f"4. Connect/mount/fio the {tree_size} nodes across clients")
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
                    # Hammer (moderate): larger fixed block size + a modest
                    # iodepth bump is enough to blow past the 500 MiB
                    # intermediate-snapshot-round threshold within one
                    # round's ~10-15s window. iodepth=32/numjobs=4/bs=1M was
                    # tried first and way overshot what's needed -- up to 3
                    # nodes round-robin onto the same client, each running
                    # its fio job in the background for the whole 2h
                    # --fio-runtime, so that stacked up to ~384 concurrent
                    # 1M I/Os per client and saturated the client VMs badly
                    # enough that even just *starting* the next fio job took
                    # minutes. numjobs=1 keeps per-client concurrency sane.
                    handle = lib.start_fio_bg(fio_file, fio_log, size=FIO_SIZE,
                                              runtime=args.fio_runtime,
                                              iodepth=1, numjobs=1, bs="256k")
                    fio_handles.append({"client_ip": ip, "fio_log": fio_log,
                                        "handle": handle, "name": m["name"],
                                        "lvol_id": m["id"]})
            lib.log.info(f"  {len(members_subset)} node(s) on {ip}")
        chk.check(len(fio_handles) == len(members),
                 f"fio started on all {len(members)} tree node(s)")

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

        chk.step("7. Verify every tree node landed on target")
        for m in members:
            node = lib.get_lvol_node(m["id"])
            chk.check(node == tgt_id, f"node {m['name']} on target (got {str(node)[:8]})")

        chk.step("7.5 Collect dmesg, sb_logs, and fio logs for the test period")
        artifacts_dir = LOG_DIR / f"artifacts_{test_start_utc.strftime('%Y%m%d_%H%M%S')}"
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        used_client_ips = sorted({h["client_ip"] for h in fio_handles})

        lib.log.info(f"  Collecting dmesg from {len(used_client_ips)} client(s)")
        for ip in used_client_ips:
            out = bridge.collect_dmesg(ip)
            if out is not None:
                (artifacts_dir / f"dmesg_{ip}.log").write_text(out, encoding="utf-8", errors="replace")
                lib.log.info(f"    dmesg saved for {ip}")

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
        elapsed_minutes = math.ceil((datetime.utcnow() - test_start_utc).total_seconds() / 60)
        duration_minutes = max(1, elapsed_minutes) + 5  # trailing buffer for
        # async cleanup/COMPLETED events that land after this collection call.
        start_dt_str = test_start_utc.strftime("%Y-%m-%d %H:%M:%S")
        lib.log.info(f"  Collecting sb_logs for window start={start_dt_str} UTC, "
                    f"duration={duration_minutes}m")
        bridge.collect_sb_logs(start_dt_str, duration_minutes, artifacts_dir)
        lib.log.info(f"  Artifacts saved under {artifacts_dir}")

        chk.step(f"8. Periodic corruption check every {CHECK_INTERVAL_S}s for up to "
                 f"{MAX_MONITOR_S}s (or until interrupted) -- fio kept running throughout")
        # fio is deliberately never stopped here: --status-interval periodically
        # rewrites its JSON report while still running (see
        # remote_bridge_lib._last_json_object), so check_fio_output() can be
        # polled on a live process without needing to kill it first. fio is
        # left running when this loop ends (timeout or Ctrl+C) -- the next
        # run's own "0. Cleanup previous run" step already does killall fio.
        ever_corrupted = {h["name"]: False for h in fio_handles}
        round_num = 0
        start = time.time()
        try:
            while time.time() - start < MAX_MONITOR_S:
                round_num += 1
                elapsed = int(time.time() - start)
                lib.log.info(f"  --- corruption check round {round_num} (t+{elapsed}s) ---")
                for h in fio_handles:
                    try:
                        with bridge.as_client(h["client_ip"]):
                            ok, verify_errors = lib.check_fio_output(h["fio_log"])
                        if not ok:
                            ever_corrupted[h["name"]] = True
                            lib.log.error(f"    {h['name']} ({h['client_ip']}): "
                                         f"CORRUPTION DETECTED (verify_errors={verify_errors})")
                        else:
                            lib.log.info(f"    {h['name']} ({h['client_ip']}): "
                                        f"clean (verify_errors={verify_errors})")
                    except Exception as e:  # noqa: BLE001 -- one node's check failing
                        # (e.g. an SSH read timing out) must not abort the whole run.
                        lib.log.error(f"    {h['name']} ({h['client_ip']}): "
                                     f"fio check raised {e!r}")
                if time.time() - start >= MAX_MONITOR_S:
                    break
                time.sleep(CHECK_INTERVAL_S)
        except KeyboardInterrupt:
            lib.log.warning(f"  Interrupted by user after round {round_num} "
                            f"(t+{int(time.time() - start)}s) -- fio left running")

        for h in fio_handles:
            chk.check(not ever_corrupted[h["name"]],
                     f"{h['name']} ({h['client_ip']}): no fio corruption across "
                     f"{round_num} round(s)")

        ok = chk.summary()
        sys.exit(0 if ok else 1)
    finally:
        bridge.disable()


if __name__ == "__main__":
    main()
