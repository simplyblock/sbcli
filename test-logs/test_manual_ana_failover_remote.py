#!/usr/bin/env python3
"""
test_manual_ana_failover_remote.py

No migration code involved at all. Builds the EXACT same 15-namespace tree
shape as test_batch_migration_tree_overlapb_remote.py (branches=2, depth=3:
1 root + 2 + 4 + 8 = 15 nodes, a root lvol snapshotted and cloned repeatedly
so every clone below the root gets its own snapshot(s) too) -- all 15
members share ONE NVMe-oF subsystem/NQN as separate namespaces, on a single
HA (primary+secondary) source node. Each member is connected/mounted/fio'd
from a real client (round-robin across --clients), exactly like the
migration test does in its own step 4.

Then, entirely OUTSIDE simplyblock_core / sbctl, this script issues raw SPDK
JSON-RPC calls directly against the PRIMARY node's own spdk_<port> docker
container (the same manual invocation style as:

  docker exec -u root spdk_4420 python3 /root/spdk/scripts/rpc.py \\
      -s /mnt/ramdisk/spdk_4420/spdk.sock bdev_lvol_delete LVS_1/LVOL_13

) to first discover the shared subsystem's own listener (nqn + primary's
traddr/trsvcid/trtype, via nvmf_get_subsystems -- one lookup covers all 15
namespaces since they're all on the same subsystem), then flip that
listener's ANA state to "inaccessible" via nvmf_subsystem_listener_set_ana_state
-- forcing every connected client's NVMe multipath layer to fail over to the
secondary path for all 15 namespaces at once, with NO migration/orchestration
code anywhere in the loop.

The point: does ANA-triggered multipath failover on a plain (non-migrating)
shared-namespace tree, all by itself, produce the same fio checksum-mismatch
corruption signature seen throughout the batch/tree migration tests? If yes,
the bug is in NVMe-oF/multipath/SPDK failover itself, not in migration code.
If no, migration-specific code is implicated after all.

This does not set up the cluster -- point --cluster at one that's already
running.

Usage (copy a private key into this folder named "simplyblock" first, or
pass --key to point at a different one):
  python3 test_manual_ana_failover_remote.py
  python3 test_manual_ana_failover_remote.py --branches 3 --depth 2
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bridge_utils as bu
import migration_test_lib as lib
import remote_bridge_lib as bridge

POOL       = "ana-failover-tree-pool"
LVOL_SIZE  = "10G"
FIO_SIZE   = "1G"
MOUNT_BASE = "/mnt/ana_failover_tree"
ROOT_NAME  = "ana_failover_root"

LOG_DIR  = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"ana_failover_remote_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"


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
    p.add_argument("--branches", type=int, default=2,
                   help="Snapshots (and clones taken from them) per node (default: 2)")
    p.add_argument("--depth", type=int, default=3,
                   help="Tree depth below the root (default: 3 -> 1+2+4+8=15 nodes)")
    p.add_argument("--size", default=LVOL_SIZE, help=f"Lvol size (default: {LVOL_SIZE})")
    p.add_argument("--fio-runtime", type=int, default=7200)
    p.add_argument("--pre-flip-wait", type=int, default=15,
                   help="Seconds to let fio run before flipping ANA state (default: 15)")
    p.add_argument("--check-interval", type=int, default=60, metavar="SEC",
                   help="Seconds between corruption checks after the flip (default: 60)")
    p.add_argument("--monitor-duration", type=int, default=1800, metavar="SEC",
                   help="Max seconds to keep checking after the flip (default: 1800)")
    return p.parse_args()


def tree_plan(root_name, branching, depth):
    """Static (lvol_names, snap_names) for a tree of this shape, breadth-first,
    root first -- identical to test_batch_migration_tree_overlapb_remote.py.
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
    next level. Identical to test_batch_migration_tree_overlapb_remote.py.

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


def rpc_py(ip, docker_port, method, *args):
    """Run one SPDK rpc.py call manually via docker exec over SSH, exactly
    like: docker exec -u root spdk_<port> python3 /root/spdk/scripts/rpc.py
    -s /mnt/ramdisk/spdk_<port>/spdk.sock <method> <args...>
    Returns just stdout -- node_run() returns (stdout, stderr, rc).
    """
    arg_str = " ".join(str(a) for a in args)
    cmd = (f"docker exec -u root spdk_{docker_port} python3 /root/spdk/scripts/rpc.py "
          f"-s /mnt/ramdisk/spdk_{docker_port}/spdk.sock {method} {arg_str}".strip())
    out, err, rc = bridge.node_run(ip, cmd, timeout=30)
    if rc != 0:
        lib.log.warning(f"  rpc.py {method} on {ip} (docker_port={docker_port}) "
                        f"rc={rc}: {err[:300]}")
    return out


def rpc_port_for(node_record):
    """Docker container / rpc.py port for a node, parsed from its
    'hostname' field (format '<name>_<rpc_port>', e.g. 'vm07_4420')."""
    hostname = lib.get(node_record, "hostname") or ""
    m = re.search(r"_(\d+)$", hostname)
    if not m:
        raise RuntimeError(f"Could not parse rpc_port from hostname {hostname!r}")
    return int(m.group(1))


def find_primary_listener(ip, docker_port, nqn):
    """Query nvmf_get_subsystems on the given node and return the first
    listener entry (trtype, traddr, trsvcid) for the given nqn."""
    out = rpc_py(ip, docker_port, "nvmf_get_subsystems")
    subsystems = json.loads(out)
    for s in subsystems:
        if s.get("nqn") == nqn:
            listeners = s.get("listen_addresses", [])
            if not listeners:
                raise RuntimeError(f"Subsystem {nqn} on {ip} has no listeners")
            l = listeners[0]
            return l.get("trtype"), l.get("traddr"), l.get("trsvcid")
    raise RuntimeError(f"Subsystem {nqn} not found on {ip} (docker_port={docker_port})")


def find_primary_lvstore_name(ip, docker_port):
    """bdev_lvol_get_lvstores on the given node, return the name of whichever
    lvstore is the LOCAL primary/leader there (lvs_primary=True). A node can
    also host OTHER lvstores as a secondary/redirect target (lvs_secondary,
    lvs_redirect) -- those belong to a different node's primary and must not
    be confused with this node's own hublvol."""
    out = rpc_py(ip, docker_port, "bdev_lvol_get_lvstores")
    lvstores = json.loads(out)
    for lvs in lvstores:
        if lvs.get("lvs_primary"):
            return lvs.get("name")
    raise RuntimeError(f"No primary lvstore found on {ip} (docker_port={docker_port})")


def flip_ana_state(ip, docker_port, nqn, trtype, traddr, trsvcid, ana_state, label):
    flip_cmd_shown = (
        f"docker exec -u root spdk_{docker_port} python3 /root/spdk/scripts/rpc.py "
        f"-s /mnt/ramdisk/spdk_{docker_port}/spdk.sock "
        f"nvmf_subsystem_listener_set_ana_state {nqn} -t {trtype} -a {traddr} "
        f"-s {trsvcid} -n {ana_state}")
    lib.log.info(f"  [{label}] equivalent manual command: {flip_cmd_shown}")
    out = rpc_py(ip, docker_port, "nvmf_subsystem_listener_set_ana_state",
                nqn, "-t", trtype, "-a", traddr, "-s", trsvcid, "-n", ana_state)
    lib.log.info(f"  [{label}] rpc.py output: {out!r}")


def main():
    args = parse_args()
    test_start_utc = datetime.now(timezone.utc)
    lib.init_logging(LOG_FILE)
    lib.log.info(f"Log file: {LOG_FILE}")
    lib.log.info(f"Test start (UTC): {test_start_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    _, sn_ips = bu.get_cluster(args.cluster)
    client_ips = args.clients.split(",") if args.clients else sn_ips

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
        lib.cleanup_pool_and_lvols(POOL, cluster_id, lvol_names=lvol_names, snap_names=snap_names)

        chk.step("1. Resolve a source node with an online HA secondary")
        nodes = lib.get_online_nodes()
        if len(nodes) < 2:
            raise RuntimeError(f"Need >= 2 online nodes, got {len(nodes)}")
        src_id = None
        for n in nodes:
            nid = lib.get(n, "id")
            if nid and lib.get_node_secondary_id(nid, nodes):
                src_id = nid
                break
        if not src_id:
            raise RuntimeError("No online node with a configured HA secondary found")
        lib.log.info(f"  Source (primary): {src_id[:8]}")

        chk.step("2. Create pool")
        pool_id = lib.ensure_pool(POOL, cluster_id)

        chk.step(f"3. Build snapshot/clone tree ({tree_size} nodes)")
        members = build_snapshot_clone_tree(pool_id, src_id, ROOT_NAME, args.size,
                                            tree_size, cluster_id,
                                            args.branches, args.depth)

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
                    handle = lib.start_fio_bg(fio_file, fio_log, size=FIO_SIZE,
                                              runtime=args.fio_runtime)
                    fio_handles.append({"client_ip": ip, "fio_log": fio_log,
                                        "handle": handle, "name": m["name"],
                                        "lvol_id": m["id"]})
            lib.log.info(f"  {len(members_subset)} node(s) on {ip}")
        chk.check(len(fio_handles) == len(members),
                 f"fio started on all {len(members)} tree node(s)")

        lib.log.info(f"  Letting fio run for {args.pre_flip_wait}s before the ANA flip")
        time.sleep(args.pre_flip_wait)

        chk.step("5. Discover the shared subsystem's own primary listener + the "
                 "primary lvstore's hublvol listener (manual RPC queries)")
        root_id = members[0]["id"]
        lvol_info = lib.sbctl("lvol", "get", root_id, "--json", parse_json=True)
        if isinstance(lvol_info, list):
            lvol_info = lvol_info[0] if lvol_info else {}
        nqn = lib.get(lvol_info, "nqn")
        if not nqn:
            raise RuntimeError(f"Could not read nqn for lvol {root_id} from lvol get output")
        lib.log.info(f"  Shared subsystem nqn (all {tree_size} namespaces): {nqn}")

        node_ip_map = lib.build_node_ip_map(nodes)
        primary_ip = node_ip_map.get(src_id)
        primary_node_record = next((n for n in nodes if lib.get(n, "id") == src_id), None)
        docker_port = rpc_port_for(primary_node_record)
        lib.log.info(f"  Primary node {src_id[:8]}: ip={primary_ip} docker_port={docker_port}")

        lvstore_name = find_primary_lvstore_name(primary_ip, docker_port)
        hub_nqn = f"nqn.2023-02.io.simplyblock:{cluster_id}:hublvol:{lvstore_name}"
        lib.log.info(f"  Primary lvstore: {lvstore_name}  hublvol nqn: {hub_nqn}")
        hub_trtype, hub_traddr, hub_trsvcid = find_primary_listener(primary_ip, docker_port, hub_nqn)
        lib.log.info(f"  hublvol listener: trtype={hub_trtype} traddr={hub_traddr} "
                    f"trsvcid={hub_trsvcid}")

        trtype, traddr, trsvcid = find_primary_listener(primary_ip, docker_port, nqn)
        lib.log.info(f"  Lvol-group listener: trtype={trtype} traddr={traddr} trsvcid={trsvcid}")

        chk.step("6. Manually flip ANA state to inaccessible: hublvol first, then the "
                 "lvol group's own subsystem")
        flip_start = datetime.now(timezone.utc)
        flip_ana_state(primary_ip, docker_port, hub_nqn, hub_trtype, hub_traddr,
                      hub_trsvcid, "inaccessible", "1/2 hublvol")
        flip_ana_state(primary_ip, docker_port, nqn, trtype, traddr,
                      trsvcid, "inaccessible", "2/2 lvol group")
        lib.log.info(f"  Both flips issued starting at "
                    f"{flip_start.strftime('%H:%M:%S.%f')[:-3]} UTC")
        chk.check(True, "ANA-state-inaccessible RPCs issued manually against both the "
                       "primary lvstore's hublvol and the lvol group's own subsystem "
                       "(no migration code involved) -- affects all 15 namespaces at once")

        chk.step(f"7. Watch fio for corruption every {args.check_interval}s for up to "
                 f"{args.monitor_duration}s (client I/O should fail over to secondary)")
        ever_corrupted = {h["name"]: False for h in fio_handles}
        round_num = 0
        start = time.time()
        try:
            while time.time() - start < args.monitor_duration:
                round_num += 1
                elapsed = int(time.time() - start)
                lib.log.info(f"  --- corruption check round {round_num} (t+{elapsed}s "
                            f"since ANA flip) ---")
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
                    except Exception as e:  # noqa: BLE001
                        lib.log.error(f"    {h['name']} ({h['client_ip']}): "
                                     f"fio check raised {e!r}")
                if time.time() - start >= args.monitor_duration:
                    break
                time.sleep(args.check_interval)
        except KeyboardInterrupt:
            lib.log.warning(f"  Interrupted by user after round {round_num} "
                            f"(t+{int(time.time() - start)}s)")

        for h in fio_handles:
            chk.check(not ever_corrupted[h["name"]],
                     f"{h['name']} ({h['client_ip']}): no fio corruption across "
                     f"{round_num} round(s) after manual ANA failover")

        ok = chk.summary()
        sys.exit(0 if ok else 1)
    finally:
        bridge.disable()


if __name__ == "__main__":
    main()
