#!/usr/bin/env python3
"""
test_batch_migration_groups_remote.py

Migrates several independent shared-namespace (batch) groups of 3-4 lvols
each, with each group's members connected/mounted/fio'd from multiple real
client nodes (not just one), driven entirely over SSH via bridge_utils (see
remote_bridge_lib.py). No ANA-state verification here by design — this only
checks: every group's migration reaches done/completed, every member ends
up on the target node, and fio verify=md5 reports no corruption anywhere.

This does not set up the cluster — point --cluster at one that's already
running (see bridge_utils.CLUSTERS).

Node placement uses stable vm hostnames (e.g. "vm07"), not uuids — those are
re-generated on every redeploy, but hostname/IP position is fixed. Each
group's actual HA relationship (no_overlap / target_is_source_secondary /
source_is_target_secondary / ...) is read live from `sn list`, not assumed,
and logged so a mislabeled --pairs entry shows up as a loud mismatch instead
of silently testing the wrong scenario.

Usage (copy a private key into this folder named "simplyblock" first, or
pass --key to point at a different one):
  python3 test_batch_migration_groups_remote.py
  python3 test_batch_migration_groups_remote.py --pairs vm07:vm12,vm07:vm08,vm08:vm07
  python3 test_batch_migration_groups_remote.py --groups 5 \\
      --clients 192.168.10.147,192.168.10.148,192.168.10.149
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

POOL       = "batch-mig-groups-pool"
LVOL_SIZE  = "10G"
FIO_SIZE   = "1G"
MOUNT_BASE = "/mnt/batch_mig_g"

LOG_DIR  = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"batch_groups_remote_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"


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
    p.add_argument("--groups", type=int, default=4,
                   help="Number of batch groups when --pairs is not given (default: 4); "
                        "all use the same auto-picked source/target")
    p.add_argument("--pairs", default="vm07:vm12,vm07:vm08,vm08:vm07", metavar="SRC:TGT,SRC:TGT,...",
                   help="One group per src:tgt vm-hostname pair (default: "
                        "vm07:vm12,vm07:vm08,vm08:vm07 -- no-overlap, target-is-source-"
                        "secondary, source-is-target-secondary). Pass an empty string "
                        "to fall back to --groups/--target with one auto-picked pair "
                        "shared by all groups instead.")
    p.add_argument("--members", default="3,4", metavar="N,N,...",
                   help="Member-count options, cycled across groups (default: 3,4 "
                        "-> group 0 has 3, group 1 has 4, group 2 has 3, ...)")
    p.add_argument("--size", default=LVOL_SIZE, help=f"Lvol size (default: {LVOL_SIZE})")
    p.add_argument("--target", default=None,
                   help="Target node ID, only used when --pairs is empty (default: "
                        "any other online node)")
    p.add_argument("--fio-runtime", type=int, default=7200)
    return p.parse_args()


def start_batch_migration_multi_client(master_id, target_node_id, client_ips,
                                       retries=5, retry_interval=10):
    """Same precreate-and-retry loop as migration_test_lib.start_batch_migration(),
    but a group's members can be connected from several different client nodes,
    so the TGT connect strings this returns must be run on every one of them —
    not just whichever client happens to be "current" when this runs, which is
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
    member_cycle = [int(x) for x in args.members.split(",")]
    pair_labels = ([tuple(p.split(":")) for p in args.pairs.split(",")]
                  if args.pairs.strip() else None)
    n_groups = len(pair_labels) if pair_labels else args.groups

    groups_spec = []
    for g in range(n_groups):
        n = member_cycle[g % len(member_cycle)]
        groups_spec.append([f"batchg{g}_ns_{i}" for i in range(n)])
    flat_names = [n for names in groups_spec for n in names]

    bridge.enable(key_path=args.key, cluster=args.cluster, client_ip=client_ips[0])
    chk = lib.Checklist()
    groups = []
    try:
        cluster_id = bridge.discover_cluster_id()
        lib.log.info(f"Cluster ID: {cluster_id}")
        lib.log.info(f"Clients   : {client_ips}")
        lib.log.info(f"Groups    : {[len(names) for names in groups_spec]} member(s) each")

        chk.step("Pre-flight: ensure nvme-cli/fio + nvme-tcp module on every client")
        for ip in set(client_ips):
            with bridge.as_client(ip):
                lib.local_run("command -v nvme || sudo dnf install -y nvme-cli "
                             "|| sudo apt-get install -y nvme-cli")
                lib.local_run("command -v fio || sudo dnf install -y fio "
                             "|| sudo apt-get install -y fio")
                # Loud on purpose: modprobe's own error was previously
                # thrown away by `2>/dev/null || true`, so a failure here
                # (module missing from the kernel entirely, vs. just not
                # loaded yet) looked identical to success.
                lib.local_run("sudo modprobe nvme-tcp; echo modprobe_rc=$?")
                lib.local_run("lsmod | grep -E '^nvme' || echo 'no nvme modules loaded'")
                lib.local_run("modinfo nvme_tcp 2>&1 | head -5 || true")
                lib.local_run("ls -la /dev/nvme-fabrics 2>&1 || true")

        chk.step("0. Cleanup previous run")
        for ip in set(client_ips):
            with bridge.as_client(ip):
                lib.local_run("sudo killall fio 2>/dev/null || true")
                lib.local_run("sudo nvme disconnect-all 2>/dev/null || true")
                lib.local_run(f"sudo umount {MOUNT_BASE}* 2>/dev/null || true")
        lib.cleanup_pool_and_lvols(POOL, cluster_id, lvol_names=flat_names)

        chk.step("1. Pick source / target nodes per group")
        nodes = lib.get_online_nodes()
        if len(nodes) < 2:
            raise RuntimeError(f"Need >= 2 online nodes, got {len(nodes)}")
        node_ids = [lib.get(n, "id") for n in nodes if lib.get(n, "id")]

        group_pairs = []   # (src_id, tgt_id) per group, index-aligned with groups_spec
        if pair_labels:
            for src_label, tgt_label in pair_labels:
                src_id = bridge.resolve_by_hostname(nodes, src_label)
                tgt_id = bridge.resolve_by_hostname(nodes, tgt_label)
                relation = bridge.classify_pair(nodes, src_id, tgt_id)
                lib.log.info(f"  {src_label}({src_id[:8]}) -> {tgt_label}({tgt_id[:8]}): "
                            f"{relation}")
                group_pairs.append((src_id, tgt_id))
        else:
            src_id = lib.pick_source_node(nodes)
            tgt_id = args.target or lib.pick_random_target_node(src_id, node_ids)
            lib.log.info(f"Source: {src_id}")
            lib.log.info(f"Target: {tgt_id}")
            group_pairs = [(src_id, tgt_id)] * n_groups

        chk.step("2. Create pool")
        pool_id = lib.ensure_pool(POOL, cluster_id)

        client_cycle_idx = 0
        for g, names in enumerate(groups_spec):
            src_id, tgt_id = group_pairs[g]
            chk.step(f"3.{g} Create batch group {g} ({len(names)} members) on "
                    f"{src_id[:8]} -> {tgt_id[:8]}, connect/mount/fio across clients")
            n = len(names)
            master_id = lib.create_lvol(names[0], args.size, pool_id, src_id,
                                        namespaced=True, max_ns=n)
            member_ids = [master_id]
            for name in names[1:]:
                member_ids.append(lib.create_lvol(name, args.size, pool_id, src_id,
                                                  namespaced=True, max_ns=n))

            # Round-robin members across clients, then group by client so each
            # client's shared subsystem gets connected exactly once (not once
            # per member landing on it) via connect_and_mount_batch_members(),
            # which discovers devices by NQN match rather than a before/after
            # delta -- the delta approach breaks the moment a client already
            # holds other members of the same subsystem.
            by_client = {}
            for i, (name, lvol_id) in enumerate(zip(names, member_ids)):
                ip = client_ips[client_cycle_idx % len(client_ips)]
                client_cycle_idx += 1
                by_client.setdefault(ip, []).append({
                    "id": lvol_id, "name": name,
                    "mount_point": f"{MOUNT_BASE}{g}_{i}",
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
                lib.log.info(f"  group {g}: {len(members_subset)} member(s) on {ip}")
            chk.check(len(fio_handles) == n, f"group {g}: fio started on all {n} member(s)")
            groups.append({"master_id": master_id, "member_ids": member_ids,
                          "tgt_id": tgt_id, "fio": fio_handles})

        time.sleep(15)

        chk.step(f"4. Batch-migrate all {len(groups)} group(s)")
        for g, grp in enumerate(groups):
            clients_in_group = sorted({h["client_ip"] for h in grp["fio"]})
            lib.log.info(f"  group {g}: -> {grp['tgt_id'][:8]}  clients={clients_in_group}")
            group_id = start_batch_migration_multi_client(grp["master_id"], grp["tgt_id"],
                                                           clients_in_group)
            lib.continue_batch_migration(group_id)
            grp["group_id"] = group_id
            lib.log.info(f"  group {g}: Migration Group ID {group_id}")

        chk.step("5. Wait for every group to complete")
        for g, grp in enumerate(groups):
            status, phase = lib.wait_for_batch_migration(grp["group_id"], cluster_id)
            chk.check(status in ("done", "completed"),
                     f"group {g} migration completed (status={status}, phase={phase})")

        chk.step("6. Verify every member landed on target")
        for g, grp in enumerate(groups):
            for lvol_id in grp["member_ids"]:
                node = lib.get_lvol_node(lvol_id)
                chk.check(node == grp["tgt_id"], f"group {g} member {lvol_id[:8]} on target "
                                                f"(got {str(node)[:8]})")

        chk.step("7. Stop fio, verify no corruption")
        for grp in groups:
            for h in grp["fio"]:
                with bridge.as_client(h["client_ip"]):
                    lib.stop_fio(h["handle"], post_wait=0)
        for g, grp in enumerate(groups):
            for h in grp["fio"]:
                try:
                    with bridge.as_client(h["client_ip"]):
                        ok, verify_errors = lib.check_fio_output(h["fio_log"])
                    chk.check(ok, f"group {g} member {h['name']} ({h['client_ip']}): "
                                 f"no fio corruption (verify_errors={verify_errors})")
                except Exception as e:  # noqa: BLE001 -- one member's check
                    # failing (e.g. an SSH read timing out) must not abort the
                    # whole run and lose every other member's result.
                    lib.log.error(f"  group {g} member {h['name']} ({h['client_ip']}): "
                                 f"fio check raised {e!r}")
                    chk.check(False, f"group {g} member {h['name']} ({h['client_ip']}): "
                                     f"fio check failed to run ({e})")

        ok = chk.summary()
        sys.exit(0 if ok else 1)
    finally:
        bridge.disable()


if __name__ == "__main__":
    main()
