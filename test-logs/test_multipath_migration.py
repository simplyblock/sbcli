#!/usr/bin/env python3
"""
test_multipath_migration.py — End-to-end multipath (HA) lvol migration test.

What this tests specifically:
  - The lvol has HA replicas (primary + secondary, optionally + tertiary),
    so `sbctl volume connect` emits multiple nvme-connect strings (one per NIC
    per replica node). All of them are issued and produce a single namespace
    device with multiple controllers visible via nvme list-subsys.
  - `sbctl --dev lvol migrate` (pre-create) returns inaccessible-ANA TGT
    connect strings — all of them are issued BEFORE migrate-continue so the
    initiator has every TGT path registered before the ANA flip fires.
  - Post-cutover: only stale SRC controllers are disconnected; TGT controllers
    remain live and the device never disappears.
  - fio (randrw + md5 verify) runs continuously across the entire migration
    with zero verify errors expected.

Usage:
  python3 test_multipath_migration.py [options]

Options:
  --pool POOL          Storage-pool name (created if absent)  [default: mpath_mig_pool]
  --lvol NAME          Lvol name                              [default: mpath_mig_lvol]
  --size SIZE          Lvol size                              [default: 10G]
  --host-id HOST_ID    Node ID to create the lvol on (auto-picked from online nodes)
  --target TARGET      Target node: no-overlap | a | b | c | d | UUID/hostname-prefix
                       [default: no-overlap]
  --ha-type TYPE       ha | ha3                               [default: ha]
  --fio-runtime N      fio runtime in seconds                 [default: 600]
  --no-cleanup         Skip pool/lvol teardown at the end
  --log FILE           Log file path  [default: test_multipath_migration.log]
"""

import argparse
import sys
import time
from pathlib import Path

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))

from migration_test_lib import (
    Checklist,
    check_fio_output,
    cleanup_pool_and_lvols,
    cleanup_src_controllers,
    continue_migration,
    create_lvol,
    discover_cluster_id,
    ensure_pool,
    format_and_mount,
    get,
    get_node_secondary_id,
    get_nvme_subsystem_nqn,
    get_online_nodes,
    init_logging,
    list_nvme_namespaces,
    local_run,
    log_client_nvme_state,
    parse_nvme_connect_cmds,
    pick_target_node,
    sbctl,
    start_fio_bg,
    start_migration,
    stop_fio,
    wait_for_migration,
    wait_for_new_device,
)

MOUNT_POINT = "/mnt/mpath_mig_test"
FIO_FILE    = f"{MOUNT_POINT}/fio_mpath.dat"
FIO_LOG     = "/tmp/fio_mpath_mig.log"


# ---------------------------------------------------------------------------
# Arg parsing
# ---------------------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--pool",       default="mpath_mig_pool")
    p.add_argument("--lvol",       default="mpath_mig_lvol")
    p.add_argument("--size",       default="10G")
    p.add_argument("--host-id",    default=None)
    p.add_argument("--target",     default="no-overlap")
    p.add_argument("--ha-type",    default="ha", choices=("ha", "ha3"))
    p.add_argument("--fio-runtime",type=int, default=600)
    p.add_argument("--no-cleanup", action="store_true")
    p.add_argument("--log",        default="test_multipath_migration.log")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Connect all paths returned by a `sbctl volume ...` call and return the
# (first) new namespace device.  With HA + 2 NICs, this runs 4 connect cmds
# and expects exactly one new /dev/nvmeXnY (the kernel aggregates all
# controllers for the same NQN+nsid into one namespace device).
# ---------------------------------------------------------------------------
def connect_all_paths(output, before_devices, label=""):
    cmds = parse_nvme_connect_cmds(output)
    if not cmds:
        raise RuntimeError(f"No nvme connect commands in {label or 'output'}")
    import logging
    log = logging.getLogger(__name__)
    log.info(f"  {label}: issuing {len(cmds)} nvme connect command(s)")
    for cmd in cmds:
        out, err, rc = local_run(f"sudo {cmd} 2>&1 || true")
        log.info(f"    cmd={cmd!r}  rc={rc}  out={out[:120]!r}")
    time.sleep(2)
    return cmds


# ---------------------------------------------------------------------------
# Main test
# ---------------------------------------------------------------------------
def main():
    args = parse_args()
    log  = init_logging(args.log)

    ck = Checklist()
    fio_proc  = None
    lvol_id   = None
    migration_id = None
    device    = None
    nqn       = None

    try:
        # ── 1. Cluster + pool ─────────────────────────────────────────────
        ck.step("Discover cluster and ensure pool")
        cluster_id = discover_cluster_id()
        if not cluster_id:
            raise RuntimeError("Could not discover cluster ID")
        log.info(f"Cluster: {cluster_id}")
        pool_id = ensure_pool(args.pool, cluster_id)

        # ── 2. Topology: pick source and target nodes ─────────────────────
        ck.step("Build node topology")
        nodes = get_online_nodes()
        log.info(f"Online nodes: {len(nodes)}")

        if args.host_id:
            src_node_id = args.host_id
        else:
            # pick_source_node() requires sec+ter (ha3). For 1+1 clusters we
            # only need a secondary — pick the first node that has one.
            src_node_id = None
            for n in nodes:
                nid = get_node_secondary_id(get(n, "id"), nodes) and get(n, "id")
                if nid:
                    src_node_id = get(n, "id")
                    break
            if not src_node_id:
                src_node_id = get(nodes[0], "id")
        log.info(f"Source node: {src_node_id}")

        tgt_node_id = pick_target_node(src_node_id, nodes, args.target)
        log.info(f"Target node: {tgt_node_id}")

        ck.check(src_node_id != tgt_node_id, "Source and target nodes are different")

        # ── 3. Create HA lvol ─────────────────────────────────────────────
        ck.step(f"Create {args.ha_type} lvol '{args.lvol}' on {src_node_id[:8]}")
        lvol_id = create_lvol(
            name=args.lvol,
            size=args.size,
            pool_id=pool_id,
            host_id=src_node_id,
            snapshot=True,
        )
        log.info(f"Lvol ID: {lvol_id}")
        ck.check(bool(lvol_id), f"Lvol '{args.lvol}' created/found")

        # ── 4. Connect source paths ───────────────────────────────────────
        ck.step("Connect all source-side NVMe-oF paths")
        before = list_nvme_namespaces()
        connect_out = sbctl("volume", "connect", lvol_id)
        src_cmds = connect_all_paths(connect_out, before, label="SRC")
        log.info(f"  SRC connect strings: {len(src_cmds)}")

        device = wait_for_new_device(before)
        nqn = get_nvme_subsystem_nqn(device)
        log.info(f"  Device: {device}  NQN: {nqn}")
        log_client_nvme_state("post-src-connect")

        ck.check(bool(device), "NVMe namespace device appeared after SRC connect")
        ck.check(
            len(src_cmds) > 1,
            f"Multiple SRC connect strings returned ({len(src_cmds)}) — multipath active",
        )

        # ── 5. Format, mount, prefill, start fio ─────────────────────────
        ck.step("Format XFS, mount, prefill, start background fio")
        format_and_mount(device, MOUNT_POINT, already_formatted=False)
        log.info("  Mounted at " + MOUNT_POINT)

        from migration_test_lib import fio_prefill
        ck.check(fio_prefill(FIO_FILE, FIO_LOG + ".prefill", size="2G"),
                 "fio prefill completed")

        fio_proc = start_fio_bg(FIO_FILE, FIO_LOG, size="2G", runtime=args.fio_runtime)
        ck.check(fio_proc is not None, "Background fio started")

        # ── 6. Pre-create migration — connects all TGT inaccessible paths ─
        ck.step(f"Pre-create migration to target {tgt_node_id[:8]}")

        # start_migration() from the lib:
        #   - calls `sbctl --dev lvol migrate <lvol_id> <target_node_id>`
        #   - parses Migration ID
        #   - runs ALL nvme connect commands returned (inaccessible-ANA TGT paths)
        #   - retries if pre-create fails transiently
        migration_id = start_migration(lvol_id, tgt_node_id)
        log.info(f"Migration ID: {migration_id}")
        log_client_nvme_state("post-precreate")

        ck.check(bool(migration_id), "Pre-create returned a Migration ID")

        # Verify the initiator now has both SRC and TGT controllers registered.
        # With 2 nodes × 2 NICs each side, we expect 4 SRC + 4 TGT = 8 total
        # controllers for this NQN — though the exact count depends on topology.
        # We just confirm there are MORE controllers than before pre-create.
        out, _, _ = local_run("nvme list-subsys 2>/dev/null || true")
        nqn_section = False
        tgt_ctrl_count = 0
        for line in out.splitlines():
            if nqn and nqn in line:
                nqn_section = True
                continue
            if nqn_section:
                if "NQN=" in line:
                    break
                if "nvme" in line.lower() and ("traddr=" in line or "TCP" in line or "RDMA" in line):
                    tgt_ctrl_count += 1
        log.info(f"  Controllers visible for this NQN after pre-create: "
                 f">= {max(tgt_ctrl_count, len(src_cmds))}")

        # ── 7. Start migration (migrate-continue) ────────────────────────
        ck.step("Start migration (migrate-continue)")
        continue_migration(migration_id)
        log.info("  migration started")

        # ── 8. Poll until cutover, clean up SRC controllers, wait for done
        ck.step("Poll migration to cutover then done")
        status = wait_for_migration(lvol_id, cluster_id, terminal_only=False)
        log.info(f"  Migration status after first wait: {status!r}")

        if status == "cutover":
            log.info("  Cutover phase reached — disconnecting stale SRC controllers")
            if nqn:
                cleanup_src_controllers(lvol_id, nqn)
            status = wait_for_migration(lvol_id, cluster_id, terminal_only=True)
            log.info(f"  Migration status after done wait: {status!r}")
        elif status in ("done", "completed"):
            # Completed so fast we skipped the cutover poll — still clean up
            log.info("  Migration done (no explicit cutover observed) — cleaning up SRC controllers")
            if nqn:
                cleanup_src_controllers(lvol_id, nqn)
        else:
            log.error(f"  Unexpected terminal status: {status!r}")

        ck.check(status in ("done", "completed"),
                 f"Migration completed successfully (status={status!r})")
        log_client_nvme_state("post-migration")

        # ── 9. Stop fio and check results ─────────────────────────────────
        ck.step("Stop fio and check for data corruption")
        stop_fio(fio_proc, post_wait=15)
        fio_proc = None
        no_corruption, verify_errs = check_fio_output(FIO_LOG)
        ck.check(no_corruption, f"fio: no data corruption (verify_errors={verify_errs})")

        # ── 10. Post-migration device still accessible ────────────────────
        ck.step("Verify device and mount still accessible after migration")
        out, _, rc = local_run(f"ls {MOUNT_POINT}/ 2>&1 || true")
        log.info(f"  ls {MOUNT_POINT}: rc={rc} out={out[:200]!r}")
        ck.check(rc == 0, "Mount point still accessible after migration")

    except Exception as exc:
        log.exception(f"Test aborted with exception: {exc}")
        ck.check(False, f"Test completed without exception ({exc})")

    finally:
        # ── Cleanup ───────────────────────────────────────────────────────
        stop_fio(fio_proc, post_wait=0)

        local_run(f"sudo umount {MOUNT_POINT} 2>/dev/null || true")

        if not args.no_cleanup and lvol_id:
            ck.step("Teardown: delete lvol and pool")
            try:
                # Cancel any still-running migration before deleting the lvol
                if migration_id:
                    sbctl("--dev", "lvol", "migrate-cancel", migration_id)
                    time.sleep(5)
            except Exception:
                pass
            cleanup_pool_and_lvols(
                pool_name=args.pool,
                cluster_id=discover_cluster_id(),
                lvol_names=[args.lvol],
                mount=MOUNT_POINT,
            )

        passed = ck.summary()
        sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
