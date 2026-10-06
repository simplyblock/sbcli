#!/usr/bin/env python3
"""
test_snap20_cancel_ha.py

Migration resilience test for a 20-snapshot lvol.

Iteration 1 — target HA restart, no cancel:
  - Migrate an lvol with 20 snapshots to a target node.
  - Once 15 snapshots have been transferred, restart a node from the
    TARGET's HA secondary or tertiary (never the target primary) — no
    cancel this time.
  - The migration must NOT fail because of this: verify it runs to
    completion (done/completed) and the cluster returns to healthy.

Iteration 2 (disabled for now) — cleanup_source HA restart:
  - Migrate the same lvol to the same target.
  - While the migration is in CLEANUP_SOURCE phase, restart a node from the
    SOURCE's HA secondary or tertiary (never the source primary).
  - Let the migration run to completion and verify it ends in done/completed.

Usage:
  python3 test_snap20_cancel_ha.py \\
      [--pool POOL] [--source SRC_NODE_ID] [--target TGT_NODE_ID] \\
      [--snap-count N] [--restart-at N] [--fault reboot|spdk_crash] \\
      [--log-dir DIR]
"""

import argparse
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import migration_test_lib as lib

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
_LOG_DIR   = Path(__file__).parent / "logs"
_LVOL_NAME = "snap20_ha_test"
_LVOL_SIZE = "10G"
_POOL_NAME = "test_pool_snap20"
_SNAP_PFX  = "snap20_"

# Iteration 2 (source HA restart during cleanup_source) is disabled for now.
RUN_ITERATION_2 = False


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--pool",      default=_POOL_NAME,
                   help="Storage pool name (created if absent)")
    p.add_argument("--source",    default=None,
                   help="Source node ID (auto-picked if omitted)")
    p.add_argument("--target",    default=None,
                   help="Target node ID (auto-picked if omitted)")
    p.add_argument("--snap-count", type=int, default=20, dest="snap_count",
                   help="Number of snapshots to create (default: 20)")
    p.add_argument("--restart-at", type=int, default=15, dest="restart_at",
                   help="Restart the target HA node after this many snaps "
                        "transferred in iter 1 (default: 15)")
    p.add_argument("--fault", default="reboot", choices=["reboot", "spdk_crash"],
                   help="Fault type to inject on the HA node (default: reboot)")
    p.add_argument("--log-dir", default=str(_LOG_DIR), dest="log_dir",
                   help="Directory for log output")
    return p.parse_args()


# ---------------------------------------------------------------------------
# HA node helpers
# ---------------------------------------------------------------------------
def _ha_node_for(node_id, nodes):
    """Return the secondary or tertiary ID for *node_id* (prefers secondary).
    Returns None if neither is configured.
    """
    sec = lib.get_node_secondary_id(node_id, nodes)
    if sec:
        return sec, "secondary"
    ter = lib.get_node_tertiary_id(node_id, nodes)
    if ter:
        return ter, "tertiary"
    return None, None


# ---------------------------------------------------------------------------
# Fault injection (direct — no phase-watcher thread)
# ---------------------------------------------------------------------------
def _fire_fault(fault_type, node_id, nodes, node_ip_map, cluster_id, label):
    """Execute *fault_type* on *node_id* immediately."""
    ip = node_ip_map.get(node_id, "")
    if not ip:
        for n in nodes:
            if lib.get(n, "id") == node_id:
                ip = (lib.get(n, "Management IP") or
                      lib.get(n, "mgmt_ip") or "")
                break
    if not ip:
        lib.log.error(f"[fault] no IP for {label} {node_id[:8]} — cannot fire")
        return

    lib.log.info(f"[fault] >>> {fault_type} on {label} ({ip}) <<<")
    if fault_type == "spdk_crash":
        rpc_port = 4420
        for n in nodes:
            if lib.get(n, "id") == node_id:
                rpc_port = int(lib.get(n, "rpc_port") or 4420)
                break
        cmd = (f"curl -sS 'http://0.0.0.0:5000/snode/spdk_process_kill"
               f"?rpc_port={rpc_port}&cluster_id={cluster_id}'")
        out, _, rc = lib.node_ssh(ip, cmd, timeout=30)
        lib.log.info(f"[fault] spdk_crash rc={rc}: {out[:120]}")
    elif fault_type == "reboot":
        try:
            lib.node_ssh(ip, "reboot", timeout=15)
        except Exception as e:
            lib.log.info(f"[fault] reboot SSH dropped (node rebooting): {e}")


# ---------------------------------------------------------------------------
# Phase-watching HA restart (background thread for iter 2)
# ---------------------------------------------------------------------------
def _watch_cleanup_source_and_restart(
        lvol_id, cluster_id, ha_node_id, nodes, node_ip_map,
        fault_type, label, fired_event, timeout=300, poll=2):
    """Wait until the migration enters CLEANUP_SOURCE, then fire the fault."""
    lib.log.info(f"[watcher] waiting for cleanup_source to restart {label} "
                 f"({ha_node_id[:8]})")
    deadline = time.time() + timeout
    while time.time() < deadline:
        m = lib.get_migration_record(lvol_id, cluster_id)
        if not m:
            lib.log.info("[watcher] migration record gone — stopping")
            break
        status = (lib.get(m, "status") or "").lower()
        phase  = (lib.get(m, "phase")  or "").lower()
        if status in ("done", "completed", "failed", "cancelled", "error"):
            lib.log.info(f"[watcher] migration terminal ({status}) — stopping")
            break
        if "cleanup" in phase:
            lib.log.info(f"[watcher] phase={phase!r} — firing {fault_type} on {label}")
            _fire_fault(fault_type, ha_node_id, nodes, node_ip_map, cluster_id, label)
            fired_event.set()
            return
        time.sleep(poll)
    if not fired_event.is_set():
        lib.log.warning(f"[watcher] cleanup_source never seen — fault NOT fired on {label}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    args = parse_args()

    log_dir = Path(args.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    lib.init_logging(log_dir / "test_snap20_cancel_ha.log")
    chk = lib.Checklist()

    # ------------------------------------------------------------------
    # Pre-run cleanup (leftovers from a previous failed run)
    # ------------------------------------------------------------------
    chk.step("Pre-run cleanup: remove any leftovers from a previous run")

    cluster_id_pre = lib.discover_cluster_id()
    if cluster_id_pre:
        snap_names_pre = [f"{_SNAP_PFX}{i}" for i in range(1, args.snap_count + 1)]
        try:
            lib.cleanup_pool_and_lvols(
                args.pool, cluster_id_pre,
                lvol_names=[_LVOL_NAME],
                snap_names=snap_names_pre,
            )
            lib.log.info("Pre-run cleanup done")
        except Exception as e:
            lib.log.warning(f"Pre-run cleanup non-fatal error: {e}")
    else:
        lib.log.warning("Could not discover cluster ID — skipping pre-run cleanup")

    # ------------------------------------------------------------------
    # Cluster / pool / node discovery
    # ------------------------------------------------------------------
    chk.step("Discover cluster, pool, and nodes")

    cluster_id = lib.discover_cluster_id()
    if not cluster_id:
        lib.log.error("Could not discover cluster ID — aborting")
        sys.exit(1)
    lib.log.info(f"Cluster: {cluster_id}")

    nodes = lib.get_online_nodes()
    node_ip_map = lib.build_node_ip_map(nodes)

    src_id = args.source or lib.pick_source_node(nodes)
    tgt_id = args.target or lib.pick_target_node(src_id, nodes, "no-overlap")
    lib.log.info(f"Source : {src_id}")
    lib.log.info(f"Target : {tgt_id}")

    # ------------------------------------------------------------------
    # Create lvol + snapshots (with data written between each snap so
    # SPDK has real blocks to transfer as special IOs during migration)
    # ------------------------------------------------------------------
    chk.step(f"Create lvol '{_LVOL_NAME}' ({_LVOL_SIZE}) with {args.snap_count} snapshots")

    pool_id = lib.ensure_pool(args.pool, cluster_id)
    lvol_id = lib.create_lvol(_LVOL_NAME, _LVOL_SIZE, pool_id, src_id)
    lib.log.info(f"Lvol: {lvol_id}")

    # Connect, format, and mount the lvol; run fio to fill real data so
    # SPDK has actual blocks to transfer as special IOs during migration.
    # Strategy: prefill 2 G sequentially, then take a snapshot every
    # ~10 s while a background randrw job keeps mutating the working set,
    # so each snapshot captures genuinely different allocated clusters.
    _MOUNT    = "/mnt/snap20_ha_test"
    _FIO_FILE = f"{_MOUNT}/fio_data.bin"
    _FIO_LOG  = str(log_dir / "fio_prefill.log")
    _FIO_BG_LOG = str(log_dir / "fio_randrw.log")

    lib.log.info(f"Connecting and mounting lvol at {_MOUNT} ...")
    device = lib.connect_and_mount_lvol(lvol_id, _MOUNT, already_formatted=False)
    lib.log.info(f"Device: {device}  mount: {_MOUNT}")

    lib.log.info("Pre-filling 2 G via fio (sequential write + verify) ...")
    lib.fio_prefill(_FIO_FILE, _FIO_LOG, size="2G")

    lib.log.info("Starting background randrw fio to keep mutating data between snaps ...")
    fio_bg = lib.start_fio_bg(_FIO_FILE, _FIO_BG_LOG, size="2G", runtime=3600)

    lib.log.info("Waiting 30 s for randrw fio to establish a dirty working set ...")
    time.sleep(30)

    lib.log.info(f"Creating {args.snap_count} snapshots "
                 f"(10 s apart, prefix={_SNAP_PFX!r})...")
    for i in range(1, args.snap_count + 1):
        time.sleep(10)
        lib.log.info(f"  snapshot {i}/{args.snap_count}: {_SNAP_PFX}{i}")
        lib.sbctl("volume", "create-snapshot", lvol_id, f"{_SNAP_PFX}{i}")

    lib.log.info(f"Snapshots: {_SNAP_PFX}1 .. {_SNAP_PFX}{args.snap_count}")

    # Stop fio, unmount, and disconnect before starting migration.
    lib.log.info("Stopping background fio ...")
    lib.stop_fio(fio_bg, post_wait=0)

    lib.log.info(f"Unmounting {_MOUNT} ...")
    lib.local_run(f"sudo umount {_MOUNT} 2>&1 || true")
    lib.log.info("Disconnecting lvol NVMe paths ...")
    lib.local_run("sudo nvme disconnect-all 2>&1 || true")
    time.sleep(2)

    chk.check(True, f"Lvol with {args.snap_count} snapshots ready on {src_id[:8]}")

    # ══════════════════════════════════════════════════════════════════
    # ITERATION 1 — restart target secondary/tertiary, no cancel
    # ══════════════════════════════════════════════════════════════════
    chk.step(f"Iter 1: migrate, restart target HA node at snap {args.restart_at} "
             f"({args.fault}), expect completion (no cancel)")

    tgt_ha_id, tgt_ha_role = _ha_node_for(tgt_id, nodes)
    if tgt_ha_id:
        lib.log.info(f"Target HA node ({tgt_ha_role}): {tgt_ha_id}")
    else:
        lib.log.warning(f"Target {tgt_id[:8]} has no secondary/tertiary configured — "
                        f"HA restart step will be skipped in iter 1")

    mig_id1 = lib.start_migration(lvol_id, tgt_id)
    lib.log.info(f"Iter 1 migration: {mig_id1}")
    lib.continue_migration(mig_id1)

    lib.log.info(f"Waiting for {args.restart_at}/{args.snap_count} snapshots copied...")
    reached = lib.wait_for_snap_count(
        lvol_id, cluster_id, target_count=args.restart_at, timeout=600)
    chk.check(reached,
              f"Reached {args.restart_at} snaps copied before migration went terminal")

    if tgt_ha_id:
        lib.log.info(f"[iter1] restarting target {tgt_ha_role} {tgt_ha_id[:8]} "
                     f"({args.fault})...")
        _fire_fault(args.fault, tgt_ha_id, nodes, node_ip_map, cluster_id,
                    f"target-{tgt_ha_role}")

    status1 = lib.wait_for_migration(
        lvol_id, cluster_id, terminal_only=True, timeout=600)
    chk.check(status1 in ("done", "completed"),
              f"Iter 1 migration completed despite target HA restart "
              f"(status={status1})")

    current_node = lib.get_lvol_node(lvol_id)
    chk.check(current_node == tgt_id,
              f"Lvol now on target {tgt_id[:8]} "
              f"(got {str(current_node)[:8]})")

    lib.log.info("Waiting for cluster to return healthy after iter 1...")
    healthy1 = lib.wait_cluster_healthy(timeout=360)
    chk.check(healthy1, "Cluster healthy after iter 1")

    # Refresh node list after the restart
    nodes = lib.get_online_nodes()
    node_ip_map = lib.build_node_ip_map(nodes)

    # ══════════════════════════════════════════════════════════════════
    # ITERATION 2 — full migration, restart source secondary/tertiary
    #               during CLEANUP_SOURCE
    # DISABLED FOR NOW — re-enable by flipping RUN_ITERATION_2 below.
    # ══════════════════════════════════════════════════════════════════
    if RUN_ITERATION_2:
        chk.step("Iter 2: migrate to completion, restart source HA node "
                 f"during cleanup_source ({args.fault})")

        src_ha_id, src_ha_role = _ha_node_for(src_id, nodes)
        if src_ha_id:
            lib.log.info(f"Source HA node ({src_ha_role}): {src_ha_id}")
        else:
            lib.log.warning(f"Source {src_id[:8]} has no secondary/tertiary configured — "
                            f"HA restart step will be skipped in iter 2")

        mig_id2 = lib.start_migration(lvol_id, tgt_id)
        lib.log.info(f"Iter 2 migration: {mig_id2}")
        lib.continue_migration(mig_id2)

        fired_event = threading.Event()
        if src_ha_id:
            watcher = threading.Thread(
                target=_watch_cleanup_source_and_restart,
                args=(lvol_id, cluster_id, src_ha_id, nodes, node_ip_map,
                      args.fault, f"source-{src_ha_role}", fired_event),
                daemon=True,
            )
            watcher.start()

        status2 = lib.wait_for_migration(
            lvol_id, cluster_id, terminal_only=True, timeout=600)

        if src_ha_id:
            watcher.join(timeout=30)
            chk.check(fired_event.is_set(),
                      "Source HA node restart was fired during cleanup_source")

        chk.check(status2 in ("done", "completed"),
                  f"Iter 2 migration completed successfully (status={status2})")

        current_node2 = lib.get_lvol_node(lvol_id)
        chk.check(current_node2 == tgt_id,
                  f"Lvol now on target {tgt_id[:8]} "
                  f"(got {str(current_node2)[:8]})")

        lib.log.info("Waiting for cluster to return healthy after iter 2...")
        healthy2 = lib.wait_cluster_healthy(timeout=360)
        chk.check(healthy2, "Cluster healthy after iter 2")
    else:
        lib.log.info("Iteration 2 disabled for now — skipping")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    ok = chk.summary()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
