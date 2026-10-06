#!/usr/bin/env python3
"""
test_migration_fail_and_retry.py

Verifies the fail-fast + manual-retry flow for lvol migration:

  1. Create an lvol, connect/mount it, and start continuous background fio
     (verify=md5) so it's actively writing data.
  2. Create --snapshots snapshots (default 6) while fio writes, giving the
     migration real state to work with.
  3. Start migrating to the target node.  Once at least --fault-after-snaps
     (default 1) snapshots have copied, bring the target NIC down.
  4. After the cluster detects the target node as offline the runner must
     enter PHASE_CLEANUP_TARGET immediately (no suspension) and end in
     STATUS_FAILED.
  5. Wait for the target node to recover (NIC auto-restores after
     --nic-down-seconds) and poll until it shows online in the DB.
  6. Start a second migration to the same target with no fault.  It must run
     to completion (STATUS_DONE / STATUS_COMPLETED) with the lvol now on the
     target node.
  7. Stop fio and check for data corruption.

Usage:
  python3 test_migration_fail_and_retry.py
  python3 test_migration_fail_and_retry.py --target no-overlap
  python3 test_migration_fail_and_retry.py --snapshots 8 --nic-down-seconds 120
"""

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import migration_test_lib as lib

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_TS         = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_DIR     = Path(f"/tmp/fail_and_retry_{_TS}")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE    = LOG_DIR / "test_migration_fail_and_retry.log"

POOL_NAME   = f"far_pool_{_TS}"
LVOL_NAME   = "far_lvol"
SNAP_PREFIX = "far_snap"
MOUNT_POINT = "/mnt/far_test"
FIO_FILE    = f"{MOUNT_POINT}/fio_data.dat"


# ---------------------------------------------------------------------------
# Phase polling helpers (same as test_migration_failover.py)
# ---------------------------------------------------------------------------

def get_phase(lvol_id, cluster_id):
    m = lib.get_migration_record(lvol_id, cluster_id)
    if not m:
        return None, None
    status = (lib.get(m, "status") or "").lower()
    phase  = (lib.get(m, "phase")  or "").lower()
    return status, phase


def wait_for_phase(lvol_id, cluster_id, target_phase, timeout=120, poll=4):
    """Poll until migration.phase == target_phase.

    Returns the observed phase string on success, or a sentinel:
      terminal-status string — migration went terminal before reaching phase
      "timeout"             — deadline expired without matching
      None                  — migration record disappeared
    """
    target_phase = target_phase.lower()
    deadline = time.time() + timeout
    while time.time() < deadline:
        status, phase = get_phase(lvol_id, cluster_id)
        if status is None:
            lib.log.info(f"  wait_for_phase({target_phase}): record gone")
            return None
        lib.log.info(f"  wait_for_phase({target_phase}): status={status} phase={phase}")
        if phase == target_phase:
            return target_phase
        if status in ("done", "completed", "failed", "cancelled", "error"):
            return status
        time.sleep(poll)
    return "timeout"


def wait_node_offline(node_id, timeout=120, poll=8):
    """Wait until the node's DB status is no longer online."""
    online_statuses = {"online", "active", "online_healthy"}
    deadline = time.time() + timeout
    while time.time() < deadline:
        for n in lib.sbctl_list("sn", "list"):
            if lib.get(n, "id") == node_id:
                s = (lib.get(n, "status") or "").lower()
                if s not in online_statuses:
                    lib.log.info(f"  node {node_id[:8]}: offline detected (status={s})")
                    return True
                break
        time.sleep(poll)
    lib.log.warning(f"  node {node_id[:8]}: never went offline within {timeout}s")
    return False


def nic_down(node_ip, seconds):
    """Bring the data NIC down on node_ip for `seconds` seconds — fire and forget."""
    cmd = (
        f"nohup sh -c 'ip link set {lib.DATA_NIC} down"
        f" && sleep {seconds}"
        f" && ip link set {lib.DATA_NIC} up' >/dev/null 2>&1 &"
    )
    lib.log.info(f"  [nic_down] {node_ip}: {lib.DATA_NIC} down for {seconds}s")
    try:
        lib.node_ssh(node_ip, cmd, timeout=15)
    except Exception as exc:
        lib.log.info(f"  [nic_down] SSH dropped (expected if NIC went down): {exc}")


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--target", default="no-overlap",
                   help="Target node: no-overlap | a | b | c | d | UUID-prefix "
                        "[default: no-overlap]")
    p.add_argument("--lvol-size", default="5G",
                   help="Lvol size [default: 5G]")
    p.add_argument("--fio-size", default="1G",
                   help="fio working-set size [default: 1G]")
    p.add_argument("--snapshots", type=int, default=6,
                   help="Snapshots to create before migrating [default: 6]")
    p.add_argument("--snap-interval", type=int, default=10,
                   help="Seconds between snapshot creations [default: 10]")
    p.add_argument("--pre-snap-wait", type=int, default=30,
                   help="Seconds to let fio write before the first snapshot [default: 30]")
    p.add_argument("--fault-after-snaps", type=int, default=1,
                   help="Inject NIC fault once this many snaps have copied [default: 1]")
    p.add_argument("--nic-down-seconds", type=int, default=90,
                   help="Seconds to keep target NIC down [default: 90]")
    p.add_argument("--offline-detect-timeout", type=int, default=120,
                   help="Seconds to wait for cluster to mark the target offline [default: 120]")
    p.add_argument("--cleanup-timeout", type=int, default=120,
                   help="Seconds to wait for cleanup_target after offline detected [default: 120]")
    p.add_argument("--first-migration-timeout", type=int, default=300,
                   help="Seconds to wait for the first migration to reach STATUS_FAILED [default: 300]")
    p.add_argument("--node-recovery-timeout", type=int, default=300,
                   help="Seconds to wait for the target node to come back online [default: 300]")
    p.add_argument("--retry-migration-timeout", type=int, default=1800,
                   help="Seconds to wait for the second migration to complete [default: 1800]")
    p.add_argument("--fio-post-wait", type=int, default=30,
                   help="Seconds to keep fio running after second migration completes [default: 30]")
    p.add_argument("--pool", default=POOL_NAME,
                   help=f"Pool name [default: {POOL_NAME}]")
    p.add_argument("--log", default=str(LOG_FILE),
                   help=f"Log file path [default: {LOG_FILE}]")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args  = parse_args()
    log   = lib.init_logging(args.log)
    chk   = lib.Checklist()
    snap_names = [f"{SNAP_PREFIX}{i}" for i in range(1, args.snapshots + 1)]

    log.info(f"Log dir : {LOG_DIR}")

    # --- Cluster and node discovery ---
    chk.step("0. Discover cluster and node topology")
    cluster_id = lib.discover_cluster_id()
    if not cluster_id:
        raise RuntimeError("Could not discover cluster ID")
    log.info(f"Cluster: {cluster_id}")

    nodes = lib.get_online_nodes()
    if len(nodes) < 2:
        raise RuntimeError(f"Need >= 2 online nodes, got {len(nodes)}")
    node_ip_map = lib.build_node_ip_map(nodes)

    src_node_id = next(
        (lib.get(n, "id") for n in nodes
         if lib.get_node_secondary_id(lib.get(n, "id"), nodes)),
        lib.get(nodes[0], "id"),
    )
    tgt_node_id = lib.pick_target_node(src_node_id, nodes, args.target)
    log.info(f"Source: {src_node_id}")
    log.info(f"Target: {tgt_node_id}")

    tgt_ip = node_ip_map.get(tgt_node_id)
    if not tgt_ip:
        raise RuntimeError(
            f"No management IP for target node {tgt_node_id}. "
            f"Available: {node_ip_map}")

    chk.check(src_node_id != tgt_node_id, "Source and target are distinct nodes")
    chk.check(bool(tgt_ip), f"Target management IP found ({tgt_ip})")

    healthy = lib.wait_cluster_healthy(timeout=120)
    chk.check(healthy, "Cluster healthy before test start")

    # --- Setup ---
    chk.step("1. Create pool, lvol, mount, and start background fio")
    pool_id = lib.ensure_pool(args.pool, cluster_id)
    lvol_id = lib.create_lvol(LVOL_NAME, args.lvol_size, pool_id, src_node_id, snapshot=True)
    chk.check(bool(lvol_id), f"Lvol created: {lvol_id}")

    lib.connect_and_mount_lvol(lvol_id, MOUNT_POINT, already_formatted=False)
    prefill_log = str(LOG_DIR / "fio_prefill.log")
    fio_log     = str(LOG_DIR / "fio_main.log")
    lib.fio_prefill(FIO_FILE, prefill_log, size=args.fio_size)
    fio_proc = lib.start_fio_bg(FIO_FILE, fio_log, size=args.fio_size, runtime=7200)
    chk.check(fio_proc is not None, "fio started in background")

    chk.step(f"2. Create {args.snapshots} snapshot(s) while fio writes")
    log.info(f"Letting fio write for {args.pre_snap_wait}s before first snapshot ...")
    time.sleep(args.pre_snap_wait)
    lib.create_snapshots(lvol_id, SNAP_PREFIX, args.snapshots, interval=args.snap_interval)
    chk.check(True, f"{args.snapshots} snapshot(s) created")

    # --- First migration: inject fault, assert fail-fast ---
    chk.step(f"3. Start first migration ({src_node_id[:8]} → {tgt_node_id[:8]}), "
             f"inject NIC fault after {args.fault_after_snaps} snap(s) copied")

    migration_id = lib.start_migration(lvol_id, tgt_node_id)
    log.info(f"Migration ID: {migration_id}")
    lib.continue_migration(migration_id, deadline=3600)

    log.info(f"Waiting for {args.fault_after_snaps} snap(s) to copy before injecting fault ...")
    reached = lib.wait_for_snap_count(lvol_id, cluster_id, args.fault_after_snaps,
                                      timeout=300)
    chk.check(reached,
              f"Migration copied {args.fault_after_snaps} snapshot(s) before fault injected")
    if not reached:
        log.warning("Snap threshold not reached — injecting fault anyway")

    nic_down(tgt_ip, args.nic_down_seconds)

    log.info("Waiting for cluster to detect target as offline ...")
    offline_detected = wait_node_offline(tgt_node_id, timeout=args.offline_detect_timeout)
    chk.check(offline_detected, "Target node detected as offline in DB")

    # Core assertion: cleanup_target entered quickly — no indefinite suspension
    log.info("Waiting for migration to enter PHASE_CLEANUP_TARGET ...")
    r = wait_for_phase(lvol_id, cluster_id, "cleanup_target",
                       timeout=args.cleanup_timeout, poll=4)
    chk.check(r == "cleanup_target",
              f"PHASE_CLEANUP_TARGET entered promptly after offline detected "
              f"(within {args.cleanup_timeout}s, got: {r!r})")

    log.info("Waiting for migration to reach STATUS_FAILED ...")
    first_status = lib.wait_for_migration(lvol_id, cluster_id, terminal_only=True,
                                          timeout=args.first_migration_timeout)
    log.info(f"First migration terminal status: {first_status}")
    chk.check(first_status == "failed",
              f"First migration ended STATUS_FAILED (got: {first_status!r})")

    chk.check(lib.get_lvol_node(lvol_id) == src_node_id,
              "Lvol still on source node after failed migration")

    # --- Wait for target node recovery ---
    chk.step("4. Wait for target node to recover")
    log.info(f"NIC auto-restores after {args.nic_down_seconds}s; "
             f"polling for node to come back online ...")
    recovered = lib.wait_node_status(tgt_node_id, timeout=args.node_recovery_timeout)
    chk.check(recovered,
              f"Target node back online within {args.node_recovery_timeout}s")

    healthy_after = lib.wait_cluster_healthy(timeout=120)
    chk.check(healthy_after, "Cluster healthy before second migration")

    # --- Second migration: no fault, must succeed ---
    chk.step(f"5. Retry migration ({src_node_id[:8]} → {tgt_node_id[:8]}) — no fault")
    retry_status = lib.migrate_lvol(
        lvol_id, tgt_node_id, cluster_id,
        deadline=3600,
        terminal_only=True,
        timeout=args.retry_migration_timeout,
    )
    log.info(f"Second migration terminal status: {retry_status}")
    chk.check(retry_status in ("done", "completed"),
              f"Second migration completed successfully (got: {retry_status!r})")

    if retry_status in ("done", "completed"):
        chk.check(lib.get_lvol_node(lvol_id) == tgt_node_id,
                  f"Lvol now on target node {tgt_node_id[:8]}")

    # --- fio integrity check ---
    chk.step("6. Stop fio and verify data integrity")
    lib.stop_fio(fio_proc, post_wait=args.fio_post_wait)
    fio_proc = None
    no_corruption, verify_errs = lib.check_fio_output(fio_log, fault_injected=True)
    chk.check(no_corruption,
              f"fio: no data corruption (verify_errors={verify_errs})")

    # --- Teardown ---
    lib.local_run(f"sudo umount {MOUNT_POINT} 2>/dev/null || true")
    lib.local_run("sudo nvme disconnect-all 2>/dev/null || true")
    lib.cleanup_pool_and_lvols(
        args.pool, cluster_id,
        lvol_names=[LVOL_NAME],
        snap_names=snap_names,
        mount=MOUNT_POINT,
    )

    passed = chk.summary()
    log.info(f"Log dir: {LOG_DIR}")
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
