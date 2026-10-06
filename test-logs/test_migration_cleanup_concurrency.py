#!/usr/bin/env python3
"""
test_migration_cleanup_concurrency.py

Verifies that while an lvol migration is executing PHASE_CLEANUP_TARGET, no
concurrent device-level data migrations are running on the involved nodes.

  1. Create an lvol with many snapshots (--snapshots, default 20) to give
     the cleanup phase enough bdev deletions that it stays in CLEANUP_TARGET
     long enough to observe.
  2. Start migrating to the target node; once --fault-after-snaps (default 1)
     snapshots have copied, bring the target NIC down so the migration
     immediately enters CLEANUP_TARGET.
  3. While CLEANUP_TARGET is active, poll `sbctl cluster list-tasks` on a
     tight loop and assert that no device-level migration task (function name:
     device_migration / failed_device_migration / new_device_migration) on
     either the source or target node has STATUS=running.
  4. Wait for CLEANUP_TARGET to complete → STATUS_FAILED.
  5. After the target node recovers (NIC auto-restores), run a second
     migration with no fault and verify it completes successfully.

Usage:
  python3 test_migration_cleanup_concurrency.py
  python3 test_migration_cleanup_concurrency.py --snapshots 20 --target no-overlap
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
LOG_DIR     = Path(f"/tmp/cleanup_concurrency_{_TS}")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE    = LOG_DIR / "test_migration_cleanup_concurrency.log"

POOL_NAME   = f"mcc_pool_{_TS}"
LVOL_NAME   = "mcc_lvol"
SNAP_PREFIX = "mcc_snap"
MOUNT_POINT = "/mnt/mcc_test"
FIO_FILE    = f"{MOUNT_POINT}/fio_data.dat"

# Function names as stored in the task DB.
_DATA_MIG_FUNCTIONS = frozenset(
    ["device_migration", "failed_device_migration", "new_device_migration"])


# ---------------------------------------------------------------------------
# NIC fault helpers (same pattern as test_migration_fail_and_retry.py)
# ---------------------------------------------------------------------------

def nic_down(node_ip, seconds):
    cmd = (
        f"nohup sh -c 'ip link set {lib.DATA_NIC} down"
        f" && sleep {seconds}"
        f" && ip link set {lib.DATA_NIC} up' >/dev/null 2>&1 &"
    )
    lib.log.info(f"  [nic_down] {node_ip}: {lib.DATA_NIC} down for {seconds}s")
    try:
        lib.node_ssh(node_ip, cmd, timeout=15)
    except Exception as exc:
        lib.log.info(f"  [nic_down] SSH dropped (expected): {exc}")


def wait_node_offline(node_id, timeout=120, poll=8):
    online_statuses = {"online", "active", "online_healthy"}
    deadline = time.time() + timeout
    while time.time() < deadline:
        for n in lib.sbctl_list("sn", "list"):
            if lib.get(n, "id") == node_id:
                s = (lib.get(n, "status") or "").lower()
                if s not in online_statuses:
                    lib.log.info(f"  node {node_id[:8]}: offline (status={s})")
                    return True
                break
        time.sleep(poll)
    lib.log.warning(f"  node {node_id[:8]}: never went offline within {timeout}s")
    return False


def get_phase(lvol_id, cluster_id):
    m = lib.get_migration_record(lvol_id, cluster_id)
    if not m:
        return None, None
    return (lib.get(m, "status") or "").lower(), (lib.get(m, "phase") or "").lower()


def wait_for_phase(lvol_id, cluster_id, target_phase, timeout=120, poll=4):
    target_phase = target_phase.lower()
    deadline = time.time() + timeout
    while time.time() < deadline:
        status, phase = get_phase(lvol_id, cluster_id)
        if status is None:
            return None
        lib.log.info(f"  wait_for_phase({target_phase}): status={status} phase={phase}")
        if phase == target_phase:
            return target_phase
        if status in ("done", "completed", "failed", "cancelled", "error"):
            return status
        time.sleep(poll)
    return "timeout"


# ---------------------------------------------------------------------------
# Task list helpers
# ---------------------------------------------------------------------------

def _parse_tasks_table(cluster_id, dump_raw=False):
    """Parse `sbctl cluster list-tasks <cluster_id> --limit 0` table output.

    Returns a list of dicts with lowercase 'function', 'status', 'node_id' keys.
    The table columns are:
      Task ID | Target ID | Function | Retry | Status | Result | Updated At
    Target ID can be "NodeID:<uuid>\\nDeviceID:..." or a master-task summary.
    """
    raw = lib.sbctl("cluster", "list-tasks", cluster_id, "--limit", "0")
    if dump_raw:
        lib.log.info(f"--- sbctl cluster list-tasks output ---\n{raw}\n--- end ---")
    tasks = []
    for line in (raw or "").splitlines():
        if "|" not in line or line.strip().startswith("+"):
            continue
        parts = [p.strip() for p in line.split("|")]
        # strip leading/trailing empty strings from the outer pipes
        parts = [p for p in parts if p != ""]
        if len(parts) < 5:
            continue
        # skip header row
        if parts[0].lower() in ("task id", "task_id"):
            continue
        target_raw = parts[1] if len(parts) > 1 else ""
        function   = parts[2].lower() if len(parts) > 2 else ""
        status     = parts[4].lower() if len(parts) > 4 else ""

        # Extract node_id from "NodeID:<uuid>" portion of the target field.
        node_id = ""
        for segment in target_raw.replace("\\n", "\n").splitlines():
            seg = segment.strip()
            if seg.startswith("NodeID:"):
                node_id = seg[len("NodeID:"):].strip()
                break

        tasks.append({
            "function": function,
            "status":   status,
            "node_id":  node_id,
            "raw":      parts,
        })
    return tasks


def get_running_data_migrations(cluster_id, node_ids, dump_raw=False):
    """Return task records for running device-level migration tasks on the
    given node IDs.  An empty list means none are running — which is what we
    assert during lvol migration cleanup."""
    tasks = _parse_tasks_table(cluster_id, dump_raw=dump_raw)
    running = []
    for t in tasks:
        if t["function"] not in _DATA_MIG_FUNCTIONS:
            continue
        if t["status"] != "running":
            continue
        if any(nid in t["node_id"] for nid in node_ids):
            running.append(t)
    return running


def poll_no_running_data_migrations(cluster_id, node_ids, duration, poll=5):
    """Poll for `duration` seconds; return (clean: bool, violations: list).

    `clean` is True only if ZERO running device migration tasks were observed
    on any of `node_ids` across ALL poll cycles during the window.
    """
    violations = []
    deadline = time.time() + duration
    cycles = 0
    while time.time() < deadline:
        running = get_running_data_migrations(cluster_id, node_ids)
        cycles += 1
        if running:
            lib.log.error(
                f"  [cycle {cycles}] RUNNING data migrations found during "
                f"lvol cleanup: {running}"
            )
            violations.extend(running)
        else:
            lib.log.info(
                f"  [cycle {cycles}] no running data migrations on "
                f"{[n[:8] for n in node_ids]} — OK"
            )
        time.sleep(poll)
    return len(violations) == 0, violations


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
    p.add_argument("--lvol-size", default="5G")
    p.add_argument("--fio-size", default="1G")
    p.add_argument("--snapshots", type=int, default=20,
                   help="Snapshots to create — more = slower cleanup, wider observation "
                        "window [default: 20]")
    p.add_argument("--snap-interval", type=int, default=8,
                   help="Seconds between snapshot creations [default: 8]")
    p.add_argument("--pre-snap-wait", type=int, default=30,
                   help="Seconds for fio to write before first snapshot [default: 30]")
    p.add_argument("--fault-after-snaps", type=int, default=1,
                   help="Inject NIC fault once this many snaps have copied [default: 1]")
    p.add_argument("--nic-down-seconds", type=int, default=120,
                   help="Seconds to keep target NIC down [default: 120]")
    p.add_argument("--offline-detect-timeout", type=int, default=120)
    p.add_argument("--cleanup-timeout", type=int, default=120,
                   help="Seconds to wait for cleanup_target phase to appear [default: 120]")
    p.add_argument("--first-migration-timeout", type=int, default=600,
                   help="Seconds to wait for the first migration to reach STATUS_FAILED "
                        "[default: 600]")
    p.add_argument("--concurrency-poll", type=int, default=5,
                   help="Polling interval (seconds) for the data-migration concurrency "
                        "check [default: 5]")
    p.add_argument("--node-recovery-timeout", type=int, default=300)
    p.add_argument("--retry-migration-timeout", type=int, default=1800)
    p.add_argument("--fio-post-wait", type=int, default=30)
    p.add_argument("--pool", default=POOL_NAME)
    p.add_argument("--log", default=str(LOG_FILE))
    # TEMPORARY DEBUG: hold CLEANUP_TARGET open for fault injection.
    # Remove once debugging is complete.
    p.add_argument("--cleanup-delay", type=int, default=0, dest="cleanup_delay_seconds",
                   help="[DEBUG] Seconds to sleep before CLEANUP_TARGET begins (default: 0)")
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
    tgt_ip      = node_ip_map.get(tgt_node_id)
    log.info(f"Source: {src_node_id}")
    log.info(f"Target: {tgt_node_id}  ip={tgt_ip}")

    if not tgt_ip:
        raise RuntimeError(f"No management IP for target node {tgt_node_id}")
    chk.check(src_node_id != tgt_node_id, "Source and target are distinct nodes")
    chk.check(lib.wait_cluster_healthy(timeout=120), "Cluster healthy before test start")

    chk.step(f"1. Create pool, lvol, mount, start fio, create {args.snapshots} snapshot(s)")
    pool_id = lib.ensure_pool(args.pool, cluster_id)
    lvol_id = lib.create_lvol(LVOL_NAME, args.lvol_size, pool_id, src_node_id, snapshot=True)
    chk.check(bool(lvol_id), f"Lvol created: {lvol_id}")

    lib.connect_and_mount_lvol(lvol_id, MOUNT_POINT, already_formatted=False)
    fio_log = str(LOG_DIR / "fio_main.log")
    lib.fio_prefill(FIO_FILE, str(LOG_DIR / "fio_prefill.log"), size=args.fio_size)
    fio_proc = lib.start_fio_bg(FIO_FILE, fio_log, size=args.fio_size, runtime=7200)
    chk.check(fio_proc is not None, "fio started in background")

    log.info(f"Letting fio write for {args.pre_snap_wait}s before first snapshot ...")
    time.sleep(args.pre_snap_wait)
    lib.create_snapshots(lvol_id, SNAP_PREFIX, args.snapshots, interval=args.snap_interval)
    chk.check(True, f"{args.snapshots} snapshot(s) created (slow cleanup payload)")

    chk.step(f"2. Start migration → inject NIC fault after {args.fault_after_snaps} snap(s)")
    migration_id = lib.start_migration(lvol_id, tgt_node_id)
    log.info(f"Migration ID: {migration_id}")
    lib.continue_migration(migration_id, deadline=3600,
                           cleanup_delay_seconds=args.cleanup_delay_seconds)

    reached = lib.wait_for_snap_count(lvol_id, cluster_id, args.fault_after_snaps, timeout=300)
    chk.check(reached, f"Migration copied {args.fault_after_snaps} snapshot(s) before fault")
    if not reached:
        log.warning("Snap threshold not reached — injecting fault anyway")

    nic_down(tgt_ip, args.nic_down_seconds)

    offline_detected = wait_node_offline(tgt_node_id, timeout=args.offline_detect_timeout)
    chk.check(offline_detected, "Target node detected as offline in DB")

    chk.step("3. Wait for CLEANUP_TARGET and assert no running data migrations during cleanup")
    r = wait_for_phase(lvol_id, cluster_id, "cleanup_target",
                       timeout=args.cleanup_timeout, poll=4)
    chk.check(r == "cleanup_target",
              f"PHASE_CLEANUP_TARGET entered promptly (got: {r!r})")

    if r == "cleanup_target":
        # Poll for the remainder of the cleanup phase and collect any violations.
        # We stop polling once the migration goes terminal (cleanup done) or
        # after first_migration_timeout, whichever comes first.
        log.info("Polling for running data migrations during CLEANUP_TARGET ...")
        node_ids_under_test = [src_node_id, tgt_node_id]

        poll_deadline = time.time() + args.first_migration_timeout
        violations_all = []
        while time.time() < poll_deadline:
            status, phase = get_phase(lvol_id, cluster_id)
            if status in ("failed", "cancelled", "done", "completed", "error") or status is None:
                log.info(f"  cleanup finished (status={status}); stopping concurrency poll")
                break
            running = get_running_data_migrations(
                cluster_id, node_ids_under_test, dump_raw=True)
            if running:
                log.error(f"  RUNNING data migrations during cleanup: {running}")
                violations_all.extend(running)
            else:
                log.info(
                    f"  status={status} phase={phase} — "
                    f"0 running data migrations on source/target nodes"
                )
            time.sleep(args.concurrency_poll)

        chk.check(
            len(violations_all) == 0,
            f"No running device-level migrations observed during PHASE_CLEANUP_TARGET "
            f"({len(violations_all)} violation(s))"
        )

    chk.step("4. Wait for first migration to reach STATUS_FAILED")
    first_status = lib.wait_for_migration(
        lvol_id, cluster_id, terminal_only=True,
        timeout=args.first_migration_timeout)
    log.info(f"First migration terminal status: {first_status}")
    chk.check(first_status == "failed",
              f"First migration ended STATUS_FAILED (got: {first_status!r})")
    chk.check(lib.get_lvol_node(lvol_id) == src_node_id,
              "Lvol still on source node after failed migration")

    chk.step("5. Wait for target node to recover")
    recovered = lib.wait_node_status(tgt_node_id, timeout=args.node_recovery_timeout)
    chk.check(recovered, f"Target node back online within {args.node_recovery_timeout}s")
    chk.check(lib.wait_cluster_healthy(timeout=120), "Cluster healthy before second migration")

    chk.step("6. Retry migration — no fault — must succeed")
    retry_status = lib.migrate_lvol(
        lvol_id, tgt_node_id, cluster_id,
        deadline=3600, terminal_only=True,
        timeout=args.retry_migration_timeout)
    log.info(f"Second migration terminal status: {retry_status}")
    chk.check(retry_status in ("done", "completed"),
              f"Second migration completed successfully (got: {retry_status!r})")
    if retry_status in ("done", "completed"):
        chk.check(lib.get_lvol_node(lvol_id) == tgt_node_id,
                  f"Lvol now on target node {tgt_node_id[:8]}")

    chk.step("7. Stop fio and verify data integrity")
    lib.stop_fio(fio_proc, post_wait=args.fio_post_wait)
    fio_proc = None
    no_corruption, verify_errs = lib.check_fio_output(fio_log, fault_injected=True)
    chk.check(no_corruption, f"fio: no data corruption (verify_errors={verify_errs})")

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
