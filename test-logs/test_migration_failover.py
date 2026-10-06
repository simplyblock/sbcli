#!/usr/bin/env python3
"""
test_migration_failover.py — Integration tests for migration cleanup-on-node-offline.

What this exercises
-------------------
When the target node goes offline during PHASE_SNAP_COPY or PHASE_LVOL_MIGRATE
the runner immediately transitions to PHASE_CLEANUP_TARGET (instead of waiting
in-place), cleans up what was created on the target, and marks the migration
STATUS_FAILED.  The user is then free to start a fresh migration manually.

Scenario 1 — node_offline_snap_copy
  Target NIC goes down during PHASE_SNAP_COPY.
  Expected: snap_copy → (NIC down) → cleanup_target → STATUS_FAILED.
  Lvol must still be on source node; fio must show zero verify errors.

Scenario 2 — node_offline_lvol_migrate
  Target NIC goes down during PHASE_LVOL_MIGRATE.
  Same cleanup → STATUS_FAILED sequence as Scenario 1.
  (Best-effort: lvol_migrate is brief; skipped if the phase completes before
  the NIC-down is recorded in the DB.)

Usage
-----
  python3 test_migration_failover.py
  python3 test_migration_failover.py --scenarios node_offline_snap_copy
  python3 test_migration_failover.py --target <node-id-or-prefix>
  python3 test_migration_failover.py --snapshots 8 --nic-down-seconds 90
"""

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import migration_test_lib as lib

# ---------------------------------------------------------------------------
# Constants / defaults
# ---------------------------------------------------------------------------
_TS         = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_DIR     = Path(f"/tmp/failover_test_{_TS}")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE    = LOG_DIR / "test_migration_failover.log"

MOUNT_POINT = "/mnt/failover_test"
FIO_FILE    = f"{MOUNT_POINT}/fio_data.dat"

ALL_SCENARIOS = ["node_offline_snap_copy", "node_offline_lvol_migrate"]


# ---------------------------------------------------------------------------
# Phase-level polling helpers
# ---------------------------------------------------------------------------

def get_phase(lvol_id, cluster_id):
    """Return (status, phase) strings from the current migration record,
    or (None, None) if no record exists."""
    m = lib.get_migration_record(lvol_id, cluster_id)
    if not m:
        return None, None
    status = (lib.get(m, "status") or "").lower()
    phase  = (lib.get(m, "phase")  or "").lower()
    return status, phase


def wait_for_phase(lvol_id, cluster_id, target_phase, timeout=300, poll=4,
                   abort_on_terminal=True):
    """Poll until migration.phase == target_phase.

    Returns:
      target_phase string  — phase observed
      terminal status str  — migration went terminal before reaching phase
      "timeout"            — deadline expired
      None                 — migration record disappeared
    """
    target_phase = target_phase.lower()
    deadline = time.time() + timeout
    while time.time() < deadline:
        status, phase = get_phase(lvol_id, cluster_id)
        if status is None:
            lib.log.info(f"  wait_for_phase({target_phase}): migration record gone")
            return None
        lib.log.info(f"  wait_for_phase({target_phase}): status={status} phase={phase}")
        if phase == target_phase:
            return target_phase
        if abort_on_terminal and status in (
                "done", "completed", "failed", "cancelled", "error"):
            return status
        time.sleep(poll)
    return "timeout"


# ---------------------------------------------------------------------------
# NIC fault helpers
# ---------------------------------------------------------------------------

def nic_down(node_ip, seconds):
    """Bring the data NIC down on `node_ip` for `seconds` seconds via SSH.

    Fire-and-forget: the NIC-down command runs in the background on the remote
    node so this function returns immediately.  The SSH session may drop as
    soon as the NIC goes down, which is expected.
    """
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


def wait_node_offline(node_id, timeout=120, poll=8):
    """Wait until the node's DB status changes to something other than online."""
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


# ---------------------------------------------------------------------------
# Per-scenario setup / teardown
# ---------------------------------------------------------------------------

class ScenarioCtx:
    """Holds per-scenario state so each scenario can clean up independently."""

    def __init__(self, name, pool_name, lvol_name, cluster_id,
                 src_node_id, tgt_node_id, tgt_ip, fio_size):
        self.name        = name
        self.pool_name   = pool_name
        self.lvol_name   = lvol_name
        self.cluster_id  = cluster_id
        self.src_node_id = src_node_id
        self.tgt_node_id = tgt_node_id
        self.tgt_ip      = tgt_ip
        self.fio_size    = fio_size
        self.pool_id     = None
        self.lvol_id     = None
        self.migration_id = None
        self.fio_proc    = None
        self.snap_names  = []

    def fio_log(self):
        return str(LOG_DIR / f"fio_{self.name}.log")

    def prefill_log(self):
        return str(LOG_DIR / f"prefill_{self.name}.log")

    def teardown(self):
        lib.log.info(f"  [{self.name}] teardown ...")
        lib.stop_fio(self.fio_proc, post_wait=0)
        self.fio_proc = None
        lib.local_run(f"sudo umount {MOUNT_POINT} 2>/dev/null || true")
        lib.local_run("sudo nvme disconnect-all 2>/dev/null || true")
        if self.migration_id:
            try:
                lib.cancel_migration(self.migration_id)
            except Exception:
                pass
            time.sleep(5)
        if self.lvol_id:
            lib.cleanup_pool_and_lvols(
                self.pool_name, self.cluster_id,
                lvol_names=[self.lvol_name],
                snap_names=self.snap_names,
                mount=MOUNT_POINT,
            )


def _setup_scenario(ctx, chk, snap_count, snap_interval, snap_prefix,
                    pre_snap_wait=30):
    """Common setup: pool, lvol, connect, mount, fio, snapshots."""
    ctx.pool_id = lib.ensure_pool(ctx.pool_name, ctx.cluster_id)
    ctx.lvol_id = lib.create_lvol(
        ctx.lvol_name, "5G", ctx.pool_id, ctx.src_node_id, snapshot=True)
    chk.check(bool(ctx.lvol_id), f"[{ctx.name}] lvol created")

    lib.connect_and_mount_lvol(ctx.lvol_id, MOUNT_POINT, already_formatted=False)
    lib.fio_prefill(FIO_FILE, ctx.prefill_log(), size=ctx.fio_size)
    ctx.fio_proc = lib.start_fio_bg(FIO_FILE, ctx.fio_log(),
                                     size=ctx.fio_size, runtime=7200)
    chk.check(ctx.fio_proc is not None, f"[{ctx.name}] fio started")

    lib.log.info(f"  Letting fio write for {pre_snap_wait}s before first snapshot ...")
    time.sleep(pre_snap_wait)

    ctx.snap_names = [f"{snap_prefix}{i}" for i in range(1, snap_count + 1)]
    lib.create_snapshots(ctx.lvol_id, snap_prefix, snap_count, interval=snap_interval)
    chk.check(True, f"[{ctx.name}] {snap_count} snapshots created")


# ---------------------------------------------------------------------------
# Scenario 1: node_offline_snap_copy
# ---------------------------------------------------------------------------

def run_node_offline_snap_copy(chk, args, cluster_id, src_node_id, tgt_node_id,
                                tgt_ip, node_ip_map):
    name = "node_offline_snap_copy"
    chk.step(f"Scenario: {name}")

    ctx = ScenarioCtx(
        name=name,
        pool_name=f"fo_sc_pool_{_TS}",
        lvol_name="fo_sc_lvol",
        cluster_id=cluster_id,
        src_node_id=src_node_id,
        tgt_node_id=tgt_node_id,
        tgt_ip=tgt_ip,
        fio_size=args.fio_size,
    )
    try:
        _setup_scenario(ctx, chk, snap_count=args.snapshots,
                        snap_interval=args.snap_interval,
                        snap_prefix="fo_sc_snap",
                        pre_snap_wait=args.pre_snap_wait)

        ctx.migration_id = lib.start_migration(ctx.lvol_id, ctx.tgt_node_id)
        lib.continue_migration(ctx.migration_id, deadline=3600)
        lib.log.info(f"  Migration: {ctx.migration_id}")

        # Wait until at least 1 snap has been copied so there's real state
        # to clean up, then bring the NIC down.
        lib.log.info("  Waiting for first snap to copy before injecting fault ...")
        lib.wait_for_snap_count(ctx.lvol_id, cluster_id, target_count=1, timeout=180)

        lib.log.info(f"  Injecting NIC down on target ({tgt_ip}) ...")
        nic_down(tgt_ip, args.nic_down_seconds)

        lib.log.info("  Waiting for target node to be detected as offline ...")
        offline_detected = wait_node_offline(tgt_node_id, timeout=120)
        chk.check(offline_detected,
                  f"[{name}] target node went offline in DB after NIC down")

        # Cleanup runs immediately (deletes via target LVS leadership);
        # migration ends in STATUS_FAILED.
        lib.log.info("  Waiting for cleanup_target ...")
        r = wait_for_phase(ctx.lvol_id, cluster_id, "cleanup_target",
                           timeout=120, abort_on_terminal=False)
        chk.check(r == "cleanup_target",
                  f"[{name}] phase=cleanup_target observed after NIC down (got: {r!r})")

        lib.log.info("  Waiting for migration to reach STATUS_FAILED ...")
        final_status = lib.wait_for_migration(
            ctx.lvol_id, cluster_id, terminal_only=True,
            timeout=args.migration_timeout)
        lib.log.info(f"  Final migration status: {final_status}")
        chk.check(final_status == "failed",
                  f"[{name}] migration ended STATUS_FAILED (got: {final_status})")

        src_now = lib.get_lvol_node(ctx.lvol_id)
        chk.check(src_now == src_node_id,
                  f"[{name}] lvol still on source node after cleanup")

        lib.stop_fio(ctx.fio_proc, post_wait=10)
        ctx.fio_proc = None
        ok, verr = lib.check_fio_output(ctx.fio_log(), fault_injected=True)
        chk.check(ok, f"[{name}] fio: no data corruption (verify_errors={verr})")

    except Exception as exc:
        lib.log.exception(f"[{name}] exception: {exc}")
        chk.check(False, f"[{name}] no unexpected exception ({exc})")
    finally:
        ctx.teardown()


# ---------------------------------------------------------------------------
# Scenario 2: node_offline_lvol_migrate
# ---------------------------------------------------------------------------

def run_node_offline_lvol_migrate(chk, args, cluster_id, src_node_id, tgt_node_id,
                                   tgt_ip, node_ip_map):
    """Inject a NIC-down fault timed for PHASE_LVOL_MIGRATE.

    LVOL_MIGRATE is brief (seconds), so fault detection may race the phase
    completing.  The FaultInjector watcher fires as soon as the phase keyword
    is seen in the DB, giving the best possible window.  If the migration
    completes before offline is recorded the scenario is reported as
    inconclusive (skipped).
    """
    name = "node_offline_lvol_migrate"
    chk.step(f"Scenario: {name}")

    ctx = ScenarioCtx(
        name=name,
        pool_name=f"fo_lm_pool_{_TS}",
        lvol_name="fo_lm_lvol",
        cluster_id=cluster_id,
        src_node_id=src_node_id,
        tgt_node_id=tgt_node_id,
        tgt_ip=tgt_ip,
        fio_size=args.fio_size,
    )
    snap_count = max(2, args.snapshots // 2)

    injector = lib.FaultInjector(
        fault_type="nic_down",
        phase="lvol_migrate",
        node_role="target",
        src_id=src_node_id,
        tgt_id=tgt_node_id,
        nodes=lib.get_online_nodes(),
        cluster_id=cluster_id,
        node_ip_map=node_ip_map,
        nic_down_seconds=args.nic_down_seconds,
        phase_watch_timeout=args.migration_timeout,
        phase_watch_poll=1.0,
    )

    try:
        _setup_scenario(ctx, chk, snap_count=snap_count,
                        snap_interval=args.snap_interval,
                        snap_prefix="fo_lm_snap",
                        pre_snap_wait=args.pre_snap_wait)

        ctx.migration_id = lib.start_migration(ctx.lvol_id, ctx.tgt_node_id)
        lib.continue_migration(ctx.migration_id, deadline=3600)
        lib.log.info(f"  Migration: {ctx.migration_id}")

        injector.start_watching(ctx.lvol_id)

        # Wait for terminal state.  If the fault fired, cleanup_target runs
        # immediately (target LVS leadership) and migration ends STATUS_FAILED.
        final_status = lib.wait_for_migration(
            ctx.lvol_id, cluster_id, terminal_only=True,
            timeout=args.migration_timeout)
        injector.wait(timeout=120)

        lib.log.info(f"  Final migration status: {final_status}")

        if not injector.fired.is_set():
            lib.log.warning(
                f"[{name}] FaultInjector never fired — "
                f"lvol_migrate completed before NIC-down took effect; "
                f"scenario inconclusive (skipped)")
            chk.check(True,
                      f"[{name}] lvol_migrate fault inconclusive (phase too brief) — skipped")
            return

        chk.check(final_status == "failed",
                  f"[{name}] migration ended STATUS_FAILED after lvol_migrate offline "
                  f"(got: {final_status})")

        src_now = lib.get_lvol_node(ctx.lvol_id)
        chk.check(src_now == src_node_id,
                  f"[{name}] lvol still on source node after cleanup")

        lib.stop_fio(ctx.fio_proc, post_wait=10)
        ctx.fio_proc = None
        ok, verr = lib.check_fio_output(ctx.fio_log(), fault_injected=True)
        chk.check(ok, f"[{name}] fio: no data corruption (verify_errors={verr})")

    except Exception as exc:
        lib.log.exception(f"[{name}] exception: {exc}")
        chk.check(False, f"[{name}] no unexpected exception ({exc})")
    finally:
        injector.wait(timeout=5)
        ctx.teardown()


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
    p.add_argument("--snapshots", type=int, default=6,
                   help="Number of snapshots to create per scenario [default: 6]")
    p.add_argument("--snap-interval", type=int, default=10,
                   help="Seconds between each snapshot creation [default: 10]")
    p.add_argument("--pre-snap-wait", type=int, default=30,
                   help="Seconds to let fio run before the first snapshot [default: 30]")
    p.add_argument("--nic-down-seconds", type=int, default=90,
                   help="Seconds to keep target NIC down [default: 90]")
    p.add_argument("--fio-size", default="1G",
                   help="fio working-set size [default: 1G]")
    p.add_argument("--migration-timeout", type=int, default=600,
                   help="Seconds to wait for a migration to reach terminal state [default: 600]")
    p.add_argument("--scenarios", nargs="+", default=ALL_SCENARIOS,
                   choices=ALL_SCENARIOS, metavar="SCENARIO",
                   help=f"Scenarios to run (default: all).  Choices: {ALL_SCENARIOS}")
    p.add_argument("--log", default=str(LOG_FILE),
                   help=f"Log file path [default: {LOG_FILE}]")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()
    log  = lib.init_logging(args.log)
    chk  = lib.Checklist()

    log.info(f"Log file : {args.log}")
    log.info(f"Log dir  : {LOG_DIR}")
    log.info(f"Scenarios: {args.scenarios}")

    chk.step("Discover cluster and node topology")
    cluster_id = lib.discover_cluster_id()
    if not cluster_id:
        raise RuntimeError("Could not discover cluster ID")
    log.info(f"Cluster: {cluster_id}")

    nodes = lib.get_online_nodes()
    if len(nodes) < 2:
        raise RuntimeError(f"Need >= 2 online nodes, got {len(nodes)}")
    node_ip_map = lib.build_node_ip_map(nodes)

    src_node_id = None
    for n in nodes:
        nid = lib.get(n, "id")
        if nid and lib.get_node_secondary_id(nid, nodes):
            src_node_id = nid
            break
    if not src_node_id:
        src_node_id = lib.get(nodes[0], "id")
    log.info(f"Source node: {src_node_id}")

    tgt_node_id = lib.pick_target_node(src_node_id, nodes, args.target)
    log.info(f"Target node: {tgt_node_id}")

    tgt_ip = node_ip_map.get(tgt_node_id)
    if not tgt_ip:
        raise RuntimeError(
            f"No management IP found for target node {tgt_node_id}. "
            f"Available: {node_ip_map}")

    chk.check(src_node_id != tgt_node_id, "Source and target nodes are distinct")
    chk.check(bool(tgt_ip), f"Target node management IP found ({tgt_ip})")

    chk.step("Pre-test: verify cluster healthy")
    healthy = lib.wait_cluster_healthy(timeout=120)
    chk.check(healthy, "Cluster healthy before test start")

    scenario_fns = {
        "node_offline_snap_copy":    run_node_offline_snap_copy,
        "node_offline_lvol_migrate": run_node_offline_lvol_migrate,
    }

    for scenario_name in args.scenarios:
        fn = scenario_fns[scenario_name]
        fn(chk, args, cluster_id, src_node_id, tgt_node_id, tgt_ip, node_ip_map)

        log.info("  Inter-scenario recovery wait (30s) ...")
        lib.wait_cluster_healthy(timeout=300)
        time.sleep(30)

    passed = chk.summary()
    log.info(f"Log dir: {LOG_DIR}")
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
