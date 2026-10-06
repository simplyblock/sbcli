#!/usr/bin/env python3
"""
test_migration_fault_fio.py

Same chaos-migration scenario as test_migration_chaos.py — a fleet of 6
lvols (plain, crypto, various snap depths) migrates randomly across all
cluster nodes, with a random node fault injected at a random migration
phase every 3-4 iterations — but fio runs continuously on every fleet lvol
from setup until the very end of the test, instead of being started/
stopped fresh around each individual migration.

Fleet (created once, reused across all iterations):
  lvol_no_snapshot          plain,  0 snaps
  lvol_with_3_snapshots     plain,  3 snaps
  lvol_crypto_no_snapshot   crypto, 0 snaps
  lvol_crypto_2_snapshots   crypto, 2 snaps
  lvol_with_10_snapshots    plain, 10 snaps
  lvol_with_5_snapshots     plain,  5 snaps

Setup (once):
  1. Create the fleet (idempotent).
  2. Connect + mount + fio-prefill + start background fio (randrw+verify=md5,
     24h runtime) on every fleet lvol. None of this is ever repeated,
     stopped, or torn down between iterations.
  3. Create one extra "anchor" lvol PER ONLINE NODE, pinned to that node and
     never migrated, with its own continuous fio. Rationale: a node with no
     active IO passing through it can lose lvstore leadership while a fault
     (e.g. nic_down) has it unreachable; the next migration that picks it as
     a target then writes through a stale/non-leader view and corrupts the
     on-disk superblock (see LVS_16 crash-loop incident, 2026-07-21). Keeping
     every node steadily busy with its own anchor lvol's IO makes that
     leadership-loss window far less likely to be hit blind.

Each iteration:
  1. Pick a random lvol + random target node (not its current home).
  2. Issue migrate + connect TGT paths.
  3. Optionally arm a fault watcher thread (fires when target phase seen).
  4. migrate-continue.
  5. Wait for terminal state (done / failed / cancelled).
  6. Update the node map. fio on every lvol (including the one just
     migrated) keeps running the whole time — nothing is stopped,
     disconnected, or reformatted.

At the very end: stop fio on every fleet lvol and check each one's fio
output for verify errors (data corruption) accumulated across the entire
run.

Usage:
  python3 test_migration_fault_fio.py                    # run forever (10000 iters)
  python3 test_migration_fault_fio.py --iterations 50
  python3 test_migration_fault_fio.py --no-fault          # happy-path only
  python3 test_migration_fault_fio.py --setup-only        # create fleet and exit
  python3 test_migration_fault_fio.py --teardown          # destroy fleet and exit
"""

import argparse
import random
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import migration_test_lib as lib

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
POOL_NAME  = "fault-fio-pool"
LVOL_SIZE  = "10G"
FIO_SIZE   = "100M"
MOUNT_BASE = "/mnt"
FIO_RUNTIME = 24 * 3600   # fio must outlive the whole test, start to end

ANCHOR_PREFIX    = "fio_anchor"
ANCHOR_SIZE      = "2G"    # anchor lvol storage allocation
ANCHOR_FIO_SIZE  = "100M"  # fio data size per anchor (steady IO, no deep prefill needed)

TEST_TIMEOUT        = 3600   # server-side deadline passed to migrate-continue
MIGRATION_TIMEOUT   = 600    # hard deadline per migration
NIC_DOWN_SECONDS    = 45     # seconds the NIC stays down during nic_down fault
FIO_POST_TEST_WAIT  = 30     # seconds to keep fio running after the last iteration

FAULT_EVERY_MIN = 3          # min iters between faults
FAULT_EVERY_MAX = 4          # max iters between faults

FLEET_DEFS = [
    {"name": "lvol_no_snapshot",          "crypto": False, "snap_count": 0},
    {"name": "lvol_with_3_snapshots",     "crypto": False, "snap_count": 3},
    {"name": "lvol_crypto_no_snapshot",   "crypto": True,  "snap_count": 0},
    {"name": "lvol_crypto_2_snapshots",   "crypto": True,  "snap_count": 2},
    {"name": "lvol_with_10_snapshots",    "crypto": False, "snap_count": 10},
    {"name": "lvol_with_5_snapshots",     "crypto": False, "snap_count": 5},
]

# Namespaced lvols that share a subsystem and always migrate as a batch group.
BATCH_GROUP_DEFS = [
    {"name": "lvol_ns_a", "crypto": False},
    {"name": "lvol_ns_b", "crypto": False},
]

FAULT_TYPES  = ["spdk_crash", "reboot", "nic_down"]
FAULT_NODES  = ["source", "target", "both"]
FAULT_PHASES = ["snap_copy", "lvol_migrate", "cleanup_source"]

LOG_DIR  = Path("/tmp/fault_fio_logs") / datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "fault_fio.log"


# ---------------------------------------------------------------------------
# Fleet setup / teardown
# ---------------------------------------------------------------------------
def setup_fleet(pool_id, nodes):
    """Create fleet lvols idempotently. Returns list of lvol state dicts
    (not yet connected/mounted/fio'd).
    """
    fleet = []
    node_ids = [lib.get(n, "id") for n in nodes if lib.get(n, "id")]

    # Create the namespaced batch group FIRST so the server allocates a fresh
    # subsystem for them.  If regular fleet lvols are created first on the same
    # node, the server's --namespaced True logic greedily joins their existing
    # subsystem, polluting it with unrelated lvols.
    batch_home = node_ids[len(FLEET_DEFS) % len(node_ids)]
    batch_members = []
    batch_current_node = batch_home
    for defn in BATCH_GROUP_DEFS:
        name = defn["name"]
        existing = lib.get_lvol_by_name(pool_id, name)
        if existing:
            lvol_id           = lib.get(existing, "id")
            batch_current_node = lib.get_lvol_node(lvol_id) or batch_home
            lib.log.info(f"  {name}: exists (namespaced) on {batch_current_node}")
        else:
            lvol_id = lib.create_lvol(name, LVOL_SIZE, pool_id, batch_home,
                                      crypto=defn["crypto"],
                                      namespaced=True,
                                      max_ns=len(BATCH_GROUP_DEFS),
                                      snapshot=True)
            lib.log.info(f"  {name}: created on {batch_home} (namespaced)")
        batch_members.append({
            "id":          lvol_id,
            "name":        name,
            "mount_point": f"{MOUNT_BASE}/{name}",
        })

    fleet.append({
        "is_batch":      True,
        "id":            batch_members[0]["id"],   # representative for busy check
        "name":          "ns_group",
        "current_node":  batch_current_node,
        "batch_members": batch_members,
    })

    for i, defn in enumerate(FLEET_DEFS):
        name = defn["name"]
        home = node_ids[i % len(node_ids)]

        existing = lib.get_lvol_by_name(pool_id, name)
        if existing:
            lvol_id      = lib.get(existing, "id")
            current_node = lib.get_lvol_node(lvol_id) or home
            lib.log.info(f"  {name}: exists on {current_node}")
        else:
            lvol_id = lib.create_lvol(name, LVOL_SIZE, pool_id, home,
                                      crypto=defn["crypto"], snapshot=True)
            current_node = home
            if defn["snap_count"]:
                lib.create_snapshots(lvol_id, f"{name}_snap", defn["snap_count"])
            lib.log.info(f"  {name}: created on {current_node}"
                        + (f" with {defn['snap_count']} snaps" if defn["snap_count"] else ""))

        fleet.append({
            "id":           lvol_id,
            "name":         name,
            "crypto":       defn["crypto"],
            "snap_count":   defn["snap_count"],
            "current_node": current_node,
            "mount_point":  f"{MOUNT_BASE}/{name}",
        })

    return fleet


def setup_anchors(pool_id, nodes):
    """Create one idle-IO "anchor" lvol per online node, pinned to that node
    via --host-id and never touched by the migration loop. Returns a list of
    lvol state dicts (not yet connected/mounted/fio'd), same shape as the
    fleet's, minus migration bookkeeping.
    """
    anchors = []
    for n in nodes:
        node_id = lib.get(n, "id")
        if not node_id:
            continue
        name = f"{ANCHOR_PREFIX}_{node_id[-6:]}"

        existing = lib.get_lvol_by_name(pool_id, name)
        if existing:
            lvol_id = lib.get(existing, "id")
            lib.log.info(f"  {name}: exists on {node_id}")
        else:
            lvol_id = lib.create_lvol(name, ANCHOR_SIZE, pool_id, node_id, snapshot=False)
            lib.log.info(f"  {name}: created on {node_id}")

        anchors.append({
            "id":           lvol_id,
            "name":         name,
            "node_id":      node_id,
            "mount_point":  f"{MOUNT_BASE}/{name}",
        })
    return anchors


def teardown_fleet(pool_id, cluster_id, anchor_names=None):
    lib.log.info("Tearing down fleet...")
    lib.local_run("sudo killall fio 2>/dev/null || true")
    lib.local_run("sudo nvme disconnect-all 2>/dev/null || true")
    time.sleep(2)
    snap_names = [f"{d['name']}_snap{i}" for d in FLEET_DEFS
                 for i in range(1, d["snap_count"] + 1)]
    all_lvol_names = ([d["name"] for d in FLEET_DEFS]
                      + [d["name"] for d in BATCH_GROUP_DEFS]
                      + list(anchor_names or []))
    lib.cleanup_pool_and_lvols(POOL_NAME, cluster_id,
                               lvol_names=all_lvol_names,
                               snap_names=snap_names)
    lib.log.info("Teardown complete")


def connect_mount_fio_lvol(lvol, args, fio_size=None):
    """Connect, mount, prefill, and start continuous background fio on one
    lvol. Shared by both the migrating fleet and the non-migrating anchors —
    once run, nothing here is ever repeated or stopped until the test ends.
    """
    fio_size = fio_size or args.fio_size
    lib.log.info(f"  {lvol['name']}: connecting + mounting...")
    device = lib.connect_and_mount_lvol(lvol["id"], lvol["mount_point"],
                                        already_formatted=False)
    fio_file = f"{lvol['mount_point']}/fio_data.0.0"
    prefill_log = str(LOG_DIR / f"fio_{lvol['name']}_prefill.log")
    fio_log     = str(LOG_DIR / f"fio_{lvol['name']}.log")

    lib.fio_prefill(fio_file, prefill_log, size=fio_size)
    proc = lib.start_fio_bg(fio_file, fio_log, size=fio_size, runtime=FIO_RUNTIME)

    lvol.update(device=device, fio_log=fio_log, fio_proc=proc)
    lib.log.info(f"  {lvol['name']}: mounted {device} -> {lvol['mount_point']}, "
                 f"fio running (pid={proc.pid if proc else 'n/a'})")


def start_fleet_fio(fleet, args):
    """connect_mount_fio_lvol() for every fleet lvol, before the chaos loop
    begins. Batch groups use a single shared-subsystem connect call then fio
    is started per member.
    """
    for lvol in fleet:
        if lvol.get("is_batch"):
            fio_size = args.fio_size
            members  = lvol["batch_members"]
            lib.log.info(f"  ns_group: connecting {len(members)} namespaced members...")
            lib.connect_and_mount_batch_members(members)
            for member in members:
                fio_file    = f"{member['mount_point']}/fio_data.0.0"
                prefill_log = str(LOG_DIR / f"fio_{member['name']}_prefill.log")
                fio_log     = str(LOG_DIR / f"fio_{member['name']}.log")
                lib.fio_prefill(fio_file, prefill_log, size=fio_size)
                proc = lib.start_fio_bg(fio_file, fio_log, size=fio_size,
                                        runtime=FIO_RUNTIME)
                member.update(fio_log=fio_log, fio_proc=proc)
                lib.log.info(
                    f"  {member['name']}: fio running "
                    f"(pid={proc.pid if proc else 'n/a'})")
        else:
            connect_mount_fio_lvol(lvol, args)


def start_anchor_fio(anchors, args):
    """connect_mount_fio_lvol() for every per-node anchor lvol."""
    for lvol in anchors:
        connect_mount_fio_lvol(lvol, args, fio_size=ANCHOR_FIO_SIZE)


# ---------------------------------------------------------------------------
# Single chaos iteration — pure migration mechanics, no connect/mount/fio
# ---------------------------------------------------------------------------
def run_iteration(lvol, target_node_id, nodes, cluster_id, node_ip_map, fault_injector=None):
    """Migrate `lvol` to `target_node_id`. fio (already running on this lvol
    since setup) is left completely untouched throughout.

    Returns migration_status: str. Raises on unrecoverable setup failures.
    """
    lvol_id   = lvol["id"]
    lvol_name = lvol["name"]

    lib.log.info(f"  lvol   : {lvol_name} ({lvol_id[-8:]})")
    lib.log.info(f"  src    : {lvol['current_node']}")
    lib.log.info(f"  target : {target_node_id}")

    migration_id = lib.start_migration(lvol_id, target_node_id)
    lib.log.info(f"  mig_id : {migration_id}")

    if fault_injector:
        fault_injector.start_watching(lvol_id)

    lib.continue_migration(migration_id, deadline=TEST_TIMEOUT)
    lib.log.info("  migrate-continue sent")

    extra = NIC_DOWN_SECONDS + 60 if fault_injector else 0
    status = lib.wait_for_migration(lvol_id, cluster_id, terminal_only=True,
                                    timeout=MIGRATION_TIMEOUT + extra)
    lib.log.info(f"  migration terminal: {status}")

    if status == "timeout":
        # The server-side deadline (TEST_TIMEOUT, 1h) is far longer than our
        # own polling budget above — a migration that outlasts our patience
        # is still perfectly "active" server-side and will reject any new
        # migrate attempt against this lvol ("already exists ... Cancel it
        # first") for up to the rest of that hour. Cancel what we're
        # abandoning so the lvol isn't poisoned for the rest of the run.
        #
        # A single cancel + short wait isn't always enough: cleanup_target
        # can't complete while the lvol's node is still unreachable, so the
        # first attempt can itself time out. Retry a few times with a longer
        # total budget (the node may need several minutes to recover) before
        # giving up and flagging the lvol as still poisoned.
        cancel_status = "timeout"
        for attempt in range(1, 4):
            lib.log.warning(f"  migration {migration_id} timed out client-side but is "
                            f"still active server-side — cancelling it before moving on "
                            f"(attempt {attempt}/3)")
            lib.cancel_migration(migration_id)
            cancel_status = lib.wait_for_migration(lvol_id, cluster_id, terminal_only=True,
                                                   timeout=120)
            lib.log.info(f"  post-timeout cancel terminal: {cancel_status}")
            if cancel_status != "timeout":
                break
        if cancel_status == "timeout":
            lib.log.error(f"  {lvol_name}: migration {migration_id} still would not "
                         f"terminate after 3 cancel attempts — this lvol may stay "
                         f"poisoned until it clears on its own")

    if fault_injector:
        fault_injector.wait(timeout=NIC_DOWN_SECONDS + 30)

    new_node = lib.get_lvol_node(lvol_id)
    if new_node:
        lvol["current_node"] = new_node
        lib.log.info(f"  lvol now on: {new_node}")
    elif status in ("done", "completed"):
        lvol["current_node"] = target_node_id

    return status


def run_batch_iteration(group, target_node_id, nodes, cluster_id, node_ip_map,
                        fault_injector=None):
    """Migrate a namespaced batch group to *target_node_id*.

    Returns (status, phase): status is the terminal group status, phase is
    the last-observed group phase (useful to assert cleanup_target success).
    """
    any_member_id = group["batch_members"][0]["id"]
    group_name    = group["name"]

    lib.log.info(f"  group  : {group_name}")
    lib.log.info(f"  member : {any_member_id[-8:]}")
    lib.log.info(f"  src    : {group['current_node']}")
    lib.log.info(f"  target : {target_node_id}")

    batch_group_id = lib.start_batch_migration(any_member_id, target_node_id)
    group["last_group_id"] = batch_group_id
    lib.log.info(f"  grp_id : {batch_group_id}")

    if fault_injector:
        fault_injector.start_watching(any_member_id, batch_group_id=batch_group_id)

    lib.continue_batch_migration(batch_group_id, deadline=TEST_TIMEOUT)
    lib.log.info("  migrate-continue --batch sent")

    extra = NIC_DOWN_SECONDS + 60 if fault_injector else 0
    status, phase = lib.wait_for_batch_migration(
        batch_group_id, cluster_id,
        timeout=MIGRATION_TIMEOUT + extra,
    )
    lib.log.info(f"  batch terminal: status={status}  phase={phase}")

    if status == "timeout":
        cancel_status = "timeout"
        for attempt in range(1, 4):
            lib.log.warning(
                f"  batch {batch_group_id[:8]} timed out client-side — "
                f"cancelling (attempt {attempt}/3)")
            lib.cancel_migration(batch_group_id, batch=True)
            cancel_status, _ = lib.wait_for_batch_migration(
                batch_group_id, cluster_id, timeout=120)
            lib.log.info(f"  post-timeout cancel terminal: {cancel_status}")
            if cancel_status != "timeout":
                break
        if cancel_status == "timeout":
            lib.log.error(
                f"  {group_name}: batch {batch_group_id[:8]} still would not "
                f"terminate after 3 cancel attempts")

    if fault_injector:
        fault_injector.wait(timeout=NIC_DOWN_SECONDS + 30)

    # Update shared current_node for the group.
    new_node = lib.get_lvol_node(any_member_id)
    if new_node:
        group["current_node"] = new_node
        lib.log.info(f"  group now on: {new_node}")
    elif status in ("done", "completed"):
        group["current_node"] = target_node_id

    return status, phase


# ---------------------------------------------------------------------------
# Summary printer
# ---------------------------------------------------------------------------
def print_summary(results):
    log = lib.log
    log.info(f"\n{'='*70}")
    log.info(lib.bold("CHAOS SUMMARY"))
    log.info(f"{'='*70}")
    log.info(f"{'#':<5}  {'LVOL':<22}  {'STATUS':<12}  {'FAULT':<18}  RESULT")
    log.info(f"{'-'*5}  {'-'*22}  {'-'*12}  {'-'*18}  {'-'*6}")
    for r in results:
        tag   = lib.green("PASS") if r["ok"] else lib.red("FAIL")
        fault = r["fault"] or "-"
        log.info(f"{r['n']:<5}  {r['lvol']:<22}  {r['status']:<12}  {fault:<18}  {tag}")
    total  = len(results)
    passed = sum(1 for r in results if r["ok"])
    failed = total - passed
    log.info(f"\n  Total: {total}  {lib.green('PASS')}: {passed}  {lib.red('FAIL')}: {failed}")
    log.info(f"  Log dir: {LOG_DIR}")
    log.info(f"{'='*70}\n")


# ---------------------------------------------------------------------------
# Args
# ---------------------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--iterations", type=int, default=10000,
                   help="Max iterations (default: 10000)")
    p.add_argument("--no-fault", action="store_true",
                   help="Disable fault injection")
    p.add_argument("--setup-only", action="store_true",
                   help="Create fleet then exit (no connect/mount/fio)")
    p.add_argument("--teardown", action="store_true",
                   help="Delete fleet then exit")
    p.add_argument("--recover", type=int, default=30,
                   help="Seconds to settle between iterations (default: 30)")
    p.add_argument("--no-health-gate", action="store_true",
                   help="Skip cluster-health wait after failures")
    p.add_argument("--fio-size", default=FIO_SIZE,
                   help=f"fio data size per lvol (default: {FIO_SIZE})")
    p.add_argument("--pool", default=POOL_NAME,
                   help=f"Pool name to create/reuse (default: {POOL_NAME})")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    args = parse_args()
    log = lib.init_logging(LOG_FILE)

    cluster_id = lib.discover_cluster_id()
    log.info(f"Cluster : {cluster_id}")
    log.info(f"Log dir : {LOG_DIR}")

    nodes = lib.get_online_nodes()
    if len(nodes) < 2:
        raise RuntimeError(f"Need >= 2 online nodes, got {len(nodes)}")
    node_ip_map = lib.build_node_ip_map(nodes)
    log.info(f"Nodes   : {[(lib.get(n, 'hostname') or lib.get(n, 'id') or '')[:12] for n in nodes]}")

    if not lib.HAS_PARAMIKO and not args.no_fault:
        log.warning("paramiko not installed — fault injection disabled (pip install paramiko)")

    pool_id = lib.ensure_pool(args.pool, cluster_id)
    log.info(f"Pool    : {pool_id}")

    if args.teardown:
        anchor_names = [f"{ANCHOR_PREFIX}_{lib.get(n, 'id')[-6:]}"
                       for n in nodes if lib.get(n, "id")]
        teardown_fleet(pool_id, cluster_id, anchor_names=anchor_names)
        return

    log.info("Setting up fleet...")
    fleet = setup_fleet(pool_id, nodes)
    log.info(f"Fleet   : {[lv['name'] for lv in fleet]}")

    log.info("Setting up per-node anchor lvols (never migrated, keep every "
             "node's lvstore under steady IO so it can't quietly lose "
             "leadership during a fault)...")
    anchors = setup_anchors(pool_id, nodes)
    log.info(f"Anchors : {[a['name'] for a in anchors]}")

    if args.setup_only:
        log.info("--setup-only done")
        return

    log.info("Disconnecting any stale NVMe connections from a prior run...")
    lib.local_run("sudo nvme disconnect-all 2>/dev/null || true")
    time.sleep(2)

    log.info("Connecting, mounting, and starting continuous fio on the whole fleet "
             "(runs until the test ends)...")
    start_fleet_fio(fleet, args)

    log.info("Connecting, mounting, and starting continuous fio on all anchor lvols...")
    start_anchor_fio(anchors, args)

    # -------------------------------------------------------------------------
    # Chaos loop
    # -------------------------------------------------------------------------
    iters_since_fault = 0
    next_fault_at     = random.randint(FAULT_EVERY_MIN, FAULT_EVERY_MAX)
    results           = []
    any_fault_fired   = False

    log.info(f"\n{lib.cyan('=' * 70)}")
    log.info(lib.bold(f"CHAOS LOOP  iters={args.iterations}  "
                      f"fault={'off' if args.no_fault else 'on'}  "
                      f"fleet={len(fleet)} lvols  fio=continuous"))
    log.info(lib.cyan("=" * 70) + "\n")

    for iteration in range(1, args.iterations + 1):
        nodes = lib.get_online_nodes()
        node_ip_map = lib.build_node_ip_map(nodes)
        node_ids = [lib.get(n, "id") for n in nodes if lib.get(n, "id")]

        # Skip any lvol that still has a non-terminal migration record from a
        # prior iteration (e.g. a timeout whose cancel didn't fully land) —
        # picking it again would just burn the iteration on the same
        # "already exists / already past pre-create" collision instead of
        # exercising a fresh lvol.
        busy_lvol_ids = set()
        for lv in fleet:
            if lv.get("is_batch"):
                gid = lv.get("last_group_id")
                if gid:
                    g = lib.get_batch_record(gid, cluster_id)
                    if g and (lib.get(g, "status") or "").lower() not in (
                            "done", "completed", "failed", "cancelled", "error"):
                        busy_lvol_ids.add(lv["id"])
                        log.info(f"  {lv['name']}: batch group {gid[:8]} still active "
                                 f"({lib.get(g, 'status')}) — excluding this round")
            else:
                m = lib.get_migration_record(lv["id"], cluster_id)
                if m and (lib.get(m, "status") or "").lower() not in (
                        "done", "completed", "failed", "cancelled", "error"):
                    busy_lvol_ids.add(lv["id"])
                    log.info(f"  {lv['name']}: still has an active migration record "
                            f"({lib.get(m, 'status')}) — excluding from selection this round")

        candidates = [(lv, [nid for nid in node_ids if nid != lv["current_node"]])
                     for lv in fleet if lv["id"] not in busy_lvol_ids]
        candidates = [(lv, others) for lv, others in candidates if others]
        if not candidates:
            log.warning("No valid (lvol, target) pair — skipping iteration")
            continue
        lvol, others = random.choice(candidates)
        target_node_id = random.choice(others)

        fault_injector = None
        do_fault = (
            not args.no_fault
            and lib.HAS_PARAMIKO
            and iters_since_fault >= next_fault_at
        )
        fault_label = None
        if do_fault:
            fault_type  = random.choice(FAULT_TYPES)
            fault_phase = random.choice(FAULT_PHASES)
            fault_node  = random.choice(FAULT_NODES)
            fault_injector = lib.FaultInjector(
                fault_type, fault_phase, fault_node,
                lvol["current_node"], target_node_id, nodes,
                cluster_id, node_ip_map, nic_down_seconds=NIC_DOWN_SECONDS,
            )
            fault_label = f"{fault_type}/{fault_phase}/{fault_node}"
            iters_since_fault = 0
            next_fault_at     = random.randint(FAULT_EVERY_MIN, FAULT_EVERY_MAX)
        else:
            iters_since_fault += 1

        log.info(lib.cyan(f"\n{'=' * 70}"))
        log.info(lib.bold(
            f"[{iteration}/{args.iterations}]  {lvol['name']}"
            f"  {lvol['current_node'][-8:]} -> {target_node_id[-8:]}"
            f"  fault={fault_label or 'none'}"
        ))
        log.info(lib.cyan("=" * 70))

        iter_ok = False
        status  = "error"
        phase   = ""
        try:
            if lvol.get("is_batch"):
                status, phase = run_batch_iteration(
                    lvol, target_node_id, nodes, cluster_id,
                    node_ip_map, fault_injector)
            else:
                status = run_iteration(lvol, target_node_id, nodes, cluster_id,
                                       node_ip_map, fault_injector)

            if fault_injector and fault_injector.fired.is_set():
                any_fault_fired = True

            if status in ("done", "completed"):
                iter_ok = True
            elif status == "failed" and do_fault:
                # A fault-induced rollback that completed cleanup_target is a
                # valid success path.  Verify the lvol landed back on source.
                if not phase:
                    # Regular (non-batch): query the record for the phase.
                    m = lib.get_migration_record(lvol["id"], cluster_id)
                    phase = (lib.get(m, "phase") or "").lower() if m else ""
                cleanup_done = (phase == "cleanup_target") or (not phase)
                actual_node  = lib.get_lvol_node(lvol["id"]) or lvol["current_node"]
                on_source    = (actual_node == lvol["current_node"])
                iter_ok = cleanup_done and on_source
                if iter_ok:
                    log.info(lib.green(
                        f"  cleanup_target success: {lvol['name']} back on {actual_node}"))
                else:
                    log.error(lib.red(
                        f"  cleanup_target assertion FAILED: phase={phase!r}  "
                        f"actual={actual_node}  expected={lvol['current_node']}"))
            else:
                # timeout → always fail; cancelled/error → fail even with fault
                iter_ok = False

        except Exception as exc:
            log.error(f"Iteration {iteration} exception: {exc}", exc_info=True)
            try:
                real = lib.get_lvol_node(lvol["id"])
                if real:
                    lvol["current_node"] = real
            except Exception:
                pass

        results.append({
            "n":      iteration,
            "lvol":   lvol["name"],
            "status": status,
            "fault":  fault_label,
            "ok":     iter_ok,
        })

        if iter_ok:
            log.info(lib.green(f"PASS  iter {iteration}: (status={status})"))
        else:
            log.error(lib.red(f"FAIL  iter {iteration}: (status={status})"))
            snap_dir = LOG_DIR / f"failure_{iteration:04d}_{datetime.now().strftime('%H%M%S')}"
            log.info(lib.yellow(f"Collecting cluster state -> {snap_dir}"))
            lib.snapshot_cluster_state(snap_dir)

            if not args.no_health_gate and iteration < args.iterations:
                log.info(lib.yellow("Waiting for cluster healthy..."))
                if lib.wait_cluster_healthy(timeout=300):
                    log.info(lib.green("Cluster healthy — continuing"))
                else:
                    log.error(lib.red("Cluster did not recover — stopping"))
                    break

        log.info(f"  Running: "
                 f"{sum(1 for r in results if r['ok'])} pass / "
                 f"{sum(1 for r in results if not r['ok'])} fail")

        if iteration < args.iterations:
            log.info(lib.yellow(f"Settling {args.recover}s..."))
            time.sleep(args.recover)

    # -------------------------------------------------------------------------
    # Stop fio on the whole fleet + anchors and check for corruption across the run
    # -------------------------------------------------------------------------
    log.info(f"\n{lib.cyan('=' * 70)}")
    log.info(lib.bold("Stopping fio on the fleet and anchors, checking for corruption"))
    log.info(f"{lib.cyan('=' * 70)}")

    chk = lib.Checklist()
    for lvol in fleet:
        if lvol.get("is_batch"):
            for member in lvol["batch_members"]:
                lib.stop_fio(member.get("fio_proc"), post_wait=FIO_POST_TEST_WAIT)
                no_corruption, verify_errs = lib.check_fio_output(
                    member["fio_log"], fault_injected=any_fault_fired)
                chk.check(no_corruption, f"{member['name']}: no data corruption "
                                         f"(verify_errors={verify_errs})")
        else:
            lib.stop_fio(lvol.get("fio_proc"), post_wait=FIO_POST_TEST_WAIT)
            no_corruption, verify_errs = lib.check_fio_output(
                lvol["fio_log"], fault_injected=any_fault_fired)
            chk.check(no_corruption, f"{lvol['name']}: no data corruption "
                                     f"(verify_errors={verify_errs})")
    for lvol in anchors:
        lib.stop_fio(lvol.get("fio_proc"), post_wait=FIO_POST_TEST_WAIT)
        no_corruption, verify_errs = lib.check_fio_output(
            lvol["fio_log"], fault_injected=any_fault_fired)
        chk.check(no_corruption, f"{lvol['name']}: no data corruption "
                                 f"(verify_errors={verify_errs})")

    print_summary(results)
    chk.summary()

    failed_iters = sum(1 for r in results if not r["ok"])
    sys.exit(1 if (failed_iters or chk.failures) else 0)


if __name__ == "__main__":
    main()
