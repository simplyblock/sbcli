#!/usr/bin/env python3
"""
test_forced_target_migration.py

EXPERIMENTAL — deterministic reproduction attempt for the "double transfer"
bug class (a snap-copy retransfer triggered on the SAME snapshot after
bdev_lvol_transfer already reported Done — see tasks_runner_lvol_migration.py
_handle_snap_copy/_handle_group_snap_copy, and the retrigger reason= logging
added there).

That bug was originally observed during real `storage-node remove` runs,
always when the migration TARGET happened to be the node whose own
"Secondary node ID" is the node being removed (per the fixed vm07->vm08->
vm09->vm12->vm13->vm14->vm07 node-add ring, this is deterministic per
cluster: whichever node is removed, its "successor" in the ring always has
it as secondary). Waiting for node removal's automatic drain target picker
to happen to choose that node is slow and non-deterministic across cluster
redeploys.

This script reproduces the SAME structural condition without node removal
or any destructive action, using two already-existing, already-deployed
capabilities:
  - `sbctl storage-node shutdown <id>` — takes a node offline in place
    (reversible: `sn restart` + `cluster activate` bring it back), unlike
    `storage-node remove`.
  - `sbctl volume migrate <lvol_id> <target_node_id> [--batch]` — migrates
    to an EXPLICIT target, bypassing node removal's automatic (load-balance
    based) target picker entirely.

Flow, per --iterations round (default 1):
  1. Auto-pick (or use --node/--target) a (node_to_shutdown, target) pair
     such that target's own secondary IS node_to_shutdown -- the exact
     topology condition under which the bug was observed.
  2. Create 1 solo lvol + 2 shared-namespace batch groups on node_to_shutdown,
     connect + mount + start fio (group A: fio on member 0 only, group B: fio
     on every member -- same layout as test_node_removal_lvol_migration.py).
  3. `sn shutdown node_to_shutdown` (non-destructive).
  4. Explicitly migrate every lvol/group to `target` (solo via
     migrate_lvol(), each batch group via start_batch_migration +
     continue_batch_migration + wait_for_batch_migration).
  5. Grep docker service logs since step 3 for "retrigger reason=" lines.
     `reason=post_process_failed` is the double-transfer bug's signature
     (transfer_context was wiped after a completed transfer, forcing a full
     re-transfer on retry); any other reason is a benign, already-expected
     retrigger cause.
  6. Stop fio, check for verify errors (general IO errors are expected and
     ignored -- the node really is down).
  7. `sn restart node_to_shutdown` + `cluster activate` to bring it back
     online, then delete this iteration's lvols/pool so the cluster is
     clean for the next round -- no full redeploy needed between iterations.

*** Real disruption, but reversible ***
This shuts down a real storage node in a real cluster (briefly -- restarted
at the end of each iteration). Requires --yes. Unlike
test_node_removal_lvol_migration.py, it does NOT permanently remove
anything.

Usage:
  python3 test_forced_target_migration.py --yes [--node <id>] [--target <id>]
                                          [--batch-size 3] [--size 5G]
                                          [--iterations 3] [--fio-runtime 900]
"""

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import migration_test_lib as lib
from migration_test_lib import sbctl, sbctl_list, local_run, Checklist, get

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
POOL_NAME    = "forced-target-mig-pool"
LVOL_PREFIX  = "forced_target_lvol"
LVOL_SIZE    = "5G"
FIO_SIZE     = "1G"
FIO_RUNTIME  = 900        # long-lived; stopped explicitly once done
MOUNT_ROOT   = "/mnt/forced_target_mig"

BATCH_PREFIX     = "forced_target_batch"
BATCH_GROUP_SIZE = 3

NODE_STATUS_POLL       = 5
NODE_DOWN_WAIT         = 120    # how long to wait for node_to_shutdown to leave online
MIGRATION_TIMEOUT      = 1200   # per solo/batch migration, real data transfer
RESTART_WAIT           = 300
CLUSTER_ACTIVE_WAIT    = 600

LOG_DIR = Path("/tmp/migration_test_logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"forced_target_migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

log = lib.init_logging(LOG_FILE)


# ---------------------------------------------------------------------------
# Topology: pick a (node_to_shutdown, target) pair reproducing the bug's
# structural condition -- target's own secondary IS node_to_shutdown.
# ---------------------------------------------------------------------------
def pick_shutdown_and_target(nodes, node_arg=None, target_arg=None):
    """node_arg/target_arg are both given, or both None (enforced by the
    caller) -- an explicit pair is used as-is; otherwise auto-pick the first
    online node for which some other online node's secondary IS it."""
    if node_arg and target_arg:
        node_id = next(
            (get(n, "id") for n in nodes
             if get(n, "id") == node_arg
             or (get(n, "hostname") or "").lower().startswith(node_arg.lower())),
            None)
        target_id = next(
            (get(n, "id") for n in nodes
             if get(n, "id") == target_arg
             or (get(n, "hostname") or "").lower().startswith(target_arg.lower())),
            None)
        if not node_id or not target_id:
            raise RuntimeError(f"--node '{node_arg}' / --target '{target_arg}' "
                               f"did not both match an online node")
        return node_id, target_id

    for nid in (get(n, "id") for n in nodes if get(n, "id")):
        try:
            target_id = lib.pick_target_node(nid, nodes, "b")
        except RuntimeError:
            continue
        log.info(f"Topology match: target {target_id}'s secondary IS {nid}")
        return nid, target_id

    raise RuntimeError(
        "No (node, target) pair found where target's secondary is the node "
        "-- pass --node/--target explicitly to force a specific pair.")


def create_batch_group(pool_id, node_id, group_idx, size, count, fio_member_idxs, mount_root):
    members = []
    for i in range(count):
        name = f"{BATCH_PREFIX}{group_idx}_{i}"
        lvol_id = lib.create_lvol(name, size, pool_id, node_id,
                                  namespaced=True, max_ns=count, snapshot=False)
        members.append({
            "name": name,
            "id": lvol_id,
            "mount_point": f"{mount_root}_batch{group_idx}_{i}",
            "run_fio": i in fio_member_idxs,
        })
    return members


def get_node_status(node_id):
    for n in sbctl_list("sn", "list"):
        if get(n, "id") == node_id:
            return (get(n, "status") or "").lower()
    return None


def wait_for_node_status_leave(node_id, leave_statuses, timeout, poll=NODE_STATUS_POLL):
    """Poll until node_id's status is NOT in leave_statuses (e.g. it actually
    went offline after `sn shutdown`)."""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        status = get_node_status(node_id)
        if status != last:
            log.info(f"  node {node_id}: status={status}")
            last = status
        if status not in leave_statuses:
            return status
        time.sleep(poll)
    return last


# ---------------------------------------------------------------------------
# Best-effort direct log evidence of a snap-copy retrigger firing, and which
# reason it fired for. reason=post_process_failed is the double-transfer
# bug's signature; every other reason is an already-expected retrigger cause.
# ---------------------------------------------------------------------------
def grep_retrigger_log_evidence(since_ts):
    since_iso = datetime.fromtimestamp(since_ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    out, _, rc = local_run("docker service ls --format '{{.Name}}' 2>/dev/null || true")
    services = [s.strip() for s in out.splitlines() if s.strip()]
    if not services:
        return [], "no docker services found (not a swarm deployment on this host?)"

    hits = []
    for svc in services:
        grep_out, _, _ = local_run(
            f"docker service logs --since {since_iso} {svc} 2>&1 "
            f"| grep -i 'retrigger reason=' || true"
        )
        for line in grep_out.splitlines():
            if line.strip():
                hits.append(f"[{svc}] {line.strip()}")
    return hits, None


# ---------------------------------------------------------------------------
# Args
# ---------------------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--yes", action="store_true",
                  help="Required: confirms you understand this shuts down a real "
                       "storage node (reversible -- it is restarted at the end of "
                       "each iteration).")
    p.add_argument("--node", default=None,
                  help="Node ID (or hostname prefix) to shut down. Default: auto-pick "
                       "a node for which some other online node's secondary IS it.")
    p.add_argument("--target", default=None,
                  help="Explicit migration target node ID (or hostname prefix). "
                       "Default: auto-picked to satisfy the topology condition above. "
                       "Must be given together with --node, or not at all.")
    p.add_argument("--batch-size", type=int, default=BATCH_GROUP_SIZE,
                  help="Members per shared-namespace batch group; two groups are "
                       f"created (default: {BATCH_GROUP_SIZE})")
    p.add_argument("--size", default=LVOL_SIZE, help=f"LVol size (default: {LVOL_SIZE})")
    p.add_argument("--fio-runtime", type=int, default=FIO_RUNTIME,
                  help=f"fio runtime cap in seconds (default: {FIO_RUNTIME})")
    p.add_argument("--iterations", type=int, default=1,
                  help="Repeat the whole shutdown->migrate->restart cycle this many "
                       "times, reusing the same cluster (default: 1)")
    p.add_argument("--force-shutdown", action="store_true",
                  help="Pass --force to `sn shutdown`.")
    p.add_argument("--teardown", action="store_true",
                  help="Delete the test pool/lvols (wherever they now live) and exit. "
                       "Does not touch node status.")
    return p.parse_args()


def run_iteration(args, cluster_id, node_id, target_id, iteration):
    checklist = Checklist()
    pool_id = lib.ensure_pool(POOL_NAME, cluster_id)

    checklist.step(
        f"[iter {iteration}] 1. Create solo lvol + 2 batch groups "
        f"({args.batch_size} members each) on {node_id}")
    lvol_id = lib.create_lvol(f"{LVOL_PREFIX}", args.size, pool_id, node_id,
                              snapshot=False, max_ns=1)
    solo = {"name": LVOL_PREFIX, "id": lvol_id, "mount_point": f"{MOUNT_ROOT}_solo", "run_fio": True}

    batch_group_a = create_batch_group(
        pool_id, node_id, 0, args.size, args.batch_size, {0}, MOUNT_ROOT)
    batch_group_b = create_batch_group(
        pool_id, node_id, 1, args.size, args.batch_size, set(range(args.batch_size)), MOUNT_ROOT)
    checklist.check(bool(lvol_id), f"Created solo lvol {lvol_id}")
    checklist.check(len(batch_group_a) == args.batch_size, "Created batch group A (fio on 1)")
    checklist.check(len(batch_group_b) == args.batch_size, "Created batch group B (fio on all)")

    all_lvols = [solo] + batch_group_a + batch_group_b
    for lv in all_lvols:
        home = lib.get_lvol_node(lv["id"])
        checklist.check(home == node_id, f"{lv['name']} is on {node_id} (got: {home})")

    checklist.step(f"[iter {iteration}] 2. Connect + mount; start fio")
    local_run("sudo nvme disconnect-all 2>/dev/null || true")
    time.sleep(2)
    solo["device"] = lib.connect_and_mount_lvol(solo["id"], solo["mount_point"])
    lib.connect_and_mount_batch_members(batch_group_a)
    lib.connect_and_mount_batch_members(batch_group_b)

    for lv in all_lvols:
        if lv["run_fio"]:
            fio_file = f"{lv['mount_point']}/testfile"
            fio_log = f"/tmp/forced_target_mig_fio_{lv['name']}.log"
            lv["fio_log"] = fio_log
            lv["fio_proc"] = lib.start_fio_bg(fio_file, fio_log, size=FIO_SIZE, runtime=args.fio_runtime)
            checklist.check(bool(lv["fio_proc"]), f"{lv['name']}: fio started ({lv['device']})")
        else:
            lv["fio_proc"] = None

    checklist.step(f"[iter {iteration}] 3. Shut down {node_id} (non-destructive)")
    shutdown_out = sbctl(*(["sn", "shutdown", node_id] + (["--force"] if args.force_shutdown else [])))
    log.info(f"sn shutdown output:\n{shutdown_out}")
    left_status = wait_for_node_status_leave(
        node_id, ("online", "active", "online_healthy"), timeout=NODE_DOWN_WAIT)
    checklist.check(
        left_status not in ("online", "active", "online_healthy"),
        f"{node_id} left online status within {NODE_DOWN_WAIT}s (now: {left_status})"
    )
    t0 = time.time()

    checklist.step(f"[iter {iteration}] 4. Explicitly migrate everything to {target_id}")
    solo_status = lib.migrate_lvol(solo["id"], target_id, cluster_id,
                                   deadline=3600, timeout=MIGRATION_TIMEOUT)
    checklist.check(solo_status in ("done", "completed"),
                    f"Solo lvol migration terminal status: {solo_status}")

    for label, group in (("A", batch_group_a), ("B", batch_group_b)):
        group_id = lib.start_batch_migration(group[0]["id"], target_id)
        lib.continue_batch_migration(group_id, deadline=3600)
        status, phase = lib.wait_for_batch_migration(group_id, cluster_id, timeout=MIGRATION_TIMEOUT)
        checklist.check(status in ("done", "completed"),
                        f"Batch group {label} migration terminal status: {status} (phase={phase})")

    checklist.step(f"[iter {iteration}] 5. Check for the double-transfer retrigger signature")
    hits, note = grep_retrigger_log_evidence(t0)
    if note:
        log.warning(f"  Could not check logs directly: {note}")
    else:
        for h in hits:
            log.info(f"    {h}")
        bad = [h for h in hits if "reason=post_process_failed" in h]
        checklist.check(
            not bad,
            f"No reason=post_process_failed retrigger observed "
            f"({len(hits)} total retrigger line(s), {len(bad)} matching the bug signature)"
        )

    checklist.step(f"[iter {iteration}] 6. Verify lvols moved to {target_id}")
    for lv in all_lvols:
        new_node = lib.get_lvol_node(lv["id"])
        checklist.check(new_node == target_id, f"{lv['name']} now on {new_node} (expected {target_id})")

    checklist.step(f"[iter {iteration}] 7. Stop fio, check for verify errors")
    for lv in all_lvols:
        if lv["fio_proc"]:
            lib.stop_fio(lv["fio_proc"], post_wait=5)
            ok, verify_errs = lib.check_fio_output(lv["fio_log"], fault_injected=True)
            checklist.check(ok, f"{lv['name']}: 0 verify errors (found {verify_errs})")

    checklist.step(f"[iter {iteration}] 8. Restart {node_id} and reactivate the cluster")
    local_run("sudo nvme disconnect-all 2>/dev/null || true")
    sbctl(*(["sn", "restart", node_id] + (["--force"] if args.force_shutdown else [])))
    restarted = lib.wait_node_status(node_id, timeout=RESTART_WAIT)
    checklist.check(restarted, f"{node_id} back online within {RESTART_WAIT}s")
    lib.activate_cluster(cluster_id)
    active = lib.wait_cluster_status(cluster_id, ("active",), timeout=CLUSTER_ACTIVE_WAIT)
    checklist.check(active, f"Cluster back to ACTIVE within {CLUSTER_ACTIVE_WAIT}s")
    healthy = lib.wait_cluster_healthy(timeout=CLUSTER_ACTIVE_WAIT)
    checklist.check(healthy, "All nodes healthy after reactivation")

    checklist.step(f"[iter {iteration}] 9. Clean up this iteration's lvols/pool")
    lib.cleanup_pool_and_lvols(POOL_NAME, cluster_id, mount=f"{MOUNT_ROOT}*")

    return checklist.summary()


def main():
    args = parse_args()

    log.info(f"Log file : {LOG_FILE}")
    cluster_id = lib.discover_cluster_id()
    log.info(f"Cluster  : {cluster_id}")

    if args.teardown:
        local_run("sudo killall fio 2>/dev/null || true")
        for mnt in Path("/mnt").glob(f"{Path(MOUNT_ROOT).name}_*"):
            local_run(f"sudo umount {mnt} 2>/dev/null || true")
        local_run("sudo nvme disconnect-all 2>/dev/null || true")
        time.sleep(2)
        lib.cleanup_pool_and_lvols(POOL_NAME, cluster_id)
        log.info("Teardown complete")
        return

    if not args.yes:
        log.error(
            "Refusing to run without --yes: this shuts down a real storage node "
            "(reversible -- it's restarted at the end of each iteration, see "
            "sn shutdown/sn restart/cluster activate). Re-run with --yes once "
            "you're sure this cluster can tolerate that."
        )
        sys.exit(2)

    if bool(args.node) != bool(args.target):
        log.error("--node and --target must be given together, or not at all "
                  "(omit both to auto-pick a matching pair).")
        sys.exit(2)

    nodes = lib.get_online_nodes()
    if len(nodes) < 2:
        raise RuntimeError(f"Need at least 2 online nodes, got {len(nodes)}")

    node_id, target_id = pick_shutdown_and_target(nodes, args.node, args.target)
    log.info(f"node_to_shutdown : {node_id}")
    log.info(f"target           : {target_id}")

    overall_ok = True
    for i in range(1, args.iterations + 1):
        ok = run_iteration(args, cluster_id, node_id, target_id, i)
        overall_ok = overall_ok and ok
        if not ok:
            log.error(f"Iteration {i} FAILED -- see checklist above. "
                      f"Continuing to next iteration anyway (best-effort).")

    log.info(f"Log: {LOG_FILE}")
    sys.exit(0 if overall_ok else 1)


if __name__ == "__main__":
    main()
