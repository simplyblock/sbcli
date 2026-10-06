#!/usr/bin/env python3
"""
test_node_removal_lvol_migration.py

EXPERIMENTAL — validates the node-removal integration on this branch
(fix/node-removal-on-lvol-migration): `storage-node remove` no longer
requires lvols to be pre-migrated off the node. Instead:

  pending_removal -> migrating_devices (failed-device migration) ->
  migrating_lvols (lvol migration) -> in_removal (replica teardown/
  relocation/JM) -> removed

and the lvol migration triggered here is a REAL exercise of "migrate from
secondary" — migration_controller.resolve_source_node() falls back to an
online secondary/tertiary whenever the primary is not ONLINE/SUSPENDED,
which covers a node mid-removal (MIGRATING_LVOLS/IN_REMOVAL) without needing
SB_TEST_FORCE_SOURCE_FALLBACK or a real, uncontrolled fault injection.

*** DESTRUCTIVE / IRREVERSIBLE ***
This test picks a real storage node and actually removes it from the
cluster (GPT wipe, docker swarm leave, etc. — see _finalize_node_removal).
It does NOT add the node back. Run only against a cluster you're fine
losing one node from. Requires --yes.

What it checks:
  1. The node has a secondary configured (so its lvols are genuinely HA and
     "migrate from secondary" has something real to source from).
  2. --lvols solo lvols pre-exist on EVERY online node (not just the removal
     target) BEFORE calling remove, each with fio running the whole time.
     This is both the actual behavior change under test (previously a hard
     precondition failure blocked remove if the target had any lvols) and a
     bystander check: the target node's lvols must migrate away, while every
     OTHER node's lvols must stay completely undisturbed by the removal.
  3. Exercises BOTH migration code paths node removal's drain can take:
       - solo: --lvols independent lvols per node, each its own NQN/subsystem,
         drained via create_migration/start_migration (one lvol = one drain
         unit) for the ones that live on the removal target.
       - batch: two shared-namespace groups (--batch-size members each, same
         NQN/subsystem) on the removal target, drained via
         create_batch_migration/start_batch_migration (a whole group = one
         drain unit). Group A runs fio on member 0 only (the others are
         connected+mounted but idle); Group B runs fio on EVERY member.
  4. fio keeps running (0 verify/IO errors) on EVERY lvol/member across the
     WHOLE removal AND for --post-removal-soak seconds afterward (default
     20min), including while the node is shut down and its lvols are being
     migrated off of a live secondary rather than the (dead) primary. The
     soak period exists to catch anything that only surfaces once the
     cluster has "settled" -- e.g. a peer left in a degraded recovery loop
     by the removal, which a stop-fio-immediately test would never notice.
  5. Node status only moves forward through REMOVAL_STATUS_ORDER, passes
     through migrating_lvols, and reaches removed; the observed sequence and
     time spent in each phase are logged.
  6. Every lvol/member that was on the removal target ends up on a DIFFERENT
     node afterward; every lvol on every OTHER node stays exactly where it
     was.
  7. Best-effort: greps docker service logs for the from-secondary fallback
     log line, as direct evidence the mechanism actually fired for real.

Usage:
  python3 test_node_removal_lvol_migration.py --yes [--node <id>] [--lvols 2]
                                              [--batch-size 3]
                                              [--size 5G] [--fio-runtime 7200]
                                              [--post-removal-soak 1200]
"""

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import migration_test_lib as lib
from migration_test_lib import sbctl, sbctl_list, local_run, Checklist

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
POOL_NAME    = "node-removal-mig-pool"
LVOL_PREFIX  = "node_removal_lvol"
LVOL_COUNT   = 2   # per node -- every online node gets this many, not just the removal target
LVOL_SIZE    = "5G"
FIO_SIZE     = "1G"
FIO_RUNTIME  = 7200       # long-lived; we stop it explicitly once done
MOUNT_ROOT   = "/mnt/node_removal_mig"

# Two shared-namespace (batch-migration) groups, both on the same target node
# as the solo lvols above. BATCH_PREFIX + group index + member index names
# each lvol; group 0 ("A") runs fio on member 0 only, group 1 ("B") on every
# member -- see create_batch_group().
BATCH_PREFIX     = "node_removal_batch"
BATCH_GROUP_SIZE = 3

NODE_STATUS_POLL       = 5
# Polling can skip a short phase, so a check needs only forward order, not every step.
REMOVAL_STATUS_ORDER   = ("online", "pending_removal", "migrating_devices",
                          "migrating_lvols", "in_removal", "removed")
REMOVED_TIMEOUT       = 1800  # node removal can take a while (real device drain)
POST_REMOVAL_SOAK_SEC  = 1200  # keep fio running this long after removal completes (20min)
SOAK_HEARTBEAT_SEC     = 60    # log + check fio liveness this often during the soak

LOG_DIR = Path("/tmp/migration_test_logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"node_removal_lvol_migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

log = lib.init_logging(LOG_FILE)


# ---------------------------------------------------------------------------
# Node selection
# ---------------------------------------------------------------------------
def pick_removal_target(nodes):
    """Prefer a node with a secondary configured -- its lvols are then
    genuinely HA and "migrate from secondary" has a real replica to source
    from. Falls back to any online node if none has one."""
    for n in nodes:
        nid = lib.get(n, "id")
        if nid and lib.get_node_secondary_id(nid, nodes):
            return nid
    return lib.get(nodes[0], "id")


def create_batch_group(pool_id, node_id, group_idx, size, count, fio_member_idxs, mount_root):
    """Create one shared-namespace group of `count` lvols on `node_id`.

    All members share one NQN/subsystem (create_lvol's namespaced=True/max_ns
    join an existing joinable subsystem on this node+pool automatically, per
    get_next_available_subsystem_on_node -- filling one group to `count`
    before starting the next is what keeps the two groups on distinct
    subsystems). node_removal's drain groups lvols into units by shared NQN
    (_drain_unit_key), so a whole group becomes ONE drain unit, migrated via
    create_batch_migration/start_batch_migration rather than the solo path.

    Every member is a real dict compatible with the solo lvol dicts built in
    main() -- "run_fio" decides whether step 2 starts fio on it; members not
    in `fio_member_idxs` are still connected + mounted (a real, idle client
    of the shared subsystem), just never written to.
    """
    members = []
    for i in range(count):
        name = f"{BATCH_PREFIX}{group_idx}_{i}"
        lvol_id = lib.create_lvol(name, size, pool_id, node_id,
                                  namespaced=True, max_ns=count, snapshot=False)
        members.append({
            "name": name,
            "id": lvol_id,
            "home_node": node_id,
            "mount_point": f"{mount_root}_batch{group_idx}_{i}",
            "run_fio": i in fio_member_idxs,
        })
    return members


# ---------------------------------------------------------------------------
# Node-status polling across the removal phases
# ---------------------------------------------------------------------------
def get_node_status(node_id):
    for n in sbctl_list("sn", "list"):
        if lib.get(n, "id") == node_id:
            return (lib.get(n, "status") or "").lower()
    return None


def watch_removal_statuses(node_id, timeout, poll=NODE_STATUS_POLL):
    """Poll until `node_id` reports 'removed' or `timeout` elapses.

    Returns [(status, seconds_in_status), ...] in the order first observed;
    the duration of the last entry runs until the watch ended.
    """
    deadline = time.time() + timeout
    sequence = []
    entered = time.time()
    while time.time() < deadline:
        status = get_node_status(node_id)
        if not sequence or status != sequence[-1][0]:
            now = time.time()
            if sequence:
                sequence[-1] = (sequence[-1][0], now - entered)
            sequence.append((status, 0.0))
            entered = now
            log.info(f"  node {node_id}: status={status}")
        if status == "removed":
            break
        time.sleep(poll)
    sequence[-1] = (sequence[-1][0], time.time() - entered)
    return sequence


# ---------------------------------------------------------------------------
# Best-effort direct log evidence of the from-secondary fallback firing
# ---------------------------------------------------------------------------
def grep_fallback_log_evidence(since_ts):
    """Search docker service logs since `since_ts` for the from-secondary
    fallback log lines emitted by resolve_source_node's callers.
    Never raises; returns a list of matching lines (possibly empty) plus a
    note if nothing could be checked at all."""
    since_iso = datetime.fromtimestamp(since_ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    out, _, rc = local_run("docker service ls --format '{{.Name}}' 2>/dev/null || true")
    services = [s.strip() for s in out.splitlines() if s.strip()]
    if not services:
        return [], "no docker services found (not a swarm deployment on this host?)"

    hits = []
    for svc in services:
        grep_out, _, _ = local_run(
            f"docker service logs --since {since_iso} {svc} 2>&1 "
            f"| grep -iE 'fallback source|is offline; (using|continuing)' || true"
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
                  help="Required: confirms you understand this permanently removes a real node.")
    p.add_argument("--node", default=None,
                  help="Node ID (or hostname prefix) to remove. Default: auto-pick a node with a secondary.")
    p.add_argument("--lvols", type=int, default=LVOL_COUNT,
                  help=f"Solo lvols to create on EACH online node, not just the removal "
                       f"target (default: {LVOL_COUNT})")
    p.add_argument("--batch-size", type=int, default=BATCH_GROUP_SIZE,
                  help="Members per shared-namespace batch group; two groups are created "
                       f"(default: {BATCH_GROUP_SIZE})")
    p.add_argument("--no-batch", action="store_true",
                  help="Skip both shared-namespace batch groups -- solo lvol(s) only "
                       "(minimal repro, e.g. --lvols 1 --no-batch).")
    p.add_argument("--size", default=LVOL_SIZE, help=f"LVol size (default: {LVOL_SIZE})")
    p.add_argument("--fio-runtime", type=int, default=FIO_RUNTIME,
                  help=f"fio runtime cap in seconds (default: {FIO_RUNTIME})")
    p.add_argument("--post-removal-soak", type=int, default=POST_REMOVAL_SOAK_SEC,
                  help="Keep fio running this many seconds after the node reaches "
                       f"'removed', before stopping it and checking for errors "
                       f"(default: {POST_REMOVAL_SOAK_SEC})")
    p.add_argument("--force-remove", action="store_true",
                  help="Pass --force-remove to `storage-node remove` (cancels any active tasks on the node first).")
    p.add_argument("--force-secondary-target", action="store_true",
                  help="TEST ONLY: bias node removal's own automatic drain target picker "
                       "(_pick_drain_target) toward the online peer whose own secondary IS "
                       "the node being removed -- the exact topology condition "
                       "create_migration's _usable_replica fix addresses "
                       "(fix/migration-target-replica-offline-check), reproduced via the REAL "
                       "node-removal drain instead of the forced-target workaround in "
                       "test_forced_target_migration.py. Sets "
                       "SB_TEST_PREFER_TARGET_WHOSE_SECONDARY_IS_DEPARTING=1 on the "
                       "app_TasksNodeRemovalRunner docker service (never set in production) "
                       "for the duration of this run, then clears it. Without this flag, node "
                       "removal picks targets the normal random-weighted way.")
    p.add_argument("--teardown", action="store_true",
                  help="Delete the test pool/lvols (wherever they now live) and exit. Does NOT undo node removal.")
    return p.parse_args()


REMOVAL_RUNNER_SERVICE = "app_TasksNodeRemovalRunner"
FORCE_SECONDARY_TARGET_ENV = "SB_TEST_PREFER_TARGET_WHOSE_SECONDARY_IS_DEPARTING"


def main():
    args = parse_args()
    checklist = Checklist()

    log.info(f"Log file : {LOG_FILE}")
    cluster_id = lib.discover_cluster_id()
    log.info(f"Cluster  : {cluster_id}")

    # fio must still be running when we get to the post-removal soak, so its
    # own --runtime cap needs comfortable headroom over the worst case: the
    # full removal timeout plus the soak itself plus setup/connect overhead.
    worst_case = REMOVED_TIMEOUT + args.post_removal_soak + 300
    if not args.teardown and args.fio_runtime < worst_case:
        log.warning(
            f"--fio-runtime {args.fio_runtime}s is less than the worst-case "
            f"removal+soak window (~{worst_case}s); fio could exit on its own "
            f"before the soak check runs. Consider raising --fio-runtime.")

    pool_id = lib.ensure_pool(POOL_NAME, cluster_id)

    if args.teardown:
        local_run("sudo killall fio 2>/dev/null || true")
        for mnt in Path("/mnt").glob(f"{Path(MOUNT_ROOT).name}_*"):
            lib.unmount_all_at(str(mnt))
        local_run("sudo nvme disconnect-all 2>/dev/null || true")
        time.sleep(2)
        lib.cleanup_pool_and_lvols(POOL_NAME, cluster_id)
        sbctl("storage-pool", "delete", pool_id)
        log.info("Teardown complete (pool/lvols only -- node removal itself is not reversed)")
        return

    if not args.yes:
        log.error(
            "Refusing to run without --yes: this test PERMANENTLY REMOVES a real "
            "storage node from the cluster (GPT wipe, docker swarm leave -- see "
            "_finalize_node_removal). It does not add the node back. Re-run with "
            "--yes once you're sure this cluster can lose a node."
        )
        sys.exit(2)

    nodes = lib.get_online_nodes()
    if len(nodes) < 2:
        raise RuntimeError(f"Need at least 2 online nodes, got {len(nodes)}")

    if args.node:
        node_id = next(
            (lib.get(n, "id") for n in nodes
             if lib.get(n, "id") == args.node
             or (lib.get(n, "hostname") or "").lower().startswith(args.node.lower())),
            None)
        if not node_id:
            raise RuntimeError(f"--node '{args.node}' did not match any online node")
    else:
        node_id = pick_removal_target(nodes)

    secondary_id = lib.get_node_secondary_id(node_id, nodes)
    log.info(f"Target node for removal : {node_id}")
    log.info(f"Target node's secondary : {secondary_id or '(none configured)'}")
    checklist.check(bool(secondary_id),
                    f"Target node has a secondary configured ({secondary_id}) -- "
                    f"lvols on it are genuinely HA")

    # -------------------------------------------------------------------
    batch_note = "no batch groups" if args.no_batch else f"+ 2 batch groups ({args.batch_size} members each) on {node_id}"
    checklist.step(
        f"1. Create {args.lvols} solo lvol(s) on EACH of {len(nodes)} online node(s) "
        f"{batch_note}")
    # -------------------------------------------------------------------
    lvols = []
    for n in nodes:
        n_id = lib.get(n, "id")
        n_host = lib.get(n, "hostname") or n_id[:8]
        for i in range(args.lvols):
            name = f"{LVOL_PREFIX}_{n_host}_{i}"
            lvol_id = lib.create_lvol(name, args.size, pool_id, n_id, snapshot=False, max_ns=1)
            lvols.append({
                "name": name, "id": lvol_id, "home_node": n_id,
                "mount_point": f"{MOUNT_ROOT}_{n_host}_{i}", "run_fio": True,
            })
    checklist.check(len(lvols) == args.lvols * len(nodes),
                    f"Created {len(lvols)} solo lvol(s) across {len(nodes)} node(s) "
                    f"({args.lvols} per node)")

    if args.no_batch:
        batch_group_a, batch_group_b = [], []
    else:
        # Group A: fio on member 0 only, the rest connected+mounted but idle --
        # the case where idle blobs are skipped by the source's failover
        # update. Group B: fio on every member. Each group is filled to
        # args.batch_size before the next starts, so they land on two
        # distinct subsystems (see create_batch_group's docstring). Both
        # groups live on the removal target only -- unlike the per-node solo
        # lvols above, batch groups are not part of the bystander check.
        batch_group_a = create_batch_group(
            pool_id, node_id, 0, args.size, args.batch_size, {0}, MOUNT_ROOT)
        batch_group_b = create_batch_group(
            pool_id, node_id, 1, args.size, args.batch_size, set(range(args.batch_size)), MOUNT_ROOT)
        checklist.check(len(batch_group_a) == args.batch_size,
                        f"Created batch group A ({args.batch_size} members, fio on member 0 only)")
        checklist.check(len(batch_group_b) == args.batch_size,
                        f"Created batch group B ({args.batch_size} members, fio on all)")

    all_lvols = lvols + batch_group_a + batch_group_b
    for lv in all_lvols:
        home = lib.get_lvol_node(lv["id"])
        checklist.check(home == lv["home_node"],
                        f"{lv['name']} is on {lv['home_node']} before removal (got: {home})")

    # -------------------------------------------------------------------
    checklist.step("2. Connect + mount every lvol/member; start fio where configured")
    # -------------------------------------------------------------------
    # Previous runs' mounts sit on devices from a since-rebuilt cluster;
    # mounting on top of them would leave this run's mount stacked over junk.
    for lv in all_lvols:
        lib.unmount_all_at(lv["mount_point"])
    local_run("sudo nvme disconnect-all 2>/dev/null || true")
    time.sleep(2)

    # Solo lvols: one connect == one new top-level device, handled by the
    # plain per-lvol helper. Batch members share ONE subsystem connection --
    # connecting member 2+ never produces a new top-level device (it's an
    # additional namespace on the controller member 1 already connected), so
    # they need the nsid-aware batch helper instead, or every member after
    # the first times out waiting for a device that will never appear.
    for lv in lvols:
        lv["device"] = lib.connect_and_mount_lvol(lv["id"], lv["mount_point"])
    if batch_group_a:
        lib.connect_and_mount_batch_members(batch_group_a)
    if batch_group_b:
        lib.connect_and_mount_batch_members(batch_group_b)

    for lv in all_lvols:
        device = lv["device"]
        if lv["run_fio"]:
            fio_file = f"{lv['mount_point']}/testfile"
            fio_log = f"/tmp/node_removal_mig_fio_{lv['name']}.log"
            lv["fio_log"] = fio_log
            lv["fio_proc"] = lib.start_fio_bg(fio_file, fio_log, size=FIO_SIZE, runtime=args.fio_runtime)
            checklist.check(bool(lv["fio_proc"]), f"{lv['name']}: fio started ({device})")
        else:
            lv["fio_proc"] = None
            log.info(f"  {lv['name']}: connected+mounted, no fio (idle shared-namespace member) ({device})")

    lib.log_client_nvme_state("before-removal")

    if args.force_secondary_target:
        checklist.step(
            f"2b. TEST ONLY: bias the drain's target picker toward the peer "
            f"whose secondary is {node_id} ({FORCE_SECONDARY_TARGET_ENV}=1 on "
            f"{REMOVAL_RUNNER_SERVICE})")
        ok = lib.set_docker_service_env(REMOVAL_RUNNER_SERVICE, FORCE_SECONDARY_TARGET_ENV, "1")
        checklist.check(ok, f"{FORCE_SECONDARY_TARGET_ENV}=1 set on {REMOVAL_RUNNER_SERVICE}")
        running = lib.wait_service_running(REMOVAL_RUNNER_SERVICE)
        checklist.check(running, f"{REMOVAL_RUNNER_SERVICE} back to Running after the env update")

    try:
        # -------------------------------------------------------------------
        checklist.step(f"3. Remove node {node_id} (fio running throughout)")
        # -------------------------------------------------------------------
        t0 = time.time()
        remove_args = ["storage-node", "remove", node_id]
        if args.force_remove:
            remove_args.append("--force-remove")
        remove_out = sbctl(*remove_args)
        log.info(f"storage-node remove output:\n{remove_out}")

        # -------------------------------------------------------------------
        checklist.step(f"4. Watch node status: {' -> '.join(REMOVAL_STATUS_ORDER)}")
        # -------------------------------------------------------------------
        sequence = watch_removal_statuses(node_id, timeout=REMOVED_TIMEOUT)
        log.info("  status sequence: " + " -> ".join(f"{s} ({d:.0f}s)" for s, d in sequence))
        statuses = [s for s, _ in sequence]

        checklist.check(statuses[-1:] == ["removed"],
                        f"Node reached 'removed' within {REMOVED_TIMEOUT}s (last: {statuses[-1:]})")
        unexpected = [s for s in statuses if s not in REMOVAL_STATUS_ORDER]
        ranks = [REMOVAL_STATUS_ORDER.index(s) for s in statuses if s in REMOVAL_STATUS_ORDER]
        checklist.check(not unexpected and ranks == sorted(ranks),
                        f"Status only moved forward through the removal phases "
                        f"(seen: {statuses}; unexpected: {unexpected})")
        checklist.check("migrating_lvols" in statuses,
                        f"Observed migrating_lvols (seen: {statuses})")
    finally:
        if args.force_secondary_target:
            lib.unset_docker_service_env(REMOVAL_RUNNER_SERVICE, FORCE_SECONDARY_TARGET_ENV)
            lib.wait_service_running(REMOVAL_RUNNER_SERVICE)

    # -------------------------------------------------------------------
    checklist.step(
        "5. Verify the removed node's lvol(s) relocated, and every other "
        "node's lvol(s) were undisturbed by the removal")
    # -------------------------------------------------------------------
    relocated = []
    for lv in all_lvols:
        new_node = lib.get_lvol_node(lv["id"])
        if lv["home_node"] == node_id:
            moved = bool(new_node) and new_node != node_id
            checklist.check(moved, f"{lv['name']} moved off {node_id} (now on: {new_node})")
        else:
            unchanged = new_node == lv["home_node"]
            checklist.check(
                unchanged,
                f"{lv['name']} stayed on {lv['home_node']} (bystander, unaffected by "
                f"removal of {node_id}; now on: {new_node})")
        if new_node != lv["home_node"]:
            relocated.append(lv)

    # -------------------------------------------------------------------
    checklist.step(
        "5b. Reconnect stale NVMe paths + restart fio for relocated lvol(s) "
        "(same NQN, but a migrated lvol is served from the destination's own "
        "lvstore -- a different port/host set -- so the client's pre-removal "
        "controllers are permanently dead; see reconnect_lvol_after_relocation)")
    # -------------------------------------------------------------------
    for lv in relocated:
        if not lv["fio_proc"]:
            continue
        old_fio_log = lv["fio_log"]
        old_proc = lv["fio_proc"]
        if old_proc.poll() is None:
            lib.stop_fio(old_proc, post_wait=0)
        # The removal-induced IO error on the OLD path is expected, not a
        # regression -- check_fio_output(fault_injected=True) skips flagging
        # it later; a fresh error after this reconnect would not be.
        ok, verify_errs = lib.check_fio_output(old_fio_log, fault_injected=True)
        checklist.check(ok, f"{lv['name']}: pre-reconnect fio log has 0 VERIFY errors (found {verify_errs})")

        nqn = lib.get_nvme_subsystem_nqn(lv["device"])
        connected_new = lib.reconnect_lvol_after_relocation(lv["id"], nqn)
        checklist.check(bool(nqn), f"{lv['name']}: resolved NQN for reconnect ({nqn or 'none'})")

        # Always remount: even when the device node survives the reconnect,
        # XFS on it has hit EIO from the vanished old path and stays unusable.
        stacked = lib.mount_sources_at(lv["mount_point"])
        try:
            lib.wait_mount_idle(lv["mount_point"])
            lib.unmount_all_at(lv["mount_point"], lazy=False)
            lib.format_and_mount(lv["device"], lv["mount_point"], already_formatted=True)
            lib.verify_mount_usable(lv["mount_point"])
            remounted = True
        except RuntimeError as e:
            log.error(f"  {lv['name']}: remount failed (mounted there before: {stacked}): {e}")
            remounted = False
        checklist.check(remounted, f"{lv['name']}: remounted {lv['device']} after reconnect "
                                    f"(connected new path: {connected_new})")

        if remounted:
            new_log = f"{old_fio_log}.post_reconnect"
            lv["fio_log"] = new_log
            lv["fio_proc"] = lib.start_fio_bg(
                f"{lv['mount_point']}/testfile", new_log,
                size=FIO_SIZE, runtime=args.fio_runtime)
            checklist.check(bool(lv["fio_proc"]), f"{lv['name']}: fio restarted on reconnected path")
        else:
            lv["fio_proc"] = None

    # -------------------------------------------------------------------
    fio_lvols = [lv for lv in all_lvols if lv["fio_proc"]]
    checklist.step(
        f"6. Post-removal soak: keep fio running {args.post_removal_soak}s "
        f"after removal completes ({len(fio_lvols)} active fio lvol(s))")
    # -------------------------------------------------------------------
    soak_deadline = time.time() + args.post_removal_soak
    dead_during_soak = []
    while True:
        remaining = soak_deadline - time.time()
        if remaining <= 0:
            break
        for lv in fio_lvols:
            if lv["fio_proc"].poll() is not None and lv["name"] not in dead_during_soak:
                dead_during_soak.append(lv["name"])
                log.error(
                    f"  {lv['name']}: fio exited unexpectedly during the "
                    f"post-removal soak (rc={lv['fio_proc'].returncode})")
        log.info(
            f"  post-removal soak: {int(remaining)}s remaining, fio alive on "
            f"{len(fio_lvols) - len(dead_during_soak)}/{len(fio_lvols)} lvol(s)")
        time.sleep(min(SOAK_HEARTBEAT_SEC, remaining))
    checklist.check(
        not dead_during_soak,
        f"fio stayed alive on all active lvols through the {args.post_removal_soak}s "
        f"post-removal soak (died early on: {dead_during_soak or 'none'})"
    )

    # -------------------------------------------------------------------
    checklist.step("7. Stop fio, check for corruption / IO errors across removal + soak")
    # -------------------------------------------------------------------
    local_run("sudo dmesg -T > /tmp/dmesg_node_removal_mig.txt 2>/dev/null || true")
    log.info("dmesg saved to /tmp/dmesg_node_removal_mig.txt")

    for lv in fio_lvols:
        lib.stop_fio(lv["fio_proc"], post_wait=5)
        ok, verify_errs = lib.check_fio_output(lv["fio_log"])
        checklist.check(ok, f"{lv['name']}: fio 0 errors across removal + soak (found {verify_errs})")

    # -------------------------------------------------------------------
    checklist.step("8. Best-effort: grep control-plane logs for the from-secondary fallback")
    # -------------------------------------------------------------------
    hits, note = grep_fallback_log_evidence(t0)
    if note:
        log.warning(f"  Could not check logs directly: {note}")
    elif hits:
        log.info(f"  Found {len(hits)} matching log line(s):")
        for h in hits[:20]:
            log.info(f"    {h}")
    else:
        log.warning(
            "  No matching log lines found -- inconclusive (may be log rotation/retention, "
            "not necessarily evidence the fallback didn't fire; see the DB-placement and "
            "fio-integrity checks above for the load-bearing proof)."
        )

    # -------------------------------------------------------------------
    ok = checklist.summary()
    log.info(f"Log: {LOG_FILE}")
    log.info(f"NOTE: node {node_id} has been permanently removed from the cluster.")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
