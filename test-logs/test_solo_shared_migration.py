#!/usr/bin/env python3
"""
test_solo_shared_migration.py

EXPERIMENTAL — validates the new `lvol migrate --solo` path added on this
branch: migrating exactly ONE member of a shared-namespace (namespaced)
subsystem to a different node, WITHOUT moving its siblings and WITHOUT the
`--batch` group machinery.

Design under test (see migration_controller.create_migration's
solo_from_shared param):
  - Target gets its own local instance of the group's EXISTING nqn (not a new
    one), containing only the migrating lvol's namespace, pinned to its
    existing ns_id.
  - Cutover reuses the ordinary single-lvol ANA-flip path unchanged — it only
    touches this lvol's own ANA group (anagrpid=lvol.ns_id), never the
    subsystem as a whole.
  - Siblings never move and must see zero disruption throughout.

Because the NQN never changes, connecting the pre-created (inaccessible)
target path must NOT produce a new top-level NVMe device — it's a second
path under the subsystem the client already has open for this lvol's
siblings. This is the central hypothesis this script checks first, before
even starting data transfer.

IMPORTANT — do NOT reuse test_lvol_migration.py's cleanup_src_controllers()
here: it disconnects the whole SRC controller once cutover is seen, which in
a regular (non-shared) migration is correct because nothing else needs that
controller. Here the SRC controller is still serving every sibling's
namespace — disconnecting it would take the whole group down. This script
never disconnects anything; it only observes.

Usage:
  python3 test_solo_shared_migration.py [--target no-overlap|a|b|c|d|<hostname>]
                                        [--members N] [--size 5G]
                                        [--fio-runtime 180] [--teardown]
"""

import argparse
import os
import re
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import migration_test_lib as lib
from migration_test_lib import sbctl, local_run, Checklist

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
POOL_NAME    = "solo-shared-mig-pool"
LVOL_PREFIX  = "solo_mig_lvol"
GROUP_SIZE   = 4          # 1 master + (GROUP_SIZE - 1) members, default
LVOL_SIZE    = "5G"
FIO_SIZE     = "1G"
FIO_RUNTIME  = 180        # seconds — covers setup + migration + buffer
MOUNT_ROOT   = "/mnt/solo_mig"
FIO_JOB_FILE = "/tmp/solo_mig.fio"
FIO_LOG      = "/tmp/solo_mig_fio.log"

# Post-migration soak: keep exercising ONLY the migrated lvol for this long
# afterward, checking for corruption and confirming routing every ~interval.
SOAK_SECONDS       = 600  # 10 minutes
SOAK_INTERVAL      = 60   # seconds between checks
SOAK_PASS_SIZE     = "256M"
SOAK_PASS_TIMEOUT  = 55   # keep comfortably under SOAK_INTERVAL
SOAK_LOG           = "/tmp/solo_mig_soak.log"

LOG_DIR = Path("/tmp/migration_test_logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"solo_shared_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

log = lib.init_logging(LOG_FILE)


def pick_group_host_node(nodes):
    """Pick the node to host the shared-namespace group.

    lib.pick_source_node() requires BOTH secondary AND tertiary configured —
    that's for the overlap Case A/B/C/D tests in test_lvol_migration.py.
    Shared-subsystem placement and --solo migration don't depend on any HA
    topology at all, so this only prefers a node with a secondary (closer to
    a realistic deployment) and falls back to any online node.
    """
    for n in nodes:
        nid = lib.get(n, "id")
        if nid and lib.get_node_secondary_id(nid, nodes):
            return nid
    return lib.get(nodes[0], "id")


# ---------------------------------------------------------------------------
# Per-namespace ANA/routing tracking (proves which physical path is actually
# serving a given nsid — not just "is the lvol online", but "which side is
# it really being read/written through right now")
# ---------------------------------------------------------------------------
def get_ana_for_nsid(nsid):
    """Return {controller_ns_name: ana_state} for exactly `nsid`, across every
    NVMe-oF path currently visible on this host (e.g. {'nvme145c171n2':
    'optimized'}). Filters sysfs paths like .../nvme145c171n2/ana_state by an
    exact trailing nsid match so nsid=2 never matches an n21, n2x, etc."""
    out, _, _ = local_run(
        "for ns in /sys/class/nvme/nvme[0-9]*/nvme[0-9]*n[0-9]*; do "
        "  n=$(basename $ns); "
        "  ana=$(cat $ns/ana_state 2>/dev/null || echo n/a); "
        "  echo \"$n:$ana\"; "
        "done 2>/dev/null || true"
    )
    result = {}
    for line in out.splitlines():
        if ":" not in line:
            continue
        name, ana = line.rsplit(":", 1)
        m = re.match(r'.*n(\d+)$', name)
        if m and int(m.group(1)) == nsid:
            result[name] = ana
    return result


def run_soak_pass(mount_point, pass_idx, size=SOAK_PASS_SIZE):
    """One short, self-contained write+verify=md5 fio pass against a
    dedicated soak file inside `mount_point` (kept separate from the group
    fio's 'testfile' so it doesn't collide with anything left over from
    earlier steps). Each pass writes fresh data and verifies it round-trip
    within the SAME pass — that's real corruption detection (not just "did
    the process not crash"), just scoped to what that pass itself wrote,
    repeated every SOAK_INTERVAL for the full soak window instead of waiting
    for one long run to finish.

    Returns (ok: bool, verify_errs: int, io_errs: int, tail: str).
    """
    fio_file = f"{mount_point}/soak_testfile"
    cmd = [
        "sudo", "fio", "--name=soak",
        f"--filename={fio_file}", f"--size={size}",
        "--numjobs=1", "--direct=1", "--ioengine=libaio", "--iodepth=8",
        "--readwrite=randrw", "--bsrange=4k:128k",
        "--verify=md5", "--do_verify=1",
        f"--output={SOAK_LOG}",
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              timeout=SOAK_PASS_TIMEOUT + 30)
        rc = proc.returncode
    except subprocess.TimeoutExpired:
        rc = -1
    try:
        content = Path(SOAK_LOG).read_text()
    except FileNotFoundError:
        content = ""
    verify_errs = sum(int(m.group(1)) for m in
                      re.finditer(r'verify_errors\s*[:=]\s*(\d+)', content, re.I))
    io_errs = sum(int(m.group(1)) for m in re.finditer(r'\berr\s*=\s*(\d+)', content))
    ok = rc == 0 and verify_errs == 0 and io_errs == 0
    tag = f"pass {pass_idx}: soak fio timed out" if rc == -1 else f"pass {pass_idx}: rc={rc}"
    log.info(f"  {tag} verify_errs={verify_errs} io_errs={io_errs}")
    return ok, verify_errs, io_errs, content[-800:]


def connect_group_members(members):
    """Connect + mkfs/mount every member (lib.connect_and_mount_batch_members),
    then return (members-with-device-and-mount_point, nqn). All members share
    one NQN, so it's read off just the first member's connect output."""
    connect_out = sbctl("volume", "connect", members[0]["id"])
    nqn = None
    for cmd in lib.parse_nvme_connect_cmds(connect_out):
        m = re.search(r'--nqn=(\S+)', cmd)
        if m:
            nqn = m.group(1)
            break
    if not nqn:
        raise RuntimeError(
            f"Could not extract NQN from volume connect output for {members[0]['name']}")
    members = lib.connect_and_mount_batch_members(members)
    return members, nqn


# ---------------------------------------------------------------------------
# Group-wide fio (single background job, one [section] per member — each
# writes to a FILE inside that member's mounted filesystem, not the raw
# device: fio refuses to write a raw block device that's currently mounted
# ("appears mounted, and 'allow_mounted_write' isn't set"), since that would
# corrupt the live filesystem sitting on top of it.)
# ---------------------------------------------------------------------------
def start_group_fio_bg(fio_files, runtime, fio_size):
    job_lines = []
    for i, f in enumerate(fio_files):
        job_lines.append(f"[job{i}]")
        job_lines.append(f"filename={f}")

    Path(FIO_JOB_FILE).write_text("\n".join([
        "[global]",
        f"size={fio_size}",
        "numjobs=1",
        "direct=1",
        "ioengine=libaio",
        "iodepth=8",
        "readwrite=randrw",
        "bsrange=4k:128k",
        "verify=md5",
        "do_verify=1",
        "time_based=1",
        f"runtime={runtime}",
        # status_interval isn't supported by every fio build on the test VMs
        # (rejected as an unknown option on at least one) and periodic
        # progress output isn't needed — only the final per-job summary is
        # parsed by check_group_fio_output(), so just omit it.
        "group_reporting=0",
        "",
    ] + job_lines))

    log.info(f"Starting group fio across {len(fio_files)} file(s) for up to {runtime}s: {fio_files}")
    proc = subprocess.Popen(
        ["sudo", "fio", FIO_JOB_FILE, f"--output={FIO_LOG}"],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, start_new_session=True,
    )
    time.sleep(2)
    if proc.poll() is not None:
        _, err = proc.communicate()
        raise RuntimeError(f"group fio exited immediately (rc={proc.returncode}): "
                           f"{(err or b'').decode()[:400]}")
    return proc


def stop_group_fio(proc, wait_extra=10):
    if proc.poll() is not None:
        log.warning(f"group fio already exited (rc={proc.returncode}) before stop")
        return
    log.info(f"Waiting {wait_extra}s then stopping group fio...")
    time.sleep(wait_extra)
    try:
        os.killpg(proc.pid, signal.SIGINT)
    except ProcessLookupError:
        pass
    try:
        _, stderr = proc.communicate(timeout=30)
        if stderr:
            log.debug(f"group fio stderr: {stderr.decode()[:600]}")
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, 9)
        except ProcessLookupError:
            pass
        proc.wait()


def check_group_fio_output(members):
    """Parse the group fio log for per-job verify/IO errors, keyed by jobname
    (job0, job1, ...) so a failure can be attributed to a specific member
    instead of just a pass/fail blob."""
    try:
        content = Path(FIO_LOG).read_text()
    except FileNotFoundError:
        log.warning(f"fio log not found: {FIO_LOG}")
        return {m["name"]: (False, -1) for m in members}

    log.info(f"--- group fio output (last 4000 chars) ---\n{content[-4000:]}\n---")

    results = {}
    # fio's normal output prints one block per job starting with "jobN: ..."
    blocks = re.split(r'\njob(\d+)[:\s]', "\n" + content)
    # blocks alternates [preamble, "0", block0, "1", block1, ...]
    per_job = {}
    for i in range(1, len(blocks), 2):
        job_idx = int(blocks[i])
        body = blocks[i + 1] if i + 1 < len(blocks) else ""
        verify_errs = sum(int(m.group(1)) for m in
                          re.finditer(r'verify_errors\s*[:=]\s*(\d+)', body, re.I))
        io_errs = sum(int(m.group(1)) for m in re.finditer(r'\berr\s*=\s*(\d+)', body))
        per_job[job_idx] = (verify_errs, io_errs)

    missing = [i for i in range(len(members)) if i not in per_job]
    if missing:
        log.warning(
            f"check_group_fio_output: could not parse per-job results for job(s) "
            f"{missing} — fio output format may have changed; treating those as "
            f"UNVERIFIED, not passed. Inspect {FIO_LOG} manually.")

    for i, member in enumerate(members):
        if i in missing:
            results[member["name"]] = (False, -1)
            log.error(f"  {member['name']} (job{i}): UNVERIFIED (parse failure)")
            continue
        verify_errs, io_errs = per_job[i]
        ok = verify_errs == 0 and io_errs == 0
        results[member["name"]] = (ok, verify_errs + io_errs)
        if ok:
            log.info(f"  {member['name']} (job{i}): OK")
        else:
            log.error(f"  {member['name']} (job{i}): verify_errs={verify_errs} io_errs={io_errs}")
    return results


# ---------------------------------------------------------------------------
# Args
# ---------------------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--target", default="no-overlap", metavar="MODE_OR_NODE",
                   help="'no-overlap' (default), 'a', 'b', 'c', 'd', or hostname prefix/UUID")
    p.add_argument("--members", type=int, default=GROUP_SIZE,
                   help=f"Total lvols in the shared group, master included (default: {GROUP_SIZE})")
    p.add_argument("--size", default=LVOL_SIZE, help=f"Lvol size (default: {LVOL_SIZE})")
    p.add_argument("--fio-runtime", type=int, default=FIO_RUNTIME,
                   help=f"fio runtime in seconds (default: {FIO_RUNTIME})")
    p.add_argument("--fio-size", default=FIO_SIZE, help=f"fio size per device (default: {FIO_SIZE})")
    p.add_argument("--teardown", action="store_true", help="Delete all test lvols and pool, then exit")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    args = parse_args()
    checklist = Checklist()

    log.info(f"Log file : {LOG_FILE}")
    cluster_id = lib.discover_cluster_id()
    log.info(f"Cluster  : {cluster_id}")

    pool_id = lib.ensure_pool(POOL_NAME, cluster_id)

    if args.teardown:
        local_run("sudo killall fio 2>/dev/null || true")
        for mnt in Path("/mnt").glob(f"{Path(MOUNT_ROOT).name}_*"):
            local_run(f"sudo umount {mnt} 2>/dev/null || true")
        local_run("sudo nvme disconnect-all 2>/dev/null || true")
        time.sleep(2)
        lib.cleanup_pool_and_lvols(POOL_NAME, cluster_id)
        sbctl("storage-pool", "delete", pool_id)
        log.info("Teardown complete")
        return

    nodes = lib.get_online_nodes()
    if len(nodes) < 2:
        raise RuntimeError(f"Need at least 2 online nodes, got {len(nodes)}")

    source_node = pick_group_host_node(nodes)
    target_node = lib.pick_target_node(source_node, nodes, args.target)
    log.info(f"Source (group host): {source_node}")
    log.info(f"Target (migration) : {target_node}  (mode: {args.target})")

    max_ns = args.members

    # -------------------------------------------------------------------
    checklist.step(f"1. Create shared-namespace group ({args.members} lvols) on {source_node}")
    # -------------------------------------------------------------------
    members = []
    for i in range(args.members):
        name = f"{LVOL_PREFIX}_{i}"
        lvol_id = lib.create_lvol(name, args.size, pool_id, source_node,
                                  namespaced=True, max_ns=max_ns, snapshot=False)
        members.append({"name": name, "id": lvol_id,
                        "mount_point": f"{MOUNT_ROOT}_{i}"})
    checklist.check(len(members) == args.members, f"Created {len(members)} group members")

    # Migrate the SECOND member (ns_id != 1), not the master — exercises the
    # nsid-pinning path on a non-trivial nsid rather than the easy nsid=1 case.
    to_migrate = members[1] if len(members) > 1 else members[0]
    siblings = [m for m in members if m["id"] != to_migrate["id"]]
    log.info(f"Migrating: {to_migrate['name']} ({to_migrate['id']})")
    log.info(f"Siblings staying on {source_node}: {[m['name'] for m in siblings]}")

    # -------------------------------------------------------------------
    checklist.step("2. Connect + mount all group members")
    # -------------------------------------------------------------------
    local_run("sudo nvme disconnect-all 2>/dev/null || true")
    time.sleep(2)
    members, nqn = connect_group_members(members)
    for m in members:
        checklist.check(bool(m.get("device")), f"{m['name']}: device resolved ({m.get('device')})")
    checklist.check(bool(nqn), f"Resolved shared NQN: {nqn}")

    lib.log_client_nvme_state("after-group-connect")

    # Capture the migrated lvol's real nsid and its pre-migration ANA paths
    # (source primary + secondary) so post-migration checks can prove those
    # specific paths are gone and only the new target path serves it.
    nsid = lib._lvol_ns_id(to_migrate["id"])
    pre_migrate_ana = get_ana_for_nsid(nsid)
    log.info(f"  {to_migrate['name']} nsid={nsid}  pre-migration ANA paths: {pre_migrate_ana}")

    # -------------------------------------------------------------------
    checklist.step(f"3. Start group fio ({args.fio_runtime}s) across all {len(members)} members")
    # -------------------------------------------------------------------
    fio_files = [f"{m['mount_point']}/testfile" for m in members]
    fio_proc = start_group_fio_bg(fio_files, args.fio_runtime, args.fio_size)

    # -------------------------------------------------------------------
    checklist.step(f"4. migrate --solo (precreate): {to_migrate['name']} -> {target_node}")
    # -------------------------------------------------------------------
    devices_before_migrate = lib.list_nvme_namespaces()

    migration_id = lib.start_solo_migration(to_migrate["id"], target_node)
    checklist.check(bool(migration_id), f"Got migration ID: {migration_id}")

    time.sleep(3)
    devices_after_connect = lib.list_nvme_namespaces()
    checklist.check(
        devices_after_connect == devices_before_migrate,
        "No new top-level NVMe device appeared after connecting the TGT path "
        f"(before={devices_before_migrate}  after={devices_after_connect})"
    )

    lib.log_client_nvme_state("after-solo-precreate-connect")

    ana_out, _, _ = local_run(
        "for ns in /sys/class/nvme/nvme[0-9]*/nvme[0-9]*n[0-9]*; do "
        "  ana=$(cat $ns/ana_state 2>/dev/null || echo n/a); echo $ana; "
        "done 2>/dev/null || true"
    )
    inaccessible_count = sum(1 for l in ana_out.splitlines() if "inaccessible" in l)
    checklist.check(inaccessible_count >= 1,
                    f"At least one inaccessible ANA path present after precreate "
                    f"(got {inaccessible_count})")

    # -------------------------------------------------------------------
    checklist.step(f"5. migrate-continue {migration_id}  (group fio still running)")
    # -------------------------------------------------------------------
    lib.continue_migration(migration_id)

    # -------------------------------------------------------------------
    checklist.step("6. Wait for migration to complete — DO NOT touch client NVMe state")
    # -------------------------------------------------------------------
    # Unlike a regular (non-shared) migration, we must not disconnect the SRC
    # controller on seeing 'cutover' — it still serves every sibling.
    status = lib.wait_for_migration(to_migrate["id"], cluster_id, terminal_only=True)
    checklist.check(status in ("done", "completed"), f"Migration status: {status}")

    lib.log_client_nvme_state("post-migration")

    # -------------------------------------------------------------------
    checklist.step("7. Verify DB placement: migrated lvol moved, siblings did not")
    # -------------------------------------------------------------------
    moved_node = lib.get_lvol_node(to_migrate["id"])
    checklist.check(moved_node == target_node,
                    f"{to_migrate['name']} now on target node {target_node} (got: {moved_node})")

    for sib in siblings:
        sib_node = lib.get_lvol_node(sib["id"])
        checklist.check(sib_node == source_node,
                        f"{sib['name']} still on source node {source_node} (got: {sib_node})")

    # Routing proof: the old (source) paths for this nsid must be gone, and
    # whatever paths remain must include an 'optimized' one — i.e. the client
    # is provably being served by the new (target) location, not a stale path.
    post_migrate_ana = get_ana_for_nsid(nsid)
    retired = set(pre_migrate_ana) - set(post_migrate_ana)
    log.info(f"  {to_migrate['name']} nsid={nsid}  post-migration ANA paths: {post_migrate_ana}")
    log.info(f"  retired (old SRC) paths: {retired or '(none)'}")
    checklist.check(bool(retired),
                    f"Old SRC path(s) for nsid={nsid} retired after migration: {retired}")
    checklist.check(
        any(state == "optimized" for state in post_migrate_ana.values()),
        f"An 'optimized' path exists for nsid={nsid} post-migration: {post_migrate_ana}"
    )

    # -------------------------------------------------------------------
    checklist.step(f"8. Stop group fio, collect dmesg, check per-member results")
    # -------------------------------------------------------------------
    local_run("sudo dmesg -T > /tmp/dmesg_solo_shared.txt 2>/dev/null || true")
    log.info("dmesg saved to /tmp/dmesg_solo_shared.txt")

    stop_group_fio(fio_proc)
    results = check_group_fio_output(members)
    for m in members:
        ok, errs = results.get(m["name"], (False, -1))
        role = "MIGRATED" if m["id"] == to_migrate["id"] else "sibling"
        checklist.check(ok, f"{m['name']} ({role}): fio 0 errors (found {errs})")

    # -------------------------------------------------------------------
    checklist.step(
        f"9. Post-migration soak on {to_migrate['name']} only: "
        f"{SOAK_SECONDS}s, checked every ~{SOAK_INTERVAL}s"
    )
    # -------------------------------------------------------------------
    soak_start = time.time()
    soak_deadline = soak_start + SOAK_SECONDS
    pass_idx = 0
    while time.time() < soak_deadline:
        pass_idx += 1
        iter_start = time.time()
        into_soak = int(iter_start - soak_start)

        ok, verify_errs, io_errs, tail = run_soak_pass(to_migrate["mount_point"], pass_idx)
        if not ok:
            log.error(f"  soak pass {pass_idx} FAILED — fio tail:\n{tail}")
        checklist.check(
            ok, f"soak pass {pass_idx} (~{into_soak}s into the {SOAK_SECONDS}s soak): "
                f"0 errors (verify={verify_errs} io={io_errs})"
        )

        cur_ana = get_ana_for_nsid(nsid)
        reappeared = retired & set(cur_ana)
        still_routed = any(state == "optimized" for state in cur_ana.values())
        checklist.check(
            not reappeared and still_routed,
            f"soak pass {pass_idx}: nsid={nsid} still routed via target only "
            f"(paths={cur_ana}, reappeared-old-paths={reappeared or '(none)'})"
        )

        elapsed = time.time() - iter_start
        remaining_in_cycle = SOAK_INTERVAL - elapsed
        if remaining_in_cycle > 0 and time.time() + remaining_in_cycle < soak_deadline:
            time.sleep(remaining_in_cycle)

    log.info(f"Soak complete: {pass_idx} pass(es) over ~{SOAK_SECONDS}s")

    # -------------------------------------------------------------------
    ok = checklist.summary()
    log.info(f"Log: {LOG_FILE}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
