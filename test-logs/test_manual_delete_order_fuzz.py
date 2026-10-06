#!/usr/bin/env python3
"""
test_manual_delete_order_fuzz.py

TEMPORARY (branch: delete-test). Fuzz-tests arbitrary delete orderings
across an lvol + its snapshots, using the temporary
`sbctl --dev debug manual-delete` command (backed by
simplyblock_core.controllers.manual_delete_controller), which fires the same
two-phase RPC delete (async-start -> poll -> sync-finalize) production uses
and cleanly removes the DB record — no task-runner, no monitor, fully
synchronous and immediately checkable.

Per iteration:
  1. Create a fresh lvol, connect/mount it, and run a short fio prefill +
     background fio against it so snapshots actually capture different data.
  2. Take a random number of snapshots (--min-snapshots..--max-snapshots)
     while fio writes, spaced by --snapshot-interval.
  3. Stop fio, unmount, and disconnect the NVMe-oF connection.
  4. Shuffle [lvol, snap1, snap2, ...] into a random delete order.
  5. Fire that exact order through `debug manual-delete --json`.
  6. Pass iff every entity reports ok=True AND the cluster is healthy
     afterward. On any failure, stop immediately and report — the failing
     iteration's leftover state is intentionally NOT cleaned up so it can be
     inspected.

Usage:
  python3 test_manual_delete_order_fuzz.py
  python3 test_manual_delete_order_fuzz.py --iterations 100 --min-snapshots 10 --max-snapshots 20
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
POOL_NAME = "manual-delete-fuzz-pool"

LOG_DIR  = Path("/tmp/manual_delete_order_fuzz_logs") / datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "manual_delete_order_fuzz.log"


def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--iterations", type=int, default=30,
                   help="Number of iterations to run (default: 30)")
    p.add_argument("--lvol-size", default="1G", help="Lvol size (default: 1G)")
    p.add_argument("--fio-size", default="256M", help="fio data size (default: 256M)")
    p.add_argument("--min-snapshots", type=int, default=10,
                   help="Minimum snapshots per iteration (default: 10)")
    p.add_argument("--max-snapshots", type=int, default=20,
                   help="Maximum snapshots per iteration (default: 20)")
    p.add_argument("--pre-snapshot-fio-wait", type=int, default=5,
                   help="Seconds to let fio write before the first snapshot (default: 5)")
    p.add_argument("--snapshot-interval", type=int, default=2,
                   help="Seconds between each snapshot, so fio writes new data in "
                        "between instead of back-to-back identical snapshots (default: 2)")
    p.add_argument("--pool", default=POOL_NAME,
                   help=f"Pool name to create/reuse (default: {POOL_NAME})")
    p.add_argument("--delete-timeout", type=int, default=120,
                   help="Per-entity delete-status poll timeout, seconds (default: 120)")
    p.add_argument("--health-timeout", type=int, default=60,
                   help="Seconds to wait for the cluster to report healthy after each "
                        "iteration's deletes (default: 60)")
    return p.parse_args()


def create_iteration_snapshots(lvol_id, prefix, count, interval):
    lib.create_snapshots(lvol_id, prefix, count, interval=interval)
    snap_ids = []
    for i in range(1, count + 1):
        snap = lib.get_snapshot(lib.discover_cluster_id(), f"{prefix}{i}")
        if snap:
            snap_ids.append(lib.get(snap, "id"))
    return snap_ids


def fire_manual_delete(entities, timeout_s):
    """entities: ordered list of (kind, id). Returns the parsed JSON results
    list from `sbctl --dev debug manual-delete ... --json`, or None on a
    hard CLI/parse failure."""
    tokens = [f"{kind}:{obj_id}" for kind, obj_id in entities]
    results = lib.sbctl("--dev", "debug", "manual-delete", *tokens,
                        "--timeout", str(timeout_s), "--json", parse_json=True)
    return results


def main():
    args = parse_args()
    log = lib.init_logging(LOG_FILE)
    chk = lib.Checklist()

    log.info(f"Log dir : {LOG_DIR}")

    cluster_id = lib.discover_cluster_id()
    log.info(f"Cluster : {cluster_id}")

    nodes = lib.get_online_nodes()
    if not nodes:
        raise RuntimeError("Need >= 1 online node, got 0")
    node_ids = [lib.get(n, "id") for n in nodes if lib.get(n, "id")]
    source_node = node_ids[0]

    chk.step("0. Ensure pool")
    pool_id = lib.ensure_pool(args.pool, cluster_id)

    completed = 0
    for i in range(1, args.iterations + 1):
        lvol_name = f"fuzz_lvol_{i}"
        snap_prefix = f"fuzz_snap_{i}_"
        mount_point = f"/mnt/{lvol_name}"

        chk.step(f"Iteration {i}/{args.iterations}: create lvol, connect, mount, start fio")
        lvol_id = lib.create_lvol(lvol_name, args.lvol_size, pool_id, source_node)
        lib.connect_and_mount_lvol(lvol_id, mount_point, already_formatted=False)
        fio_file    = f"{mount_point}/fio_data.0.0"
        prefill_log = str(LOG_DIR / f"fio_prefill_{i}.log")
        fio_log     = str(LOG_DIR / f"fio_output_{i}.log")
        lib.fio_prefill(fio_file, prefill_log, size=args.fio_size)
        fio_proc = lib.start_fio_bg(fio_file, fio_log, size=args.fio_size, runtime=7200)
        chk.check(fio_proc is not None, f"[{i}] fio started in background")

        chk.step(f"Iteration {i}/{args.iterations}: create snapshots while fio writes")
        time.sleep(args.pre_snapshot_fio_wait)
        n_snaps = random.randint(args.min_snapshots, args.max_snapshots)
        snap_ids = create_iteration_snapshots(lvol_id, snap_prefix, n_snaps, args.snapshot_interval)
        chk.check(len(snap_ids) == n_snaps,
                  f"[{i}] created {len(snap_ids)}/{n_snaps} snapshots on {lvol_name}")

        chk.step(f"Iteration {i}/{args.iterations}: stop fio, unmount, disconnect")
        lib.stop_fio(fio_proc, post_wait=0)
        lib.local_run(f"sudo umount {mount_point} 2>/dev/null || true")
        lib.local_run("sudo nvme disconnect-all 2>/dev/null || true")

        entities = [("lvol", lvol_id)] + [("snapshot", sid) for sid in snap_ids]
        random.shuffle(entities)
        log.info(f"[{i}] delete order: " +
                ", ".join(f"{k}:{v[:8]}" for k, v in entities))

        results = fire_manual_delete(entities, args.delete_timeout)
        if results is None:
            chk.check(False, f"[{i}] manual-delete: no parseable JSON output")
            log.error(f"[{i}] Aborting — leaving iteration state for inspection")
            break

        failures = [r for r in results if not r.get("ok")]
        if failures:
            for f in failures:
                log.error(f"[{i}] FAILED {f.get('kind')} {f.get('id')} "
                          f"({f.get('bdev')}): {f.get('error')}")
        chk.check(not failures, f"[{i}] all {len(results)} deletes reported ok")
        if failures:
            log.error(f"[{i}] Aborting — leaving iteration state for inspection")
            break

        healthy = lib.wait_cluster_healthy(timeout=args.health_timeout)
        chk.check(healthy, f"[{i}] cluster healthy after deletes")
        if not healthy:
            log.error(f"[{i}] Aborting — leaving iteration state for inspection")
            break

        completed += 1
        log.info(f"[{i}] OK ({n_snaps} snapshots, iteration complete)")

    chk.step("Summary")
    chk.check(completed == args.iterations,
              f"Completed {completed}/{args.iterations} iterations")

    ok = chk.summary()
    log.info(f"Log dir: {LOG_DIR}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
