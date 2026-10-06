#!/usr/bin/env python3
"""
test_migration_continuous_fio.py

Continuous-fio migration scenario built on migration_test_lib.py.

Each iteration:
  1. Create `--lvol-count` lvols on EVERY online storage node (so total new
     lvols per iteration = lvol_count * node_count).
  2. Connect + mount each new lvol, prefill it, then start background fio
     (randrw + verify=md5) that is never stopped.
  3. Migrate every new lvol to a random *different* node (concurrency
     controlled by --migrate-concurrency: 1 = one at a time, N = up to N
     migrations in flight at once).
  4. Move to the next iteration WITHOUT deleting, disconnecting, or
     stopping fio on any lvol from this or any previous iteration — fio
     keeps running through and after the migration.

Nothing is ever cleaned up by this script (no --teardown): lvols, mounts,
and fio processes from every iteration are left running so migration keeps
happening against an ever-growing, continuously-active fleet.

Usage:
  python3 test_migration_continuous_fio.py
  python3 test_migration_continuous_fio.py --iterations 5 --lvol-count 3
  python3 test_migration_continuous_fio.py --lvol-size 10G --snapshots 2
  python3 test_migration_continuous_fio.py --migrate-concurrency 4
"""

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import migration_test_lib as lib

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
POOL_NAME  = "continuous-fio-pool"
MOUNT_BASE = "/mnt/continuous_fio"
FIO_RUNTIME = 24 * 3600   # fio is meant to run indefinitely across iterations

LOG_DIR = Path("/tmp/continuous_fio_logs") / datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "continuous_fio.log"


def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--lvol-size", default="5G",
                   help="Size of each created lvol (default: 5G)")
    p.add_argument("--snapshots", type=int, default=0,
                   help="Snapshots to create on each lvol before migration (default: 0)")
    p.add_argument("--iterations", type=int, default=2,
                   help="Number of iterations to run (default: 2)")
    p.add_argument("--lvol-count", type=int, default=2,
                   help="Lvols created PER storage node, per iteration (default: 2)")
    p.add_argument("--migrate-concurrency", type=int, default=1,
                   help="Migrations in flight at once per iteration: "
                        "1 = one at a time (default), N = up to N concurrently")
    p.add_argument("--fio-size", default="1G",
                   help="fio data size per lvol (default: 1G)")
    p.add_argument("--pool", default=POOL_NAME,
                   help=f"Pool name to create/reuse (default: {POOL_NAME})")
    return p.parse_args()


def create_iteration_batch(iteration, args, pool_id, node_ids):
    """Create lvol_count lvols on every node for this iteration. Returns a
    list of dicts (not yet connected/mounted/migrated).
    """
    batch = []
    for node_id in node_ids:
        for i in range(args.lvol_count):
            name = f"cfio_{node_id[-6:]}_{iteration}_{i}"
            lvol_id = lib.create_lvol(name, args.lvol_size, pool_id, node_id, snapshot=True)
            if args.snapshots:
                lib.create_snapshots(lvol_id, f"{name}_snap", args.snapshots)
            batch.append({"name": name, "id": lvol_id, "home_node": node_id})
            lib.log.info(f"  created {name} ({lvol_id}) on {node_id}")
    return batch


def connect_mount_and_run_fio(entry, args):
    """Connect, mount, and start continuous background fio for one lvol.
    Mutates `entry` in place with mount_point/device/fio_log/fio_proc.
    """
    mount_point = f"{MOUNT_BASE}/{entry['name']}"
    device = lib.connect_and_mount_lvol(entry["id"], mount_point, already_formatted=False)

    fio_file = f"{mount_point}/fio_data.0.0"
    prefill_log = str(LOG_DIR / f"fio_{entry['name']}_prefill.log")
    fio_log     = str(LOG_DIR / f"fio_{entry['name']}.log")

    lib.fio_prefill(fio_file, prefill_log, size=args.fio_size)
    proc = lib.start_fio_bg(fio_file, fio_log, size=args.fio_size, runtime=FIO_RUNTIME)

    entry.update(mount_point=mount_point, device=device, fio_log=fio_log, fio_proc=proc)
    lib.log.info(f"  {entry['name']}: mounted {device} -> {mount_point}, fio running (pid={proc.pid if proc else 'n/a'})")


def migrate_entry(entry, node_ids, cluster_id):
    """Migrate one lvol to a random other node and wait for a terminal
    status. fio (already started) is left completely untouched.
    """
    target = lib.pick_random_target_node(entry["home_node"], node_ids)
    lib.log.info(f"  migrating {entry['name']}: {entry['home_node']} -> {target}")
    status = lib.migrate_lvol(entry["id"], target, cluster_id)
    entry["target_node"] = target
    entry["migration_status"] = status
    lib.log.info(f"  {entry['name']}: migration terminal status={status}")
    return entry


def main():
    args = parse_args()
    log = lib.init_logging(LOG_FILE)

    log.info(f"Log dir : {LOG_DIR}")

    cluster_id = lib.discover_cluster_id()
    log.info(f"Cluster : {cluster_id}")

    pool_id = lib.ensure_pool(args.pool, cluster_id)
    log.info(f"Pool    : {pool_id}")

    nodes = lib.get_online_nodes()
    if len(nodes) < 2:
        raise RuntimeError(f"Need >= 2 online nodes, got {len(nodes)}")
    node_ids = [lib.get(n, "id") for n in nodes if lib.get(n, "id")]
    log.info(f"Nodes   : {node_ids}")

    managed = []  # every lvol ever created this run — never removed

    for iteration in range(1, args.iterations + 1):
        log.info(f"\n{'=' * 70}")
        log.info(f"ITERATION {iteration}/{args.iterations}")
        log.info(f"{'=' * 70}")

        log.info(f"Creating {args.lvol_count} lvol(s) per node "
                 f"({args.lvol_count * len(node_ids)} total)...")
        batch = create_iteration_batch(iteration, args, pool_id, node_ids)

        log.info(f"Connecting, mounting, and starting continuous fio "
                 f"on {len(batch)} lvol(s)...")
        for entry in batch:
            connect_mount_and_run_fio(entry, args)
            managed.append(entry)

        log.info(f"Migrating {len(batch)} lvol(s) "
                 f"(concurrency={args.migrate_concurrency})...")
        with ThreadPoolExecutor(max_workers=max(1, args.migrate_concurrency)) as pool:
            list(pool.map(lambda e: migrate_entry(e, node_ids, cluster_id), batch))

        log.info(f"Iteration {iteration} done — "
                 f"{len(managed)} lvol(s) total, all with fio still running.")

    log.info(f"\n{'=' * 70}")
    log.info("ALL ITERATIONS COMPLETE — nothing deleted, disconnected, or stopped")
    log.info(f"{'=' * 70}")
    for entry in managed:
        node_now = entry.get("target_node", entry["home_node"])
        log.info(f"  {entry['name']:<30} node={node_now}  "
                 f"migration={entry.get('migration_status', 'n/a')}  "
                 f"fio_log={entry['fio_log']}")
    log.info(f"\nTotal lvols running: {len(managed)}")
    log.info(f"Log dir: {LOG_DIR}")


if __name__ == "__main__":
    main()
