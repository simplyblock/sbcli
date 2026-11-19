#!/usr/bin/env python3
"""Find ``object/LVolStatObject`` records whose LVol has been deleted.

LVol stats are stored under ``object/LVolStatObject/<pool_id>/<lvol_id>/<date>``
(see ``LVolStatObject.get_id``). For every distinct ``lvol_id`` found there,
this script checks whether ``object/LVol/<lvol_id>`` still exists — the same
record ``sbctl lvol get <lvol_id>`` reads (``DBController.get_lvol_by_id``).

Output: one row per LVol with its stat-key count, value bytes and status
(``exists`` / ``DELETED``), followed by totals.

The scan is paginated (one transaction per chunk), so it is safe on large
databases. It is read-only unless ``--delete`` is given.

``--delete`` clears every stats record of the DELETED LVols. Each LVol is
cleared in its own transaction that first re-checks that ``object/LVol/<id>``
is still absent, so an LVol that exists at delete time is never touched. It
asks for confirmation unless ``--yes`` is also given.

Usage:
    python scripts/find_orphan_lvol_stats.py [--cluster-file PATH] [--deleted-only] [--json]
    python scripts/find_orphan_lvol_stats.py --delete [--yes]
"""
import argparse
import json
import sys
from collections import defaultdict

import fdb

from simplyblock_core import constants

CHUNK_SIZE = 5000
STATS_PREFIX = b'object/LVolStatObject/'
LVOL_PREFIX = 'object/LVol/'


def next_prefix(prefix: bytes) -> bytes:
    """Smallest key greater than every key starting with ``prefix`` (fdb's
    ``strinc``, which this binding version does not export)."""
    stripped = prefix.rstrip(b'\xff')
    return stripped[:-1] + bytes([stripped[-1] + 1])


def scan(db, begin: bytes, end: bytes):
    while True:
        kvs = list(db.get_range(begin, end, limit=CHUNK_SIZE))
        if not kvs:
            return
        yield from kvs
        if len(kvs) < CHUNK_SIZE:
            return
        begin = kvs[-1].key + b'\x00'


def lvol_key(lvol_id: str) -> bytes:
    return (LVOL_PREFIX + lvol_id).encode('utf-8')


def stats_prefix(pool_id: str, lvol_id: str) -> bytes:
    return STATS_PREFIX + f'{pool_id}/{lvol_id}/'.encode()


def clear_orphan_stats(tr, lvol_id: str, pool_ids: list[str]) -> bool:
    """Clear the LVol's stats if, within this transaction, the LVol is still
    absent. Returns False (and clears nothing) if the LVol exists.

    Not decorated with ``@fdb.transactional`` here: the decorator refuses to
    run before ``fdb.api_version()``, which main() calls after import. It is
    wrapped at the call site instead."""
    if tr[lvol_key(lvol_id)].present():
        return False
    for pool_id in pool_ids:
        prefix = stats_prefix(pool_id, lvol_id)
        tr.clear_range(prefix, next_prefix(prefix))
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--cluster-file', default=constants.KVD_DB_FILE_PATH)
    parser.add_argument('--deleted-only', action='store_true', help='only list LVols that no longer exist')
    parser.add_argument('--json', action='store_true', help='print the result as JSON')
    parser.add_argument('--delete', action='store_true', help='clear the stats of the DELETED LVols')
    parser.add_argument('--yes', action='store_true', help='with --delete: do not ask for confirmation')
    args = parser.parse_args()

    fdb.api_version(constants.KVD_DB_VERSION)
    db = fdb.open(args.cluster_file)

    # lvol_id -> {pool_ids, keys, bytes}
    stats: dict[str, dict] = defaultdict(lambda: {'pool_ids': [], 'keys': 0, 'bytes': 0})
    for kv in scan(db, STATS_PREFIX, next_prefix(STATS_PREFIX)):
        parts = kv.key[len(STATS_PREFIX):].decode('utf-8', errors='replace').split('/')
        if len(parts) < 2:
            continue
        pool_id, lvol_id = parts[0], parts[1]
        entry = stats[lvol_id]
        if pool_id not in entry['pool_ids']:
            entry['pool_ids'].append(pool_id)
        entry['keys'] += 1
        entry['bytes'] += len(kv.value)

    rows = []
    for lvol_id, entry in stats.items():
        # On a Database (not a Transaction) the binding resolves the read and
        # returns the raw value, or None when the key is absent.
        exists = db[lvol_key(lvol_id)] is not None
        rows.append({'lvol_id': lvol_id, **entry, 'status': 'exists' if exists else 'DELETED'})
    rows.sort(key=lambda r: (r['status'] != 'DELETED', -r['keys']))

    deleted = [r for r in rows if r['status'] == 'DELETED']
    total = {
        'lvols': len(rows),
        'deleted_lvols': len(deleted),
        'stat_keys': sum(r['keys'] for r in rows),
        'orphan_stat_keys': sum(r['keys'] for r in deleted),
        'orphan_bytes': sum(r['bytes'] for r in deleted),
    }
    if args.deleted_only:
        rows = deleted

    if args.json:
        print(json.dumps({'lvols': rows, 'total': total}, indent=2))
    else:
        print(f"{'LVOL_ID':<36}  {'POOL_ID':<36}  {'KEYS':>8}  {'BYTES':>12}  STATUS")
        for r in rows:
            pools = ','.join(r['pool_ids'])
            print(f"{r['lvol_id']:<36}  {pools:<36}  {r['keys']:>8}  {r['bytes']:>12}  {r['status']}")
        print()
        print(f"LVols with stats:      {total['lvols']}")
        print(f"  of which deleted:    {total['deleted_lvols']}")
        print(f"Stat keys total:       {total['stat_keys']}")
        print(f"  orphaned (deleted):  {total['orphan_stat_keys']} ({total['orphan_bytes']} bytes)")

    if args.delete:
        delete_orphans(db, deleted, total, args.yes)


def delete_orphans(db, deleted: list[dict], total: dict, assume_yes: bool) -> None:
    if not deleted:
        print("Nothing to delete.", file=sys.stderr)
        return
    if not assume_yes:
        print(f"\nDelete {total['orphan_stat_keys']} stat keys of {len(deleted)} deleted LVols? [y/N] ",
              end='', file=sys.stderr, flush=True)
        if input().strip().lower() not in ('y', 'yes'):
            print("Aborted.", file=sys.stderr)
            return

    clear = fdb.transactional(clear_orphan_stats)
    cleared = skipped = 0
    for r in deleted:
        if clear(db, r['lvol_id'], r['pool_ids']):
            cleared += 1
        else:
            skipped += 1
            print(f"Skipped {r['lvol_id']}: LVol exists now", file=sys.stderr)
    print(f"Cleared stats of {cleared} LVols, skipped {skipped}.", file=sys.stderr)


if __name__ == '__main__':
    main()
