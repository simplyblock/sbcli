#!/usr/bin/env python3
"""Find and clean up ``object/PoolStatObject`` records: stats of deleted
pools, or stats older than a given age.

Pool stats are stored under ``object/PoolStatObject/<pool_id>/<pool_id>/<date>``
(``PoolStatObject`` inherits ``LVolStatObject.get_id``; the collector sets
both segments to the pool id, and ``<date>`` is epoch seconds). For every
distinct ``<pool_id>/<uuid>`` prefix found there, this script checks whether
``object/Pool/<pool_id>`` still exists — the same record
``sbctl pool get <pool_id>`` reads (``DBController.get_pool_by_id``).

Output: one row per pool with its stat-key count, value bytes, oldest/newest
date, the number of records older than the cutoff (with
``--older-than-days``) and status (``exists`` / ``DELETED``), followed by
totals.

``--delete`` has two modes:

* without ``--older-than-days``: clears every stats record of the DELETED
  pools. Each pool is cleared in its own transaction that first re-checks
  that ``object/Pool/<id>`` is still absent, so a pool that exists at delete
  time is never touched.
* with ``--older-than-days N``: clears the records dated before now - N days,
  for EVERY pool (existing and DELETED). Newer records are kept.

It asks for confirmation unless ``--yes`` is also given. The scan is
paginated (one transaction per chunk), so it is safe on large databases; it is
read-only unless ``--delete`` is given.

Usage:
    python scripts/find_orphan_pool_stats.py [--cluster-file PATH] [--deleted-only] [--json]
    python scripts/find_orphan_pool_stats.py --delete [--yes]
    python scripts/find_orphan_pool_stats.py --older-than-days 90 [--delete [--yes]]
"""
import argparse
import json
import sys
import time
from collections import defaultdict
from datetime import datetime

import fdb

from simplyblock_core import constants

CHUNK_SIZE = 5000
STATS_PREFIX = b'object/PoolStatObject/'
POOL_PREFIX = 'object/Pool/'


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


def pool_key(pool_id: str) -> bytes:
    return (POOL_PREFIX + pool_id).encode()


def stats_prefix(group: str) -> bytes:
    """``group`` is ``<pool_id>/<uuid>``."""
    return STATS_PREFIX + f'{group}/'.encode()


def format_date(date: str) -> str:
    """Stats dates are epoch seconds; tolerate epoch ms too."""
    try:
        ts = int(date)
    except ValueError:
        return date
    return str(datetime.fromtimestamp(ts / 1000 if ts > 1e10 else ts))[:19]


def clear_orphan_stats(tr, pool_id: str, groups: list[str]) -> bool:
    """Clear the pool's stats if, within this transaction, the pool is still
    absent. Returns False (and clears nothing) if the pool exists.

    Neither clear function is decorated with ``@fdb.transactional`` here: the
    decorator refuses to run before ``fdb.api_version()``, which main() calls
    after import. They are wrapped at the call site instead."""
    if tr[pool_key(pool_id)].present():
        return False
    for group in groups:
        prefix = stats_prefix(group)
        tr.clear_range(prefix, next_prefix(prefix))
    return True


def clear_old_stats(tr, groups: list[str], cutoff: str) -> None:
    """Clear the records whose date sorts before ``cutoff`` (epoch seconds).

    The range end ``<prefix><cutoff>`` compares the date segment as a string,
    which is exactly the rule the scan uses to count old records. It also holds
    for a stray 13-digit epoch-ms date: its first 10 digits are the same
    instant in seconds, so it sorts on the correct side of the cutoff."""
    for group in groups:
        prefix = stats_prefix(group)
        tr.clear_range(prefix, prefix + cutoff.encode())


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--cluster-file', default=constants.KVD_DB_FILE_PATH)
    parser.add_argument('--deleted-only', action='store_true', help='only list pools that no longer exist')
    parser.add_argument('--older-than-days', type=int, metavar='N',
                        help='count (and with --delete, clear) records older than N days for every pool')
    parser.add_argument('--json', action='store_true', help='print the result as JSON')
    parser.add_argument('--delete', action='store_true',
                        help='clear stats of DELETED pools, or with --older-than-days the old records')
    parser.add_argument('--yes', action='store_true', help='with --delete: do not ask for confirmation')
    args = parser.parse_args()
    if args.older_than_days is not None and args.older_than_days < 1:
        parser.error('--older-than-days must be at least 1')

    cutoff = ''
    if args.older_than_days is not None:
        cutoff = str(int(time.time() - args.older_than_days * 86400))

    fdb.api_version(constants.KVD_DB_VERSION)
    db = fdb.open(args.cluster_file)

    # pool_id -> counters; keys are date-ordered within a group, so the first
    # key seen is the oldest.
    stats: dict[str, dict] = defaultdict(lambda: {
        'groups': [], 'keys': 0, 'bytes': 0, 'old_keys': 0, 'old_bytes': 0, 'oldest': '', 'newest': ''})
    for kv in scan(db, STATS_PREFIX, next_prefix(STATS_PREFIX)):
        parts = kv.key[len(STATS_PREFIX):].decode('utf-8', errors='replace').split('/')
        if len(parts) < 3:
            continue
        pool_id, uuid, date = parts[0], parts[1], parts[2]
        entry = stats[pool_id]
        group = f'{pool_id}/{uuid}'
        if group not in entry['groups']:
            entry['groups'].append(group)
        entry['keys'] += 1
        entry['bytes'] += len(kv.value)
        if cutoff and date < cutoff:
            entry['old_keys'] += 1
            entry['old_bytes'] += len(kv.value)
        if not entry['oldest'] or date < entry['oldest_raw']:
            entry['oldest_raw'], entry['oldest'] = date, format_date(date)
        if not entry['newest'] or date > entry['newest_raw']:
            entry['newest_raw'], entry['newest'] = date, format_date(date)

    rows = []
    for pool_id, entry in stats.items():
        entry.pop('oldest_raw', None)
        entry.pop('newest_raw', None)
        # On a Database (not a Transaction) the binding resolves the read and
        # returns the raw value, or None when the key is absent.
        status = 'exists' if pool_id and db[pool_key(pool_id)] is not None else 'DELETED'
        rows.append({'pool_id': pool_id, **entry, 'status': status})
    rows.sort(key=lambda r: (r['status'] != 'DELETED', -r['keys']))

    deleted = [r for r in rows if r['status'] == 'DELETED']
    total = {
        'pools': len(rows),
        'deleted_pools': len(deleted),
        'stat_keys': sum(r['keys'] for r in rows),
        'orphan_stat_keys': sum(r['keys'] for r in deleted),
        'orphan_bytes': sum(r['bytes'] for r in deleted),
    }
    if cutoff:
        total['cutoff'] = format_date(cutoff)
        total['old_stat_keys'] = sum(r['old_keys'] for r in rows)
        total['old_bytes'] = sum(r['old_bytes'] for r in rows)
    to_delete = [r for r in rows if r['old_keys']] if cutoff else deleted
    if args.deleted_only:
        rows = deleted

    if args.json:
        print(json.dumps({'pools': rows, 'total': total}, indent=2))
    else:
        print(f"{'POOL_ID':<36}  {'KEYS':>10}  {'BYTES':>12}  {'OLD':>10}  "
              f"{'OLDEST':<19}  {'NEWEST':<19}  STATUS")
        for r in rows:
            old = r['old_keys'] if cutoff else '-'
            print(f"{r['pool_id'] or '<none>':<36}  {r['keys']:>10}  {r['bytes']:>12}  {old:>10}  "
                  f"{r['oldest']:<19}  {r['newest']:<19}  {r['status']}")
        print()
        print(f"Pools with stats:      {total['pools']}")
        print(f"  of which deleted:    {total['deleted_pools']}")
        print(f"Stat keys total:       {total['stat_keys']}")
        print(f"  orphaned (deleted):  {total['orphan_stat_keys']} ({total['orphan_bytes']} bytes)")
        if cutoff:
            print(f"  older than {total['cutoff']}:  {total['old_stat_keys']} ({total['old_bytes']} bytes)")

    if args.delete:
        delete_stats(db, to_delete, cutoff, args.yes)


def delete_stats(db, to_delete: list[dict], cutoff: str, assume_yes: bool) -> None:
    if not to_delete:
        print("Nothing to delete.", file=sys.stderr)
        return
    if cutoff:
        count = sum(r['old_keys'] for r in to_delete)
        question = f"Delete {count} stat keys older than {format_date(cutoff)} of {len(to_delete)} pools?"
    else:
        count = sum(r['keys'] for r in to_delete)
        question = f"Delete {count} stat keys of {len(to_delete)} deleted pools?"
    if not assume_yes:
        print(f"\n{question} [y/N] ", end='', file=sys.stderr, flush=True)
        if input().strip().lower() not in ('y', 'yes'):
            print("Aborted.", file=sys.stderr)
            return

    if cutoff:
        clear_old = fdb.transactional(clear_old_stats)
        for r in to_delete:
            clear_old(db, r['groups'], cutoff)
        print(f"Cleared {count} stat keys older than {format_date(cutoff)} of {len(to_delete)} pools.",
              file=sys.stderr)
        return

    clear_orphan = fdb.transactional(clear_orphan_stats)
    cleared = skipped = 0
    for r in to_delete:
        if clear_orphan(db, r['pool_id'], r['groups']):
            cleared += 1
        else:
            skipped += 1
            print(f"Skipped {r['pool_id']}: pool exists now", file=sys.stderr)
    print(f"Cleared stats of {cleared} pools, skipped {skipped}.", file=sys.stderr)


if __name__ == '__main__':
    main()
