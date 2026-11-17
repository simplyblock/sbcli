#!/usr/bin/env python3
"""Count the unique objects stored in the simplyblock FDB keyspace.

Every model record is written under ``<object_type>/<name>/<id...>`` (see
``BaseModel.get_db_id``). This script scans the whole user keyspace (system
keys under ``\\xff`` are skipped), groups keys by their ``object_type/name``
prefix and reports, per group:

  * keys  - number of keys under the prefix
  * uuids - number of distinct ``uuid`` values found in the JSON records
            (the same object may be stored under more than one key)
  * bytes - total size of the values

The scan is paginated: each chunk is its own transaction, so it never hits
FDB's 5s transaction limit on large databases.

Usage:
    python scripts/count_fdb_objects.py [--cluster-file PATH] [--prefix PREFIX] [--json]
"""
import argparse
import json
from collections import defaultdict

import fdb

from simplyblock_core import constants

CHUNK_SIZE = 5000
SYSTEM_KEYS = b'\xff'


def group_of(key: bytes) -> str:
    parts = key.decode('utf-8', errors='replace').split('/')
    return '/'.join(parts[:2]) if len(parts) > 1 else '<no-prefix>'


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


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--cluster-file', default=constants.KVD_DB_FILE_PATH)
    parser.add_argument('--prefix', default='', help='only scan keys starting with this prefix (e.g. "object/LVol")')
    parser.add_argument('--json', action='store_true', help='print the result as JSON')
    args = parser.parse_args()

    fdb.api_version(constants.KVD_DB_VERSION)
    db = fdb.open(args.cluster_file)

    begin = args.prefix.encode('utf-8')
    end = next_prefix(begin) if begin else SYSTEM_KEYS

    keys: dict[str, int] = defaultdict(int)
    size: dict[str, int] = defaultdict(int)
    uuids: dict[str, set] = defaultdict(set)

    for kv in scan(db, begin, end):
        group = group_of(kv.key)
        keys[group] += 1
        size[group] += len(kv.value)
        try:
            record = json.loads(kv.value)
        except (ValueError, UnicodeDecodeError):
            continue
        if isinstance(record, dict) and record.get('uuid'):
            uuids[group].add(record['uuid'])

    rows = [
        {'group': g, 'keys': keys[g], 'uuids': len(uuids[g]), 'bytes': size[g]}
        for g in sorted(keys, key=lambda g: keys[g], reverse=True)
    ]
    total = {
        'keys': sum(keys.values()),
        'uuids': len(set().union(*uuids.values())) if uuids else 0,
        'bytes': sum(size.values()),
    }

    if args.json:
        print(json.dumps({'groups': rows, 'total': total}, indent=2))
        return

    width = max([len(r['group']) for r in rows] + [len('TOTAL')])
    print(f"{'GROUP':<{width}}  {'KEYS':>10}  {'UUIDS':>10}  {'BYTES':>14}")
    for r in rows:
        print(f"{r['group']:<{width}}  {r['keys']:>10}  {r['uuids']:>10}  {r['bytes']:>14}")
    print('-' * (width + 40))
    print(f"{'TOTAL':<{width}}  {total['keys']:>10}  {total['uuids']:>10}  {total['bytes']:>14}")


if __name__ == '__main__':
    main()
