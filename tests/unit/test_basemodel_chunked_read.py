# coding=utf-8
"""
test_basemodel_chunked_read.py — completeness contract for the chunked
read_from_db pagination (review of d6dfcd73).

The review concern: "_READ_CHUNK_SIZE = 2000 implicitly reads only the
first 2000 objects, so enumerations that rely on completeness (e.g.
free-slot accounting over all lvols for multi-namespace subsystems)
silently break." These tests pin the actual contract: the chunk size
bounds ONE range read, and read_from_db paginates until the prefix is
exhausted — the result is complete for any table size, forward and
reverse, with and without limit, including chunk-boundary edge cases.
"""

import json
import unittest

from simplyblock_core.models.base_model import BaseModel


class _FakeKV:
    """Byte-range store emulating fdb get_range semantics (begin inclusive,
    end exclusive, per-call limit, reverse) over a static sorted key space.
    Counts calls so tests can assert pagination actually happened."""

    class _KV:
        def __init__(self, key, value):
            self.key = key
            self.value = value

    def __init__(self, items):
        self._items = sorted(items)
        self.range_calls = 0

    def get_range(self, begin, end, limit=0, reverse=False):
        self.range_calls += 1
        rows = [(k, v) for k, v in self._items if begin <= k < end]
        if reverse:
            rows.reverse()
        if limit:
            rows = rows[:limit]
        return [self._KV(k, v) for k, v in rows]

    def get_range_startswith(self, prefix, limit=0, reverse=False):
        rows = [(k, v) for k, v in self._items if k.startswith(prefix)]
        if reverse:
            rows.reverse()
        if limit:
            rows = rows[:limit]
        return rows


class _Obj(BaseModel):
    """Minimal concrete model — inherits uuid/object_type annotations."""


def _store(count, chunk):
    _Obj._READ_CHUNK_SIZE = chunk
    prefix = _Obj().get_db_id("").strip().encode()
    items = [
        (prefix + b"%08d" % i, json.dumps({"uuid": "%08d" % i}).encode())
        for i in range(count)
    ]
    return _FakeKV(items)


class TestChunkedReadCompleteness(unittest.TestCase):

    def test_multiple_chunks_return_all_objects(self):
        # 2.5 chunks: completeness beyond the per-transaction chunk size.
        kv = _store(250, chunk=100)
        objs = _Obj().read_from_db(kv)
        self.assertEqual([o.uuid for o in objs], ["%08d" % i for i in range(250)])
        self.assertGreaterEqual(kv.range_calls, 3)  # pagination really ran

    def test_exact_chunk_boundary(self):
        kv = _store(200, chunk=100)
        self.assertEqual(len(_Obj().read_from_db(kv)), 200)

    def test_reverse_returns_all_in_reverse_order(self):
        kv = _store(250, chunk=100)
        objs = _Obj().read_from_db(kv, reverse=True)
        self.assertEqual([o.uuid for o in objs],
                         ["%08d" % i for i in reversed(range(250))])

    def test_limit_larger_than_chunk(self):
        kv = _store(250, chunk=100)
        objs = _Obj().read_from_db(kv, limit=150)
        self.assertEqual([o.uuid for o in objs], ["%08d" % i for i in range(150)])

    def test_small_limit_single_scan(self):
        kv = _store(250, chunk=100)
        objs = _Obj().read_from_db(kv, limit=10)
        self.assertEqual(len(objs), 10)
        self.assertEqual(kv.range_calls, 0)  # took the single-scan path

    def test_store_without_get_range_falls_back_complete(self):
        # Stores exposing only get_range_startswith (unit-tier fdb stub,
        # test fakes) take the legacy single-scan path — still complete.
        class _StartswithOnlyKV:
            def __init__(self, inner):
                self._inner = inner

            def get_range_startswith(self, prefix, limit=0, reverse=False):
                return self._inner.get_range_startswith(prefix, limit=limit, reverse=reverse)

        kv = _StartswithOnlyKV(_store(250, chunk=100))
        objs = _Obj().read_from_db(kv)
        self.assertEqual(len(objs), 250)

    def test_next_prefix(self):
        self.assertEqual(BaseModel._next_prefix(b"abc"), b"abd")
        self.assertEqual(BaseModel._next_prefix(b"ab\xff"), b"ac")
        with self.assertRaises(ValueError):
            BaseModel._next_prefix(b"\xff\xff")


if __name__ == "__main__":
    unittest.main()
