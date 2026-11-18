"""The stats collector does not write a sample for an lvol or a pool that was
deleted during its pass.

The collector reads its lvol list once per cluster and then spends seconds on
RPCs per volume. A volume deleted in between had its stats cleared with its
record, and the collector's plain write then put one record back that nothing
ever removed: the retention worker only walks volumes that still exist.
"""
import json
from unittest.mock import MagicMock

import pytest

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.stats import LVolStatObject
from simplyblock_core.services import lvol_stat_collector as collector


class _FakeStore:
    """A dict with the few kv-store calls the collector's write path uses. It
    has no ``create_transaction``, so DBController takes its non-atomic path."""

    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value):
        self.data[key] = value

    def clear(self, key):
        self.data.pop(key, None)

    def get_range_startswith(self, prefix, limit=0, reverse=False):
        keys = sorted((k for k in self.data if k.startswith(prefix)), reverse=reverse)
        if limit:
            keys = keys[:limit]
        return [(k, self.data[k]) for k in keys]

    def stats_keys(self):
        return [k for k in self.data if b"StatObject/" in k]


@pytest.fixture()
def store(monkeypatch):
    store = _FakeStore()
    db = DBController()
    monkeypatch.setattr(db, "kv_store", store, raising=False)
    monkeypatch.setattr(collector, "db", db)
    monkeypatch.setattr(collector, "last_object_record", {})
    return store


def _lvol():
    lvol = LVol()
    lvol.uuid = "lvol-uuid"
    lvol.pool_uuid = "pool-uuid"
    return lvol


def _pool():
    pool = Pool()
    pool.uuid = "pool-uuid"
    return pool


def _put(store, obj):
    store.set(obj.get_db_id().encode(),
              json.dumps(obj.to_dict(unwrap_secrets=True)).encode())


def test_sample_of_an_existing_lvol_is_written(store):
    lvol = _lvol()
    _put(store, lvol)

    record = collector.add_lvol_stats(MagicMock(), lvol, [])

    assert record is not None
    assert len(store.stats_keys()) == 1
    assert collector.last_object_record[lvol.get_id()] is record


def test_sample_of_a_deleted_lvol_is_dropped(store):
    lvol = _lvol()
    collector.last_object_record[lvol.get_id()] = LVolStatObject()

    record = collector.add_lvol_stats(MagicMock(), lvol, [])

    assert record is None
    assert store.stats_keys() == []
    assert lvol.get_id() not in collector.last_object_record


def test_sample_of_a_deleted_pool_is_dropped(store):
    pool = _pool()
    sample = LVolStatObject(data={"uuid": "lvol-uuid", "pool_id": pool.get_id(), "date": 1})

    assert collector.add_pool_stats(pool, [sample]) is False
    assert store.stats_keys() == []

    _put(store, pool)
    assert collector.add_pool_stats(pool, [sample])
    assert len(store.stats_keys()) == 1
