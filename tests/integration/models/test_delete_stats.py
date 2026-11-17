"""Deleting an lvol or a pool clears its stats records.

Stats live under their own keys (``object/LVolStatObject/<pool>/<lvol>/<date>``,
``object/PoolStatObject/<pool>/<pool>/<date>``), so removing the entity's
record left them behind forever: nothing else ever reads or prunes the stats of
an id that no longer exists.
"""

import uuid

import pytest

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.stats import (
    CachedLVolStatObject,
    LVolStatObject,
    PoolStatObject,
)


@pytest.fixture()
def db():
    return DBController()


def _stat(cls, pool_id, entity_id, date):
    stat = cls()
    stat.pool_id = pool_id
    stat.uuid = entity_id
    stat.date = date
    return stat


def _lvol(pool_id, lvol_id=None):
    lvol = LVol()
    lvol.uuid = lvol_id or str(uuid.uuid4())
    lvol.pool_uuid = pool_id
    return lvol


def test_delete_lvol_stats_clears_only_that_lvol(db):
    pool_id = str(uuid.uuid4())
    lvol = _lvol(pool_id)
    # Shares the deleted lvol's id as a prefix: the clear must not reach it.
    neighbour = _lvol(pool_id, lvol.uuid + "x")
    for date in (1, 2, 3):
        _stat(LVolStatObject, pool_id, lvol.uuid, date).write_to_db(db.kv_store)
        _stat(LVolStatObject, pool_id, neighbour.uuid, date).write_to_db(db.kv_store)
    CachedLVolStatObject().from_dict(
        {"uuid": lvol.uuid, "pool_id": lvol.uuid, "date": 1}).write_to_db(db.kv_store)

    db.delete_lvol_stats(lvol)

    assert db.get_lvol_stats(lvol, limit=0) == []
    assert db.get_cached_lvol_stats(lvol.uuid, limit=0) == []
    assert len(db.get_lvol_stats(neighbour, limit=0)) == 3


def test_delete_pool_stats_clears_only_that_pool(db):
    pool, other = Pool(), Pool()
    pool.uuid, other.uuid = str(uuid.uuid4()), str(uuid.uuid4())
    for date in (1, 2):
        _stat(PoolStatObject, pool.uuid, pool.uuid, date).write_to_db(db.kv_store)
        _stat(PoolStatObject, other.uuid, other.uuid, date).write_to_db(db.kv_store)
    lvol = _lvol(pool.uuid)
    _stat(LVolStatObject, pool.uuid, lvol.uuid, 1).write_to_db(db.kv_store)

    db.delete_pool_stats(pool)

    assert db.get_pool_stats(pool, limit=0) == []
    assert len(db.get_pool_stats(other, limit=0)) == 2
    # The pool's lvols' stats are theirs to clear, on their own delete.
    assert len(db.get_lvol_stats(lvol, limit=0)) == 1
