"""A pool's capacity limit counts the pool's volumes and snapshots as they are now,
not as a TTL-cached scan last saw them. Seeded records are read through a live DBController."""

from unittest.mock import patch

import pytest

from simplyblock_core.controllers import lvol_controller, pool_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol, LVolMini
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.snapshot import SnapShot

GIB = 1024 ** 3
CLUSTER_ID = "11111111-1111-4111-8111-111111111111"
POOL_ID = "22222222-2222-4222-8222-222222222222"
OTHER_POOL_ID = "33333333-3333-4333-8333-333333333333"


@pytest.fixture
def db():
    db = DBController()
    if db.kv_store is None:
        pytest.skip("FoundationDB is not available")
    cluster = Cluster()
    cluster.uuid = CLUSTER_ID
    cluster.status = Cluster.STATUS_ACTIVE
    cluster.write_to_db(db.kv_store)
    return db


def _pool(db, uuid=POOL_ID, max_size=5 * GIB):
    pool = Pool()
    pool.uuid, pool.cluster_id, pool.pool_name = uuid, CLUSTER_ID, f"pool-{uuid[:4]}"
    pool.status, pool.pool_max_size = Pool.STATUS_ACTIVE, max_size
    pool.write_to_db(db.kv_store)
    return pool


def _lvol(db, n, pool_id, size, status=LVol.STATUS_ONLINE):
    lvol = LVol()
    lvol.uuid, lvol.pool_uuid, lvol.lvol_name = f"aaaaaaaa-0000-4000-8000-{n:012d}", pool_id, f"vol-{n}"
    lvol.size, lvol.status = size, status
    lvol.write_to_db(db.kv_store)
    return lvol


def _snapshot(db, n, pool_id, source, used_size):
    snap = SnapShot()
    snap.uuid, snap.cluster_id, snap.pool_uuid = f"cccccccc-0000-4000-8000-{n:012d}", CLUSTER_ID, pool_id
    snap.snap_name, snap.lvol, snap.status = f"snap-{n}", source, SnapShot.STATUS_ONLINE
    snap.used_size = used_size
    snap.write_to_db(db.kv_store)
    return snap


def test_the_total_counts_only_the_pools_own_live_volumes_and_snapshots(db):
    _pool(db)
    _pool(db, OTHER_POOL_ID)
    mine = _lvol(db, 1, POOL_ID, 2 * GIB)
    _lvol(db, 2, POOL_ID, GIB)
    _lvol(db, 3, POOL_ID, 8 * GIB, status=LVol.STATUS_DELETED)
    theirs = _lvol(db, 4, OTHER_POOL_ID, 4 * GIB)
    _snapshot(db, 1, POOL_ID, mine, GIB // 2)
    _snapshot(db, 2, OTHER_POOL_ID, theirs, 3 * GIB)

    assert pool_controller.get_pool_total_capacity(POOL_ID) == 3 * GIB + GIB // 2


# (pool limit, volume GiB, snapshot GiB, requested GiB, admitted)
@pytest.mark.parametrize("limit, vol, snap, request_, admitted", [
    (5, 2, 0, 2, True),
    (5, 4, 0, 2, False),
    (5, 2, 2, 1, True),
    (5, 2, 2, 2, False),
    (0, 40, 0, 2, True),
])
def test_admission_against_the_pool_limit(db, limit, vol, snap, request_, admitted):
    pool = _pool(db, max_size=limit * GIB)
    source = _lvol(db, 1, POOL_ID, vol * GIB)
    if snap:
        _snapshot(db, 1, POOL_ID, source, snap * GIB)

    ok, error = lvol_controller.validate_add_lvol_func("new-volume", request_ * GIB, None, pool.get_id(), 0, 0, 0, 0)

    assert ok is admitted, error
    if not admitted:
        assert "Pool max size has reached" in error


# Regression: three 2 GiB volumes created seconds apart were all admitted into a 5 GB pool,
# because each create measured the pool from a cached scan that lacked the previous volume.
def test_a_create_is_refused_when_the_cached_scan_predates_the_volume(db):
    _pool(db)
    _lvol(db, 1, POOL_ID, 4 * GIB)
    other = LVolMini()  # a cached scan taken before that volume existed holds only another pool's
    other.uuid = other.lvol_uuid = "bbbbbbbb-0000-4000-8000-000000000001"
    other.pool_uuid, other.size = OTHER_POOL_ID, GIB

    with patch("simplyblock_core.utils.ttl_cache.cached_mini_lvols", return_value=[other]):
        ok, error = lvol_controller.add_lvol_ha("new-volume", 2 * GIB, None, "default", POOL_ID)

    assert not ok
    assert "Pool max size has reached" in error
