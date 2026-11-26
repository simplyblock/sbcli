"""`volume qos-set` with a subset of the four limits (SFAM-2823).

The CLI options have no argparse default, so every limit the operator did not
pass reaches set_lvol() as None. None means "keep the current value": it must
neither crash the negative-value check nor overwrite the stored limit.
Seeded records are read back through a live DBController; the node is mocked.
"""

from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core.controllers import lvol_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.storage_node import StorageNode

CLUSTER_ID = "11111111-1111-4111-8111-111111111111"
POOL_ID = "22222222-2222-4222-8222-222222222222"
NODE_ID = "33333333-3333-4333-8333-333333333333"
LVOL_ID = "44444444-4444-4444-8444-444444444444"
STORED = (100, 200, 300, 400)


@pytest.fixture
def db():
    db = DBController()
    if db.kv_store is None:
        pytest.skip("FoundationDB is not available")
    cluster = Cluster()
    cluster.uuid, cluster.status = CLUSTER_ID, Cluster.STATUS_ACTIVE
    cluster.write_to_db(db.kv_store)

    pool = Pool()
    pool.uuid, pool.cluster_id, pool.pool_name = POOL_ID, CLUSTER_ID, "pool-1"
    pool.status = Pool.STATUS_ACTIVE
    pool.write_to_db(db.kv_store)

    node = StorageNode()
    node.uuid, node.cluster_id, node.status = NODE_ID, CLUSTER_ID, StorageNode.STATUS_ONLINE
    node.write_to_db(db.kv_store)

    lvol = LVol()
    lvol.uuid, lvol.pool_uuid, lvol.node_id, lvol.lvol_name = LVOL_ID, POOL_ID, NODE_ID, "vol-1"
    lvol.top_bdev, lvol.status = "LVS_1/LVOL_1", LVol.STATUS_ONLINE
    (lvol.rw_ios_per_sec, lvol.rw_mbytes_per_sec,
     lvol.r_mbytes_per_sec, lvol.w_mbytes_per_sec) = STORED
    lvol.write_to_db(db.kv_store)
    return db


@pytest.fixture
def rpc():
    rpc = MagicMock(name="rpc")
    rpc.bdev_set_qos_limit.return_value = True
    with patch.object(StorageNode, "rpc_client", return_value=rpc):
        yield rpc


def _stored(db):
    lvol = db.get_lvol_by_id(LVOL_ID)
    return (lvol.rw_ios_per_sec, lvol.rw_mbytes_per_sec,
            lvol.r_mbytes_per_sec, lvol.w_mbytes_per_sec)


@pytest.mark.parametrize("position", range(4))
def test_one_limit_is_applied_and_the_others_kept(db, rpc, position):
    limits = [None] * 4
    limits[position] = 7
    expected = list(STORED)
    expected[position] = 7

    assert lvol_controller.set_lvol(LVOL_ID, *limits) is True

    rpc.bdev_set_qos_limit.assert_called_once_with("LVS_1/LVOL_1", *expected)
    assert _stored(db) == tuple(expected)


def test_a_negative_limit_is_still_rejected(db, rpc):
    assert lvol_controller.set_lvol(LVOL_ID, None, -1, None, None) is False

    rpc.bdev_set_qos_limit.assert_not_called()
    assert _stored(db) == STORED
