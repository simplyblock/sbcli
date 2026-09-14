"""Regression test for the API v1 ``/cluster/metrics`` leak fix (48c0e2f6ee).

That commit rebuilt the metric Gauges on every scrape instead of caching
them at module scope forever -- fixing the original leak (deleted entities'
label series stuck around) -- but it registered the fresh Gauges against a
still-module-level, never-reset ``CollectorRegistry``. Two failure modes
followed:

1. Any scrape with more than one entity of a kind (two pools, two lvols, ...)
   called ``get_pool_metrics()``/``get_lvol_metrics()`` once per entity,
   registering the same Gauge name against the same registry twice and
   raising ``prometheus_client.registry.DuplicateTimeseries`` mid-request.
2. Even with only one entity per kind, the second scrape hit the same
   duplicate-registration error, since the registry was never reset between
   requests.

The real fix scopes both the ``CollectorRegistry`` and each Gauge dict to a
single call of ``get_data()``, built once per request and reused across that
request's entities. This test seeds two pools and two lvols (to catch
failure mode 1), scrapes twice (to catch failure mode 2), and asserts a
deleted pool/lvol's series is gone by the second scrape (the original leak).
"""
import time

import pytest

from simplyblock_core import db_controller
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.stats import ClusterStatObject, LVolStatObject, PoolStatObject
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_web.api.v1 import metrics


@pytest.fixture
def db():
    d = db_controller.DBController()
    if d.kv_store is None:
        pytest.skip("FoundationDB is not available")
    return d


def _write_pool(db, cluster, name):
    pool = Pool()
    pool.uuid = name
    pool.cluster_id = cluster.get_id()
    pool.pool_name = name
    pool.status = Pool.STATUS_ACTIVE
    pool.write_to_db(db.kv_store)

    stat = PoolStatObject(data={
        "pool_id": pool.get_id(), "uuid": pool.get_id(), "date": int(time.time()),
    })
    stat.write_to_db(db.kv_store)
    return pool


def _write_lvol(db, pool, node, name):
    lvol = LVol()
    lvol.uuid = name
    lvol.pool_uuid = pool.get_id()
    lvol.pool_name = pool.pool_name
    lvol.lvol_name = name
    lvol.pvc_name = ""
    lvol.node_id = node.get_id()
    lvol.status = LVol.STATUS_ONLINE
    lvol.write_to_db(db.kv_store)

    stat = LVolStatObject(data={
        "pool_id": lvol.pool_uuid, "uuid": lvol.get_id(), "date": int(time.time()),
    })
    stat.write_to_db(db.kv_store)
    return lvol


def test_metrics_survive_multiple_entities_and_repeated_scrapes(db):
    cluster = Cluster()
    cluster.uuid = "cluster-metrics-test"
    cluster.cluster_name = "metrics-test-cluster"
    cluster.status = Cluster.STATUS_ACTIVE
    cluster.write_to_db(db.kv_store)

    ClusterStatObject(data={
        "cluster_id": cluster.get_id(), "uuid": cluster.get_id(), "date": int(time.time()),
    }).write_to_db(db.kv_store)

    # Non-ONLINE so get_data()'s node loop `continue`s before it ever calls
    # rpc_client() -- this test only needs the node to exist so
    # db.get_lvols(cluster_id) resolves lvols by node_id.
    node = StorageNode()
    node.uuid = "node-metrics-test"
    node.cluster_id = cluster.get_id()
    node.hostname = "metrics-test-node"
    node.status = StorageNode.STATUS_OFFLINE
    node.write_to_db(db.kv_store)

    pool_a = _write_pool(db, cluster, "pool-a")
    pool_b = _write_pool(db, cluster, "pool-b")
    _write_lvol(db, pool_a, node, "lvol-a")
    lvol_b = _write_lvol(db, pool_b, node, "lvol-b")

    # First scrape: two pools and two lvols each call get_pool_metrics() /
    # get_lvol_metrics() -- this is where the buggy fix raised
    # DuplicateTimeseries on the second entity of a kind.
    resp = metrics.get_data()
    body = resp.get_data(as_text=True)
    assert 'pool="pool-a"' in body
    assert 'pool="pool-b"' in body
    assert 'lvol="lvol-a"' in body
    assert 'lvol="lvol-b"' in body

    # Delete one pool and one lvol, mirroring the scenario the original fix
    # targeted: a deleted entity's series must not persist.
    pool_b.remove(db.kv_store)
    lvol_b.remove(db.kv_store)

    # Second scrape: the buggy fix raised DuplicateTimeseries here even with
    # a single surviving entity per kind, since the registry was never reset
    # between requests.
    resp2 = metrics.get_data()
    body2 = resp2.get_data(as_text=True)
    assert 'pool="pool-a"' in body2
    assert 'lvol="lvol-a"' in body2
    # The deleted pool/lvol's series must not leak into the next scrape.
    assert 'pool="pool-b"' not in body2
    assert 'lvol="lvol-b"' not in body2
