# coding=utf-8
"""Every entity write path bumps the per-class change counter — real FDB."""

from uuid import uuid4

from simplyblock_core import watches
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.stats import LVolStatObject
from simplyblock_core.models.storage_node import StorageNode


def _counter(db, model_cls):
    raw = db.kv_store.get(watches.watch_counter_key(model_cls))
    return watches.unpack_counter(bytes(raw)) if raw is not None else 0


def _make_pool(cluster_id=None):
    return Pool({
        'uuid': str(uuid4()),
        'cluster_id': cluster_id or str(uuid4()),
        'pool_name': 'watch-test-pool',
        'status': Pool.STATUS_ACTIVE,
    })


def test_write_to_db_bumps_counter_by_one():
    db = DBController()
    pool = _make_pool()
    before = _counter(db, Pool)
    pool.write_to_db(db.kv_store)
    assert _counter(db, Pool) == before + 1
    assert db.get_pool_by_id(pool.get_id()).get_id() == pool.get_id()


def test_remove_bumps_counter():
    db = DBController()
    pool = _make_pool()
    pool.write_to_db(db.kv_store)
    before = _counter(db, Pool)
    pool.remove(db.kv_store)
    assert _counter(db, Pool) == before + 1


def test_atomic_update_bumps_counter():
    db = DBController()
    node = StorageNode({
        'uuid': str(uuid4()),
        'cluster_id': str(uuid4()),
        'status': StorageNode.STATUS_ONLINE,
    })
    node.write_to_db(db.kv_store)
    before = _counter(db, StorageNode)

    def mutate(fresh):
        fresh.status = StorageNode.STATUS_SUSPENDED

    updated = db.atomic_update(node, mutate)
    assert updated.status == StorageNode.STATUS_SUSPENDED
    assert _counter(db, StorageNode) == before + 1


def test_try_set_node_restarting_bumps_counter(monkeypatch):
    from simplyblock_core import distr_controller
    monkeypatch.setattr(distr_controller, 'send_node_status_event', lambda *a, **kw: None)

    db = DBController()
    cluster_id = str(uuid4())
    node = StorageNode({
        'uuid': str(uuid4()),
        'cluster_id': cluster_id,
        'status': StorageNode.STATUS_ONLINE,
    })
    node.write_to_db(db.kv_store)
    before = _counter(db, StorageNode)

    acquired, reason = db.try_set_node_restarting(cluster_id, node.get_id())
    assert acquired, reason
    assert _counter(db, StorageNode) == before + 1


def test_counter_keys_invisible_to_entity_reads():
    db = DBController()
    pool = _make_pool()
    pool.write_to_db(db.kv_store)
    # A counter key inside the entity prefix would blow up the JSON parse in
    # read_from_db; parsing the whole range cleanly is the collision guard.
    pools = Pool().read_from_db(db.kv_store)
    assert pool.get_id() in {p.get_id() for p in pools}
    entity_keys = [bytes(k) for k, _ in db.kv_store.get_range_startswith(b'object/Pool/')]
    assert all(b'watch_seq' not in key for key in entity_keys)


def test_unwatched_write_creates_no_counter():
    db = DBController()
    before = _counter(db, LVolStatObject)
    stat = LVolStatObject({
        'uuid': str(uuid4()),
        'cluster_id': str(uuid4()),
        'pool_id': str(uuid4()),
        'date': 1,
    })
    stat.write_to_db(db.kv_store)
    assert _counter(db, LVolStatObject) == before
