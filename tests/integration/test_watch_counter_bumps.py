# coding=utf-8
"""Every entity write path maintains the version index — real FDB."""

from uuid import uuid4

from simplyblock_core import watches
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.stats import LVolStatObject
from simplyblock_core.models.storage_node import StorageNode


def _rollup(db, model_cls, scope):
    raw = db.kv_store.get(watches.watch_index_rollup_key(model_cls, scope))
    return watches.unpack_counter(bytes(raw)) if raw is not None else 0


def _version_present(db, model_cls, scope, leaf):
    return db.kv_store.get(watches.watch_index_version_key(model_cls, scope, leaf)) is not None


def _make_pool(cluster_id=None):
    return Pool({
        'uuid': str(uuid4()),
        'cluster_id': cluster_id or str(uuid4()),
        'pool_name': 'watch-test-pool',
        'status': Pool.STATUS_ACTIVE,
    })


def test_write_to_db_bumps_rollup_and_version():
    db = DBController()
    pool = _make_pool()
    scope = pool.watch_scope()
    before = _rollup(db, Pool, scope)
    pool.write_to_db(db.kv_store)
    assert _rollup(db, Pool, scope) == before + 1
    assert _version_present(db, Pool, scope, pool.get_id())
    assert db.get_pool_by_id(pool.get_id()).get_id() == pool.get_id()


def test_remove_bumps_rollup_and_clears_version():
    db = DBController()
    pool = _make_pool()
    pool.write_to_db(db.kv_store)
    scope = pool.watch_scope()
    before = _rollup(db, Pool, scope)
    pool.remove(db.kv_store)
    assert _rollup(db, Pool, scope) == before + 1
    assert not _version_present(db, Pool, scope, pool.get_id())


def test_atomic_update_bumps_index():
    db = DBController()
    node = StorageNode({
        'uuid': str(uuid4()),
        'cluster_id': str(uuid4()),
        'status': StorageNode.STATUS_ONLINE,
    })
    node.write_to_db(db.kv_store)
    scope = node.watch_scope()
    before = _rollup(db, StorageNode, scope)

    def mutate(fresh):
        fresh.status = StorageNode.STATUS_SUSPENDED

    updated = db.atomic_update(node, mutate)
    assert updated.status == StorageNode.STATUS_SUSPENDED
    assert _rollup(db, StorageNode, scope) == before + 1
    assert _version_present(db, StorageNode, scope, node.get_id())


def test_try_set_node_restarting_bumps_index(monkeypatch):
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
    scope = node.watch_scope()
    before = _rollup(db, StorageNode, scope)

    acquired, reason = db.try_set_node_restarting(cluster_id, node.get_id())
    assert acquired, reason
    assert _rollup(db, StorageNode, scope) == before + 1


def test_index_keys_invisible_to_entity_reads():
    db = DBController()
    pool = _make_pool()
    pool.write_to_db(db.kv_store)
    # An index key inside the entity prefix would blow up the JSON parse in
    # read_from_db; parsing the whole range cleanly is the collision guard.
    pools = Pool().read_from_db(db.kv_store)
    assert pool.get_id() in {p.get_id() for p in pools}
    entity_keys = [bytes(k) for k, _ in db.kv_store.get_range_startswith(b'object/Pool/')]
    assert all(b'watch_index' not in key for key in entity_keys)


def test_unwatched_write_creates_no_index():
    db = DBController()
    scope = ()  # LVolStatObject is unwatched; default scope
    before = _rollup(db, LVolStatObject, scope)
    stat = LVolStatObject({
        'uuid': str(uuid4()),
        'cluster_id': str(uuid4()),
        'pool_id': str(uuid4()),
        'date': 1,
    })
    stat.write_to_db(db.kv_store)
    assert _rollup(db, LVolStatObject, scope) == before
