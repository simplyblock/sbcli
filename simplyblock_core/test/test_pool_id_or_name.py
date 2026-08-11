"""Regression tests for "pool ID or name" resolution.

Several surfaces document `--target-pool` / pool arguments as "ID or name" but
resolved by ID only, so a valid pool name failed with a raw KeyError
(`cluster add-replication` in a two-cluster replication setup). The idiom had
been copied per call site and the copies drifted; it now lives in
DBController.get_pool_by_id_or_name().
"""
import pytest

from simplyblock_core import cluster_ops
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.pool import Pool

POOL_UUID = "0c67b956-79e3-4355-9480-b35a5b4b4727"
POOL_NAME = "pool_tgt"


class _PoolLookups:
    """Just enough of DBController for the unbound resolver under test."""

    def __init__(self, pool):
        self._pool = pool
        self.by_id_calls = []
        self.by_name_calls = []

    def get_pool_by_id(self, id):
        self.by_id_calls.append(id)
        if id != self._pool.get_id():
            raise KeyError(f'Pool {id} not found')
        return self._pool

    def get_pool_by_name(self, name):
        self.by_name_calls.append(name)
        if name != self._pool.pool_name:
            raise KeyError(f'Pool {name} not found')
        return self._pool


def _pool(status=Pool.STATUS_ACTIVE):
    p = Pool()
    p.uuid = POOL_UUID
    p.pool_name = POOL_NAME
    p.status = status
    return p


def _resolve(lookups, value):
    return DBController.get_pool_by_id_or_name(lookups, value)


def test_resolves_uuid_by_id():
    lookups = _PoolLookups(_pool())
    assert _resolve(lookups, POOL_UUID).get_id() == POOL_UUID
    assert lookups.by_name_calls == []


def test_resolves_name_by_name():
    lookups = _PoolLookups(_pool())
    assert _resolve(lookups, POOL_NAME).get_id() == POOL_UUID
    assert lookups.by_id_calls == []


def test_unknown_name_raises_keyerror():
    lookups = _PoolLookups(_pool())
    with pytest.raises(KeyError):
        _resolve(lookups, "no_such_pool")


class _FakeDB(_PoolLookups):
    """Fake with realistic by-id/by-name lookups.

    get_pool_by_id_or_name is the real implementation, and get_pool_by_id
    raises for a name exactly as the FDB-backed one does — so if add_replication
    ever regresses to an ID-only lookup, the name test fails instead of silently
    passing on a permissive stub.
    """

    get_pool_by_id_or_name = DBController.get_pool_by_id_or_name

    def __init__(self, pool=None, clusters=("CL_src", "CL_tgt")):
        super().__init__(pool)
        self._clusters = clusters
        self.written = {}

    def get_pool_by_id(self, id):
        if self._pool is None:
            raise KeyError(f'Pool {id} not found')
        return super().get_pool_by_id(id)

    def get_pool_by_name(self, name):
        if self._pool is None:
            raise KeyError(f'Pool {name} not found')
        return super().get_pool_by_name(name)

    def get_cluster_by_id(self, cluster_id):
        if cluster_id not in self._clusters:
            raise KeyError(f'Cluster {cluster_id} not found')
        return object()

    def atomic_update(self, obj, mutate):
        class _Captured:
            snapshot_replication_target_cluster = None
            snapshot_replication_target_pool = None
            snapshot_replication_timeout = None
        captured = _Captured()
        mutate(captured)
        self.written = {
            "cluster": captured.snapshot_replication_target_cluster,
            "pool": captured.snapshot_replication_target_pool,
            "timeout": captured.snapshot_replication_timeout,
        }
        return True


@pytest.fixture
def fake_db(monkeypatch):
    holder = {}

    def _install(db):
        holder["db"] = db
        monkeypatch.setattr(cluster_ops, "DBController", lambda *a, **kw: db)
        return db

    return _install


def test_add_replication_accepts_pool_name(fake_db):
    """The regression: a pool NAME used to raise KeyError: 'Pool pool_tgt not found'."""
    db = fake_db(_FakeDB(pool=_pool()))
    assert cluster_ops.add_replication("CL_src", "CL_tgt", 3600, POOL_NAME) is True
    # The stored reference must be the immutable UUID, not the name given.
    assert db.written["pool"] == POOL_UUID
    assert db.written["cluster"] == "CL_tgt"
    assert db.written["timeout"] == 3600


def test_add_replication_accepts_pool_uuid(fake_db):
    db = fake_db(_FakeDB(pool=_pool()))
    assert cluster_ops.add_replication("CL_src", "CL_tgt", 3600, POOL_UUID) is True
    assert db.written["pool"] == POOL_UUID


def test_add_replication_missing_pool_raises_valueerror(fake_db):
    """Was a bare KeyError traceback: the `if not pool` guard was dead code."""
    fake_db(_FakeDB(pool=None))
    with pytest.raises(ValueError, match="Pool not found"):
        cluster_ops.add_replication("CL_src", "CL_tgt", 3600, "ghost_pool")


def test_add_replication_inactive_pool_raises_valueerror(fake_db):
    fake_db(_FakeDB(pool=_pool(status=Pool.STATUS_INACTIVE)))
    with pytest.raises(ValueError, match="Pool not active"):
        cluster_ops.add_replication("CL_src", "CL_tgt", 3600, POOL_NAME)


def test_add_replication_missing_cluster_raises_valueerror(fake_db):
    fake_db(_FakeDB(pool=_pool()))
    with pytest.raises(ValueError, match="Cluster not found"):
        cluster_ops.add_replication("CL_ghost", "CL_tgt", 3600, POOL_NAME)


def test_add_replication_missing_target_cluster_raises_valueerror(fake_db):
    fake_db(_FakeDB(pool=_pool()))
    with pytest.raises(ValueError, match="Target cluster not found"):
        cluster_ops.add_replication("CL_src", "CL_ghost", 3600, POOL_NAME)
