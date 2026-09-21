"""The lookups the indices exist for, and the reads they are allowed to cost."""
import pytest

from simplyblock_core import index_ops
from simplyblock_core.models import indices
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.storage_node import StorageNode

from .helpers import CLUSTER, make_lvol, ready


class _CountingStore:
    """The real store, with the reads counted.

    Not a stand-in for the database — every call is delegated to the live
    FoundationDB handle and its real answer is returned. It exists because
    nothing in the suite fails on an O(N) lookup: every scan this work removes
    was found by a production incident rather than by a test.
    """

    def __init__(self, inner):
        self._inner = inner
        self.point_reads = 0
        self.range_reads = 0

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def __setitem__(self, key, value):
        self._inner[key] = value

    def get(self, key):
        self.point_reads += 1
        return self._inner.get(key)

    def get_range(self, *args, **kwargs):
        self.range_reads += 1
        return self._inner.get_range(*args, **kwargs)

    def get_range_startswith(self, *args, **kwargs):
        self.range_reads += 1
        return self._inner.get_range_startswith(*args, **kwargs)


@pytest.fixture
def counting(db, monkeypatch):
    store = _CountingStore(db.kv_store)
    monkeypatch.setattr(db, 'kv_store', store)
    return store


def _cluster_pools(db, count, cluster=CLUSTER):
    for n in range(count):
        pool = Pool()
        pool.uuid = f"pool-{n}"
        pool.pool_name = f"pool-{n}"
        pool.cluster_id = cluster
        pool.write_to_db(db.kv_store)
    index_ops.build_indices([Pool])


def test_lvols_by_cluster_agree_across_the_flip(db):
    """A volume belongs to its pool's cluster, on both sides of the flip.

    Both paths derive membership from `pool_uuid`, so a volume in another
    cluster's pool is excluded either way — and its node, which moves for the
    length of every migration, does not enter into it.
    """
    _cluster_pools(db, 2)
    stray = Pool()
    stray.uuid = "pool-elsewhere"
    stray.pool_name = "pool-elsewhere"
    stray.cluster_id = "other-cluster"
    stray.write_to_db(db.kv_store)

    make_lvol("vol-a", pool="pool-0").write_to_db(db.kv_store)
    make_lvol("vol-b", pool="pool-1", node="node-elsewhere").write_to_db(db.kv_store)
    make_lvol("vol-c", pool="pool-elsewhere").write_to_db(db.kv_store)

    scanned = sorted(lv.lvol_name for lv in db.get_lvols(CLUSTER))
    ready(LVol)
    indexed = sorted(lv.lvol_name for lv in db.get_lvols(CLUSTER))

    assert scanned == indexed == ["vol-a", "vol-b"]


def test_cluster_of_a_volume_agrees_with_the_listing(db):
    """The per-volume accessor and the listing are one relation, two directions."""
    _cluster_pools(db, 1)
    lvol = make_lvol("vol-a", pool="pool-0")
    lvol.write_to_db(db.kv_store)
    ready(LVol)

    assert db.get_cluster_id_by_lvol(lvol) == CLUSTER
    assert [lv.get_id() for lv in db.get_lvols(CLUSTER)] == [lvol.get_id()]


def test_lvols_by_cluster_reads_one_range_per_pool(db, counting):
    _cluster_pools(db, 4)
    for n in range(4):
        make_lvol(f"vol-{n}", pool=f"pool-{n}").write_to_db(db.kv_store)
    ready(LVol)
    counting.range_reads = 0

    assert len(db.get_lvols(CLUSTER)) == 4

    # One for the pool index, one per pool's volumes — bounded by the pool
    # count, never by the volume count. A pass over the volume keyspace, which
    # is what this replaced, would add a sixth.
    assert counting.range_reads == 5


def test_lvols_by_cluster_scans_once_while_the_index_builds(db, counting):
    """The fallback must not degrade with the number of pools in the cluster.

    Asking `query` per pool would take its scan fallback once per pool; the
    state is consulted once so the unready path stays the single table scan it
    was before the index existed.
    """
    _cluster_pools(db, 6)
    for n in range(6):
        make_lvol(f"vol-{n}", pool=f"pool-{n}").write_to_db(db.kv_store)
    db.set_index_state(LVol, 'pool_uuid', indices.STATE_BUILDING)
    counting.range_reads = 0

    assert len(db.get_lvols(CLUSTER)) == 6

    # One for the pool index, one for the volume scan.
    assert counting.range_reads == 2


def test_task_lookup_by_uuid_does_not_scan_the_table(db, counting):
    for n in range(20):
        task = JobSchedule()
        task.uuid = f"task-{n}"
        task.cluster_id = CLUSTER
        task.date = n
        task.function_name = JobSchedule.FN_NODE_ADD
        task.status = JobSchedule.STATUS_NEW
        task.write_to_db(db.kv_store)
    index_ops.build_indices([JobSchedule])
    counting.range_reads = 0

    task = db.get_task_by_id("task-7")

    assert task.uuid == "task-7"
    # One range read for the index prefix; the entity itself is a point read.
    assert counting.range_reads == 1


def test_device_lookup_reads_one_node(db, counting):
    for n in range(10):
        node = StorageNode({
            'uuid': f"node-{n}",
            'cluster_id': CLUSTER,
            'nvme_devices': [{'uuid': f"dev-{n}"}],
        })
        node.write_to_db(db.kv_store)
    index_ops.build_indices([StorageNode])
    counting.range_reads = 0

    device = db.get_storage_device_by_id("dev-4")

    assert device.get_id() == "dev-4"
    assert counting.range_reads == 1


def test_node_lookups_by_cluster_agree_across_the_flip(db):
    for n in range(3):
        node = StorageNode()
        node.uuid = f"node-{n}"
        node.cluster_id = CLUSTER if n < 2 else "other-cluster"
        node.create_dt = f"2026-01-01 00:00:{n:02d}"
        node.write_to_db(db.kv_store)

    scanned = [n.get_id() for n in db.get_storage_nodes_by_cluster_id(CLUSTER)]
    index_ops.build_indices([StorageNode])
    indexed = [n.get_id() for n in db.get_storage_nodes_by_cluster_id(CLUSTER)]

    assert scanned == indexed == ["node-0", "node-1"]


def test_failover_index_finds_the_primaries_of_a_peer(db):
    primary = StorageNode()
    primary.uuid = "node-p"
    primary.cluster_id = CLUSTER
    primary.secondary_node_id = "node-s"
    primary.lvstore = "LVS_1"
    primary.write_to_db(db.kv_store)
    index_ops.build_indices([StorageNode])

    found = db.get_primary_storage_nodes_by_secondary_node_id("node-s")

    assert [n.get_id() for n in found] == ["node-p"]
