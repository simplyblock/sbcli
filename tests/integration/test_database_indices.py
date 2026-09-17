"""Declared secondary indices against a real FoundationDB.

The unit tier covers key derivation as a pure function of a record
(``tests/unit/models/test_indices.py``). What needs a real database is
everything the design turns on: that maintenance is in the *same* transaction
as the entity write, that the scan fallback and the index answer identically,
that the backfill is restartable, and that a unique violation leaves nothing
behind and does not get swallowed on the way out.
"""
import json
import uuid

import fdb
import pytest

from simplyblock_core import index_ops, indices
from simplyblock_core.controllers import lvol_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.indices import UniqueIndexViolation
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.storage_node import StorageNode

CLUSTER = "cluster-idx-1"
POOL = "pool-idx-1"


@pytest.fixture
def db():
    return DBController()


def _lvol(name, *, uuid_=None, pool=POOL, node="node-1", cluster=CLUSTER):
    lvol = LVol()
    lvol.uuid = uuid_ or str(uuid.uuid4())
    lvol.lvol_name = name
    lvol.pool_uuid = pool
    lvol.cluster_id = cluster
    lvol.node_id = node
    lvol.status = LVol.STATUS_ONLINE
    lvol.create_dt = f"2026-01-01 00:00:{len(name):02d}"
    return lvol


def _ready(model_cls):
    index_ops.build_indices([model_cls])


def _index_keys(db, model_cls):
    prefix = f'{indices.INDEX_PREFIX}{model_cls.__name__}/'.encode()
    return {bytes(k): bytes(v) for k, v in db.kv_store.get_range_startswith(prefix)}


# --- the round trip ---------------------------------------------------------

def test_write_then_query_returns_the_row(db):
    lvol = _lvol("vol-a")
    lvol.write_to_db(db.kv_store)
    _ready(LVol)

    found = db.query(LVol, 'pool_uuid', POOL)

    assert [v.get_id() for v in found] == [lvol.get_id()]


def test_update_moves_the_entry(db):
    lvol = _lvol("vol-a", node="node-1")
    lvol.write_to_db(db.kv_store)
    _ready(LVol)

    lvol.node_id = "node-2"
    lvol.write_to_db(db.kv_store)

    assert db.query(LVol, 'node_id', "node-1") == []
    assert [v.get_id() for v in db.query(LVol, 'node_id', "node-2")] == [lvol.get_id()]


def test_remove_clears_every_entry(db):
    lvol = _lvol("vol-a")
    lvol.write_to_db(db.kv_store)
    _ready(LVol)
    assert _index_keys(db, LVol)

    lvol.remove(db.kv_store)

    assert _index_keys(db, LVol) == {}
    assert db.query(LVol, 'pool_uuid', POOL) == []


def test_remove_uses_the_stored_record_not_the_callers_copy(db):
    """A caller holding a stale copy must not leave the live entry behind."""
    lvol = _lvol("vol-a", node="node-1")
    lvol.write_to_db(db.kv_store)
    _ready(LVol)

    fresh = db.get_lvol_by_id(lvol.get_id())
    fresh.node_id = "node-2"
    fresh.write_to_db(db.kv_store)

    lvol.remove(db.kv_store)  # the stale copy still says node-1

    assert _index_keys(db, LVol) == {}


def test_atomic_update_maintains_the_index(db):
    lvol = _lvol("vol-a", node="node-1")
    lvol.write_to_db(db.kv_store)
    _ready(LVol)

    db.atomic_update(lvol, lambda obj: setattr(obj, 'node_id', "node-2"))

    assert db.query(LVol, 'node_id', "node-1") == []
    assert [v.get_id() for v in db.query(LVol, 'node_id', "node-2")] == [lvol.get_id()]


def test_atomic_update_that_aborts_leaves_the_index_alone(db):
    lvol = _lvol("vol-a", node="node-1")
    lvol.write_to_db(db.kv_store)
    _ready(LVol)
    before = _index_keys(db, LVol)

    def _mutate(obj):
        obj.node_id = "node-2"
        return False  # guard no longer holds: abort the write

    db.atomic_update(lvol, _mutate)

    assert _index_keys(db, LVol) == before


# --- atomicity --------------------------------------------------------------

def test_a_transaction_that_fails_after_the_write_leaves_no_orphan(db):
    """The property the hand-rolled indices did not have: maintenance is in the
    entity's own transaction, so a crash between the two is not a state."""
    _ready(LVol)
    lvol = _lvol("vol-a")

    def _write_then_fail(tr, obj):
        obj.write_to_db(tr)
        raise RuntimeError("caller failed after the write")

    with pytest.raises(RuntimeError):
        fdb.transactional(_write_then_fail)(db.kv_store, lvol)

    assert db.kv_store.get(lvol.get_db_id().encode()) is None
    assert _index_keys(db, LVol) == {}


# --- uniqueness -------------------------------------------------------------

def test_duplicate_unique_value_is_refused(db):
    _ready(LVol)
    first = _lvol("same-name")
    first.write_to_db(db.kv_store)

    second = _lvol("same-name")
    with pytest.raises(UniqueIndexViolation) as excinfo:
        second.write_to_db(db.kv_store)

    assert excinfo.value.index_name == 'pool_uuid+lvol_name'
    assert excinfo.value.holder == first.get_id()


def test_a_refused_write_leaves_neither_entity_nor_index(db):
    _ready(LVol)
    first = _lvol("same-name")
    first.write_to_db(db.kv_store)

    second = _lvol("same-name")
    with pytest.raises(UniqueIndexViolation):
        second.write_to_db(db.kv_store)

    assert db.kv_store.get(second.get_db_id().encode()) is None
    assert [v.get_id() for v in db.query(LVol, 'pool_uuid', POOL)] == [first.get_id()]


def test_rewriting_the_same_entity_is_not_a_violation(db):
    _ready(LVol)
    lvol = _lvol("vol-a")
    lvol.write_to_db(db.kv_store)

    lvol.size = 4096
    lvol.write_to_db(db.kv_store)  # must not collide with its own entry

    assert db.get_lvol_by_id(lvol.get_id()).size == 4096


def test_the_same_name_in_another_pool_is_allowed(db):
    _ready(LVol)
    _lvol("vol-a", pool="pool-1").write_to_db(db.kv_store)

    _lvol("vol-a", pool="pool-2").write_to_db(db.kv_store)

    assert len(db.query(LVol, 'lvol_name', "vol-a")) == 2


def test_the_violation_propagates_out_of_the_write(db):
    """It must not become a 4xx: a create site that caught it would turn the
    only signal that means corruption into an ordinary user error."""
    _ready(LVol)
    _lvol("same-name").write_to_db(db.kv_store)

    source = lvol_controller.add_lvol_ha.__code__.co_consts
    assert UniqueIndexViolation not in source, "add_lvol_ha must not catch it"

    with pytest.raises(UniqueIndexViolation):
        _lvol("same-name").write_to_db(db.kv_store)


def test_the_precheck_still_reports_a_duplicate_name_cleanly(db):
    """The constraint is a backstop; the user-facing answer comes from the
    pre-check, which is now a point read on a trustworthy key."""
    _ready(LVol)
    pool = Pool()
    pool.uuid = POOL
    pool.cluster_id = CLUSTER
    pool.pool_name = "p"
    pool.status = Pool.STATUS_ACTIVE
    pool.write_to_db(db.kv_store)
    _lvol("taken").write_to_db(db.kv_store)

    assert db.lvol_name_taken(POOL, "taken") is True
    assert db.lvol_name_taken(POOL, "free") is False
    assert db.lvol_name_lookup(POOL, "taken").lvol_name == "taken"


def test_the_precheck_holds_while_the_index_is_still_building(db):
    """The window the backfill has not reached is exactly where a uniqueness
    pre-check reading the index directly would report a taken name as free."""
    lvol = _lvol("taken")
    db.kv_store[lvol.get_db_id().encode()] = json.dumps(
        lvol.to_dict(unwrap_secrets=True)).encode()

    assert db.index_state(LVol, 'pool_uuid+lvol_name') == indices.STATE_BUILDING
    assert db.lvol_name_taken(POOL, "taken") is True


def test_snapshot_name_precheck_holds_while_building(db):
    from simplyblock_core.models.snapshot import SnapShot

    snap = SnapShot()
    snap.uuid = "snap-1"
    snap.snap_name = "taken"
    snap.cluster_id = CLUSTER
    snap.pool_uuid = POOL
    db.kv_store[snap.get_db_id().encode()] = json.dumps(
        snap.to_dict(unwrap_secrets=True)).encode()

    assert db.index_state(SnapShot, 'cluster_id+snap_name') == indices.STATE_BUILDING
    assert db.snap_name_taken(CLUSTER, "taken") is True
    assert db.snap_name_taken(CLUSTER, "free") is False


# --- state, fallback and backfill -------------------------------------------

def test_state_defaults_to_building(db):
    assert db.index_state(LVol, 'pool_uuid') == indices.STATE_BUILDING


def test_building_falls_back_to_the_scan_and_agrees_with_ready(db):
    """The strongest single assertion available: index and scan pinned to the
    same answer, which is what makes the rollout switchable at all."""
    for name in ("vol-a", "vol-b", "vol-c"):
        _lvol(name).write_to_db(db.kv_store)
    _lvol("elsewhere", pool="pool-other").write_to_db(db.kv_store)

    scanned = [v.get_id() for v in db.query(LVol, 'pool_uuid', POOL)]
    _ready(LVol)
    indexed = [v.get_id() for v in db.query(LVol, 'pool_uuid', POOL)]

    assert scanned == indexed
    assert len(indexed) == 3


def test_ordered_index_agrees_between_scan_and_index(db):
    """Ordering, not just membership: `limit`/`reverse` must mean the same on
    both paths or the chain tail moves when the index flips."""
    from simplyblock_core.models.snapshot import SnapShot

    lvol = _lvol("vol-a")
    lvol.write_to_db(db.kv_store)
    for seq in (30, 10, 20):
        snap = SnapShot()
        snap.uuid = f"snap-{seq}"
        snap.snap_name = f"snap-{seq}"
        snap.cluster_id = CLUSTER
        snap.pool_uuid = POOL
        snap.created_at = seq
        snap.vuid = seq
        snap.lvol = lvol
        snap.write_to_db(db.kv_store)

    scanned = db.get_lvol_latest_snapshot(lvol.get_id())
    _ready(SnapShot)
    indexed = db.get_lvol_latest_snapshot(lvol.get_id())

    assert scanned.get_id() == indexed.get_id() == "snap-30"


def test_disabled_is_a_kill_switch_for_reads_and_writes(db):
    _lvol("vol-a").write_to_db(db.kv_store)
    _ready(LVol)
    db.set_index_state(LVol, 'node_id', indices.STATE_DISABLED)

    # The read still answers — from the scan.
    assert len(db.query(LVol, 'node_id', "node-1")) == 1

    # And the write no longer touches that index's keys.
    before = {k for k in _index_keys(db, LVol) if b'/node_id/' in k}
    _lvol("vol-b").write_to_db(db.kv_store)
    after = {k for k in _index_keys(db, LVol) if b'/node_id/' in k}
    assert before == after


def test_backfill_indexes_records_written_before_the_declaration(db):
    """Seeded raw, the way records written by an older release exist: no index
    entry was ever derived for them."""
    seeded = []
    for n in range(5):
        lvol = _lvol(f"vol-{n}")
        db.kv_store[lvol.get_db_id().encode()] = json.dumps(
            lvol.to_dict(unwrap_secrets=True)).encode()
        seeded.append(lvol.get_id())
    assert _index_keys(db, LVol) == {}

    index_ops.build_indices([LVol])

    assert db.index_state(LVol, 'pool_uuid') == indices.STATE_READY
    assert sorted(v.get_id() for v in db.query(LVol, 'pool_uuid', POOL)) == sorted(seeded)


def test_backfill_resumes_from_its_cursor(db):
    for n in range(5):
        lvol = _lvol(f"vol-{n}")
        db.kv_store[lvol.get_db_id().encode()] = json.dumps(
            lvol.to_dict(unwrap_secrets=True)).encode()

    keys = sorted(bytes(key) for key, _value in db.kv_store.get_range_startswith(
        LVol.keyspace_prefix()))
    db.kv_store.clear_range_startswith(f'{indices.INDEX_PREFIX}LVol/'.encode())
    for index in indices.indexes_of(LVol):
        db.set_index_state(LVol, index, indices.STATE_BUILDING,
                           cursor=keys[2].decode())

    index_ops.build_indices([LVol])

    # Everything from the cursor on is indexed; the records the interrupted run
    # already covered are not walked again.
    indexed = {v for v in _index_keys(db, LVol).values()}
    assert len(indexed) == 3
    assert all(key.split(b'/')[-1] in indexed for key in keys[2:])


def test_backfill_is_idempotent(db):
    _lvol("vol-a").write_to_db(db.kv_store)
    index_ops.build_indices([LVol])
    first = _index_keys(db, LVol)

    index_ops.build_indices([LVol])

    assert _index_keys(db, LVol) == first


def test_backfill_skips_a_disabled_index(db):
    _lvol("vol-a").write_to_db(db.kv_store)
    db.set_index_state(LVol, 'node_id', indices.STATE_DISABLED)
    db.kv_store.clear_range_startswith(f'{indices.INDEX_PREFIX}LVol/'.encode())

    index_ops.build_indices([LVol])

    assert db.index_state(LVol, 'node_id') == indices.STATE_DISABLED
    assert not any(b'/node_id/' in k for k in _index_keys(db, LVol))


def test_lvol_cluster_id_is_backfilled_from_the_node(db):
    node = StorageNode()
    node.uuid = "node-1"
    node.cluster_id = CLUSTER
    node.write_to_db(db.kv_store)
    lvol = _lvol("vol-a", cluster="")
    db.kv_store[lvol.get_db_id().encode()] = json.dumps(
        lvol.to_dict(unwrap_secrets=True)).encode()

    index_ops.backfill_lvol_cluster_id()

    assert db.get_lvol_by_id(lvol.get_id()).cluster_id == CLUSTER


# --- the verifier -----------------------------------------------------------

def test_verifier_is_quiet_on_a_healthy_index(db):
    _lvol("vol-a").write_to_db(db.kv_store)
    _ready(LVol)

    findings = index_ops.check_indices([LVol])

    assert findings['missing'] == []
    assert findings['stale'] == []
    assert findings['orphaned'] == []


def test_verifier_detects_and_repairs_a_missing_entry(db):
    lvol = _lvol("vol-a")
    lvol.write_to_db(db.kv_store)
    _ready(LVol)
    victim = next(iter(indices.get_index(LVol, 'node_id').keys(LVol, lvol)))
    db.kv_store.clear(victim)

    assert index_ops.check_indices([LVol])['missing']

    index_ops.check_indices([LVol], repair=True)

    assert index_ops.check_indices([LVol])['missing'] == []


def test_verifier_detects_a_corrupted_entry(db):
    lvol = _lvol("vol-a")
    lvol.write_to_db(db.kv_store)
    _ready(LVol)
    victim = next(iter(indices.get_index(LVol, 'node_id').keys(LVol, lvol)))
    db.kv_store[victim] = b'some-other-entity'

    findings = index_ops.check_indices([LVol])

    assert [entry[0] for entry in findings['stale']] == [victim.decode()]


def test_verifier_names_the_duplicate_behind_a_unique_violation(db):
    """Two live records deriving one unique key is the condition a
    UniqueIndexViolation reports at write time; this is what says so afterwards."""
    _ready(LVol)
    first = _lvol("same-name")
    first.write_to_db(db.kv_store)
    # Seeded raw: the write path is exactly what refuses this.
    second = _lvol("same-name")
    db.kv_store[second.get_db_id().encode()] = json.dumps(
        second.to_dict(unwrap_secrets=True)).encode()

    findings = index_ops.check_indices([LVol])

    duplicates = [entry for entry in findings['duplicate']
                  if 'pool_uuid+lvol_name' in entry[0]]
    assert len(duplicates) == 1
    assert set(duplicates[0][1:]) == {first.get_id(), second.get_id()}


def test_verifier_repairs_a_corrupted_non_unique_entry(db):
    lvol = _lvol("vol-a")
    lvol.write_to_db(db.kv_store)
    _ready(LVol)
    victim = next(iter(indices.get_index(LVol, 'node_id').keys(LVol, lvol)))
    db.kv_store[victim] = b'some-other-entity'

    index_ops.check_indices([LVol], repair=True)

    assert bytes(db.kv_store.get(victim)).decode() == lvol.get_id()
    assert index_ops.check_indices([LVol])['stale'] == []


def test_verifier_detects_and_repairs_an_orphan(db):
    _lvol("vol-a").write_to_db(db.kv_store)
    _ready(LVol)
    orphan = b'index/LVol/node_id/ghost-node/ghost-lvol'
    db.kv_store[orphan] = b'ghost-lvol'

    assert index_ops.check_indices([LVol])['orphaned'] == [orphan.decode()]

    index_ops.check_indices([LVol], repair=True)

    assert db.kv_store.get(orphan) is None


# --- the lookups the indices exist for --------------------------------------

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
