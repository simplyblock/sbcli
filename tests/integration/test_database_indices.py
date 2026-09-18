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

from simplyblock_core import index_ops
from simplyblock_core.controllers import lvol_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models import indices
from simplyblock_core.models.base_model import BaseModel
from simplyblock_core.models.indices import UniqueIndexViolation
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.utils import ttl_cache

CLUSTER = "cluster-idx-1"
POOL = "pool-idx-1"


@pytest.fixture
def db():
    return DBController()


@pytest.fixture(autouse=True)
def no_convergence_wait(monkeypatch):
    """Every state switch waits out the state cache fleet-wide; the tests that
    are not about that wait would each pay it in real seconds. The three that
    are about it set it back."""
    monkeypatch.setattr(ttl_cache, 'INDEX_STATE_CONVERGENCE_SEC', 0)


def _lvol(name, *, uuid_=None, pool=POOL, node="node-1"):
    lvol = LVol()
    lvol.uuid = uuid_ or str(uuid.uuid4())
    lvol.lvol_name = name
    lvol.pool_uuid = pool
    lvol.node_id = node
    lvol.status = LVol.STATUS_ONLINE
    lvol.create_dt = f"2026-01-01 00:00:{len(name):02d}"
    return lvol


def _ready(model_cls):
    index_ops.build_indices([model_cls])


def _index_keys(db, model_cls):
    prefix = f'{indices.INDEX_PREFIX}{model_cls.__name__}/'.encode()
    return {bytes(k): bytes(v) for k, v in db.kv_store.get_range_startswith(prefix)}


def _indexed_ids(db, model_cls, index_name):
    """The entity ids one index holds entries for, read the way a reader does."""
    index = indices.get_index(model_cls, index_name)
    return {
        index.entry_id(model_cls, bytes(key), bytes(value))
        for key, value in db.kv_store.get_range_startswith(index.prefix(model_cls, ()))
    }


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
    index_ops.disable_index(LVol, 'node_id')

    # The read still answers — from the scan.
    assert len(db.query(LVol, 'node_id', "node-1")) == 1

    # And the write no longer touches that index's keys.
    before = {k for k in _index_keys(db, LVol) if b'/node_id/' in k}
    _lvol("vol-b").write_to_db(db.kv_store)
    after = {k for k in _index_keys(db, LVol) if b'/node_id/' in k}
    assert before == after


def test_disabling_leaves_the_entries_in_place(db):
    """Deliberate: another process may hold `ready` from its TTL cache for a
    few seconds yet, and an emptied index would answer its reads with no rows
    instead of sending them to the scan."""
    _lvol("vol-a").write_to_db(db.kv_store)
    _ready(LVol)
    before = {k for k in _index_keys(db, LVol) if b'/node_id/' in k}

    index_ops.disable_index(LVol, 'node_id')

    assert {k for k in _index_keys(db, LVol) if b'/node_id/' in k} == before


def _record_waits(db, monkeypatch, model_cls, index_name):
    """Make the convergence wait observable: what the switch looks like from
    another process at the moment it starts waiting."""
    monkeypatch.setattr(ttl_cache, 'INDEX_STATE_CONVERGENCE_SEC', 30)
    waits = []
    monkeypatch.setattr(index_ops.time, 'sleep', lambda delay: waits.append((
        delay,
        db.index_meta(model_cls, index_name).get('state'),
        {k for k in _index_keys(db, model_cls) if f'/{index_name}/'.encode() in k},
    )))
    return waits


def test_disabling_waits_until_every_process_has_seen_it(db, monkeypatch):
    """The state is TTL-cached per process, so a kill switch that returned on
    the commit would return before it was a kill switch."""
    _lvol("vol-a").write_to_db(db.kv_store)
    _ready(LVol)
    waits = _record_waits(db, monkeypatch, LVol, 'node_id')

    index_ops.disable_index(LVol, 'node_id')

    assert [(delay, state) for delay, state, _keys in waits] == [
        (30, indices.STATE_DISABLED)]


def test_enabling_clears_only_after_the_disable_has_converged(db, monkeypatch):
    """A process still holding `ready` from its cache would read the emptied
    index as "no such records" — the answer the kill switch exists to avoid."""
    _lvol("vol-a").write_to_db(db.kv_store)
    _ready(LVol)
    index_ops.disable_index(LVol, 'node_id')
    waits = _record_waits(db, monkeypatch, LVol, 'node_id')

    index_ops.enable_index(LVol, 'node_id')

    assert len(waits) == 1
    _delay, state, keys = waits[0]
    assert state == indices.STATE_DISABLED
    assert keys, "the index was emptied before the readers had converged"
    assert not any(b'/node_id/' in k for k in _index_keys(db, LVol))


def test_enabling_a_ready_index_takes_it_out_of_service_first(db, monkeypatch):
    """`--set building` on an index that was never disabled is the same race:
    the clear must not run while readers still trust it."""
    _lvol("vol-a").write_to_db(db.kv_store)
    _ready(LVol)
    waits = _record_waits(db, monkeypatch, LVol, 'node_id')

    index_ops.enable_index(LVol, 'node_id')

    assert [state for _delay, state, _keys in waits] == [indices.STATE_DISABLED]
    assert db.index_state(LVol, 'node_id') == indices.STATE_BUILDING


def test_enabling_clears_only_its_own_index(db):
    _lvol("vol-a").write_to_db(db.kv_store)
    _ready(LVol)
    index_ops.disable_index(LVol, 'node_id')

    index_ops.enable_index(LVol, 'node_id')

    keys = _index_keys(db, LVol)
    assert not any(b'/node_id/' in k for k in keys)
    assert any(b'/pool_uuid/' in k for k in keys)


def test_enabling_pool_uuid_leaves_the_composite_index_alone(db):
    """`index/LVol/pool_uuid/` must not reach `index/LVol/pool_uuid+lvol_name/`."""
    _lvol("vol-a").write_to_db(db.kv_store)
    _ready(LVol)
    index_ops.disable_index(LVol, 'pool_uuid')

    index_ops.enable_index(LVol, 'pool_uuid')

    keys = _index_keys(db, LVol)
    assert not any(b'/pool_uuid/' in k for k in keys)
    assert any(b'/pool_uuid+lvol_name/' in k for k in keys)


def test_reenabling_after_a_change_does_not_leave_a_stale_entry(db):
    """The reason enabling clears: while an index is out of service nothing
    maintains it, and the backfill that follows only ever *adds*."""
    lvol = _lvol("vol-a", node="node-1")
    lvol.write_to_db(db.kv_store)
    _ready(LVol)

    index_ops.disable_index(LVol, 'node_id')
    lvol.node_id = "node-2"
    lvol.write_to_db(db.kv_store)

    index_ops.enable_index(LVol, 'node_id')
    assert db.index_state(LVol, 'node_id') == indices.STATE_BUILDING
    index_ops.build_indices([LVol])

    assert db.index_state(LVol, 'node_id') == indices.STATE_READY
    assert db.query(LVol, 'node_id', "node-1") == []
    assert [v.get_id() for v in db.query(LVol, 'node_id', "node-2")] == [lvol.get_id()]
    assert index_ops.check_indices([LVol])['stale'] == []


def test_index_states_reports_every_declared_index(db):
    _ready(LVol)
    index_ops.disable_index(LVol, 'node_id')

    states = {row['index']: row['state'] for row in index_ops.index_states(LVol)}

    assert states['LVol.node_id'] == indices.STATE_DISABLED
    assert states['LVol.pool_uuid'] == indices.STATE_READY
    assert set(states) == {f'LVol.{index.name}' for index in indices.indexes_of(LVol)}


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
    indexed = _indexed_ids(db, LVol, 'pool_uuid')
    assert len(indexed) == 3
    assert all(key.split(b'/')[-1].decode() in indexed for key in keys[2:])


def test_backfill_is_idempotent(db):
    _lvol("vol-a").write_to_db(db.kv_store)
    index_ops.build_indices([LVol])
    first = _index_keys(db, LVol)

    index_ops.build_indices([LVol])

    assert _index_keys(db, LVol) == first


def _race_on_read(monkeypatch, mutate, *, every_attempt=False):
    """Commit ``mutate`` between the backfill's read of a record and its commit.

    The window the backfill has to be correct in. ``BaseModel._read_record`` is
    the hook because it is what `_index_record` reads through; `atomic_update`
    is the racing write because it does *not* go through it, so the race cannot
    re-enter itself.
    """
    real, attempts = BaseModel._read_record, []

    def read_then_race(tr, key, model_cls):
        obj = real(tr, key, model_cls)
        if every_attempt or not attempts:
            attempts.append(True)
            mutate()
        return obj

    monkeypatch.setattr(BaseModel, '_read_record', staticmethod(read_then_race))
    return attempts


def test_a_record_rewritten_mid_derivation_is_not_indexed_stale(db, monkeypatch):
    """Entries are derived from the record read *inside* the writing
    transaction, so a write that lands in between is re-derived from rather
    than committed over. An entry written under a value the record no longer
    carries would never be cleared: no live write derives it, so no write diff
    removes it, and only `check-indices --repair` would ever find it."""
    lvol = _seed_raw(db, _lvol("vol-a", node="node-1"))
    raced = _race_on_read(monkeypatch, lambda: db.atomic_update(
        db.get_lvol_by_id(lvol.get_id()),
        lambda lv: setattr(lv, 'node_id', "node-2")))

    index_ops.build_indices([LVol])

    assert raced, "the race never fired; the test proves nothing"
    assert db.index_state(LVol, 'node_id') == indices.STATE_READY
    assert db.query(LVol, 'node_id', "node-1") == []
    assert [v.get_id() for v in db.query(LVol, 'node_id', "node-2")] == [lvol.get_id()]


def test_a_record_that_never_settles_holds_its_indices_back(db, monkeypatch):
    """A derivation that loses every attempt is the one case the backfill
    cannot complete on its own. Flipping `ready` around it would publish an
    index whose missing entries nothing is going to add."""
    lvol = _seed_raw(db, _lvol("vol-a", node="node-1"))
    counter = iter(range(1000))
    raced = _race_on_read(monkeypatch, lambda: db.atomic_update(
        db.get_lvol_by_id(lvol.get_id()),
        lambda lv: setattr(lv, 'node_id', f"node-{next(counter)}")),
        every_attempt=True)

    report = index_ops.build_indices([LVol])

    assert len(raced) == index_ops.INDEX_RACE_ATTEMPTS
    assert db.index_state(LVol, 'node_id') == indices.STATE_BUILDING
    assert index_ops.unready_indices([LVol])
    assert any('could not be indexed' in line for line in report)


def _seed_raw(db, lvol):
    """Persist a record the way an older release left it: entity only, no index
    entry derived for it."""
    db.kv_store[lvol.get_db_id().encode()] = json.dumps(
        lvol.to_dict(unwrap_secrets=True)).encode()
    return lvol


def test_backfill_refuses_to_flip_a_violated_unique_index(db):
    """Pre-index data can already carry two records with one unique value —
    nothing enforced the constraint before the declaration shipped. Flipping the
    index `ready` over that collapses them: the key holds one id, so the point
    read behind every name lookup answers for whichever record the walk wrote
    last and the other stops existing."""
    first = _seed_raw(db, _lvol("same-name"))
    second = _seed_raw(db, _lvol("same-name"))

    report = index_ops.build_indices([LVol])

    assert db.index_state(LVol, 'pool_uuid+lvol_name') == indices.STATE_BUILDING
    # Only the violated index is held back; state is per index for this reason.
    assert db.index_state(LVol, 'pool_uuid') == indices.STATE_READY
    assert {v.get_id() for v in db.query(LVol, 'pool_uuid+lvol_name', POOL, "same-name")} \
        == {first.get_id(), second.get_id()}
    assert any('pool_uuid+lvol_name' in line and 'duplicate' in line for line in report)


def test_backfill_detects_a_duplicate_of_an_already_indexed_record(db):
    """The other half of the detection: the holder of the key is not one of the
    records being walked but was written by live traffic while the index was
    building."""
    first = _lvol("same-name")
    first.write_to_db(db.kv_store)
    _seed_raw(db, _lvol("same-name"))

    index_ops.build_indices([LVol])

    assert db.index_state(LVol, 'pool_uuid+lvol_name') == indices.STATE_BUILDING
    assert db.query_ids(LVol, 'pool_uuid+lvol_name', POOL, "same-name") != [first.get_id()]


def test_a_refused_index_is_re_examined_on_the_next_run(db):
    """Leaving it `building` is only safe if the next run re-walks the table: a
    cursor left at the last record would have the re-run find nothing to check
    and flip the very index it just refused."""
    _seed_raw(db, _lvol("same-name"))
    _seed_raw(db, _lvol("same-name"))
    index_ops.build_indices([LVol])

    index_ops.build_indices([LVol])

    assert db.index_state(LVol, 'pool_uuid+lvol_name') == indices.STATE_BUILDING


def test_the_refused_index_flips_once_the_duplicate_is_resolved(db):
    first = _seed_raw(db, _lvol("same-name"))
    second = _seed_raw(db, _lvol("same-name"))
    index_ops.build_indices([LVol])

    second.remove(db.kv_store)
    index_ops.build_indices([LVol])

    assert db.index_state(LVol, 'pool_uuid+lvol_name') == indices.STATE_READY
    assert db.lvol_name_lookup(POOL, "same-name").get_id() == first.get_id()


def test_backfill_skips_a_disabled_index(db):
    _lvol("vol-a").write_to_db(db.kv_store)
    index_ops.disable_index(LVol, 'node_id')
    db.kv_store.clear_range_startswith(f'{indices.INDEX_PREFIX}LVol/'.encode())

    index_ops.build_indices([LVol])

    assert db.index_state(LVol, 'node_id') == indices.STATE_DISABLED
    assert not any(b'/node_id/' in k for k in _index_keys(db, LVol))


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

    _lvol("vol-a", pool="pool-0").write_to_db(db.kv_store)
    _lvol("vol-b", pool="pool-1", node="node-elsewhere").write_to_db(db.kv_store)
    _lvol("vol-c", pool="pool-elsewhere").write_to_db(db.kv_store)

    scanned = sorted(lv.lvol_name for lv in db.get_lvols(CLUSTER))
    _ready(LVol)
    indexed = sorted(lv.lvol_name for lv in db.get_lvols(CLUSTER))

    assert scanned == indexed == ["vol-a", "vol-b"]


def test_cluster_of_a_volume_agrees_with_the_listing(db):
    """The per-volume accessor and the listing are one relation, two directions."""
    _cluster_pools(db, 1)
    lvol = _lvol("vol-a", pool="pool-0")
    lvol.write_to_db(db.kv_store)
    _ready(LVol)

    assert db.get_cluster_id_by_lvol(lvol) == CLUSTER
    assert [lv.get_id() for lv in db.get_lvols(CLUSTER)] == [lvol.get_id()]


def test_lvols_by_cluster_reads_one_range_per_pool(db, counting):
    _cluster_pools(db, 4)
    for n in range(4):
        _lvol(f"vol-{n}", pool=f"pool-{n}").write_to_db(db.kv_store)
    _ready(LVol)
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
        _lvol(f"vol-{n}", pool=f"pool-{n}").write_to_db(db.kv_store)
    db.set_index_state(LVol, 'pool_uuid', indices.STATE_BUILDING)
    counting.range_reads = 0

    assert len(db.get_lvols(CLUSTER)) == 6

    # One for the pool index, one for the volume scan.
    assert counting.range_reads == 2


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


def test_a_non_unique_entry_cannot_name_the_wrong_record(db):
    """The drift the design does not have: a non-unique entry stores nothing, so
    there is no second copy of the id that could disagree with the key's own
    tail. Junk in the value changes no answer and is no finding."""
    lvol = _lvol("vol-a")
    lvol.write_to_db(db.kv_store)
    _ready(LVol)
    victim = next(iter(indices.get_index(LVol, 'node_id').keys(LVol, lvol)))
    db.kv_store[victim] = b'some-other-entity'

    assert [v.get_id() for v in db.query(LVol, 'node_id', "node-1")] == [lvol.get_id()]
    assert index_ops.check_indices([LVol])['stale'] == []


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


def _unique_key(lvol):
    return next(iter(
        indices.get_index(LVol, 'pool_uuid+lvol_name').keys(LVol, lvol)))


def test_verifier_repairs_a_unique_key_its_holder_no_longer_derives(db):
    """The repair the `unique` flag used to refuse wholesale. A unique key names
    one of several claimants only while the one it names still derives it; once
    that record has moved on, the candidate is the only claimant left and the
    key has exactly one right answer — as a non-unique key always does."""
    lvol = _lvol("vol-a")
    lvol.write_to_db(db.kv_store)
    moved_on = _lvol("vol-b")
    moved_on.write_to_db(db.kv_store)
    _ready(LVol)
    db.kv_store[_unique_key(lvol)] = moved_on.get_id().encode()

    findings = index_ops.check_indices([LVol], repair=True)

    assert findings['unresolved'] == 0
    assert db.lvol_name_lookup(POOL, "vol-a").get_id() == lvol.get_id()


def test_verifier_refuses_a_unique_key_two_live_records_derive(db):
    """The other half of the same question, and the one case a repair cannot
    settle: both claimants still carry the value, and overwriting the key would
    take whichever loses out of every lookup the index serves."""
    first = _lvol("same-name")
    first.write_to_db(db.kv_store)
    _ready(LVol)
    second = _seed_raw(db, _lvol("same-name"))

    findings = index_ops.check_indices([LVol], repair=True)

    # The seeded record's non-unique entries are written back...
    assert findings['repaired'] > 0
    assert second.get_id() in _indexed_ids(db, LVol, 'node_id')
    # ...while the name they collide on is left exactly as it was.
    assert findings['duplicate']
    assert findings['unresolved'] > 0
    assert bytes(db.kv_store.get(_unique_key(first))).decode() == first.get_id()


def test_every_finding_ends_in_exactly_one_counter(db):
    """What makes `unresolved` answerable at all: a finding is repaired, or it
    evaporated, or it is still outstanding — never none of the three."""
    first = _lvol("same-name")
    first.write_to_db(db.kv_store)
    _ready(LVol)
    _seed_raw(db, _lvol("same-name"))
    db.kv_store[b'index/LVol/node_id/ghost-node/ghost-lvol'] = b''

    findings = index_ops.check_indices([LVol], repair=True)

    total = sum(len(findings[kind])
                for kind in ('missing', 'stale', 'orphaned', 'duplicate'))
    assert total == (findings['repaired'] + findings['vanished']
                     + findings['unresolved'])


def test_verifier_detects_and_repairs_an_orphan(db):
    _lvol("vol-a").write_to_db(db.kv_store)
    _ready(LVol)
    orphan = b'index/LVol/node_id/ghost-node/ghost-lvol'
    db.kv_store[orphan] = b'ghost-lvol'

    assert index_ops.check_indices([LVol])['orphaned'] == [orphan.decode()]

    index_ops.check_indices([LVol], repair=True)

    assert db.kv_store.get(orphan) is None


def _between_the_walks(monkeypatch, action):
    """Run ``action`` after ``check_indices`` walks the entities and before it
    walks the index keyspace — the window its findings can go stale in.

    Hooked on the index-keyspace walk itself rather than on the range reader
    underneath it, which both walks share."""
    real = index_ops._stored_entries

    def stored_entries(*args, **kwargs):
        action()
        return real(*args, **kwargs)

    monkeypatch.setattr(index_ops, '_stored_entries', stored_entries)


def test_repair_leaves_a_record_deleted_mid_check_alone(db, monkeypatch):
    """A record deleted mid-check is indistinguishable from a missing entry.
    Writing the entry back would leave a key for a record that is gone — an
    orphan, and on a unique index one that blocks the name from then on."""
    lvol = _lvol("vol-a")
    lvol.write_to_db(db.kv_store)
    _ready(LVol)
    _between_the_walks(monkeypatch, lambda: lvol.remove(db.kv_store))

    findings = index_ops.check_indices([LVol], repair=True)

    assert findings['missing']
    assert findings['repaired'] == 0
    assert findings['unresolved'] == 0
    assert findings['vanished'] == len(findings['missing'])
    assert _index_keys(db, LVol) == {}


def test_repair_leaves_a_record_created_mid_check_alone(db, monkeypatch):
    """A record created after the entity walk passed its key has live index
    entries the walk never saw. Clearing those as orphans would take a healthy
    volume out of every lookup."""
    _ready(LVol)
    newcomer = _lvol("vol-b")
    _between_the_walks(monkeypatch, lambda: newcomer.write_to_db(db.kv_store))

    findings = index_ops.check_indices([LVol], repair=True)

    assert findings['orphaned']
    assert findings['repaired'] == 0
    assert findings['unresolved'] == 0
    assert findings['vanished'] == len(findings['orphaned'])
    assert [v.get_id() for v in db.query(LVol, 'node_id', "node-1")] == [newcomer.get_id()]


def _after_the_walks(monkeypatch, action):
    """Run ``action`` once ``check_indices`` has walked the index keyspace —
    the window between a finding and the repair that acts on it."""
    real = index_ops._stored_entries

    def stored_entries(*args, **kwargs):
        entries = real(*args, **kwargs)
        action()
        return entries

    monkeypatch.setattr(index_ops, '_stored_entries', stored_entries)


def test_repair_does_not_steal_a_unique_key_claimed_since_the_walk(db, monkeypatch):
    """A unique key free during the walk and held by a live claimant by the time
    the repair runs is a duplicate for the operator to resolve, not drift to
    overwrite."""
    # Two records derive the one key, and the entity walk keeps the last it
    # meets as the candidate to repair towards — so the ids are pinned, or which
    # of the two that is would be left to uuid order.
    claimant = _seed_raw(db, _lvol("vol-a", uuid_="aaaa-walked-first"))
    lvol = _lvol("vol-a", uuid_="zzzz-walked-last")
    lvol.write_to_db(db.kv_store)
    _ready(LVol)
    unique_key = _unique_key(lvol)
    db.kv_store.clear(unique_key)
    _after_the_walks(
        monkeypatch, lambda: db.kv_store.__setitem__(unique_key, claimant.get_id().encode()))

    findings = index_ops.check_indices([LVol], repair=True)

    assert (unique_key.decode(), lvol.get_id()) in findings['missing']
    assert findings['unresolved'] > 0
    assert bytes(db.kv_store.get(unique_key)).decode() == claimant.get_id()


def test_repair_takes_back_a_unique_key_held_by_a_record_that_is_gone(db, monkeypatch):
    """The converse, and why the refusal cannot be keyed on `unique` alone: a
    key naming a record that does not exist blocks the name for the one record
    that does derive it — forever, since nothing else ever clears it."""
    lvol = _lvol("vol-a")
    lvol.write_to_db(db.kv_store)
    _ready(LVol)
    unique_key = _unique_key(lvol)
    db.kv_store.clear(unique_key)
    _after_the_walks(
        monkeypatch, lambda: db.kv_store.__setitem__(unique_key, b'ghost-lvol'))

    findings = index_ops.check_indices([LVol], repair=True)

    assert findings['unresolved'] == 0
    assert db.lvol_name_lookup(POOL, "vol-a").get_id() == lvol.get_id()


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
