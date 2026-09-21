"""The backfill: restartable, idempotent, and refusing what it cannot index."""
from simplyblock_core import index_ops
from simplyblock_core.models import indices
from simplyblock_core.models.base_model import BaseModel
from simplyblock_core.models.lvol_model import LVol

from .helpers import POOL, index_keys, indexed_ids, make_lvol, seed_raw


def test_backfill_indexes_records_written_before_the_declaration(db):
    """Seeded raw, the way records written by an older release exist: no index
    entry was ever derived for them."""
    seeded = []
    for n in range(5):
        seeded.append(seed_raw(db, make_lvol(f"vol-{n}")).get_id())
    assert index_keys(db, LVol) == {}

    index_ops.build_indices([LVol])

    assert db.index_state(LVol, 'pool_uuid') == indices.STATE_READY
    assert sorted(v.get_id() for v in db.query(LVol, 'pool_uuid', POOL)) == sorted(seeded)


def test_backfill_resumes_from_its_cursor(db):
    for n in range(5):
        seed_raw(db, make_lvol(f"vol-{n}"))

    keys = sorted(bytes(key) for key, _value in db.kv_store.get_range_startswith(
        LVol.keyspace_prefix()))
    db.kv_store.clear_range_startswith(f'{indices.INDEX_PREFIX}LVol/'.encode())
    for index in indices.indexes_of(LVol):
        db.set_index_state(LVol, index, indices.STATE_BUILDING,
                           cursor=keys[2].decode())

    index_ops.build_indices([LVol])

    # Everything from the cursor on is indexed; the records the interrupted run
    # already covered are not walked again.
    indexed = indexed_ids(db, LVol, 'pool_uuid')
    assert len(indexed) == 3
    assert all(key.split(b'/')[-1].decode() in indexed for key in keys[2:])


def test_backfill_is_idempotent(db):
    make_lvol("vol-a").write_to_db(db.kv_store)
    index_ops.build_indices([LVol])
    first = index_keys(db, LVol)

    index_ops.build_indices([LVol])

    assert index_keys(db, LVol) == first


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
    lvol = seed_raw(db, make_lvol("vol-a", node="node-1"))
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
    lvol = seed_raw(db, make_lvol("vol-a", node="node-1"))
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


def test_backfill_refuses_to_flip_a_violated_unique_index(db):
    """Pre-index data can already carry two records with one unique value —
    nothing enforced the constraint before the declaration shipped. Flipping the
    index `ready` over that collapses them: the key holds one id, so the point
    read behind every name lookup answers for whichever record the walk wrote
    last and the other stops existing."""
    first = seed_raw(db, make_lvol("same-name"))
    second = seed_raw(db, make_lvol("same-name"))

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
    first = make_lvol("same-name")
    first.write_to_db(db.kv_store)
    seed_raw(db, make_lvol("same-name"))

    index_ops.build_indices([LVol])

    assert db.index_state(LVol, 'pool_uuid+lvol_name') == indices.STATE_BUILDING
    assert db.query_ids(LVol, 'pool_uuid+lvol_name', POOL, "same-name") != [first.get_id()]


def test_a_refused_index_is_re_examined_on_the_next_run(db):
    """Leaving it `building` is only safe if the next run re-walks the table: a
    cursor left at the last record would have the re-run find nothing to check
    and flip the very index it just refused."""
    seed_raw(db, make_lvol("same-name"))
    seed_raw(db, make_lvol("same-name"))
    index_ops.build_indices([LVol])

    index_ops.build_indices([LVol])

    assert db.index_state(LVol, 'pool_uuid+lvol_name') == indices.STATE_BUILDING


def test_the_refused_index_flips_once_the_duplicate_is_resolved(db):
    first = seed_raw(db, make_lvol("same-name"))
    second = seed_raw(db, make_lvol("same-name"))
    index_ops.build_indices([LVol])

    second.remove(db.kv_store)
    index_ops.build_indices([LVol])

    assert db.index_state(LVol, 'pool_uuid+lvol_name') == indices.STATE_READY
    assert db.lvol_name_lookup(POOL, "same-name").get_id() == first.get_id()


def test_backfill_skips_a_disabled_index(db):
    make_lvol("vol-a").write_to_db(db.kv_store)
    index_ops.disable_index(LVol, 'node_id')
    db.kv_store.clear_range_startswith(f'{indices.INDEX_PREFIX}LVol/'.encode())

    index_ops.build_indices([LVol])

    assert db.index_state(LVol, 'node_id') == indices.STATE_DISABLED
    assert not any(b'/node_id/' in k for k in index_keys(db, LVol))
