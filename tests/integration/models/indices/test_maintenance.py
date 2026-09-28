"""Index entries are maintained by the entity write that derives them."""
import fdb
import pytest

from simplyblock_core.models.lvol_model import LVol

from .helpers import POOL, index_keys, make_lvol, ready


# --- the round trip ---------------------------------------------------------

def test_write_then_query_returns_the_row(db):
    lvol = make_lvol("vol-a")
    lvol.write_to_db(db.kv_store)
    ready(LVol)

    found = db.query(LVol, 'pool_uuid', POOL)

    assert [v.get_id() for v in found] == [lvol.get_id()]


def test_update_moves_the_entry(db):
    lvol = make_lvol("vol-a", node="node-1")
    lvol.write_to_db(db.kv_store)
    ready(LVol)

    lvol.node_id = "node-2"
    lvol.write_to_db(db.kv_store)

    assert db.query(LVol, 'node_id', "node-1") == []
    assert [v.get_id() for v in db.query(LVol, 'node_id', "node-2")] == [lvol.get_id()]


def test_remove_clears_every_entry(db):
    lvol = make_lvol("vol-a")
    lvol.write_to_db(db.kv_store)
    ready(LVol)
    assert index_keys(db, LVol)

    lvol.remove(db.kv_store)

    assert index_keys(db, LVol) == {}
    assert db.query(LVol, 'pool_uuid', POOL) == []


def test_remove_uses_the_stored_record_not_the_callers_copy(db):
    """A caller holding a stale copy must not leave the live entry behind."""
    lvol = make_lvol("vol-a", node="node-1")
    lvol.write_to_db(db.kv_store)
    ready(LVol)

    fresh = db.get_lvol_by_id(lvol.get_id())
    fresh.node_id = "node-2"
    fresh.write_to_db(db.kv_store)

    lvol.remove(db.kv_store)  # the stale copy still says node-1

    assert index_keys(db, LVol) == {}


def test_atomic_update_maintains_the_index(db):
    lvol = make_lvol("vol-a", node="node-1")
    lvol.write_to_db(db.kv_store)
    ready(LVol)

    db.atomic_update(lvol, lambda obj: setattr(obj, 'node_id', "node-2"))

    assert db.query(LVol, 'node_id', "node-1") == []
    assert [v.get_id() for v in db.query(LVol, 'node_id', "node-2")] == [lvol.get_id()]


def test_atomic_update_that_aborts_leaves_the_index_alone(db):
    lvol = make_lvol("vol-a", node="node-1")
    lvol.write_to_db(db.kv_store)
    ready(LVol)
    before = index_keys(db, LVol)

    def _mutate(obj):
        obj.node_id = "node-2"
        return False  # guard no longer holds: abort the write

    db.atomic_update(lvol, _mutate)

    assert index_keys(db, LVol) == before


# --- atomicity --------------------------------------------------------------

def test_a_transaction_that_fails_after_the_write_leaves_no_orphan(db):
    """The property the hand-rolled indices did not have: maintenance is in the
    entity's own transaction, so a crash between the two is not a state."""
    ready(LVol)
    lvol = make_lvol("vol-a")

    def _write_then_fail(tr, obj):
        obj.write_to_db(tr)
        raise RuntimeError("caller failed after the write")

    with pytest.raises(RuntimeError):
        fdb.transactional(_write_then_fail)(db.kv_store, lvol)

    assert db.kv_store.get(lvol.get_db_id().encode()) is None
    assert index_keys(db, LVol) == {}
