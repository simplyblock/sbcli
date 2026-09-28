"""A unique index as the constraint, and the pre-check in front of it."""
import pytest

from simplyblock_core.controllers import lvol_controller
from simplyblock_core.models import indices
from simplyblock_core.models.indices import UniqueIndexViolation
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.pool import Pool

from .helpers import CLUSTER, POOL, make_lvol, ready, seed_raw


# --- uniqueness -------------------------------------------------------------

def test_duplicate_unique_value_is_refused(db):
    ready(LVol)
    first = make_lvol("same-name")
    first.write_to_db(db.kv_store)

    second = make_lvol("same-name")
    with pytest.raises(UniqueIndexViolation) as excinfo:
        second.write_to_db(db.kv_store)

    assert excinfo.value.index_name == 'pool_uuid+lvol_name'
    assert excinfo.value.holder == first.get_id()


def test_a_refused_write_leaves_neither_entity_nor_index(db):
    ready(LVol)
    first = make_lvol("same-name")
    first.write_to_db(db.kv_store)

    second = make_lvol("same-name")
    with pytest.raises(UniqueIndexViolation):
        second.write_to_db(db.kv_store)

    assert db.kv_store.get(second.get_db_id().encode()) is None
    assert [v.get_id() for v in db.query(LVol, 'pool_uuid', POOL)] == [first.get_id()]


def test_rewriting_the_same_entity_is_not_a_violation(db):
    ready(LVol)
    lvol = make_lvol("vol-a")
    lvol.write_to_db(db.kv_store)

    lvol.size = 4096
    lvol.write_to_db(db.kv_store)  # must not collide with its own entry

    assert db.get_lvol_by_id(lvol.get_id()).size == 4096


def test_the_same_name_in_another_pool_is_allowed(db):
    ready(LVol)
    make_lvol("vol-a", pool="pool-1").write_to_db(db.kv_store)

    make_lvol("vol-a", pool="pool-2").write_to_db(db.kv_store)

    assert len(db.query(LVol, 'lvol_name', "vol-a")) == 2


def test_the_violation_propagates_out_of_the_write(db):
    """It must not become a 4xx: a create site that caught it would turn the
    only signal that means corruption into an ordinary user error."""
    ready(LVol)
    make_lvol("same-name").write_to_db(db.kv_store)

    source = lvol_controller.add_lvol_ha.__code__.co_consts
    assert UniqueIndexViolation not in source, "add_lvol_ha must not catch it"

    with pytest.raises(UniqueIndexViolation):
        make_lvol("same-name").write_to_db(db.kv_store)


def test_the_precheck_still_reports_a_duplicate_name_cleanly(db):
    """The constraint is a backstop; the user-facing answer comes from the
    pre-check, which is now a point read on a trustworthy key."""
    ready(LVol)
    pool = Pool()
    pool.uuid = POOL
    pool.cluster_id = CLUSTER
    pool.pool_name = "p"
    pool.status = Pool.STATUS_ACTIVE
    pool.write_to_db(db.kv_store)
    make_lvol("taken").write_to_db(db.kv_store)

    assert db.lvol_name_taken(POOL, "taken") is True
    assert db.lvol_name_taken(POOL, "free") is False
    assert db.lvol_name_lookup(POOL, "taken").lvol_name == "taken"


def test_the_precheck_holds_while_the_index_is_still_building(db):
    """The window the backfill has not reached is exactly where a uniqueness
    pre-check reading the index directly would report a taken name as free."""
    seed_raw(db, make_lvol("taken"))

    assert db.index_state(LVol, 'pool_uuid+lvol_name') == indices.STATE_BUILDING
    assert db.lvol_name_taken(POOL, "taken") is True


def test_snapshot_name_precheck_holds_while_building(db):
    from simplyblock_core.models.snapshot import SnapShot

    snap = SnapShot()
    snap.uuid = "snap-1"
    snap.snap_name = "taken"
    snap.cluster_id = CLUSTER
    snap.pool_uuid = POOL
    seed_raw(db, snap)

    assert db.index_state(SnapShot, 'cluster_id+snap_name') == indices.STATE_BUILDING
    assert db.snap_name_taken(CLUSTER, "taken") is True
    assert db.snap_name_taken(CLUSTER, "free") is False
