"""A fail-over/-back clone returning into its origin's name, when a stale unique
index entry outlived that origin.

Regression: 2026-09-29-clone-home-ghost-unique — a 5-member group fail-back live
run. Fail-back deletes the original source lvols on the home cluster and then
clones the promoted volumes back into the SAME PVC names. When a
``pool_uuid+lvol_name`` unique entry survived the delete of the record it named
(an orphan left under the heavy concurrent create/delete of a group promote), the
clone's write raised ``UniqueIndexViolation`` against a holder that no longer
existed. Every member 409'd on ``already held by <deleted lvol>`` and the group
never reconstituted. The clone-home write must self-heal a ghost-held name and
retry, while still refusing a value a live record genuinely holds.
"""
import pytest

from simplyblock_core.controllers import lvol_controller
from simplyblock_core.models.indices import UniqueIndexViolation
from simplyblock_core.models.lvol_model import LVol

from .helpers import POOL, make_lvol, ready


def _unique_value(db, lvol):
    """The id the ``pool_uuid+lvol_name`` unique key currently names."""
    return db.lvol_name_lookup(POOL, lvol.lvol_name).get_id()


def _make_ghost_unique_entry(db, name):
    """Leave a ``pool_uuid+lvol_name`` unique entry naming a record that is gone.

    This is the orphan a concurrent delete left behind: the record key was
    cleared while its unique index entry stayed, so the value is held by an id
    ``get_lvol_by_id`` can no longer resolve.
    """
    origin = make_lvol(name)
    origin.write_to_db(db.kv_store)
    ready(LVol)
    db.kv_store.clear(origin.get_db_id().encode())  # record only; index survives
    return origin


def test_bare_write_still_reproduces_the_bug(db):
    """The condition the fix exists for: without the self-heal, a clone reusing a
    ghost-held name is refused against a holder that no longer exists."""
    ghost = _make_ghost_unique_entry(db, "pvc-clone-home")

    clone = make_lvol("pvc-clone-home")
    with pytest.raises(UniqueIndexViolation) as excinfo:
        clone.write_to_db(db.kv_store)

    assert excinfo.value.holder == ghost.get_id()


def test_clone_home_reclaims_a_ghost_held_name(db):
    _make_ghost_unique_entry(db, "pvc-clone-home")

    clone = make_lvol("pvc-clone-home")
    lvol_controller._persist_clone_reclaiming_ghost_unique(db, clone)

    assert db.get_lvol_by_id(clone.get_id()).get_id() == clone.get_id()
    assert _unique_value(db, clone) == clone.get_id()


def test_clone_home_still_refuses_a_name_a_live_record_holds(db):
    """The self-heal clears only confirmed orphans, so a genuine duplicate — a
    value a live record still derives — re-raises on the retry rather than being
    masked into silent data loss."""
    live = make_lvol("pvc-clone-home")
    live.write_to_db(db.kv_store)
    ready(LVol)

    clone = make_lvol("pvc-clone-home")
    with pytest.raises(UniqueIndexViolation) as excinfo:
        lvol_controller._persist_clone_reclaiming_ghost_unique(db, clone)

    assert excinfo.value.holder == live.get_id()
    # The live holder is untouched: repair left its key, the lookup still resolves it.
    assert _unique_value(db, live) == live.get_id()
