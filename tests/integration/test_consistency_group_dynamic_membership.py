"""Dynamic consistency-group membership (design §4.5), against real FDB.

An EXISTING volume joins a group when its PVC gains the membership label, and
detaches when the label is removed. The join guards
(``consistency_group_controller.join_existing_volume``) decide by *reading
model state*: the group's members map, and each open member's LVol record for
the pool-alignment check — every one a DBController accessor, so these tests
belong to the FDB-backed tier.

The guards under test, in the order the controller applies them:

1. idempotency: joining a current open-epoch member returns unchanged;
2. one-way (§4.3): a closed epoch never reopens — re-establishment is a clone;
3. pool alignment (§4.5): all members share one storage pool, because the
   group's subsystem-scoped operations rely on subsystems never spanning pools
   (P0-5);
4. the placement pin and epoch arithmetic shared with the create path:
   ``joined_seq = last_group_seq + 1``, so prior generations exclude the
   late joiner.

Nothing above the database is involved: no group snapshot is taken, so no
storage node is mocked.
"""

import pytest

from simplyblock_core.controllers import consistency_group_controller as cgc
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.lvol_model import LVol

CLUSTER_ID = "cg-dyn-cluster-1"
POOL_A = "cg-dyn-pool-a"
POOL_B = "cg-dyn-pool-b"
NODE = "cg-dyn-node-1"
LVS = "LVS_1"


@pytest.fixture
def db():
    db = DBController()
    if db.kv_store is None:
        pytest.skip("FoundationDB is not available")
    return db


def _write_lvol(db, uuid, pool=POOL_A, node=NODE, lvs=LVS):
    lvol = LVol()
    lvol.uuid = uuid
    lvol.pool_uuid = pool
    lvol.node_id = node
    lvol.lvs_name = lvs
    lvol.lvol_name = f"VOL_{uuid}"
    lvol.status = LVol.STATUS_ONLINE
    lvol.write_to_db(db.kv_store)
    return lvol


def _group_with_member(db, name="dyn-group"):
    """A pinned group with one founding member, the state a late joiner meets."""
    founder = _write_lvol(db, "dyn-lv-1")
    group = cgc.join_new_volume(CLUSTER_ID, founder, name)
    return db.get_consistency_group_by_id(group.get_id())


def test_late_join_opens_epoch_at_next_generation(db):
    """A volume labeled after creation joins with ``joined_seq`` pointing at
    the NEXT generation: history is not inherited (design §4.3, §4.5)."""
    group = _group_with_member(db)
    group.last_group_seq = 3
    group.write_to_db(db.kv_store)
    group = db.get_consistency_group_by_id(group.get_id())

    late = _write_lvol(db, "dyn-lv-2")
    cgc.join_existing_volume(group, late)

    fresh = db.get_consistency_group_by_id(group.get_id())
    assert fresh.members["dyn-lv-2"] == {"joined_seq": 4, "removed_seq": 0}
    assert not fresh.included_in_seq("dyn-lv-2", 3)
    assert fresh.included_in_seq("dyn-lv-2", 4)
    assert db.get_lvol_by_id("dyn-lv-2").group_id == group.get_id()


def test_late_join_is_idempotent_for_an_open_member(db):
    """A retried label reconcile must converge, not error or reset the epoch."""
    group = _group_with_member(db)
    late = _write_lvol(db, "dyn-lv-2")
    cgc.join_existing_volume(group, late)

    fresh = db.get_consistency_group_by_id(group.get_id())
    epoch_before = dict(fresh.members["dyn-lv-2"])
    cgc.join_existing_volume(fresh, db.get_lvol_by_id("dyn-lv-2"))

    fresh = db.get_consistency_group_by_id(group.get_id())
    assert fresh.members["dyn-lv-2"] == epoch_before


def test_late_join_refuses_a_closed_epoch(db):
    """One-way membership (design §4.3): a volume that left cannot rejoin by
    re-adding the label; re-establishment is a labeled clone."""
    group = _group_with_member(db)
    group.last_group_seq = 5
    group.write_to_db(db.kv_store)
    group = db.get_consistency_group_by_id(group.get_id())

    late = _write_lvol(db, "dyn-lv-2")
    cgc.join_existing_volume(group, late)
    fresh = db.get_consistency_group_by_id(group.get_id())
    fresh.last_group_seq = 7  # a generation contained the member, so the
    fresh.write_to_db(db.kv_store)  # detach closes rather than drops the epoch
    fresh = db.get_consistency_group_by_id(group.get_id())
    cgc.remove_member_from_group(fresh, "dyn-lv-2")

    fresh = db.get_consistency_group_by_id(group.get_id())
    assert fresh.members["dyn-lv-2"]["removed_seq"] == 7
    with pytest.raises(cgc.ConsistencyGroupError, match="one-way"):
        cgc.join_existing_volume(fresh, db.get_lvol_by_id("dyn-lv-2"))

    fresh = db.get_consistency_group_by_id(group.get_id())
    assert fresh.members["dyn-lv-2"]["removed_seq"] == 7, (
        "a refused rejoin must not touch the closed epoch")


def test_late_join_refuses_a_cross_pool_volume(db):
    """Pool alignment (design §4.5): the group's subsystem-scoped operations
    are safe because every subsystem belongs to one pool (P0-5); a member from
    another pool would put a foreign pool inside the group's freeze scope."""
    group = _group_with_member(db)
    stray = _write_lvol(db, "dyn-lv-2", pool=POOL_B)
    with pytest.raises(cgc.ConsistencyGroupError, match="pool"):
        cgc.join_existing_volume(group, stray)
    assert not stray.group_id
    fresh = db.get_consistency_group_by_id(group.get_id())
    assert "dyn-lv-2" not in (fresh.members or {})


def test_late_join_refuses_a_volume_off_the_pinned_store(db):
    """A late join never moves a volume: off the pinned node/LVS means refused
    (design §4.2, §4.5), exactly as the create path refuses it."""
    group = _group_with_member(db)
    stray = _write_lvol(db, "dyn-lv-2", node="cg-dyn-node-2", lvs="LVS_9")
    with pytest.raises(cgc.ConsistencyGroupError, match="pinned"):
        cgc.join_existing_volume(group, stray)
    assert not stray.group_id


def test_detach_clears_the_volume_group_pointer(db):
    """The label-removal detach closes the epoch AND clears ``lvol.group_id``,
    so the volume reads as a non-member (the migration webhook keys off it),
    while prior generations keep the closed epoch (design §8.2)."""
    group = _group_with_member(db)
    group.last_group_seq = 2
    group.write_to_db(db.kv_store)
    group = db.get_consistency_group_by_id(group.get_id())

    late = _write_lvol(db, "dyn-lv-2")
    cgc.join_existing_volume(group, late)
    fresh = db.get_consistency_group_by_id(group.get_id())
    fresh.last_group_seq = 4  # a generation contains the member: history exists
    fresh.write_to_db(db.kv_store)
    fresh = db.get_consistency_group_by_id(group.get_id())

    cgc.detach_existing_volume(fresh, "dyn-lv-2")

    assert db.get_lvol_by_id("dyn-lv-2").group_id == ""
    fresh = db.get_consistency_group_by_id(group.get_id())
    assert fresh.members["dyn-lv-2"]["removed_seq"] == 4
    assert fresh.included_in_seq("dyn-lv-2", 4)
    assert not fresh.included_in_seq("dyn-lv-2", 5)


def test_detach_is_idempotent(db):
    """Detaching a non-member (or repeating a detach) is a no-op, so a retried
    label reconcile converges on the remove side too."""
    group = _group_with_member(db)
    cgc.detach_existing_volume(group, "never-a-member")
    fresh = db.get_consistency_group_by_id(group.get_id())
    cgc.detach_existing_volume(fresh, "never-a-member")
    assert "never-a-member" not in (fresh.members or {})
