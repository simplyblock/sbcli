"""A member joining a consistency group while a generation is being taken.

A fail-back promote reconstitutes the group one clone at a time, and the
snapshot monitor takes the group's interval generation whenever it is due, so
the two meet. The generation used to finish by writing back the whole group
record it read before the take: the join that landed meanwhile was dropped, the
group attached its policy with one member short, that member never shipped a
group generation, and the next fail-over was refused for 30 minutes ("No group
generation ... is fully replicated for all 2 pending member(s)", bed run
20261117-1534, test 7).

Real FDB; the storage nodes (RPC) are mocked.
"""
import pytest

from simplyblock_core.controllers import consistency_group_controller as cgc
from tests.integration import test_sync_replication_active_triplet as triplet
from tests.integration import test_sync_replication_cadence_snapshots as cadence
from tests.integration import test_sync_replication_lvol_publish as publish
from tests.integration import test_sync_replication_lvs_stack as stack

db = stack.db
rpcs = stack.rpcs
hub = stack.hub
env = stack.env
no_sleep = triplet.no_sleep


@pytest.fixture()
def layout(db, rpcs, hub, env, no_sleep):
    """A group of one volume on n0's LVS, which n0 leads; a second volume on
    the same LVS, not yet a member."""
    cluster, owner, pool = cadence._plain_layout(db)
    rpcs.lead(owner)
    cadence._can_snapshot(rpcs, [owner])
    first = publish._volume(db, cluster, owner, pool, active_site="")
    joiner = publish._volume(db, cluster, owner, pool, active_site="")
    group = cadence._group(db, cluster, owner, [first])
    return owner, group, first, joiner


def _members(db, group):
    return db.get_consistency_group_by_id(group.get_id()).members


def test_a_member_joining_during_a_take_is_kept(db, rpcs, layout):
    owner, group, first, joiner = layout

    def _join_while_frozen(lvs_name, snapshots):
        cgc.add_member_to_group(db.get_consistency_group_by_id(group.get_id()), joiner)
        return True

    rpcs[owner.get_id()].bdev_lvol_snapshot_group.side_effect = _join_while_frozen

    assert cgc.create_group_snapshot_for_group(group) == (1, None)

    members = _members(db, group)
    assert set(members) == {first.get_id(), joiner.get_id()}, "the join is not overwritten"
    assert db.get_consistency_group_by_id(group.get_id()).last_group_seq == 1


def test_a_member_that_joined_during_a_take_starts_at_the_next_generation(db, rpcs, layout):
    """The take froze the members it planned; the joiner is not in it, so its
    epoch cannot claim this generation."""
    owner, group, first, joiner = layout

    def _join_while_frozen(lvs_name, snapshots):
        cgc.add_member_to_group(db.get_consistency_group_by_id(group.get_id()), joiner)
        return True

    rpcs[owner.get_id()].bdev_lvol_snapshot_group.side_effect = _join_while_frozen

    cgc.create_group_snapshot_for_group(group)

    assert _members(db, group)[joiner.get_id()]["joined_seq"] == 2
    assert _members(db, group)[first.get_id()]["joined_seq"] == 1


def test_two_joins_read_before_either_wrote_both_land(db, layout):
    """The fail-back reconstitutes a group one clone at a time; a join that
    read the group before the other wrote must not drop it."""
    owner, group, first, joiner = layout
    cluster = db.get_cluster_by_id(owner.cluster_id)
    pool = db.get_pool_by_id(first.pool_uuid)
    third = publish._volume(db, cluster, owner, pool, active_site="")
    read_by_one = db.get_consistency_group_by_id(group.get_id())
    read_by_other = db.get_consistency_group_by_id(group.get_id())

    cgc.add_member_to_group(read_by_one, joiner)
    cgc.add_member_to_group(read_by_other, third)

    assert set(_members(db, group)) == {first.get_id(), joiner.get_id(), third.get_id()}
