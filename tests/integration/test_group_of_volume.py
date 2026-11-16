"""The consistency group of a volume, as the replication task and the cutover
runner resolve it, when several groups follow one replication policy.

A standalone group attaches to an ordinary policy (attach_group_policy), and
any number of them can share it: after the fail-over in bed run
20261113-2112-ebs-splitdr, Gitea's and WordPress's groups on site A both
followed the policy toward site B. Both runners looked the group up through
the policy, as if each policy owned one group; the lookup found two, every
replication task logged "Could not resolve the consistency group of ...:
Multiple values present", and the volume counted as in no group: the cutover
hold no longer exempted the members of one group from each other.

A member carries its group (group_id), so that is what decides. The policy
only answers for a policy that owns its group (consistency_group), the
legacy path.

Real FDB: the groups, the policy and the volumes are seeded.
"""
import uuid

import pytest

from simplyblock_core.controllers import consistency_group_controller as cgc
from simplyblock_core.models.replication import ConsistencyGroup
from simplyblock_core.services import snapshot_replication as sr
from simplyblock_core.services import tasks_runner_replication_final as final
from tests.integration import test_deferred_peer_placement as placement

db = placement.db

RESOLVERS = [sr._group_id_for_lvol, final._group_id_for_lvol]


def _group(db, cluster, node, name, policy_id):
    group = ConsistencyGroup()
    group.uuid = str(uuid.uuid4())
    group.cluster_id = cluster.get_id()
    group.group_name = name
    group.node_id = node.get_id()
    group.lvs_name = node.lvstore
    group.policy_id = policy_id
    group.write_to_db(db.kv_store)
    return group


def _two_groups_on_one_policy(db):
    cluster, node, policy, (gitea, wordpress, loose) = placement._site_a(db, members=3)
    groups = {}
    for name, vol in (("gitea", gitea), ("wordpress", wordpress)):
        groups[name] = _group(db, cluster, node, name, policy.get_id())
        placement._with(db, vol, group_id=groups[name].get_id(),
                        replication_policy_id=policy.get_id())
    loose = placement._with(db, loose, replication_policy_id=policy.get_id())
    return groups, {"gitea": db.get_lvol_by_id(gitea.get_id()),
                    "wordpress": db.get_lvol_by_id(wordpress.get_id()),
                    "loose": loose}, policy


@pytest.mark.parametrize("resolve", RESOLVERS, ids=["replication", "cutover"])
def test_a_member_resolves_to_its_own_group(db, resolve, caplog):
    groups, vols, _ = _two_groups_on_one_policy(db)

    assert resolve(vols["gitea"]) == groups["gitea"].get_id()
    assert resolve(vols["wordpress"]) == groups["wordpress"].get_id()
    assert "Could not resolve the consistency group" not in caplog.text


@pytest.mark.parametrize("resolve", RESOLVERS, ids=["replication", "cutover"])
def test_a_volume_in_no_group_under_a_shared_policy_is_in_none(db, resolve, caplog):
    _, vols, _ = _two_groups_on_one_policy(db)

    assert resolve(vols["loose"]) == ""
    assert "Could not resolve the consistency group" not in caplog.text


@pytest.mark.parametrize("resolve", RESOLVERS, ids=["replication", "cutover"])
def test_a_policy_that_owns_its_group_still_answers_for_an_unstamped_volume(db, resolve):
    """The legacy path: a policy declared with consistency_group creates its
    group, and a volume recorded before group_id existed resolves through it."""
    cluster, node, policy, (vol,) = placement._site_a(db)
    policy.consistency_group = True
    policy.write_to_db(db.kv_store)
    group = cgc.create_group_for_policy(policy)
    vol = placement._with(db, vol, replication_policy_id=policy.get_id())

    assert resolve(vol) == group.get_id()
