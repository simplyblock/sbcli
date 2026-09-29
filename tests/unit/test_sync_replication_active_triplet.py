"""Pure rules of the active triplet of a sync-replication LVS: which triplet
may lead it (and be granted it), the kernel role of every instance, the
promote-task ownership of a move, and the local journal quorum of a grant.
The flows against the real database (lookups, recovery, restart, the fenced
transfer, the move reconciler) are in
tests/integration/test_sync_replication_active_triplet.py.
"""
import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from simplyblock_core import storage_node_ops as ops
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode


def _owner(**fields):
    node = StorageNode()
    node.uuid = "p"
    node.site = "site-a"
    node.lvstore = "LVS_1"
    node.secondary_node_id = "s"
    node.tertiary_node_id = "t"
    node.remote_primary_node_id = "rp"
    node.remote_secondary_node_id = "rs"
    node.remote_tertiary_node_id = "rt"
    for key, value in fields.items():
        setattr(node, key, value)
    return node


class TestActiveTriplet:

    @pytest.mark.parametrize("active_site", ["", "site-a"])
    def test_home_led(self, active_site):
        active = ops.lvs_active_triplet(_owner(lvs_active_site=active_site))
        assert active.node_ids == ("p", "s", "t")
        assert active.grants_allowed and active.moving_to == ""

    def test_led_from_the_other_site(self):
        active = ops.lvs_active_triplet(_owner(lvs_active_site="site-b"))
        assert active.node_ids == ("rp", "rs", "rt")
        assert active.grants_allowed

    def test_moving_away_from_home_offers_both_source_first_and_grants_nothing(self):
        active = ops.lvs_active_triplet(_owner(lvs_active_site="moving:site-b"))
        assert active.node_ids == ("p", "s", "t", "rp", "rs", "rt")
        assert active.moving_to == "site-b" and not active.grants_allowed

    def test_moving_back_home_offers_the_remote_triplet_first(self):
        active = ops.lvs_active_triplet(_owner(lvs_active_site="moving:site-a"))
        assert active.node_ids == ("rp", "rs", "rt", "p", "s", "t")
        assert active.moving_to == "site-a" and not active.grants_allowed

    def test_ftt1_home_triplet_has_two_members(self):
        assert ops.lvs_active_triplet(_owner(tertiary_node_id="")).node_ids == ("p", "s")

    def test_non_sync_owner_is_always_home_led(self):
        owner = _owner(site="", remote_primary_node_id="", remote_secondary_node_id="",
                       remote_tertiary_node_id="")
        assert ops.lvs_active_triplet(owner).node_ids == ("p", "s", "t")
        assert ops.lvs_led_from_home(owner)

    @pytest.mark.parametrize("active_site, home", [
        ("", True), ("site-a", True), ("site-b", False), ("moving:site-b", False),
        ("moving:site-a", False)])
    def test_led_from_home(self, active_site, home):
        assert ops.lvs_led_from_home(_owner(lvs_active_site=active_site)) is home


class TestInstanceRole:

    @pytest.mark.parametrize("node_id, leading, role", [
        ("p", True, "primary"), ("p", False, "secondary"),
        ("s", True, "secondary"), ("s", False, "secondary"),
        ("t", True, "tertiary"), ("t", False, "tertiary"),
        ("rp", True, "primary"), ("rp", False, "secondary"),
        ("rs", True, "secondary"), ("rs", False, "secondary"),
        ("rt", True, "tertiary"), ("rt", False, "tertiary"),
    ])
    def test_position_in_the_own_triplet_never_primary_unless_leading(self, node_id, leading, role):
        assert ops.lvs_instance_role(_owner(), node_id, leading=leading) == role

    def test_ftt1_has_no_tertiary_slot(self):
        owner = _owner(tertiary_node_id="")
        assert ops.lvs_instance_role(owner, "s", leading=False) == "secondary"
        assert ops.lvs_instance_role(owner, "p", leading=True) == "primary"


class TestLeaderCandidatesWithoutSites:

    def test_the_callers_list_unchanged(self):
        nodes = [SimpleNamespace(site="", lvstore="LVS_1", get_id=lambda i=i: i) for i in ("p", "s")]
        candidates = ops._lvs_leader_candidates(nodes, "LVS_1")
        assert candidates.nodes == nodes
        assert candidates.grants_allowed
        assert candidates.owner_id is None and candidates.taker_id is None

    def test_duck_typed_nodes_without_a_site_attribute(self):
        nodes = [SimpleNamespace(lvstore="LVS_1", get_id=lambda: "p")]
        assert ops._lvs_leader_candidates(nodes, "LVS_1").nodes == nodes


def _task(**fields):
    task = JobSchedule()
    task.function_name = JobSchedule.FN_SYNC_PROMOTE
    task.status = JobSchedule.STATUS_RUNNING
    task.updated_at = str(datetime.datetime.now(datetime.UTC))
    for key, value in fields.items():
        setattr(task, key, value)
    return task


class TestPromoteTaskOwnership:

    def test_a_live_running_task_owns_the_move(self):
        assert ops.sync_promote_task_owns_move(_task())
        assert ops.sync_promote_task_owns_move(_task(status=JobSchedule.STATUS_SUSPENDED))
        assert ops.sync_promote_task_owns_move(_task(status=JobSchedule.STATUS_NEW))

    def test_done_or_canceled_owns_nothing(self):
        assert not ops.sync_promote_task_owns_move(_task(status=JobSchedule.STATUS_DONE))
        assert not ops.sync_promote_task_owns_move(_task(canceled=True))

    def test_a_task_whose_runner_died_owns_nothing(self):
        stale = datetime.datetime.now(datetime.UTC) - datetime.timedelta(
            seconds=ops.constants.TASK_LEASE_TTL_SEC + 60)
        assert not ops.sync_promote_task_owns_move(_task(updated_at=str(stale)))

    def test_other_task_kinds_are_not_promote_owners(self):
        assert not ops.sync_promote_task_owns_move(_task(function_name=JobSchedule.FN_PORT_ALLOW))


class TestTakerJournalQuorum:

    def _taker(self, status):
        rpc = MagicMock()
        rpc.jc_get_jm_status.return_value = status
        return SimpleNamespace(jm_vuid=7, get_id=lambda: "taker", rpc_client=lambda **kwargs: rpc), rpc

    def test_sync_needs_two_ready_local_copies(self):
        taker, rpc = self._taker({"remote_xs_jm_a1n1": True, "remote_xs_jm_a2n1": True,
                                  "jm_b0": False, "remote_jm_b1n1": False})
        assert ops._taker_jm_quorum_ok(taker, 42, sync=True) is False
        rpc.jc_get_jm_status.assert_called_once_with(42)

    def test_sync_accepts_two_local_without_any_remote(self):
        taker, _ = self._taker({"jm_b0": True, "remote_jm_b1n1": True,
                                "remote_xs_jm_a1n1": False, "remote_xs_jm_a2n1": False})
        assert ops._taker_jm_quorum_ok(taker, 42, sync=True) is True

    def test_non_sync_counts_every_ready_copy_of_the_takers_own_journal(self):
        taker, rpc = self._taker({"jm_b0": False, "remote_jm_b1n1": True, "remote_jm_b2n1": True})
        assert ops._taker_jm_quorum_ok(taker) is True
        rpc.jc_get_jm_status.assert_called_once_with(7)


class TestBeginMoveArguments:

    @pytest.mark.parametrize("site", ["", "moving:site-b"])
    def test_an_invalid_target_site_is_rejected(self, site):
        with pytest.raises(ValueError):
            ops.begin_lvs_move("p", site, expect="")
