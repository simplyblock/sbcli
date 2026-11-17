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


#: An owner without sync replication has no site and no remote triplet.
_NON_SYNC = {"site": "", "remote_primary_node_id": "", "remote_secondary_node_id": "",
             "remote_tertiary_node_id": ""}


class TestActiveTriplet:

    @pytest.mark.parametrize("fields, node_ids, moving_to", [
        pytest.param({"lvs_active_site": ""}, ("p", "s", "t"), "", id="home-led"),
        pytest.param({"lvs_active_site": "site-a"}, ("p", "s", "t"), "", id="home-site-named"),
        pytest.param(_NON_SYNC, ("p", "s", "t"), "", id="non-sync"),
        pytest.param({"lvs_active_site": "site-b"}, ("rp", "rs", "rt"), "", id="led-from-the-other-site"),
        pytest.param({"lvs_active_site": "moving:site-b"}, ("p", "s", "t", "rp", "rs", "rt"), "site-b",
                     id="moving-away-offers-the-home-triplet-first"),
        pytest.param({"lvs_active_site": "moving:site-a"}, ("rp", "rs", "rt", "p", "s", "t"), "site-a",
                     id="moving-back-home-offers-the-remote-triplet-first"),
    ])
    def test_triplet_that_may_lead(self, fields, node_ids, moving_to):
        active = ops.lvs_active_triplet(_owner(**fields))
        assert active.node_ids == node_ids
        assert active.moving_to == moving_to
        assert active.grants_allowed is (not moving_to)

    def test_ftt1_home_triplet_has_two_members(self):
        assert ops.lvs_active_triplet(_owner(tertiary_node_id="")).node_ids == ("p", "s")

    @pytest.mark.parametrize("fields, home", [
        pytest.param({"lvs_active_site": ""}, True, id="unset"),
        pytest.param({"lvs_active_site": "site-a"}, True, id="own-site"),
        pytest.param(_NON_SYNC, True, id="non-sync"),
        pytest.param({"lvs_active_site": "site-b"}, False, id="other-site"),
        pytest.param({"lvs_active_site": "moving:site-b"}, False, id="moving-away"),
        pytest.param({"lvs_active_site": "moving:site-a"}, False, id="moving-home"),
    ])
    def test_led_from_home(self, fields, home):
        assert ops.lvs_led_from_home(_owner(**fields)) is home


_HOME = ("p", "s", "t")
_REMOTE = ("rp", "rs", "rt")


class TestHublvolExpected:

    @pytest.mark.parametrize("fields, passive", [
        pytest.param({"lvs_active_site": ""}, _REMOTE, id="unset"),
        pytest.param({"lvs_active_site": "site-a"}, _REMOTE, id="own-site"),
        pytest.param(_NON_SYNC, (), id="non-sync"),
    ])
    def test_home_led_expects_it_on_home_triplet_only(self, fields, passive):
        owner = _owner(**fields)
        assert all(ops.lvs_hublvol_expected(owner, nid) for nid in _HOME)
        assert not any(ops.lvs_hublvol_expected(owner, nid) for nid in passive)

    def test_other_site_led_expects_remote_triplet_only(self):
        owner = _owner(lvs_active_site="site-b")
        assert not any(ops.lvs_hublvol_expected(owner, nid) for nid in _HOME)
        assert all(ops.lvs_hublvol_expected(owner, nid) for nid in _REMOTE)

    @pytest.mark.parametrize("active_site", ["moving:site-b", "moving:site-a"])
    def test_move_in_flight_expects_it_on_both_triplets(self, active_site):
        owner = _owner(lvs_active_site=active_site)
        assert all(ops.lvs_hublvol_expected(owner, nid) for nid in _HOME + _REMOTE)

    @pytest.mark.parametrize("fields", [
        {"lvs_active_site": ""},
        {"lvs_active_site": "site-b"},
        _NON_SYNC,
    ], ids=["home-led", "remote-led", "non-sync"])
    def test_node_in_neither_triplet_keeps_it(self, fields):
        # A relocation target that the owner does not list in either triplet yet.
        assert ops.lvs_hublvol_expected(_owner(**fields), "new-host")


def _site_node(node_id, site):
    return SimpleNamespace(site=site, get_id=lambda: node_id)


class TestNonLeaderHubOk:
    """The restart's hub_ok: ``_non_leader_hub_ok`` combines
    ``lvs_hublvol_expected`` with the same-site check. It equals the same-site
    check alone wherever the leader is a member of the active triplet."""

    @pytest.mark.parametrize("sync, owner_fields, node, leader, expected", [
        (False, {"site": "", "remote_primary_node_id": "", "remote_secondary_node_id": "",
                 "remote_tertiary_node_id": ""},
         ("new-host", ""), ("p", ""), True),
        (True, {"lvs_active_site": ""}, ("s", "site-a"), ("p", "site-a"), True),
        (True, {"lvs_active_site": "site-b"}, ("s", "site-a"), ("rp", "site-b"), False),
        (True, {"lvs_active_site": "moving:site-b"}, ("s", "site-a"), ("p", "site-a"), True),
        (True, {"lvs_active_site": "moving:site-b"}, ("rs", "site-b"), ("p", "site-a"), False),
        (True, {"lvs_active_site": ""}, ("new-host", "site-a"), ("p", "site-a"), True),
    ], ids=["non-sync-relocation", "active-member", "passive-member", "moving-same-site",
            "moving-other-site", "relocation-new-host"])
    def test_equals_site_rule(self, sync, owner_fields, node, leader, expected):
        cluster = SimpleNamespace(sync_replication=sync)
        snode, leader_node = _site_node(*node), _site_node(*leader)
        owner = _owner(**owner_fields)
        assert ops._non_leader_hub_ok(cluster, snode, leader_node, owner) is expected
        assert ops._hublvol_same_site(cluster, snode, leader_node) is expected

    def test_passive_member_behind_local_leader_gets_none(self):
        # Cluster expansion rebuilds a new home member behind the owner even
        # when the LVS is led from the other site.
        cluster = SimpleNamespace(sync_replication=True)
        snode, leader_node = _site_node("s", "site-a"), _site_node("p", "site-a")
        owner = _owner(lvs_active_site="site-b")
        assert ops._hublvol_same_site(cluster, snode, leader_node) is True
        assert ops._non_leader_hub_ok(cluster, snode, leader_node, owner) is False


class TestInstanceRole:

    @pytest.mark.parametrize("node_id, leading, role", [
        ("p", True, "primary"), ("p", False, "secondary"),
        ("s", True, "secondary"), ("s", False, "secondary"),
        ("t", True, "tertiary"), ("t", False, "tertiary"),
        ("rp", True, "primary"), ("rp", False, "secondary"),
        ("rs", True, "secondary"), ("rs", False, "secondary"),
        ("rt", True, "tertiary"), ("rt", False, "tertiary"),
    ])
    def test_role_follows_slot_but_primary_must_lead(self, node_id, leading, role):
        assert ops.lvs_instance_role(_owner(), node_id, leading=leading) == role


class TestLeaderCandidatesWithoutSites:

    def test_callers_list_unchanged(self):
        nodes = [SimpleNamespace(site="", lvstore="LVS_1", get_id=lambda i=i: i) for i in ("p", "s")]
        candidates = ops._lvs_leader_candidates(nodes, "LVS_1")
        assert candidates.nodes == nodes
        assert candidates.grants_allowed
        assert candidates.owner_id is None and candidates.taker_id is None

    def test_duck_typed_nodes_without_site_attribute(self):
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

    def test_live_running_task_owns_move(self):
        assert ops.sync_promote_task_owns_move(_task())
        assert ops.sync_promote_task_owns_move(_task(status=JobSchedule.STATUS_SUSPENDED))
        assert ops.sync_promote_task_owns_move(_task(status=JobSchedule.STATUS_NEW))

    def test_done_or_canceled_owns_nothing(self):
        assert not ops.sync_promote_task_owns_move(_task(status=JobSchedule.STATUS_DONE))
        assert not ops.sync_promote_task_owns_move(_task(canceled=True))

    def test_task_whose_runner_died_owns_nothing(self):
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

    def test_non_sync_counts_every_ready_journal_copy(self):
        taker, rpc = self._taker({"jm_b0": False, "remote_jm_b1n1": True, "remote_jm_b2n1": True})
        assert ops._taker_jm_quorum_ok(taker) is True
        rpc.jc_get_jm_status.assert_called_once_with(7)
