"""Pure rules of the disaster fail-over (sync replication): the disaster
marker check, the zone predicate, the running-node refusal, the whole-site-loss
predicate of the cluster status and the activation leader builder. The
DB-backed flows are in tests/integration/test_sync_replication_disaster.py.
"""
from unittest.mock import MagicMock

import pytest

from simplyblock_core import storage_node_ops as ops
from simplyblock_core.controllers import sync_replication_controller as src
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCException
from simplyblock_core.services import storage_node_monitor

SITE_A, SITE_B = "site-a", "site-b"


def _node(node_id, site=SITE_A, status=StorageNode.STATUS_ONLINE, devices=(NVMeDevice.STATUS_ONLINE,),
          **fields):
    node = StorageNode()
    node.uuid = node_id
    node.site = site
    node.status = status
    node.mgmt_ip = f"10.0.0.{sum(map(ord, node_id)) % 250 + 1}"
    devs = []
    for i, st in enumerate(devices):
        dev = NVMeDevice()
        dev.uuid = f"{node_id}-d{i}"
        dev.status = st
        devs.append(dev)
    node.nvme_devices = devs
    for key, value in fields.items():
        setattr(node, key, value)
    return node


def _owner(**fields):
    return _node("a0", lvstore="LVS_1", secondary_node_id="a1", tertiary_node_id="a2",
                 remote_primary_node_id="b0", remote_secondary_node_id="b1", remote_tertiary_node_id="b2",
                 **fields)


def _cluster(lost_site="", state=""):
    cluster = Cluster()
    cluster.sync_replication = True
    cluster.lost_site, cluster.lost_site_state = lost_site, state
    return cluster


def _vol(vol_id, site=SITE_A, demoted=()):
    lvol = LVol()
    lvol.uuid = vol_id
    lvol.status = LVol.STATUS_ONLINE
    lvol.sync_active_site = site
    lvol.sync_demoted_sites = list(demoted)
    return lvol


class TestDisasterMoveProblems:

    def _problems(self, cluster, owner, volumes, expect="", request=("v1",)):
        return src.disaster_move_problems(cluster, owner, volumes, expect, SITE_B, SITE_A, request)

    def test_ok_when_fenced_and_only_the_request_is_served(self):
        vols = [_vol("v1"), _vol("v2", demoted=[SITE_A])]
        assert self._problems(_cluster(SITE_A, "done"), _owner(), vols) == []

    @pytest.mark.parametrize("lost,state", [("", ""), (SITE_A, "fencing"), (SITE_B, "done")])
    def test_the_fence_must_be_done_for_that_site(self, lost, state):
        (problem,) = self._problems(_cluster(lost, state), _owner(), [_vol("v1")])
        assert "fence" in problem

    def test_the_marker_must_be_unchanged(self):
        problems = self._problems(_cluster(SITE_A, "done"), _owner(lvs_active_site=SITE_B), [_vol("v1")])
        assert problems == [f"LVS LVS_1: lvs_active_site is {SITE_B!r}, expected ''"]

    def test_a_survivor_led_lvs_is_no_disaster_move(self):
        problems = self._problems(_cluster(SITE_A, "done"), _owner(lvs_active_site=SITE_B), [],
                                  expect=SITE_B)
        assert any("not from the lost site" in p for p in problems)

    def test_a_volume_outside_the_request_blocks_the_move(self):
        """The extra volume is still served on the lost site."""
        (problem,) = self._problems(_cluster(SITE_A, "done"), _owner(), [_vol("v1"), _vol("late")])
        assert "late" in problem


class TestSiteRecordedLost:
    """A demote of a site the monitor has authoritatively recorded lost (drained to
    `down` or beyond) is a no-op -- the site cannot be cleanly demoted and the forced
    promote fences it, so the node's planned flip can converge on an unplanned
    failover instead of looping on "planned gate failed" (#2026-10-16). A planned
    move (no record, or still `draining`) is not lost, so the planned gate runs."""

    @pytest.mark.parametrize("lost,state,expected", [
        (SITE_A, Cluster.LOST_SITE_DOWN, True),
        (SITE_A, Cluster.LOST_SITE_FENCING, True),
        (SITE_A, Cluster.LOST_SITE_DONE, True),
        (SITE_A, Cluster.LOST_SITE_DRAINING, False),   # drain not converged: still gated
        (SITE_A, "", False),                            # no record
        ("", "", False),                                # planned move
        (SITE_B, Cluster.LOST_SITE_DOWN, False),        # a different site is lost
    ])
    def test_only_an_authoritatively_lost_site_is_a_noop(self, lost, state, expected):
        assert src.site_recorded_lost(_cluster(lost, state), SITE_A) is expected


class TestZoneNotUp:

    def test_zone_not_up_counts_a_failed_device(self):
        nodes = [_node("a0", devices=(NVMeDevice.STATUS_FAILED,)),
                 _node("a1", devices=(NVMeDevice.STATUS_FAILED_AND_MIGRATED, NVMeDevice.STATUS_REMOVED)),
                 _node("b0", site=SITE_B, devices=(NVMeDevice.STATUS_UNAVAILABLE,))]
        assert src.zone_not_up(nodes, SITE_A) == ["dev:a0-d0"]

    def test_zone_not_up_ignores_a_suspended_node(self):
        nodes = [_node("a0", status=StorageNode.STATUS_SUSPENDED), _node("a1")]
        assert src.zone_not_up(nodes, SITE_A) == []


class TestSiteRunningNodes:

    @pytest.mark.parametrize("status,running", [
        (StorageNode.STATUS_ONLINE, True), (StorageNode.STATUS_DOWN, True),
        (StorageNode.STATUS_RESTARTING, True), (StorageNode.STATUS_IN_CREATION, True),
        (StorageNode.STATUS_OFFLINE, False), (StorageNode.STATUS_UNREACHABLE, False),
        (StorageNode.STATUS_REMOVED, False)])
    def test_states_in_which_spdk_may_run(self, status, running):
        nodes = [_node("a0", status=status), _node("b0", site=SITE_B)]
        assert bool(src.site_running_nodes(nodes, SITE_A)) is running


class TestSiteLost:

    def test_offline_and_data_plane_lost_nodes_lose_the_site(self):
        nodes = [_node("a0", status=StorageNode.STATUS_OFFLINE),
                 _node("a1", status=StorageNode.STATUS_UNREACHABLE),
                 _node("a2", status=StorageNode.STATUS_REMOVED)]
        assert storage_node_monitor._site_lost(nodes, {"a1": True})

    @pytest.mark.parametrize("status", [StorageNode.STATUS_UNREACHABLE, StorageNode.STATUS_SCHEDULABLE,
                                        StorageNode.STATUS_IN_SHUTDOWN])
    def test_a_transient_node_is_gone_only_by_the_data_plane(self, status):
        nodes = [_node("a0", status=StorageNode.STATUS_OFFLINE),
                 _node("a1", status=status, devices=(NVMeDevice.STATUS_FAILED,))]
        assert not storage_node_monitor._site_lost(nodes, {})
        assert not storage_node_monitor._site_lost(nodes, {"a1": False})
        assert storage_node_monitor._site_lost(nodes, {"a1": True})

    @pytest.mark.parametrize("status", [StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN,
                                        StorageNode.STATUS_RESTARTING])
    def test_a_node_that_serves_keeps_the_site(self, status):
        nodes = [_node("a0", status=StorageNode.STATUS_OFFLINE),
                 _node("a1", status=status, devices=(NVMeDevice.STATUS_FAILED,))]
        assert not storage_node_monitor._site_lost(nodes, {"a1": True})

    def test_no_counted_node_is_no_loss(self):
        assert not storage_node_monitor._site_lost(
            [_node("a0", status=StorageNode.STATUS_IN_CREATION)], {})


class TestActivationLeaderBuilder:

    @pytest.mark.parametrize("active,builder", [("", "a0"), (SITE_A, "a0"), (SITE_B, "b0"),
                                                ("moving:" + SITE_B, "a0")])
    def test_rule(self, active, builder):
        assert ops.activation_leader_builder_id(_owner(lvs_active_site=active)) == builder

    def test_outside_sync_replication_the_owner(self):
        owner = _owner()
        owner.site = ""
        assert ops.activation_leader_builder_id(owner) == "a0"


class TestFollowerRedirectOk:

    @staticmethod
    def _rpc(answer=None, error=None):
        rpc = MagicMock()
        if error is not None:
            rpc.bdev_lvol_get_lvstores.side_effect = error
        else:
            rpc.bdev_lvol_get_lvstores.return_value = answer
        return rpc

    @pytest.mark.parametrize("connected,redirect,expected", [
        (True, True, True), (False, True, False), (True, False, False), (False, False, False)])
    def test_a_follower_needs_both(self, connected, redirect, expected):
        rpc = self._rpc([{"lvs leadership": False, "connect_state": connected, "lvs_redirect": redirect}])
        assert ops.follower_redirect_ok(rpc, "LVS_1") is expected
        rpc.bdev_lvol_get_lvstores.assert_called_once_with("LVS_1")

    def test_a_leader_has_no_redirect_to_judge(self):
        rpc = self._rpc([{"lvs leadership": True, "connect_state": False, "lvs_redirect": False}])
        assert ops.follower_redirect_ok(rpc, "LVS_1") is None

    @pytest.mark.parametrize("answer", [
        [], None, "x", [{"lvs leadership": False}],
        [{"lvs leadership": False, "connect_state": 1, "lvs_redirect": True}],
        [{"connect_state": True, "lvs_redirect": True}],
        [{"lvs leadership": "yes", "connect_state": True, "lvs_redirect": True}]])
    def test_what_cannot_be_told_is_none(self, answer):
        assert ops.follower_redirect_ok(self._rpc(answer), "LVS_1") is None

    def test_a_failing_rpc_is_none(self):
        assert ops.follower_redirect_ok(self._rpc(error=RPCException("down")), "LVS_1") is None


class TestUnwiredFollower:

    def _lvol(self):
        lvol = LVol()
        lvol.uuid, lvol.lvs_name, lvol.nqn, lvol.ns_id = "v1", "LVS_1", "nqn", 1
        return lvol

    def test_only_non_optimized_paths_are_checked_once(self):
        rpc = MagicMock()
        rpc.bdev_lvol_get_lvstores.return_value = [{"lvs leadership": False, "connect_state": False,
                                                     "lvs_redirect": False}]
        node, memo = _node("a1"), {}
        assert not ops._unwired_follower(rpc, self._lvol(), node, "optimized", memo)
        assert not ops._unwired_follower(rpc, self._lvol(), node, "inaccessible", memo)
        rpc.bdev_lvol_get_lvstores.assert_not_called()
        assert ops._unwired_follower(rpc, self._lvol(), node, "non_optimized", memo)
        assert ops._unwired_follower(rpc, self._lvol(), node, "non_optimized", memo)
        assert rpc.bdev_lvol_get_lvstores.call_count == 1

    def test_unknown_changes_nothing(self):
        rpc = MagicMock()
        rpc.bdev_lvol_get_lvstores.side_effect = RPCException("x")
        assert not ops._unwired_follower(rpc, self._lvol(), _node("a1"), "non_optimized", {})


class TestRuleInSiteFailover:
    """The site rule keeps the active triplet's secondary optimized while its
    primary is OFFLINE, without any caller's ``promoted``."""

    def _state(self, node, primary_status, active=""):
        cluster = _cluster()
        owner = _owner(lvs_active_site=active)
        lvol = _vol("v1", site=active or SITE_A)
        ctx = ops.SyncAnaContext(cluster, owner, primary_status)
        return ops.lvol_ana_state(lvol, node, "non_optimized", ctx=ctx)

    def test_primary_offline_the_secondary_is_optimized(self):
        assert self._state(_node("a1"), StorageNode.STATUS_OFFLINE) == "optimized"

    @pytest.mark.parametrize("status", [StorageNode.STATUS_RESTARTING, StorageNode.STATUS_ONLINE,
                                        StorageNode.STATUS_UNREACHABLE, ""])
    def test_otherwise_it_stays_non_optimized(self, status):
        assert self._state(_node("a1"), status) == "non_optimized"

    def test_the_tertiary_is_unchanged(self):
        assert self._state(_node("a2"), StorageNode.STATUS_OFFLINE) == "non_optimized"

    def test_a_remote_led_lvs_uses_its_active_triplet(self):
        assert self._state(_node("b1", site=SITE_B), StorageNode.STATUS_OFFLINE,
                           active=SITE_B) == "optimized"
        assert self._state(_node("a1"), StorageNode.STATUS_OFFLINE, active=SITE_B) == "inaccessible"

    def test_a_move_in_flight_opens_nothing(self):
        assert self._state(_node("a1"), StorageNode.STATUS_OFFLINE,
                           active="moving:" + SITE_B) == "inaccessible"
