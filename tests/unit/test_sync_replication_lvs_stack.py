"""Pure rules of the LVS stack on a sync-replication remote triplet: the
triplet a node belongs to, the leader candidates of a non-leader rebuild, the
hublvol site rule, the pending marks kept with the refs, and the probes that
tell a torn-down or not-yet-built distrib from a failure. The flows against
the real database are in tests/integration/test_sync_replication_lvs_stack.py.
"""
import logging
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, create_autospec

import pytest

from simplyblock_core import constants, distr_controller
from simplyblock_core import storage_node_ops as ops
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import (
    RPCClient,
    RPCConnectionError,
    RPCException,
    RPCRemoteError,
)


def _owner(**fields):
    node = StorageNode()
    node.uuid = "p"
    node.site = "site-a"
    node.secondary_node_id = "s"
    node.tertiary_node_id = "t"
    node.remote_primary_node_id = "rp"
    node.remote_secondary_node_id = "rs"
    node.remote_tertiary_node_id = "rt"
    for key, value in fields.items():
        setattr(node, key, value)
    return node


class TestLvsTripletOf:

    def test_home_members_get_the_home_triplet(self):
        owner = _owner()
        for node_id in ("p", "s", "t"):
            assert ops.lvs_triplet_of(owner, node_id) == ("p", "s", "t")

    def test_remote_members_get_the_remote_triplet(self):
        owner = _owner()
        for node_id in ("rp", "rs", "rt"):
            assert ops.lvs_triplet_of(owner, node_id) == ("rp", "rs", "rt")

    def test_without_refs_everything_is_home(self):
        owner = _owner(remote_primary_node_id="", remote_secondary_node_id="",
                       remote_tertiary_node_id="")
        assert ops.lvs_triplet_of(owner, "x") == ("p", "s", "t")
        assert ops.lvs_triplet_of(owner, "") == ("p", "s", "t")

    def test_ftt1_home_triplet_has_no_tertiary(self):
        owner = _owner(tertiary_node_id="")
        assert ops.lvs_triplet_of(owner, "s") == ("p", "s", "")


class TestRebuildCandidates:
    """The leader candidates of a non-leader rebuild are the LVS's ACTIVE
    triplet (lvs_active_triplet): home, remote, or both while it moves."""

    def test_home_triplet_without_the_rebuilding_node(self):
        owner = _owner()
        assert ops.non_leader_rebuild_candidates(owner, "rs") == ["p", "s", "t"]
        assert ops.non_leader_rebuild_candidates(owner, "s") == ["p", "t"]

    def test_led_from_the_remote_site_is_the_remote_triplet(self):
        owner = _owner(lvs_active_site="site-b")
        assert ops.non_leader_rebuild_candidates(owner, "s") == ["rp", "rs", "rt"]
        assert ops.non_leader_rebuild_candidates(owner, "p") == ["rp", "rs", "rt"]
        assert ops.non_leader_rebuild_candidates(owner, "rs") == ["rp", "rt"]

    def test_home_site_named_explicitly_is_the_home_triplet(self):
        owner = _owner(lvs_active_site="site-a")
        assert ops.non_leader_rebuild_candidates(owner, "rs") == ["p", "s", "t"]

    def test_moving_offers_both_triplets_source_first(self):
        owner = _owner(lvs_active_site="moving:site-b")
        assert ops.non_leader_rebuild_candidates(owner, "t") == ["p", "s", "rp", "rs", "rt"]
        owner = _owner(lvs_active_site="moving:site-a")
        assert ops.non_leader_rebuild_candidates(owner, "t") == ["rp", "rs", "rt", "p", "s"]

    def test_empty_slots_are_skipped(self):
        owner = _owner(tertiary_node_id="")
        assert ops.non_leader_rebuild_candidates(owner, "rp") == ["p", "s"]


class TestHublvolSiteRule:

    def test_sync_cluster_only_within_a_site(self):
        cluster = SimpleNamespace(sync_replication=True)
        a1, a2, b1 = (SimpleNamespace(site=s) for s in ("site-a", "site-a", "site-b"))
        assert ops._hublvol_same_site(cluster, a1, a2) is True
        assert ops._hublvol_same_site(cluster, a1, b1) is False

    def test_non_sync_cluster_is_never_restricted(self):
        cluster = SimpleNamespace(sync_replication=False)
        assert ops._hublvol_same_site(cluster, SimpleNamespace(site=""), SimpleNamespace(site="x")) is True


class TestPendingMarks:

    def test_new_members_become_pending(self):
        node = StorageNode()
        ops._set_remote_triplet_refs(node, ("b1", "b2", "b3"))
        assert ops.remote_triplet_refs(node) == ("b1", "b2", "b3")
        assert node.remote_instances_pending == ["b1", "b2", "b3"]

    def test_an_unchanged_member_is_not_marked_again(self):
        node = StorageNode()
        node.remote_primary_node_id, node.remote_secondary_node_id, node.remote_tertiary_node_id = (
            "b1", "b2", "b3")
        ops._set_remote_triplet_refs(node, ("b1", "b4", "b3"))
        assert node.remote_instances_pending == ["b4"]

    def test_a_member_leaving_the_triplet_is_unmarked(self):
        node = StorageNode()
        ops._set_remote_triplet_refs(node, ("b1", "b2", "b3"))
        node.remote_instances_pending = ["b2"]
        ops._set_remote_triplet_refs(node, ("b1", "b4", "b3"))
        assert node.remote_instances_pending == ["b4"]

    def test_clearing_the_refs_clears_the_marks(self):
        node = StorageNode()
        ops._set_remote_triplet_refs(node, ("b1", "b2", "b3"))
        ops._set_remote_triplet_refs(node, ("", "", ""))
        assert node.remote_instances_pending == []


def _stack():
    return [{"type": "bdev_distr", "name": "d1"}, {"type": "bdev_distr", "name": "d2"},
            {"type": "bdev_raid", "name": "raid0_1"}, {"type": "bdev_lvstore", "name": "LVS_1"}]


class TestRemoteInstanceLeftovers:

    def _peer(self, bdev_get):
        peer = MagicMock()
        peer.get_id.return_value = "b1"
        peer.rpc_client.return_value.bdev_get.side_effect = bdev_get
        return peer

    def test_all_gone(self):
        owner = SimpleNamespace(lvstore_stack=_stack())
        assert ops._remote_instance_leftovers(self._peer(lambda name: None), owner) == []

    def test_a_surviving_bdev_is_reported(self):
        owner = SimpleNamespace(lvstore_stack=_stack())
        peer = self._peer(lambda name: {"name": name} if name == "d2" else None)
        assert ops._remote_instance_leftovers(peer, owner) == ["d2"]

    def test_a_failed_probe_counts_as_present(self):
        def _probe(name):
            if name == "raid0_1":
                raise RPCException("connection error")
        owner = SimpleNamespace(lvstore_stack=_stack())
        assert ops._remote_instance_leftovers(self._peer(_probe), owner) == ["raid0_1"]

    def test_the_lvstore_entry_is_not_probed(self):
        owner = SimpleNamespace(lvstore_stack=_stack())
        peer = self._peer(lambda name: None)
        ops._remote_instance_leftovers(peer, owner)
        probed = [c.args[0] for c in peer.rpc_client.return_value.bdev_get.call_args_list]
        assert probed == ["d1", "d2", "raid0_1"]


class TestDistribNotBuilt:

    def _node(self):
        node = MagicMock()
        node.get_id.return_value = "b1"
        return node

    def test_no_such_device_means_not_built(self):
        rpc = MagicMock()
        rpc.bdev_get.return_value = None
        assert distr_controller._distrib_not_built(rpc, self._node(), "d1") is True

    def test_an_existing_distrib_keeps_the_failure(self):
        rpc = MagicMock()
        rpc.bdev_get.return_value = {"name": "d1"}
        assert distr_controller._distrib_not_built(rpc, self._node(), "d1") is False

    def test_an_unreachable_node_keeps_the_failure(self):
        rpc = MagicMock()
        rpc.bdev_get.side_effect = RPCException("connection error")
        assert distr_controller._distrib_not_built(rpc, self._node(), "d1") is False


class TestLvsLeadershipVerdict:
    """_lvs_leadership_verdict reads each member's get_lvstore: a dict that
    reports the leadership leads, no lvstore or a failed probe is silent."""

    @staticmethod
    def _member(node_id, site="A", answer=None, error=None):
        rpc = create_autospec(RPCClient, instance=True)
        if error is not None:
            rpc.get_lvstore.side_effect = error
        else:
            rpc.get_lvstore.return_value = answer
        node = StorageNode()
        node.uuid, node.site = node_id, site
        node.rpc_client = MagicMock(return_value=rpc)
        return node, rpc

    def test_each_answer_is_classified(self):
        members, rpcs = {}, {}
        for node_id, kwargs in (
                ("leads", {"answer": {"name": "LVS_1", "lvs leadership": True}}),
                ("follows", {"answer": {"name": "LVS_1", "lvs leadership": False}}),
                ("no_lvs", {"answer": None}),
                ("remote_err", {"error": RPCRemoteError(message="busy", code=-16)}),
                ("down", {"error": RPCConnectionError("down")})):
            members[node_id], rpcs[node_id] = self._member(node_id, **kwargs)
        verdict = ops._lvs_leadership_verdict("LVS_1", members)
        assert verdict.leading == ["leads"]
        assert verdict.silent == ["no_lvs", "remote_err", "down"]
        assert verdict.skipped == set()
        for rpc in rpcs.values():
            rpc.get_lvstore.assert_called_once_with("LVS_1")

    def test_a_skipped_site_is_not_probed(self):
        lost, lost_rpc = self._member("b0", site="B", answer={"lvs leadership": True})
        here, _rpc = self._member("a0", answer={"lvs leadership": True})
        verdict = ops._lvs_leadership_verdict("LVS_1", {"b0": lost, "a0": here}, skip_site="B")
        assert (verdict.leading, verdict.silent, verdict.skipped) == (["a0"], [], {"b0"})
        lost_rpc.get_lvstore.assert_not_called()


def _member(node_id):
    node = StorageNode()
    node.uuid = node_id
    return node


class TestNonLeaderRole:
    """The role a non-leader instance is built in: the caller's, else its
    position in its own triplet (lvs_triplet_of)."""

    @pytest.mark.parametrize("node_id, role", [
        ("s", "secondary"), ("t", "tertiary"),               # home triplet
        ("rp", "secondary"), ("rs", "secondary"), ("rt", "tertiary"),   # remote triplet
        ("p", "secondary"),                                   # the owner rebuilt as a non-leader
    ])
    def test_by_position(self, node_id, role):
        assert ops._non_leader_role(_member(node_id), _owner()) == role

    @pytest.mark.parametrize("role", ["secondary", "tertiary"])
    def test_the_named_role_wins(self, role):
        assert ops._non_leader_role(_member("rt"), _owner(), role) == role

    def test_a_leader_role_is_refused(self):
        with pytest.raises(ValueError):
            ops._non_leader_role(_member("s"), _owner(), "primary")


class TestLvolBdevRegistered:
    """By uuid, else by <lvs>/<bdev>; a probe that raises counts as absent."""

    @staticmethod
    def _lvol():
        lvol = SimpleNamespace(lvol_uuid="u-1", lvs_name="LVS_1", lvol_bdev="LVOL_1")
        return lvol

    def test_a_uuid_hit(self):
        rpc = create_autospec(RPCClient, instance=True)
        rpc.bdev_get.return_value = {"name": "u-1"}
        assert ops._lvol_bdev_registered(rpc, self._lvol()) is True
        rpc.bdev_get.assert_called_once_with("u-1")

    def test_absent_by_uuid_then_found_by_alias(self):
        rpc = create_autospec(RPCClient, instance=True)
        rpc.bdev_get.side_effect = [None, {"name": "LVS_1/LVOL_1"}]
        assert ops._lvol_bdev_registered(rpc, self._lvol()) is True
        assert [c.args[0] for c in rpc.bdev_get.call_args_list] == ["u-1", "LVS_1/LVOL_1"]

    def test_both_probes_raising_is_absent(self):
        rpc = create_autospec(RPCClient, instance=True)
        rpc.bdev_get.side_effect = [RPCRemoteError(message="busy", code=-16), RPCConnectionError("down")]
        assert ops._lvol_bdev_registered(rpc, self._lvol()) is False


class TestSendLvolAna:
    """A failed per-NIC flip is logged; the other NICs are still flipped."""

    def test_a_failed_nic_does_not_stop_the_next(self):
        rpc = create_autospec(RPCClient, instance=True)
        rpc.nvmf_subsystem_listener_set_ana_state.side_effect = [
            RPCRemoteError(message="boom", code=-1), True]
        node = MagicMock()
        node.site = "site-a"
        node.active_tcp = True
        node.rpc_client.return_value = rpc
        node.get_lvol_subsys_port.return_value = 4402
        node.data_nics = [SimpleNamespace(ip4_address="10.0.0.1", trtype="TCP"),
                          SimpleNamespace(ip4_address="10.0.0.2", trtype="TCP")]
        lvol = SimpleNamespace(nqn="nqn.v1", fabric="tcp", ns_id=3, lvs_name="LVS_1")
        ops._send_lvol_ana(lvol, node, "inaccessible")
        assert [(c.args[1], c.kwargs["ana"], c.kwargs["anagrpid"])
                for c in rpc.nvmf_subsystem_listener_set_ana_state.call_args_list] == [
            ("10.0.0.1", "inaccessible", 3), ("10.0.0.2", "inaccessible", 3)]


class TestFenceUnblockWarning:
    """_LvsFence.warn_if_unblocking_a_non_leader reads get_lvstore: only a
    peer whose lvstore answers "not leading" is warned about."""

    @staticmethod
    def _fence():
        lvs_node = MagicMock()
        lvs_node.lvstore = "LVS_1"
        lvs_node.get_lvol_subsys_port.return_value = 4402
        return ops._LvsFence(lvs_node, MagicMock(), None)

    @staticmethod
    def _peer(answer=None, error=None):
        rpc = create_autospec(RPCClient, instance=True)
        if error is not None:
            rpc.get_lvstore.side_effect = error
        else:
            rpc.get_lvstore.return_value = answer
        peer = MagicMock()
        peer.get_id.return_value = "peer-1234"
        peer.rpc_client.return_value = rpc
        return peer, rpc

    @pytest.mark.parametrize("answer, warned", [
        ({"name": "LVS_1", "lvs leadership": False}, True),
        ({"name": "LVS_1", "lvs leadership": True}, False),
        (None, False),
    ])
    def test_the_answer_decides(self, caplog, answer, warned):
        peer, rpc = self._peer(answer)
        with caplog.at_level(logging.ERROR):
            self._fence().warn_if_unblocking_a_non_leader(peer)
        rpc.get_lvstore.assert_called_once_with("LVS_1")
        assert ("NO LONGER leader" in caplog.text) is warned

    def test_a_failed_probe_is_silent(self, caplog):
        peer, _rpc = self._peer(error=RPCConnectionError("down"))
        with caplog.at_level(logging.ERROR):
            self._fence().warn_if_unblocking_a_non_leader(peer)
        assert "NO LONGER leader" not in caplog.text


# ---------------------------------------------------------------------------
# the fence of a leadership hand-off: release, abort, RPC bounds, grant budget
# ---------------------------------------------------------------------------

def _lvs_owner():
    return _owner(cluster_id="cl", lvstore="LVS_1", jm_vuid=1, lvol_subsys_port=4402,
                  lvstore_ports={"LVS_1": {"lvol_subsys_port": 4402, "hublvol_port": 4403}})


class _Recorder:
    """An RPC client that records ``(client kwargs, method)`` per call and
    answers every call with ``answers.get(method, True)``."""

    def __init__(self, kwargs, calls, answers):
        self._kwargs, self._calls, self._answers = kwargs, calls, answers

    def __getattr__(self, method):
        def _call(*args, **kwargs):
            self._calls.append((self._kwargs, method))
            answer = self._answers.get(method, True)
            if isinstance(answer, Exception):
                raise answer
            return answer
        return _call


def _taker(calls, answers=None, *, jm_vuid=7):
    """The taker of a hand-off (``fence.rpc_node``); its RPC clients record."""
    node = _owner(uuid="p", cluster_id="cl", jm_vuid=jm_vuid)
    node.rpc_client = MagicMock(side_effect=lambda **kw: _Recorder(kw, calls, answers or {}))
    return node


class TestFenceRelease:

    @pytest.fixture(autouse=True)
    def _regular_cluster(self, monkeypatch):
        cluster = Cluster()
        cluster.uuid = "cl"
        ops._remember_cluster_arbitration(cluster)
        self.events = MagicMock(name="tcp_ports_events")
        monkeypatch.setattr(ops, "tcp_ports_events", self.events)
        self.clear_budget = MagicMock(name="clear_budget")
        monkeypatch.setattr(ops.rpc_budget, "clear_budget", self.clear_budget)

    @staticmethod
    def _peer(name, log):
        peer = _owner(uuid=name, cluster_id="cl")
        rpc = create_autospec(RPCClient, instance=True, name=f"rpc-{name}")
        rpc.get_lvstore.return_value = {"name": "LVS_1", "lvs leadership": True}
        rpc.nvmf_port_unblock.side_effect = lambda port: log.append(("unblock", name, port))
        peer.rpc_client = MagicMock(return_value=rpc)
        return peer, rpc

    def test_an_abort_releases_every_peer_before_its_own_step(self):
        log: list = []
        peers = [self._peer(name, log)[0] for name in ("s", "t", "rs")]
        fence = ops._LvsFence(_lvs_owner(), _taker([]), None,
                              on_abort=lambda: log.append(("on_abort",)))
        fence.blocked_peers.extend(peers)
        for peer in peers:
            fence.block_started[peer.get_id()] = time.monotonic()
        with pytest.raises(ops.LeadershipTransferError, match="Abort restart: lost"):
            fence.abort("lost")
        assert log == [("unblock", "s", 4402), ("unblock", "t", 4402), ("unblock", "rs", 4402),
                       ("on_abort",)]
        assert fence.blocked_peers == []

    def test_a_regular_cluster_unblocks_without_asking_the_ha_state(self):
        log: list = []
        (s, s_rpc), (t, t_rpc) = self._peer("s", log), self._peer("t", log)
        fence = ops._LvsFence(_lvs_owner(), _taker([]), None)
        fence.blocked_peers.extend([s, t])
        fence.unblock_peer_port(s, demoted=True)
        assert log == [("unblock", "s", 4402)]
        assert fence.blocked_peers == [t]
        assert not fence.defer_gate_event.is_set()
        self.clear_budget.assert_not_called()
        fence.unblock_peer_port(t, demoted=True)
        assert log == [("unblock", "s", 4402), ("unblock", "t", 4402)]
        assert fence.blocked_peers == []
        assert fence.defer_gate_event.is_set()
        self.clear_budget.assert_called_once_with()
        s_rpc.jc_ha_status.assert_not_called()
        t_rpc.jc_ha_status.assert_not_called()

    def test_a_port_the_guard_keeps_blocked_is_dropped_unopened(self, monkeypatch):
        log: list = []
        asked: list = []

        def _guard(peer, jm_vuid, db_controller, rebuild_too=True):
            asked.append((peer.get_id(), jm_vuid, rebuild_too))
            return "LVS 7 is fenced on the peer"
        monkeypatch.setattr(ops, "_two_node_peer_port_must_stay_blocked", _guard)
        s, _ = self._peer("s", log)
        fence = ops._LvsFence(_lvs_owner(), _taker([], jm_vuid=7), None)
        fence.blocked_peers.append(s)
        fence.unblock_peer_port(s, demoted=True)
        assert log == []
        assert fence.blocked_peers == []
        # the restarting (taker) node's journal vuid, as main passes it
        assert asked == [("s", 7, False)]


class TestFencedBounds:

    @staticmethod
    def _fence(calls, answers=None, *, removal=False, held_for=None):
        fence = ops._LvsFence(_lvs_owner(), _taker(calls, answers), None,
                              removal_in_progress=removal)
        if held_for is not None:
            fence.block_started["s"] = time.monotonic() - held_for
        return fence

    def test_unfenced_during_a_removal_uses_the_ordinary_client(self):
        calls: list = []
        assert self._fence(calls, removal=True).fenced("bdev_lvol_set_leader", "LVS_1", leader=True)
        assert calls == [({}, "bdev_lvol_set_leader")]

    def test_unfenced_failure_during_a_removal_names_it(self):
        calls: list = []
        fence = self._fence(calls, {"bdev_lvol_set_leader": RPCConnectionError("down")}, removal=True)
        with pytest.raises(ops.LeadershipTransferError, match="no port fenced during a removal"):
            fence.fenced("bdev_lvol_set_leader", "LVS_1", leader=True)

    def test_unfenced_without_a_removal_keeps_the_bound(self):
        calls: list = []
        self._fence(calls).fenced("get_lvstore", "LVS_1")
        assert calls == [({"timeout": constants.FENCE_RPC_TIMEOUT_SEC,
                           "retry": constants.FENCE_RPC_RETRY}, "get_lvstore")]

    @pytest.mark.parametrize("removal", [False, True])
    def test_a_held_fence_clamps_either_way(self, removal):
        calls: list = []
        left = 0.2
        self._fence(calls, removal=removal,
                    held_for=constants.FENCE_DEADLINE_SEC - left).fenced("get_lvstore", "LVS_1")
        [(kwargs, method)] = calls
        assert method == "get_lvstore"
        assert kwargs["retry"] == constants.FENCE_RPC_RETRY
        assert 0 < kwargs["timeout"] <= left


class TestGrantBudget:

    @staticmethod
    def _grant(held_for):
        calls: list = []
        lvs_node = _lvs_owner()
        taker = _taker(calls, {"get_lvstore": {"name": "LVS_1", "lvs leadership": True}})
        fence = ops._LvsFence(lvs_node, taker, None)
        fence.block_started["s"] = time.monotonic() - held_for
        ops._grant_lvs_leadership(fence)
        assert fence.granted is True
        return {method: kwargs["timeout"] for kwargs, method in calls}

    def test_the_leader_set_gets_its_own_budget(self):
        timeouts = self._grant(held_for=0.0)
        assert timeouts["bdev_lvol_set_leader"] == pytest.approx(constants.FENCE_SET_LEADER_TIMEOUT_SEC, abs=0.05)
        assert timeouts["bdev_lvol_set_lvs_opts"] == pytest.approx(constants.FENCE_RPC_TIMEOUT_SEC, abs=0.05)
        assert timeouts["get_lvstore"] == pytest.approx(constants.FENCE_RPC_TIMEOUT_SEC, abs=0.05)

    def test_near_the_deadline_the_leader_set_is_clamped_too(self):
        left = 1.0
        timeouts = self._grant(held_for=constants.FENCE_DEADLINE_SEC - left)
        assert timeouts["bdev_lvol_set_leader"] <= left
        assert timeouts["bdev_lvol_set_leader"] > constants.FENCE_RPC_TIMEOUT_SEC
        assert timeouts["bdev_lvol_set_lvs_opts"] == pytest.approx(constants.FENCE_RPC_TIMEOUT_SEC, abs=0.05)
