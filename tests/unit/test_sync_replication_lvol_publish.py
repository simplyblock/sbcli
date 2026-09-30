"""Pure rules of volume publication on a sync-replication cluster: the site
ANA rule of every path (lvol_ana_state), the member list of an LVS (six paths,
five on FTT1, at stable positions), the remote part of ``lvol.nodes`` after a
ref change, the listener probe that never reads "cannot tell" as "absent",
the rejection helper and the CLI ``--site``. The flows against the real
database (create, clone, restart, repair, failover, Pass 4, connect, the
rejections) are in tests/integration/test_sync_replication_lvol_publish.py.
"""
import contextlib
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_cli import cli as cli_module
from simplyblock_cli import clibase
from simplyblock_core import storage_node_ops as ops
from simplyblock_core.controllers import lvol_controller
from simplyblock_core.exceptions import (
    PreconditionError, SyncReplicationUnsupportedError, reject_on_sync_replication,
)
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import snapshot_monitor

SITE_A, SITE_B = "site-a", "site-b"


def _owner(**fields):
    node = StorageNode()
    node.uuid = "p"
    node.site = SITE_A
    node.lvstore = "LVS_1"
    node.secondary_node_id = "s"
    node.tertiary_node_id = "t"
    node.remote_primary_node_id = "rp"
    node.remote_secondary_node_id = "rs"
    node.remote_tertiary_node_id = "rt"
    for key, value in fields.items():
        setattr(node, key, value)
    return node


def _node(node_id, site):
    node = StorageNode()
    node.uuid = node_id
    node.site = site
    return node


def _lvol(**fields):
    lvol = LVol()
    lvol.uuid = "lv-1"
    lvol.node_id = "p"
    lvol.sync_active_site = SITE_A
    for key, value in fields.items():
        setattr(lvol, key, value)
    return lvol


def _ctx(owner=None, lost_site=""):
    cluster = Cluster()
    cluster.sync_replication = True
    cluster.lost_site = lost_site
    return ops.SyncAnaContext(cluster, owner or _owner())


HOME = [("p", SITE_A), ("s", SITE_A), ("t", SITE_A)]
REMOTE = [("rp", SITE_B), ("rs", SITE_B), ("rt", SITE_B)]


def _states(lvol, ctx, state="non_optimized", **kwargs):
    return {nid: ops.lvol_ana_state(lvol, _node(nid, site), state, ctx=ctx, **kwargs)
            for nid, site in HOME + REMOTE}


class TestAnaRule:

    def test_home_active_volume(self):
        assert _states(_lvol(), _ctx()) == {
            "p": "optimized", "s": "non_optimized", "t": "non_optimized",
            "rp": "inaccessible", "rs": "inaccessible", "rt": "inaccessible"}

    def test_volume_active_on_the_remote_triplet(self):
        ctx = _ctx(_owner(lvs_active_site=SITE_B))
        assert _states(_lvol(sync_active_site=SITE_B), ctx) == {
            "p": "inaccessible", "s": "inaccessible", "t": "inaccessible",
            "rp": "optimized", "rs": "non_optimized", "rt": "non_optimized"}

    def test_the_callers_state_never_decides_on_a_sync_cluster(self):
        """A caller computing "primary" as lvol.node_id (the home owner) must
        not open the home owner of a remote-led volume."""
        ctx = _ctx(_owner(lvs_active_site=SITE_B))
        lvol = _lvol(sync_active_site=SITE_B)
        assert ops.lvol_ana_state(lvol, _node("p", SITE_A), "optimized", ctx=ctx) == "inaccessible"
        assert ops.lvol_ana_state(lvol, _node("rp", SITE_B), "non_optimized", ctx=ctx) == "optimized"

    def test_demoted_site_is_fenced(self):
        states = _states(_lvol(sync_demoted_sites=[SITE_A]), _ctx())
        assert set(states.values()) == {"inaccessible"}

    def test_lost_site_is_fenced(self):
        states = _states(_lvol(), _ctx(lost_site=SITE_A))
        assert set(states.values()) == {"inaccessible"}

    @pytest.mark.parametrize("moving", ["moving:site-b", "moving:site-a"])
    def test_nothing_is_open_while_the_leadership_moves(self, moving):
        states = _states(_lvol(), _ctx(_owner(lvs_active_site=moving)))
        assert set(states.values()) == {"inaccessible"}

    def test_a_volume_whose_site_does_not_lead_the_lvs_is_fenced_everywhere(self):
        """Judgment call A: sync_active_site B on an LVS led from A (a create
        racing a move) opens nothing - B's instances are non-leaders without a
        hublvol to the leader and would take the leadership on the first IO."""
        states = _states(_lvol(sync_active_site=SITE_B), _ctx())
        assert set(states.values()) == {"inaccessible"}

    def test_promoted_is_the_optimized_path_of_an_open_site_only(self):
        ctx = _ctx()
        assert ops.lvol_ana_state(_lvol(), _node("s", SITE_A), "optimized",
                                  promoted=True, ctx=ctx) == "optimized"
        assert ops.lvol_ana_state(_lvol(), _node("rs", SITE_B), "optimized",
                                  promoted=True, ctx=ctx) == "inaccessible"

    def test_a_callers_inaccessible_always_wins(self):
        """Activation registers every path closed until its Pass 4."""
        assert set(_states(_lvol(), _ctx(), state="inaccessible").values()) == {"inaccessible"}

    def test_an_empty_sync_active_site_means_the_home_site(self):
        assert _states(_lvol(sync_active_site=""), _ctx())["p"] == "optimized"

    def test_ftt1_home_triplet(self):
        ctx = _ctx(_owner(tertiary_node_id=""))
        assert ops.lvol_ana_state(_lvol(), _node("s", SITE_A), "optimized", ctx=ctx) == "non_optimized"
        assert ops.lvol_ana_state(_lvol(), _node("p", SITE_A), "non_optimized", ctx=ctx) == "optimized"

    @pytest.mark.parametrize("site", ["", MagicMock(name="site")])
    def test_outside_sync_replication_the_callers_state_without_a_db_read(self, site):
        # No ctx and no database in this tier: a read would raise.
        node = SimpleNamespace(site=site, get_id=lambda: "p")
        for state in ("optimized", "non_optimized", "inaccessible"):
            assert ops.lvol_ana_state(_lvol(), node, state) == state


class TestActiveSite:

    @pytest.mark.parametrize("value, site", [
        ("", SITE_A), (SITE_A, SITE_A), (SITE_B, SITE_B),
        ("moving:site-b", SITE_B), ("moving:site-a", SITE_A)])
    def test_lvs_active_site_of(self, value, site):
        assert ops.lvs_active_site_of(_owner(lvs_active_site=value)) == site


class TestMembers:

    def test_six_paths_home_then_remote(self):
        assert lvol_controller.role_secondary_ids(_owner()) == ["s", "t", "rp", "rs", "rt"]

    def test_ftt1_has_five(self):
        assert lvol_controller.role_secondary_ids(_owner(tertiary_node_id="")) == [
            "s", "rp", "rs", "rt"]

    def test_non_sync_unchanged(self):
        owner = _owner(site="", remote_primary_node_id="", remote_secondary_node_id="",
                       remote_tertiary_node_id="")
        assert lvol_controller.role_secondary_ids(owner) == ["s", "t"]

    def test_a_duck_typed_node_without_a_real_site_has_no_remote_members(self):
        node = MagicMock(secondary_node_id="s", tertiary_node_id="")
        assert lvol_controller.role_secondary_ids(node) == ["s"]


class TestSnapshotSyncDeletePeers:
    """A snapshot is registered on every instance, so its phase-2 sync
    deletes go to every instance but the phase-1 node."""

    def test_every_other_instance(self):
        assert snapshot_monitor.sync_delete_peer_ids("ha", _owner(), "p") == [
            "s", "t", "rp", "rs", "rt"]

    def test_phase_one_on_a_remote_leader(self):
        assert snapshot_monitor.sync_delete_peer_ids("ha", _owner(), "rs") == [
            "p", "s", "t", "rp", "rt"]

    def test_single_has_no_peers(self):
        assert snapshot_monitor.sync_delete_peer_ids("single", _owner(), "p") == []


class TestRemoteMembers:
    """lvol_remote_members: the remote part of lvol.nodes follows the refs."""

    def _members(self, nodes, triplet, owner=None):
        by_id = {nid: _node(nid, site) for nid, site in
                 HOME + REMOTE + [("x", SITE_B), ("y", SITE_B)]}
        return ops.lvol_remote_members(nodes, owner or _owner(), triplet, by_id)

    def test_replacement(self):
        assert self._members(["p", "s", "t", "rp", "rs", "rt"], ("rp", "x", "rt")) == [
            "p", "s", "t", "rp", "x", "rt"]

    def test_swap(self):
        assert self._members(["p", "s", "t", "rp", "rs", "rt"], ("rs", "rp", "rt")) == [
            "p", "s", "t", "rs", "rp", "rt"]

    def test_shift(self):
        assert self._members(["p", "s", "t", "rp", "rs", "rt"], ("rs", "rt", "y")) == [
            "p", "s", "t", "rs", "rt", "y"]

    def test_empty_slot_filled(self):
        assert self._members(["p", "s", "t", "rp", "rt"], ("rp", "x", "rt")) == [
            "p", "s", "t", "rp", "x", "rt"]

    def test_ftt1_home_prefix_of_two(self):
        assert self._members(["p", "s", "rp", "rs", "rt"], ("x", "rs", "rt"),
                             owner=_owner(tertiary_node_id="")) == ["p", "s", "x", "rs", "rt"]

    def test_the_home_part_keeps_its_order_and_unknown_ids(self):
        assert self._members(["p", "gone", "s", "rp", "rs", "rt"], ("rp", "rs", "rt")) == [
            "p", "gone", "s", "rp", "rs", "rt"]

    def test_unchanged(self):
        nodes = ["p", "s", "t", "rp", "rs", "rt"]
        assert self._members(nodes, ("rp", "rs", "rt")) == nodes


def _nic(ip, trtype="TCP"):
    nic = IFace()
    nic.ip4_address = ip
    nic.trtype = trtype
    return nic


class TestListenerProbe:

    def _node(self):
        node = _node("p", SITE_A)
        node.data_nics = [_nic("10.0.0.1"), _nic("10.0.0.2")]
        return node

    def test_only_the_listeners_that_exist(self):
        rpc = MagicMock()
        rpc.subsystem_get.return_value = {"listen_addresses": [
            {"trtype": "tcp", "traddr": "10.0.0.2", "trsvcid": "4420"}]}
        lvol = _lvol(fabric="tcp", nqn="nqn:1")
        assert ops.live_listener_nics(rpc, lvol, self._node(), 4420) == [("TCP", "10.0.0.2")]

    def test_other_port_does_not_count(self):
        rpc = MagicMock()
        rpc.subsystem_get.return_value = {"listen_addresses": [
            {"trtype": "TCP", "traddr": "10.0.0.2", "trsvcid": "4421"}]}
        assert ops.live_listener_nics(rpc, _lvol(fabric="tcp", nqn="nqn:1"), self._node(), 4420) == []

    def test_no_subsystem_means_no_listener(self):
        rpc = MagicMock()
        rpc.subsystem_get.return_value = None
        assert ops.live_listener_nics(rpc, _lvol(fabric="tcp", nqn="nqn:1"), self._node(), 4420) == []

    def test_a_failed_probe_is_never_read_as_absent(self):
        rpc = MagicMock()
        rpc.subsystem_get.side_effect = RuntimeError("timeout")
        with pytest.raises(RuntimeError):
            ops.live_listener_nics(rpc, _lvol(fabric="tcp", nqn="nqn:1"), self._node(), 4420)


class TestGroupsWithoutASweep:
    """The rule's inputs are read fresh under the site-rule lock (its
    serialization is tested against FDB in
    tests/integration/test_sync_replication_demote_promote.py); here the lock
    is a no-op and the fresh read answers the volume as given."""

    @pytest.fixture(autouse=True)
    def _fresh_inputs(self):
        with patch.object(ops, "sync_site_rule_locks", lambda *a, **k: contextlib.nullcontext()), \
                patch.object(ops, "_fresh_site_rule_inputs",
                             side_effect=lambda lvol, db=None: (lvol, _ctx())):
            yield

    def test_own_group_on_the_given_listeners_only(self):
        node = _node("p", SITE_A)
        node.data_nics = [_nic("10.0.0.1"), _nic("10.0.0.2")]
        rpc = MagicMock()
        rpc.nvmf_subsystem_listener_set_ana_state.return_value = True
        ok, err = ops.apply_sync_ana_groups(
            rpc, _lvol(fabric="tcp", nqn="nqn:1"), node, 4420, "non_optimized", ns_id=3,
            nics=[("TCP", "10.0.0.2")])
        assert (ok, err) == (True, None)
        rpc.nvmf_subsystem_listener_set_ana_state.assert_called_once_with(
            "nqn:1", "10.0.0.2", 4420, trtype="TCP", ana="optimized", anagrpid=3)
        rpc.subsystem_get.assert_not_called()

    def test_a_failed_group_rpc_is_reported_and_queued_for_a_durable_retry(self):
        node = _node("s", SITE_A)
        node.cluster_id = "cl-1"
        node.data_nics = [_nic("10.0.0.1")]
        rpc = MagicMock()
        rpc.nvmf_subsystem_listener_set_ana_state.return_value = False
        lvol = _lvol(fabric="tcp", nqn="nqn:1", nodes=["p", "s", "t", "rp", "rs", "rt"])
        with patch.object(ops.tasks_controller, "add_lvol_sync_op_task") as queue:
            ok, err = ops.apply_sync_ana_groups(
                rpc, lvol, node, 4420, "non_optimized", ns_id=2)
        assert not ok and "ns [2]" in err
        queue.assert_called_once_with("cl-1", "s", "lv-1", "register", secondary_index=0)


class TestAnaDrift:
    """lvol_ana_drift: what the lvol monitor compares on a sync path."""

    def _node(self, node_id="s", site=SITE_A):
        node = _node(node_id, site)
        node.lvol_subsys_port = 4402
        return node

    def _rpc(self, state, port="4402", group=1):
        rpc = MagicMock()
        rpc.listeners_list.return_value = [{
            "address": {"trtype": "TCP", "traddr": "10.0.0.1", "trsvcid": port},
            "ana_states": [{"ana_group": group, "ana_state": state}]}]
        return rpc

    def _drift(self, rpc, node=None, **fields):
        return ops.lvol_ana_drift(rpc, _lvol(nqn="nqn:1", ns_id=1, **fields),
                                  node or self._node(), ctx=_ctx())

    @pytest.mark.parametrize("state", ["optimized", "non_optimized"])
    def test_an_open_site_accepts_either_accessible_state(self, state):
        assert self._drift(self._rpc(state)) is None

    def test_an_open_site_left_inaccessible_is_drift(self):
        assert "inaccessible" in self._drift(self._rpc("inaccessible"))

    def test_a_closed_site_reported_open_is_drift(self):
        assert "wants it inaccessible" in self._drift(self._rpc("optimized"), node=self._node("rs", SITE_B))

    def test_a_closed_site_inaccessible_is_right(self):
        assert self._drift(self._rpc("inaccessible"), node=self._node("rs", SITE_B)) is None

    @pytest.mark.parametrize("rpc_kwargs", [{"port": "4499"}, {"group": 7}])
    def test_other_listeners_and_groups_are_not_judged(self, rpc_kwargs):
        assert self._drift(self._rpc("inaccessible", **rpc_kwargs)) is None

    def test_no_listener_list_is_no_drift(self):
        rpc = MagicMock()
        rpc.listeners_list.return_value = None
        assert self._drift(rpc) is None

    def test_outside_sync_replication_no_rpc(self):
        rpc = MagicMock()
        node = MagicMock(site="")
        assert ops.lvol_ana_drift(rpc, _lvol(nqn="nqn:1", ns_id=1), node) is None
        rpc.listeners_list.assert_not_called()


class TestReject:

    def test_sync_cluster_is_refused_as_a_precondition(self):
        with pytest.raises(SyncReplicationUnsupportedError) as exc:
            reject_on_sync_replication(SimpleNamespace(sync_replication=True), "Volume suspend")
        assert isinstance(exc.value, PreconditionError)
        assert "Volume suspend" in str(exc.value)

    @pytest.mark.parametrize("value", [False, MagicMock(name="flag")])
    def test_anything_else_passes(self, value):
        reject_on_sync_replication(SimpleNamespace(sync_replication=value), "x")


class TestCliConnect:

    def _connect(self, argv):
        controller = MagicMock()
        controller.connect_lvol.return_value = ([SimpleNamespace(connect="nvme connect x")], None)
        with patch.object(sys, "argv", ["sbctl", "volume", "connect", "vol-1", *argv]), \
                patch.object(clibase, "lvol_controller", controller), \
                patch("builtins.print"):
            cli_module.CLIWrapper().run()
        controller.connect_lvol.assert_called_once()
        return controller.connect_lvol.call_args

    def test_site_is_forwarded(self):
        call = self._connect(["--site", SITE_B])
        assert call.args == ("vol-1",) and call.kwargs["site"] == SITE_B

    def test_no_site_passes_none(self):
        assert "site" not in self._connect([]).kwargs
