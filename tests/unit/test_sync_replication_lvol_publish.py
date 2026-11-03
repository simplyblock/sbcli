"""Pure rules of volume publication on a sync-replication cluster: the site
ANA rule of every path (lvol_ana_state), the member list of an LVS (six paths,
five on FTT1, at stable positions), the remote part of ``lvol.nodes`` after a
ref change, the listener probe that never reads "cannot tell" as "absent",
the rejection helper and the CLI ``--site``. The flows against the real
database (create, clone, restart, repair, failover, Pass 4, connect, the
rejections) are in tests/integration/test_sync_replication_lvol_publish.py.
"""
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_cli import cli as cli_module
from simplyblock_cli import clibase
from simplyblock_core import storage_node_ops as ops
from simplyblock_core.controllers import lvol_controller
from simplyblock_core.exceptions import (
    PreconditionError,
    SyncReplicationUnsupportedError,
    reject_on_sync_replication,
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

    def test_volume_active_on_remote_triplet(self):
        ctx = _ctx(_owner(lvs_active_site=SITE_B))
        assert _states(_lvol(sync_active_site=SITE_B), ctx) == {
            "p": "inaccessible", "s": "inaccessible", "t": "inaccessible",
            "rp": "optimized", "rs": "non_optimized", "rt": "non_optimized"}

    def test_callers_state_never_decides_on_sync_cluster(self):
        """A caller computing "primary" as lvol.node_id (the home owner) must
        not open the home owner of a remote-led volume."""
        ctx = _ctx(_owner(lvs_active_site=SITE_B))
        lvol = _lvol(sync_active_site=SITE_B)
        assert ops.lvol_ana_state(lvol, _node("p", SITE_A), "optimized", ctx=ctx) == "inaccessible"
        assert ops.lvol_ana_state(lvol, _node("rp", SITE_B), "non_optimized", ctx=ctx) == "optimized"

    @pytest.mark.parametrize("lvol_fields, ctx_kwargs, state", [
        pytest.param({"sync_demoted_sites": [SITE_A]}, {}, "non_optimized", id="demoted-site"),
        pytest.param({}, {"lost_site": SITE_A}, "non_optimized", id="lost-site"),
        pytest.param({}, {"owner": {"lvs_active_site": "moving:site-b"}}, "non_optimized",
                     id="moving-to-b"),
        pytest.param({}, {"owner": {"lvs_active_site": "moving:site-a"}}, "non_optimized",
                     id="moving-to-a"),
        # The volume says B is active while its LVS is led from A (a create
        # racing a move). No path opens: B's instances are not leaders, have no
        # hublvol to the leader, and would take the leadership on the first IO.
        pytest.param({"sync_active_site": SITE_B}, {}, "non_optimized", id="off-the-leading-site"),
        # Activation registers every path closed until its Pass 4.
        pytest.param({}, {}, "inaccessible", id="callers-inaccessible"),
    ])
    def test_every_path_is_fenced(self, lvol_fields, ctx_kwargs, state):
        owner_fields = ctx_kwargs.get("owner")
        ctx = _ctx(_owner(**owner_fields) if owner_fields else None,
                   lost_site=ctx_kwargs.get("lost_site", ""))
        states = _states(_lvol(**lvol_fields), ctx, state=state)
        assert set(states.values()) == {"inaccessible"}

    def test_promoted_is_optimized_on_open_site_only(self):
        ctx = _ctx()
        assert ops.lvol_ana_state(_lvol(), _node("s", SITE_A), "optimized",
                                  promoted=True, ctx=ctx) == "optimized"
        assert ops.lvol_ana_state(_lvol(), _node("rs", SITE_B), "optimized",
                                  promoted=True, ctx=ctx) == "inaccessible"

    def test_empty_sync_active_site_means_home_site(self):
        assert _states(_lvol(sync_active_site=""), _ctx())["p"] == "optimized"

    def test_ftt1_home_triplet(self):
        ctx = _ctx(_owner(tertiary_node_id=""))
        assert ops.lvol_ana_state(_lvol(), _node("s", SITE_A), "optimized", ctx=ctx) == "non_optimized"
        assert ops.lvol_ana_state(_lvol(), _node("p", SITE_A), "non_optimized", ctx=ctx) == "optimized"

    @pytest.mark.parametrize("site", ["", MagicMock(name="site")])
    def test_non_sync_node_keeps_callers_state(self, site):
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

    def test_siteless_mock_node_has_no_remote_members(self):
        node = MagicMock(secondary_node_id="s", tertiary_node_id="")
        assert lvol_controller.role_secondary_ids(node) == ["s"]


class TestSnapshotSyncDeletePeers:
    """A snapshot is registered on every instance, so its phase-2 sync
    deletes go to every instance but the phase-1 node."""

    def test_every_other_instance(self):
        assert snapshot_monitor.sync_delete_peer_ids("ha", _owner(), "p") == [
            "s", "t", "rp", "rs", "rt"]

    def test_phase_one_on_remote_leader(self):
        assert snapshot_monitor.sync_delete_peer_ids("ha", _owner(), "rs") == [
            "p", "s", "t", "rp", "rt"]


class TestRemoteMembers:
    """lvol_remote_members: the remote part of lvol.nodes follows the refs."""

    @pytest.mark.parametrize("nodes, triplet, owner_fields, expected", [
        pytest.param(["p", "s", "t", "rp", "rs", "rt"], ("rp", "x", "rt"), {},
                     ["p", "s", "t", "rp", "x", "rt"], id="replacement"),
        pytest.param(["p", "s", "t", "rp", "rs", "rt"], ("rs", "rp", "rt"), {},
                     ["p", "s", "t", "rs", "rp", "rt"], id="swap"),
        pytest.param(["p", "s", "t", "rp", "rs", "rt"], ("rs", "rt", "y"), {},
                     ["p", "s", "t", "rs", "rt", "y"], id="shift"),
        pytest.param(["p", "s", "t", "rp", "rt"], ("rp", "x", "rt"), {},
                     ["p", "s", "t", "rp", "x", "rt"], id="empty-slot-filled"),
        pytest.param(["p", "s", "rp", "rs", "rt"], ("x", "rs", "rt"), {"tertiary_node_id": ""},
                     ["p", "s", "x", "rs", "rt"], id="ftt1-home-prefix-of-two"),
        pytest.param(["p", "gone", "s", "rp", "rs", "rt"], ("rp", "rs", "rt"), {},
                     ["p", "gone", "s", "rp", "rs", "rt"],
                     id="home-part-keeps-its-order-and-unknown-ids"),
        pytest.param(["p", "s", "t", "rp", "rs", "rt"], ("rp", "rs", "rt"), {},
                     ["p", "s", "t", "rp", "rs", "rt"], id="nodes-matching-the-refs-are-kept"),
    ])
    def test_remote_members(self, nodes, triplet, owner_fields, expected):
        by_id = {nid: _node(nid, site) for nid, site in
                 HOME + REMOTE + [("x", SITE_B), ("y", SITE_B)]}
        assert ops.lvol_remote_members(nodes, _owner(**owner_fields), triplet, by_id) == expected


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

    def test_only_listeners_that_exist(self):
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

    def test_failed_probe_is_never_read_as_absent(self):
        rpc = MagicMock()
        rpc.subsystem_get.side_effect = RuntimeError("timeout")
        with pytest.raises(RuntimeError):
            ops.live_listener_nics(rpc, _lvol(fabric="tcp", nqn="nqn:1"), self._node(), 4420)

    def test_pod_network_listener_matched_on_bind_address(self):
        # On the pod network a listener binds listen_ip4, while clients still
        # connect to ip4_address (the Service's address).
        node = _node("p", SITE_A)
        nic = _nic("10.96.0.7")
        nic.listen_ip4 = "10.42.1.5"
        node.data_nics = [nic]
        rpc = MagicMock()
        rpc.subsystem_get.return_value = {"listen_addresses": [
            {"trtype": "TCP", "traddr": "10.42.1.5", "trsvcid": "4420"}]}
        lvol = _lvol(fabric="tcp", nqn="nqn:1")
        assert ops._lvol_listener_nics(lvol, node) == [("TCP", "10.42.1.5")]
        assert ops.live_listener_nics(rpc, lvol, node, 4420) == [("TCP", "10.42.1.5")]


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
    def test_open_site_accepts_either_accessible_state(self, state):
        assert self._drift(self._rpc(state)) is None

    def test_open_site_left_inaccessible_is_drift(self):
        assert "inaccessible" in self._drift(self._rpc("inaccessible"))

    def test_closed_site_reported_open_is_drift(self):
        assert "wants it inaccessible" in self._drift(self._rpc("optimized"), node=self._node("rs", SITE_B))

    def test_closed_site_inaccessible_is_right(self):
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

    def test_sync_cluster_is_refused_as_precondition(self):
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
