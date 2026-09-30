"""Pure rules of the sync-replication demote and planned promote: the promote
decision table, the record view of where a volume is served, the triplet of a
site, the DB-only re-check of the promote transaction, the strict ANA helper
(which paths it touches, what fails it) and the order and re-entrancy of the
site-rule locks. The flows against the real database (demote, the promote
task, the claim transaction, cancellation, the lock against concurrent ANA
writers) are in tests/integration/test_sync_replication_demote_promote.py.
"""
import contextlib
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import storage_node_ops as ops
from simplyblock_core.controllers import sync_replication_controller as src
from simplyblock_core.exceptions import SyncAnaError
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCException

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


def _lvol(uuid="v1", **fields):
    lvol = LVol()
    lvol.uuid = uuid
    lvol.node_id = "p"
    lvol.lvs_name = "LVS_1"
    lvol.nqn = f"nqn:{uuid}"
    lvol.ns_id = 1
    lvol.fabric = "tcp"
    lvol.status = LVol.STATUS_ONLINE
    for key, value in fields.items():
        setattr(lvol, key, value)
    return lvol


BOTH = {SITE_A, SITE_B}


def _decide(lvol, owner=None, others=(), site=SITE_B, online=BOTH, force=False):
    owner = owner or _owner()
    return src.promote_decision(lvol, owner, [lvol, *others], site, online_sites=set(online),
                                force=force)


class TestRecordView:

    def test_a_volume_is_active_on_its_lvs_home_site_until_set(self):
        assert src.volume_site(_lvol(), _owner()) == SITE_A
        assert src.volume_site(_lvol(sync_active_site=SITE_B), _owner()) == SITE_B

    def test_open_means_active_and_not_demoted_there(self):
        owner = _owner()
        assert src.volume_open_on(_lvol(), owner, SITE_A)
        assert not src.volume_open_on(_lvol(), owner, SITE_B)
        assert not src.volume_open_on(_lvol(sync_demoted_sites=[SITE_A]), owner, SITE_A)

    def test_deleted_records_never_block(self):
        vols = [_lvol("v1"), _lvol("v2", status=LVol.STATUS_DELETED),
                _lvol("v3", sync_demoted_sites=[SITE_A]), _lvol("v4", status=LVol.STATUS_IN_CREATION)]
        assert src.volumes_open_on(vols, _owner(), SITE_A) == ["v1", "v4"]

    def test_site_triplet_primary_first(self):
        assert src.lvs_site_triplet(_owner(), SITE_A) == ("p", "s", "t")
        assert src.lvs_site_triplet(_owner(), SITE_B) == ("rp", "rs", "rt")
        assert src.lvs_site_triplet(_owner(tertiary_node_id="", remote_tertiary_node_id=""),
                                    SITE_B) == ("rp", "rs")


class TestPromoteDecision:
    """One case per row of the promote table."""

    def test_already_served_on_the_site_is_a_no_op(self):
        owner = _owner(lvs_active_site=SITE_B)
        d = _decide(_lvol(sync_active_site=SITE_B, sync_demoted_sites=[SITE_A]), owner)
        assert d == src.PromoteDecision(src.PROMOTE_ACTIVE, SITE_B)

    @pytest.mark.parametrize("fields", [
        {"sync_active_site": SITE_A, "sync_demoted_sites": [SITE_A]},    # the sibling of a moved LVS
        {"sync_active_site": SITE_B, "sync_demoted_sites": [SITE_B]},    # demoted on S, asked back
    ])
    def test_led_from_the_site_but_not_open_there_is_only_the_ana_step(self, fields):
        d = _decide(_lvol(**fields), _owner(lvs_active_site=SITE_B))
        assert d.kind == src.PROMOTE_ANA_ONLY

    def test_active_on_t_not_demoted_is_refused(self):
        d = _decide(_lvol())
        assert d == src.PromoteDecision(src.PROMOTE_NOT_DEMOTED, SITE_A, ("v1",))

    def test_other_volumes_of_the_lvs_still_active_on_t_are_listed(self):
        others = [_lvol("v2"), _lvol("v3", sync_demoted_sites=[SITE_A]), _lvol("v4")]
        d = _decide(_lvol(sync_demoted_sites=[SITE_A]), others=others)
        assert d == src.PromoteDecision(src.PROMOTE_LVS_BUSY, SITE_A, ("v2", "v4"))

    def test_every_volume_demoted_on_t_moves_the_lvs(self):
        others = [_lvol("v2", sync_demoted_sites=[SITE_A]),
                  _lvol("v3", sync_active_site=SITE_B)]     # stale: not served on T either
        d = _decide(_lvol(sync_demoted_sites=[SITE_A]), others=others)
        assert d == src.PromoteDecision(src.PROMOTE_MOVE, SITE_A)

    def test_t_not_online_without_force_is_412(self):
        d = _decide(_lvol(sync_demoted_sites=[SITE_A]), online={SITE_B})
        assert d == src.PromoteDecision(src.PROMOTE_SITE_OFFLINE, SITE_A)

    def test_t_not_online_with_force_is_the_disaster_fail_over(self):
        # the table decides it before the demotion rows: T may never have
        # been reachable to demote
        d = _decide(_lvol(), online={SITE_B}, force=True)
        assert d == src.PromoteDecision(src.PROMOTE_DISASTER, SITE_A)

    def test_force_never_acts_while_t_is_online(self):
        d = _decide(_lvol(sync_demoted_sites=[SITE_A]), force=True)
        assert d.kind == src.PROMOTE_FORCE_ONLINE

    def test_a_move_in_flight_is_in_progress(self):
        d = _decide(_lvol(sync_demoted_sites=[SITE_A]), _owner(lvs_active_site="moving:site-b"))
        assert d.kind == src.PROMOTE_IN_PROGRESS

    def test_group_union_rule_is_the_per_member_rule(self):
        """A group's members are judged each against every volume of its own
        LVS: a volume outside the group blocks the group's move."""
        member = _lvol("m1", sync_demoted_sites=[SITE_A])
        outsider = _lvol("x1")
        assert _decide(member, others=[outsider]).blocking == ("x1",)


class TestMoveProblems:
    """promote_move_problems: the DB-only re-check inside the promote tx."""

    def _problems(self, owner=None, vols=(), expect="", lost=""):
        cluster = Cluster()
        cluster.lost_site = lost
        return src.promote_move_problems(cluster, owner or _owner(), list(vols), expect, SITE_B)

    def test_all_demoted_nothing_changed_is_clean(self):
        assert self._problems(vols=[_lvol(sync_demoted_sites=[SITE_A])]) == []

    def test_a_new_volume_on_t_blocks(self):
        problems = self._problems(vols=[_lvol(sync_demoted_sites=[SITE_A]), _lvol("new")])
        assert problems == ["LVS LVS_1: volumes still active on site site-a: ['new']"]

    def test_a_changed_active_site_blocks(self):
        problems = self._problems(owner=_owner(lvs_active_site="moving:site-b"))
        assert "expected ''" in problems[0]

    def test_a_lost_site_blocks_the_planned_promote(self):
        assert "lost" in self._problems(lost=SITE_A)[0]

    def test_an_lvs_already_led_from_the_target_is_not_moved(self):
        owner = _owner(lvs_active_site=SITE_B)
        assert "already led" in self._problems(owner=owner, expect=SITE_B)[0]


def _nic(ip):
    nic = IFace()
    nic.ip4_address = ip
    nic.trtype = "TCP"
    return nic


class TestStrictAna:

    def _nodes(self, statuses=None):
        statuses = statuses or {}
        nodes = {}
        for nid, site in (("p", SITE_A), ("s", SITE_A), ("t", SITE_A),
                          ("rp", SITE_B), ("rs", SITE_B), ("rt", SITE_B)):
            node = MagicMock(spec=StorageNode)
            node.get_id.return_value = nid
            node.site = site
            node.status = statuses.get(nid, StorageNode.STATUS_ONLINE)
            node.data_nics = [_nic(f"10.0.0.{len(nodes)}")]
            node.active_tcp = True
            node.get_lvol_subsys_port.return_value = 4402
            node.rpc_client.return_value.nvmf_subsystem_listener_set_ana_state.return_value = True
            nodes[nid] = node
        return nodes

    def _triplet(self, nodes, site):
        return [nodes[nid] for nid in src.lvs_site_triplet(_owner(), site)]

    def _calls(self, nodes):
        return {nid: [c.kwargs["ana"] for c in
                      n.rpc_client.return_value.nvmf_subsystem_listener_set_ana_state.call_args_list]
                for nid, n in nodes.items()}

    def test_close_sets_every_path_of_the_triplet_in_the_volumes_group(self):
        nodes = self._nodes()
        src.set_site_ana_strict(_lvol(ns_id=7), self._triplet(nodes, SITE_A), open_site=False)
        assert self._calls(nodes) == {"p": ["inaccessible"], "s": ["inaccessible"],
                                      "t": ["inaccessible"], "rp": [], "rs": [], "rt": []}
        call = nodes["s"].rpc_client.return_value.nvmf_subsystem_listener_set_ana_state.call_args
        assert call.args == ("nqn:v1", nodes["s"].data_nics[0].ip4_address, 4402)
        assert call.kwargs == {"trtype": "TCP", "ana": "inaccessible", "anagrpid": 7}
        nodes["s"].get_lvol_subsys_port.assert_called_with("LVS_1")

    def test_open_optimizes_the_primary_only(self):
        nodes = self._nodes()
        src.set_site_ana_strict(_lvol(), self._triplet(nodes, SITE_B), open_site=True)
        assert self._calls(nodes) == {"p": [], "s": [], "t": [], "rp": ["optimized"],
                                      "rs": ["non_optimized"], "rt": ["non_optimized"]}

    def test_close_skips_only_a_node_whose_spdk_is_down(self):
        nodes = self._nodes({"s": StorageNode.STATUS_OFFLINE, "t": StorageNode.STATUS_UNREACHABLE})
        src.set_site_ana_strict(_lvol(), self._triplet(nodes, SITE_A), open_site=False)
        calls = self._calls(nodes)
        assert (calls["p"], calls["s"], calls["t"]) == (["inaccessible"], [], ["inaccessible"])

    def test_open_touches_online_members_only(self):
        nodes = self._nodes({"rs": StorageNode.STATUS_UNREACHABLE})
        src.set_site_ana_strict(_lvol(), self._triplet(nodes, SITE_B), open_site=True)
        assert self._calls(nodes)["rs"] == []

    @pytest.mark.parametrize("failure", ["false", "raise"])
    def test_any_failed_path_fails_the_call(self, failure):
        nodes = self._nodes()
        rpc = nodes["s"].rpc_client.return_value.nvmf_subsystem_listener_set_ana_state
        if failure == "false":
            rpc.return_value = False
        else:
            rpc.side_effect = RPCException("timeout")
        with pytest.raises(SyncAnaError, match="on s "):
            src.set_site_ana_strict(_lvol(), self._triplet(nodes, SITE_A), open_site=False)

    def test_a_volume_without_namespace_id_is_refused(self):
        with pytest.raises(SyncAnaError, match="namespace id"):
            src.set_site_ana_strict(_lvol(ns_id=0), self._triplet(self._nodes(), SITE_A),
                                    open_site=False)


class TestSiteRuleLocks:

    @pytest.fixture()
    def taken(self):
        taken = []

        @contextlib.contextmanager
        def fake_lock(cluster_id, name, **kwargs):
            taken.append(name)
            yield
            taken.append(f"-{name}")

        with patch.object(ops.snapshot_controller, "lvstore_op_lock", fake_lock):
            yield taken

    def test_several_locks_are_taken_in_sorted_order(self, taken):
        with ops.sync_site_rule_locks("cl", ["LVS_9", "LVS_2", "LVS_2"]):
            pass
        assert taken == ["__sync_ana__LVS_2", "__sync_ana__LVS_9",
                         "-__sync_ana__LVS_9", "-__sync_ana__LVS_2"]

    def test_re_entrant_per_thread(self, taken):
        with ops.sync_site_rule_locks("cl", ["LVS_1"]):
            with ops.sync_site_rule_locks("cl", ["LVS_1"]):
                pass
            assert taken == ["__sync_ana__LVS_1"]
        assert taken == ["__sync_ana__LVS_1", "-__sync_ana__LVS_1"]
        with ops.sync_site_rule_locks("cl", ["LVS_1"]):
            pass
        assert taken.count("__sync_ana__LVS_1") == 2

    def test_a_non_sync_writer_takes_no_lock(self, taken):
        node = MagicMock()
        node.site = ""
        node.rpc_client.return_value.nvmf_subsystem_listener_set_ana_state.return_value = True
        node.data_nics = []
        ops._set_lvol_ana_on_node(_lvol(), node, "optimized")
        assert taken == []
