"""Pure node rules of the activation gate (``controllers/activation_gate``):
the parts of the node status rule of ``_activation_node_gate`` that
tests/unit/test_activation_gate.py does not cover, the transition rule on its
own, the minimum number of ONLINE nodes a site-aware journal needs on a site,
and which operations wait while a site activation runs."""
import pytest

from simplyblock_core import constants, storage_node_ops
from simplyblock_core.controllers import activation_gate as gate
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode

SITE_A, SITE_B = "site-a", "site-b"
TRANSITION = (StorageNode.STATUS_IN_SHUTDOWN, StorageNode.STATUS_IN_CREATION, StorageNode.STATUS_RESTARTING)


def _node(uuid, status=StorageNode.STATUS_ONLINE, *, site=SITE_A, auto_restart_disabled=False):
    node = StorageNode()
    node.uuid = uuid
    node.status = status
    node.site = site
    node.auto_restart_disabled = auto_restart_disabled
    return node


def _stopped(uuid):
    return _node(uuid, StorageNode.STATUS_OFFLINE, auto_restart_disabled=True)


class TestActivationNodeStatusProblem:

    @pytest.mark.parametrize("status", [*TRANSITION, StorageNode.STATUS_UNREACHABLE])
    @pytest.mark.parametrize("deliberate", [False, True])
    def test_transitioning_node_refuses(self, status, deliberate):
        nodes = [_node("n0"), _node("n1", status, auto_restart_disabled=deliberate)]
        problem = gate.activation_node_status_problem(nodes, 1)
        assert "n1" in problem and "transitioning" in problem

    @pytest.mark.parametrize("status", [StorageNode.STATUS_OFFLINE, StorageNode.STATUS_DOWN,
                                        StorageNode.STATUS_SCHEDULABLE])
    def test_node_down_not_deliberately_refuses(self, status):
        problem = gate.activation_node_status_problem([_node("n0"), _node("n1", status)], 1)
        assert "n1" in problem and "not deliberately" in problem

    @pytest.mark.parametrize("ftt", [pytest.param(1, id="ftt1-2-stopped"), pytest.param(2, id="ftt2-3-stopped")])
    def test_stopped_nodes_beyond_ftt_refuse(self, ftt):
        nodes = [_node("n0"), *(_stopped(f"n{i}") for i in range(1, ftt + 2))]
        problem = gate.activation_node_status_problem(nodes, ftt)
        assert "fault tolerance" in problem

    @pytest.mark.parametrize("ftt", [0, None, "1"])
    def test_unusable_ftt_counts_as_one(self, ftt):
        assert gate.activation_node_status_problem([_node("n0"), _stopped("n1")], ftt) == ""
        assert "fault tolerance" in gate.activation_node_status_problem(
            [_node("n0"), _stopped("n1"), _stopped("n2")], ftt)


class TestNodesInTransitionProblem:

    @pytest.mark.parametrize("status", TRANSITION)
    def test_node_in_transition_refuses(self, status):
        problem = gate.nodes_in_transition_problem([_node("n0"), _node("n1", status)])
        assert "n1" in problem and status in problem

    @pytest.mark.parametrize("status", [StorageNode.STATUS_ONLINE, StorageNode.STATUS_OFFLINE,
                                        StorageNode.STATUS_UNREACHABLE, StorageNode.STATUS_REMOVED])
    def test_node_out_of_transition_passes(self, status):
        assert gate.nodes_in_transition_problem([_node("n0"), _node("n1", status)]) == ""


class TestSiteJcFloorProblem:

    def test_one_online_node_of_site_refuses(self):
        nodes = [_node("a0"), _node("a1", StorageNode.STATUS_OFFLINE), _node("a2", StorageNode.STATUS_OFFLINE)]
        problem = gate.site_jc_floor_problem(nodes, SITE_A)
        assert SITE_A in problem and str(constants.SYNC_JC_MIN_LOCAL_JMS) in problem

    def test_two_online_nodes_of_site_pass(self):
        nodes = [_node("a0"), _node("a1"), _node("a2", StorageNode.STATUS_OFFLINE)]
        assert gate.site_jc_floor_problem(nodes, SITE_A) == ""

    def test_other_sites_nodes_do_not_count(self):
        nodes = [_node("a0"), _node("a1", StorageNode.STATUS_OFFLINE),
                 _node("b0", site=SITE_B), _node("b1", site=SITE_B)]
        assert gate.site_jc_floor_problem(nodes, SITE_A) != ""
        assert gate.site_jc_floor_problem(nodes, SITE_B) == ""


def _cluster(ftt, failure_domain=False):
    cluster = Cluster()
    cluster.sync_replication = True
    cluster.max_fault_tolerance = ftt
    cluster.enable_failure_domain = failure_domain
    return cluster


class TestFloorWithinFtt:
    """A site holds ``ha_jm_count // 2`` local JMs, one per node; the gate lets
    up to FTT of its nodes stay stopped, which must never leave fewer local JMs
    than the journal's floor."""

    @pytest.mark.parametrize("ftt, failure_domain, ha_jm_count", [(1, False, 6), (2, False, 8), (1, True, 8)])
    def test_stopped_nodes_within_ftt_keep_floor(self, ftt, failure_domain, ha_jm_count):
        assert storage_node_ops.get_required_ha_jm_count(_cluster(ftt, failure_domain)) == ha_jm_count
        site = [_node(f"a{i}") for i in range(ha_jm_count // 2)]
        for node in site[:ftt]:
            node.status, node.auto_restart_disabled = StorageNode.STATUS_OFFLINE, True
        assert gate.activation_node_status_problem(site, ftt) == ""
        assert gate.site_jc_floor_problem(site, SITE_A) == ""


def _owner(site, lvs_active_site=""):
    owner = _node(f"owner-{site}", site=site)
    owner.lvs_active_site = lvs_active_site
    return owner


#: The operation is node-level (no owner) or on an LVS led from A.
OWNER_SITES = [pytest.param(None, id="node-level"), pytest.param(SITE_A, id="lvs-led-from-a")]


def _maybe_owner(site):
    return _owner(site) if site else None


class TestSiteActivationBlocks:

    @pytest.mark.parametrize("owner_site", OWNER_SITES)
    def test_no_flag_blocks_nothing(self, owner_site):
        cluster = _cluster(1)
        assert gate.site_activation_blocks(cluster, _maybe_owner(owner_site)) == ""
        assert gate.activating_site(cluster) == ""

    def test_running_site_activation_blocks(self):
        cluster = _cluster(1)
        cluster.site_activation_site = SITE_A
        assert gate.site_activation_blocks(cluster) == f"site {SITE_A} is being activated; retry"
        assert gate.activating_site(cluster) == SITE_A

    @pytest.mark.parametrize("owner_site", OWNER_SITES)
    def test_flag_on_non_sync_cluster_blocks_nothing(self, owner_site):
        cluster = _cluster(1)
        cluster.sync_replication = False
        cluster.site_activation_site = SITE_A
        assert gate.site_activation_blocks(cluster, _maybe_owner(owner_site)) == ""
        assert gate.activating_site(cluster) == ""


class TestSiteActivationBlocksAnLvs:
    """With an owner, only an operation on an LVS led from the activating site
    (or whose leadership is moving) waits."""

    @pytest.fixture()
    def activating(self):
        cluster = _cluster(1)
        cluster.site_activation_site = SITE_A
        return cluster

    @pytest.mark.parametrize("home, active", [(SITE_A, ""), (SITE_A, SITE_A), (SITE_B, SITE_A)])
    def test_lvs_led_from_site_is_blocked(self, activating, home, active):
        assert gate.site_activation_blocks(activating, _owner(home, active)) == (
            f"site {SITE_A} is being activated; retry")

    @pytest.mark.parametrize("home, active", [(SITE_B, ""), (SITE_B, SITE_B), (SITE_A, SITE_B)])
    def test_lvs_led_from_other_site_goes_on(self, activating, home, active):
        assert gate.site_activation_blocks(activating, _owner(home, active)) == ""

    @pytest.mark.parametrize("home, target", [(SITE_A, SITE_B), (SITE_B, SITE_A), (SITE_B, SITE_B)])
    def test_moving_lvs_is_blocked(self, activating, home, target):
        owner = _owner(home, storage_node_ops.lvs_moving_value(target))
        assert gate.site_activation_blocks(activating, owner) == f"site {SITE_A} is being activated; retry"
