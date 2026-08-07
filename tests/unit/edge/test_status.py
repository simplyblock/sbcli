# coding=utf-8
"""Unit tests for edge node/cluster status derivation (spec §6)."""
import pytest

from simplyblock_core.models.cluster import Cluster
from simplyblock_edge.models import EdgeNode
from simplyblock_edge.status import NodeProbe, derive_cluster_status, derive_node_status

ALL_GOOD = NodeProbe(k8s_reachable=True, node_ready=True, pod_running=True, rpc_alive=True)
API_DEAD = NodeProbe(k8s_reachable=False)
NODE_NOT_READY = NodeProbe(k8s_reachable=True, node_ready=False)
POD_GONE = NodeProbe(k8s_reachable=True, node_ready=True, pod_running=False)
RPC_DEAD = NodeProbe(k8s_reachable=True, node_ready=True, pod_running=True, rpc_alive=False)


# ------------------------------------------------------------- node status

@pytest.mark.parametrize("hands_off", [
    EdgeNode.STATUS_DOWN, EdgeNode.STATUS_REMOVED,
    EdgeNode.STATUS_IN_CREATION, EdgeNode.STATUS_RESTARTING,
])
@pytest.mark.parametrize("probe", [ALL_GOOD, API_DEAD, POD_GONE])
def test_monitor_never_overrides_flow_owned_states(hands_off, probe):
    assert derive_node_status(hands_off, probe) == (None, False)


def test_api_unreachable_maps_to_unreachable_not_offline():
    assert derive_node_status(EdgeNode.STATUS_ONLINE, API_DEAD) == (
        EdgeNode.STATUS_UNREACHABLE, False)
    assert derive_node_status(EdgeNode.STATUS_ONLINE, NODE_NOT_READY) == (
        EdgeNode.STATUS_UNREACHABLE, False)
    # idempotent
    assert derive_node_status(EdgeNode.STATUS_UNREACHABLE, API_DEAD) == (None, False)


def test_pod_or_rpc_dead_maps_to_offline():
    assert derive_node_status(EdgeNode.STATUS_ONLINE, POD_GONE) == (
        EdgeNode.STATUS_OFFLINE, False)
    assert derive_node_status(EdgeNode.STATUS_ONLINE, RPC_DEAD) == (
        EdgeNode.STATUS_OFFLINE, False)
    assert derive_node_status(EdgeNode.STATUS_OFFLINE, POD_GONE) == (None, False)


def test_returned_node_needs_reassembly_before_online():
    """A node whose data plane answers again is NOT flipped straight to
    online — a restart task must reassemble the stack first (spec §5.6)."""
    assert derive_node_status(EdgeNode.STATUS_OFFLINE, ALL_GOOD) == (None, True)
    assert derive_node_status(EdgeNode.STATUS_UNREACHABLE, ALL_GOOD) == (None, True)


def test_online_stays_online():
    assert derive_node_status(EdgeNode.STATUS_ONLINE, ALL_GOOD) == (None, False)


# ---------------------------------------------------------- cluster status

def test_cluster_all_online_is_active():
    assert derive_cluster_status(['online', 'online']) == Cluster.STATUS_ACTIVE
    assert derive_cluster_status(['online']) == Cluster.STATUS_ACTIVE


def test_cluster_partial_online_is_degraded():
    assert derive_cluster_status(['online', 'offline']) == Cluster.STATUS_DEGRADED
    assert derive_cluster_status(['online', 'unreachable']) == Cluster.STATUS_DEGRADED
    assert derive_cluster_status(['online', 'down']) == Cluster.STATUS_DEGRADED
    assert derive_cluster_status(['online', 'in_restart']) == Cluster.STATUS_DEGRADED


def test_cluster_all_not_serving_is_suspended():
    assert derive_cluster_status(['offline', 'offline']) == Cluster.STATUS_SUSPENDED
    assert derive_cluster_status(['offline', 'unreachable']) == Cluster.STATUS_SUSPENDED
    assert derive_cluster_status(['down']) == Cluster.STATUS_SUSPENDED
    assert derive_cluster_status(['offline']) == Cluster.STATUS_SUSPENDED


def test_cluster_transitional_states_hold_degraded_not_suspended():
    assert derive_cluster_status(['in_restart', 'offline']) == Cluster.STATUS_DEGRADED
    assert derive_cluster_status(['in_creation']) == Cluster.STATUS_DEGRADED


def test_cluster_no_nodes_is_unready():
    assert derive_cluster_status([]) == Cluster.STATUS_UNREADY
    assert derive_cluster_status(['removed']) == Cluster.STATUS_UNREADY
