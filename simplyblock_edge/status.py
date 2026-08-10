# coding=utf-8
"""Pure status derivation for edge nodes and clusters (spec §6).

No RPC/k8s/DB access here — the monitor collects a NodeProbe per node and
feeds it through these functions.
"""
from dataclasses import dataclass
from typing import Iterable, Optional, Tuple

from simplyblock_core.models.cluster import Cluster
from simplyblock_edge.models import EdgeNode

# Statuses the monitor must never override: admin intent (down), lifecycle
# ownership (in_creation / in_restart belong to the add/restart flows), and
# tombstones (removed).
_MONITOR_HANDS_OFF = (
    EdgeNode.STATUS_DOWN,
    EdgeNode.STATUS_REMOVED,
    EdgeNode.STATUS_IN_CREATION,
    EdgeNode.STATUS_RESTARTING,
)

# Statuses that mean "the stack must be reassembled before the node may be
# called online again".
_NEEDS_REASSEMBLY = (
    EdgeNode.STATUS_OFFLINE,
    EdgeNode.STATUS_UNREACHABLE,
)


@dataclass
class NodeProbe:
    """Result of one monitor probe of an edge node.

    k8s_reachable: the edge cluster's kube-apiserver answered.
    node_ready:    the worker node object exists and reports Ready.
    pod_running:   the SPDK pod exists and its phase is Running.
    rpc_alive:     SPDK JSON-RPC (spdk_get_version) answered.
    """
    k8s_reachable: bool
    node_ready: bool = False
    pod_running: bool = False
    rpc_alive: bool = False


def derive_node_status(current_status: str, probe: NodeProbe) -> Tuple[Optional[str], bool]:
    """Decide (new_status, needs_restart_task) for one node.

    new_status None means "leave the record unchanged". needs_restart_task
    True means the data plane answers but the stack must be reassembled by a
    FN_EDGE_NODE_RESTART task before the node can be ONLINE (spec §5.6) —
    the task flips the node to in_restart and, on success, online.

    UNREACHABLE is a management-plane verdict: the edge data plane may well be
    serving clients while the CP cannot see it. Nothing destructive keys off
    it (spec §6.1).
    """
    if current_status in _MONITOR_HANDS_OFF:
        return None, False

    if not probe.k8s_reachable or not probe.node_ready:
        if current_status == EdgeNode.STATUS_UNREACHABLE:
            return None, False
        return EdgeNode.STATUS_UNREACHABLE, False

    if not probe.pod_running or not probe.rpc_alive:
        if current_status == EdgeNode.STATUS_OFFLINE:
            return None, False
        return EdgeNode.STATUS_OFFLINE, False

    # Data plane answers.
    if current_status in _NEEDS_REASSEMBLY:
        # Not online yet — the stack state after a pod restart is unknown.
        return None, True
    if current_status == EdgeNode.STATUS_ONLINE:
        return None, False
    return EdgeNode.STATUS_ONLINE, False


def derive_cluster_status(node_statuses: Iterable[str]) -> str:
    """Michael's rule verbatim (spec §6.2): suspended if all nodes are
    offline-ish, degraded if some are while at least one is online, active
    otherwise. DOWN counts as not-serving (deliberate stop). Nodes in a
    transitional state (in_creation / in_restart) count as not-online but
    not-suspending either — they resolve on the next sweep."""
    statuses = [s for s in node_statuses if s != EdgeNode.STATUS_REMOVED]
    if not statuses:
        return Cluster.STATUS_UNREADY

    online = sum(1 for s in statuses if s == EdgeNode.STATUS_ONLINE)
    if online == len(statuses):
        return Cluster.STATUS_ACTIVE
    if online > 0:
        return Cluster.STATUS_DEGRADED

    not_serving = (EdgeNode.STATUS_OFFLINE, EdgeNode.STATUS_UNREACHABLE,
                   EdgeNode.STATUS_DOWN)
    if all(s in not_serving for s in statuses):
        return Cluster.STATUS_SUSPENDED
    # Only transitional states left (creation/restart in progress).
    return Cluster.STATUS_DEGRADED
