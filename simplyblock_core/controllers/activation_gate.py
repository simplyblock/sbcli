"""The node rules of the activation gate, pure over the storage-node records,
and the refusal of what waits for a running site activation and the token
that activation carries, pure over the cluster record.

``storage_node_monitor._activation_node_gate`` (a cluster's automatic
re-activation) and the site gate of a sync-replication cluster share them. A
leaf: models and constants only, so the controllers may import it - unlike
``storage_node_monitor``, which creates a ``DBController`` on import, or
``sync_replication_controller``, which imports ``storage_node_ops``.
"""
from datetime import UTC, datetime
from typing import NamedTuple

from simplyblock_core import constants
from simplyblock_core.models.storage_node import StorageNode

_TRANSITION_STATUSES = (StorageNode.STATUS_IN_SHUTDOWN, StorageNode.STATUS_IN_CREATION,
                        StorageNode.STATUS_RESTARTING)


def activation_node_status_problem(nodes, ftt) -> str:
    """The status rules of the activation gate over ``nodes``; "" when they pass.

    * No node may be mid-transition (IN_SHUTDOWN / IN_CREATION / RESTARTING)
      or UNREACHABLE - recovery is still in flight.
    * Every node must be ONLINE, except up to ``ftt`` nodes an operator
      deliberately shut down (``auto_restart_disabled``): those are not coming
      back on their own and the cluster is designed to run without them. A
      node OFFLINE / DOWN / SCHEDULABLE that was not shut down deliberately is
      still recovering (auto-restart brings it back) - wait.
    * An ONLINE node must have been ONLINE for 30 s (settled).

    REMOVED nodes are ignored; an ``ftt`` that is not an int >= 1 counts as 1.
    """
    ftt = ftt if isinstance(ftt, int) and ftt >= 1 else 1
    deliberate_down = 0
    for node in nodes:
        if node.status == StorageNode.STATUS_REMOVED:
            continue
        if node.status in (*_TRANSITION_STATUSES, StorageNode.STATUS_UNREACHABLE):
            return f"node {node.get_id()} is still transitioning ({node.status})"
        if node.status != StorageNode.STATUS_ONLINE:
            if node.auto_restart_disabled:
                deliberate_down += 1
                continue
            return (f"node {node.get_id()} is {node.status} and was not deliberately "
                    f"shut down; waiting for it to restart and come back online")
        if node.online_since:
            try:
                diff = datetime.now(UTC) - datetime.fromisoformat(node.online_since)
                if diff.total_seconds() < 30:
                    return f"node {node.get_id()} has been online less than 30 seconds"
            except (ValueError, TypeError):
                pass
    if deliberate_down > ftt:
        return (f"{deliberate_down} deliberately-shut-down node(s) exceed the cluster "
                f"fault tolerance ({ftt}); not enough nodes to activate safely")
    return ""


def nodes_in_transition_problem(nodes) -> str:
    """"" when no node of ``nodes`` is IN_SHUTDOWN / IN_CREATION / RESTARTING -
    the transition part of activation_node_status_problem, without its
    UNREACHABLE rule."""
    for node in nodes:
        if node.status in _TRANSITION_STATUSES:
            return f"node {node.get_id()} is still transitioning ({node.status})"
    return ""


def site_jc_floor_problem(nodes, site) -> str:
    """"" when at least SYNC_JC_MIN_LOCAL_JMS nodes of ``site`` are ONLINE - the
    local JMs a site-aware journal needs to open there."""
    online = sum(1 for node in nodes if node.site == site and node.status == StorageNode.STATUS_ONLINE)
    if online < constants.SYNC_JC_MIN_LOCAL_JMS:
        return (f"site {site} has {online} ONLINE node(s); a site-aware journal needs "
                f"{constants.SYNC_JC_MIN_LOCAL_JMS} local JMs")
    return ""


class SiteActivation(NamedTuple):
    """A running site activation: its ``site`` and the ``token`` its
    begin_site_activation returned (the ``site_activation_since`` it wrote)."""

    site: str
    token: str

    def holds(self, cluster) -> bool:
        """Whether the flag of the sync-replication ``cluster`` (as read) is
        still this activation's."""
        return (cluster.sync_replication is True and bool(self.token)
                and cluster.site_activation_site == self.site
                and cluster.site_activation_since == self.token)


def activating_site(cluster) -> str:
    """The site a site activation of the sync-replication ``cluster`` runs for
    (``site_activation_site``); "" when none runs."""
    return cluster.site_activation_site if cluster.sync_replication is True else ""


def site_activation_blocks(cluster, owner=None) -> str:
    """"" or why an operation must wait for the running site activation of the
    sync-replication ``cluster`` - what main's ``IN_ACTIVATION`` holds, as far
    as it touches the activating site.

    A node-level operation (``owner`` None: a node restart, add or removal, a
    full activation, a migration) waits whenever one runs. An operation on the
    LVS of ``owner`` waits when that LVS is led from the activating site or its
    leadership is moving; one led from the other site goes on."""
    site = activating_site(cluster)
    if not site:
        return ""
    if (owner is None or owner.lvs_active_site.startswith(constants.LVS_MOVING_PREFIX)
            or (owner.lvs_active_site or owner.site) == site):
        return f"site {site} is being activated; retry"
    return ""
