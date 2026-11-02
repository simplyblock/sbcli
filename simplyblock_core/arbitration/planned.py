"""Planned loss of a node: the control plane itself is about to stop its SPDK.

Campaign c2, F3: a restart of a live node keeps its SPDK proxy reachable, so
the proxy answers "framework not initialized" instead of HTTP 500 and the
arbiter never saw the node as gone; the peer held, got no verdict and fenced
itself at the hold deadline, taking down IO on both LVS. The restart and
shutdown flows therefore announce the loss BEFORE they stop SPDK. The arbiter
fences the node (best effort, it is going away anyway) and grants the peer at
once; the marker ends when the node answers with a new SPDK instance.

Best effort by contract: nothing here may fail or delay a restart.
"""

import logging
import time

logger = logging.getLogger(__name__)


def note_planned_loss(node, reason: str, db=None) -> bool:
    """Record that ``node``'s SPDK is about to be stopped by the control plane.

    Returns True when a marker was written (the cluster is arbitrated)."""
    try:
        if db is None:
            from simplyblock_core.db_controller import DBController
            db = DBController()
        cluster = db.get_cluster_by_id(node.cluster_id)
        # Strict checks: a mocked or legacy record must never look arbitrated.
        if getattr(cluster, "two_node_arbitration", False) is not True:
            return False
        # Mode pacemaker: no arbiter grants on the marker (kubernetes-fencing contract, 4.2).
        if getattr(cluster, "two_node_fencing_authority", "witness") == "pacemaker":
            return False
        from simplyblock_core.models.arbitration import ClusterArbitration
        rec = db.get_cluster_arbitration(node.cluster_id)
        if not isinstance(rec, ClusterArbitration):
            return False
        instance = ""
        try:
            status = node.rpc_client(timeout=1, retry=0).jc_ha_status() or {}
            instance = str(status.get("instance", ""))
        except Exception:                           # noqa: BLE001 - SPDK may be gone already
            pass
        nid = node.get_id()
        now = int(time.time() * 1000)

        def put(fresh):
            fresh.planned_loss = dict(fresh.planned_loss or {})
            fresh.planned_loss[nid] = {"at": now, "instance": instance, "reason": reason}
        db.atomic_update(rec, put)
        logger.warning("Two-node arbitration: planned loss of %s (%s); its peer may be "
                       "granted at once", nid, reason)
        return True
    except Exception as e:                          # noqa: BLE001 - never blocks a restart
        logger.warning("Could not record the planned loss of %s: %s", getattr(node, "uuid", node), e)
        return False
