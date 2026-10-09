"""What the collector delivers, folded into per-node signal state.

Contract sections 4 and 5. Events arrive at least once, in order per node, with
``(cluster_id, node_id, instance, seq)``; ``SignalState.apply`` drops
duplicates and stale ones, and an ``ha_resync`` (a full ``jc_ha_status``)
replaces the node's view outright. Pure: no I/O.
"""

from dataclasses import dataclass, field

from simplyblock_core.models.arbitration import LVS_FENCED, LVS_HOLDING, LVS_NORMAL, LVS_SOLO

ST_UNHEALTHY = "remote_jm_unhealthy"
ST_HEALTHY = "remote_jm_healthy"
ST_RESYNC = "ha_resync"

#: Event status -> per-LVS state it implies on the reporting node.
_STATE_OF = {
    "ha_hold_started": LVS_HOLDING,
    "ha_solo": LVS_SOLO,
    "ha_fenced": LVS_FENCED,
    "ha_self_fenced": LVS_FENCED,
    "ha_unfenced": LVS_NORMAL,
}


@dataclass
class NodeSignals:
    instance: str = ""
    last_seq: int = 0
    #: jm_vuid -> True while the node reports its peer's JM unhealthy.
    peer_unhealthy: dict = field(default_factory=dict)
    #: jm_vuid -> node-side state.
    lvs_state: dict = field(default_factory=dict)
    epoch_seen: int = 0

    def reports_peer_unhealthy(self) -> bool:
        return any(self.peer_unhealthy.values())

    def reports_peer_healthy(self) -> bool:
        return bool(self.peer_unhealthy) and not any(self.peer_unhealthy.values())


def apply(sig: NodeSignals, event: dict) -> bool:
    """Fold one event into ``sig``. Returns False for a duplicate or stale event.

    A changed ``instance`` (node restarted) resets the sequence; the collector
    sends an ``ha_resync`` right after, which carries the full state.
    """
    instance = str(event.get("instance", ""))
    seq = int(event.get("seq", 0))
    status = event.get("status", "")

    if status == ST_RESYNC:
        status_doc = event.get("ha_status") or {}
        sig.instance = instance or str(status_doc.get("instance", ""))
        sig.last_seq = int(status_doc.get("seq", seq))
        sig.epoch_seen = max(sig.epoch_seen, int(status_doc.get("epoch", 0)))
        sig.lvs_state, sig.peer_unhealthy = {}, {}
        for lvs in status_doc.get("lvs", []):
            vuid = int(lvs["jm_vuid"])
            sig.lvs_state[vuid] = lvs.get("state", LVS_NORMAL)
            health = (lvs.get("remote_jm") or {}).get("health", "healthy")
            sig.peer_unhealthy[vuid] = health != "healthy"
        return True

    if instance and instance != sig.instance:
        sig.instance, sig.last_seq = instance, 0
    if seq and seq <= sig.last_seq:
        return False
    if seq:
        sig.last_seq = seq

    vuid = event.get("jm_vuid")
    if vuid is None:
        return True
    vuid = int(vuid)
    if status == ST_UNHEALTHY:
        sig.peer_unhealthy[vuid] = True
    elif status == ST_HEALTHY:
        sig.peer_unhealthy[vuid] = False
    if status in _STATE_OF:
        sig.lvs_state[vuid] = _STATE_OF[status]
    elif event.get("ha_state"):
        sig.lvs_state[vuid] = event["ha_state"]
    if event.get("epoch"):
        sig.epoch_seen = max(sig.epoch_seen, int(event["epoch"]))
    return True
