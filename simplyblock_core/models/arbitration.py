"""Two-node arbitration state, one record per cluster.

docs/design/two-node-arbitration.md, section 7.2. The arbiter writes the new
epoch and the verdict here in one transaction BEFORE it sends any verdict RPC,
so a restarted (or failed-over) arbiter can reload the record, raise the epoch
and re-send the last verdict.
"""

from simplyblock_core.models.base_model import BaseModel, default_factory

#: Cluster states (section 7.1).
ARB_STEADY = "steady"
ARB_DECIDING = "deciding"
ARB_PARTITIONED = "partitioned"
ARB_DEGRADED = "degraded"
ARB_HEALING = "healing"

#: Per-LVS states, as the node reports them (section 6).
LVS_NORMAL = "normal"
LVS_HOLDING = "holding"
LVS_SOLO = "solo"
LVS_FENCED = "fenced"


class ClusterArbitration(BaseModel):
    cluster_id: str = ""
    epoch: int = 0
    state: str = ARB_STEADY
    preferred_node: str = ""
    #: When the current state was entered (ms since epoch).
    state_since: int = 0
    #: [{"jm_vuid", "leader", "state", "fenced_node", "since"}]
    lvs: list[dict] = default_factory(list)
    #: {node_id: {"granted_at": ms, "ttl_ms": int}}
    leases: dict = default_factory(dict)
    #: Newest last, bounded to constants.TWO_NODE_VERDICT_HISTORY.
    verdicts: list[dict] = default_factory(list)
    #: Node ids the operator should taint as storage-fenced.
    taint_requests: list[str] = default_factory(list)
    #: When both nodes were last seen healthy again (ms); 0 while not healthy.
    healthy_since: int = 0
    #: Operator decision waiting to be applied: {"winner", "reason", "by", "at"}
    #: (CP API ``POST .../arbitration/override``; audited in the cluster log).
    override: dict = default_factory(dict)

    def get_id(self):
        return self.cluster_id

    # ---- pure helpers (no I/O; safe inside DBController.atomic_update) ----

    def lvs_entry(self, jm_vuid: int) -> dict:
        for entry in self.lvs:
            if int(entry.get("jm_vuid", -1)) == int(jm_vuid):
                return entry
        entry = {"jm_vuid": int(jm_vuid), "leader": "", "state": LVS_NORMAL,
                 "fenced_node": "", "since": 0}
        self.lvs.append(entry)
        return entry

    def record_verdict(self, verdict: dict, limit: int) -> None:
        self.verdicts.append(verdict)
        if len(self.verdicts) > limit:
            del self.verdicts[: len(self.verdicts) - limit]

    def lease_expires_at(self, node_id: str) -> int:
        """Expiry in ms since epoch; 0 when the node never held a lease."""
        lease = self.leases.get(node_id) or {}
        if not lease.get("granted_at"):
            return 0
        return int(lease["granted_at"]) + int(lease.get("ttl_ms", 0))


class ArbitrationEvent(BaseModel):
    """One HA event from a node, queued by the Go collector for the arbiter.

    Key ``{cluster_id}/{node_id}/{instance}/{seq:020d}``: a prefix read per
    cluster returns them in order per node, and a redelivered event (at-least-
    once, contract section 3.7) overwrites its own key, so the queue never
    holds duplicates. The arbiter removes an event once it has applied it.
    ``ha_resync`` events carry the node's full ``jc_ha_status`` in ``payload``.
    """
    cluster_id: str = ""
    node_id: str = ""
    instance: str = ""
    seq: int = 0
    received_at: int = 0
    payload: dict = default_factory(dict)

    def get_id(self):
        return "%s/%s/%s/%020d" % (self.cluster_id, self.node_id, self.instance, int(self.seq))
