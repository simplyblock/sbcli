# coding=utf-8
from simplyblock_core.models.base_model import BaseModel


class ClusterRestartLock(BaseModel):
    """Distributed lock ensuring only one node restarts at a time per cluster.

    Stored in FDB keyed by cluster_id. Includes TTL for automatic
    expiration if the holding process crashes.
    """

    cluster_id: str = ""
    node_id: str = ""
    acquired_at: int = 0
    ttl_seconds: int = 1800
    holder_id: str = ""

    def get_id(self):
        return self.cluster_id
