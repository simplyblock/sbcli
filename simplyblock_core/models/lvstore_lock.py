# coding=utf-8
from simplyblock_core.models.base_model import BaseModel


class LVStoreMutationLock(BaseModel):
    """Per-lvstore mutex held across a snapshot's primary-create + replica
    registration sequence.

    Concurrent snapshot creates of the same lvstore (distinct API workers /
    threads) otherwise register their snapshots on the secondary/tertiary in
    arbitrary, thread-scheduling order. Because snapshots of an lvol form a
    chain, registering a child blob before its parent builds the replica blob
    tree inconsistently and corrupts the lvstore. Serializing the
    create→register-all-replicas section per lvstore makes replica registration
    follow primary creation (blobid) order.

    Stored in FDB keyed by ``cluster_id/lvs_name``. ``heartbeat_at`` is
    refreshed by the holder while the section runs; a lock whose heartbeat is
    older than ``LVSTORE_MUTATION_LOCK_TTL_SEC`` is considered abandoned (holder
    crashed) and may be reclaimed.
    """

    cluster_id: str = ""
    lvs_name: str = ""
    owner: str = ""
    acquired_at: int = 0
    heartbeat_at: int = 0

    def get_id(self):
        return f"{self.cluster_id}/{self.lvs_name}"
