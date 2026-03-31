# coding=utf-8

from simplyblock_core.models.base_model import BaseModel
from simplyblock_core.models.lvol_model import LVol


class SnapShot(BaseModel):

    STATUS_ONLINE = 'online'
    STATUS_OFFLINE = 'offline'
    STATUS_IN_DELETION = 'in_deletion'
    STATUS_IN_REPLICATION = 'in_replication'

    base_bdev: str = ""
    blobid: int = 0
    cluster_id: str = ""
    created_at: int = 0
    health_check: bool = True
    lvol: LVol = None # type: ignore[assignment]
    mem_diff: dict = {}
    pool_uuid: str = ""
    ref_count: int = 0
    size: int = 0
    used_size: int = 0
    snap_bdev: str = ""
    snap_name: str = ""
    snap_ref_id: str = ""
    snap_uuid: str = ""
    vuid: int = 0
    deletion_status: str = ""
    status: str = ""
    fabric: str = "tcp"
    target_replicated_snap_uuid: str = ""
    source_replicated_snap_uuid: str = ""
    next_snap_uuid: str = ""
    prev_snap_uuid: str = ""
    instances: list = []