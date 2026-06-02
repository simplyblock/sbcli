# coding=utf-8

from simplyblock_core.models.base_model import BaseModel
from simplyblock_core.models.lvol_model import LVol, LVolMini


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

    def write_to_db(self, kv_store=None):
        super().write_to_db(kv_store)
        snap_mini = SnapShotMini().from_snapshot(self)
        snap_mini.write_to_db(kv_store)

    def remove(self, kv_store):
        super().remove(kv_store)
        try:
            snap_mini = SnapShotMini().read_from_db(kv_store, self.uuid)[0]
            snap_mini.remove(kv_store)
        except Exception as e:
            print(f"Failed to remove snapshot mini from DB: {e}")


class SnapShotMini(BaseModel):
    snap_uuid: str = ""
    snap_name: str = ""
    pool_uuid: str = ""
    size: int = 0
    status: str = ""
    lvol: LVolMini = None # type: ignore[assignment]
    next_snap_uuid: str = ""
    prev_snap_uuid: str = ""
    vuid: int = 0
    created_at: int = 0
    used_size: int = 0

    def from_snapshot(self, snapshot: SnapShot):
        self.uuid = snapshot.uuid
        self.create_dt = snapshot.create_dt
        self.snap_uuid = snapshot.snap_uuid
        self.snap_name = snapshot.snap_name
        self.pool_uuid = snapshot.pool_uuid
        self.size = snapshot.size
        self.status = snapshot.status
        self.lvol = LVolMini().from_lvol(snapshot.lvol)
        self.next_snap_uuid = snapshot.next_snap_uuid
        self.prev_snap_uuid = snapshot.prev_snap_uuid
        self.vuid = snapshot.vuid
        self.created_at = snapshot.created_at
        self.used_size = snapshot.used_size
        return self
