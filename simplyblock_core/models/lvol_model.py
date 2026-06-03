# coding=utf-8

from typing import List

from simplyblock_core.models.base_model import BaseModel


class LVol(BaseModel):

    STATUS_IN_CREATION = 'in_creation'
    STATUS_ONLINE = 'online'
    STATUS_OFFLINE = 'offline'
    STATUS_IN_DELETION = 'in_deletion'
    STATUS_RESTORING = 'restoring'
    STATUS_DELETED = 'deleted'
    STATUS_RESTORE_FAILED = 'restore_failed'

    _STATUS_CODE_MAP = {
        STATUS_ONLINE: 1,
        STATUS_OFFLINE: 2,
        STATUS_IN_DELETION: 3,
        STATUS_IN_CREATION: 4,
        STATUS_RESTORING: 5,
        STATUS_DELETED: 6,
        STATUS_RESTORE_FAILED: 7,
    }

    base_bdev: str = ""
    bdev_stack: List = []
    blobid: int = 0
    cloned_from_snap: str = ""
    comp_bdev: str = ""
    crypto_bdev: str = ""
    crypto_key1: str = ""
    crypto_key2: str = ""
    crypto_key_name: str = ""
    deletion_status: str = ""
    guid: str = ""
    ha_type: str = ""
    health_check: bool = True
    hostname: str = ""
    io_error: bool = False
    lvol_bdev: str = ""
    lvol_name: str = ""
    lvol_priority_class: int = 0
    lvol_type: str = "lvol"
    lvol_uuid: str = ""
    lvs_name: str = ""
    max_size: int = 0
    namespace: str = ""
    node_id: str = ""
    nodes: List[str] = []
    nqn: str = ""
    ns_id: int = 1
    max_namespace_per_subsys: int = 1
    subsys_port: int = 9090
    pool_uuid: str = ""
    pool_name: str = ""
    pvc_name: str = ""
    r_mbytes_per_sec: int = 0
    rw_ios_per_sec: int = 0
    rw_mbytes_per_sec: int = 0
    size: int = 0
    snapshot_name: str = ""
    top_bdev: str = ""
    vuid: int = 0
    w_mbytes_per_sec: int = 0
    fabric: str = "tcp"
    ndcs: int = 0
    npcs: int = 0
    allowed_hosts: List[dict] = []
    delete_snap_on_lvol_delete: bool = False
    do_replicate: bool = False
    replication_node_id: str = ""
    from_source: bool = True

    def has_qos(self):
        return (self.rw_ios_per_sec > 0 or self.rw_mbytes_per_sec > 0 or self.r_mbytes_per_sec > 0 or self.w_mbytes_per_sec > 0)

    def write_to_db(self, kv_store=None):
        super().write_to_db(kv_store)
        lvol_mini = LVolMini().from_lvol(self)
        lvol_mini.write_to_db(kv_store)

    def remove(self, kv_store):
        super().remove(kv_store)
        try:
            lvol_mini = LVolMini().read_from_db(kv_store, self.uuid)[0]
            lvol_mini.remove(kv_store)
        except Exception as e:
            print(f"Failed to remove snapshot mini from DB: {e}")



class LVolReplication(BaseModel):
    source_lvol: LVol = None # type: ignore[assignment]
    target_lvol: LVol = None # type: ignore[assignment]
    source_cluster_id: str = ""
    target_cluster_id: str = ""

class LVolMini(BaseModel):
    lvol_uuid: str = ""
    lvol_name: str = ""
    pool_uuid: str = ""
    pool_name: str = ""
    size: int = 0
    vuid: int = 0
    status: str = ""
    cloned_from_snap: str = ""
    nqn: str = ""
    node_id: str = ""
    namespace: str = ""
    hostname: str = ""
    blobid: int = 0
    ns_id: int = 0
    max_namespace_per_subsys: int = 0

    def from_lvol(self, lvol: LVol):
        self.uuid = lvol.uuid
        self.create_dt = lvol.create_dt
        self.lvol_uuid = lvol.lvol_uuid
        self.lvol_name = lvol.lvol_name
        self.pool_uuid = lvol.pool_uuid
        self.pool_name = lvol.pool_name
        self.size = lvol.size
        self.vuid = lvol.vuid
        self.status = lvol.status
        self.cloned_from_snap = lvol.cloned_from_snap
        self.nqn = lvol.nqn
        self.node_id = lvol.node_id
        self.namespace = lvol.namespace
        self.hostname = lvol.hostname
        self.blobid = lvol.blobid
        self.ns_id = lvol.ns_id
        self.max_namespace_per_subsys = lvol.max_namespace_per_subsys
        return self
