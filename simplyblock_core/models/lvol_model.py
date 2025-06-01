# coding=utf-8

from typing import List

from simplyblock_core.models.base_model import BaseModel
from simplyblock_core.models.nvme_device import NVMeDevice


class LVol(BaseModel):

    STATUS_IN_CREATION = 'in_creation'
    STATUS_ONLINE = 'online'
    STATUS_OFFLINE = 'offline'
    STATUS_IN_DELETION = 'in_deletion'

    _STATUS_CODE_MAP = {
        STATUS_ONLINE: 1,
        STATUS_OFFLINE: 2,
        STATUS_IN_DELETION: 3,
        STATUS_IN_CREATION: 4,
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
    mem_diff: dict = {}
    mode: str = "read-write"
    namespace: str = ""
    node_id: str = ""
    nodes: List[str] = []
    nqn: str = ""
    ns_id: int = 1
    subsys_port: int = 9090
    nvme_dev: NVMeDevice = None
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

    def has_qos(self):
        return (self.rw_ios_per_sec > 0 or self.rw_mbytes_per_sec > 0 or self.r_mbytes_per_sec > 0 or self.w_mbytes_per_sec > 0)
