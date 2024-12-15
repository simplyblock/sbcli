# coding=utf-8

from typing import List

from simplyblock_core.models.base_model import BaseModel, BaseNodeObject
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.nvme_device import NVMeDevice


class CachedLVol(BaseModel):

    device_path: str = ""
    hostname: str = ""
    local_nqn: str = ""
    lvol: LVol = None
    lvol_id: str = ""
    ocf_bdev: str = ""


class CachingNode(BaseNodeObject):

    api_endpoint: str = ""
    baseboard_sn: str = ""
    cache_bdev: str = ""
    cache_size: int = 0
    cache_split_factor: int = 0
    cluster_id: str = ""
    cpu: int = 0
    cpu_hz: int = 0
    ctrl_secret: str = ""
    data_nics: List[IFace] = []
    host_nqn: str = ""
    host_secret: str = ""
    hostname: str = ""
    hugepages: int = 0
    ib_devices: List[IFace] = []
    lvols: List[CachedLVol] = []
    memory: int = 0
    mgmt_ip: str = ""
    multipathing: bool = True
    node_lvs: str = "lvs"
    nvme_devices: List[NVMeDevice] = []
    partitions_count: int = 0
    remote_devices: List[NVMeDevice] = []
    rpc_password: str = ""
    rpc_port: int = -1
    rpc_username: str = ""
    sequential_number: int = 0
    services: List[str] = []
    subsystem: str = ""
    system_uuid: str = ""
