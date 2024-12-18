# coding=utf-8
from typing import List

from simplyblock_core.models.base_model import BaseModel


class NVMeDevice(BaseModel):

    STATUS_JM = "JM_DEV"

    STATUS_NEW = "new"
    STATUS_ONLINE = 'online'
    STATUS_UNAVAILABLE = 'unavailable'
    STATUS_REMOVED = 'removed'
    STATUS_FAILED = 'failed'
    STATUS_FAILED_AND_MIGRATED = 'failed_and_migrated'
    STATUS_READONLY = 'read_only'

    _STATUS_CODE_MAP = {
        STATUS_ONLINE: 1,
        STATUS_NEW: 2,
        STATUS_UNAVAILABLE: 3,
        STATUS_REMOVED: 4,
        STATUS_FAILED: 5,
        STATUS_READONLY: 6,
        STATUS_JM: 7

    }

    alceml_bdev: str = ""
    alceml_name: str = ""
    bdev_stack: List = []
    capacity: int = -1
    cluster_device_order: int = 0
    cluster_id: str = ""
    device_name: str = ""
    health_check: bool = True
    io_error: bool = False
    is_partition: bool = False
    model_id: str = ""
    node_id: str = ""
    nvme_bdev: str = ""
    nvme_controller: str = ""
    nvmf_ip: str = ""
    nvmf_nqn: str = ""
    nvmf_port: int = 0
    overload_percentage: int = 0
    partition_jm_bdev: str = ""
    partition_jm_size: int = 0
    partition_main_bdev: str = ""
    partition_main_size: int = 0
    partitions_count: int = 0
    pcie_address: str = ""
    physical_label: int = 0
    pt_bdev: str = ""
    qos_bdev: str = ""
    remote_bdev: str = ""
    retries_exhausted: bool = False
    sequential_number: int = 0
    serial_number: str = ""
    size: int = -1
    testing_bdev: str = ""


class JMDevice(NVMeDevice):

    device_data_dict: dict = {}
    jm_bdev: str = ""
    jm_nvme_bdev_list: List[str] = []
    raid_bdev: str = ""

