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
    STATUS_CANNOT_ALLOCATE = 'cannot_allocate'

    _STATUS_CODE_MAP = {
        STATUS_ONLINE: 1,
        STATUS_NEW: 2,
        STATUS_UNAVAILABLE: 3,
        STATUS_REMOVED: 4,
        STATUS_FAILED: 5,
        STATUS_READONLY: 6,
        STATUS_JM: 7,
        STATUS_CANNOT_ALLOCATE: 8

    }

    alceml_bdev: str = ""
    alceml_name: str = ""
    bdev_stack: List = []
    capacity: int = -1
    cluster_device_order: int = -1
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
    nvmf_multipath: bool = False
    pcie_address: str = ""
    physical_label: int = 0
    pt_bdev: str = ""
    qos_bdev: str = ""
    remote_bdev: str = ""
    retries_exhausted: bool = False
    serial_number: str = ""
    size: int = -1
    testing_bdev: str = ""
    connecting_from_node: str = ""
    previous_status: str = ""

    def __change_dev_connection_to(self, connecting_from_node):
        from simplyblock_core.db_controller import DBController
        db = DBController()
        for n in db.get_storage_nodes():
            if n.nvme_devices:
                for d in n.nvme_devices:
                    if d.get_id() == self.get_id():
                        d.connecting_from_node = connecting_from_node
                        n.write_to_db()
                        break

    def lock_device_connection(self, node_id):
        self.__change_dev_connection_to(node_id)

    def release_device_connection(self):
        self.__change_dev_connection_to("")

    def is_connection_in_progress_to_node(self, node_id):
        if self.connecting_from_node and self.connecting_from_node == node_id:
            return True


class JMDevice(NVMeDevice):

    device_data_dict: dict = {}
    jm_bdev: str = ""
    jm_nvme_bdev_list: List[str] = []
    raid_bdev: str = ""


class RemoteDevice(BaseModel):

    remote_bdev: str = ""
    alceml_name: str = ""
    node_id: str = ""
    size: int = -1
    nvmf_multipath: bool = False


class RemoteJMDevice(RemoteDevice):

    jm_bdev: str = ""

