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

    STATUS_CODE_MAP = {
        STATUS_ONLINE: 1,
        STATUS_NEW: 2,
        STATUS_UNAVAILABLE: 3,
        STATUS_REMOVED: 4,
        STATUS_FAILED: 5,
        STATUS_READONLY: 6,
        STATUS_JM: 7

    }

    attributes = {
        "uuid": {"type": str, 'default': ""},
        "device_name": {"type": str, 'default': ""},
        "status": {"type": str, 'default': ""},
        "sequential_number": {"type": int, 'default': 0},
        "partitions_count": {"type": int, 'default': 0},
        "capacity": {"type": int, 'default': -1},
        "size": {"type": int, 'default': -1},
        "pcie_address": {"type": str, 'default': ""},
        "model_id": {"type": str, 'default': ""},
        "serial_number": {"type": str, 'default': ""},
        "overload_percentage": {"type": int, 'default': 0},
        "nvme_bdev": {"type": str, 'default': ""},
        "nvme_controller": {"type": str, 'default': ""},
        "alceml_bdev": {"type": str, 'default': ""},
        "node_id": {"type": str, 'default': ""},
        "pt_bdev": {"type": str, 'default': ""},
        "nvmf_nqn": {"type": str, 'default': ""},
        "nvmf_ip": {"type": str, 'default': ""},
        "nvmf_port": {"type": int, 'default': 0},
        "remote_bdev": {"type": str, 'default': ""},
        "testing_bdev": {"type": str, 'default': ""},
        "cluster_device_order": {"type": int, 'default': 0},
        "health_check": {"type": bool, "default": True},
        "cluster_id": {"type": str, 'default': ""},

        "bdev_stack": {"type": List, 'default': []},

        "io_error": {"type": bool, 'default': False},
        "retries_exhausted": {"type": bool, 'default': False},

        "partition_main_bdev": {"type": str, 'default': ""},
        "partition_main_size": {"type": int, 'default': 0},
        "partition_jm_bdev": {"type": str, 'default': ""},
        "partition_jm_size": {"type": int, 'default': 0},

        "physical_label": {"type": int, 'default': 0},
        "is_partition": {"type": bool, 'default': False},

    }

    def __init__(self, data=None):
        super(NVMeDevice, self).__init__()
        self.set_attrs(self.attributes, data)
        self.object_type = "object"

    def get_id(self):
        return self.uuid

    def get_status_code(self):
        if self.status in self.STATUS_CODE_MAP:
            return self.STATUS_CODE_MAP[self.status]
        else:
            return -1

    def get_clean_dict(self):
        data = super(NVMeDevice, self).get_clean_dict()
        data['status_code'] = self.get_status_code()
        return data


class JMDevice(BaseModel):

    STATUS_ONLINE = 'online'
    STATUS_UNAVAILABLE = 'unavailable'
    STATUS_REMOVED = 'removed'
    STATUS_FAILED = 'failed'
    STATUS_READONLY = 'read_only'

    attributes = {
        "uuid": {"type": str, 'default': ""},
        "device_name": {"type": str, 'default': ""},
        "status": {"type": str, 'default': ""},
        "size": {"type": int, 'default': -1},

        "jm_nvme_bdev_list": {"type": List[str], 'default': []},
        "raid_bdev": {"type": str, 'default': ""},
        "nvme_bdev": {"type": str, 'default': ""},
        "alceml_bdev": {"type": str, 'default': ""},
        "testing_bdev": {"type": str, 'default': ""},
        "jm_bdev": {"type": str, 'default': ""},
        "device_data_dict": {"type": dict, 'default': {}},

        "pt_bdev": {"type": str, 'default': ""},
        "nvmf_nqn": {"type": str, 'default': ""},
        "nvmf_ip": {"type": str, 'default': ""},
        "nvmf_port": {"type": int, 'default': 4420},
        "remote_bdev": {"type": str, 'default': ""},

        "health_check": {"type": bool, "default": True},
        "io_error": {"type": bool, 'default': False},
    }

    def __init__(self, data=None):
        super(JMDevice, self).__init__()
        self.set_attrs(self.attributes, data)
        self.object_type = "object"

    def get_id(self):
        return self.uuid

