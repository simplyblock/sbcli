# coding=utf-8

from simplyblock_core.models.base_model import BaseModel


class NVMeDevice(BaseModel):

    STATUS_ONLINE = 'online'
    STATUS_AVAILABLE = 'available'
    STATUS_UNAVAILABLE = 'unavailable'
    STATUS_READONLY = 'read_only'
    STATUS_OVERLOADED = 'overloaded'
    STATUS_FAILED = 'failed'
    STATUS_REMOVED = 'removed'
    STATUS_RESETTING = 'resetting'
    STATUS_UNRECOGNIZED = 'unrecognized'

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
        "alloc_bdev": {"type": str, 'default': ""},
        "alceml_bdev": {"type": str, 'default': ""},
        "node_id": {"type": str, 'default': ""},
        "pt_bdev": {"type": str, 'default': ""},
        "nvmf_nqn": {"type": str, 'default': ""},
        "nvmf_ip": {"type": str, 'default': ""},
        "nvmf_port": {"type": int, 'default': 0},
        "remote_bdev": {"type": str, 'default': ""},
        "testing_bdev": {"type": str, 'default': ""},
        "jm_bdev": {"type": str, 'default': ""},
        "cluster_device_order": {"type": int, 'default': 0},
        "health_check": {"type": bool, "default": True},
        "cluster_id": {"type": str, 'default': ""},

    }

    def __init__(self, data=None):
        super(NVMeDevice, self).__init__()
        self.set_attrs(self.attributes, data)
        self.object_type = "object"

    def get_id(self):
        return self.uuid

    def get_capacity_percentage(self):
        return ((self.size - self.capacity) / self.size) * 100
