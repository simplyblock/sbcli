# coding=utf-8

from datetime import datetime
from typing import List

from simplyblock_core.models.base_model import BaseModel
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.nvme_device import NVMeDevice


class StorageNode(BaseModel):

    STATUS_ONLINE = 'online'
    STATUS_OFFLINE = 'offline'
    STATUS_SUSPENDED = 'suspended'
    STATUS_IN_CREATION = 'in_creation'
    STATUS_IN_SHUTDOWN = 'in_shutdown'
    STATUS_RESTARTING = 'restarting'
    STATUS_UNREACHABLE = 'unreachable'
    STATUS_REMOVED = 'removed'

    STATUS_CODE_MAP = {
        STATUS_ONLINE: 0,
        STATUS_OFFLINE: 1,
        STATUS_SUSPENDED: 2,
        STATUS_REMOVED: 3,

        STATUS_IN_CREATION: 10,
        STATUS_IN_SHUTDOWN: 11,
        STATUS_RESTARTING: 12,

        STATUS_UNREACHABLE: 20,
    }

    attributes = {
        "uuid": {"type": str, 'default': ""},
        "baseboard_sn": {"type": str, 'default': ""},
        "system_uuid": {"type": str, 'default': ""},
        "hostname": {"type": str, 'default': ""},
        "host_nqn": {"type": str, 'default': ""},
        "subsystem": {"type": str, 'default': ""},
        "nvme_devices": {"type": List[NVMeDevice], 'default': []},
        "sequential_number": {"type": int, 'default': 0},
        "partitions_count": {"type": int, 'default': 0},
        "ib_devices": {"type": List[IFace], 'default': []},
        "status": {"type": str, 'default': "in_creation"},
        "updated_at": {"type": str, 'default': str(datetime.now())},
        "create_dt": {"type": str, 'default': str(datetime.now())},
        "remove_dt": {"type": str, 'default': str(datetime.now())},
        "mgmt_ip": {"type": str, 'default': ""},
        "rpc_port": {"type": int, 'default': -1},
        "rpc_username": {"type": str, 'default': ""},
        "rpc_password": {"type": str, 'default': ""},
        "data_nics": {"type": List[IFace], 'default': []},
        "lvols": {"type": List[str], 'default': []},
        "node_lvs": {"type": str, 'default': "lvs"},
        "services": {"type": List[str], 'default': []},
        "cluster_id": {"type": str, 'default': ""},
        "api_endpoint": {"type": str, 'default': ""},
        "remote_devices": {"type": List[NVMeDevice], 'default': []},
        "host_secret": {"type": str, "default": ""},
        "ctrl_secret": {"type": str, "default": ""},

        "cpu": {"type": int, "default": 0},
        "cpu_hz": {"type": int, "default": 0},
        "memory": {"type": int, "default": 0},
        "hugepages": {"type": int, "default": 0},
        "health_check": {"type": bool, "default": True},

        # spdk params
        "spdk_cpu_mask": {"type": str, "default": ""},
        "spdk_mem": {"type": int, "default": 0},
        "spdk_image": {"type": str, "default": ""},
        "cmd_params": {"type": List, "default": []},
    }

    def __init__(self, data=None):
        super(StorageNode, self).__init__()
        self.set_attrs(self.attributes, data)
        self.object_type = "object"

    def get_id(self):
        return self.uuid

    def get_status_code(self):
        if self.status in self.STATUS_CODE_MAP:
            return self.STATUS_CODE_MAP[self.status]
        else:
            return -1
