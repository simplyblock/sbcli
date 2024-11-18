# coding=utf-8

from datetime import datetime
from typing import List

from simplyblock_core.models.base_model import BaseModel
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.nvme_device import NVMeDevice


class CachedLVol(BaseModel):

    attributes = {
        "uuid": {"type": str, 'default': ""},
        "lvol_id": {"type": str, 'default': ""},
        "lvol": {"type": LVol, 'default': None},
        "hostname": {"type": str, 'default': ""},
        "local_nqn": {"type": str, 'default': ""},
        "ocf_bdev": {"type": str, 'default': ""},
        "device_path": {"type": str, 'default': ""},
    }

    def __init__(self, data=None):
        super(CachedLVol, self).__init__()
        self.set_attrs(self.attributes, data)
        self.object_type = "object"

    def get_id(self):
        return self.uuid


class CachingNode(BaseModel):

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
        "lvols": {"type": List[CachedLVol], 'default': []},
        "node_lvs": {"type": str, 'default': "lvs"},
        "services": {"type": List[str], 'default': []},
        "cluster_id": {"type": str, 'default': ""},
        "api_endpoint": {"type": str, 'default': ""},
        "remote_devices": {"type": List[NVMeDevice], 'default': []},
        "host_secret": {"type": str, "default": ""},
        "ctrl_secret": {"type": str, "default": ""},

        "cache_bdev": {"type": str, "default": ""},
        "cache_size": {"type": int, "default": 0},
        "cache_split_factor": {"type": int, "default": 0},
        "cpu": {"type": int, "default": 0},
        "cpu_hz": {"type": int, "default": 0},
        "memory": {"type": int, "default": 0},
        "hugepages": {"type": int, "default": 0},
        "multipathing": {"type": bool, "default": True},
        "is_iscsi": {"type": bool, "default": False},
        "no_cache": {"type": bool, "default": False},

    }

    def __init__(self, data=None):
        super(CachingNode, self).__init__()
        self.set_attrs(self.attributes, data)
        self.object_type = "object"

    def get_id(self):
        return self.uuid

    def get_status_code(self):
        if self.status in self.STATUS_CODE_MAP:
            return self.STATUS_CODE_MAP[self.status]
        else:
            return -1
