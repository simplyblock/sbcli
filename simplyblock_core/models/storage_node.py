# coding=utf-8

from datetime import datetime
from typing import List

from simplyblock_core.models.base_model import BaseModel
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.nvme_device import NVMeDevice, JMDevice


class StorageNode(BaseModel):

    STATUS_ONLINE = 'online'
    STATUS_OFFLINE = 'offline'
    STATUS_SUSPENDED = 'suspended'
    STATUS_IN_SHUTDOWN = 'in_shutdown'
    STATUS_REMOVED = 'removed'
    STATUS_RESTARTING = 'in_restart'

    STATUS_IN_CREATION = 'in_creation'
    STATUS_UNREACHABLE = 'unreachable'
    STATUS_SCHEDULABLE = 'schedulable'

    STATUS_CODE_MAP = {
        STATUS_ONLINE: 0,
        STATUS_OFFLINE: 1,
        STATUS_SUSPENDED: 2,
        STATUS_REMOVED: 3,

        STATUS_IN_CREATION: 10,
        STATUS_IN_SHUTDOWN: 11,
        STATUS_RESTARTING: 12,

        STATUS_UNREACHABLE: 20,

        STATUS_SCHEDULABLE: 30,
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
        "primary_ip": {"type": str, 'default': ""},
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
        "max_lvol": {"type": int, "default": 0},
        "max_snap": {"type": int, "default": 0},
        "max_prov": {"type": int, "default": 0},
        "number_of_devices": {"type": int, "default": 0},
        "cpu": {"type": int, "default": 0},
        "cpu_hz": {"type": int, "default": 0},
        "memory": {"type": int, "default": 0},
        "hugepages": {"type": int, "default": 0},
        "health_check": {"type": bool, "default": True},
        "enable_test_device": {"type": bool, "default": False},
        "number_of_distribs": {"type": int, "default": 4},
        "lvstore": {"type": str, 'default': ""},
        "raid": {"type": str, 'default': ""},
        "lvstore_stack": {"type": List[dict], 'default': []},
        "jm_vuid": {"type": int, "default": 0},

        # spdk params
        "spdk_cpu_mask": {"type": str, "default": ""},
        "app_thread_mask": {"type": str, "default": ""},
        "pollers_mask": {"type": str, "default": ""},
        "poller_cpu_cores": {"type": List[int], "default": []},
        "jm_cpu_mask": {"type": str, "default": ""},
        "alceml_cpu_cores": {"type": List[int], "default": []},
        "alceml_worker_cpu_cores": {"type": List[int], "default": []},
        "distrib_cpu_cores": {"type": List[int], "default": []},
        "alceml_cpu_index": {"type": int, "default": 0},
        "alceml_worker_cpu_index": {"type": int, "default": 0},
        "distrib_cpu_index": {"type": int, "default": 0},
        "jc_singleton_mask": {"type": str, "default": ""},


        "distrib_cpu_mask": {"type": str, "default": ""},

        "spdk_mem": {"type": int, "default": 0},
        "spdk_image": {"type": str, "default": ""},
        "spdk_debug": {"type": bool, "default": False},

        "cloud_instance_id": {"type": str, "default": ""},
        "cloud_instance_type": {"type": str, "default": ""},
        "cloud_instance_public_ip": {"type": str, "default": ""},

        # IO buffer options
        "iobuf_small_pool_count": {"type": int, "default": 0},
        "iobuf_large_pool_count": {"type": int, "default": 0},
        "iobuf_small_bufsize": {"type": int, "default": 0},
        "iobuf_large_bufsize": {"type": int, "default": 0},

        "num_partitions_per_dev": {"type": int, "default": 1},
        "jm_percent": {"type": int, "default": 3},
        "jm_device": {"type": JMDevice, "default": None},
        "remote_jm_devices": {"type": List[JMDevice], 'default': []},
        "enable_ha_jm": {"type": bool, "default": False},

        "namespace": {"type": str, "default": ""},

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

    def get_clean_dict(self):
        data = super(StorageNode, self).get_clean_dict()
        data['status_code'] = self.get_status_code()
        return data
