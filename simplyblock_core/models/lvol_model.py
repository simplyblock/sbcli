# coding=utf-8

from typing import List

from simplyblock_core.models.base_model import BaseModel
from simplyblock_core.models.nvme_device import NVMeDevice


class LVol(BaseModel):

    STATUS_ONLINE = 'online'
    STATUS_OFFLINE = 'offline'
    STATUS_IN_DELETION = 'in_deletion'

    attributes = {
        "lvol_name": {"type": str, 'default': ""},
        "size": {"type": int, 'default': 0},
        "max_size": {"type": int, 'default': 0},
        "uuid": {"type": str, 'default': ""},
        "guid": {"type": str, 'default': ""},
        "ha_type": {"type": str, 'default': ""},
        "status": {"type": str, 'default': ""},

        "base_bdev": {"type": str, 'default': ""},
        "lvol_bdev": {"type": str, 'default': ""},
        "lvs_name": {"type": str, 'default': ""},
        "comp_bdev": {"type": str, 'default': ""},
        "crypto_bdev": {"type": str, 'default': ""},
        "top_bdev": {"type": str, 'default': ""},

        "nvme_dev": {"type": NVMeDevice, 'default': None},
        "pool_uuid": {"type": str, 'default': ""},
        "hostname": {"type": str, 'default': ""},
        "node_id": {"type": str, 'default': ""},
        "nodes": {"type": List[str], 'default': []},

        "mode": {"type": str, 'default': "read-write"},
        "lvol_type": {"type": str, 'default': "lvol"},  # lvol, compressed, crypto, dedup
        "bdev_stack": {"type": List, 'default': []},

        "crypto_key_name": {"type": str, 'default': ""},

        "rw_ios_per_sec": {"type": int, 'default': 0},
        "rw_mbytes_per_sec": {"type": int, 'default': 0},
        "r_mbytes_per_sec": {"type": int, 'default': 0},
        "w_mbytes_per_sec": {"type": int, 'default': 0},

        "cloned_from_snap": {"type": str, 'default': ""},

        "nqn": {"type": str, 'default': ""},
        "vuid": {"type": int, 'default': 0},
        "ndcs": {"type": int, 'default': 0},
        "npcs": {"type": int, 'default': 0},
        "distr_bs": {"type": int, 'default': 0},
        "distr_chunk_bs": {"type": int, 'default': 0},
        "distr_page_size": {"type": int, 'default': 0},

        "health_check": {"type": bool, "default": True},

        "snapshot_name": {"type": str, 'default': ""},
        "mem_diff": {"type": dict, 'default': {}},
        "io_error": {"type": bool, 'default': False},

        "deletion_status": {"type": str, 'default': ""},

    }

    def __init__(self, data=None):
        super(LVol, self).__init__()
        self.set_attrs(self.attributes, data)
        self.object_type = "object"

    def get_id(self):
        return self.uuid
