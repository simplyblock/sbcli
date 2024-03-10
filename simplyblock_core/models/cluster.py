# coding=utf-8

from typing import Mapping, List

from simplyblock_core.models.base_model import BaseModel
from simplyblock_core.models.storage_node import StorageNode


class Cluster(BaseModel):

    STATUS_ACTIVE = "active"
    STATUS_READONLY = 'read_only'
    STATUS_INACTIVE = "inactive"
    STATUS_SUSPENDED = "suspended"
    STATUS_DEGRADED = "degraded"

    STATUS_CODE_MAP = {
        STATUS_ACTIVE: 0,
        STATUS_INACTIVE: 1,

        STATUS_SUSPENDED: 10,
        STATUS_DEGRADED: 11,

    }
    attributes = {
        "uuid": {"type": str, 'default': ""},
        "blk_size": {"type": int, 'default': 0},
        "page_size_in_blocks": {"type": int, 'default': 2097152},
        "model_ids": {"type": List[str], "default": []},
        "ha_type": {"type": str, 'default': "single"},
        "tls": {"type": bool, 'default': False},
        "auth_hosts_only": {"type": bool, 'default': False},
        "nqn": {"type": str, 'default': ""},
        "iscsi": {"type": str, 'default': ""},
        "dhchap": {"type": str, "default": ""},
        "cli_pass": {"type": str, "default": ""},
        "db_connection": {"type": str, "default": ""},

        ## cluster-level: cap-warn ( % ), cap-crit ( % ), prov-cap-warn ( % ), prov-cap-crit. ( % )
        "cap_warn": {"type": int, "default": 80},
        "cap_crit": {"type": int, "default": 90},
        "prov_cap_warn": {"type": int, "default": 180},
        "prov_cap_crit": {"type": int, "default": 190},

        "secret": {"type": str, "default": ""},
        "status": {"type": str, "default": ""},
        "updated_at": {"type": str, "default": ""},
    }

    def __init__(self, data=None):
        super(Cluster, self).__init__()
        self.set_attrs(self.attributes, data)
        self.object_type = "object"

    def get_id(self):
        return self.uuid

    def get_status_code(self):
        if self.status in self.STATUS_CODE_MAP:
            return self.STATUS_CODE_MAP[self.status]
        else:
            return -1


class ClusterMap(BaseModel):

    attributes = {
        "partitions_count": {"type": int, 'default': 0},
        "nodes": {"type": Mapping[str, StorageNode], 'default': {}},
    }

    def __init__(self, data=None):
        super(ClusterMap, self).__init__()
        self.set_attrs(self.attributes, data)
        self.object_type = "object"

    def get_id(self):
        return "0"

    def recalculate_partitions(self):
        self.partitions_count = 0
        for node_id in self.nodes:
            self.partitions_count += self.nodes[node_id].partitions_count
