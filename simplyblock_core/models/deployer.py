# coding=utf-8

from typing import Mapping, List

from simplyblock_core.models.base_model import BaseModel

class Deployer(BaseModel):

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
        "jobid": {"type": str, 'default': ""},
        "snodes": {"type": str, 'default': ""},
        "az": {"type": str, 'default': ""},
        "cluster_id": {"type": str, "default": ""},
    }

    def __init__(self, data=None):
        super(Deployer, self).__init__()
        self.set_attrs(self.attributes, data)
        self.object_type = "object"

    def get_id(self):
        return self.uuid

    def get_status_code(self):
        if self.status in self.STATUS_CODE_MAP:
            return self.STATUS_CODE_MAP[self.status]
        else:
            return -1
