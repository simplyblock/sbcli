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
        "uuid": {"type": str, 'default': ""},
        "snodes": {"type": int, 'default': 0},
        "snodes_type": {"type": str, 'default': ""},
        "mnodes": {"type": int, 'default': 0},
        "mnodes_type": {"type": str, 'default': ""},
        "az": {"type": str, 'default': ""},
        "region": {"type": str, 'default': ""},
        "workspace": {"type": str, 'default': ""},
        "bucket_name": {"type": str, 'default': ""},
    }

    def __init__(self, data=None):
        super(Deployer, self).__init__()
        self.set_attrs(self.attributes, data)
        self.object_type = "object"

    def get_id(self):
        return self.uuid
