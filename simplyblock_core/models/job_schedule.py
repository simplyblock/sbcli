# coding=utf-8
import datetime
import time

from simplyblock_core.models.base_model import BaseModel


class JobSchedule(BaseModel):

    STATUS_NEW = 'new'
    STATUS_RUNNING = 'running'
    STATUS_SUSPENDED = 'suspended'
    STATUS_DONE = 'done'

    FN_DEV_RESTART = "device_restart"
    FN_NODE_RESTART = "node_restart"
    FN_DEV_MIG = "device_migration"
    FN_FAILED_DEV_MIG = "failed_device_migration"
    FN_NEW_DEV_MIG = "new_device_migration"
    FN_NODE_ADD = "node_add"

    attributes = {
        "uuid": {"type": str, 'default': ""},
        "cluster_id": {"type": str, 'default': ""},
        "node_id": {"type": str, 'default': ""},
        "device_id": {"type": str, 'default': ""},
        "date": {"type": int, 'default': 0},

        "canceled": {"type": bool, 'default': False},

        "function_name": {"type": str, 'default': ""},
        "function_params": {"type": dict, 'default': {}},
        "function_result": {"type": str, 'default': ""},

        "retry": {"type": int, 'default': 0},
        "max_retry": {"type": int, 'default': -1},
        "status": {"type": str, 'default': ""},
        "updated_at": {"type": int, 'default': 0},

    }

    def __init__(self, data=None):
        super(JobSchedule, self).__init__()
        self.set_attrs(self.attributes, data)
        self.object_type = "object"

    def get_id(self):
        return "%s/%s/%s" % (self.cluster_id, self.date, self.uuid)

    def write_to_db(self, kv_store=None):
        self.updated_at = int(time.time())
        super().write_to_db(kv_store)
