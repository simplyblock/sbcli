# coding=utf-8
from simplyblock_core.models.base_model import BaseModel


class JobSchedule(BaseModel):

    STATUS_NEW = 'new'
    STATUS_RUNNING = 'running'
    STATUS_DONE = 'done'

    attributes = {
        "uuid": {"type": str, 'default': ""},
        "cluster_uuid": {"type": str, 'default': ""},
        "node_id": {"type": str, 'default': ""},
        "date": {"type": int, 'default': 0},

        "function_name": {"type": str, 'default': ""},
        "function_params": {"type": dict, 'default': {}},

        "status": {"type": str, 'default': ""},


        "event": {"type": str, 'default': ""},
        "domain": {"type": str, 'default': ""},
        "object_name": {"type": str, 'default': ""},
        "object_dict": {"type": dict, 'default': {}},
        "caused_by": {"type": str, 'default': ""},
        "message": {"type": str, 'default': ""},
        "storage_id": {"type": int, 'default': -1},
        "vuid": {"type": int, 'default': -1},
        "meta_data": {"type": str, 'default': ""},


    }

    def __init__(self, data=None):
        super(JobSchedule, self).__init__()
        self.set_attrs(self.attributes, data)
        self.object_type = "object"

    def get_id(self):
        return "%s/%s/%s" % (self.cluster_uuid, self.date, self.uuid)
