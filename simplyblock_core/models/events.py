# coding=utf-8
from datetime import datetime

from simplyblock_core.models.base_model import BaseModel


class EventObj(BaseModel):
    LEVEL_DEBUG = "Debug"
    LEVEL_INFO = "Info"
    LEVEL_WARN = "Warning"
    LEVEL_CRITICAL = "Critical"
    LEVEL_ERROR = "Error"

    """
    uuid:
    cluster_uuid: 1234
    event: STATUS_CHANGE
    domain: Cluster, Management, Storage
    object_name: cluster,
    object_dict:
    caused_by: CLI, API, MONITOR
    message:
    meta_data:
    date:
    """
    attributes = {
        "uuid": {"type": str, 'default': ""},
        "cluster_uuid": {"type": str, 'default': ""},
        "node_id": {"type": str, 'default': ""},
        "date": {"type": int, 'default': 0},  # in milliseconds

        "event_level": {"type": str, 'default': LEVEL_INFO},

        "event": {"type": str, 'default': ""},
        "domain": {"type": str, 'default': ""},
        "object_name": {"type": str, 'default': ""},
        "object_dict": {"type": dict, 'default': {}},
        "caused_by": {"type": str, 'default': ""},
        "message": {"type": str, 'default': ""},
        "storage_id": {"type": int, 'default': -1},
        "vuid": {"type": int, 'default': -1},
        "meta_data": {"type": str, 'default': ""},
        "status": {"type": str, 'default': ""},

    }

    def __init__(self, data=None):
        super(EventObj, self).__init__()
        self.set_attrs(self.attributes, data)
        self.object_type = "object"

    def get_id(self):
        return "%s/%s/%s" % (self.cluster_uuid, self.date, self.uuid)

    def get_date_string(self):
        if self.date > 1e10:
            return str(datetime.fromtimestamp(self.date/1000))
        else:
            return str(datetime.fromtimestamp(self.date))
