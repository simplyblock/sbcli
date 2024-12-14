# coding=utf-8
from datetime import datetime

from simplyblock_core.models.base_model import BaseModel


class EventObj(BaseModel):

    LEVEL_DEBUG = "Debug"
    LEVEL_INFO = "Info"
    LEVEL_WARN = "Warning"
    LEVEL_CRITICAL = "Critical"
    LEVEL_ERROR = "Error"

    caused_by: str = ""
    cluster_uuid: str = ""
    count: int = 1
    date: int = 0
    domain: str = ""
    event: str = ""
    event_level: str = "Info"
    message: str = ""
    meta_data: str = ""
    node_id: str = ""
    object_dict: dict = {}
    object_name: str = ""
    storage_id: int = -1
    vuid: int = -1

    def get_id(self):
        return "%s/%s/%s" % (self.cluster_uuid, self.date, self.uuid)

    def get_date_string(self):
        if self.date > 1e10:
            return str(datetime.fromtimestamp(self.date/1000))[:23]
        else:
            return str(datetime.fromtimestamp(self.date))[:23]
