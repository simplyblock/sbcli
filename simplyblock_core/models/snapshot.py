# coding=utf-8

from simplyblock_core.models.base_model import BaseModel
from simplyblock_core.models.lvol_model import LVol


class SnapShot(BaseModel):
    attributes = {
        "uuid": {"type": str, 'default': ""},
        "snap_name": {"type": str, 'default': 0},
        "snap_bdev": {"type": str, 'default': 0},
        "lvol": {"type": LVol, 'default': None},
        "created_at": {"type": int, 'default': 0},
    }

    def __init__(self, data=None):
        super(SnapShot, self).__init__()
        self.set_attrs(self.attributes, data)
        self.object_type = "object"

    def get_id(self):
        return self.uuid
