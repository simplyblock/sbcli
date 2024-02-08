# coding=utf-8

from simplyblock_core.models.base_model import BaseModel


class PortStat(BaseModel):
    attributes = {
        "uuid": {"type": str, 'default': ""},
        "node_id": {"type": str, 'default': ""},
        "date": {"type": int, 'default': 0},

        "bytes_sent": {"type": int, 'default': 0},
        "bytes_received": {"type": int, 'default': 0},
        "packets_sent": {"type": int, 'default': 0},
        "packets_received": {"type": int, 'default': 0},
        "errin": {"type": int, 'default': 0},
        "errout": {"type": int, 'default': 0},
        "dropin": {"type": int, 'default': 0},
        "dropout": {"type": int, 'default': 0},

        "out_speed": {"type": int, 'default': 0},
        "in_speed": {"type": int, 'default': 0},

    }

    def __init__(self, data=None):
        super(PortStat, self).__init__()
        self.set_attrs(self.attributes, data)
        self.object_type = "object"

    def get_id(self):
        return "%s/%s/%s" % (self.node_id, self.uuid, self.date)
