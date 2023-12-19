# coding=utf-8

from management.models.base_model import BaseModel


class CapacityStat(BaseModel):
    attributes = {
        "uuid": {"type": str, 'default': ""},
        "node_id": {"type": str, 'default': ""},
        "device_id": {"type": str, 'default': ""},
        "date": {"type": int, 'default': 0},

        "size_total": {"type": int, 'default': 0},
        "size_used": {"type": int, 'default': 0},
        "size_free": {"type": int, 'default': 0},
        "size_util": {"type": int, 'default': 0},
        "size_prov": {"type": int, 'default': 0},
        "size_prov_util": {"type": int, 'default': 0},

        "stats_dict": {"type": dict, 'default': {}},

            # "stats_dict": {
        #     "npages_allocated": {"type": int, 'default': 0},
        #     "npages_used": {"type": int, 'default': 0},
        #     "npages_nmax": {"type": int, 'default': 0},
        #     "pba_page_size": {"type": int, 'default': 0},
        #     "nvols": {"type": int, 'default': 0},
        # },
    }

    def __init__(self, data=None):
        super(CapacityStat, self).__init__()
        self.set_attrs(self.attributes, data)
        self.object_type = "object"

    def get_id(self):
        return "%s/%s/%s" % (self.node_id, self.device_id, self.date)
