# coding=utf-8
from typing import Mapping

from management.models.base_model import BaseModel


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



class LVolStat(BaseModel):

    attributes = {
        "uuid": {"type": str, 'default': ""},
        "node_id": {"type": str, 'default': ""},
        "date": {"type": int, 'default': 0},

        "read_bytes_per_sec": {"type": int, 'default': 0},
        "read_iops": {"type": int, 'default': 0},
        "write_bytes_per_sec": {"type": int, 'default': 0},
        "write_iops": {"type": int, 'default': 0},
        "unmapped_bytes_per_sec": {"type": int, 'default': 0},

        "read_latency_ticks": {"type": int, 'default': 0},
        "write_latency_ticks": {"type": int, 'default': 0},
        "queue_depth": {"type": int, 'default': 0},  # queue depth, not included into CLI, but let us store now
        "io_time": {"type": int, 'default': 0},  # this is important to calculate utilization of disk (io_time2 - io_time1)/elapsed_time = % utilization of disk
        "weighted_io_timev": {"type": int, 'default': 0},  # still unclear how to use, but let us store now.

        "stats": {"type": dict, 'default': {}},

        # capacity attributes
        "npages_allocated": {"type": int, 'default': 0},
        "npages_used": {"type": int, 'default': 0},
        "npages_nmax": {"type": int, 'default': 0},
        "pba_page_size": {"type": int, 'default': 0},
        "nvols": {"type": int, 'default': 0},
    }

    def __init__(self, data=None):
        super(LVolStat, self).__init__()
        self.set_attrs(self.attributes, data)
        self.object_type = "object"

    def get_id(self):
        return "%s/%s/%s" % (self.node_id, self.uuid, self.date)


class DeviceStat(LVolStat):
    def __init__(self, data=None):
        super(LVolStat, self).__init__()
        self.set_attrs(self.attributes, data)
        self.object_type = "object"

    def get_id(self):
        return "%s/%s/%s" % (self.node_id, self.uuid, self.date)
