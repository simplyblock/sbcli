# coding=utf-8
import uuid

from simplyblock_core.models.base_model import BaseModel


class StatsObject(BaseModel):

    attributes = {
        "cluster_id": {"type": str, 'default': ""},
        "uuid": {"type": str, 'default': ""},
        "date": {"type": int, 'default': 0},

        # io stats
        "read_bytes": {"type": int, 'default': 0},
        "read_io": {"type": int, 'default': 0},
        "read_bytes_ps": {"type": int, 'default': 0},
        "read_io_ps": {"type": int, 'default': 0},
        "read_latency_ticks": {"type": int, 'default': 0},
        "read_latency_ps": {"type": int, 'default': 0},

        "write_bytes": {"type": int, 'default': 0},
        "write_io": {"type": int, 'default': 0},
        "write_bytes_ps": {"type": int, 'default': 0},
        "write_io_ps": {"type": int, 'default': 0},
        "write_latency_ticks": {"type": int, 'default': 0},
        "write_latency_ps": {"type": int, 'default': 0},

        "unmap_bytes": {"type": int, 'default': 0},
        "unmap_io": {"type": int, 'default': 0},
        "unmap_bytes_ps": {"type": int, 'default': 0},
        "unmap_io_ps": {"type": int, 'default': 0},
        "unmap_latency_ticks": {"type": int, 'default': 0},
        "unmap_latency_ps": {"type": int, 'default': 0},

        # capacity stats
        "size_total": {"type": int, 'default': 0},
        "size_used": {"type": int, 'default': 0},
        "size_free": {"type": int, 'default': 0},
        "size_util": {"type": int, 'default': 0},
        "size_prov": {"type": int, 'default': 0},
        "size_prov_util": {"type": int, 'default': 0},

        "capacity_dict": {"type": dict, 'default': {}},

    }

    def __init__(self, data=None):
        super(StatsObject, self).__init__()
        self.set_attrs(self.attributes, data)
        self.object_type = "object"

    def get_id(self):
        return "%s/%s/%s" % (self.cluster_id, self.uuid, self.date)

    def keys(self):
        return self.attributes

    def __add__(self, other):
        data = {
            "cluster_id": self.cluster_id,
            "uuid": str(uuid.uuid4())}
        if isinstance(other, StatsObject):
            self_dict = self.to_dict()
            other_dict = other.to_dict()
            for attr in self.attributes:
                if self.attributes[attr]['type'] in [int, float]:
                    data[attr] = self_dict[attr] + other_dict[attr]
        return StatsObject(data)

    def __sub__(self, other):
        data = {
            "cluster_id": self.cluster_id,
            "uuid": str(uuid.uuid4())}
        if isinstance(other, StatsObject):
            self_dict = self.to_dict()
            other_dict = other.to_dict()
            for attr in self.attributes:
                if self.attributes[attr]['type'] in [int, float]:
                    data[attr] = self_dict[attr] - other_dict[attr]
        return StatsObject(data)


class DeviceStatObject(StatsObject):
    pass


class NodeStatObject(StatsObject):
    pass


class ClusterStatObject(StatsObject):
    pass


class LVolStatObject(StatsObject):

    def __init__(self, data=None):
        super(StatsObject, self).__init__()
        attributes = self.attributes
        attributes["pool_id"] = {"type": str, 'default': ""}
        self.set_attrs(attributes, data)
        self.object_type = "object"

    def get_id(self):
        return "%s/%s/%s" % (self.pool_id, self.uuid, self.date)


class PoolStatObject(LVolStatObject):
    pass
