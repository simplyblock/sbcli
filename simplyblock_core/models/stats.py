# coding=utf-8
import json
import uuid

from simplyblock_core.models.base_model import BaseModel
from simplyblock_core.models.pool import Pool


class StatsObject(BaseModel):

    capacity_dict: dict = {}
    cluster_id: str = ""
    connected_clients: int = 0
    date: int = 0
    read_bytes: int = 0
    read_bytes_ps: int = 0
    read_io: int = 0
    read_io_ps: int = 0
    read_latency_ps: int = 0
    read_latency_ticks: int = 0
    record_duration: int = 2
    record_end_time: int = 0
    record_start_time: int = 0
    pool_id: str = ""
    size_free: int = 0
    size_prov: int = 0
    size_prov_util: int = 0
    size_total: int = 0
    size_used: int = 0
    size_util: int = 0
    unmap_bytes: int = 0
    unmap_bytes_ps: int = 0
    unmap_io: int = 0
    unmap_io_ps: int = 0
    unmap_latency_ps: int = 0
    unmap_latency_ticks: int = 0
    write_bytes: int = 0
    write_bytes_ps: int = 0
    write_io: int = 0
    write_io_ps: int = 0
    write_latency_ps: int = 0
    write_latency_ticks: int = 0


    def get_id(self):
        return f"{self.cluster_id}/{self.uuid}/{self.date}/{self.record_duration}"

    def __add__(self, other):
        data = {
            "cluster_id": self.cluster_id,
            "uuid": str(uuid.uuid4())}
        if isinstance(other, StatsObject):
            self_dict = self.to_dict()
            other_dict = other.to_dict()
            for attr, value in self.get_attrs_map().items():
                if value['type'] in [int, float]:
                    data[attr] = self_dict[attr] + other_dict[attr]
        return StatsObject(data)

    def __sub__(self, other):
        data = {
            "cluster_id": self.cluster_id,
            "uuid": str(uuid.uuid4())}
        if isinstance(other, StatsObject):
            self_dict = self.to_dict()
            other_dict = other.to_dict()
            for attr, value in self.get_attrs_map().items():
                if value['type'] in [int, float]:
                    data[attr] = self_dict[attr] - other_dict[attr]
        return StatsObject(data)

    def get_range(self, kv_store, start_date, end_date):
        try:
            prefix = f"{self.object_type}/{self.name}/{self.cluster_id}/{self.uuid}"
            start_key = f"{prefix}/{start_date}"
            end_key = f"{prefix}/{end_date}"
            objects = []
            for k, v in kv_store.db.get_range(start_key.encode('utf-8'), end_key.encode('utf-8')):
                objects.append(self.__class__().from_dict(json.loads(v)))
            return objects
        except Exception as e:
            print(f"Error reading from FDB: {e}")
            return []


class DeviceStatObject(StatsObject):
    pass


class NodeStatObject(StatsObject):
    pass


class ClusterStatObject(StatsObject):
    pass


class LVolStatObject(StatsObject):

    def get_id(self):
        return "%s/%s/%s" % (self.pool_id, self.uuid, self.date)


class PoolStatObject(LVolStatObject):
    pass


class CachedLVolStatObject(StatsObject):
    pass
