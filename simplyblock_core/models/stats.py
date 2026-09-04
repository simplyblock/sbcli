import json
import uuid
from typing import TypedDict

from simplyblock_core.models.base_model import BaseModel, default_factory


class StatsObject(BaseModel):

    capacity_dict: dict = default_factory(dict)
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
    # size_total/size_used/size_free above are EFFECTIVE (client-visible)
    # bytes at every level, so they are directly comparable with size_prov
    # (the sum of provisioned lvol sizes) and with the lvol/snapshot figures.
    # The raw (physical, parity-inclusive) numbers the devices actually
    # reported are kept here; see simplyblock_core.utils.capacity. Zero on
    # records written before this split existed, and on levels whose collector
    # does not measure raw capacity (lvol, pool).
    size_total_raw: int = 0
    size_used_raw: int = 0
    size_free_raw: int = 0
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


# `total=False` throughout: these records are read back from FoundationDB and
# may have been written by an older collector, so every key is genuinely
# optional at read time. Key names and value types are still checked — only
# presence is not. Readers get `Optional[...]` from `.get()` and have to say
# what absence means rather than defaulting it away.
class ThreadStats(TypedDict, total=False):
    id: int
    name: str
    busy: int
    idle: int


class ReactorStats(TypedDict, total=False):
    lcore: int
    busy: int
    idle: int
    irq: int
    sys: int
    threads: list[ThreadStats]


class CpuStats(TypedDict, total=False):
    reactors: list[ReactorStats]


class NodeStatObject(StatsObject):
    cpu_dict: CpuStats = default_factory(CpuStats)


class ClusterStatObject(StatsObject):
    pass


class LVolStatObject(StatsObject):

    def get_id(self):
        return "%s/%s/%s" % (self.pool_id, self.uuid, self.date)


class PoolStatObject(LVolStatObject):
    pass


class CachedLVolStatObject(StatsObject):
    pass
