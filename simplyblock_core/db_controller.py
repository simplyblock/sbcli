# coding=utf-8
import logging
import os.path

import fdb
from typing import List

from simplyblock_core import constants
from simplyblock_core.models.caching_node import CachingNode
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.deployer import Deployer
from simplyblock_core.models.events import EventObj
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.mgmt_node import MgmtNode
from simplyblock_core.models.nvme_device import NVMeDevice, JMDevice
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.port_stat import PortStat
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.stats import DeviceStatObject, NodeStatObject, ClusterStatObject, LVolStatObject, \
    PoolStatObject, CachedLVolStatObject
from simplyblock_core.models.storage_node import StorageNode

logger = logging.getLogger()


class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls in cls._instances:
            return cls._instances[cls]
        else:
            ins = super(Singleton, cls).__call__(*args, **kwargs)
            if ins is not None and ins.kv_store is not None:
                cls._instances[cls] = ins
            return ins



class DBController(metaclass=Singleton):

    kv_store=None

    def __init__(self):
        try:
            if not os.path.isfile(constants.KVD_DB_FILE_PATH):
                return
            fdb.api_version(constants.KVD_DB_VERSION)
            self.kv_store = fdb.open(constants.KVD_DB_FILE_PATH)
            self.kv_store.options.set_transaction_timeout(constants.KVD_DB_TIMEOUT_MS)
        except Exception as e:
            logger.error(e)

    def get_storage_nodes(self) -> List[StorageNode]:
        ret = StorageNode().read_from_db(self.kv_store)
        ret = sorted(ret, key=lambda x: x.create_dt)
        return ret

    def get_storage_nodes_by_cluster_id(self, cluster_id) -> List[StorageNode]:
        ret = StorageNode().read_from_db(self.kv_store)
        nodes = []
        for n in ret:
            if n.cluster_id == cluster_id:
                nodes.append(n)
        return sorted(nodes, key=lambda x: x.create_dt)

    def get_storage_node_by_system_id(self, system_id) -> StorageNode:
        nodes = StorageNode().read_from_db(self.kv_store)
        for node in nodes:
            if node.system_uuid == system_id:
                return node

    def get_storage_node_by_id(self, id) -> StorageNode:
        ret = StorageNode().read_from_db(self.kv_store, id)
        if ret:
            return ret[0]

    # todo: change this function for multi cluster
    def get_caching_nodes(self) -> List[CachingNode]:
        ret = CachingNode().read_from_db(self.kv_store)
        ret = sorted(ret, key=lambda x: x.create_dt)
        return ret

    def get_caching_node_by_id(self, id)  -> CachingNode:
        ret = CachingNode().read_from_db(self.kv_store, id)
        if ret:
            return ret[0]

    def get_caching_node_by_system_id(self, system_id)  -> List[CachingNode]:
        nodes = CachingNode().read_from_db(self.kv_store)
        for node in nodes:
            if node.system_uuid == system_id:
                return node

    def get_caching_node_by_hostname(self, hostname)  -> CachingNode:
        nodes = self.get_caching_nodes()
        for node in nodes:
            if node.hostname == hostname:
                return node

    def get_storage_node_by_hostname(self, hostname) -> StorageNode:
        nodes = self.get_storage_nodes()
        for node in nodes:
            if node.hostname == hostname:
                return node

    def get_storage_device_by_id(self, id) -> NVMeDevice:
        nodes = self.get_storage_nodes()
        for node in nodes:
            for dev in node.nvme_devices:
                if dev.get_id() == id:
                    return dev

    def get_pools(self, cluster_id=None) -> List[Pool]:
        pools = []
        if cluster_id:
            for pool in Pool().read_from_db(self.kv_store):
                if pool.cluster_id == cluster_id:
                    pools.append(pool)
        else:
            pools = Pool().read_from_db(self.kv_store)
        return pools

    def get_pool_by_id(self, id) -> Pool:
        ret = Pool().read_from_db(self.kv_store, id)
        if ret:
            return ret[0]

    def get_pool_by_name(self, name) -> Pool:
        pools = Pool().read_from_db(self.kv_store)
        for pool in pools:
            if pool.pool_name == name:
                return pool

    def get_lvols(self, cluster_id=None) -> List[LVol]:
        lvols = self.get_all_lvols()
        if not cluster_id:
            return sorted(lvols, key=lambda x: x.create_dt)

        node_ids=[]
        cluster_lvols = []
        for node in self.get_storage_nodes_by_cluster_id(cluster_id):
            node_ids.append(node.get_id())

        for lvol in lvols:
            if lvol.node_id in node_ids:
                cluster_lvols.append(lvol)

        return sorted(cluster_lvols, key=lambda x: x.create_dt)

    def get_all_lvols(self) -> List[LVol]:
        lvols = LVol().read_from_db(self.kv_store)
        return sorted(lvols, key=lambda x: x.create_dt)

    def get_lvols_by_node_id(self, node_id) -> List[LVol]:
        lvols = []
        for lvol in self.get_lvols():
            if lvol.node_id == node_id:
                lvols.append(lvol)
        return sorted(lvols, key=lambda x: x.create_dt)

    def get_lvols_by_pool_id(self, pool_id) -> List[LVol]:
        lvols = []
        for lvol in self.get_lvols():
            if lvol.pool_uuid == pool_id:
                lvols.append(lvol)
        return sorted(lvols, key=lambda x: x.create_dt)

    def get_snapshots(self) -> List[SnapShot]:
        ret = SnapShot().read_from_db(self.kv_store)
        return ret

    def get_snapshot_by_id(self, id) -> SnapShot:
        ret = SnapShot().read_from_db(self.kv_store, id)
        if ret:
            return ret[0]

    def get_lvol_by_id(self, id) -> LVol:
        ret = LVol().read_from_db(self.kv_store, id)
        if ret:
            return ret[0]

    def get_lvol_by_name(self, lvol_name) -> LVol:
        for lvol in self.get_lvols():
            if lvol.lvol_name == lvol_name:
                return lvol

    def get_mgmt_node_by_id(self, id) -> MgmtNode:
        ret = MgmtNode().read_from_db(self.kv_store, id)
        if ret:
            return ret[0]

    def get_mgmt_nodes(self, cluster_id=None) -> List[MgmtNode]:
        nodes = MgmtNode().read_from_db(self.kv_store)
        if cluster_id:
            nodes = [n for n in nodes if n.cluster_id == cluster_id]
        return sorted(nodes, key=lambda x: x.create_dt)

    def get_mgmt_node_by_hostname(self, hostname) -> MgmtNode:
        nodes = self.get_mgmt_nodes()
        for node in nodes:
            if node.hostname == hostname:
                return node

    def get_lvol_stats(self, lvol, limit=20) -> List[LVolStatObject]:
        if isinstance(lvol, str):
            lvol = self.get_lvol_by_id(lvol)
        stats = LVolStatObject().read_from_db(self.kv_store, id="%s/%s" % (lvol.pool_uuid, lvol.uuid), limit=limit,
                                              reverse=True)
        return stats

    def get_cached_lvol_stats(self, lvol_id, limit=20) -> List[CachedLVolStatObject]:
        stats = CachedLVolStatObject().read_from_db(self.kv_store, id="%s/%s" % (lvol_id, lvol_id), limit=limit,
                                                    reverse=True)
        return stats

    def get_pool_stats(self, pool, limit=20) -> List[PoolStatObject]:
        stats = PoolStatObject().read_from_db(self.kv_store, id="%s/%s" % (pool.get_id(), pool.get_id()), limit=limit,
                                              reverse=True)
        return stats

    def get_cluster_stats(self, cluster, limit=20) -> List[ClusterStatObject]:
        return self.get_cluster_capacity(cluster, limit)

    def get_node_stats(self, node, limit=20) -> List[NodeStatObject]:
        return self.get_node_capacity(node, limit)

    def get_device_stats(self, device, limit=20) -> List[DeviceStatObject]:
        return self.get_device_capacity(device, limit)

    def get_cluster_capacity(self, cl, limit=1) -> List[ClusterStatObject]:
        stats = ClusterStatObject().read_from_db(
            self.kv_store, id="%s/%s" % (cl.get_id(), cl.get_id()), limit=limit, reverse=True)
        return stats

    def get_node_capacity(self, node, limit=1) -> List[NodeStatObject]:
        stats = NodeStatObject().read_from_db(
            self.kv_store, id="%s/%s" % (node.cluster_id, node.get_id()), limit=limit, reverse=True)
        return stats

    def get_device_capacity(self, device, limit=1) -> List[DeviceStatObject]:
        stats = DeviceStatObject().read_from_db(
            self.kv_store, id="%s/%s" % (device.cluster_id, device.get_id()), limit=limit, reverse=True)
        return stats

    def get_clusters(self) -> List[Cluster]:
        return Cluster().read_from_db(self.kv_store)

    def get_cluster_by_id(self, cluster_id) -> Cluster:
        ret = Cluster().read_from_db(self.kv_store, id=cluster_id)
        if ret:
            return ret[0]

    def get_deployers(self) -> List[Deployer]:
        return Deployer().read_from_db(self.kv_store)

    def get_deployer_by_id(self, deployer_id) -> Deployer:
        ret = Deployer().read_from_db(self.kv_store, id=deployer_id)
        if ret:
            return ret[0]

    def get_port_stats(self, node_id, port_id, limit=20) -> List[PortStat]:
        stats = PortStat().read_from_db(self.kv_store, id="%s/%s" % (node_id, port_id), limit=limit, reverse=True)
        return stats

    def get_events(self, event_id=" ") -> List[EventObj]:
        return EventObj().read_from_db(self.kv_store, id=event_id)

    def get_job_tasks(self, cluster_id, reverse=True) -> List[JobSchedule]:
        return JobSchedule().read_from_db(self.kv_store, id=cluster_id, reverse=reverse)

    def get_task_by_id(self, task_id) -> JobSchedule:
        for task in self.get_job_tasks(" "):
            if task.uuid == task_id:
                return task

    def get_snapshots_by_node_id(self, node_id) -> List[SnapShot]:
        ret = []
        snaps = SnapShot().read_from_db(self.kv_store)
        for snap in snaps:
            if snap.lvol.node_id == node_id:
                ret.append(snap)
        return ret

    def get_snode_size(self, node_id) -> int:
        snode = self.get_storage_node_by_id(node_id)
        total_node_capacity = 0
        for dev in snode.nvme_devices:
            total_node_capacity += dev.size
        return total_node_capacity

    def get_jm_device_by_id(self, jm_id) -> JMDevice:
        for node in self.get_storage_nodes():
            if node.jm_device and node.jm_device.get_id() == jm_id:
                return node.jm_device

    def get_primary_storage_nodes_by_cluster_id(self, cluster_id) -> List[StorageNode]:
        ret = StorageNode().read_from_db(self.kv_store)
        nodes = []
        for n in ret:
            if n.cluster_id == cluster_id and not n.is_secondary_node:  # pass
                nodes.append(n)
        return sorted(nodes, key=lambda x: x.create_dt)

    def get_primary_storage_nodes_by_secondary_node_id(self, node_id) -> List[StorageNode]:
        ret = StorageNode().read_from_db(self.kv_store)
        nodes = []
        for node in ret:
            if node.secondary_node_id == node_id and node.lvstore:
                nodes.append(node)
        return sorted(nodes, key=lambda x: x.create_dt)
