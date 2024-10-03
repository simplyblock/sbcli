# coding=utf-8
import logging

import fdb
import time

from simplyblock_core import constants
from simplyblock_core.models.caching_node import CachingNode
from simplyblock_core.models.cluster import ClusterMap

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.deployer import Deployer
from simplyblock_core.models.compute_node import ComputeNode
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.port_stat import PortStat
from simplyblock_core.models.events import EventObj
from simplyblock_core.models.global_settings import GlobalSettings
from simplyblock_core.models.mgmt_node import MgmtNode
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.stats import DeviceStatObject, NodeStatObject, ClusterStatObject, LVolStatObject, \
    PoolStatObject, CachedLVolStatObject
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.lvol_model import LVol

logger = logging.getLogger()


class KVStore:
    def __init__(self):
        try:
            fdb.api_version(constants.KVD_DB_VERSION)
            self.db = fdb.open(constants.KVD_DB_FILE_PATH)
            self.db.options.set_transaction_timeout(constants.KVD_DB_TIMEOUT_MS)
        except Exception as e:
            pass
            # print(f"Error init db client, {e}")

    def set(self, key, value):
        self.db.set(key, value)
        return

    def rm(self, key):
        self.db.clear(key)
        return

    def readfk(self, mkey):
        return self.db.get_range_startswith(mkey)


class DBController:

    def __init__(self, kv_store=None):
        if kv_store:
            self.kv_store = kv_store
        else:
            self.kv_store = KVStore()

    def get_global_settings(self):
        ret = GlobalSettings().read_from_db(self.kv_store)
        if ret:
            return ret[0]
        else:
            logger.error("Cluster is not initialized")
            exit(1)

    def set_global_settings(self, **kwargs):
        gs = GlobalSettings()
        gs.LB_PER_PAGE = kwargs['lb_per_page']
        gs.NS_LB_SIZE = kwargs['ns_lb_size']
        gs.NS_SIZE_IN_LBS = kwargs['ns_size_in_lbs']
        gs.MODEL_IDS = kwargs['model_ids']
        gs.NVME_PROGRAM_FAIL_COUNT = kwargs['nvme_program_fail_count']
        gs.NVME_ERASE_FAIL_COUNT = kwargs['nvme_erase_fail_count']
        gs.NVME_CRC_ERROR_COUNT = kwargs['nvme_crc_error_count']
        gs.DEVICE_OVERLOAD_STDEV_VALUE = kwargs['device_overload_stdev_value']
        gs.DEVICE_OVERLOAD_CAPACITY_THRESHOLD = kwargs['device_overload_capacity_threshold']
        gs.write_to_db(self.kv_store)
        return gs

    def clear_prefix(self, prefix):
        self.kv_store.db.clear_range_startswith(prefix)

    def get_cluster_map(self):
        cmap = ClusterMap().read_from_db(self.kv_store)
        return cmap[0] if cmap else None

    def get_storage_nodes(self):
        ret = StorageNode().read_from_db(self.kv_store)
        ret = sorted(ret, key=lambda x: x.create_dt)
        return ret

    def get_storage_nodes_by_cluster_id(self, cluster_id):
        ret = StorageNode().read_from_db(self.kv_store)
        nodes = []
        for n in ret:
            if n.cluster_id == cluster_id:
                nodes.append(n)
        return sorted(nodes, key=lambda x: x.create_dt)

    def get_storage_node_by_system_id(self, system_id):
        nodes = StorageNode().read_from_db(self.kv_store)
        for node in nodes:
            if node.system_uuid == system_id:
                return node
        return None

    def get_storage_node_by_id(self, id):
        ret = StorageNode().read_from_db(self.kv_store, id)
        if ret:
            return ret[0]

    # todo: change this function for multi cluster
    def get_caching_nodes(self):
        ret = CachingNode().read_from_db(self.kv_store)
        ret = sorted(ret, key=lambda x: x.create_dt)
        return ret

    def get_caching_node_by_id(self, id):
        ret = CachingNode().read_from_db(self.kv_store, id)
        if ret:
            return ret[0]

    def get_caching_node_by_system_id(self, system_id):
        nodes = CachingNode().read_from_db(self.kv_store)
        for node in nodes:
            if node.system_uuid == system_id:
                return node

    def get_caching_node_by_hostname(self, hostname):
        nodes = self.get_caching_nodes()
        for node in nodes:
            if node.hostname == hostname:
                return node

    def get_storage_node_by_hostname(self, hostname):
        nodes = self.get_storage_nodes()
        for node in nodes:
            if node.hostname == hostname:
                return node

    def get_storage_device_by_id(self, id):
        nodes = self.get_storage_nodes()
        for node in nodes:
            for dev in node.nvme_devices:
                if dev.get_id() == id:
                    return dev

    def get_storage_devices(self, id):
        return self.get_storage_device_by_id(id)

    def get_storage_by_jm_id(self, id):
        nodes = self.get_storage_nodes()
        for node in nodes:
            if node.jm_device.get_id() == id:
                return node


    # Compute node functions
    def get_compute_node_by_id(self, id):
        ret = ComputeNode().read_from_db(self.kv_store, id)
        if ret:
            return ret[0]

    def get_compute_nodes(self):
        ret = ComputeNode().read_from_db(self.kv_store)
        return ret

    def get_pools(self, cluster_id=None):
        pools = []
        if cluster_id:
            for pool in Pool().read_from_db(self.kv_store):
                if pool.cluster_id == cluster_id:
                    pools.append(pool)
        else:
            pools = Pool().read_from_db(self.kv_store)
        return pools

    def get_pool_by_id(self, id):
        ret = Pool().read_from_db(self.kv_store, id)
        if ret:
            return ret[0]

    def get_pool_by_name(self, name):
        pools = Pool().read_from_db(self.kv_store)
        for pool in pools:
            if pool.pool_name == name:
                return pool

    def get_lvols(self, cluster_id=None):
        lvols = []
        if cluster_id:
            for pool in self.get_pools(cluster_id):
                if pool.cluster_id == cluster_id:
                    for lv_id in pool.lvols:
                        lv = self.get_lvol_by_id(lv_id)
                        if lv:
                            lvols.append(lv)
        else:
            lvols = LVol().read_from_db(self.kv_store)
        return lvols

    def get_snapshots(self):
        ret = SnapShot().read_from_db(self.kv_store)
        return ret

    def get_snapshot_by_id(self, id):
        ret = SnapShot().read_from_db(self.kv_store, id)
        if ret:
            return ret[0]

    def get_lvol_by_id(self, id):
        ret = LVol().read_from_db(self.kv_store, id)
        if ret:
            return ret[0]

    def get_lvol_by_name(self, lvol_name):
        for lvol in self.get_lvols():
            if lvol.lvol_name == lvol_name:
                return lvol

    def get_mgmt_node_by_id(self, id):
        ret = MgmtNode().read_from_db(self.kv_store, id)
        if ret:
            return ret[0]

    def get_mgmt_nodes(self, cluster_id=None):
        return MgmtNode().read_from_db(self.kv_store)

    def get_mgmt_node_by_hostname(self, hostname):
        nodes = self.get_mgmt_nodes()
        for node in nodes:
            if node.hostname == hostname:
                return node

    def get_lvol_stats(self, lvol, limit=20):
        if isinstance(lvol, str):
            lvol = self.get_lvol_by_id(lvol)
        stats = LVolStatObject().read_from_db(self.kv_store, id="%s/%s" % (lvol.pool_uuid, lvol.uuid), limit=limit, reverse=True)
        return stats

    def get_cached_lvol_stats(self, lvol_id, limit=20):
        stats = CachedLVolStatObject().read_from_db(self.kv_store, id="%s/%s" % (lvol_id, lvol_id), limit=limit, reverse=True)
        return stats

    def get_pool_stats(self, pool, limit=20):
        stats = PoolStatObject().read_from_db(self.kv_store, id="%s/%s" % (pool.get_id(), pool.get_id()), limit=limit, reverse=True)
        return stats

    def get_cluster_stats(self, cluster, limit=20):
        return self.get_cluster_capacity(cluster, limit)

    def get_node_stats(self, node, limit=20):
        return self.get_node_capacity(node, limit)

    def get_device_stats(self, device, limit=20):
        return self.get_device_capacity(device, limit)

    def get_cluster_capacity(self, cl, limit=1):
        stats = ClusterStatObject().read_from_db(
            self.kv_store, id="%s/%s" % (cl.get_id(), cl.get_id()), limit=limit, reverse=True)
        return stats

    def get_node_capacity(self, node, limit=1):
        stats = NodeStatObject().read_from_db(
            self.kv_store, id="%s/%s" % (node.cluster_id, node.get_id()), limit=limit, reverse=True)
        return stats

    def get_device_capacity(self, device, limit=1):
        stats = DeviceStatObject().read_from_db(
            self.kv_store, id="%s/%s" % (device.cluster_id, device.get_id()), limit=limit, reverse=True)
        return stats

    def get_clusters(self):
        return Cluster().read_from_db(self.kv_store)

    def get_cluster_by_id(self, cluster_id):
        ret = Cluster().read_from_db(self.kv_store, id=cluster_id)
        if ret:
            return ret[0]

    def get_deployers(self):
        return Deployer().read_from_db(self.kv_store)

    def get_deployer_by_id(self, deployer_id):
        ret = Deployer().read_from_db(self.kv_store, id=deployer_id)
        if ret:
            return ret[0]

    def get_port_stats(self, node_id, port_id, limit=20):
        stats = PortStat().read_from_db(self.kv_store, id="%s/%s" % (node_id, port_id), limit=limit, reverse=True)
        return stats

    def get_events(self, event_id=""):
        return EventObj().read_from_db(self.kv_store, id=event_id)

    def get_job_tasks(self, cluster_id, reverse=True):
        return JobSchedule().read_from_db(self.kv_store, id=cluster_id, reverse=reverse)

    def get_task_by_id(self, task_id):
        for task in self.get_job_tasks(""):
            if task.uuid == task_id:
                return task

    def get_snapshots_by_node_id(self, node_id):
        ret = []
        snaps = SnapShot().read_from_db(self.kv_store)
        for snap in snaps:
            if snap.lvol.node_id == node_id:
                ret.append(snap)
        return ret

    def get_snode_size(self, node_id):
        snode = self.get_storage_node_by_id(node_id)
        total_node_capacity = 0
        for dev in snode.nvme_devices:
            total_node_capacity += dev.size
        return total_node_capacity

    def get_jm_device_by_id(self, jm_id):
        for node in self.get_storage_nodes():
            if node.jm_device and node.jm_device.get_id() == jm_id:
                return node.jm_device
        return None
