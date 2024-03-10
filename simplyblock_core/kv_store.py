# coding=utf-8
import logging

import fdb
import time

from simplyblock_core import constants
from simplyblock_core.models.caching_node import CachingNode
from simplyblock_core.models.cluster import ClusterMap

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.compute_node import ComputeNode
from simplyblock_core.models.port_stat import PortStat
from simplyblock_core.models.events import EventObj
from simplyblock_core.models.global_settings import GlobalSettings
from simplyblock_core.models.mgmt_node import MgmtNode
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.stats import DeviceStatObject, NodeStatObject, ClusterStatObject, LVolStatObject, \
    PoolStatObject
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

    def get_storage_node_by_id(self, id):
        ret = StorageNode().read_from_db(self.kv_store, id)
        if ret:
            return ret[0]

    def get_caching_nodes(self):
        ret = CachingNode().read_from_db(self.kv_store)
        ret = sorted(ret, key=lambda x: x.create_dt)
        return ret

    def get_caching_node_by_id(self, id):
        ret = CachingNode().read_from_db(self.kv_store, id)
        if ret:
            return ret[0]

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

    def get_storage_devices(self, id=""):
        # workaround because nvme devices are stored inside the node object itself.
        nodes = self.get_storage_nodes()
        devices = []
        device = None
        for node in nodes:
            if node.nvme_devices:
                devices.extend(node.nvme_devices)
                for dev in node.nvme_devices:
                    if dev.get_id() == id:
                        device = dev
        if id:
            return device
        return devices

    # Compute node functions
    def get_compute_node_by_id(self, id):
        ret = ComputeNode().read_from_db(self.kv_store, id)
        if ret:
            return ret[0]

    def get_compute_nodes(self):
        ret = ComputeNode().read_from_db(self.kv_store)
        return ret

    def get_pools(self):
        ret = Pool().read_from_db(self.kv_store)
        return ret

    def get_pool_by_id(self, id):
        ret = Pool().read_from_db(self.kv_store, id)
        if ret:
            return ret[0]

    def get_pool_by_name(self, name):
        pools = Pool().read_from_db(self.kv_store)
        for pool in pools:
            if pool.pool_name == name:
                return pool

    def get_lvols(self):
        ret = LVol().read_from_db(self.kv_store)
        return ret

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

    def get_port_stats(self, node_id, port_id, limit=20):
        stats = PortStat().read_from_db(self.kv_store, id="%s/%s" % (node_id, port_id), limit=limit, reverse=True)
        return stats

    def get_events(self, event_id=""):
        return EventObj().read_from_db(self.kv_store, id=event_id)
