# coding=utf-8
import json
import logging
import os.path
import time

import fdb
from typing import Any, List, Optional

from simplyblock_core import constants
from simplyblock_core.models.cluster import Cluster, ClusterAddNodeLock, PortReservation
from simplyblock_core.models.events import EventObj
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol, LVolReplication, LVolMini
from simplyblock_core.models.mgmt_node import MgmtNode
from simplyblock_core.models.nvme_device import NVMeDevice, JMDevice
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.port_stat import PortStat
from simplyblock_core.models.backup import Backup, BackupChainLock, BackupPolicy, BackupPolicyAttachment
from simplyblock_core.models.lvol_migration import LVolMigration
from simplyblock_core.models.qos import QOSClass
from simplyblock_core.models.snapshot import SnapShot, SnapShotMini
from simplyblock_core.models.stats import DeviceStatObject, NodeStatObject, ClusterStatObject, LVolStatObject, \
    PoolStatObject, CachedLVolStatObject
from simplyblock_core.models.storage_node import StorageNode, NodeLVolDelLock
from simplyblock_core.models.lvstore_lock import LVStoreMutationLock
from simplyblock_core.utils.helpers import single_or_none

logger = logging.getLogger(__name__)


class Singleton(type):
    _instances = {}  # type: ignore
    def __call__(cls, *args, **kwargs):
        if cls in cls._instances:
            return cls._instances[cls]
        else:
            ins = super(Singleton, cls).__call__(*args, **kwargs)
            if ins is not None and ins.kv_store is not None:
                cls._instances[cls] = ins
            return ins



class DBController(metaclass=Singleton):

    kv_store: Any = None

    def __init__(self):
        try:
            if not os.path.isfile(constants.KVD_DB_FILE_PATH):
                return
            fdb.api_version(constants.KVD_DB_VERSION)
            self.kv_store = fdb.open(constants.KVD_DB_FILE_PATH)  # type: ignore[func-returns-value]
            self.kv_store.options.set_transaction_timeout(constants.KVD_DB_TIMEOUT_MS)
        except Exception:
            logger.exception("FDB initialization failed")

    def get_storage_nodes(self) -> List[StorageNode]:
        ret = StorageNode().read_from_db(self.kv_store)
        ret = sorted(ret, key=lambda x: x.create_dt)
        return ret

    def get_storage_nodes_by_cluster_id(self, cluster_id: str) -> List[StorageNode]:
        ret = StorageNode().read_from_db(self.kv_store)
        nodes = []
        for n in ret:
            if n.cluster_id == cluster_id:
                nodes.append(n)
        return sorted(nodes, key=lambda x: x.create_dt)

    def get_storage_nodes_by_system_id(self, system_id: str) -> List[StorageNode]:
        return [
            node for node
            in StorageNode().read_from_db(self.kv_store)
            if node.system_uuid == system_id
        ]

    def get_storage_nodes_by_hostname(self, hostname: str) -> List[StorageNode]:
        return [
            node for node
            in self.get_storage_nodes()
            if node.hostname == hostname
        ]

    def get_storage_node_by_id(self, id: str) -> StorageNode:
        node = single_or_none(StorageNode().read_from_db(self.kv_store, id))
        if node is None:
            raise KeyError(f'StorageNode {id} not found')
        return node

    def get_storage_device_by_id(self, id: str) -> NVMeDevice:
        device = single_or_none(
            device
            for node in self.get_storage_nodes()
            for device in node.nvme_devices
            if device.get_id() == id
        )
        if device is None:
            raise KeyError(f'Device {id} not found')
        return device


    def get_pools(self, cluster_id: Optional[str] = None) -> List[Pool]:
        pools = []
        if cluster_id:
            for pool in Pool().read_from_db(self.kv_store):
                if pool.cluster_id == cluster_id:
                    pools.append(pool)
        else:
            pools = Pool().read_from_db(self.kv_store)
        return pools

    def get_pool_by_id(self, id: str) -> Pool:
        pool = single_or_none(Pool().read_from_db(self.kv_store, id))
        if pool is None:
            raise KeyError(f'Pool {id} not found')
        return pool

    def get_pool_by_name(self, name: str) -> Pool:
        pool = single_or_none(p for p in Pool().read_from_db(self.kv_store) if p.pool_name == name)
        if pool is None:
            raise KeyError(f'Pool {name} not found')
        return pool

    def get_lvols(self, cluster_id: Optional[str] = None) -> List[LVol]:
        lvols = self.get_all_lvols()
        lvols = [lvol for lvol in lvols if lvol.status != LVol.STATUS_DELETED]
        if not cluster_id:
            return lvols

        node_ids=[]
        cluster_lvols = []
        for node in self.get_storage_nodes_by_cluster_id(cluster_id):
            node_ids.append(node.get_id())

        for lvol in lvols:
            if lvol.node_id in node_ids:
                cluster_lvols.append(lvol)

        return cluster_lvols

    def get_all_lvols(self) -> List[LVol]:
        start_time = time.time()
        lvols = LVol().read_from_db(self.kv_store)
        ret = sorted(lvols, key=lambda x: x.create_dt)
        end_time = time.time()
        logger.debug(f"time taken to read all LVols: {round(end_time - start_time, 2)}s")
        return ret

    def get_lvols_by_node_id(self, node_id: str) -> List[LVol]:
        lvols = []
        for lvol in self.get_lvols():
            if lvol.node_id == node_id:
                lvols.append(lvol)
        return sorted(lvols, key=lambda x: x.create_dt)

    def get_lvols_by_pool_id(self, pool_id: str) -> List[LVol]:
        lvols = []
        for lvol in self.get_lvols():
            if lvol.pool_uuid == pool_id:
                lvols.append(lvol)
        return sorted(lvols, key=lambda x: x.create_dt)

    def get_hostnames_by_pool_id(self, pool_id: str) -> List[str]:
        lvols = self.get_lvols_by_pool_id(pool_id)
        hostnames = []
        for lv in lvols:
            if (lv.hostname not in hostnames):
                hostnames.append(lv.hostname)
        return hostnames

    def get_snapshots(self, cluster_id: Optional[str] = None) -> List[SnapShot]:
        start_time = time.time()
        snaps = SnapShot().read_from_db(self.kv_store)
        if cluster_id:
            snaps = [n for n in snaps if n.cluster_id == cluster_id]
        ret = sorted(snaps, key=lambda x: x.created_at)
        end_time = time.time()
        logger.debug(f"time taken to read all SnapShots: {round(end_time - start_time, 2)}s")
        return ret

    def get_mini_lvols(self) -> List[LVolMini]:
        start_time = time.time()
        lvols = LVolMini().read_from_db(self.kv_store)
        ret = sorted(lvols, key=lambda x: x.create_dt)
        end_time = time.time()
        logger.debug(f"time taken to read all mini lvols: {round(end_time - start_time, 2)}s")
        return ret

    def get_mini_snapshots(self) -> List[SnapShotMini]:
        start_time = time.time()
        snaps = SnapShotMini().read_from_db(self.kv_store)
        ret = sorted(snaps, key=lambda x: x.created_at)
        end_time = time.time()
        logger.debug(f"time taken to read all mini snapshots: {round(end_time - start_time, 2)}s")
        return ret

    def get_snapshot_by_id(self, id: str) -> SnapShot:
        snap = single_or_none(SnapShot().read_from_db(self.kv_store, id))
        if snap is None:
            raise KeyError(f'Snapshot {id} not found')
        return snap

    def get_lvol_by_id(self, id: str) -> LVol:
        lvol = single_or_none(LVol().read_from_db(self.kv_store, id=id))
        if lvol is None:
            raise KeyError(f'LVol {id} not found')
        return lvol

    def get_lvol_replication_objects(self) -> List[LVolReplication]:
        ret = LVolReplication().read_from_db(self.kv_store)
        return sorted(ret, key=lambda x: x.create_dt)

    def get_lvol_replication_by_id(self, uuid) -> LVolReplication:
        ret = LVolReplication().read_from_db(self.kv_store, uuid)
        if not ret:
            raise KeyError(f'LVolReplication {uuid} not found')
        return ret[0]

    def get_lvol_by_name(self, lvol_name: str) -> LVol:
        lvol = single_or_none(lvol for lvol in self.get_lvols() if lvol.lvol_name == lvol_name)
        if lvol is None:
            raise KeyError(f'LVol {lvol_name} not found')
        return lvol

    def get_mgmt_node_by_id(self, id: str) -> MgmtNode:
        node = single_or_none(MgmtNode().read_from_db(self.kv_store, id))
        if node is None:
            raise KeyError(f'ManagementNode {id} not found')
        return node

    def get_mgmt_nodes(self, cluster_id: Optional[str] = None) -> List[MgmtNode]:
        nodes = MgmtNode().read_from_db(self.kv_store)
        if cluster_id:
            nodes = [n for n in nodes if n.cluster_id == cluster_id]
        return sorted(nodes, key=lambda x: x.create_dt)

    def get_mgmt_node_by_hostname(self, hostname: str) -> MgmtNode:
        node = single_or_none(node for node in self.get_mgmt_nodes() if node.hostname == hostname)
        if node is None:
            raise KeyError(f'No management node found for hostname {hostname}')
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

    def get_cluster_by_id(self, cluster_id: str) -> Cluster:
        cluster = single_or_none(Cluster().read_from_db(self.kv_store, id=cluster_id))
        if cluster is None:
            raise KeyError(f'Cluster {cluster_id} not found')
        return cluster

    def get_port_stats(self, node_id: str, port_id: str, limit: int = 20) -> List[PortStat]:
        stats = PortStat().read_from_db(self.kv_store, id="%s/%s" % (node_id, port_id), limit=limit, reverse=True)
        return stats

    def get_events(self, event_id: str = " ", limit: int = 0, reverse: bool = False) -> List[EventObj]:
        return EventObj().read_from_db(self.kv_store, id=event_id, limit=limit, reverse=reverse)

    def get_job_tasks(self, cluster_id: str, reverse: bool = True, limit: int = 0) -> List[JobSchedule]:
        ret = JobSchedule().read_from_db(self.kv_store, id=cluster_id, reverse=reverse, limit=limit)
        return sorted(ret, key=lambda x: x.date)


    def get_active_migration_tasks(self, cluster_id: str) -> List[JobSchedule]:
        """Return all non-done FN_LVOL_MIG tasks for the given cluster (single FDB scan)."""
        return [
            t for t in self.get_job_tasks(cluster_id, reverse=False)
            if t.function_name == JobSchedule.FN_LVOL_MIG
            and t.status != JobSchedule.STATUS_DONE
        ]

    def get_task_by_id(self, task_id: str) -> JobSchedule:
        task = single_or_none(t for t in self.get_job_tasks(" ") if t.uuid == task_id)
        if task is None:
            raise KeyError(f'Task {task_id} not found')
        return task

    def get_snapshots_by_node_id(self, node_id: str) -> List[SnapShot]:
        ret = []
        snaps = self.get_snapshots()
        for snap in snaps:
            if snap.lvol.node_id == node_id:
                ret.append(snap)
        return sorted(ret, key=lambda x: x.create_dt)

    def get_snapshots_by_pool_id(self, pool_id: str) -> List[SnapShot]:
        ret = []
        snaps = self.get_snapshots()
        for snap in snaps:
            if snap.pool_uuid == pool_id:
                ret.append(snap)
        return sorted(ret, key=lambda x: x.create_dt)

    def get_snapshots_by_lvol_id(self, lvol_id: str) -> List[SnapShot]:
        return [s for s in self.get_snapshots() if s.lvol and s.lvol.get_id() == lvol_id]

    def get_snode_size(self, node_id: str) -> int:
        snode = self.get_storage_node_by_id(node_id)
        return sum(dev.size for dev in snode.nvme_devices)

    def get_jm_device_by_id(self, jm_id: str) -> JMDevice:
        device = single_or_none(
            node.jm_device
            for node in self.get_storage_nodes()
            if node.jm_device and node.jm_device.get_id() == jm_id
        )
        if device is None:
            raise KeyError(f'JMDevice {jm_id} not found')
        return device

    def get_primary_storage_nodes_by_cluster_id(self, cluster_id: str) -> List[StorageNode]:
        ret = StorageNode().read_from_db(self.kv_store)
        nodes = []
        for n in ret:
            if n.cluster_id == cluster_id and not n.is_secondary_node:  # pass
                nodes.append(n)
        return sorted(nodes, key=lambda x: x.create_dt)

    def get_primary_storage_nodes_by_secondary_node_id(self, node_id: str) -> List[StorageNode]:
        ret = StorageNode().read_from_db(self.kv_store)
        nodes = []
        for node in ret:
            if (node.secondary_node_id == node_id or node.tertiary_node_id == node_id) and node.lvstore:
                nodes.append(node)
        return sorted(nodes, key=lambda x: x.create_dt)

    def get_qos(self, cluster_id: Optional[str] = None) -> List[QOSClass]:
        classes = []
        if cluster_id:
            for qos in QOSClass().read_from_db(self.kv_store):
                if qos.cluster_id == cluster_id:
                    classes.append(qos)
        else:
            classes = QOSClass().read_from_db(self.kv_store)
        return sorted(classes, key=lambda x: x.class_id)

    def get_migrations(self, cluster_id: Optional[str] = None) -> List[LVolMigration]:
        """Return all LVolMigration records, optionally filtered by cluster."""
        prefix = cluster_id if cluster_id else " "
        return LVolMigration().read_from_db(self.kv_store, id=prefix)

    def get_migration_by_id(self, migration_id: str) -> LVolMigration:
        migration = single_or_none(m for m in self.get_migrations() if m.uuid == migration_id)
        if migration is None:
            raise KeyError(f'LVolMigration {migration_id} not found')
        return migration

    def get_migration_by_lvol_id(self, lvol_id: str) -> Optional[LVolMigration]:
        return single_or_none(
            m for m in self.get_migrations() if m.lvol_id == lvol_id and m.is_active()
        )

    def get_lvol_del_lock(self, node_id: str) -> Optional[NodeLVolDelLock]:
        return single_or_none(NodeLVolDelLock().read_from_db(self.kv_store, id=node_id))

    def get_backup_chain_lock(self, snapshot_id: str) -> Optional[BackupChainLock]:
        return single_or_none(BackupChainLock().read_from_db(self.kv_store, id=snapshot_id))

    def _acquire_backup_chain_locks_tx(self, tr, snapshot_ids, requested_snapshot_id, lvol_id):
        import time

        existing_lock = None
        keys = []
        for snapshot_id in snapshot_ids:
            lock = BackupChainLock()
            lock.uuid = snapshot_id
            lock.snapshot_id = snapshot_id
            key = lock.get_db_id().encode()
            keys.append((key, lock))
            raw = tr.get(key).wait()
            if raw.present():
                existing_lock = BackupChainLock().from_dict(json.loads(raw))
                break

        if existing_lock is not None:
            return False, existing_lock

        # The lock only protects the enqueue window; wall-clock time is sufficient here.
        now = int(time.time())

        for key, lock in keys:
            lock.requested_snapshot_id = requested_snapshot_id
            lock.lvol_id = lvol_id
            lock.created_at = now
            tr[key] = json.dumps(lock.to_dict(unwrap_secrets=True)).encode()

        return True, None

    def acquire_backup_chain_locks(self, snapshot_ids, requested_snapshot_id, lvol_id):
        if not self.kv_store or not snapshot_ids:
            return True, None
        ordered_snapshot_ids = sorted(set(snapshot_ids))
        transactional = fdb.transactional(DBController._acquire_backup_chain_locks_tx)
        return transactional(self, self.kv_store, ordered_snapshot_ids, requested_snapshot_id, lvol_id)

    def _release_backup_chain_locks_tx(self, tr, snapshot_ids):
        for snapshot_id in snapshot_ids:
            lock = BackupChainLock()
            lock.uuid = snapshot_id
            lock.snapshot_id = snapshot_id
            del tr[lock.get_db_id().encode()]

    def release_backup_chain_locks(self, snapshot_ids):
        if not self.kv_store or not snapshot_ids:
            return
        ordered_snapshot_ids = sorted(set(snapshot_ids))
        transactional = fdb.transactional(DBController._release_backup_chain_locks_tx)
        transactional(self, self.kv_store, ordered_snapshot_ids)

    # ---- Cluster node-add mesh lock (Single FDB Transaction) ----

    def get_cluster_add_lock(self, cluster_id: str) -> Optional[ClusterAddNodeLock]:
        return single_or_none(ClusterAddNodeLock().read_from_db(self.kv_store, id=cluster_id))

    def _try_acquire_cluster_add_lock_tx(self, tr, cluster_id, owner, now):
        lock = ClusterAddNodeLock()
        lock.cluster_id = cluster_id
        key = lock.get_db_id().encode()
        raw = tr.get(key).wait()
        if raw.present():
            existing = ClusterAddNodeLock().from_dict(json.loads(raw))
            fresh = (now - existing.heartbeat_at) <= constants.CLUSTER_ADD_LOCK_TTL_SEC
            if existing.owner and existing.owner != owner and fresh:
                return False, existing.owner
            # Stale (holder presumed dead) or already ours: (re)take it.
            lock.acquired_at = existing.acquired_at if existing.owner == owner else now
        else:
            lock.acquired_at = now
        lock.owner = owner
        lock.heartbeat_at = now
        tr[key] = json.dumps(lock.to_dict()).encode()
        return True, None

    def acquire_cluster_add_lock(self, cluster_id, owner):
        """Atomically acquire the per-cluster node-add mesh lock.

        Returns (True, None) if ``owner`` now holds the lock (newly acquired,
        reclaimed from a dead holder whose heartbeat went stale, or already
        held by this owner), or (False, current_owner) if a live holder owns it.
        """
        if not self.kv_store:
            return False, "No DB connection"
        now = int(time.time())
        transactional = fdb.transactional(DBController._try_acquire_cluster_add_lock_tx)
        return transactional(self, self.kv_store, cluster_id, owner, now)

    def _refresh_cluster_add_lock_tx(self, tr, cluster_id, owner, now):
        lock = ClusterAddNodeLock()
        lock.cluster_id = cluster_id
        key = lock.get_db_id().encode()
        raw = tr.get(key).wait()
        if not raw.present():
            return False
        existing = ClusterAddNodeLock().from_dict(json.loads(raw))
        if existing.owner != owner:
            return False  # lost the lock (reclaimed by someone else)
        existing.heartbeat_at = now
        tr[key] = json.dumps(existing.to_dict()).encode()
        return True

    def refresh_cluster_add_lock(self, cluster_id, owner):
        """Heartbeat the lock so a long mesh section isn't reclaimed. Returns
        True if still held by ``owner``, False if it was lost/reclaimed."""
        if not self.kv_store:
            return False
        now = int(time.time())
        transactional = fdb.transactional(DBController._refresh_cluster_add_lock_tx)
        return transactional(self, self.kv_store, cluster_id, owner, now)

    def _release_cluster_add_lock_tx(self, tr, cluster_id, owner):
        lock = ClusterAddNodeLock()
        lock.cluster_id = cluster_id
        key = lock.get_db_id().encode()
        raw = tr.get(key).wait()
        if not raw.present():
            return
        existing = ClusterAddNodeLock().from_dict(json.loads(raw))
        if existing.owner == owner:
            del tr[key]

    def release_cluster_add_lock(self, cluster_id, owner):
        """Release the lock only if still owned by ``owner`` (owner-scoped, so a
        late release never deletes a lock another host has since reclaimed)."""
        if not self.kv_store:
            return
        transactional = fdb.transactional(DBController._release_cluster_add_lock_tx)
        transactional(self, self.kv_store, cluster_id, owner)

    # ---- Per-lvstore snapshot-mutation lock (Single FDB Transaction) ----

    def _try_acquire_lvstore_lock_tx(self, tr, cluster_id, lvs_name, owner, now, ttl):
        lock = LVStoreMutationLock()
        lock.cluster_id = cluster_id
        lock.lvs_name = lvs_name
        key = lock.get_db_id().encode()
        raw = tr.get(key).wait()
        if raw.present():
            existing = LVStoreMutationLock().from_dict(json.loads(raw))
            fresh = (now - existing.heartbeat_at) <= ttl
            if existing.owner and existing.owner != owner and fresh:
                return False, existing.owner
            # Stale (holder presumed dead) or already ours: (re)take it.
            lock.acquired_at = existing.acquired_at if existing.owner == owner else now
        else:
            lock.acquired_at = now
        lock.owner = owner
        lock.heartbeat_at = now
        tr[key] = json.dumps(lock.to_dict()).encode()
        return True, None

    def acquire_lvstore_lock(self, cluster_id, lvs_name, owner):
        """Atomically acquire the per-lvstore snapshot-mutation lock.

        Returns (True, None) if ``owner`` now holds the lock (newly acquired,
        reclaimed from a dead holder whose heartbeat went stale, or already held
        by this owner), or (False, current_owner) if a live holder owns it."""
        if not self.kv_store:
            return False, "No DB connection"
        now = int(time.time())
        ttl = constants.LVSTORE_MUTATION_LOCK_TTL_SEC
        transactional = fdb.transactional(DBController._try_acquire_lvstore_lock_tx)
        return transactional(self, self.kv_store, cluster_id, lvs_name, owner, now, ttl)

    def _refresh_lvstore_lock_tx(self, tr, cluster_id, lvs_name, owner, now):
        lock = LVStoreMutationLock()
        lock.cluster_id = cluster_id
        lock.lvs_name = lvs_name
        key = lock.get_db_id().encode()
        raw = tr.get(key).wait()
        if not raw.present():
            return False
        existing = LVStoreMutationLock().from_dict(json.loads(raw))
        if existing.owner != owner:
            return False  # lost the lock (reclaimed by someone else)
        existing.heartbeat_at = now
        tr[key] = json.dumps(existing.to_dict()).encode()
        return True

    def refresh_lvstore_lock(self, cluster_id, lvs_name, owner):
        """Heartbeat the lock so a slow create→register section isn't reclaimed.
        Returns True if still held by ``owner``, False if it was lost."""
        if not self.kv_store:
            return False
        now = int(time.time())
        transactional = fdb.transactional(DBController._refresh_lvstore_lock_tx)
        return transactional(self, self.kv_store, cluster_id, lvs_name, owner, now)

    def _release_lvstore_lock_tx(self, tr, cluster_id, lvs_name, owner):
        lock = LVStoreMutationLock()
        lock.cluster_id = cluster_id
        lock.lvs_name = lvs_name
        key = lock.get_db_id().encode()
        raw = tr.get(key).wait()
        if not raw.present():
            return
        existing = LVStoreMutationLock().from_dict(json.loads(raw))
        if existing.owner == owner:
            del tr[key]

    def release_lvstore_lock(self, cluster_id, lvs_name, owner):
        """Release the lock only if still owned by ``owner`` (owner-scoped, so a
        late release never deletes a lock another holder has since reclaimed)."""
        if not self.kv_store:
            return
        transactional = fdb.transactional(DBController._release_lvstore_lock_tx)
        transactional(self, self.kv_store, cluster_id, lvs_name, owner)

    def watch_lvstore_lock(self, cluster_id, lvs_name):
        """Return an FDB watch future that fires when the lock key changes
        (release, reclaim, heartbeat), or None when no DB connection exists.

        Lets lock waiters block on the actual release instead of sleeping a
        fixed poll interval: the future's ``is_ready()`` is a local check (no
        FDB round-trip), so waiters can spin on it cheaply and re-attempt the
        acquire the moment the holder releases."""
        if not self.kv_store:
            return None
        lock = LVStoreMutationLock()
        lock.cluster_id = cluster_id
        lock.lvs_name = lvs_name
        key = lock.get_db_id().encode()
        tr = self.kv_store.create_transaction()
        watch = tr.watch(key)
        tr.commit().wait()
        return watch

    # ---- Node-add port reservation (Single FDB Transaction) ----

    def _reserve_next_nvmf_port_tx(self, tr, cluster_id, base_port, node_used, owner, now):
        used = set(node_used)
        # Read this cluster's reservations transactionally so concurrent
        # reservers conflict-retry instead of picking the same port. Drop
        # stale ones (abandoned by a crashed add) in the same transaction.
        for res in PortReservation().read_from_db(tr, id=cluster_id):
            if res.cluster_id != cluster_id:
                continue
            if (now - res.created_at) > constants.PORT_RESERVATION_TTL_SEC:
                del tr[res.get_db_id().encode()]
                continue
            used.add(res.port)
        port = base_port
        while port in used:
            port += 1
        res = PortReservation()
        res.cluster_id = cluster_id
        res.port = port
        res.owner = owner
        res.created_at = now
        tr[res.get_db_id().encode()] = json.dumps(res.to_dict()).encode()
        return port

    def reserve_cluster_nvmf_port(self, cluster_id, owner):
        """Atomically allocate and reserve the next free NVMe-oF port for a
        node being added. The reservation makes the chosen port visible to
        concurrent allocators until the node record itself is persisted (after
        which the node's own port fields keep it reserved and the reservation
        ages out by TTL)."""
        from simplyblock_core import utils
        base_port = utils.get_nvmf_base_port(cluster_id)
        node_used = utils.get_node_nvmf_ports(cluster_id)
        now = int(time.time())
        transactional = fdb.transactional(DBController._reserve_next_nvmf_port_tx)
        return transactional(self, self.kv_store, cluster_id, base_port, node_used, owner, now)

    # ---- Generic atomic read-modify-write (Single FDB Transaction) ----

    def _atomic_update_tx(self, tr, key, model_cls, mutate_fn):
        raw = tr.get(key).wait()
        if not raw.present():
            return None
        obj = model_cls().from_dict(json.loads(raw))
        if mutate_fn(obj) is False:
            return obj
        tr[key] = json.dumps(obj.to_dict(unwrap_secrets=True)).encode()
        return obj

    def atomic_update(self, obj, mutate_fn):
        """Transactional read-modify-write of a single object.

        Re-reads ``obj`` fresh from FDB inside a transaction, applies
        ``mutate_fn(fresh)`` (which must mutate the object in place), and writes
        it back atomically. FoundationDB's serializable isolation plus the
        automatic conflict-retry of ``fdb.transactional`` make this a true
        compare-and-set: if another writer commits between this read and the
        write, the whole transaction (including ``mutate_fn``) is replayed on
        the new value. This avoids the lost-update that plain
        ``read(); obj.x = y; obj.write_to_db()`` suffers — the latter writes the
        entire serialized object and silently clobbers concurrent updates to
        other fields.

        IMPORTANT: ``mutate_fn`` may be invoked more than once (on conflict
        retry), so it MUST be free of side effects other than mutating the
        object passed to it — no RPCs, no DB writes, no event emission. Do that
        work after this returns. Return ``False`` from ``mutate_fn`` to abort
        the write (e.g. when a guard condition no longer holds on the fresh
        object).

        Returns the fresh, post-mutation object, or ``None`` if the object no
        longer exists in the DB.
        """
        if not self.kv_store:
            return None
        key = obj.get_db_id().encode()
        transactional = fdb.transactional(DBController._atomic_update_tx)
        return transactional(self, self.kv_store, key, type(obj), mutate_fn)

    # ---- vuid allocation (monotonic sequence) ----
    #
    # vuids (lvol / snapshot / clone bdev-name numbers, distrib vuid, JM jm_vuid)
    # share one numeric space — SPDK rejects a create whose bdev-name number
    # already exists. The old allocator picked a random number in a bounded range
    # and rejected collisions by scanning EVERY lvol + snapshot + node lvstore
    # stack on each create — O(N) per create, i.e. O(N^2) for a mass create
    # (incident mass_create_delete_docker-20260629: lvol create degraded 12x as
    # the count grew; snapshot create likewise). A single monotonic counter
    # removes the scan: every allocation is strictly larger than all earlier
    # ones, so a name collision is impossible by construction. vuids are plain
    # ints (formatted at most as 64-bit :016X), so the unbounded growth is fine.
    _VUID_SEQ_KEY = b"sequence/vuid"

    def _incr_vuid_tx(self, tr):
        raw = tr.get(DBController._VUID_SEQ_KEY).wait()
        if not raw.present():
            return None
        nxt = int(json.loads(raw)) + 1
        tr[DBController._VUID_SEQ_KEY] = json.dumps(nxt).encode()
        return nxt

    def _seed_vuid_tx(self, tr, seed):
        # Only-if-absent CAS: the first allocator (across all API workers / mgmt
        # nodes) seeds the counter; concurrent racers see it present and skip.
        raw = tr.get(DBController._VUID_SEQ_KEY).wait()
        if raw.present():
            return
        tr[DBController._VUID_SEQ_KEY] = json.dumps(int(seed)).encode()

    def _max_existing_vuid(self) -> int:
        """Highest vuid currently in use across every source the old allocator
        deduped against. Read once to seed the counter on an upgraded cluster so
        the sequence never reuses a pre-existing vuid; never read again."""
        mx = 0
        for lv in self.get_mini_lvols():
            mx = max(mx, lv.vuid or 0)
        for sn in self.get_mini_snapshots():
            mx = max(mx, sn.vuid or 0)
        for node in self.get_storage_nodes():
            for bdev in (node.lvstore_stack or []):
                if bdev.get("type") == "bdev_distr":
                    mx = max(mx, (bdev.get("params", {}) or {}).get("vuid", 0) or 0)
                elif bdev.get("type") == "bdev_raid" and "jm_vuid" in bdev:
                    mx = max(mx, bdev.get("jm_vuid", 0) or 0)
        return mx

    def next_vuid(self) -> int:
        """Allocate the next globally-unique vuid (monotonic, O(1))."""
        val = fdb.transactional(DBController._incr_vuid_tx)(self, self.kv_store)
        if val is not None:
            return val
        # Counter absent (first allocation ever / freshly upgraded cluster):
        # seed it above any pre-existing vuid (one-time scan, outside the txn),
        # then increment. The seed CAS is idempotent under concurrency.
        seed = self._max_existing_vuid()
        fdb.transactional(DBController._seed_vuid_tx)(self, self.kv_store, seed)
        return fdb.transactional(DBController._incr_vuid_tx)(self, self.kv_store)

    # ---- snapshot indexes (replace per-create cluster-wide scans) ----
    #
    # Snapshot create used to read EVERY snapshot in the cluster on each request
    # (name-uniqueness scan + chain-linking scan) — O(N) per create, O(N^2) for a
    # mass run (incident mass_create_delete_docker-20260629, Phase 3). Two indexes
    # remove that:
    #   name_index/snapshot/<cluster_id>/<name>   -> snap uuid   (uniqueness, O(1))
    #   lvol_snaps/<lvol_uuid>/<created_at>/<vuid> -> snap uuid   (this lvol's
    #     snapshots in creation order; the tail = chain predecessor, one read)
    # The name index is self-healing: a hit is verified against the real record,
    # so a stale entry (missed clear) is treated as free, never a false reject.
    @staticmethod
    def _snap_name_idx_key(cluster_id, name) -> bytes:
        return ("name_index/snapshot/%s/%s" % (cluster_id, name)).encode()

    @staticmethod
    def _snap_lvol_idx_prefix(lvol_uuid) -> str:
        return "lvol_snaps/%s/" % lvol_uuid

    @staticmethod
    def _snap_lvol_idx_key(lvol_uuid, created_at, vuid, snap_uuid) -> bytes:
        return ("%s%020d/%020d/%s" % (DBController._snap_lvol_idx_prefix(lvol_uuid),
                                      int(created_at or 0), int(vuid or 0), snap_uuid)).encode()

    def snap_name_taken(self, cluster_id, name) -> bool:
        raw = self.kv_store.get(self._snap_name_idx_key(cluster_id, name))
        if raw is None:
            return False
        try:
            snap = self.get_snapshot_by_id(raw.decode())
        except KeyError:
            self.kv_store.clear(self._snap_name_idx_key(cluster_id, name))  # stale
            return False
        if snap.snap_name == name and snap.cluster_id == cluster_id:
            return True
        self.kv_store.clear(self._snap_name_idx_key(cluster_id, name))  # stale
        return False

    def index_snapshot(self, snap) -> None:
        self.kv_store[self._snap_name_idx_key(snap.cluster_id, snap.snap_name)] = snap.uuid.encode()
        self.kv_store[self._snap_lvol_idx_key(
            snap.lvol.get_id(), snap.created_at, snap.vuid, snap.uuid)] = snap.uuid.encode()

    def unindex_snapshot(self, snap) -> None:
        self.kv_store.clear(self._snap_name_idx_key(snap.cluster_id, snap.snap_name))
        self.kv_store.clear(self._snap_lvol_idx_key(
            snap.lvol.get_id(), snap.created_at, snap.vuid, snap.uuid))

    def get_lvol_latest_snapshot(self, lvol_uuid, exclude_uuid=None):
        """Newest snapshot of an lvol (chain tail) via a single reverse range
        read of the by-lvol index — replaces the cluster-wide chain-linking scan.
        Returns the SnapShot or None. On a fresh cluster the index is complete
        from the first snapshot; pre-index snapshots on an upgraded cluster are
        simply not found (a one-time cosmetic chain gap, not corruption)."""
        prefix = self._snap_lvol_idx_prefix(lvol_uuid).encode()
        for _k, v in self.kv_store.get_range_startswith(prefix, limit=2, reverse=True):
            snap_uuid = v.decode()
            if exclude_uuid and snap_uuid == exclude_uuid:
                continue
            try:
                return self.get_snapshot_by_id(snap_uuid)
            except KeyError:
                continue
        return None

    # ---- lvol name index (per pool) ----
    #
    # lvol name uniqueness used to scan every lvol in the DB (get_mini_lvols) on
    # each create. Index it per pool for an O(1) point read. Maintained at the
    # LVol model layer (write_to_db / remove) so every create/delete path keeps
    # it current without per-call-site wiring. Self-healing: a hit is verified
    # against the real record, so a stale entry is treated as free.
    @staticmethod
    def _lvol_name_idx_key(pool_uuid, name) -> bytes:
        return ("name_index/lvol/%s/%s" % (pool_uuid, name)).encode()

    def lvol_name_lookup(self, pool_uuid, name):
        """Return the LVol with this name in this pool, or None (verify-on-hit)."""
        key = self._lvol_name_idx_key(pool_uuid, name)
        raw = self.kv_store.get(key)
        if raw is None:
            return None
        try:
            lv = self.get_lvol_by_id(raw.decode())
        except KeyError:
            self.kv_store.clear(key)  # stale
            return None
        if lv.lvol_name == name and lv.pool_uuid == pool_uuid:
            return lv
        self.kv_store.clear(key)  # stale
        return None

    def lvol_name_taken(self, pool_uuid, name) -> bool:
        return self.lvol_name_lookup(pool_uuid, name) is not None

    def index_lvol_name(self, lvol) -> None:
        if self.kv_store is not None and lvol.pool_uuid and lvol.lvol_name:
            self.kv_store[self._lvol_name_idx_key(lvol.pool_uuid, lvol.lvol_name)] = lvol.get_id().encode()

    def unindex_lvol_name(self, lvol) -> None:
        if self.kv_store is not None and lvol.pool_uuid and lvol.lvol_name:
            self.kv_store.clear(self._lvol_name_idx_key(lvol.pool_uuid, lvol.lvol_name))

    # ---- Pre-Restart Guard (Single FDB Transaction) ----

    def _try_set_node_restarting_tx(self, tr, cluster_id, node_id):
        """Pre-restart check as a single FDB transaction.

        Opens transaction, queries status of all nodes in the cluster.
        If any node is in restart or shutdown, returns False.
        Otherwise sets this node to in_restart and commits.

        Returns (True, None) on success, or (False, reason) if blocked.
        """
        all_nodes = StorageNode().read_from_db(tr)
        for n in all_nodes:
            if n.cluster_id != cluster_id:
                continue
            if n.get_id() == node_id:
                continue
            if n.status in [StorageNode.STATUS_RESTARTING, StorageNode.STATUS_IN_SHUTDOWN]:
                return False, f"Node {n.get_id()} is {n.status}"

        # Set this node to in_restart atomically within the same transaction
        target = None
        for n in all_nodes:
            if n.get_id() == node_id:
                target = n
                break
        if target:
            target.status = StorageNode.STATUS_RESTARTING
            prefix = target.get_db_id()
            data = json.dumps(target.get_clean_dict(unwrap_secrets=True))
            tr[prefix.encode()] = data.encode()

        return True, None

    def try_set_node_restarting(self, cluster_id, node_id):
        """Pre-restart check: single FDB transaction.

        Opens FDB transaction, queries status of all nodes.
        If any node is in restart or shutdown, returns False.
        Sets node to in_restart and commits transaction.

        On successful acquisition the status-change event and peer
        notification are emitted AFTER the commit. The FDB tx itself
        writes directly via ``tr[...] = ...`` and so bypasses
        ``set_node_status``; without this post-commit emission every
        offline→in_restart transition via the guard would be invisible
        in the cluster event log and to peers, leaving DeviceMonitor
        and HealthCheck to observe the new state with no event trail.

        Returns (True, None) on success, or (False, reason) if blocked.
        """
        if not self.kv_store:
            return False, "No DB connection"

        # Snapshot old status before the tx so we can emit an accurate
        # change event after it commits. Best-effort: if the read fails,
        # we still emit with ``old_status="unknown"`` rather than skip
        # the event.
        old_status = None
        try:
            pre = self.get_storage_node_by_id(node_id)
            if pre is not None:
                old_status = pre.status
        except Exception:
            pass

        transactional = fdb.transactional(DBController._try_set_node_restarting_tx)
        acquired, reason = transactional(self, self.kv_store, cluster_id, node_id)

        if acquired:
            # Emit the status-change event and peer notification AFTER commit.
            # These side-effects must live outside the FDB transaction because
            # they don't compose with FDB retry semantics (a retried tx would
            # re-emit). Delayed imports avoid any dependency cycle between
            # db_controller and the controllers package.
            try:
                from simplyblock_core.controllers import storage_events
                from simplyblock_core import distr_controller
                snode = self.get_storage_node_by_id(node_id)
                if snode is not None and old_status != snode.status:
                    storage_events.snode_status_change(
                        snode, snode.status, old_status or "unknown",
                        caused_by="restart_guard",
                    )
                    distr_controller.send_node_status_event(snode, snode.status)
            except Exception as e:
                logger.warning(
                    "try_set_node_restarting committed but event emission "
                    "failed for %s: %s", node_id, e,
                )
        return acquired, reason

    # ---- S3 Backup ----

    def get_backups(self, cluster_id: Optional[str] = None) -> List[Backup]:
        prefix = cluster_id if cluster_id else " "
        return Backup().read_from_db(self.kv_store, id=prefix)

    def get_backup_by_id(self, backup_id: str) -> Backup:
        backup = single_or_none(b for b in self.get_backups() if b.uuid == backup_id)
        if backup is None:
            raise KeyError(f'Backup {backup_id} not found')
        return backup

    def get_backups_by_lvol_id(self, lvol_id: str) -> List[Backup]:
        return [b for b in self.get_backups() if b.lvol_id == lvol_id]

    def get_backups_by_snapshot_id(self, snapshot_id: str) -> List[Backup]:
        return [b for b in self.get_backups() if b.snapshot_id == snapshot_id]

    def get_backup_chain(self, backup_id: str) -> List[Backup]:
        """Return the full backup chain ending at backup_id, oldest first."""
        chain = []
        current_id = backup_id
        visited = set()
        while current_id and current_id not in visited:
            visited.add(current_id)
            try:
                backup = self.get_backup_by_id(current_id)
            except KeyError:
                break
            chain.append(backup)
            current_id = backup.prev_backup_id
        chain.reverse()
        return chain

    def get_backup_policies(self, cluster_id: Optional[str] = None) -> List[BackupPolicy]:
        prefix = cluster_id if cluster_id else " "
        return BackupPolicy().read_from_db(self.kv_store, id=prefix)

    def get_backup_policy_by_id(self, policy_id: str) -> BackupPolicy:
        policy = single_or_none(p for p in self.get_backup_policies() if p.uuid == policy_id)
        if policy is None:
            raise KeyError(f'BackupPolicy {policy_id} not found')
        return policy

    def get_backup_policy_attachments(self, cluster_id: Optional[str] = None) -> List[BackupPolicyAttachment]:
        prefix = cluster_id if cluster_id else " "
        return BackupPolicyAttachment().read_from_db(self.kv_store, id=prefix)

    def get_policy_for_lvol(self, lvol) -> Optional[BackupPolicy]:
        """Get the effective backup policy for an lvol.
        LVol-level policy overrides pool-level policy."""
        attachments = self.get_backup_policy_attachments(lvol.pool_uuid.split('/')[0] if '/' in lvol.pool_uuid else None)
        lvol_policy_id = None
        pool_policy_id = None
        for att in attachments:
            if att.target_type == "lvol" and att.target_id == lvol.get_id():
                lvol_policy_id = att.policy_id
            elif att.target_type == "pool" and att.target_id == lvol.pool_uuid:
                pool_policy_id = att.policy_id
        policy_id = lvol_policy_id or pool_policy_id
        if policy_id:
            try:
                return self.get_backup_policy_by_id(policy_id)
            except KeyError:
                return None
        return None
