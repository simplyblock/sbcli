import asyncio
import logging
from operator import truediv
from os import MFD_ALLOW_SEALING

from e2e.utils.get_lba_diff_report import fetch_files
from ..models.lvol_migration import *
from dataclasses import dataclass
from typing import List, Optional
from simplyblock_core.rpc_client import RPCClient
from simplyblock_core.storage_node_ops import *
from simplyblock_core.db_controller import *
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.lvol_migration import Snapshot
from simplyblock_core.models.snapshot import SnapShot

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# ---------------------------------------------------------------------------
# Migration Service
# ---------------------------------------------------------------------------

class MigrationQueueObjectType:
    SNAPSHOT = "snapshot"
    CLONE = "clone"
    LVOL = "lvol"


@dataclass
class MigrationQueueObject:
    obj: object
    type: str
    status: ObjectMigrationState = ObjectMigrationState.NEW
    retries: int = 0
    last_offset: Optional[int] = None  # For snapshot continuation


class MigrationQueue:
    """Queue holding migration objects for a single LVOL."""

    def __init__(self):
        self.objects: List[MigrationQueueObject] = []

    def add(self, obj, obj_type, status=ObjectMigrationState.NEW):
        mqo = MigrationQueueObject(obj=obj, type=obj_type, status=status)
        self.objects.append(mqo)
        return mqo

    def reset(self):
        self.objects.clear()


class MigrationService:
    """Service containing core migration logic."""

    MAX_RETRIES = 3
    RETRY_DELAY = 5  # seconds, can be increased exponentially

    async def migrate_object(self, mqo: MigrationQueueObject, target_node: str, secondary_node: str):
        """Perform actual migration of snapshot/clone/lvol."""
        try:
            mqo.status = ObjectMigrationState.RUNNING
            logger.info(f"Starting migration of {mqo.type} {getattr(mqo.obj, 'name', None)}")

            # Simulate RPC / async migration
            await asyncio.sleep(0.1)  # replace with actual migration RPC

            # Example: if snapshot, migrate from source to target subsystem
            # handle last_offset, retries, errors here

            mqo.status = ObjectMigrationState.DONE
            logger.info(f"Completed migration of {mqo.type} {getattr(mqo.obj, 'name', None)}")
        except Exception as e:
            logger.error(f"Error migrating {mqo.type}: {e}")
            mqo.status = ObjectMigrationState.SUSPENDED
            if mqo.retries < self.MAX_RETRIES:
                mqo.retries += 1
                await asyncio.sleep(self.RETRY_DELAY * mqo.retries)
                await self.migrate_object(mqo, target_node, secondary_node)
            else:
                mqo.status = ObjectMigrationState.FAILED

    async def process_migration_queue(self, mq: MigrationQueue, all_nodes_online: callable):
        """Process the migration queue (snapshots -> clones -> LVOL)."""
        # Step 1: Snapshots
        for mqo in mq.objects:
            if mqo.type == MigrationQueueObjectType.SNAPSHOT and mqo.status in [ObjectMigrationState.NEW,
                                                                                ObjectMigrationState.RUNNING]:
                await self.migrate_object(mqo, target_node="primary", secondary_node="secondary")

        if any(mqo.status != ObjectMigrationState.DONE for mqo in mq.objects if
               mqo.type == MigrationQueueObjectType.SNAPSHOT) or not all_nodes_online():
            return ObjectMigrationState.CANCELED

        # Step 2: Clones
        for mqo in mq.objects:
            if mqo.type == MigrationQueueObjectType.CLONE and mqo.status in [ObjectMigrationState.NEW,
                                                                             ObjectMigrationState.RUNNING]:
                await self.migrate_object(mqo, target_node="primary", secondary_node="secondary")

        if any(mqo.status != ObjectMigrationState.DONE for mqo in mq.objects if
               mqo.type == MigrationQueueObjectType.CLONE) or not all_nodes_online():
            return ObjectMigrationState.CANCELED

        # Step 3: LVOL
        for mqo in mq.objects:
            if mqo.type == MigrationQueueObjectType.LVOL and mqo.status in [ObjectMigrationState.NEW,
                                                                            ObjectMigrationState.RUNNING]:
                await self.migrate_object(mqo, target_node="primary", secondary_node="secondary")

        if any(mqo.status != ObjectMigrationState.DONE for mqo in mq.objects if
               mqo.type == MigrationQueueObjectType.LVOL) or not all_nodes_online():
            return ObjectMigrationState.CANCELED

        return ObjectMigrationState.DONE

    async def cleanup_migration_queue(self, mq: MigrationQueue):
        """Remove all partially migrated objects from target."""
        for mqo in mq.objects:
            if mqo.status != ObjectMigrationState.NEW:
                logger.info(f"Cleaning up {mqo.type} {getattr(mqo.obj, 'name', None)} on target")
                await asyncio.sleep(0.05)  # simulate async delete RPC
                mqo.status = ObjectMigrationState.CANCELED

        mq.reset()


# ---------------------------------------------------------------------------
# Migration Controller
# ---------------------------------------------------------------------------

class MigrationController:
    """Controller orchestrates LVOL migrations."""

    m: MigrationObject

    def assign_lvol(lvol:LVol):
        m = MigrationObject()
        m.main_logical_volume.name = lvol.name
        m.main_logical_volume.state = ObjectMigrationState.NEW
        m.main_logical_volume.nqn = lvol.nqn
        m.main_logical_volume.uuid = lvol.uuid
        m.main_logical_volume.node_id = lvol.hostname
        if lvol.crypto_bdev != "":
           m.main_logical_volume.crypto_bdev_name = lvol.crypto_bdev
        m.main_logical_volume.mapid = 0
        m.main_logical_volume.namespace_id = lvol.namespace
        m.main_logical_volume.cloned = lvol.cloned_from_snap
        return m

    def assign_snap(lvol:LVol, snap: SnapShot):
        s = Snapshot()
        s.status = ObjectMigrationState.NEW
        s.name = snap.name
        s.source_uuid = snap.snap_uuid
        return s

    def create_tmp_nqn(self):
        #create subsystem
        #create listener
        #create namespace
        return

    def delete_tmp_nqn(self):
        return

    def create_target_object(self, is_lvol: bool):

        return

    def connect_client(node):
        return RPCClient(node.mgmt_ip, node.rpc_port, node.rpc_username, node.rpc_password, timeout=3, retry=1)

    def check_nodes_online(n1: StorageNode, n2: StorageNode, n3: StorageNode, n4: StorageNode):
        if (n1.status == StorageNode.STATUS_ONLINE and
                n2.status == StorageNode.STATUS_ONLINE and
                n2.status == StorageNode.STATUS_ONLINE and
                n3.status == StorageNode.STATUS_ONLINE):
            return True
        return False

    def migrate_stream(s: MigrationStream):
        return

    def cleanup_stream(s: MigrationStream):
        return

    def cleanup_migration(s: MigrationStream):
        return

    def migrate_streams(self):
            for s in self.m.streams:
                if s.status == StreamState.NEW or s.status == StreamState.RUNNING:
                    self.migrate_stream(s)
                if s.status == StreamState.CLEANUP:
                    self.cleanup_stream(s)
            self.cleanup_migration(True)
            partially=False
            final=MigrationState.DONE
            for s in self.m.streams:
                if s.status == StreamState.DONE:
                    partially=True
                if s.status == StreamState.FAILED:
                    final=MigrationState.PARTIALLY_FAILED
            if not partially:
                final = MigrationState.FAILED
            return final

    def check_status_migration(self):
        return

    def migrate_lvol(self, lvol, target_node: str):
        """Migrate a logical volume and its snapshots/clones."""
        db_controller = DBController()
        lvol.frozen=True
        lvol.write_to_db(db_controller.kv_store)
        self.m = self.assign_lvol(lvol)
        self.m.node_pri = StorageNode(db_controller.get_storage_node_by_id(self.m.main_logical_volume.node_id))
        self.m.node_sec = self.m.node_pri.secondary_node_id
        self.m.target_node_pri = StorageNode(db_controller.get_storage_node_by_id(target_node))
        self.m.target_node_sec = self.m.target_node_pri.secondary_node_id
        if self.check_nodes_online(self.m.node_pri, self.m.node_sec,self.m.target_node_pri, self.m.target_node_sec):
            self.rpc_client1 = self.connect_client(self.m.node_pri)
            self.rpc_client2 = self.connect_client(self.m.target_node_pri)
            lvols=db_controller.get_lvols_by_node_id(self.m.main_logical_volume.node_id)
            snapshots=db_controller.get_snapshots()
            self.m.snapshots = []
            for s in snapshots:
                if s.lvol.uuid==self.m.main_logical_volume.uuid:
                    self.m.snapshots.append(self.assign_snap(s.lvol,s))
                    s.frozen = True
                    s.write_to_db(db_controller.kv_store)
            for l in lvols:
                if

            #get all snapshots of lvol
            #get all clones
            #freeze service

            #now run
            #fill snapshots
            #fill lvols
            #create all streams

            self.migrate_streams()




