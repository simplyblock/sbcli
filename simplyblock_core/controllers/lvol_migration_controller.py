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



# ---------------------------------------------------------------------------
# Migration Controller
# ---------------------------------------------------------------------------

class MigrationController:
    """Controller orchestrates LVOL migrations."""

    m: MigrationObject

    def assign_lvol(lvol:LVol, target_lvs: str):
        m = MigrationObject()
        m.main_logical_volume.state = ObjectMigrationState.NEW

        #unique identifier:
        m.main_logical_volume.uuid = lvol.uuid

        m.main_logical_volume.bdev_name = lvol.lvol_bdev
        m.main_logical_volume.lvs_name = lvol.lvs_name
        m.main_logical_volume.target_lvs_name = target_lvs
        m.main_logical_volume.nqn = lvol.nqn
        m.main_logical_volume.source_uuid = lvol.lvol_uuid
        m.main_logical_volume.node_id = lvol.hostname
        if lvol.crypto_bdev != "":
           m.main_logical_volume.crypto_bdev_name = lvol.crypto_bdev
        m.main_logical_volume.mapid = 0
        m.main_logical_volume.namespace_id = lvol.namespace
        m.main_logical_volume.cloned = lvol.cloned_from_snap
        return m

    def assign_snap(lvol: LogicalVolumeRef, snap: SnapShot, target_lvs: str):
        s = Snapshot()
        s.status = ObjectMigrationState.NEW
        s.bdev_name = snap.snap_bdev.split("/", 1)[1]
        s.lvs_name = lvol.lvs_name
        s.target_lvs_name = lvol.target_lvs_name
        s.target_lvs_name = target_lvs
        s.uuid = snap.uuid
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
        self.rpc_client2.create_lvol(self, name, size_in_mib, lvs_name, lvol_priority_class=0, ndcs=0, npcs=0):
        ef
        create_lvol(self, name, size_in_mib, lvs_name, lvol_priority_class=0, ndcs=0, npcs=0):

        self.rpc.client2.lvol_set_migration_flag()
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

    def migrate_snaps(self):
        if self.m.status==MigrationState.RUNNING:
            for s in self.m.snapshots:
                if s.snapshot.status==ObjectMigrationState.NEW:
                   s.snapshot.target_uuid=self.create_target_lvol(s.snapshot.name)
                   s.snapshot.temporary_nqn,s.snapshot.temporary_namespace=self.create_tmp_nqn(s.snapshot.target_uuid)

                elif s.snapshot.status==ObjectMigrationState.SUSPENDED:




        return

    def migrate_lvol(self):
        return

    def cleanup_migration(status: bool):
        return

    def check_status_migration(self):
        return

    def migrate_lvol(self, lvol, target_node: str):
        """Migrate a logical volume and its snapshots/clones."""
        self.m.status = MigrationState.NEW

        #update lvol: frozen means it cannot be deleted or resized. new snapshots cannot be taken.
        db_controller = DBController()
        lvol.frozen=True
        lvol.write_to_db(db_controller.kv_store)

        #get all 4 storage node objects: primary, secondary source and target
        self.m.node_pri = StorageNode(db_controller.get_storage_node_by_id(self.m.main_logical_volume.node_id))
        self.m.node_sec = self.m.node_pri.secondary_node_id
        self.m.target_node_pri = StorageNode(db_controller.get_storage_node_by_id(target_node))
        self.m.target_node_sec = self.m.target_node_pri.secondary_node_id

        #copy now all data from the lvol to the migration lvol (temporary object for lvol during migration)
        self.m = self.assign_lvol(lvol, self.m.target_node_pri.lvstore)

        #create rpc clients for both primaries:
        self.rpc_client1 = self.connect_client(self.m.node_pri)
        self.rpc_client2 = self.connect_client(self.m.target_node_pri)

        #now we create a chain of snapshots from all snapshots taken from this lvol
        snapshots=db_controller.get_snapshots()
        snapshots.sort(key=lambda s: s.created_at)
        self.m.snapshots = []
        sr=None
        for s in snapshots:
                if s.lvol.uuid==self.m.main_logical_volume.uuid:
                     s.frozen=True
                     #need to reset that one on node restart
                     s.write_to_db(db_controller.kv_store)
                     srn=Snapshot()
                     if sr:
                       sr.next=srn
                     sr=srn
                     sr.snapshot=self.assign_snap(s.lvol,s)
                     self.m.snapshots.append(sr)


        if self.check_nodes_online(self.m.node_pri, self.m.node_sec, self.m.target_node_pri, self.m.target_node_sec):
            self.m.status = MigrationState.RUNNING
            self.migrate_snaps()
        else:
            logger.warning(f"Not all nodes online. Suspending lvol life migration {lvol.uuid}")
            self.m.status=MigrationState.SUSPENDED



