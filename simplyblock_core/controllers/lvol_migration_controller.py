import logging
from logging import exception
from time import sleep

from ..cluster_ops import db_controller
from ..models.lvol_migration import *
from dataclasses import dataclass
from typing import Optional
from simplyblock_core.storage_node_ops import *
from simplyblock_core.db_controller import *
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.lvol_migration import Snapshot
from simplyblock_core.models.snapshot import SnapShot
from datetime import datetime

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
import uuid



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


    def __init__(self):
        self._stop_event = threading.Event()
        self.lock = threading.Lock()
        self.m: MigrationObject = MigrationObject()
        self.db_controller = DBController()
        self.prev_time = datetime.now()

    def lvol_assign(self, lvol:LVol, target_lvs: str):
        m=MigrationObject()
        m.main_logical_volume.state = ObjectMigrationState.NEW

        #unique identifier:
        m.main_logical_volume.retry=0
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
        m.main_logical_volume.size = lvol.size
        m.main_logical_volume.ndcs = lvol.ndcs
        m.main_logical_volume.npcs = lvol.npcs
        m.main_logical_volume.priority_class = lvol.lvol_priority_class
        m.main_logical_volume.namespace_id = lvol.namespace
        m.main_logical_volume.cloned = lvol.cloned_from_snap
        return m.main_logical_volume

    def snap_assign(lvol: LogicalVolumeRef, snap: SnapShot, target_lvs: str):
        s = Snapshot()
        s.retry = 0
        s.status = ObjectMigrationState.NEW
        s.bdev_name = snap.snap_bdev.split("/", 1)[1]
        s.lvs_name = lvol.lvs_name
        s.lvol_size = snap.size
        s.target_lvs_name = lvol.target_lvs_name
        s.target_lvs_name = target_lvs
        s.uuid = snap.uuid
        s.source_uuid = snap.snap_uuid
        return s

    def snap_init(self, uuid: str, lvol: LogicalVolumeRef, target_lvs: str):
        s = Snapshot()
        s.retry = 0
        s.status = ObjectMigrationState.NEW
        s.bdev_name = "MIG_SNAP"
        s.lvs_name = lvol.lvs_name
        s.lvol_size = lvol.size
        s.target_lvs_name = lvol.target_lvs_name
        s.target_lvs_name = target_lvs
        s.uuid = uuid
        s.source_uuid = uuid
        return s


    @property
    def connect_client(node:StorageNode):
        return RPCClient(node.mgmt_ip, node.rpc_port, node.rpc_username, node.rpc_password, timeout=3, retry=1)

    def check_nodes_online(self, n1: StorageNode, n2: StorageNode, n3: StorageNode, n4: StorageNode):
        if (n1.status == StorageNode.STATUS_ONLINE and
                n2.status == StorageNode.STATUS_ONLINE and
                n3.status == StorageNode.STATUS_ONLINE and
                n4.status == StorageNode.STATUS_ONLINE):
            return True
        return False

    def unfreeze_objects(self):
        db_controller = DBController()
        l = db_controller.get_lvol_by_id(self.m.main_logical_volume.uuid)
        l.frozen = False
        l.write_to_db(db_controller.kv_store)
        snaps = db_controller.get_snapshots_by_node_id(self.m.node_pri.uuid)
        for s in snaps:
            s.frozen = False
            s.write_to_db(db_controller.kv_store)
        return

    def get_transfer_state(self, lvolname: str, node_id: str):

        return

    def export_lvol(self, s: Snapshot):
        # create subsystem
        # create listener
        # create namespace
        return

    def delete_tmp_nqn(self, s: Snapshot):
        return

    def get_lvol_by_name(self, lvol_name):
        return

    def create_lvol(self, snap: Snapshot):
            name = snap.target_lvs_name + "/" + snap.bdev_name
            if snap.status == ObjectMigrationState.NEW:
                snap_uuid=self.get_lvol_by_name(name)
                if not snap_uuid:
                   snap_uuid = self.rpc_client2.create_lvol(name, snap.size, snap.target_lvs_name,
                                                         self.m.main_logical_volume.priority_class,
                                                         self.m.main_logical_volume.ndcs,
                                                         self.m.main_logical_volume.npcs)
                if snap_uuid:
                    snap.target_uuid = snap_uuid
                    snap.status = ObjectMigrationState.LVOL_CREATED
                    self.m.write_to_db(self.db_controller.kv_store)
                else:
                    raise exception(f"could not create lvol on target. snap: {snap.uuid}")
            return True

    def set_mig_status(self, snap: Snapshot):
            name = snap.target_lvs_name + "/" + snap.bdev_name
            if snap.status == ObjectMigrationState.LVOL_CREATED:
                if not self.rpc_client2.lvol_set_migration_flag(name):
                    raise (f"issue creating an target object during migration of snapshot {snap.uuid} ")
                else:
                    snap.status = ObjectMigrationState.MIG_FLAG_SET
                    self.m.write_to_db(self.db_controller.kv_store)
            return True

    def connect_lvol(self, s: Snapshot):
        return

    def transfer_data(self, snap: Snapshot, offset: int):
            self.m.completion_poll_queue.append(snap)
            return

    def convert_lvol(self, s: Snapshot):
            return

    def convert_to_snap(self, s1, s2: Snapshot):
            return

    def create_snapshot(self, lvol: LogicalVolumeRef):
            return

    def time_difference(self):
           return (datetime.now()-self.prev_time).total_seconds()

    def create_target_lvol(self, s: Snapshot):
          return

    def create_final_lvol(self):
        return

    def connect_hublvol(self):
          return

    def transfer_data_final(self):
          return

    def reconnect_subsystems(self):
         return

    def set_mig_state_lvol(self, s: Snapshot):
         return

    def cleanup_migration(self, status: bool):
        db_controller = DBController()
        real_snapshots = db_controller.get_snapshots()
        self.unfreeze_objects()
        #Migration was not successful
        if not status:
              return
        else:
              return
        return

    def migrate_final_lvol(self):
      try:
        if self.m.status==MigrationState.SNAPS_MIGRATED:
           self.create_final_lvol()
        elif self.m.status==MigrationState.TARGET_LVOL_CREATED:
           self.connect_hublvol()
        elif self.m.status==MigrationState.HUBLVOL_CONNECTED:
           self.transfer_data_final()
        elif self.m.status==MigrationState.TRANSFERRED_TO_TARGET:
           self.reconnect_subsystems()
        elif self.m.status == MigrationState.RECONNECT_DONE:
           self.cleanup_migration(True)
      except:
        raise (f"cannot transfer to target: {self.m.main_logical_volume.uuid}")
      return True


    def migrate_snaps(self):
        if self.m.status==MigrationState.RUNNING:
          try:
            all_snaps_done = True
            p=""
            for s in self.m.snapshots:
              if s.status is not ObjectMigrationState.DONE:
                  all_snaps_done = False
              if s.status in ObjectMigrationState.NEW:
                  self.create_target_lvol(s)
              elif s.status in ObjectMigrationState.LVOL_CREATED:
                  self.set_mig_state_lvol(s)
              elif s.status in ObjectMigrationState.MIG_FLAG_SET:
                  self.export_lvol(s)
              elif s.status in ObjectMigrationState.LVOL_EXPORTED:
                  self.connect_lvol(s)
              elif s.status in ObjectMigrationState.LVOL_CONNECTED:
                  self.transfer_data(s, 0)
              elif s.status==ObjectMigrationState.TRANSFERRED:
                   self.convert_to_snap(s,p)
              elif s.status == ObjectMigrationState.CONVERTED:
                   self.delete_tmp_nqn(s)
              p=s
            if self.m.rerun < 3 or self.time_difference()>5:
                snap_uuid=self.create_snapshot(self.m.main_logical_volume)
                sn=self.snap_init(snap_uuid,self.m.main_logical_volume,self.m.target_node_pri.lvstore)
                self.m.snapshots.append(sn)
                self.prev_time=datetime.now()
                self.migrate_snaps()
            elif all_snaps_done:
                self.m.status = MigrationState.SNAPS_MIGRATED
                self.m.write_to_db(self.db_controller.kv_store)
                self.migrate_final_lvol()
          except:
               self.m.pre_status = self.m.status
               self.m.status = MigrationState.FAILED
               self.cleanup_migration(False)
        return True

    def lvol_migrate(self, lvol: LogicalVolumeRef, target_node: StorageNode, m: MigrationObject=None):
        """Migrate a logical volume and its snapshots/clones."""

        # if this Migration Object does not exist (first call to lvol_migrate):
        if not m:
            self.m = MigrationObject()
            self.m.uuid = str(uuid.uuid4())
            self.m.create_dt = str(datetime.datetime)
            self.m.status = MigrationState.NEW
            self.m.write_to_db(self.db_controller.kv_store)
        else:
            self.m = m

        # update lvol: frozen means it cannot be deleted or resized. new snapshots cannot be taken.
        try:
            lvol1=self.db_controller.get_lvol_by_id(lvol.uuid)
            lvol1.frozen = True
            lvol1.write_to_db(self.db_controller.kv_store)

            # copy now all data from the lvol to the migration lvol (temporary object for lvol during migration)
            self.m.main_logical_volume = self.lvol_assign(lvol)

            # get all 4 storage node objects: primary, secondary source and target
            self.m.node_pri = StorageNode(self.db_controller.get_storage_node_by_id(self.m.main_logical_volume.node_id))
            self.m.node_sec = self.db_controller.get_storage_node_by_id(self.m.node_pri.secondary_node_id)
            self.m.target_node_pri = target_node
            self.m.target_node_sec = self.db_controller.get_storage_node_by_id(self.m.target_node_pri.secondary_node_id)

            # create rpc clients for both primaries:
            self.rpc_client1 = self.connect_client
            self.rpc_client2 = self.connect_client

            # now we create a chain of snapshots from all snapshots taken from this lvol
            snapshots = self.db_controller.get_snapshots()
            snapshots.sort(key=lambda s: s.created_at)
            self.m.snapshots = []
            sr = None
            for s in snapshots:
                if s.lvol.uuid == self.m.main_logical_volume.uuid:
                    s.frozen = True
                    # need to reset that one on node restart
                    s.write_to_db(self.db_controller.kv_store)
                    sr = self.snap_assign(self.m.main_logical_volume, s,  self.db_controller.get(self.m.target_node_pri.lvstore)
                    self.m.snapshots.append(sr)
        except:
            return False

        if self.check_nodes_online(self.m.node_pri, self.m.node_sec, self.m.target_node_pri, self.m.target_node_sec):
            self.m.status = MigrationState.RUNNING
            self.m.write_to_db(self.db_controller.kv_store)
            self.migrate_snaps()
            return True
        else:
            logger.warning(f"Not all nodes online. Suspending lvol life migration {lvol.uuid}")
            self.m.write_to_db(self.db_controller.kv_store)
            return False

    def check_status_migration(self, on_restart: bool):
      while True:
          sleep(10)
          try:
            migrations=self.db_controller.get_migrations()
            for m in migrations:
              if m.status!=MigrationState.DONE and m.status!=MigrationState.FAILED:
                 if self.check_nodes_online(m.node_pri,self.db_controller.get_storage_node_by_id(m.node_pri.secondary_node_id),
                                            m.target_node_pri,m.target_node_sec):
                     if (m.status==MigrationState.NEW):
                         self.lvol_migrate(m.main_logical_volume,m.node_pri,m)
                     elif (m.status==MigrationState.RUNNING):
                         for q in m.completion_poll_queue:
                             m.completion_poll_queue.remove(q)
                             if (q.status==ObjectMigrationState.TRANSFER):
                                 if q.retry>5:
                                     raise (f"could not transfer snapshot. max retries. name: {q.lvs_name+"/"+q.bdev_name}. uuid: {q.uuid}")
                                 q.retry+=1
                                 result=self.get_transfer_state(q.target_lvs_name+"/"+q.bdev_name)
                                 if not result.status:
                                    self.transfer_data(q,result.offset)
                                    m.completion_poll_queue.append(q)
                                 else:
                                    q.status=ObjectMigrationState.TRANSFERRED
                             self.migrate_snaps
                     elif (m.status in (MigrationState.SNAPS_MIGRATED, MigrationState.HUBLVOL_CONNECTED, MigrationState.TARGET_LVOL_CREATED, MigrationState.TRANSFERRED_TO_TARGET, MigrationState.RECONNECT_DONE)):
                          self.migrate_final_lvol()
          except:
              logger.error(f"migration controller exception. Migration failed: {m.uuid} ")
              m.status=MigrationState.FAILED
              self.cleanup_migration(m, False)
              return False
          return True

    migrate_lock = threading.Lock()

    def add_new_migration(self, lvol, target_node: StorageNode):
      with self.migrate_lock:
            try:
              migrations = self.db_controller.get_migrations()
              for m in migrations:
                if lvol.node_id==m.main_logical_volume.node_id and (m.status!=MigrationState.DONE or m.status!=MigrationState.FAILED_AND_CLEANED):
                   raise exception("cannot add migration - ongoing migration")
              self.lvol_migrate(lvol, target_node)
            except:
              logger.error(f"could not add lvol {lvol.uuid} for migration as another migration is currently running.")
              return False

        #are all 4 nodes online?
        #if migration is suspended, resume it. If it was before in
        #depending on previous state, continue in migrate_snaps, migrate_lvol or cleanup
        #did total time expire? --> cleanup, failed
        #any snaps in queue?
        #poll for completion, trigger restart or if completed change the state
        #stop
      return

    def migrations_list(self):
        db_controller = DBController()
        migrations = db_controller.get_migrations()
        data = []
        for m in migrations:
            logger.debug(m)
            data.append({
                "UUID": m.uuid,
                "Lvol UUID": m.main_logical_volume.uuid,
                "Primary (source):": m.node_pri,
                "Primary (target):": m.target_node_pri,
                "DateTime:": m.create_dt,
                "Status": m.status,
            })
        return utils.print_table(data)

    def start_service(self, on_restart=False):
        """
        Starts the migration checker in a background thread.
        """
        self._thread = threading.Thread(
            target=self.check_status_migration, args=(on_restart,), daemon=True
        )
        self._thread.start()

    def stop_service(self):
        """
        Stops the background service gracefully.
        """
        self._stop_event.set()
        self._thread.join()

