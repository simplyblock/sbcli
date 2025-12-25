import logging
from logging import exception
from time import sleep

from jc.parsers.asn1crypto.core import Boolean

from ..cluster_ops import db_controller
from ..models.lvol_migration import *
from dataclasses import dataclass
from typing import Optional, Dict
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
import copy



#TODOS: Integrate in Task Mgmt
#       Asynchronous delete of objects must check results before sync delete and cleanup is ready
#       must reconnect rpc clients after node restart
#       double-check all object states
#       we must convert with previous

# ---------------------------------------------------------------------------
# Migration Service
# ---------------------------------------------------------------------------

def generate_nqn():
    random_uuid = str(uuid.uuid4())
    nqn = f"nqn.2024-01.io.simplyblock:tmp:{random_uuid}"
    return nqn

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

    #connect clients for both primary (source) and secondary (target) nodes
    def connect_clients(self):
      try:
        self.m.rpc_client1 = self.connect_client(self.m.node_pri)
        self.m.rpc_client2 = self.connect_client(self.m.node_sec)
        self.m.rpc_client3 = self.connect_client(self.m.target_node_pri)
        self.m.rpc_client4 = self.connect_client(self.m.target_node_sec)
      except:
        raise f"migration {self.m.uuid}: cannot create rpc client for all nodes. all nodes online?"
      return

    def get_rpc_client(self, node: StorageNode):
        if node.uuid == self.m.node_pri.uuid:
            client = self.m.rpc_client1
        elif node.uuid == self.m.target_node_pri.uuid:
            client = self.m.rpc_client3
        elif node.uuid == self.m.node_sec.uuid:
            client = self.m.rpc_client2
        elif node.uuid == self.m.target_node_sec.uuid:
            client = self.m.rpc_client4
        else:
            raise RuntimeError(f"migration {self.m.uuid}: invalid node {node.uuid}, stopping. ")
        if not client or node.status != StorageNode.STATUS_ONLINE:
            raise RuntimeError(f"migration {self.m.uuid}: node {node.uuid} not online, stopping. ")
        return client

    def snap_assign(self, lvol: LogicalVolumeRef, snap: SnapShot):
        s = Snapshot()
        s.lvol=lvol
        s.snap=snap
        return s

    def lvol_assign(self, lvol: LVol):
        m = LogicalVolumeRef()
        m.lvol = lvol
        return m

    def check_nodes_online(self):
        if self.m.node_pri.status == StorageNode.STATUS_ONLINE and self.m.node_sec.status == StorageNode.STATUS_ONLINE and self.m.target_node_pri.status == StorageNode.STATUS_ONLINE and self.m.target_node_sec.status == StorageNode.STATUS_ONLINE:
               return True
        return False

    def raise_exception_on_error(self, ret: dict, err_str: str):
        error="object not found"
        if not ret or "error" in ret:
            if ret:
                error = f"{ret['error']['message']}:{ret['error']['code']}"
            raise RuntimeError(
                f"migration {self.m.uuid}:" + err_str + f": {error}")
        return True

    def get_transfer_state(self, node: StorageNode, counter: int):
        client = self.get_rpc_client(node)
        for m in self.m.completion_poll_queue:
            if m.status==ObjectMigrationState.TRANSFER:
              try:
                 name=m.snap.lvol.lvs_name+"/"+m.snap.snap_bdev
                 ret = client.bdev_lvol_transfer_stat(name)
                 self.raise_exception_on_error(ret, f"could not get transfer state for lvol: {name}")
                 if ret["transfer_state"]=="Done":
                        m.status=ObjectMigrationState.TRANSFERRED
                        self.m.write_to_db(db_controller.kv_store)
                        self.m.completion_poll_queue.remove(m)
                        return True, 0
                 else:
                     return False, ret["offset"]
              except:
                  logger.error(f"could not get transfer state for lvol")
                  return False, 0
        return False, 0

    def create_snapshot(self, node: StorageNode, index: int):
        client = self.get_rpc_client(node)
        ret=client.lvol_exists(node.lvstore,"mig_snap_"+str(index)+"_"+self.m.vol.lvol.lvol_name)
        if not ret or "error" in ret:
            ret=client.lvol_create_snapshot(self.m.vol.lvol.lvol_uuid, "mig_snap_"+str(index)+"_"+self.m.vol.lvol.lvol_name)
            self.raise_exception_on_error(ret, f"could not create snapshot for lvol: {self.m.vol.lvol.uuid}")
        for sn in self.m.snapshots:
            if sn.snap.uuid==ret["result"]:
                return True
        s=self.snap_assign(self.m.vol,ret["result"])
        self.m.snapshots.append(s)
        return True

    def migrations_list(self):
        self.db_controller = DBController()
        migrations = db_controller.get_migrations()
        data = []
        for m in migrations:
            logger.debug(m)
            data.append({
                "UUID": m.uuid,
                "Lvol UUID": m.vol.lvol.uuid,
                "Primary (source):": m.node_pri,
                "Primary (target):": m.target_node_pri,
                "DateTime:": m.create_dt,
                "Status": m.status,
            })
        return utils.print_table(data)

    @staticmethod
    def connect_client(node:StorageNode):
        return RPCClient(node.mgmt_ip, node.rpc_port, node.rpc_username, node.rpc_password, timeout=3, retry=1)

    def unfreeze_objects(self):
        self.db_controller = DBController()
        l = db_controller.get_lvol_by_id(self.m.vol.lvol.uuid)
        l.frozen = False
        l.write_to_db(db_controller.kv_store)
        snaps = db_controller.get_snapshots_by_node_id(self.m.node_pri)
        for s in snaps:
            s.frozen = False
            s.write_to_db(db_controller.kv_store)
        snaps = db_controller.get_snapshots_by_node_id(self.m.node_sec)
        for s in snaps:
            s.frozen = False
            s.write_to_db(db_controller.kv_store)
        return True

    def complete_snapshot_migration(self):
        tr=db_controller.kv_store.create_transaction()
        #snapshot objects are always create new, while lvols are really migrated
        for s in self.m.snapshots:
            if s.status==ObjectMigrationState.DONE:
                snapshot = copy.copy(s.snap)
                snapshot.uuid = str(uuid.uuid4())
                snapshot.snap_uuid = s.target_uuid
                snapshot.node_id=self.m.node_pri.get_id()
                snapshot.write_to_db(db_controller.kv_store,tr)

        lvol = copy.copy(self.m.vol.lvol)
        lvol.node_id=self.m.node_pri.get_id()
        lvol.lvol_bdev=self.m.vol.lvol.lvol_bdev
        lvol.blobid=self.m.vol.lvol.blobid
        lvol.lvol_uuid=self.m.vol.lvol.lvol_uuid
        lvol.lvs_name=self.m.vol.lvol.lvs_name
        lvol.write_to_db(db_controller.kv_store,tr)
        try:
          tr.commit.wait()
        except:
          raise RuntimeError(f"migration {self.m.uuid}: error updating snapshots and volumes in db.")
        return True

    def create_lvol(self, node: StorageNode, snap: Snapshot):
            client = self.get_rpc_client(node)
            name = node.lvstore + "/" + snap.snap.snap_bdev
            snap_uuid = client.lvol_exists(node.lvstore,node.lvstore+"/"+snap.snap.snap_bdev)
            if not snap_uuid or "error" in snap_uuid:
                     snap_uuid = client.create_lvol(name, snap.snap.size, self.m.target_node_pri.lvstore,
                                                         self.m.vol.lvol.lvol_priority_class,
                                                         self.m.vol.lvol.ndcs,
                                                         self.m.vol.lvol.npcs)
                     self.raise_exception_on_error(snap_uuid,f"could not create lvol on target: {snap.snap.uuid}")
            snap.target_uuid = snap_uuid["result"]
            return True

    def set_mig_status(self, node: StorageNode, snap: Snapshot):
            client = self.get_rpc_client(node)
            name = self.m.target_node_pri.lvstore + "/" + snap.snap.snap_bdev
            ret=client.bdev_lvol_set_migration_flag(name)
            self.raise_exception_on_error(ret, f"issue creating an target object during migration of snapshot {snap.uuid}")
            snap.status = ObjectMigrationState.MIG_FLAG_SET
            self.m.write_to_db(self.db_controller.kv_store)
            return True

    def export_lvol(self, node: StorageNode, nqn: str, s: Snapshot, anaState: str, namespaces: int, serial: str, model: str):
        client = self.get_rpc_client(node)
        #check if subsystem exists, namespace is added and listener exists
        #nqn=generate_nqn()
        ss,listener,ns=client.find_subsystem_by_nqn(nqn)
        if not ss:
             ret=client.subsystem_create(nqn,serial, model, 1, namespaces)
             self.raise_exception_on_error(ret, f"could not list subsystem for lvol: {s.snap.uuid}")
        if not ns:
             ret=client.nvmf_subsystem_add_ns(s.temporary_nqn,s.snap.lvol.lvs_name+"/"+s.snap.snap_bdev)
             self.raise_exception_on_error(ret,f"could not list subsystem for lvol: {s.snap.uuid} ")
        if not listener:
            if self.m.target_node_pri.active_rdma:
               fabric="RDMA"
            else:
               fabric="TCP"
            ret=client.nvmf_subsystem_add_listener(s.temporary_nqn, fabric,self.m.target_node_pri.nvmf_port,
                    self.m.target_node_pri.hostname, anaState)
            self.raise_exception_on_error(ret, f"could not list subsystem for lvol: {s.snap.uuid}")
        return True

    #delete subystem only, if there is only zero or one namespaces left;
    #if one namespace is left, it must match the volume
    def delete_subsystem(self, node: StorageNode, nqn:str, lvol: LVol):
        client=self.get_rpc_client(node)
        data=client.subsystem_list(nqn)
        if not data:
            return False
        ret = None
        for subsystem in data['result']:
            # Check if the subsystem has namespaces
            namespaces = subsystem.get('namespaces', None)
            if not namespaces or len(namespaces<2):
                   ret=client.subsystem_delete(nqn)
                   self.raise_exception_on_error(data, f"could not delete subsystem: {nqn} for lvol: {lvol.uuid}")
            elif len(namespaces>1):
                client.nvmf_subsystem_remove_ns(nqn,lvol.namespace)
        return True

    def connect_lvol(self, node: StorageNode, s: Snapshot):
        client = self.get_rpc_client(node)
        if node.active_rdma:
            transport="RDMA"
        else:
            transport="TCP"
        ret=client.nvmf_get_subsystems()
        subsystem = None
        if ret and not "error" in ret:
            subsystem = next((s for s in ret["result"] if s["nqn"] == s.temporary_nqn), None)
        attach=True
        if subsystem:
            attach=False
            first_namespace_name = subsystem.get("namespaces", [{}])[0].get("name")
            if first_namespace_name == None:
                client.bdev_nvme_detach_controller(s.snap.snap_bdev)
                self.raise_exception_on_error(ret, f"could not remove remote controller: {s.snap.uuid}")
            attach=True
        if attach:
           ret = client.bdev_nvme_attach_controller(s.snap.snap_bdev,s.temporary_nqn,node.hostname,node.nvmf_port,transport)
           self.raise_exception_on_error(ret, f"could not connect lvol: {s.snap.uuid}")
           s.controller = ret[0]
        return True

    def delete_lvol_from_node(self, node: StorageNode, oid: str, deleteType: bool):
        client=self.get_rpc_client(node)
        lvol=db_controller.get_lvol_by_id(oid)
        if lvol:
           ret=client.delete_lvol(lvol.lvs_name+"/"+lvol.lvol_name, deleteType)
        else:
           snap=db_controller.get_snapshot_by_id(oid)
           ret=client.delete_lvol(snap.lvol.lvs_name + "/" + snap.lvol.lvol_name, deleteType)
        self.raise_exception_on_error(ret, f"could not delete snapshot/lvol: {oid} ")
        return

    def transfer_data(self, node: StorageNode, snap: Snapshot, offset: int):
        try:
          client = self.get_rpc_client(node)
          ret=client.bdev_lvol_transfer(snap.snap.lvol.lvs_name+"/"+snap.snap.snap_bdev,offset,4,snap.controller, "migrate")
          self.raise_exception_on_error(ret, f"could not transfer data: {snap.snap.uuid} ")
        except Exception as e:
            logger.error(e)
            return False
        return True

    def convert_lvol(self, s: Snapshot):
        client=self.get_rpc_client(self.m.target_node_pri)
        ret=client.bdev_lvol_convert(s.snap.lvol.lvs_name+"/"+s.snap.snap_bdev)
        if ret and "exists" in ret:
            return True
        self.raise_exception_on_error(ret, f"could not convert lvol to snapshot: {s.snap.uuid} to remote subsystem:")
        return True

    def time_difference(self):
        return (datetime.now()-self.prev_time).total_seconds()

    def create_target_lvol(self, s: Snapshot):
        client = self.get_rpc_client(self.m.target_node_pri)
        ret=client.create_lvol(s.snap.snap_bdev,s.snap.size,self.m.target_node_pri.lvstore,self.m.vol.lvol.lvol_priority_class,self.m.vol.lvol.ndcs,self.m.vol.lvol.npcs)
        self.raise_exception_on_error(ret, f"could not create target lvol for snapshot:{s.snap.uuid}")
        return True

    def create_target_lvol2(self, node: StorageNode, l: LogicalVolumeRef):
        client = self.get_rpc_client(node)
        if l.lvol.crypto_bdev != "":
               client.lvol_crypto_create(l.lvol.crypto_bdev,l.lvol.lvol_bdev,l.lvol.crypto_key_name)
        ret = client.create_lvol(l.lvol.lvol_bdev, l.lvol.size, node.lvstore, l.lvol.lvol_priority_class, l.lvol.ndcs, l.lvol.npcs)
        ret=client.create_lvol(l.lvol.lvol_bdev,l.lvol.size,node.lvstore,l.lvol.lvol_priority_class,l.lvol.ndcs,l.lvol.npcs)
        self.raise_exception_on_error(ret, f"could not create target lvol for main lvol:{l.lvol.uuid}")
        return True

    def connect_hublvol(self, node: StorageNode):
        client = self.get_rpc_client(node)
        if node.active_rdma:
            fabric="RDMA"
        else:
            fabric="TCP"

        ret=client.bdev_nvme_controller_list("migratelvol")
        if not ret:
           ret=client.bdev_nvme_attach_controller("migratelvol",node.hublvol,node.hostname,node.nvmf_port,fabric)
           self.raise_exception_on_error(ret, f"could not attach controller for {self.m.vol.lvol.uuid} for hublvol")

        return True

    def transfer_data_final(self):
        client1 = self.get_rpc_client(self.m.node_pri)
        client2 = self.get_rpc_client(self.m.target_node_sec)
        client3 = self.get_rpc_client(self.m.target_node_pri)
        uuid, map_id = client3.lvol_exists(self.m.target_node_pri,self.m.vol)
        if not uuid:
             self.create_target_lvol2(self.m.target_node_pri,self.m.vol)
             uuid1, _ = client2.lvol_exists(self.m.target_node_sec, self.m.vol)
             if not uuid1:
                ret=client2.bdev_lvol_register(self.m.vol.lvol.lvol_bdev,self.m.target_node_sec.lvstore, self.m.vol.lvol.blobid, self.m.vol.lvol.lvol_uuid)
                self.raise_exception_on_error(ret, f"could not register on secondary {self.m.vol.lvol.uuid}")

        self.connect_hublvol(self.m.node_pri)

        uuid, map_id = client3.lvol_exists(self.m.target_node_pri.lvstore,self.m.vol.lvol.lvol_bdev)
        if not uuid or not map_id:
            raise  RuntimeError(
                f"migration {self.m.uuid}: could not get mapid of volume: {self.m.vol.lvol.uuid}")
        last_snap_uuid = (self.m.snapshots)[-1].snap.snap_uuid
        ret = client1.bdev_lvol_final_migration(self.m.vol.lvol.lvol_bdev,map_id,
                                                last_snap_uuid,4,self.m.target_node_pri.hublvol.nqn)
        self.raise_exception_on_error(ret, f"could not initiate final lvol migration: {self.m.vol.lvol.uuid}")
        return True

    def delete_hublvol_controller(self):
        return

    def reconnect_subsystems(self):

        #if "error" in ret:
        #    raise f"migration {self.m.uuid}: could not convert lvol to snapshot: {s.uuid} to remote subsystem:  {ret["error"]["message"]}:{ret["error"]["code"]}"
        return

    def cleanup_migration(self, status: bool):
        db_controller = DBController()
        real_snapshots = db_controller.get_snapshots()
        self.unfreeze_objects()
        #Migration was not successful
        try:
          if self.m.status >= MigrationState.HUBLVOL_CONNECTED:
              self.delete_hublvol_controller()
          if not status:
              pri_node=self.m.node_pri
              sec_node=self.m.node_sec
          else:
              pri_node = self.m.target_node_pri
              sec_node = self.m.target_node_sec

          if (self.m.status >= MigrationState.TARGET_LVOL_CREATED and not status) or self.m.status == MigrationState.DONE:
              self.delete_subsystem(pri_node, self.m.vol.lvol.nqn, self.m.vol.lvol)
              self.delete_subsystem(sec_node, self.m.vol.lvol.uuid, )
              self.delete_lvol_from_node(pri_node, self.m.vol.lvol.uuid, True)
              self.(sec_node, self.m.vol.lvol.uuid)

          snaps = self.m.snapshots
          snaps.reverse()
          for sn in snaps:
                     if sn.snap.uuid:
                        rsn = db_controller.get_snapshot_by_id(sn.snap.uuid)
                        if len(rsn.successor)==1:



                            self.delete_lvol_from_node(pri_node, sn.snap.uuid, True)
                            self.delete_subsystem(pri_node,sn.snap.uuid)
                            self.delete_lvol_from_node(sec_node, sn.snap.uuid)
                        else:
                            break
        except:
            raise f"cleanup of migration not successful, will try later {self.m.uuid}"
        return True

    def migrate_final_lvol(self):
      try:
        if self.m.status==MigrationState.SNAPS_MIGRATED:
           self.transfer_data_final()
        elif self.m.status==MigrationState.TARGET_LVOL_CREATED:
           self.connect_hublvol()
        elif self.m.status==MigrationState.HUBLVOL_CONNECTED:
           self.transfer_data_final()
        elif self.m.status==MigrationState.TRANSFERRED_TO_TARGET:
           self.reconnect_subsystems()
        elif self.m.status == MigrationState.RECONNECT_DONE:
           self.cleanup_migration(True)
      except:
        raise f"cannot transfer to target: {self.m.vol.lvol.uuid}"
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
                  self.set_mig_status(self.m.target_node_pri,s)
              elif s.status in ObjectMigrationState.MIG_FLAG_SET:
                  self.export_lvol(s)
              elif s.status in ObjectMigrationState.LVOL_EXPORTED:
                  self.connect_lvol(s)
              elif s.status in ObjectMigrationState.LVOL_CONNECTED:
                  self.transfer_data(s, 0)
              elif s.status==ObjectMigrationState.TRANSFERRED:
                   self.convert_lvol(s,p)
              elif s.status == ObjectMigrationState.CONVERTED:
                   self.delete_subsystem(self.m.target_node_pri,s.snap.uuid)
              elif s.status == ObjectMigrationState.CLEANING:
                   self.delete_lvol_from_node(self.m.target_node_sec, s.snap.uuid)
              p=s
            if self.m.rerun < 3 or self.time_difference()>5:
                ret, snap_uuid=self.create_snapshot(self.m.vol)
                sn=self.snap_assign(self.m.vol,snap_uuid)
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

    def lvol_migrate(self, lvol: LVol, target_node: StorageNode, m: MigrationObject=None):
        """Migrate a logical volume and its snapshots/clones."""

        # if this Migration Object does not exist (first call to lvol_migrate):
        if not m:
          try:
            self.m = MigrationObject()
            self.m.uuid = str(uuid.uuid4())
            self.m.create_dt = str(datetime.datetime)
            self.m.status = MigrationState.NEW
            self.m.write_to_db(self.db_controller.kv_store)
          except:
              return False #not even in database, lvol_migrate call must be repeated
        else:
            self.m = m

        # freeze lvols and snapshots during migration
        try:
            lvol.frozen = True
            lvol.write_to_db(self.db_controller.kv_store)

            # copy now all data from the lvol to the migration lvol (temporary object for lvol during migration)

            self.m.node_pri = StorageNode(self.db_controller.get_storage_node_by_id(lvol.node_id))
            self.m.node_sec = self.db_controller.get_storage_node_by_id(self.m.node_pri.secondary_node_id)
            self.m.target_node_pri = target_node
            self.m.target_node_sec = self.db_controller.get_storage_node_by_id(self.m.target_node_pri.secondary_node_id)

            self.m.vol = self.lvol_assign(lvol)

            # get all 4 storage node objects: primary, secondary source and target

            # create rpc clients for both primaries:
            self.connect_clients()

            # now we create a chain of snapshots from all snapshots taken from this lvol
            snapshots = self.db_controller.get_snapshots()
            snapshots.sort(key=lambda s: s.created_at)
            self.m.snapshots = []
            sr = None
            for s in snapshots:
                if s.lvol.uuid == self.m.vol.lvol.uuid:
                    s.frozen = True
                    # need to reset that one on node restart
                    s.write_to_db(self.db_controller.kv_store)
                    sr = self.snap_assign(self.m.vol, s)
                    self.m.snapshots.append(sr)
        except:
            return True
        self.m.status=MigrationState.RUNNING
        self.m.write_to_db(self.db_controller.kv_store)
        self.migrate_snaps()
        return True

        if self.check_nodes_online():
            self.m.status = MigrationState.RUNNING
            self.m.write_to_db(self.db_controller.kv_store)
            self.migrate_snaps()
            return True
        else:
            logger.warning(f"Not all nodes online. Suspending lvol life migration {lvol.uuid}")
            self.m.write_to_db(self.db_controller.kv_store)
            return -1

    def check_status_migration(self, on_restart: bool):
      while True:
          sleep(10)
          try:
            migrations=self.db_controller.get_migrations()
            for m in migrations:
              if m.status!=MigrationState.DONE and m.status!=MigrationState.FAILED:
                 if self.check_nodes_online():
                     if m.status==MigrationState.NEW:
                         self.lvol_migrate(m.vol.lvol,m.node_pri,m)
                     elif m.status==MigrationState.RUNNING:
                         for q in m.completion_poll_queue:
                             m.completion_poll_queue.remove(q)
                             if q.status==ObjectMigrationState.TRANSFER:
                                 result, offset = self.get_transfer_state(self.m.node_pri,q.retry)
                                 if not result:
                                     if q.retry > 5:
                                         raise f"could not transfer snapshot. max retries. name: {q.snap.lvol.lvs_name + "/" + q.snap.snap_bdev}. uuid: {q.snap.uuid}"
                                     q.retry += 1
                                     self.transfer_data(self.m.node_pri,q,offset)
                                     m.completion_poll_queue.append(q)
                         self.migrate_snaps()
                     else:
                          self.migrate_final_lvol()
          except:
              logger.error(f"migration controller exception. Migration failed: {self.m.uuid} ")
              self.m.status=MigrationState.FAILED
              self.cleanup_migration(False)
              return False
          return True

    migrate_lock = threading.Lock()

    def add_new_migration(self, lvol, target_node: StorageNode):
      with self.migrate_lock:
            try:
              migrations = self.db_controller.get_migrations()
              for m in migrations:
                if lvol.node_id==m.vol.lvol.node_id and (m.status!=MigrationState.DONE or m.status!=MigrationState.FAILED_AND_CLEANED):
                   raise exception("cannot add migration - ongoing migration")
              self.lvol_migrate(lvol, target_node)
            except:
              logger.error(f"could not add lvol {lvol.uuid} for migration as another migration is currently running.")
              return False
            return self.lvol_migrate(lvol,target_node)

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

