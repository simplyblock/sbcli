# coding=utf-8

from simplyblock_core.models.base_model import BaseModel, default_factory
from simplyblock_core.models.lvol_model import LVol, LVolMini


class SnapShot(BaseModel):

    STATUS_ONLINE = 'online'
    STATUS_OFFLINE = 'offline'
    STATUS_IN_DELETION = 'in_deletion'
    STATUS_IN_REPLICATION = 'in_replication'

    # User-created snapshots are kept indefinitely on the replication target.
    # Internal snapshots are taken automatically at a fixed interval purely to
    # drive replication; only the most recent successfully-replicated internal
    # snapshot is retained on the target (older ones are pruned).
    TYPE_USER = 'user'
    TYPE_INTERNAL = 'internal'

    base_bdev: str = ""
    blobid: int = 0
    cluster_id: str = ""
    #: Whole seconds, so snapshots taken within one second of each other are
    #: indistinguishable by it. It cannot order a lineage; prev_snap_uuid can.
    created_at: int = 0
    health_check: bool = True
    lvol: LVol = None # type: ignore[assignment]
    pool_uuid: str = ""
    ref_count: int = 0
    size: int = 0
    used_size: int = 0
    snap_bdev: str = ""
    snap_name: str = ""
    #: Ref-count bookkeeping only, never a lineage pointer. It names the
    #: snapshot holding the ref_count that this snapshot's volume contributes
    #: to as a clone, reached by dereferencing the clone source's own
    #: snap_ref_id (snapshot_controller.py:913-924). It is therefore written
    #: once per clone rather than once per snapshot -- every snapshot a given
    #: clone takes carries the same value -- and for a clone of a clone's
    #: snapshot it names neither the snapshot cloned from nor any blob parent.
    snap_ref_id: str = ""
    snap_uuid: str = ""
    vuid: int = 0
    deletion_status: str = ""
    status: str = ""
    fabric: str = "tcp"
    target_replicated_snap_uuid: str = ""
    source_replicated_snap_uuid: str = ""
    snap_type: str = "user"
    #: The volume's own snapshot sequence, in creation order. prev_snap_uuid
    #: is also the blob parent: a snapshot's blob holds only the clusters
    #: written since that predecessor. It is empty on the first snapshot taken
    #: on a volume, whose blob was created over whatever the volume was cloned
    #: from -- lvol.cloned_from_snap, frozen in the embedded LVol at the moment
    #: of the snapshot, so a later inflate of the volume cannot retract it.
    #: Those two, not snap_ref_id, are the ancestry a restore has to walk back.
    next_snap_uuid: str = ""
    prev_snap_uuid: str = ""
    instances: list[dict] = default_factory(list)
    # Uniquely identifies the data block device that backs this snapshot.
    # It is created once Snapshot is created from an LVol
    # On Snapshot transfer or replicate this field is the same
    # This value can be used to identify the same snapshot on other nodes
    data_uuid: str = ""

    def write_to_db(self, kv_store=None):
        super().write_to_db(kv_store)
        snap_mini = SnapShotMini().from_snapshot(self)
        snap_mini.write_to_db(kv_store)

    def remove(self, kv_store):
        super().remove(kv_store)
        try:
            snap_mini = SnapShotMini().read_from_db(kv_store, self.uuid)[0]
            snap_mini.remove(kv_store)
        except Exception as e:
            print(f"Failed to remove snapshot mini from DB: {e}")


class SnapShotMini(BaseModel):
    snap_uuid: str = ""
    snap_name: str = ""
    pool_uuid: str = ""
    size: int = 0
    status: str = ""
    lvol: LVolMini = None # type: ignore[assignment]
    next_snap_uuid: str = ""
    prev_snap_uuid: str = ""
    vuid: int = 0
    created_at: int = 0
    used_size: int = 0
    snap_type: str = "user"
    deleted: bool = False

    def from_snapshot(self, snapshot: SnapShot):
        self.uuid = snapshot.uuid
        self.create_dt = snapshot.create_dt
        self.snap_uuid = snapshot.snap_uuid
        self.snap_name = snapshot.snap_name
        self.pool_uuid = snapshot.pool_uuid
        self.size = snapshot.size
        self.status = snapshot.status
        self.lvol = LVolMini().from_lvol(snapshot.lvol)
        self.next_snap_uuid = snapshot.next_snap_uuid
        self.prev_snap_uuid = snapshot.prev_snap_uuid
        self.vuid = snapshot.vuid
        self.created_at = snapshot.created_at
        self.used_size = snapshot.used_size
        self.snap_type = snapshot.snap_type
        self.deleted = snapshot.deleted
        return self
