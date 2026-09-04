# coding=utf-8

from typing import ClassVar, List

from simplyblock_core.models.base_model import BaseModel, default_factory


class LVol(BaseModel):

    STATUS_IN_CREATION = 'in_creation'
    STATUS_ONLINE = 'online'
    STATUS_OFFLINE = 'offline'
    STATUS_IN_DELETION = 'in_deletion'
    STATUS_RESTORING = 'restoring'
    STATUS_DELETED = 'deleted'
    STATUS_RESTORE_FAILED = 'restore_failed'

    _STATUS_CODE_MAP: ClassVar[dict] = {
        STATUS_ONLINE: 1,
        STATUS_OFFLINE: 2,
        STATUS_IN_DELETION: 3,
        STATUS_IN_CREATION: 4,
        STATUS_RESTORING: 5,
        STATUS_DELETED: 6,
        STATUS_RESTORE_FAILED: 7,
    }

    base_bdev: str = ""
    bdev_stack: List = default_factory(list)
    blobid: int = 0
    cloned_from_snap: str = ""
    comp_bdev: str = ""
    crypto_bdev: str = ""
    crypto_key_name: str = ""
    deletion_status: str = ""
    guid: str = ""
    ha_type: str = ""
    health_check: bool = True
    hostname: str = ""
    io_error: bool = False
    lvol_bdev: str = ""
    lvol_name: str = ""
    lvol_priority_class: int = 0
    lvol_type: str = "lvol"
    lvol_uuid: str = ""
    lvs_name: str = ""
    max_size: int = 0
    namespace: str = ""
    node_id: str = ""
    nodes: List[str] = default_factory(list)
    nqn: str = ""
    ns_id: int = 1
    # The UUID the NVMe namespace advertises on the wire when it differs from
    # the record's uuid (migration/fail-back clones inherit another volume's
    # identity so the client's multipath head keeps its paths). Empty means
    # the namespace carries the record's own uuid. connect_lvol reports it as
    # target_lvol_id so the CSI globs /dev/disk/by-id/nvme-uuid.<this>.
    ns_uuid: str = ""
    max_namespace_per_subsys: int = 1

    def get_ns_uuid(self) -> str:
        """The UUID this volume's NVMe namespace advertises on the wire.

        Every site that registers or verifies the namespace must use this,
        never the record ``uuid``: after a fail-back the two differ, and
        probing or re-adding by the record uuid flags a healthy volume
        unhealthy — or re-registers the namespace under an identity the
        client's multipath head rejects ("IDs don't match for shared
        namespace N"), severing its paths (run 2026-09-02 ~19:40: the lvol
        monitor's self-heal fought the fail-back identity every cycle).
        """
        return self.ns_uuid or self.uuid
    subsys_port: int = 9090
    # Node ids whose sync delete already completed inline in the API delete
    # call (lvol_controller._delete_lvol_from_all_nodes). lvol_monitor skips
    # these when it finalises the record, so a node never receives a second
    # sync delete — a repeat walks the replica blob tree again and errors on
    # every entry the first pass cleaned.
    sync_deleted_nodes: List[str] = default_factory(list)
    pool_uuid: str = ""
    pool_name: str = ""
    pvc_name: str = ""
    r_mbytes_per_sec: int = 0
    rw_ios_per_sec: int = 0
    rw_mbytes_per_sec: int = 0
    size: int = 0
    snapshot_name: str = ""
    top_bdev: str = ""
    vuid: int = 0
    w_mbytes_per_sec: int = 0
    fabric: str = "tcp"
    ndcs: int = 0
    npcs: int = 0
    allowed_hosts: List[dict] = default_factory(list)
    delete_snap_on_lvol_delete: bool = False
    do_replicate: bool = False
    replication_node_id: str = ""
    from_source: bool = True
    # Declared intent, NOT a behaviour switch: this field is only ever copied
    # onto the LVolReplication record (lvol_controller, tasks_runner_replication
    # _final) and nothing branches on it, so both modes share one identical
    # replication stream and differ purely in which ending is invoked.
    #   "failover":  async DR — the target volume is materialised by
    #                replicate_lvol_on_target_cluster, cloned from the last
    #                replicated snapshot, with IO already interrupted.
    #   "migration": planned move — the target volume is materialised by
    #                replication_commit, which shrinks the delta first and flips
    #                ANA, so the client is not interrupted.
    # An earlier comment claimed migration mode pre-creates the target subsystem
    # up front; it does not. The clone is built at the ending in both paths.
    replication_mode: str = "failover"
    # Interval in minutes for automatic internal snapshots that drive
    # replication. 0 disables interval snapshots (only user snaps replicate).
    replication_interval_min: int = 0
    # ReplicationPolicy.get_id() this volume follows, "" when it is not managed
    # by a policy. The fields above stay as the RESOLVED effective values so the
    # replication service keeps reading exactly what it reads today; attaching a
    # policy derives them from policy + target.
    replication_policy_id: str = ""

    def has_qos(self):
        return (self.rw_ios_per_sec > 0 or self.rw_mbytes_per_sec > 0 or self.r_mbytes_per_sec > 0 or self.w_mbytes_per_sec > 0)

    def write_to_db(self, kv_store=None):
        super().write_to_db(kv_store)
        lvol_mini = LVolMini().from_lvol(self)
        lvol_mini.write_to_db(kv_store)
        # Maintain the per-pool name index here so every create/update path keeps
        # it current (used for O(1) name-uniqueness instead of scanning all lvols).
        from simplyblock_core.db_controller import DBController
        DBController().index_lvol_name(self)

    def remove(self, kv_store):
        super().remove(kv_store)
        from simplyblock_core.db_controller import DBController
        DBController().unindex_lvol_name(self)
        try:
            lvol_mini = LVolMini().read_from_db(kv_store, self.uuid)[0]
            lvol_mini.remove(kv_store)
        except Exception as e:
            print(f"Failed to remove snapshot mini from DB: {e}")



class LVolReplication(BaseModel):
    # Lifecycle of the replication relationship.
    STATE_REPLICATING = "replicating"        # snapshots streaming to target
    STATE_CUTOVER_PENDING = "cutover_pending"  # final-step queued/running
    STATE_CUTOVER_DONE = "cutover_done"      # migration cutover completed
    STATE_FAILED_OVER = "failed_over"        # volume now live on target

    DIRECTION_TO_TARGET = "to_target"
    DIRECTION_TO_SOURCE = "to_source"

    source_lvol: LVol = None # type: ignore[assignment]
    target_lvol: LVol = None # type: ignore[assignment]
    source_cluster_id: str = ""
    target_cluster_id: str = ""
    mode: str = "failover"
    state: str = STATE_REPLICATING
    direction: str = DIRECTION_TO_TARGET
    # Identity of the pre-created / cut-over target subsystem. Preserved so the
    # client keeps the same NQN/namespace across fail-over and migration.
    target_nqn: str = ""
    target_ns_id: int = 0
    # Set to True by POST .../replication/cutover-proceed once the operator has
    # connected the target NVMe paths. The task runner waits for this before
    # calling run_cutover(); REPL_CUTOVER_PROCEED_TIMEOUT_SEC is the safety fallback.
    cutover_proceed: bool = False

class LVolMini(BaseModel):
    lvol_uuid: str = ""
    lvol_name: str = ""
    pool_uuid: str = ""
    pool_name: str = ""
    size: int = 0
    vuid: int = 0
    status: str = ""
    cloned_from_snap: str = ""
    nqn: str = ""
    node_id: str = ""
    namespace: str = ""
    hostname: str = ""
    blobid: int = 0
    ns_id: int = 0
    max_namespace_per_subsys: int = 0

    def from_lvol(self, lvol: LVol):
        self.uuid = lvol.uuid
        self.create_dt = lvol.create_dt
        self.lvol_uuid = lvol.lvol_uuid
        self.lvol_name = lvol.lvol_name
        self.pool_uuid = lvol.pool_uuid
        self.pool_name = lvol.pool_name
        self.size = lvol.size
        self.vuid = lvol.vuid
        self.status = lvol.status
        self.cloned_from_snap = lvol.cloned_from_snap
        self.nqn = lvol.nqn
        self.node_id = lvol.node_id
        self.namespace = lvol.namespace
        self.hostname = lvol.hostname
        self.blobid = lvol.blobid
        self.ns_id = lvol.ns_id
        self.max_namespace_per_subsys = lvol.max_namespace_per_subsys
        return self
