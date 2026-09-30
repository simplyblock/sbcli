import datetime
from typing import ClassVar

from simplyblock_core.models.indices import Index
from simplyblock_core.models.base_model import BaseModel, default_factory


class JobSchedule(BaseModel):

    _WATCHED = True

    _INDEXES: ClassVar[tuple] = (
        # get_id() embeds cluster and date, which is what makes a per-cluster
        # range read cheap; `uuid` is what makes a lookup by the bare task id a
        # point read instead of a scan of the entire (never-pruned) table.
        Index('uuid'),
        Index(('cluster_id', 'function_name', 'status')),
    )

    STATUS_NEW = 'new'
    STATUS_RUNNING = 'running'
    STATUS_SUSPENDED = 'suspended'
    STATUS_DONE = 'done'

    FN_DEV_RESTART = "device_restart"
    FN_NODE_RESTART = "node_restart"
    FN_DEV_MIG = "device_migration"
    FN_FAILED_DEV_MIG = "failed_device_migration"
    FN_NEW_DEV_MIG = "new_device_migration"
    FN_NODE_ADD = "node_add"
    FN_NODE_REMOVAL = "node_removal"
    FN_PORT_ALLOW = "port_allow"
    FN_BALANCING_AFTER_NODE_RESTART = "balancing_on_restart"
    FN_BALANCING_AFTER_DEV_REMOVE = "balancing_on_dev_rem"
    FN_BALANCING_AFTER_DEV_EXPANSION = "balancing_on_dev_add"
    FN_JC_COMP_RESUME = "jc_comp_resume"
    FN_SNAPSHOT_REPLICATION = "snapshot_replication"
    FN_LVOL_SYNC_DEL = "lvol_sync_del"
    # Deferred per-node lvol operation ("register" / "resize") — DB-backed
    # replacement for the in-memory restart drain queue (incident 2026-07-10).
    FN_LVOL_SYNC_OP = "lvol_sync_op"
    FN_LVOL_MIG = "lvol_migration"
    FN_LVOL_BATCH_MIG = "lvol_batch_migration"
    FN_BACKUP = "s3_backup"
    FN_BACKUP_RESTORE = "s3_backup_restore"
    FN_BACKUP_MERGE = "s3_backup_merge"
    FN_CLUSTER_EXPAND = "cluster_expand"
    # Cross-cluster replication cutover: freeze source IO, transfer the final
    # lvol delta to the target, flip ANA so the client fails over. Used for
    # migration commit and fail-back (fresh or recovered source).
    FN_REPLICATION_FINAL = "replication_final"
    FN_FDB_BACKUP = "fdb_backup"
    # Sync replication: move the leadership of one or more LVS to the other
    # site's triplet and open the promoted volumes there (promote,
    # tasks_runner_sync_promote). ``node_id`` is the owner of the first LVS;
    # ``function_params``: ``site`` (the target), ``lvol_ids`` (the volumes to
    # open there), ``lvs_names`` every LVS the promote may move (a group
    # promote spans several), ``owners`` ({lvs: owner id}), and the journal
    # the runner keeps: ``moves`` ({lvs: lvs_active_site before}, written in
    # the transaction that sets the ``moving:`` markers) and ``transferring``
    # (the LVS whose leadership hand-off has started). While such a task is
    # active with a live lease it owns the move: no leaderless recovery grants
    # for those LVS, and an LVS it left at ``moving:<site>`` is reconciled
    # only afterwards (storage_node_ops.reconcile_lvs_move). Its runner grants
    # only through storage_node_ops.move_lvs_leadership, and on failure /
    # cancel records the task DONE BEFORE settling its markers (the previous
    # site written back before any hand-off, reconcile_lvs_move after one).
    # One pass, no retry: the caller's next promote call judges again.
    FN_SYNC_PROMOTE = "sync_promote"
    # Sync replication: catch up the lagging zone of one LVS after a zone
    # desync (tasks_runner_sync_resync). ``node_id`` is the owner of the LVS,
    # ``function_params["lvs_name"]`` the LVS; one active task per LVS
    # (tasks_controller.add_sync_resync_task). Unbounded (max_retry=-1): a
    # failed catch-up re-runs with backoff once the lagging zone is back.
    FN_SYNC_RESYNC = "sync_resync"

    canceled: bool = False
    cluster_id: str = ""
    date: int = 0
    device_id: str = ""
    function_name: str = ""
    function_params: dict = default_factory(dict)
    function_result: str = ""
    max_retry: int = -1
    node_id: str = ""
    retry: int = 0
    sub_tasks: list = default_factory(list)
    # Hostname of the runner that currently holds this task's lease. Empty
    # means unclaimed. Combined with updated_at (refreshed on every write) this
    # gives a soft lease: a different host may take over only once the lease
    # goes stale (see constants.TASK_LEASE_TTL_SEC). See tasks_controller.claim_task.
    owner: str = ""

    def watch_scope(self):
        return (self.cluster_id,)

    def write_to_db(self, kv_store=None):
        self.updated_at = str(datetime.datetime.now(datetime.UTC))
        super().write_to_db(kv_store)


    def get_id(self):
        return "%s/%s/%s" % (self.cluster_id, self.date, self.uuid)
