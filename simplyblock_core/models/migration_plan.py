"""
migration_plan.py – a bulk online migration of volumes to another cluster.

One plan moves a selection of a cluster's volumes (all of them, one pool's, or
an explicit list) through migration-mode replication and the IO-freeze cutover
(replication-commit). The per-volume work is the ordinary FN_REPLICATION_FINAL
task; the plan only records the selection, the destination placement and the
task of each volume so progress and a forecast can be read for the whole move.
See docs/online-migration.md.
"""

from simplyblock_core.models.base_model import BaseModel, default_factory


class MigrationPlan(BaseModel):

    STATUS_RUNNING = 'running'
    STATUS_DONE = 'done'
    STATUS_PARTIAL = 'partial'      # finished, some volumes failed or were skipped
    STATUS_FAILED = 'failed'        # finished, no volume moved
    STATUS_CANCELED = 'canceled'

    TERMINAL_STATUSES = (STATUS_DONE, STATUS_PARTIAL, STATUS_FAILED, STATUS_CANCELED)

    # Per-volume states, in the order a volume passes through them.
    VOL_SKIPPED = 'skipped'
    VOL_REPLICATING = 'replicating'     # initial copy or catch-up
    VOL_QUEUED = 'queued'               # caught up, waiting for the LVS cutover slot
    VOL_CONVERGING = 'converging'       # shrink rounds
    VOL_CUTTING_OVER = 'cutting_over'   # freeze + ANA flip
    VOL_DONE = 'done'
    VOL_FAILED = 'failed'
    VOL_CANCELED = 'canceled'

    VOL_TERMINAL = (VOL_SKIPPED, VOL_DONE, VOL_FAILED, VOL_CANCELED)

    cluster_id: str = ""
    # Caller-chosen, unique among the cluster's unfinished plans; makes a
    # create idempotent (start_plan).
    plan_name: str = ""
    target_cluster_id: str = ""
    pool_id: str = ""
    delete_source: bool = False
    interval_min: int = 0
    status: str = STATUS_RUNNING
    created_at: int = 0
    finished_at: int = 0
    # One dict per selected volume: lvol_id, name, size, used_bytes,
    # lvs_name, target_node_id, state, task_id, error.
    volumes: list[dict] = default_factory(list)
    # Conditions found at planning time that do not refuse the plan but put it
    # at risk (e.g. too little free capacity for the worst-case snapshot pin).
    warnings: list[str] = default_factory(list)

    def get_id(self):
        # Prefixed with the source cluster so a cluster's plans are one range read.
        return "%s/%s" % (self.cluster_id, self.uuid)

    def is_finished(self):
        return self.status in self.TERMINAL_STATUSES
