# coding=utf-8
import datetime
import time
from typing import List

from simplyblock_core import constants
from simplyblock_core.models.base_model import BaseModel


class LVolMigration(BaseModel):
    """
    Tracks the full lifecycle of a live volume migration between storage nodes.

    A migration moves a volume (lvol) from a source node to a target node without
    interrupting I/O. The process works in phases:

    1. SNAP_COPY   – copy the ordered snapshot chain from source → target (oldest first).
                     No new snapshots are allowed on the source during this phase.
                     Intermediate snapshots may be taken recursively to shrink the
                     remaining delta in the lvol itself.
    2. LVOL_MIGRATE – migrate the lvol (the live, writable blob). I/O is frozen for
                     only the brief moment needed to transfer the final delta.
    3. CLEANUP_SOURCE – remove snapshots from the source that are no longer needed
                     (safe only when no other volume on the source still references them).
    4. CLEANUP_TARGET – (failure path only) roll back by removing all copied snapshots
                     from the target (safe only when no other migrated volume references them).
    5. COMPLETED   – migration finished successfully.
    """

    STATUS_NEW = 'new'
    STATUS_RUNNING = 'running'
    STATUS_SUSPENDED = 'suspended'
    STATUS_DONE = 'done'
    STATUS_FAILED = 'failed'
    STATUS_CANCELLED = 'cancelled'

    PHASE_SNAP_COPY = 'snap_copy'
    PHASE_LVOL_MIGRATE = 'lvol_migrate'
    PHASE_CLEANUP_SOURCE = 'cleanup_source'
    PHASE_CLEANUP_TARGET = 'cleanup_target'
    PHASE_COMPLETED = 'completed'

    _STATUS_CODE_MAP = {
        STATUS_NEW: 0,
        STATUS_RUNNING: 1,
        STATUS_SUSPENDED: 2,
        STATUS_DONE: 3,
        STATUS_FAILED: 4,
        STATUS_CANCELLED: 5,
    }

    # --- Identity & topology ---
    cluster_id: str = ""
    lvol_id: str = ""
    source_node_id: str = ""
    target_node_id: str = ""

    # --- Phase tracking ---
    phase: str = ""

    # Ordered list of snapshot UUIDs to copy, oldest → newest.
    # Built once at migration start from the volume's snapshot chain.
    snap_migration_plan: List[str] = []

    # Snapshot UUIDs that are available on the target node (either transferred by
    # this migration or already present from a prior migration of a related volume).
    snaps_migrated: List[str] = []

    # Subset of snaps_migrated that were already present on the target node when
    # this migration started (e.g. copied by an earlier migration of a clone).
    # These must NEVER be deleted during CLEANUP_TARGET rollback.
    snaps_preexisting_on_target: List[str] = []

    # Snapshot UUIDs of intermediate ("shrink") snapshots taken on the source
    # during migration to progressively reduce the live delta. These are
    # created by the migration process itself and must be cleaned up afterward.
    intermediate_snaps: List[str] = []

    # In-progress RPC job ID for the currently executing data-plane operation
    # (either a snapshot copy or the final lvol migrate). Empty when idle.
    current_job_id: str = ""

    # Per-snapshot (or final-lvol) transfer context.  Tracks fine-grained state
    # so that the runner can poll an async transfer and resume after a restart.
    # Keys used: stage, snap_uuid, temp_nqn, ctrl_name, nqn (lvol migrate).
    transfer_context: dict = {}

    # Index into snap_migration_plan: the next snapshot to be copied.
    # Allows resuming after a suspension without re-copying already-migrated snaps.
    next_snap_index: int = 0

    # How many intermediate "shrink" snapshot rounds have already been performed.
    intermediate_snap_rounds: int = 0

    # Maximum number of intermediate shrink rounds before forcing the lvol migration.
    max_intermediate_snap_rounds: int = constants.LVOL_MIG_MAX_INTERMEDIATE_SNAPS

    # --- Timestamps ---
    started_at: int = 0
    completed_at: int = 0
    # Unix timestamp after which the migration must abort (0 = no deadline).
    deadline: int = 0

    # --- Error / retry tracking ---
    error_message: str = ""
    retry_count: int = 0
    max_retries: int = constants.LVOL_MIG_MAX_RETRIES
    canceled: bool = False

    def get_id(self):
        # Prefix with cluster_id so that FDB range queries can filter by cluster.
        return "%s/%s" % (self.cluster_id, self.uuid)

    def write_to_db(self, kv_store=None):
        self.updated_at = str(datetime.datetime.now(datetime.timezone.utc))
        super().write_to_db(kv_store)

    def is_active(self):
        return self.status in (self.STATUS_NEW, self.STATUS_RUNNING, self.STATUS_SUSPENDED)

    def has_deadline_passed(self):
        if not self.deadline:
            return False
        return time.time() > self.deadline
