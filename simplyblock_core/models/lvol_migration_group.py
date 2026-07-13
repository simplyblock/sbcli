# coding=utf-8
"""
lvol_migration_group.py – coordination record for a batch migration of all
lvols within a shared NVMe-oF namespace subsystem.

One LVolMigrationGroup is created per batch migration.  Each participating
LVolMigration record references it via migration_group_id.

Phase state-machine
-------------------
PRECREATE
    Main orchestrator creates the target subsystem and all N migration bdevs.
SNAP_COPY
    N worker tasks copy their owned snapshot chains in parallel.
    snap_copy_done tracks which workers have finished.
INTERMEDIATE
    All workers take exactly one intermediate ('shrink') snapshot each.
    intermediates_done tracks which workers have finished.
BATCH_MIGRATE
    Main calls bdev_lvol_batch_final_step with all lvols ordered by ns_id.
    batch_result is set to True on success, False on failure.
CLEANUP_SOURCE
    Workers delete their own source bdevs/snaps.  Main performs the ANA flip
    and source subsystem teardown, then waits for cleanup_source_done.
CLEANUP_TARGET  (failure / cancel path)
    Main signals batch_result=False; workers go to CLEANUP_TARGET and delete
    their target snaps.  Main deletes the target subsystem.
COMPLETED
    All N migrations finished successfully.
"""

import datetime
from typing import List, Optional

from simplyblock_core.models.base_model import BaseModel


class LVolMigrationGroup(BaseModel):

    PHASE_PRECREATE      = 'precreate'
    PHASE_SNAP_COPY      = 'snap_copy'
    PHASE_INTERMEDIATE   = 'intermediate'
    PHASE_BATCH_MIGRATE  = 'batch_migrate'
    PHASE_CLEANUP_SOURCE = 'cleanup_source'
    PHASE_CLEANUP_TARGET = 'cleanup_target'
    PHASE_COMPLETED      = 'completed'

    STATUS_RUNNING   = 'running'
    STATUS_DONE      = 'done'
    STATUS_FAILED    = 'failed'
    STATUS_CANCELLED = 'cancelled'

    _STATUS_CODE_MAP = {
        STATUS_RUNNING:   1,
        STATUS_DONE:      3,
        STATUS_FAILED:    4,
        STATUS_CANCELLED: 5,
    }

    # --- Identity & topology ---
    cluster_id:     str = ""
    source_node_id: str = ""
    target_node_id: str = ""

    # Shared NVMe-oF NQN created on the target during PRECREATE.
    target_nqn: str = ""

    # Ordered list of {ns_id: int, migration_id: str} dicts sorted by ns_id
    # ascending.  This is the order bdev_lvol_batch_final_step expects.
    members: List[dict] = []

    # Static snap-ownership map: snap_uuid → migration_id.
    # Computed once at group creation from all members' snapshot chains.
    # Each snap is assigned to the member with the lowest ns_id that references
    # it.  Workers only transfer their owned snaps; the rest are immediately
    # marked snaps_preexisting_on_target on that worker.
    snap_owners: dict = {}

    # migration_ids that finished transferring their owned snaps and are now
    # waiting for the INTERMEDIATE phase signal from the main orchestrator.
    snap_copy_done: List[str] = []

    # migration_ids that have taken and transferred their single intermediate
    # snapshot and are waiting for batch_result.
    intermediates_done: List[str] = []

    # migration_ids that have completed CLEANUP_SOURCE.
    cleanup_source_done: List[str] = []

    # Written by the main orchestrator after bdev_lvol_batch_final_step.
    # None = call not yet made; True = success; False = failure.
    batch_result: Optional[bool] = None

    # Current orchestration phase — drives worker state transitions.
    phase: str = PHASE_PRECREATE

    error_message: str = ""

    # --- Helpers ---

    def get_id(self):
        return "%s/%s" % (self.cluster_id, self.uuid)

    def write_to_db(self, kv_store=None):
        self.updated_at = str(datetime.datetime.now(datetime.timezone.utc))
        super().write_to_db(kv_store)

    def member_count(self) -> int:
        return len(self.members)

    def leader_migration_id(self) -> Optional[str]:
        """migration_id of the member with the lowest ns_id (the master lvol)."""
        if not self.members:
            return None
        return min(self.members, key=lambda m: m['ns_id'])['migration_id']

    def ordered_migration_ids(self) -> List[str]:
        """migration_ids sorted by ns_id ascending — the SPDK batch order."""
        return [m['migration_id'] for m in sorted(self.members, key=lambda m: m['ns_id'])]
