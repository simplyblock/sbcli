"""
migration_bdev_ops.py -- shared bdev-delete primitive for migration code.

``delete_bdev_blocking`` used to live in tasks_runner_lvol_migration.py, with
migration_controller.py pulling it in via a local (function-scoped) import at
every call site. That local-import dance is required there because
tasks_runner_lvol_migration.py imports migration_controller at module level
(``from simplyblock_core.controllers import (migration_controller, ...)``),
so the reverse import at module level would be a real cycle:
tasks_runner_lvol_migration -> migration_controller -> tasks_runner_lvol_migration.

Living in its own module sidesteps that: nothing importing this module needs
to worry about the direction of the migration_controller <-> task-runner
relationship, since this module doesn't import either of them. All three
migration files (migration_controller.py, tasks_runner_lvol_migration.py,
tasks_runner_batch_migration.py) import it directly at module level.
"""

import logging
import time

from tenacity import RetryError, Retrying, before_sleep_log, stop_after_attempt, wait_fixed

from simplyblock_core import utils

logger = utils.get_logger(__name__)


def delete_bdev_blocking(bdev_name, primary_rpc, secondary_rpc=None, tertiary_rpc=None,
                         timeout_s=120, coalescing=False, all_nodes=None, lvs_name=None):
    """
    Two-phase blocking bdev delete.

    Phase 1 — leader node, sync=False: initiates the async delete.  When
      all_nodes + lvs_name are supplied, the actual LVS leader is resolved via
      execute_on_leader_with_failover so the delete goes to the secondary or
      tertiary if the primary is down.  Without them the call falls back to
      primary_rpc directly (original behaviour).  By default (coalescing=False)
      special_delete=True tells SPDK to free the bdev's own clusters without
      merging them into any child — correct for source cleanup, rollback, and
      any path where no child needs to inherit data.  Pass coalescing=True when
      the bdev's child must inherit its clusters (e.g. deleting a migration
      intermediate snapshot).
    Wait   — poll bdev_lvol_get_lvol_delete_status on the leader until done.
    Phase 2 — all nodes (primary + secondary + tertiary), sync=True
      (sync=True, special_delete=False): finalises the deletion on every replica.
    """
    if all_nodes and lvs_name:
        # Local import: storage_node_ops transitively imports migration_controller
        # (storage_node_ops -> snapshot_controller -> migration_controller), so
        # importing it at this module's top level would recreate the exact cycle
        # this module exists to avoid, just one hop further out.
        from simplyblock_core.storage_node_ops import execute_on_leader_with_failover

        def _async_delete(leader):
            ret, _ = leader.rpc_client().delete_lvol(
                bdev_name, sync=False, special_delete=not coalescing)
            return ret or False
        ok, leader_node, _ = execute_on_leader_with_failover(all_nodes, lvs_name, _async_delete)
        if not ok or leader_node is None:
            raise RuntimeError(f"delete bdev {bdev_name}: initiation failed")
        leader_rpc = leader_node.rpc_client()
    else:
        ret, _ = primary_rpc.delete_lvol(bdev_name, sync=False, special_delete=not coalescing)
        if not ret:
            raise RuntimeError(f"delete bdev {bdev_name}: initiation failed")
        leader_rpc = primary_rpc

    deadline = time.monotonic() + timeout_s
    while leader_rpc.bdev_lvol_get_lvol_delete_status(bdev_name) == 1:
        if time.monotonic() > deadline:
            if not leader_rpc.get_bdevs(bdev_name):
                logger.warning(
                    f"delete bdev {bdev_name}: poll timed out after {timeout_s}s "
                    f"but bdev is gone — treating as success")
                break
            raise RuntimeError(
                f"delete bdev {bdev_name}: timed out after {timeout_s}s, bdev still present")
        time.sleep(0.2)

    for rpc in filter(None, [primary_rpc, secondary_rpc, tertiary_rpc]):
        try:
            Retrying(
                stop=stop_after_attempt(3),
                wait=wait_fixed(1),
                before_sleep=before_sleep_log(logger, logging.WARNING),
            )(rpc.delete_lvol, bdev_name, sync=True, special_delete=False)
        except RetryError:
            logger.exception(
                f"delete bdev {bdev_name} sync finalize STILL failing after 3 attempts "
                f"(non-fatal, blob metadata may not be cleared on this replica)"
            )
