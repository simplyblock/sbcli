# coding=utf-8
"""
manual_delete_controller.py — TEMPORARY debug tool (branch: delete-test).

Fires the same two-phase blocking bdev delete production uses (see
``tasks_runner_lvol_migration._delete_bdev_blocking``) against a manually
given ordered structure of lvols and snapshots, then removes the DB record
directly — no monitor, no task queue, fully synchronous.

This bypasses lvol_controller/snapshot_controller's object locks and
precondition checks entirely. It exists to fuzz-test arbitrary delete
orderings across an lvol + its snapshots outside the task-runner. Never call
this outside of manual/automated debugging on this branch.
"""
import time

from simplyblock_core import utils
from simplyblock_core.db_controller import DBController
from simplyblock_core.storage_node_ops import execute_on_leader_with_failover

logger = utils.get_logger(__name__)

VALID_KINDS = ("lvol", "snapshot")


def _resolve_bdev(db_controller, kind, obj_id):
    """Returns (bdev_name, lvs_name, host_node_id, obj) for a DB-tracked lvol/snapshot."""
    if kind == "lvol":
        lvol = db_controller.get_lvol_by_id(obj_id)
        return f"{lvol.lvs_name}/{lvol.lvol_bdev}", lvol.lvs_name, lvol.node_id, lvol
    if kind == "snapshot":
        snap = db_controller.get_snapshot_by_id(obj_id)
        return snap.snap_bdev, snap.lvol.lvs_name, snap.lvol.node_id, snap
    raise ValueError(f"Unknown kind '{kind}', expected one of {VALID_KINDS}")


def _replica_nodes(db_controller, host_node_id):
    """host node + secondary/tertiary (if configured and still resolvable)."""
    host_node = db_controller.get_storage_node_by_id(host_node_id)
    nodes = [host_node]
    if host_node.secondary_node_id:
        try:
            nodes.append(db_controller.get_storage_node_by_id(host_node.secondary_node_id))
        except KeyError:
            pass
    if host_node.tertiary_node_id:
        try:
            nodes.append(db_controller.get_storage_node_by_id(host_node.tertiary_node_id))
        except KeyError:
            pass
    return nodes


def _delete_bdev_blocking(bdev_name, all_nodes, lvs_name, coalescing=False,
                          timeout_s=120, poll_interval_s=0.2):
    """Mirrors tasks_runner_lvol_migration._delete_bdev_blocking exactly:

    Phase 1 — current LVS leader (resolved via execute_on_leader_with_failover,
      with failover if the primary is down), sync=False: initiates the async
      delete.
    Wait   — poll bdev_lvol_get_lvol_delete_status on the leader until done.
    Phase 2 — every replica (primary + secondary + tertiary), sync=True,
      special_delete=False: finalizes the deletion everywhere.

    Returns the leader node's ID.
    """
    def _async_delete(leader):
        ret, _ = leader.rpc_client().delete_lvol(
            bdev_name, sync=False, special_delete=not coalescing)
        return ret or False

    ok, leader_node, err = execute_on_leader_with_failover(all_nodes, lvs_name, _async_delete)
    if not ok or leader_node is None:
        raise RuntimeError(f"delete bdev {bdev_name}: initiation failed ({err})")

    leader_rpc = leader_node.rpc_client()
    deadline = time.monotonic() + timeout_s
    while leader_rpc.bdev_lvol_get_lvol_delete_status(bdev_name) == 1:
        if time.monotonic() > deadline:
            if not leader_rpc.get_bdevs(bdev_name):
                logger.warning(
                    f"[manual-delete] {bdev_name}: poll timed out after {timeout_s}s "
                    f"but bdev is gone — treating as success")
                break
            raise RuntimeError(
                f"delete bdev {bdev_name}: timed out after {timeout_s}s, bdev still present")
        time.sleep(poll_interval_s)

    for node in all_nodes:
        ret, err = node.rpc_client().delete_lvol(bdev_name, sync=True, special_delete=False)
        if not ret:
            raise RuntimeError(f"delete bdev {bdev_name}: sync finalize failed on "
                               f"{node.get_id()} ({err})")

    return leader_node.get_id()


def _cleanup_db_record(db_controller, kind, obj):
    """Removes the DB record cleanly, mirroring the real delete paths'
    final bookkeeping (index maintenance included)."""
    if kind == "lvol":
        db_controller.release_lvol_ns_slot(obj)  # atomically removes base+mini record
    else:
        db_controller.unindex_snapshot(obj)
        obj.remove(db_controller.kv_store)


def delete_structure(entities, coalescing=False, timeout_s=120):
    """Runs the two-phase blocking delete + clean DB removal for each entity
    in the given order, back-to-back with no artificial delay between
    entities — each entity's async-start fires as soon as the previous
    entity's DB cleanup returns.

    entities: ordered list of (kind, id) tuples, kind in {"lvol", "snapshot"}.
    Returns a list of per-entity result dicts, in the same order.
    """
    db_controller = DBController()
    results = []

    for kind, obj_id in entities:
        entry = {"kind": kind, "id": obj_id, "bdev": None, "leader_node_id": None,
                  "ok": False, "error": None}
        try:
            bdev_name, lvs_name, host_node_id, obj = _resolve_bdev(db_controller, kind, obj_id)
            all_nodes = _replica_nodes(db_controller, host_node_id)
        except (KeyError, ValueError) as e:
            entry["error"] = str(e)
            logger.error(f"[manual-delete] cannot resolve {kind} {obj_id}: {e}")
            results.append(entry)
            continue

        entry["bdev"] = bdev_name
        logger.info(f"[manual-delete] {kind} {bdev_name}: starting two-phase delete")
        try:
            entry["leader_node_id"] = _delete_bdev_blocking(
                bdev_name, all_nodes, lvs_name, coalescing=coalescing, timeout_s=timeout_s)
            _cleanup_db_record(db_controller, kind, obj)
            entry["ok"] = True
        except Exception as e:
            entry["error"] = str(e)
            logger.error(f"[manual-delete] FAILED {kind} {bdev_name}: {e}")

        results.append(entry)

    return results
