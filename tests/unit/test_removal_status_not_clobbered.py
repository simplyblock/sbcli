"""A removal status must survive the shutdown's final OFFLINE write.

The operator's ShuttingDown step starts a shutdown that runs in the
background and ends by writing OFFLINE. When the drain stamped
MIGRATING_DEVICES while that shutdown was still running, the OFFLINE write
undid the stamp, the rebuild of the node's own distribs was queued on the
node itself, and the removal waited for ever (2026-09-30, runs 19 and 24).
"""

from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import storage_node_ops
from simplyblock_core.models.storage_node import StorageNode


def _set_status(pre_status, new_status):
    node = StorageNode()
    node.uuid = "n1"
    node.status = pre_status
    db = MagicMock()
    db.get_storage_node_by_id.return_value = node

    def atomic_update(obj, mutate):
        mutate(node)
        return node
    db.atomic_update.side_effect = atomic_update
    with patch.object(storage_node_ops, "DBController", return_value=db), \
            patch.object(storage_node_ops, "storage_events") as events, \
            patch.object(storage_node_ops, "distr_controller"), \
            patch("simplyblock_core.controllers.tasks_controller.get_active_node_restart_task",
                  return_value=None, create=True):
        ok = storage_node_ops.set_node_status("n1", new_status)
    return ok, node, events


@pytest.mark.parametrize("status", StorageNode.REMOVAL_SHUT_DOWN_STATUSES)
def test_offline_does_not_undo_a_removal_status(status):
    ok, node, events = _set_status(status, StorageNode.STATUS_OFFLINE)
    assert ok is False
    assert node.status == status
    events.snode_status_change.assert_not_called()


def test_offline_from_online_still_applies():
    ok, node, _ = _set_status(StorageNode.STATUS_ONLINE, StorageNode.STATUS_OFFLINE)
    assert ok is True
    assert node.status == StorageNode.STATUS_OFFLINE


def test_pending_removal_can_still_go_offline():
    """PENDING_REMOVAL is departing but still running; it is not a shut-down
    status, so a real outage must still be recorded."""
    ok, node, _ = _set_status(StorageNode.STATUS_PENDING_REMOVAL, StorageNode.STATUS_OFFLINE)
    assert ok is True
    assert node.status == StorageNode.STATUS_OFFLINE
