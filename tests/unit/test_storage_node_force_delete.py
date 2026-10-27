"""A storage node deleted as part of bringing its cluster down. Its SPDK is
killed whatever the node's status, bypassing the shutdown's guards for an
active restart or a running migration; the node is marked removed first so
no restart driver brings SPDK back, and its tasks are cancelled before the
kill. A kill that cannot be confirmed is reported, and the node is deleted
anyway: the cluster is going."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import storage_node_ops
from simplyblock_core.models.storage_node import StorageNode


def _db(status):
    node = SimpleNamespace(uuid="n1", status=status, cluster_id="c1")
    node.get_id = lambda: "n1"
    db = MagicMock()
    db.get_storage_node_by_id.return_value = node
    return db


@pytest.mark.parametrize("status", [
    StorageNode.STATUS_ONLINE, StorageNode.STATUS_PENDING_REMOVAL, StorageNode.STATUS_RESTARTING,
    StorageNode.STATUS_IN_SHUTDOWN, StorageNode.STATUS_SUSPENDED, StorageNode.STATUS_OFFLINE,
])
def test_spdk_is_killed_whatever_the_status_after_the_node_is_fenced(status):
    order = []
    task = SimpleNamespace(uuid="t1")
    with patch.object(storage_node_ops, "DBController", return_value=_db(status)), \
            patch.object(storage_node_ops, "set_node_status",
                         side_effect=lambda nid, st, caused_by="": order.append(("status", st))), \
            patch.object(storage_node_ops.tasks_controller, "get_active_node_tasks", return_value=[task]), \
            patch.object(storage_node_ops.tasks_controller, "cancel_task",
                         side_effect=lambda uuid: order.append(("cancel", uuid))), \
            patch.object(storage_node_ops, "_kill_spdk_until_dead",
                         side_effect=lambda node: order.append(("kill", node.get_id())) or True), \
            patch.object(storage_node_ops, "delete_storage_node",
                         side_effect=lambda nid, force=False: order.append(("delete", force))):
        stopped = storage_node_ops.force_delete_storage_node("n1")

    assert stopped is True
    assert order == [("status", StorageNode.STATUS_REMOVED), ("cancel", "t1"), ("kill", "n1"), ("delete", True)]


@pytest.mark.parametrize("kill", [MagicMock(return_value=False), MagicMock(side_effect=RuntimeError("unreachable"))])
def test_an_unconfirmed_kill_is_reported_and_the_node_still_deleted(kill, caplog):
    delete = MagicMock()
    with patch.object(storage_node_ops, "DBController", return_value=_db(StorageNode.STATUS_ONLINE)), \
            patch.object(storage_node_ops, "set_node_status"), \
            patch.object(storage_node_ops.tasks_controller, "get_active_node_tasks", return_value=[]), \
            patch.object(storage_node_ops, "_kill_spdk_until_dead", kill), \
            patch.object(storage_node_ops, "delete_storage_node", delete):
        stopped = storage_node_ops.force_delete_storage_node("n1")

    assert stopped is False
    delete.assert_called_once_with("n1", force=True)
    assert any("n1" in r.getMessage() and r.levelname == "ERROR" for r in caplog.records)
