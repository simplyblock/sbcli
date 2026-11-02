"""A forced shutdown marks the node's devices unavailable on the peers too.

It skipped that step (Loop 1) and left the peers to "discover the loss
through TCP errors". But the device errors the peers report are skipped while
the node is in_restart, so after `sn shutdown --force` + `sn restart` the
peers kept routing writes to the dead devices until the restart brought them
back. A leader whose writes fail for longer than the lvol layer waits (6s)
drops leadership and returns the queued IO as EIO: 2026-10-29 page copy soak,
iteration 21, the LVS_1 leader on a peer failed client writes after 10s.

Graceful shutdown keeps Loop 1 before the kill; forced runs it right after
the kill, never before (that would be a graceful shutdown).
"""

from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import storage_node_ops
from simplyblock_core.models.storage_node import StorageNode


def _run_shutdown(force, kill_ok=True):
    order = []
    node = MagicMock(spec=StorageNode)
    node.status = StorageNode.STATUS_ONLINE
    node.hostname = "h1"
    node.cluster_id = "c1"
    node.rpc_port = 8080
    node.nvme_devices = []
    node.get_id = MagicMock(return_value="n1")
    client = MagicMock()

    def _kill(*_a, **_kw):
        order.append("kill")
        if not kill_ok:
            raise storage_node_ops.SNodeClientException("kill failed")
    client.spdk_process_kill.side_effect = _kill
    node.client = MagicMock(return_value=client)

    db = MagicMock()
    db.get_storage_node_by_id.return_value = node
    db.get_cluster_by_id.return_value = MagicMock(status="active")
    db.get_storage_nodes_by_cluster_id.return_value = [node]

    tasks = MagicMock()
    tasks.get_active_lvol_migration.return_value = False
    with patch.object(storage_node_ops, "DBController", return_value=db), \
            patch.object(storage_node_ops, "tasks_controller", tasks), \
            patch.object(storage_node_ops, "check_node_shutdown_preconditions",
                         return_value=(True, "")), \
            patch.object(storage_node_ops, "_remember_cluster_arbitration"), \
            patch.object(storage_node_ops, "set_node_status", return_value=True), \
            patch.object(storage_node_ops, "_note_planned_loss"), \
            patch.object(storage_node_ops, "_detach_remote_controllers_from_peers",
                         side_effect=lambda *a: order.append("detach") or 0), \
            patch.object(storage_node_ops, "trigger_ana_failover_for_node"), \
            patch.object(storage_node_ops, "_broadcast_devices_unavailable",
                         side_effect=lambda *a: order.append("unavailable")):
        res = storage_node_ops.shutdown_storage_node("n1", force=force)
    return res, order


def test_forced_shutdown_marks_devices_unavailable_after_the_kill():
    res, order = _run_shutdown(force=True)
    assert res is True
    assert order == ["kill", "unavailable"]


def test_graceful_shutdown_still_marks_devices_unavailable_before_the_kill():
    res, order = _run_shutdown(force=False)
    assert res is True
    assert order == ["unavailable", "detach", "kill"]


def test_failed_forced_kill_does_not_mark_devices_unavailable():
    """SPDK may still be serving: its devices are not gone."""
    res, order = _run_shutdown(force=True, kill_ok=False)
    assert res is False
    assert order == ["kill"]


@pytest.mark.parametrize("status,expected", [
    ("online", True), ("unavailable", True), ("cannot_allocate", True),
    ("read_only", True), ("removed", False), ("failed", False),
])
def test_broadcast_marks_only_serving_devices(status, expected):
    from simplyblock_core.models.nvme_device import NVMeDevice
    dev = MagicMock()
    dev.status = {"online": NVMeDevice.STATUS_ONLINE,
                  "unavailable": NVMeDevice.STATUS_UNAVAILABLE,
                  "cannot_allocate": NVMeDevice.STATUS_CANNOT_ALLOCATE,
                  "read_only": NVMeDevice.STATUS_READONLY,
                  "removed": NVMeDevice.STATUS_REMOVED,
                  "failed": NVMeDevice.STATUS_FAILED}[status]
    dev.get_id = MagicMock(return_value="d1")
    node = MagicMock(spec=StorageNode)
    node.jm_device = None
    node.nvme_devices = [dev]
    dc = MagicMock()
    with patch.object(storage_node_ops, "device_controller", dc):
        storage_node_ops._broadcast_devices_unavailable(node)
    assert dc.device_set_unavailable.called is expected
