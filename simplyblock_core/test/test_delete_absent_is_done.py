"""Deleting something that is already gone is success, not failure.

Lab 2026-08-20: snapshots sat in ``in_deletion`` for hours while the monitor
logged ~10,300 "Failed to delete snap from node" in 2.5 hours. Every one was
``-19 / No such device`` for a bdev the async phase had already removed — the
delete-status poll one line earlier returned 0 and the monitor even logged
"Snapshot deleted successfully" before issuing the sync delete that then
"failed".

Reading -19 as a failure makes the delete non-idempotent, so the record is
never finalized, the monitor retries it next cycle, and it never converges.
``sync_delete_on_peer`` had always tolerated this; the leader and primary paths
had not, so the two halves of one delete disagreed about what had happened.
"""
from simplyblock_core.controllers import snapshot_controller


class _RPC:
    def __init__(self, result):
        self._result = result
        self.calls = []

    def delete_lvol(self, bdev_name, sync=False, special_delete=False):
        self.calls.append((bdev_name, sync, special_delete))
        if isinstance(self._result, Exception):
            raise self._result
        return self._result


class _Node:
    def __init__(self, result):
        self._rpc = _RPC(result)

    def get_id(self):
        return "NODE1234"

    def rpc_client(self):
        return self._rpc


def test_successful_delete_is_success():
    node = _Node((True, None))
    assert snapshot_controller.delete_bdev_absent_ok(node, "LVS_1/SNAP_1") is True


def test_already_absent_is_success():
    """The regression: -19 means the desired end state already holds."""
    node = _Node((False, {"code": -19, "message": "No such device"}))
    assert snapshot_controller.delete_bdev_absent_ok(node, "LVS_1/SNAP_1") is True


def test_a_real_error_is_still_a_failure():
    node = _Node((False, {"code": -16, "message": "Device or resource busy"}))
    assert snapshot_controller.delete_bdev_absent_ok(node, "LVS_1/SNAP_1") is False


def test_transport_exception_is_a_failure():
    """An unreachable node is not evidence that the bdev is gone."""
    node = _Node(ConnectionError("connection refused"))
    assert snapshot_controller.delete_bdev_absent_ok(node, "LVS_1/SNAP_1") is False


def test_non_dict_error_is_a_failure():
    node = _Node((False, "something went wrong"))
    assert snapshot_controller.delete_bdev_absent_ok(node, "LVS_1/SNAP_1") is False


def test_flags_are_passed_through():
    node = _Node((True, None))
    snapshot_controller.delete_bdev_absent_ok(
        node, "LVS_1/SNAP_1", sync=True, special_delete=True)
    assert node.rpc_client().calls == [("LVS_1/SNAP_1", True, True)]
