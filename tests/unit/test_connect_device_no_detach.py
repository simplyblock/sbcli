# coding=utf-8
"""connect_device must never detach a controller.

The old code issued ``bdev_nvme_detach_controller`` when it observed a
controller in state ``failed``. That raced the data plane's own
destruct/reconnect machinery: the state string reports ``is_failed``
BEFORE ``resetting``/``reconnect_is_delayed`` (nvme_ctrlr_get_state_str),
so a controller mid reset/reconnect cycle transiently reads ``failed`` in
the window between a reset failure and its disposition — and with the
cluster-wide bdev_nvme options (reconnect_delay_sec=1,
ctrlr_loss_timeout_sec=1, applied at node bring-up before any attach) the
module self-resolves every failure to ``enabled`` or destruct within ~1s.
A persistently parked ``failed`` controller (only possible with
reconnect_delay=0) cannot arise. The detach therefore only ever fired on
transients, and — followed by a fixed sleep with no detach-and-wait-gone
— set up the attach-during-destroy race ("cntlid N are duplicated"
class) on the immediate re-attach.

New contract: ``failed`` is waited out like the other intermediate states
(``resetting``/``deleting``/``reconnect_is_delayed``); if it persists past
the poll budget, connect_device raises so the calling task suspends and
retries. No detach, ever.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import storage_node_ops


def _make_rpc(controller_states):
    """RPC mock whose bdev_nvme_controller_list walks ``controller_states``
    (a list per call; [] = controller gone) and sticks on the last entry."""
    rpc = MagicMock(name="rpc")
    seq = list(controller_states)

    def _list(name):
        state = seq.pop(0) if len(seq) > 1 else seq[0]
        if state is None:
            return []
        return [{"ctrlrs": [{"state": state}]}]

    rpc.bdev_nvme_controller_list.side_effect = _list
    rpc.bdev_nvme_attach_controller.return_value = ["ctrl-bdev"]
    rpc.get_bdevs.return_value = [{"name": "ctrl-bdev"}]
    return rpc


class TestConnectDeviceNoDetach(unittest.TestCase):

    def setUp(self):
        self.device = MagicMock(name="device")
        self.device.nvmf_ip = "10.0.0.5"
        self.device.nvmf_multipath = False
        self.device.nvmf_nqn = "nqn.test:dev"
        self.device.nvmf_port = 4420
        self.device.node_id = "owner-node"
        self.device.is_connection_in_progress_to_node.return_value = False

        self.node = MagicMock(name="node")

        self.target_node = MagicMock(name="target_node")
        self.target_node.active_rdma = False
        self.target_node.active_tcp = True

        self._patches = [
            patch.object(storage_node_ops, "DBController"),
            patch.object(storage_node_ops.time, "sleep", return_value=None),
        ]
        mock_db_cls = self._patches[0].start()
        mock_db_cls.return_value.get_storage_node_by_id.return_value = self.target_node
        self._patches[1].start()
        for p in self._patches:
            self.addCleanup(p.stop)

    def _connect(self, rpc):
        self.node.rpc_client.return_value = rpc
        return storage_node_ops.connect_device(
            "remote_jm_x", self.device, self.node, bdev_names=[], reattach=True)

    def test_transient_failed_waits_then_attaches_without_detach(self):
        # failed -> failed -> gone (the module destructed it on its own) ->
        # fresh attach. No detach may ever be issued.
        rpc = _make_rpc(["failed", "failed", None, None])
        bdev = self._connect(rpc)
        rpc.bdev_nvme_detach_controller.assert_not_called()
        self.assertEqual(bdev, "ctrl-bdev")
        rpc.bdev_nvme_attach_controller.assert_called()

    def test_transient_failed_recovering_to_enabled_is_reused(self):
        # failed -> enabled (reset succeeded): controller reused, bdev
        # returned via get_bdevs, still no detach and no re-attach.
        rpc = _make_rpc(["failed", "enabled", "enabled"])
        rpc.get_bdevs.return_value = [{"name": "remote_jm_xn1"}]
        bdev = self._connect(rpc)
        rpc.bdev_nvme_detach_controller.assert_not_called()
        self.assertEqual(bdev, "remote_jm_xn1")

    def test_persistent_failed_raises_instead_of_detaching(self):
        rpc = _make_rpc(["failed"])
        with self.assertRaises(RuntimeError):
            self._connect(rpc)
        rpc.bdev_nvme_detach_controller.assert_not_called()

    def test_source_has_no_detach_in_connect_device(self):
        import inspect
        src = inspect.getsource(storage_node_ops.connect_device)
        self.assertNotIn(
            "bdev_nvme_detach_controller", src,
            "connect_device must never detach — a 'failed' state here is a "
            "transient of the module's own destruct/reconnect cycle, and a "
            "CP detach + immediate re-attach races it (cntlid duplicated)")


if __name__ == "__main__":
    unittest.main()
