"""A status event must only be addressed to a node that can receive it.

send_node_status_event and send_dev_status_event fan the same kind of event to
the same peers, but stated their recipient rule opposite ways: the node one as
an inclusion list (ONLINE / SUSPENDED / DOWN), the device one as an exclusion
list (not OFFLINE, not REMOVED). The exclusion list named no in-flight removal
status, so every device event was also sent to a node whose SPDK the removal
had already stopped.

That is worse than wasted work. send_dev_status_event returns
`all(results)`, and tasks_runner_port_allow uses it as a positive confirmation
gate before touching hublvols, leadership or the port. The unreachable
"peer" being a node on its way out is indistinguishable from a live peer that
refused the event, so a node recovering while another was being removed parked
for ever on "Local device status for <dev> not applied by all distribs, retry
task" (cluster a6e7569d, 2026-09-15).
"""
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import distr_controller
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode


def _node(node_id, status):
    node = MagicMock()
    node.get_id.return_value = node_id
    node.status = status
    node.mgmt_ip = f"10.0.0.{abs(hash(node_id)) % 200}"
    node.remote_devices = []
    return node


def _recipients_for_device(nodes):
    """Which nodes send_dev_status_event actually addresses."""
    device = MagicMock()
    device.cluster_device_order = 2
    device.get_id.return_value = "dev-1"
    device.cluster_id = "c1"
    device.node_id = "owner"
    sent_to = []
    db = MagicMock()
    db.get_storage_nodes_by_cluster_id.return_value = nodes
    db.get_storage_node_by_id.side_effect = lambda nid: _node(nid, StorageNode.STATUS_ONLINE)

    def _fake_send(node, events, result=None):
        sent_to.append(node.get_id())
        if result is not None:
            result["sent"] = True
        return True

    with patch.object(distr_controller, "DBController", return_value=db), \
         patch.object(distr_controller, "_send_event_to_node", side_effect=_fake_send), \
         patch.object(distr_controller, "_persist_target_device_event"):
        distr_controller.send_dev_status_event(device, NVMeDevice.STATUS_FAILED_AND_MIGRATED)
    return sent_to


class TestDepartingNodesGetNoEvents(unittest.TestCase):

    def test_no_departing_status_is_addressed(self):
        for status in StorageNode.DEPARTING_STATUSES:
            self.assertEqual(
                _recipients_for_device([_node("leaving", status)]), [],
                f"{status}: its SPDK is stopped, the send can only fail")

    def test_migrating_lvols_specifically(self):
        """The status that wedged tasks_runner_port_allow."""
        self.assertEqual(
            _recipients_for_device(
                [_node("a1b050f1", StorageNode.STATUS_MIGRATING_LVOLS)]), [])

    def test_a_departing_peer_does_not_poison_the_confirmation_gate(self):
        """The whole point: one healthy peer plus one departing node must
        confirm, not fail."""
        nodes = [_node("healthy", StorageNode.STATUS_ONLINE),
                 _node("leaving", StorageNode.STATUS_MIGRATING_LVOLS)]
        self.assertEqual(_recipients_for_device(nodes), ["healthy"])


class TestRealRecipientsStillGetEvents(unittest.TestCase):

    def test_online_suspended_and_down_all_receive(self):
        for status in (StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED,
                       StorageNode.STATUS_DOWN):
            self.assertEqual(_recipients_for_device([_node("n1", status)]),
                             ["n1"], status)

    def test_unreachable_nodes_are_not_addressed(self):
        for status in (StorageNode.STATUS_OFFLINE,
                       StorageNode.STATUS_UNREACHABLE,
                       StorageNode.STATUS_IN_CREATION):
            self.assertEqual(_recipients_for_device([_node("n1", status)]),
                             [], status)


class TestTheTwoFanoutsAgree(unittest.TestCase):

    def test_one_recipient_rule(self):
        import inspect
        for fn in (distr_controller.send_node_status_event,
                   distr_controller.send_dev_status_event):
            self.assertIn("_EVENT_RECIPIENT_STATUSES", inspect.getsource(fn),
                          f"{fn.__name__} must share the recipient rule")

    def test_down_is_a_recipient(self):
        """Only its client LVS port is fenced; SPDK and its RPC are alive."""
        self.assertIn(StorageNode.STATUS_DOWN,
                      distr_controller._EVENT_RECIPIENT_STATUSES)


if __name__ == "__main__":
    unittest.main()
