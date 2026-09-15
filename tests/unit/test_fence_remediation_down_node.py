"""A node fenced by SPDK must be able to get un-fenced (k8s, 2026-09-15).

SPDK blocks an lvstore's client port on a writer conflict or a leadership
change and never releases it; lifting it is the control plane's job, and
_remediate_stale_port_blocks is where that happens. It required the node to be
ONLINE -- which excluded the one case it exists for.

A fence on the node's OWN lvstore port is not advisory, so node_port_check_fun
returns False on the next ~6s tick and set_node_down flips the node DOWN, well
before the 12s the fence needs to age into "stale". The node is then never
ONLINE on the tick that crosses the threshold, so the remediation skips it for
ever; and the DOWN->ONLINE clear at the end of the node check cannot fire
because it needs the port check to pass. Both sides wait for the other.

Live: an lvol migration on 2f59f60f hit a device on the node being removed, the
failed IO demoted LVS_16 and fenced port 4442 at 17:23:58, the node went DOWN at
17:24:05, and 70 minutes later SPDK was still up, the pod had 0 restarts, every
other probe passed, and the port was still blocked. The peer b30f8f0c was
rescued by this same function at 17:24:28 -- its copy of 4442 is advisory, so it
stayed ONLINE long enough to qualify. Only the owner could not.
"""
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import storage_node_monitor
from simplyblock_core.utils import port_block


OWN_PORT = 4442


def _node(node_id, status):
    node = MagicMock()
    node.get_id.return_value = node_id
    node.status = status
    node.lvstore = "LVS_16"
    return node


class _Harness:
    """One fenced port, aged past the threshold, with every other gate open."""

    def __init__(self, status, hublvol_ok=True, restart_owns=False):
        self.node = _node("2f59f60f", status)
        self.db = MagicMock()
        self.db.get_storage_node_by_id.return_value = self.node
        self.hublvol_ok = hublvol_ok
        self.restart_owns = restart_owns
        self.unblocked = []

    def run(self):
        storage_node_monitor._blocked_port_since.clear()
        hc = storage_node_monitor.health_controller
        with patch.object(hc, "_restart_owns_lvs", return_value=self.restart_owns), \
             patch.object(hc, "_check_node_hublvol", return_value=self.hublvol_ok), \
             patch.object(port_block, "set_port",
                          side_effect=lambda n, p, **kw: self.unblocked.append(p)), \
             patch.object(storage_node_monitor.time, "monotonic",
                          side_effect=[0.0, 100.0]):
            # First reading stamps first_seen, second measures how long it has
            # been held: 100s, long past STALE_PORT_BLOCK_SEC.
            storage_node_monitor._remediate_stale_port_blocks(
                self.db, self.node, {OWN_PORT: False}, {OWN_PORT: "2f59f60f"})
        return self.unblocked


class TestTheDeadlock(unittest.TestCase):

    def test_a_down_node_is_unfenced(self):
        """The regression. DOWN is the status the fence itself produces."""
        self.assertEqual(_Harness(StorageNode.STATUS_DOWN).run(), [OWN_PORT])

    def test_an_online_node_is_still_unfenced(self):
        self.assertEqual(_Harness(StorageNode.STATUS_ONLINE).run(), [OWN_PORT])

    def test_down_is_admitted_by_the_status_set(self):
        self.assertIn(StorageNode.STATUS_DOWN,
                      storage_node_monitor._REMEDIABLE_FENCE_STATUSES)
        self.assertIn(StorageNode.STATUS_ONLINE,
                      storage_node_monitor._REMEDIABLE_FENCE_STATUSES)


class TestTheOtherGatesStillHold(unittest.TestCase):
    """Admitting DOWN must not weaken anything else. These are the gates that
    keep the remediation off a deliberate fence."""

    def test_a_restart_owning_the_lvs_is_not_raced(self):
        self.assertEqual(
            _Harness(StorageNode.STATUS_DOWN, restart_owns=True).run(), [],
            "the restart flow is the legitimate author of port blocks")

    def test_an_unhealthy_hublvol_still_refuses(self):
        self.assertEqual(
            _Harness(StorageNode.STATUS_DOWN, hublvol_ok=False).run(), [],
            "unblocking without a redirect re-opens the promote/fence loop")

    def test_a_node_that_is_neither_online_nor_down_is_left_alone(self):
        for status in (StorageNode.STATUS_OFFLINE,
                       StorageNode.STATUS_UNREACHABLE,
                       StorageNode.STATUS_RESTARTING,
                       StorageNode.STATUS_IN_SHUTDOWN,
                       StorageNode.STATUS_SUSPENDED):
            self.assertEqual(_Harness(status).run(), [], status)

    def test_a_fence_younger_than_the_threshold_is_left_alone(self):
        h = _Harness(StorageNode.STATUS_DOWN)
        storage_node_monitor._blocked_port_since.clear()
        hc = storage_node_monitor.health_controller
        with patch.object(hc, "_restart_owns_lvs", return_value=False), \
             patch.object(hc, "_check_node_hublvol", return_value=True), \
             patch.object(port_block, "set_port",
                          side_effect=lambda n, p, **kw: h.unblocked.append(p)), \
             patch.object(storage_node_monitor.time, "monotonic",
                          side_effect=[0.0, 1.0]):
            storage_node_monitor._remediate_stale_port_blocks(
                h.db, h.node, {OWN_PORT: False}, {OWN_PORT: "2f59f60f"})
        self.assertEqual(h.unblocked, [],
                         "1s in, this is an in-flight fence, not a leak")

    def test_an_open_port_clears_its_timer(self):
        storage_node_monitor._blocked_port_since.clear()
        node = _node("2f59f60f", StorageNode.STATUS_DOWN)
        storage_node_monitor._blocked_port_since[(node.get_id(), OWN_PORT)] = 0.0
        storage_node_monitor._remediate_stale_port_blocks(
            MagicMock(), node, {OWN_PORT: True}, {OWN_PORT: "2f59f60f"})
        self.assertNotIn((node.get_id(), OWN_PORT),
                         storage_node_monitor._blocked_port_since)


if __name__ == "__main__":
    unittest.main()
