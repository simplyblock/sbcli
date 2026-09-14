"""test_repair_gating.py — when repairs may run, and that hublvol path repair runs at all.

Two defects found during the multipath soak on 2026-08-20, both about repairs
firing at the wrong time rather than doing the wrong thing.

1. Hublvol path repair was unreachable. The caller only escalated to auto_fix
   after the coarse existence check FAILED, but a hublvol holding 1 of 2 paths
   passes that check (its controller exists, its bdev is fine). The repair
   logged nothing in 4 hours while hublvols sat single-pathed for 5-11 minutes,
   redundancy returning only when unrelated restart churn happened to touch the
   lvstore. Path repair now runs on its own flag, every cycle.

2. Repairs dialled out to nodes that could not answer. 1372 device repairs in
   four hours, 478 replying "-5 Input/output error", every burst inside a
   NIC-down window. Dial-out is now gated on ONLINE/DOWN, separately from the
   wider test used to judge whether a missing connection counts as a fault.
"""

import unittest
from typing import ClassVar
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import health_controller
from simplyblock_core.models.storage_node import StorageNode


class TestRepairsAllowed(unittest.TestCase):

    def _node(self, status):
        node = MagicMock()
        node.status = status
        return node

    def test_online_and_down_may_be_repaired(self):
        for status in (StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN):
            with self.subTest(status=status):
                self.assertTrue(
                    health_controller.repairs_allowed(self._node(status)))

    def test_everything_else_is_refused(self):
        for status in (StorageNode.STATUS_UNREACHABLE,
                       StorageNode.STATUS_OFFLINE,
                       StorageNode.STATUS_RESTARTING,
                       StorageNode.STATUS_SUSPENDED,
                       StorageNode.STATUS_REMOVED,
                       StorageNode.STATUS_IN_CREATION):
            with self.subTest(status=status):
                self.assertFalse(
                    health_controller.repairs_allowed(self._node(status)))

    def test_missing_node_is_refused(self):
        self.assertFalse(health_controller.repairs_allowed(None))


class TestHublvolPathRepairReachable(unittest.TestCase):
    """One path present, one missing: the coordinator must be asked to fix it."""

    ONE_PATH: ClassVar[list] = [{"ctrlrs": [
        {"state": "enabled",
         "trid": {"traddr": "172.31.96.10"}}]}]
    BOTH_PATHS: ClassVar[list] = [{"ctrlrs": [
        {"state": "enabled", "trid": {"traddr": "172.31.96.10"}},
        {"state": "enabled", "trid": {"traddr": "172.31.97.10"}}]}]

    def _call(self, ctrlr_list, primary_status=StorageNode.STATUS_ONLINE,
              node_status=StorageNode.STATUS_ONLINE, **kwargs):
        def nic(ip):
            iface = MagicMock()
            iface.trtype = "TCP"
            iface.ip4_address = ip
            return iface

        primary = MagicMock()
        primary.status = primary_status
        primary.lvstore_status = "ready"
        primary.active_rdma = False
        primary.active_tcp = True
        primary.data_nics = [nic("172.31.96.10"), nic("172.31.97.10")]
        primary.hublvol.bdev_name = "LVS_1/hublvol"
        primary.get_id.return_value = "primary-1"
        primary.secondary_node_id = None

        node = MagicMock()
        node.status = node_status
        node.get_id.return_value = "sec-1"
        node.lvstore_stack_tertiary = "someone-else"
        node.rpc_client.return_value.bdev_nvme_controller_list.return_value = ctrlr_list

        coordinator_cls = MagicMock()
        with patch.object(health_controller, "DBController") as db, \
                patch.object(health_controller, "_restart_owns_lvs",
                             return_value=False), \
                patch("simplyblock_core.utils.hublvol_reconnect."
                      "HublvolReconnectCoordinator", coordinator_cls):
            db.return_value.get_storage_node_by_id.return_value = primary
            health_controller._check_sec_node_hublvol(
                node, primary_node_id="primary-1", **kwargs)
        # The repair is driven by connect_to_hublvol now, not by the
        # coordinator directly: reconcile() only re-attaches the
        # transport and leaves the lvstore unconnected (2026-09-01,
        # LVS_10). The question these tests ask -- "was the repair
        # driven?" -- is unchanged; only the call that answers it is.
        # Either step means the repair was driven; reconcile() is the one
        # that dials out to add paths, so gate on it.
        return coordinator_cls.return_value.reconcile.called

    def test_repair_paths_reconciles_the_missing_path(self):
        self.assertTrue(self._call(self.ONE_PATH, repair_paths=True))

    def test_no_repair_without_the_flag(self):
        """Default stays read-only, so ordinary checks never dial out."""
        self.assertFalse(self._call(self.ONE_PATH))

    def test_complete_controller_is_left_alone(self):
        self.assertFalse(self._call(self.BOTH_PATHS, repair_paths=True))

    def test_tertiary_missing_a_secondary_path_is_repaired(self):
        """A tertiary connects to primary AND secondary. Three paths (primary
        complete, secondary half-attached) must count as a missing path — on
        the 2026-08-24 deploy both tertiaries sat at 3/4 forever because
        expected_ips was built from the primary alone."""
        from simplyblock_core.controllers import health_controller as hc
        from unittest.mock import MagicMock, patch

        def nic(ip):
            iface = MagicMock()
            iface.trtype = "TCP"
            iface.ip4_address = ip
            return iface

        primary = MagicMock()
        primary.status = StorageNode.STATUS_ONLINE
        primary.lvstore_status = "ready"
        primary.active_rdma = False
        primary.active_tcp = True
        primary.data_nics = [nic("10.0.0.1"), nic("10.0.1.1")]
        primary.hublvol.bdev_name = "LVS_1/hublvol"
        primary.get_id.return_value = "primary-1"
        primary.secondary_node_id = "sec-1"

        secondary = MagicMock()
        secondary.status = StorageNode.STATUS_ONLINE
        secondary.active_rdma = False
        secondary.active_tcp = True
        secondary.data_nics = [nic("10.0.0.2"), nic("10.0.1.2")]

        node = MagicMock()
        node.status = StorageNode.STATUS_ONLINE
        node.get_id.return_value = "tert-1"
        # tertiary: primary's both paths + only ONE of the secondary's
        node.rpc_client.return_value.bdev_nvme_controller_list.return_value = [
            {"ctrlrs": [
                {"state": "enabled", "trid": {"traddr": "10.0.0.1"}},
                {"state": "enabled", "trid": {"traddr": "10.0.1.1"}},
                {"state": "enabled", "trid": {"traddr": "10.0.0.2"}}]}]
        node.lvstore_stack_tertiary = "primary-1"    # makes is_sec2 True

        coordinator_cls = MagicMock()
        with patch.object(hc, "DBController") as db,                 patch.object(hc, "_restart_owns_lvs", return_value=False),                 patch("simplyblock_core.utils.hublvol_reconnect."
                      "HublvolReconnectCoordinator", coordinator_cls):
            db.return_value.get_storage_node_by_id.side_effect =                 lambda i: {"primary-1": primary, "sec-1": secondary}[i]
            hc._check_sec_node_hublvol(node, primary_node_id="primary-1",
                                       repair_paths=True)
        self.assertTrue(coordinator_cls.return_value.reconcile.called,
                        "the missing secondary path was not reconciled")
        # A tertiary must be wired to the primary AND the secondary, with its
        # role stamped from topology rather than defaulted.
        kwargs = node.connect_to_hublvol.call_args.kwargs
        self.assertEqual(kwargs.get("role"), "tertiary")
        self.assertIs(kwargs.get("failover_node"), secondary)

    def test_refused_when_the_primary_cannot_answer(self):
        self.assertFalse(self._call(
            self.ONE_PATH, primary_status=StorageNode.STATUS_UNREACHABLE,
            repair_paths=True))

    def test_refused_when_this_node_is_not_online(self):
        self.assertFalse(self._call(
            self.ONE_PATH, node_status=StorageNode.STATUS_RESTARTING,
            repair_paths=True))


if __name__ == "__main__":
    unittest.main()
