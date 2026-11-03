"""The parts of controllers/port_release_budget.py that decide, without the
DB, which lvstore a port belongs to and which path of it is never left
fenced (review of #1580: a per-(node, port) budget let the loop fence all
three paths of the lvstore and left every volume on it without a route).
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import port_release_budget as budget
from simplyblock_core.models.storage_node import StorageNode


def _node(node_id, lvstore, own_port, followed=None, status=StorageNode.STATUS_ONLINE,
          secondary="", tertiary=""):
    node = StorageNode()
    node.uuid = node_id
    node.cluster_id = "cl-1"
    node.lvstore = lvstore
    node.status = status
    node.secondary_node_id = secondary
    node.tertiary_node_id = tertiary
    node.lvstore_ports = {lvstore: {"lvol_subsys_port": own_port, "hublvol_port": own_port - 80}}
    for name, port in (followed or {}).items():
        node.lvstore_ports[name] = {"lvol_subsys_port": port, "hublvol_port": port - 80}
    return node


def _ams1():
    """LVS_4033: primary cm-20, secondary cm-17, tertiary cm-18 (sn list 2026-11-01)."""
    cm20 = _node("d3a07483", "LVS_4033", 9113, {"LVS_2898": 9112, "LVS_1908": 9111},
                 secondary="428ae3c4", tertiary="438a0878")
    cm17 = _node("428ae3c4", "LVS_8071", 9114, {"LVS_4033": 9113, "LVS_2898": 9112})
    cm18 = _node("438a0878", "LVS_5685", 9110, {"LVS_8071": 9114, "LVS_4033": 9113})
    return cm20, cm17, cm18


class TestLvstoreOfPort(unittest.TestCase):

    def test_the_own_port_maps_to_the_own_lvstore(self):
        cm20, _, _ = _ams1()
        self.assertEqual(budget.lvstore_of_port(cm20, 9113), "LVS_4033")

    def test_a_followed_port_maps_to_the_followed_lvstore(self):
        _, cm17, cm18 = _ams1()
        self.assertEqual(budget.lvstore_of_port(cm17, 9113), "LVS_4033")
        self.assertEqual(budget.lvstore_of_port(cm18, 9114), "LVS_8071")

    def test_an_unknown_port_has_no_lvstore(self):
        cm20, _, _ = _ams1()
        self.assertIsNone(budget.lvstore_of_port(cm20, 4430))


class TestPreferredPath(unittest.TestCase):

    def test_the_online_primary_is_preferred(self):
        cm20, cm17, cm18 = _ams1()
        self.assertIs(budget.preferred_path("LVS_4033", [cm18, cm17, cm20]), cm20)

    def test_the_secondary_takes_over_while_the_primary_is_down(self):
        """The incident: cm-20 rebooting, cm-17 and cm-18 looping."""
        cm20, cm17, cm18 = _ams1()
        cm20.status = StorageNode.STATUS_DOWN
        self.assertIs(budget.preferred_path("LVS_4033", [cm20, cm17, cm18]), cm17)

    def test_then_the_tertiary(self):
        cm20, cm17, cm18 = _ams1()
        cm20.status = StorageNode.STATUS_OFFLINE
        cm17.status = StorageNode.STATUS_UNREACHABLE
        self.assertIs(budget.preferred_path("LVS_4033", [cm20, cm17, cm18]), cm18)

    def test_no_online_path_means_no_exemption(self):
        cm20, cm17, cm18 = _ams1()
        for n in (cm20, cm17, cm18):
            n.status = StorageNode.STATUS_DOWN
        self.assertIsNone(budget.preferred_path("LVS_4033", [cm20, cm17, cm18]))

    def test_an_lvstore_without_a_primary_has_no_preferred_path(self):
        self.assertIsNone(budget.preferred_path("LVS_9999", list(_ams1())))


class TestTheGate(unittest.TestCase):
    """``exhausted`` with the ledger and the node list patched: the DB-backed
    pieces are one range read and one compare-and-set, covered by the
    integration tier."""

    def _exhausted(self, node, port, nodes, ledger_exhausted):
        ledger = MagicMock()
        ledger.exhausted.return_value = ledger_exhausted
        db = MagicMock()
        db.get_storage_nodes_by_cluster_id.return_value = nodes
        with patch.object(budget, "load", return_value=ledger) as load:
            result = budget.exhausted(db, node, port, now=1000.0)
        self.load_calls = load.call_args_list
        return result

    def test_the_preferred_path_is_never_refused(self):
        cm20, cm17, cm18 = _ams1()
        self.assertFalse(self._exhausted(cm20, 9113, [cm20, cm17, cm18], ledger_exhausted=True))
        self.assertEqual(self.load_calls, [], "the ledger is not even consulted")

    def test_a_follower_path_is_refused_once_the_lvstore_budget_is_gone(self):
        cm20, cm17, cm18 = _ams1()
        self.assertTrue(self._exhausted(cm17, 9113, [cm20, cm17, cm18], ledger_exhausted=True))
        (db, cluster_id, lvstore), _ = self.load_calls[0]
        self.assertEqual((cluster_id, lvstore), ("cl-1", "LVS_4033"))

    def test_a_follower_path_with_budget_is_released(self):
        cm20, cm17, cm18 = _ams1()
        self.assertFalse(self._exhausted(cm17, 9113, [cm20, cm17, cm18], ledger_exhausted=False))

    def test_the_primary_down_moves_the_exemption_to_the_secondary(self):
        cm20, cm17, cm18 = _ams1()
        cm20.status = StorageNode.STATUS_DOWN
        self.assertFalse(self._exhausted(cm17, 9113, [cm20, cm17, cm18], ledger_exhausted=True))
        self.assertTrue(self._exhausted(cm18, 9113, [cm20, cm17, cm18], ledger_exhausted=True))

    def test_a_port_of_no_known_lvstore_is_not_gated(self):
        cm20, cm17, cm18 = _ams1()
        self.assertFalse(self._exhausted(cm20, 4430, [cm20, cm17, cm18], ledger_exhausted=True))


class TestForgetNode(unittest.TestCase):

    def test_a_restart_drops_the_node_from_every_lvstore_it_serves(self):
        cm20, _, _ = _ams1()
        ledgers = {}

        def _load(db, cluster_id, lvstore):
            ledger = MagicMock()
            ledger.releases = {"d3a07483": [1.0], "428ae3c4": [2.0]}
            ledgers[lvstore] = ledger
            return ledger

        stored = []
        with patch.object(budget, "load", side_effect=_load), \
             patch.object(budget, "_store", side_effect=lambda db, ledger, mutate: stored.append((ledger, mutate))):
            budget.forget_node(MagicMock(), cm20)
        self.assertEqual(set(ledgers), {"LVS_4033", "LVS_2898", "LVS_1908"})
        self.assertEqual(len(stored), 3)
        ledger, mutate = stored[0]
        mutate(ledger)
        ledger.forget.assert_called_once_with("d3a07483")

    def test_a_node_with_no_stamps_writes_nothing(self):
        cm20, _, _ = _ams1()
        ledger = MagicMock()
        ledger.releases = {"428ae3c4": [2.0]}
        with patch.object(budget, "load", return_value=ledger), \
             patch.object(budget, "_store") as store:
            budget.forget_node(MagicMock(), cm20)
        store.assert_not_called()


if __name__ == "__main__":
    unittest.main()
