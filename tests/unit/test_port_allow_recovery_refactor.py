# coding=utf-8
"""Unit tests for the 2026-07-08 port-allow recovery refactor.

Covers the network-outage recovery fixes:
  * _node_lvs_ports — enumerate ALL of a node's LVS subsystem ports.
  * _reconnect_own_sec_tert_hublvols — OUTBOUND only: skip a leg only if the
    target peer is neither online nor down; no leadership, no inbound work.
  * _reconnect_inbound_hublvols — INBOUND (tertiary -> recovering secondary),
    no leadership.
  * source-level invariants: unblock ALL node LVS ports; device-status
    positive gate (no blind sleep); never promote leadership in the runner.
"""

import os
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.storage_node import StorageNode


def _read_src():
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    with open(os.path.join(root, "simplyblock_core/services/tasks_runner_port_allow.py")) as f:
        return f.read()


def _node(uuid="n", status=StorageNode.STATUS_ONLINE, lvs_ports=None,
          lvstore=None, sec=None, tert=None):
    n = MagicMock(spec=StorageNode)
    n.uuid = uuid
    n.get_id = MagicMock(return_value=uuid)
    n.status = status
    n.lvstore = lvstore
    n.lvstore_ports = lvs_ports or {}
    n.lvstore_stack_secondary = None
    n.lvstore_stack_tertiary = None
    n.secondary_node_id = sec
    n.tertiary_node_id = tert
    n.hublvol = MagicMock()
    n.cluster_id = "c1"
    n.get_lvol_subsys_port = MagicMock(
        side_effect=lambda name=None: (n.lvstore_ports.get(name, {}) or {}).get(
            "lvol_subsys_port", 9090))
    return n


class TestNodeLvsPorts(unittest.TestCase):
    def setUp(self):
        from simplyblock_core.services import tasks_runner_port_allow as mod
        self.mod = mod

    def test_collects_all_lvs_subsys_ports(self):
        n = _node(lvstore="LVS_7", lvs_ports={
            "LVS_7": {"lvol_subsys_port": 4436, "hublvol_port": 4437},
            "LVS_4": {"lvol_subsys_port": 4434, "hublvol_port": 4435},
            "LVS_1": {"lvol_subsys_port": 4432, "hublvol_port": 4433},
        })
        self.assertEqual(self.mod._node_lvs_ports(n), [4432, 4434, 4436])

    def test_includes_own_primary_when_ports_map_empty(self):
        n = _node(lvstore="LVS_7", lvs_ports={})
        n.get_lvol_subsys_port = MagicMock(return_value=4436)
        self.assertEqual(self.mod._node_lvs_ports(n), [4436])

    def test_empty_when_nothing(self):
        n = _node(lvstore=None, lvs_ports={})
        self.assertEqual(self.mod._node_lvs_ports(n), [])


class TestOutboundReconnectSkipRule(unittest.TestCase):
    """Skip an outbound leg only if the target primary is neither online nor
    down; never touch leadership; never do inbound exposure."""

    def setUp(self):
        from simplyblock_core.services import tasks_runner_port_allow as mod
        self.mod = mod

    def _run(self, primary_status):
        node = _node("sec")
        node.lvstore_stack_secondary = "pri"
        primary = _node("pri", status=primary_status, sec="sec")
        db = MagicMock()
        db.get_storage_node_by_id.return_value = primary
        with patch.object(self.mod, "db", db), \
             patch.object(self.mod, "_verify_or_reconnect_peer_hublvol",
                          return_value=True) as verify:
            ok, _ = self.mod._reconnect_own_sec_tert_hublvols(node)
        return ok, verify

    def test_online_primary_connects(self):
        ok, verify = self._run(StorageNode.STATUS_ONLINE)
        self.assertTrue(ok)
        verify.assert_called_once()

    def test_down_primary_connects(self):
        ok, verify = self._run(StorageNode.STATUS_DOWN)
        self.assertTrue(ok)
        verify.assert_called_once()

    def test_offline_primary_skipped_not_failed(self):
        # Offline primary: nothing to attach to for a secondary -> skip,
        # not fail (its own recovery re-drives the leg).
        ok, verify = self._run(StorageNode.STATUS_OFFLINE)
        self.assertTrue(ok)
        verify.assert_not_called()

    def test_verify_failure_suspends(self):
        node = _node("sec")
        node.lvstore_stack_secondary = "pri"
        primary = _node("pri", status=StorageNode.STATUS_ONLINE, sec="sec")
        db = MagicMock()
        db.get_storage_node_by_id.return_value = primary
        with patch.object(self.mod, "db", db), \
             patch.object(self.mod, "_verify_or_reconnect_peer_hublvol",
                          return_value=False):
            ok, msg = self.mod._reconnect_own_sec_tert_hublvols(node)
        self.assertFalse(ok)


class TestInboundReconnect(unittest.TestCase):
    def setUp(self):
        from simplyblock_core.services import tasks_runner_port_allow as mod
        self.mod = mod

    def test_tertiary_inbound_connected(self):
        node = _node("sec")
        node.lvstore_stack_secondary = "pri"
        primary = _node("pri", sec="sec", tert="tert")
        primary.lvstore = "LVS_4"
        primary.hublvol.nqn = "nqn.pri.hub"
        tert = _node("tert", status=StorageNode.STATUS_ONLINE)
        tert.add_hublvol_failover_path = MagicMock(return_value=True)

        def _get(nid):
            return {"pri": primary, "tert": tert}[str(nid)]
        db = MagicMock()
        db.get_storage_node_by_id.side_effect = _get
        db.get_cluster_by_id.return_value = MagicMock(nqn="nqn.cluster")
        rpc = MagicMock()
        rpc.subsystem_list.return_value = [{"nqn": "nqn.pri.hub"}]  # already exposed
        node.rpc_client = MagicMock(return_value=rpc)
        with patch.object(self.mod, "db", db):
            ok, msg = self.mod._reconnect_inbound_hublvols(node)
        self.assertTrue(ok, msg)
        tert.add_hublvol_failover_path.assert_called_once()

    def test_tertiary_inbound_failure_gates(self):
        node = _node("sec")
        node.lvstore_stack_secondary = "pri"
        primary = _node("pri", sec="sec", tert="tert")
        primary.lvstore = "LVS_4"
        primary.hublvol.nqn = "nqn.pri.hub"
        tert = _node("tert", status=StorageNode.STATUS_ONLINE)
        tert.add_hublvol_failover_path = MagicMock(return_value=False)

        def _get(nid):
            return {"pri": primary, "tert": tert}[str(nid)]
        db = MagicMock()
        db.get_storage_node_by_id.side_effect = _get
        db.get_cluster_by_id.return_value = MagicMock(nqn="nqn.cluster")
        rpc = MagicMock()
        rpc.subsystem_list.return_value = [{"nqn": "nqn.pri.hub"}]
        node.rpc_client = MagicMock(return_value=rpc)
        with patch.object(self.mod, "db", db):
            ok, msg = self.mod._reconnect_inbound_hublvols(node)
        self.assertFalse(ok)

    def test_non_secondary_noop(self):
        node = _node("x")  # no lvstore_stack_secondary
        db = MagicMock()
        with patch.object(self.mod, "db", db):
            ok, _ = self.mod._reconnect_inbound_hublvols(node)
        self.assertTrue(ok)
        db.get_storage_node_by_id.assert_not_called()


class TestSourceInvariants(unittest.TestCase):
    """Pins that survive refactors and match the agreed design."""

    @classmethod
    def setUpClass(cls):
        cls.src = _read_src()

    def test_runner_never_promotes_leadership(self):
        # Ignore comments/docstrings (they explain WHY we don't promote);
        # assert no executable line promotes leadership.
        code_lines = [ln for ln in self.src.splitlines()
                      if "leader=True" in ln and not ln.lstrip().startswith("#")
                      and "``leader=True``" not in ln]
        self.assertEqual(code_lines, [],
                         "the port-allow runner must never promote leadership "
                         f"(primary self-promotes on first IO); found: {code_lines}")

    def _fn_body(self, name):
        s = self.src.index(f"def {name}(")
        e = self.src.index("\ndef ", s + 1)
        return self.src[s:e]

    def test_outbound_fn_has_no_leadership_or_inbound(self):
        body = self._fn_body("_reconnect_own_sec_tert_hublvols")
        self.assertNotIn("bdev_lvol_set_leader", body)
        self.assertNotIn("bdev_distrib_force_to_non_leader", body)
        self.assertNotIn("create_secondary_hublvol", body)
        self.assertNotIn("add_hublvol_failover_path", body)

    def test_outbound_uses_online_or_down_skip(self):
        body = self._fn_body("_reconnect_own_sec_tert_hublvols")
        self.assertIn("_HUBLVOL_TARGET_REACHABLE", body)
        self.assertNotIn('lvstore_status != "ready"', body)

    def test_inbound_fn_has_no_leadership(self):
        body = self._fn_body("_reconnect_inbound_hublvols")
        self.assertNotIn("bdev_lvol_set_leader", body)
        self.assertNotIn("bdev_distrib_force_to_non_leader", body)

    def test_unblocks_all_node_lvs_ports(self):
        # exec_port_allow_task must unblock every LVS port, not a single one.
        s = self.src.index("def exec_port_allow_task")
        body = self.src[s:]
        self.assertIn("_node_lvs_ports(node)", body)
        self.assertIn("for p in lvs_ports:", body)

    def test_device_status_positive_gate_not_blind_sleep(self):
        s = self.src.index("def exec_port_allow_task")
        body = self.src[s:]
        self.assertIn("not applied by all", body)          # local broadcast gate
        self.assertIn("not applied by recovering", body)   # targeted gate
        self.assertNotIn('waiting 5s for JMs to connect', body)  # old blind sleep gone


if __name__ == "__main__":
    unittest.main()
