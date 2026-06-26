# coding=utf-8
"""
test_activation_idempotent.py — tests for the activation-mode idempotency
helpers and for the new ``LVSRestartRequiredError`` behaviour.

Background: when ``cluster_activate`` runs against a live SPDK that still
owns its LVS stack (typical after SUSPENDED without process kill), the
recreate path used to re-issue ``bdev_examine``, ``subsystem_create``,
``nvmf_subsystem_add_ns`` and ``listeners_create`` — every one of which
fails with a "Duplicate" or "already exists" error in SPDK and leaves
noisy logs behind. The helpers here let the recreate path probe first
and skip the RPC when the object is already present. On the one case the
probe can't reconcile — an LVS that ``bdev_examine`` did not resurrect —
we raise ``LVSRestartRequiredError`` so the caller rejects the activation.
"""

import unittest
from unittest.mock import MagicMock

from simplyblock_core.storage_node_ops import (
    LVSRestartRequiredError,
    _rpc_bdev_exists,
    _rpc_lvstore_exists,
    _rpc_subsystem_exists,
    _rpc_subsystem_has_listener,
    _rpc_subsystem_has_ns,
)


# ---------------------------------------------------------------------------
# 1. Existence probes
# ---------------------------------------------------------------------------


class TestExistenceProbes(unittest.TestCase):

    def test_bdev_exists_true(self):
        rpc = MagicMock()
        rpc.get_bdevs.return_value = [{"name": "raid0_1"}]
        self.assertTrue(_rpc_bdev_exists(rpc, "raid0_1"))

    def test_bdev_exists_false_when_empty(self):
        rpc = MagicMock()
        rpc.get_bdevs.return_value = []
        self.assertFalse(_rpc_bdev_exists(rpc, "raid0_1"))

    def test_bdev_exists_false_when_rpc_raises(self):
        rpc = MagicMock()
        rpc.get_bdevs.side_effect = RuntimeError("rpc fail")
        self.assertFalse(_rpc_bdev_exists(rpc, "raid0_1"))

    def test_lvstore_exists_true(self):
        rpc = MagicMock()
        rpc.bdev_lvol_get_lvstores.return_value = [{"name": "LVS_A"}]
        self.assertTrue(_rpc_lvstore_exists(rpc, "LVS_A"))

    def test_lvstore_exists_false(self):
        rpc = MagicMock()
        rpc.bdev_lvol_get_lvstores.return_value = None
        self.assertFalse(_rpc_lvstore_exists(rpc, "LVS_A"))

    def test_subsystem_exists_true(self):
        rpc = MagicMock()
        rpc.subsystem_list.return_value = [{"nqn": "nqn.test"}]
        self.assertTrue(_rpc_subsystem_exists(rpc, "nqn.test"))

    def test_subsystem_exists_false_when_missing(self):
        rpc = MagicMock()
        rpc.subsystem_list.return_value = []
        self.assertFalse(_rpc_subsystem_exists(rpc, "nqn.test"))

    def test_subsystem_has_ns_by_nsid_and_bdev(self):
        rpc = MagicMock()
        rpc.subsystem_list.return_value = [{
            "nqn": "nqn.test",
            "namespaces": [{"nsid": 1, "bdev_name": "LVS_A/vol1"}],
        }]
        self.assertTrue(_rpc_subsystem_has_ns(rpc, "nqn.test",
                                              nsid=1, bdev_name="LVS_A/vol1"))

    def test_subsystem_has_ns_mismatched_bdev_returns_false(self):
        rpc = MagicMock()
        rpc.subsystem_list.return_value = [{
            "nqn": "nqn.test",
            "namespaces": [{"nsid": 1, "bdev_name": "OTHER"}],
        }]
        self.assertFalse(_rpc_subsystem_has_ns(rpc, "nqn.test",
                                               nsid=1, bdev_name="LVS_A/vol1"))

    def test_subsystem_has_ns_no_subsystem(self):
        rpc = MagicMock()
        rpc.subsystem_list.return_value = []
        self.assertFalse(_rpc_subsystem_has_ns(rpc, "nqn.test", nsid=1))

    def test_subsystem_has_listener_match(self):
        rpc = MagicMock()
        rpc.subsystem_list.return_value = [{
            "nqn": "nqn.test",
            "listen_addresses": [
                {"trtype": "tcp", "traddr": "10.0.0.1", "trsvcid": "4420"},
            ],
        }]
        self.assertTrue(_rpc_subsystem_has_listener(
            rpc, "nqn.test", "TCP", "10.0.0.1", 4420))

    def test_subsystem_has_listener_mismatch_trsvcid(self):
        rpc = MagicMock()
        rpc.subsystem_list.return_value = [{
            "nqn": "nqn.test",
            "listen_addresses": [
                {"trtype": "TCP", "traddr": "10.0.0.1", "trsvcid": 4420},
            ],
        }]
        self.assertFalse(_rpc_subsystem_has_listener(
            rpc, "nqn.test", "TCP", "10.0.0.1", 4422))

    def test_subsystem_has_listener_mismatch_traddr(self):
        rpc = MagicMock()
        rpc.subsystem_list.return_value = [{
            "nqn": "nqn.test",
            "listen_addresses": [
                {"trtype": "TCP", "traddr": "10.0.0.1", "trsvcid": 4420},
            ],
        }]
        self.assertFalse(_rpc_subsystem_has_listener(
            rpc, "nqn.test", "TCP", "10.0.0.2", 4420))


# ---------------------------------------------------------------------------
# 2. LVSRestartRequiredError shape
# ---------------------------------------------------------------------------


class TestLVSRestartRequiredError(unittest.TestCase):

    def test_carries_node_and_lvs(self):
        e = LVSRestartRequiredError("node-xyz", "LVS_777",
                                    detail="raid present but lvstore missing")
        self.assertEqual(e.node_id, "node-xyz")
        self.assertEqual(e.lvs_name, "LVS_777")
        self.assertIn("LVS_777", str(e))
        self.assertIn("node-xyz", str(e))
        self.assertIn("Restart this node", str(e))

    def test_omits_detail_cleanly_when_absent(self):
        e = LVSRestartRequiredError("node-xyz", "LVS_777")
        self.assertNotIn(": .", str(e))
        self.assertIn("Restart this node", str(e))


if __name__ == "__main__":
    unittest.main()
