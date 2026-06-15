# coding=utf-8
"""
test_shared_placement.py — unit tests for the cluster-wide shared_placement
upgrade procedure.

Spec recap (from the upgrade-procedure note in PR review):
- Forward-only safety: off->on is always accepted.
- on->off is debug-only and requires force=True.
- Preflight gates (skippable only with force): cluster must be ACTIVE and
  not rebalancing; every storage node must be ONLINE.
- On accept: the runtime distr_shared_placement RPC is dispatched to every
  online node (no name => all distrib bdevs on that node); the flag is
  persisted on cluster.shared_placement AND on every node's lvstore_stack
  distrib entries so restarts re-create with the new mode.

These tests pin those invariants without touching FDB.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_cluster(status=Cluster.STATUS_ACTIVE, rebalancing=False,
                  shared_placement=False):
    c = Cluster()
    c.uuid = "cluster-uuid-1"
    c.status = status
    c.is_re_balancing = rebalancing
    c.shared_placement = shared_placement
    return c


def _make_node(node_id, status=StorageNode.STATUS_ONLINE,
               distrib_names=("distrib_1",)):
    """Return a MagicMock StorageNode whose rpc_client returns a per-node
    MagicMock with subsystem_list/distr_shared_placement stubs and whose
    lvstore_stack carries one bdev_distr entry per name in distrib_names.
    """
    n = MagicMock(spec=StorageNode)
    n.uuid = node_id
    n.status = status
    n.get_id.return_value = node_id
    n.lvstore_stack = [
        {"type": "bdev_distr",
         "name": name,
         "params": {"name": name, "vuid": 7000 + i, "ndcs": 1, "npcs": 1,
                    "num_blocks": 1000, "block_size": 4096,
                    "chunk_size": 4096, "pba_page_size": 2097152,
                    "write_protection": False}}
        for i, name in enumerate(distrib_names)
    ]
    n.lvstore_stack_secondary = []
    n.lvstore_stack_tertiary = []

    rpc = MagicMock()
    rpc.distr_shared_placement = MagicMock(return_value=True)
    rpc.jm_set_shared_placement = MagicMock(return_value=True)
    n.rpc_client_mock = rpc
    n.rpc_client.return_value = rpc

    n.write_to_db = MagicMock()
    return n


class _Patched(unittest.TestCase):
    """Wires DBController / cluster_ops module so set_shared_placement can
    run end-to-end against in-memory mocks.
    """

    def _patch(self, cluster, nodes):
        from simplyblock_core import cluster_ops as mod

        db_mock = MagicMock()
        db_mock.get_cluster_by_id.return_value = cluster
        db_mock.get_storage_nodes_by_cluster_id.return_value = nodes
        db_mock.kv_store = MagicMock()

        cluster.write_to_db = MagicMock()

        self._patches = [
            patch.object(mod, "db_controller", db_mock),
        ]
        for p in self._patches:
            p.start()
        return mod, db_mock

    def tearDown(self):
        for p in getattr(self, "_patches", []):
            p.stop()


# ---------------------------------------------------------------------------
# Preflight gates
# ---------------------------------------------------------------------------


class TestPreflight(_Patched):

    def test_rejects_when_cluster_not_active(self):
        c = _make_cluster(status=Cluster.STATUS_DEGRADED)
        nodes = [_make_node("n1"), _make_node("n2"), _make_node("n3")]
        mod, _db = self._patch(c, nodes)

        ok = mod.set_shared_placement(c.uuid, enable=True)

        self.assertFalse(ok)
        self.assertFalse(c.shared_placement)
        # No RPC fired.
        for n in nodes:
            n.rpc_client_mock.distr_shared_placement.assert_not_called()

    def test_rejects_when_rebalancing(self):
        c = _make_cluster(rebalancing=True)
        nodes = [_make_node("n1"), _make_node("n2"), _make_node("n3")]
        mod, _db = self._patch(c, nodes)

        ok = mod.set_shared_placement(c.uuid, enable=True)

        self.assertFalse(ok)
        self.assertFalse(c.shared_placement)

    def test_rejects_when_any_node_not_online(self):
        c = _make_cluster()
        nodes = [_make_node("n1"),
                 _make_node("n2", status=StorageNode.STATUS_RESTARTING),
                 _make_node("n3")]
        mod, _db = self._patch(c, nodes)

        ok = mod.set_shared_placement(c.uuid, enable=True)

        self.assertFalse(ok)
        # No RPC fired anywhere — preflight rejected before dispatch.
        for n in nodes:
            n.rpc_client_mock.distr_shared_placement.assert_not_called()

    def test_force_bypasses_rebalancing_and_offline_node_guards(self):
        c = _make_cluster(rebalancing=True)
        nodes = [_make_node("n1"),
                 _make_node("n2", status=StorageNode.STATUS_DOWN),
                 _make_node("n3")]
        mod, _db = self._patch(c, nodes)

        ok = mod.set_shared_placement(c.uuid, enable=True, force=True)

        self.assertTrue(ok)
        self.assertTrue(c.shared_placement)
        # Only ONLINE nodes get the runtime RPC.
        nodes[0].rpc_client_mock.distr_shared_placement.assert_called_once()
        nodes[1].rpc_client_mock.distr_shared_placement.assert_not_called()
        nodes[2].rpc_client_mock.distr_shared_placement.assert_called_once()


# ---------------------------------------------------------------------------
# Forward-only direction guard
# ---------------------------------------------------------------------------


class TestDirectionGuards(_Patched):

    def test_disable_without_force_is_rejected(self):
        c = _make_cluster(shared_placement=True)
        nodes = [_make_node("n1"), _make_node("n2"), _make_node("n3")]
        mod, _db = self._patch(c, nodes)

        ok = mod.set_shared_placement(c.uuid, enable=False, force=False)

        self.assertFalse(ok)
        # Still on.
        self.assertTrue(c.shared_placement)
        for n in nodes:
            n.rpc_client_mock.distr_shared_placement.assert_not_called()

    def test_disable_with_force_is_accepted(self):
        c = _make_cluster(shared_placement=True)
        n1 = _make_node("n1")
        # Mark stacks with shared_placement=True so we can assert removal.
        n1.lvstore_stack[0]["params"]["shared_placement"] = True
        mod, _db = self._patch(c, [n1])

        ok = mod.set_shared_placement(c.uuid, enable=False, force=True)

        self.assertTrue(ok)
        self.assertFalse(c.shared_placement)
        n1.rpc_client_mock.distr_shared_placement.assert_called_once()
        # Argument check: enable=False, no name.
        kwargs = n1.rpc_client_mock.distr_shared_placement.call_args.kwargs
        self.assertEqual(kwargs.get("enable"), False)
        # Param scrubbed off the stack entry.
        self.assertNotIn("shared_placement", n1.lvstore_stack[0]["params"])
        n1.write_to_db.assert_called()

    def test_idempotent_when_already_at_target_state(self):
        c = _make_cluster(shared_placement=True)
        nodes = [_make_node("n1")]
        mod, _db = self._patch(c, nodes)

        ok = mod.set_shared_placement(c.uuid, enable=True)

        self.assertTrue(ok)
        # No RPC, no DB write — short-circuit.
        nodes[0].rpc_client_mock.distr_shared_placement.assert_not_called()
        c.write_to_db.assert_not_called()


# ---------------------------------------------------------------------------
# Happy path: RPC dispatch + persistence
# ---------------------------------------------------------------------------


class TestEnableHappyPath(_Patched):

    def test_dispatches_rpc_to_every_online_node(self):
        c = _make_cluster()
        nodes = [_make_node("n1"), _make_node("n2"), _make_node("n3")]
        mod, _db = self._patch(c, nodes)

        ok = mod.set_shared_placement(c.uuid, enable=True)

        self.assertTrue(ok)
        for n in nodes:
            n.rpc_client_mock.distr_shared_placement.assert_called_once()
            kwargs = n.rpc_client_mock.distr_shared_placement.call_args.kwargs
            self.assertEqual(kwargs.get("enable"), True)
            # No name -> applies to all distrib bdevs on the node.
            self.assertNotIn("name", kwargs)

    def test_dispatches_jm_shared_placement_to_every_online_node(self):
        # JM is migrated in full analogy with distrib, in the same loop.
        c = _make_cluster()
        nodes = [_make_node("n1"), _make_node("n2"), _make_node("n3")]
        mod, _db = self._patch(c, nodes)

        ok = mod.set_shared_placement(c.uuid, enable=True)

        self.assertTrue(ok)
        for n in nodes:
            n.rpc_client_mock.jm_set_shared_placement.assert_called_once()
            kwargs = n.rpc_client_mock.jm_set_shared_placement.call_args.kwargs
            self.assertEqual(kwargs.get("enable"), True)
            # The JM RPC requires an explicit bdev name (one JM per node,
            # named jm_<node_id>) — unlike distrib's all-bdevs no-name form.
            self.assertEqual(kwargs.get("name"), f"jm_{n.get_id()}")

    def test_jm_rpc_failure_without_force_aborts_persist(self):
        # A JM RPC rejection alone (distrib OK) must still abort + not persist.
        c = _make_cluster()
        n1 = _make_node("n1")
        n2 = _make_node("n2")
        n2.rpc_client_mock.jm_set_shared_placement.return_value = False
        mod, _db = self._patch(c, [n1, n2])

        ok = mod.set_shared_placement(c.uuid, enable=True, force=False)

        self.assertFalse(ok)
        self.assertFalse(c.shared_placement)
        c.write_to_db.assert_not_called()

    def test_persists_flag_on_cluster_and_every_node_stack(self):
        c = _make_cluster()
        nodes = [
            _make_node("n1", distrib_names=("d1", "d2")),
            _make_node("n2", distrib_names=("d3",)),
        ]
        # Mix in a non-distrib stack entry to make sure we don't touch it.
        nodes[0].lvstore_stack.insert(
            0, {"type": "bdev_alceml", "name": "alc", "params": {"name": "alc"}})

        mod, _db = self._patch(c, nodes)
        mod.set_shared_placement(c.uuid, enable=True)

        self.assertTrue(c.shared_placement)
        c.write_to_db.assert_called_once()

        # Each distrib entry on each node got the flag; non-distrib entry
        # is unchanged.
        for entry in nodes[0].lvstore_stack:
            if entry["type"] == "bdev_distr":
                self.assertTrue(entry["params"]["shared_placement"])
            else:
                self.assertNotIn("shared_placement", entry["params"])
        for entry in nodes[1].lvstore_stack:
            self.assertTrue(entry["params"]["shared_placement"])

        for n in nodes:
            n.write_to_db.assert_called()

    def test_rpc_failure_without_force_aborts_persist(self):
        c = _make_cluster()
        n1 = _make_node("n1")
        n2 = _make_node("n2")
        n2.rpc_client_mock.distr_shared_placement.return_value = False
        mod, _db = self._patch(c, [n1, n2])

        ok = mod.set_shared_placement(c.uuid, enable=True, force=False)

        self.assertFalse(ok)
        self.assertFalse(c.shared_placement)
        c.write_to_db.assert_not_called()
        # Stack entries left untouched.
        for n in (n1, n2):
            for entry in n.lvstore_stack:
                self.assertNotIn("shared_placement", entry["params"])

    def test_rpc_failure_with_force_still_persists(self):
        c = _make_cluster()
        n1 = _make_node("n1")
        n2 = _make_node("n2")
        n2.rpc_client_mock.distr_shared_placement.return_value = False
        mod, _db = self._patch(c, [n1, n2])

        ok = mod.set_shared_placement(c.uuid, enable=True, force=True)

        self.assertTrue(ok)
        self.assertTrue(c.shared_placement)
        c.write_to_db.assert_called_once()


# ---------------------------------------------------------------------------
# RPC method shape (independent of the controller flow)
# ---------------------------------------------------------------------------


class TestRpcMethodShape(unittest.TestCase):

    def test_bdev_distrib_create_emits_shared_placement_when_true(self):
        from simplyblock_core.rpc_client import RPCClient

        with patch("requests.session"):
            c = RPCClient("127.0.0.1", 8081, "u", "p", timeout=1, retry=0)
        with patch.object(c, "get_bdevs", return_value=None), \
             patch.object(c, "_request", return_value=True) as mock_req:
            c.bdev_distrib_create(
                name="distrib_42", vuid=42, ndcs=1, npcs=1,
                num_blocks=1000, block_size=4096,
                jm_names=["jm_1"], chunk_size=4096,
                shared_placement=True)

        params = mock_req.call_args.args[1]
        self.assertEqual(mock_req.call_args.args[0], "bdev_distrib_create")
        self.assertEqual(params["shared_placement"], True)

    def test_bdev_distrib_create_omits_shared_placement_by_default(self):
        from simplyblock_core.rpc_client import RPCClient

        with patch("requests.session"):
            c = RPCClient("127.0.0.1", 8081, "u", "p", timeout=1, retry=0)
        with patch.object(c, "get_bdevs", return_value=None), \
             patch.object(c, "_request", return_value=True) as mock_req:
            c.bdev_distrib_create(
                name="distrib_42", vuid=42, ndcs=1, npcs=1,
                num_blocks=1000, block_size=4096,
                jm_names=["jm_1"], chunk_size=4096)

        params = mock_req.call_args.args[1]
        # Absent (not False) — matches the spec's "Default: false" semantics.
        self.assertNotIn("shared_placement", params)

    def test_distr_shared_placement_single_bdev(self):
        from simplyblock_core.rpc_client import RPCClient

        with patch("requests.session"):
            c = RPCClient("127.0.0.1", 8081, "u", "p", timeout=1, retry=0)
        with patch.object(c, "_request", return_value=True) as mock_req:
            c.distr_shared_placement(name="distrib_1", enable=True)

        self.assertEqual(mock_req.call_args.args[0], "distr_shared_placement")
        self.assertEqual(mock_req.call_args.args[1],
                         {"name": "distrib_1", "enable": True})

    def test_distr_shared_placement_all_bdevs_when_name_omitted(self):
        from simplyblock_core.rpc_client import RPCClient

        with patch("requests.session"):
            c = RPCClient("127.0.0.1", 8081, "u", "p", timeout=1, retry=0)
        with patch.object(c, "_request", return_value=True) as mock_req:
            c.distr_shared_placement(enable=True)

        params = mock_req.call_args.args[1]
        self.assertEqual(params, {"enable": True})
        self.assertNotIn("name", params)

    # --- JM analog ---------------------------------------------------------

    def test_bdev_jm_create_emits_shared_placement_when_true(self):
        from simplyblock_core.rpc_client import RPCClient

        with patch("requests.session"):
            c = RPCClient("127.0.0.1", 8081, "u", "p", timeout=1, retry=0)
        with patch.object(c, "_request", return_value=True) as mock_req:
            c.bdev_jm_create(name="jm_1", name_storage1="alceml_1",
                             shared_placement=True)

        self.assertEqual(mock_req.call_args.args[0], "bdev_jm_create")
        params = mock_req.call_args.args[1]
        self.assertEqual(params["shared_placement"], True)

    def test_bdev_jm_create_omits_shared_placement_by_default(self):
        from simplyblock_core.rpc_client import RPCClient

        with patch("requests.session"):
            c = RPCClient("127.0.0.1", 8081, "u", "p", timeout=1, retry=0)
        with patch.object(c, "_request", return_value=True) as mock_req:
            c.bdev_jm_create(name="jm_1", name_storage1="alceml_1")

        params = mock_req.call_args.args[1]
        # Absent (not False) when the cluster has not opted in — matches the
        # spec's "Default: false" semantics and the distrib create flag.
        self.assertNotIn("shared_placement", params)

    def test_jm_set_shared_placement_single_bdev(self):
        from simplyblock_core.rpc_client import RPCClient

        with patch("requests.session"):
            c = RPCClient("127.0.0.1", 8081, "u", "p", timeout=1, retry=0)
        with patch.object(c, "_request", return_value=True) as mock_req:
            c.jm_set_shared_placement(name="jm_1", enable=True)

        self.assertEqual(mock_req.call_args.args[0], "jm_set_shared_placement")
        self.assertEqual(mock_req.call_args.args[1],
                         {"name": "jm_1", "enable": True})

    def test_jm_set_shared_placement_requires_name(self):
        # The data-plane RPC mandates a bdev name (one JM per node); the
        # client signature enforces it rather than silently sending an
        # all-bdevs request the way distr_shared_placement does.
        from simplyblock_core.rpc_client import RPCClient

        with patch("requests.session"):
            c = RPCClient("127.0.0.1", 8081, "u", "p", timeout=1, retry=0)
        with self.assertRaises(TypeError):
            c.jm_set_shared_placement(enable=True)

    def test_jm_set_shared_placement_disable(self):
        from simplyblock_core.rpc_client import RPCClient

        with patch("requests.session"):
            c = RPCClient("127.0.0.1", 8081, "u", "p", timeout=1, retry=0)
        with patch.object(c, "_request", return_value=True) as mock_req:
            c.jm_set_shared_placement(name="jm_1", enable=False)

        self.assertEqual(mock_req.call_args.args[1],
                         {"name": "jm_1", "enable": False})


if __name__ == "__main__":
    unittest.main()
